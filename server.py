import os
import asyncio
import json
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from google import genai
from google.genai import types

app = FastAPI()
client = None

# Structure: { "room_id": { "user_id_1": WebSocket, "user_id_2": WebSocket } }
ROOMS = {}


def get_gemini_client():
    global client
    if client is not None:
        return client

    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        key_file = Path(__file__).with_name("API Key.txt")
        if key_file.exists():
            api_key = key_file.read_text(encoding="utf-8").strip()

    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not set and API Key.txt was not found.")

    client = genai.Client(api_key=api_key)
    return client

@app.get("/")
def get():
    return FileResponse(Path(__file__).with_name("index.html"), media_type="text/html")


@app.get("/healthz")
def healthz():
    return {"status": "ok"}

async def bridge_user_to_gemini(user_ws: WebSocket, target_ws: WebSocket, target_lang: str, user_name: str):
    """
    Pipes User A's mic into Gemini, and sends the translated audio AND text transcripts to User B.
    """
    config = types.LiveConnectConfig(
        response_modalities=["AUDIO"],
        translation_config=types.TranslationConfig(
            target_language_code=target_lang,
            echo_target_language=False
        ),
        # Enable text transcriptions for the UI
        input_audio_transcription=types.AudioTranscriptionConfig(),
        output_audio_transcription=types.AudioTranscriptionConfig()
    )
    
    try:
        async with get_gemini_client().aio.live.connect(model="gemini-3.5-live-translate-preview", config=config) as gemini_session:

            async def safe_send_text(websocket: WebSocket, payload: str):
                try:
                    await websocket.send_text(payload)
                except Exception:
                    pass

            async def safe_send_bytes(websocket: WebSocket, payload: bytes):
                try:
                    await websocket.send_bytes(payload)
                except Exception:
                    pass
            
            # Task A: Receive Mic Audio -> Send to Gemini
            async def stream_mic_to_gemini():
                try:
                    while True:
                        data = await user_ws.receive_bytes()
                        await gemini_session.send_realtime_input(
                            audio=types.Blob(data=data, mime_type="audio/pcm;rate=16000")
                        )
                except Exception:
                    pass # User disconnected

            # Task B: Receive Output from Gemini -> Send to Remote Speaker (User B)
            async def stream_gemini_to_remote():
                try:
                    async for response in gemini_session.receive():
                        if response.server_content and response.server_content.model_turn:
                            for part in response.server_content.model_turn.parts:
                                # 1. If it's audio data, send as raw bytes
                                if part.inline_data:
                                    await safe_send_bytes(target_ws, part.inline_data.data)
                                    
                                # 2. If it's text data (transcript), send as a JSON string
                                elif part.text:
                                    transcript_payload = json.dumps({
                                        "type": "transcript",
                                        "speaker": user_name,
                                        "text": part.text
                                    })
                                    await safe_send_text(target_ws, transcript_payload)
                                    await safe_send_text(user_ws, transcript_payload)
                except Exception:
                    pass

            await asyncio.gather(stream_mic_to_gemini(), stream_gemini_to_remote())
            
    except Exception as e:
        print(f"Pipeline closed for {user_name}: {e}")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, room: str, user: str, target_lang: str):
    await websocket.accept()
    
    if room not in ROOMS:
        ROOMS[room] = {}
        
    ROOMS[room][user] = websocket
    print(f"[{user}] joined Room [{room}] | Target output: [{target_lang}]")
    
    try:
        # Wait in the lobby until a second person joins
        while True:
            await asyncio.sleep(0.1)
            if len(ROOMS[room]) == 2:
                # Find the other person's WebSocket
                peer_id = [uid for uid in ROOMS[room].keys() if uid != user][0]
                peer_ws = ROOMS[room][peer_id]
                
                # Start the translation bridge for this user
                asyncio.create_task(bridge_user_to_gemini(websocket, peer_ws, target_lang, user))
                break
                
        # Keep the connection open while the conversation happens
        while True:
            await asyncio.sleep(10)
            
    except WebSocketDisconnect:
        print(f"[{user}] left the call.")
    finally:
        if room in ROOMS and user in ROOMS[room]:
            del ROOMS[room][user]
            if not ROOMS[room]:
                del ROOMS[room]