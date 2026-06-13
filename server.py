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

# Structure: { "room_id": { "users": { "user": { "ws": WebSocket, "target_lang": str } }, "bridged": bool } }
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
        response_modalities=["AUDIO", "TEXT"], # FIX: Added TEXT so transcripts actually stream
        translation_config=types.TranslationConfig(
            target_language_code=target_lang,
            echo_target_language=False
        ),
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
                                # 1. Audio data
                                if part.inline_data:
                                    await safe_send_bytes(target_ws, part.inline_data.data)
                                    
                                # 2. Text data (Transcripts)
                                elif part.text:
                                    transcript_payload = json.dumps({
                                        "type": "transcript",
                                        "speaker": user_name,
                                        "text": part.text
                                    })
                                    # Send transcript to both people so they both see the text
                                    await safe_send_text(target_ws, transcript_payload)
                                    await safe_send_text(user_ws, transcript_payload)
                except Exception:
                    pass

            await asyncio.gather(stream_mic_to_gemini(), stream_gemini_to_remote())
            
    except Exception as e:
        print(f"Pipeline closed for {user_name}: {e}")

async def safe_send_text(websocket: WebSocket, payload: str):
    try:
        await websocket.send_text(payload)
    except Exception:
        pass

async def announce_room_ready(room: str, first_user: str, second_user: str):
    room_state = ROOMS.get(room)
    if not room_state:
        return

    first_ws = room_state["users"].get(first_user, {}).get("ws")
    second_ws = room_state["users"].get(second_user, {}).get("ws")
    if not first_ws or not second_ws:
        return

    await safe_send_text(first_ws, json.dumps({
        "type": "status",
        "message": f"Connected with {second_user}. Speak now!",
        "partner": second_user
    }))
    await safe_send_text(second_ws, json.dumps({
        "type": "status",
        "message": f"Connected with {first_user}. Speak now!",
        "partner": first_user
    }))

async def start_bidirectional_bridge(room: str, user_a: str, user_b: str):
    room_state = ROOMS.get(room)
    if not room_state:
        return

    user_a_ws = room_state["users"][user_a]["ws"]
    user_b_ws = room_state["users"][user_b]["ws"]
    user_a_target = room_state["users"][user_a]["target_lang"]
    user_b_target = room_state["users"][user_b]["target_lang"]

    await announce_room_ready(room, user_a, user_b)

    asyncio.create_task(bridge_user_to_gemini(user_a_ws, user_b_ws, user_a_target, user_a))
    asyncio.create_task(bridge_user_to_gemini(user_b_ws, user_a_ws, user_b_target, user_b))

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, room: str, user: str, target_lang: str):
    await websocket.accept()
    
    if room not in ROOMS:
        ROOMS[room] = {"users": {}, "bridged": False}

    ROOMS[room]["users"][user] = {"ws": websocket, "target_lang": target_lang}
    print(f"[{user}] joined Room [{room}] | Target output: [{target_lang}]")
    
    try:
        # Wait in the lobby until a second person joins
        while True:
            await asyncio.sleep(0.1)
            room_state = ROOMS.get(room)
            if not room_state:
                break

            if len(room_state["users"]) == 2 and not room_state["bridged"]:
                room_state["bridged"] = True
                user_ids = list(room_state["users"].keys())
                await start_bidirectional_bridge(room, user_ids[0], user_ids[1])
                break
                
        # Keep the connection open
        while True:
            await asyncio.sleep(10)
            
    except WebSocketDisconnect:
        print(f"[{user}] left the call.")
    finally:
        if room in ROOMS and user in ROOMS[room]["users"]:
            del ROOMS[room]["users"][user]
            ROOMS[room]["bridged"] = False # FIX: Reset bridged state so room can be reused
            if not ROOMS[room]["users"]:
                del ROOMS[room]