import os
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from google import genai
from google.genai import types

app = FastAPI()
client = genai.Client()

# In-memory database to manage rooms and active participants
# Structure: { "room_id": { "user_id_1": WebSocket, "user_id_2": WebSocket } }
ROOMS = {}

@app.get("/")
async def get():
    with open("index.html", "r") as f:
        return HTMLResponse(f.read())

async def bridge_user_to_gemini(user_ws: WebSocket, target_ws: WebSocket, target_lang: str):
    """
    Spawns a dedicated Gemini translation instance for one user stream,
    and pipes the resulting audio directly to the other user's speaker.
    """
    config = types.LiveConnectConfig(
        response_modalities=["AUDIO"],
        translation_config=types.TranslationConfig(
            target_language_code=target_lang,
            echo_target_language=False
        )
    )
    
    try:
        async with client.aio.live.connect(model="gemini-3.5-live-translate-preview", config=config) as gemini_session:
            
            # Task A: Listen to this user's mic and forward to their Gemini translator
            async def stream_mic_to_gemini():
                while True:
                    data = await user_ws.receive_bytes()
                    await gemini_session.send_realtime_input(
                        audio=types.Blob(data=data, mime_type="audio/pcm;rate=16000")
                    )
            
            # Task B: Collect translated audio from Gemini and stream it to the *other* user
            async def stream_gemini_to_remote_speaker():
                async for response in gemini_session.receive():
                    if response.server_content and response.server_content.model_turn:
                        for part in response.server_content.model_turn.parts:
                            if part.inline_data:
                                # Send directly to the peer's browser client
                                await target_ws.send_bytes(part.inline_data.data)
                                
            await asyncio.gather(stream_mic_to_gemini(), stream_gemini_to_remote_speaker())
            
    except Exception as e:
        print(f"Stream pipeline closed or interrupted: {e}")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, room: str, user: str, target_lang: str):
    await websocket.accept()
    
    # Initialize room if it doesn't exist
    if room not in ROOMS:
        ROOMS[room] = {}
        
    ROOMS[room][user] = websocket
    print(f"User [{user}] joined Room [{room}] targeting language [{target_lang}]")
    
    try:
        # Keep connection open and wait for a second user to link up the pipelines
        while True:
            await asyncio.sleep(1)
            # If both participants are present, establish the cross-translation bridges
            if len(ROOMS[room]) == 2:
                peer_id = [uid for uid in ROOMS[room].keys() if uid != user][0]
                peer_ws = ROOMS[room][peer_id]
                
                # Fire up this user's outgoing translation pipeline
                await bridge_user_to_gemini(websocket, peer_ws, target_lang)
                break
                
    except WebSocketDisconnect:
        print(f"User [{user}] disconnected from Room [{room}].")
    finally:
        if room in ROOMS and user in ROOMS[room]:
            del ROOMS[room][user]
            if not ROOMS[room]:
                del ROOMS[room]