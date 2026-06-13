import os
import asyncio
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from google import genai
from google.genai import types

app = FastAPI()
client = genai.Client() # Automatically picks up the GEMINI_API_KEY from your environment variables

# Serve the frontend HTML page
@app.get("/")
async def get():
    with open("index.html", "r") as f:
        return HTMLResponse(f.read())

# Handle the WebSocket connection from the browser
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    
    # Configure Gemini 3.5 Live Translate
    config = types.LiveConnectConfig(
        response_modalities=["AUDIO"],
        translation_config=types.TranslationConfig(
            target_language_code="es", # Translating to Spanish for this prototype
            echo_target_language=False
        )
    )
    
    # Connect to Gemini
    async with client.aio.live.connect(model="gemini-3.5-live-translate-preview", config=config) as session:
        
        # Task 1: Receive audio from Browser -> Send to Gemini
        async def receive_from_browser():
            try:
                while True:
                    data = await websocket.receive_bytes()
                    # Send raw PCM chunks to Gemini (requires 16kHz)
                    await session.send_realtime_input(
                        audio=types.Blob(data=data, mime_type="audio/pcm;rate=16000")
                    )
            except Exception as e:
                print("Browser disconnected.")

        # Task 2: Receive translated audio from Gemini -> Send to Browser
        async def receive_from_gemini():
            async for response in session.receive():
                if response.server_content and response.server_content.model_turn:
                    for part in response.server_content.model_turn.parts:
                        if part.inline_data:
                            # Forward the translated 24kHz PCM audio back to the browser
                            await websocket.send_bytes(part.inline_data.data)

        # Run both tasks simultaneously
        await asyncio.gather(receive_from_browser(), receive_from_gemini())