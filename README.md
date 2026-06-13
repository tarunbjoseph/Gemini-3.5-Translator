# Global Live Voice Translator (`gemini-3.5-live-translate`)

A real-time, bi-directional, multi-user voice translation web application powered by Google's low-latency `gemini-3.5-live-translate-preview` streaming model. This application acts like a cross-lingual phone call over the web—allowing two users anywhere in the world to open a shared room, speak in their native tongues, and hear each other seamlessly translated in real time.

## 🚀 Features
* **True Streaming Translation:** Utilizes continuous bi-directional WebSockets to minimize audio latency.
* **Multi-User Rooms:** Supports dynamic room creation (`?room=room_name`) for isolated dual-participant call sessions.
* **Multi-Language Selectors:** Choose target output paths dynamically (Spanish, French, German, Japanese, Chinese, Italian, Hindi, etc.).
* **Pure Audio Pipeline:** Automatically downsamples microphone capture to 16kHz PCM (Gemini input requirement) and plays back the returning 24kHz PCM translated stream smoothly.
* **Production Ready for Free Deployment:** Configured to work natively behind HTTPS proxies required for secure browser microphone access.

---

## 🏗️ Architecture Flow

```text
[User A (English)] ──(16kHz PCM)──> [WebSocket A] ──> [Server Room Session]
                                                               │
                                                    (Gemini Live Instance A)
                                                               │
[User B (Spanish)] <──(24kHz PCM)── [WebSocket B] <────────────┘
