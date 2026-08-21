"""Asynchronous client for the downstream automatic speech recognizer."""

import base64

import httpx

from realtime_voice.audio.pcm import pcm16_wav_bytes
from realtime_voice.clients.limits import BoundedAdmission


class AsrError(RuntimeError):
    """Raised when the ASR service cannot provide a usable transcription."""


def clean_asr_text(content: str) -> str:
    """Extract a Qwen ASR transcript and normalize empty sentinel values."""
    _, tag, extracted = content.partition("<asr_text>")
    text = (extracted if tag else content).strip()
    return "" if text.lower() in {"", "none", "null", "undefined"} else text


class AsrClient:
    """Submit PCM16 audio to the injected shared ASR HTTP client."""

    def __init__(self, http: httpx.AsyncClient, admission: BoundedAdmission) -> None:
        self.http = http
        self.admission = admission

    async def transcribe(self, pcm16_16k: bytes) -> str:
        """Return the normalized transcript for 16 kHz mono PCM16 audio."""
        wav = pcm16_wav_bytes(pcm16_16k, 16000)
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_audio",
                            "input_audio": {
                                "data": base64.b64encode(wav).decode("ascii"),
                                "format": "wav",
                            },
                        },
                    ],
                },
            ],
        }

        async def operation() -> str:
            try:
                response = await self.http.post(
                    "/v1/chat/completions", json=payload, timeout=30.0
                )
                response.raise_for_status()
                content = _completion_content(response.json())
                return clean_asr_text(content)
            except (httpx.HTTPError, TypeError, ValueError) as error:
                raise AsrError("ASR transcription failed") from error

        return await self.admission.run(operation)


def _completion_content(payload: object) -> str:
    """Validate and extract completion text without leaking shape errors."""
    if not isinstance(payload, dict):
        raise TypeError("ASR response must be an object")

    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        raise ValueError("ASR response must contain a choice")

    message = choices[0].get("message")
    if not isinstance(message, dict):
        raise TypeError("ASR response choice must contain a message")

    content = message.get("content")
    if not isinstance(content, str):
        raise TypeError("ASR response message content must be text")
    return content
