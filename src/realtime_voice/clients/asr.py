"""下游语音识别服务的异步客户端。"""

import base64

import httpx

from realtime_voice.audio.pcm import pcm16_wav_bytes
from realtime_voice.clients.limits import BoundedAdmission


class AsrError(RuntimeError):
    """当 ASR 服务无法返回可用转写结果时抛出。"""


def clean_asr_text(content: str) -> str:
    """提取 Qwen ASR 转写文本，并将空占位值归一化为空字符串。"""
    _, tag, extracted = content.partition("<asr_text>")
    text = (extracted if tag else content).strip()
    return "" if text.lower() in {"", "none", "null", "undefined"} else text


class AsrClient:
    """通过注入的共享 ASR HTTP 客户端提交 PCM16 音频。"""

    def __init__(self, http: httpx.AsyncClient, admission: BoundedAdmission) -> None:
        self.http = http
        self.admission = admission

    async def transcribe(self, pcm16_16k: bytes) -> str:
        """返回 16 kHz 单声道 PCM16 音频的归一化转写文本。"""
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
    """校验并提取补全文本，且不外泄响应结构的细节错误。"""
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
