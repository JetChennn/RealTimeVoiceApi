"""Send one PCM16 WAV turn to RealTimeVoiceAPI and save the returned audio."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import sys
import uuid
import wave
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from websockets.asyncio.client import connect


@dataclass(frozen=True, slots=True)
class AudioChunk:
    sequence: int
    pcm: bytes


def iter_pcm_chunks(pcm: bytes, sample_rate: int, chunk_ms: int = 40) -> Iterator[AudioChunk]:
    """Split sample-aligned PCM16 into monotonically numbered chunks."""
    if sample_rate <= 0 or chunk_ms <= 0:
        raise ValueError("sample_rate and chunk_ms must be positive")
    if len(pcm) % 2:
        raise ValueError("PCM16 data must contain complete samples")
    chunk_bytes = sample_rate * chunk_ms // 1000 * 2
    if chunk_bytes <= 0:
        raise ValueError("chunk duration is too short for the sample rate")
    for sequence, offset in enumerate(range(0, len(pcm), chunk_bytes)):
        yield AudioChunk(sequence, pcm[offset : offset + chunk_bytes])


def read_pcm16_wav(path: Path, expected_sample_rate: int) -> bytes:
    with wave.open(str(path), "rb") as source:
        description = (source.getnchannels(), source.getsampwidth(), source.getframerate())
        expected = (1, 2, expected_sample_rate)
        if description != expected:
            raise ValueError(
                f"WAV must be mono PCM16 at {expected_sample_rate} Hz; got {description}"
            )
        if source.getcomptype() != "NONE":
            raise ValueError("WAV must contain uncompressed PCM")
        return source.readframes(source.getnframes())


class TurnAudioWriter:
    """Collect ordered audio deltas and retain the newest non-interrupted turn."""

    def __init__(self, path: Path, sample_rate: int) -> None:
        self.path = path
        self.sample_rate = sample_rate
        self._chunks: dict[int, list[bytes]] = {}
        self._expected_sequence: dict[int, int] = {}
        self._interrupted: set[int] = set()

    def add(self, turn_id: int, sequence: int, pcm: bytes) -> None:
        if turn_id in self._interrupted:
            return
        if len(pcm) % 2:
            raise ValueError("AUDIO_DELTA contains an incomplete PCM16 sample")
        expected = self._expected_sequence.get(turn_id, 0)
        if sequence != expected:
            raise ValueError(
                f"turn {turn_id} audio sequence mismatch: expected {expected}, got {sequence}"
            )
        self._chunks.setdefault(turn_id, []).append(pcm)
        self._expected_sequence[turn_id] = expected + 1

    def interrupt(self, turn_id: int) -> None:
        self._interrupted.add(turn_id)
        self._chunks.pop(turn_id, None)
        self._expected_sequence.pop(turn_id, None)

    def write(self) -> int | None:
        turns = [turn_id for turn_id, chunks in self._chunks.items() if chunks]
        selected = max(turns, default=None)
        pcm16 = b"" if selected is None else b"".join(self._chunks[selected])
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with wave.open(str(self.path), "wb") as output:
            output.setnchannels(1)
            output.setsampwidth(2)
            output.setframerate(self.sample_rate)
            output.writeframes(pcm16)
        return selected


def create_session_message(session_id: str, sample_rate: int) -> dict[str, Any]:
    return {
        "type": "CREATE_SESSION",
        "protocol_version": 1,
        "device_id": f"realtime-client-{uuid.uuid4().hex[:12]}",
        "session_id": session_id,
        "audio_format": "PCM16",
        "audio_transport": "BASE64_JSON",
        "sample_rate": sample_rate,
        "channels": 1,
    }


async def _receive_response(websocket: Any, writer: TurnAudioWriter, timeout: float) -> None:
    while True:
        raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
        if not isinstance(raw, str):
            raise TypeError("server sent an unexpected binary WebSocket frame")
        message = json.loads(raw)
        kind = message.get("type")
        if kind == "ASR_RESULT":
            print(f"ASR[{message['turn_id']}]: {message['text']}")
        elif kind == "TEXT_DELTA":
            print(message["delta"], end="", flush=True)
        elif kind == "TEXT_END":
            print()
        elif kind == "AUDIO_DELTA":
            writer.add(
                message["turn_id"],
                message["sequence"],
                base64.b64decode(message["audio_b64"], validate=True),
            )
        elif kind == "TURN_STATE" and message.get("state") == "INTERRUPTED":
            writer.interrupt(message["turn_id"])
        elif kind == "ERROR":
            print(
                f"ERROR[{message.get('stage')}:{message.get('code')}]: {message.get('message')}",
                file=sys.stderr,
            )
            if not message.get("recoverable", False):
                raise RuntimeError(message.get("code", "server error"))
        elif kind == "RESPONSE_END":
            return


async def run_client(
    *, url: str, wav_path: Path, sample_rate: int, output: Path, timeout: float = 180.0
) -> None:
    pcm16 = read_pcm16_wav(wav_path, sample_rate)
    session_id = f"client-{uuid.uuid4().hex}"
    writer = TurnAudioWriter(output, sample_rate)
    async with connect(url, max_size=None, open_timeout=timeout) as websocket:
        await websocket.send(json.dumps(create_session_message(session_id, sample_rate)))
        created = json.loads(await asyncio.wait_for(websocket.recv(), timeout=timeout))
        if created.get("type") != "SESSION_CREATED":
            raise RuntimeError(f"session creation failed: {created}")

        sequence = 0
        for chunk in iter_pcm_chunks(pcm16, sample_rate):
            await websocket.send(
                json.dumps(
                    {
                        "type": "AUDIO_CHUNK",
                        "session_id": session_id,
                        "sequence": sequence,
                        "timestamp_ms": sequence * 40,
                        "audio_b64": base64.b64encode(chunk.pcm).decode("ascii"),
                    }
                )
            )
            sequence += 1
            await asyncio.sleep(0.04)

        # Give the VAD enough trailing silence to close the spoken segment.
        silence = b"\x00\x00" * (sample_rate * 600 // 1000)
        for chunk in iter_pcm_chunks(silence, sample_rate):
            await websocket.send(
                json.dumps(
                    {
                        "type": "AUDIO_CHUNK",
                        "session_id": session_id,
                        "sequence": sequence,
                        "timestamp_ms": sequence * 40,
                        "audio_b64": base64.b64encode(chunk.pcm).decode("ascii"),
                    }
                )
            )
            sequence += 1
            await asyncio.sleep(0.04)

        await _receive_response(websocket, writer, timeout)
        await websocket.send(json.dumps({"type": "CLOSE_SESSION", "session_id": session_id}))

    turn = writer.write()
    print(f"Audio saved to {output} (turn {turn if turn is not None else 'none'})")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="Realtime WebSocket URL")
    parser.add_argument("--wav", required=True, type=Path, help="Mono PCM16 input WAV")
    parser.add_argument("--sample-rate", type=int, default=16000, choices=(16000, 24000, 48000))
    parser.add_argument("--output", required=True, type=Path, help="Output WAV path")
    parser.add_argument("--timeout", type=float, default=180.0)
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    asyncio.run(
        run_client(
            url=args.url,
            wav_path=args.wav,
            sample_rate=args.sample_rate,
            output=args.output,
            timeout=args.timeout,
        )
    )


if __name__ == "__main__":
    main()
