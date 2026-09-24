"""Speaker-verification abstractions kept independent from the session runtime."""

from __future__ import annotations

from typing import Protocol

import numpy as np


class SpeakerEmbedder(Protocol):
    """Application-shared, async speaker-vector extractor."""

    async def embed(self, pcm16_16k: bytes) -> np.ndarray: ...

    async def aclose(self) -> None: ...
