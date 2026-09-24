import numpy as np

from realtime_voice.audio.vad import SpeechSegment
from realtime_voice.config import Settings
from realtime_voice.speaker.gate import SessionSpeakerGate


class FakeEmbedder:
    def __init__(self, embeddings):
        self.embeddings = iter(embeddings)
        self.inputs: list[bytes] = []

    async def embed(self, pcm16_16k: bytes):
        self.inputs.append(pcm16_16k)
        value = next(self.embeddings)
        if isinstance(value, Exception):
            raise value
        return value

    async def aclose(self):
        return None


def audio(milliseconds: int) -> bytes:
    return b"\xe8\x03\x18\xfc" * (milliseconds * 8)


def segment(segment_id: int = 1, *, milliseconds: int = 1000) -> SpeechSegment:
    samples = milliseconds * 16
    return SpeechSegment(segment_id, audio(milliseconds), voiced_samples=samples)


async def test_unregistered_session_allows_audio_without_extracting_embedding() -> None:
    embedder = FakeEmbedder([])
    gate = SessionSpeakerGate(
        Settings(_env_file=None, speaker_verification_enabled=True), embedder
    )

    decision = await gate.evaluate(segment())

    assert decision.allow is True
    assert decision.reason == "unregistered"
    assert decision.registered is False
    assert embedder.inputs == []


async def test_explicit_registration_enables_verification() -> None:
    embedder = FakeEmbedder(
        [np.array([1.0, 0.0]), np.array([0.98, 0.02]), np.array([0.0, 1.0])]
    )
    gate = SessionSpeakerGate(
        Settings(
            _env_file=None,
            speaker_verification_enabled=True,
            speaker_register_min_audio_ms=2000,
            speaker_verification_threshold=0.8,
        ),
        embedder,
    )

    registered = await gate.register(audio(2500))
    matched = await gate.evaluate(segment(1))
    mismatch = await gate.evaluate(segment(2))

    assert registered.success is True
    assert registered.reason == "registered"
    assert registered.registered is True
    assert registered.replaced is False
    assert matched.reason == "matched"
    assert matched.allow is True
    assert mismatch.reason == "mismatch"
    assert mismatch.allow is False


async def test_similarity_is_clamped_to_the_protocol_cosine_range() -> None:
    embedding = np.array(
        [
            -0.099440284,
            -0.18011458,
            -0.18242268,
            0.04694661,
            0.062732346,
            0.23130009,
            -0.0024336996,
            0.18221956,
            0.24525854,
            0.20116596,
            -0.41369575,
            0.2148989,
            0.059400123,
            0.07411834,
            0.064928316,
            0.06694489,
            0.0558661,
            -0.062774554,
            -0.3325993,
            -0.019049374,
            -0.1405741,
            0.18892244,
            -0.05050576,
            0.014599985,
            -0.14859755,
            -0.089308746,
            -0.0020171525,
            -0.25979468,
            0.052590344,
            -0.018552221,
            -0.20738445,
            -0.41945508,
        ],
        dtype=np.float32,
    )
    # This vector's float32 self-dot exceeds 1 by one ULP after normalization.
    assert float(np.dot(embedding, embedding)) > 1.0
    gate = SessionSpeakerGate(
        Settings(_env_file=None, speaker_verification_enabled=True),
        FakeEmbedder([embedding, embedding]),
    )

    await gate.register(audio(2500))
    decision = await gate.evaluate(segment())

    assert decision.allow is True
    assert decision.similarity == 1.0


async def test_successful_registration_replaces_previous_reference() -> None:
    gate = SessionSpeakerGate(
        Settings(_env_file=None, speaker_verification_enabled=True),
        FakeEmbedder([np.array([1.0, 0.0]), np.array([0.0, 1.0])]),
    )

    first = await gate.register(audio(2500))
    second = await gate.register(audio(2500))

    assert first.reason == "registered"
    assert second.reason == "updated"
    assert second.replaced is True
    assert gate.registered is True


async def test_failed_registration_keeps_previous_reference() -> None:
    gate = SessionSpeakerGate(
        Settings(_env_file=None, speaker_verification_enabled=True),
        FakeEmbedder([np.array([1.0, 0.0]), RuntimeError("failed")]),
    )
    await gate.register(audio(2500))

    failed = await gate.register(audio(2500))

    assert failed.success is False
    assert failed.reason == "embedding_failed"
    assert failed.registered is True
    assert failed.replaced is False
    assert gate.registered is True


async def test_short_registration_fails_without_calling_embedder() -> None:
    embedder = FakeEmbedder([])
    gate = SessionSpeakerGate(
        Settings(
            _env_file=None,
            speaker_verification_enabled=True,
            speaker_register_min_audio_ms=2000,
        ),
        embedder,
    )

    result = await gate.register(audio(1500))

    assert result.success is False
    assert result.reason == "too_short"
    assert result.registered is False
    assert embedder.inputs == []


async def test_silent_registration_fails_without_replacing_previous_reference() -> None:
    embedder = FakeEmbedder([np.array([1.0, 0.0])])
    gate = SessionSpeakerGate(
        Settings(_env_file=None, speaker_verification_enabled=True), embedder
    )
    await gate.register(audio(2500))

    result = await gate.register(bytes(2500 * 16 * 2))

    assert result.success is False
    assert result.reason == "no_speech"
    assert result.registered is True
    assert result.replaced is False
    assert embedder.inputs == [audio(2500)]


async def test_short_audio_is_filtered_only_after_registration() -> None:
    gate = SessionSpeakerGate(
        Settings(
            _env_file=None,
            speaker_verification_enabled=True,
            speaker_verification_min_audio_ms=800,
        ),
        FakeEmbedder([np.array([1.0, 0.0])]),
    )

    before = await gate.evaluate(segment(milliseconds=500))
    await gate.register(audio(2500))
    after = await gate.evaluate(segment(milliseconds=500))

    assert before.allow is True
    assert before.reason == "unregistered"
    assert after.allow is False
    assert after.reason == "too_short"


async def test_gate_trims_vad_trailing_silence_before_verification() -> None:
    embedder = FakeEmbedder([np.array([1.0, 0.0]), np.array([1.0, 0.0])])
    gate = SessionSpeakerGate(
        Settings(_env_file=None, speaker_verification_enabled=True), embedder
    )
    await gate.register(audio(2500))
    speech = audio(1000)
    trailing_silence = b"\x00\x00" * (500 * 16)

    decision = await gate.evaluate(
        SpeechSegment(
            1,
            speech + trailing_silence,
            trailing_silence_samples=500 * 16,
            voiced_samples=1000 * 16,
        )
    )

    assert decision.reason == "matched"
    assert embedder.inputs == [audio(2500), speech]
