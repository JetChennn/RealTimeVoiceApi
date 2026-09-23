import pytest
from pydantic import ValidationError

from realtime_voice.protocol.client_messages import CreateSession

CREATE = {
    "type": "CREATE_SESSION",
    "protocol_version": 1,
    "device_id": "u",
    "session_id": "s",
    "audio_format": "PCM16",
    "audio_transport": "BASE64_JSON",
    "sample_rate": 16000,
    "channels": 1,
}


@pytest.mark.parametrize(
    "fields",
    [
        {"scenes": [""]},
        {"scenes": ["a", "b", "c", "d"]},
        {"scenes": "a"},
        {"scenes": [1]},
        {"rag_enabled": "true"},
        {"rag_enabled": 1},
    ],
)
def test_invalid_rag_configuration(fields):
    with pytest.raises(ValidationError):
        CreateSession(**CREATE, **fields)


def test_rag_defaults_and_scene_normalization():
    default = CreateSession(**CREATE)
    assert default.rag_enabled is False
    assert default.scenes == []

    general = CreateSession(**CREATE, rag_enabled=True)
    assert general.rag_enabled is True
    assert general.scenes == []

    scoped = CreateSession(
        **CREATE,
        rag_enabled=True,
        scenes=[" a ", "a", "b", "b"],
    )
    assert scoped.scenes == ["a", "b"]
