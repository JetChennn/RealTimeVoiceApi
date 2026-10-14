from copy import deepcopy

import pytest

from realtime_voice.session.actor import RecordFilteredInput, SessionActor, StartThinker
from realtime_voice.session.events import ThinkerDeltaReceived, UserInputCommitted
from realtime_voice.session.input_filter import FillerFilter
from realtime_voice.session.state import SessionState
from tests.helpers import valid_wav
from tests.unit.session.conftest import (
    actor_for_test,
    actor_with_tts_turn,
    outbound_of_type,
    recognize,
)


@pytest.mark.parametrize(
    ("text", "count"),
    [
        ("嗯", 1),
        ("哦", 1),
        ("噢", 1),
        ("喔", 1),
        ("啊", 1),
        ("呃", 1),
        ("额", 1),
        ("嗯嗯嗯嗯嗯嗯", 6),
        (" 嗯，哦。噢！喔、啊；呃……\n", 6),
        ("\u3000额\t啊\n", 2),
    ],
)
def test_pure_fillers_are_silent_and_consume_pending_asr(text, count):
    actor = actor_for_test()
    assert recognize(actor, 41, text) == [RecordFilteredInput(count)]
    assert actor.state.turns == {}
    assert actor.state.next_turn_id == 1
    assert actor.state.pending_asr_segment_ids == set()
    assert not actor.state.llm_queue
    effects = recognize(actor, 42, "你好")
    assert outbound_of_type(effects, "ASR_RESULT").turn_id == 1
    assert any(isinstance(effect, StartThinker) for effect in effects)


@pytest.mark.parametrize(
    "text",
    [
        "嗯？",
        "哦?",
        "？啊",
        "啊啊啊啊啊啊啊?",
        "嗯嗯嗯嗯嗯嗯嗯",
        "嗯，我想去杭州。",
        "好的",
        "好",
        "不",
        "是",
        "停",
        "嗯1",
        "哦OK",
        "啊🙂",
        "嗯+",
        "呜",
        "唔",
    ],
)
def test_meaningful_or_question_input_is_preserved_verbatim(text):
    actor = actor_for_test()
    effects = recognize(actor, 1, text)
    assert outbound_of_type(effects, "ASR_RESULT").text == text
    assert actor.state.turns[1].asr_text == text


@pytest.mark.parametrize("text", ["", " \n\t", "……，。！"])
def test_empty_normalized_text_is_not_a_filler_match(text):
    assert FillerFilter().filtered_char_count(text) is None


@pytest.mark.parametrize("stage", ["llm", "tts", "bistream"])
@pytest.mark.parametrize("semantic", [False, True])
def test_filter_does_not_change_active_generation_or_playback(stage, semantic):
    if stage == "tts":
        actor = actor_with_tts_turn()
    else:
        actor = actor_for_test(bistream_enabled=stage == "bistream")
        recognize(actor, 1, "旧问题")
        actor.handle(ThinkerDeltaReceived("s", 1, 1, "回答"))
    before = deepcopy(actor.state)
    if semantic:
        effects = actor.handle(UserInputCommitted("s", "嗯啊……", valid_wav(), 20, "semantic_end"))
    else:
        effects = recognize(actor, 2, "嗯啊……")
        # ASR 段号按既有规则消耗；所有轮次相关状态保持不变。
        before.registered_asr_segment_ids.add(2)
    assert effects == [RecordFilteredInput(2)]
    assert actor.state == before


def test_disabled_filter_preserves_filler_text():
    actor = SessionActor(SessionState("u", "s", 16000), filler_filter=FillerFilter(enabled=False))
    assert outbound_of_type(recognize(actor, 1, "嗯"), "ASR_RESULT").text == "嗯"


def test_character_set_and_limit_are_configurable():
    policy = FillerFilter(chars="嗯", max_chars=2)
    assert policy.filtered_char_count("嗯嗯") == 2
    assert policy.filtered_char_count("嗯嗯嗯") is None
    assert policy.filtered_char_count("哦") is None
