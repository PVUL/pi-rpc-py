"""Event-reading tests, built from payloads captured off a real pi run."""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from pi_rpc import collect_text, is_run_finished, text_delta, thinking_delta, tool_start  # noqa: E402


def msg(sub: dict) -> dict:
    return {"type": "message_update", "assistantMessageEvent": sub}


def test_text_start_is_not_emitted_twice():
    """pi repeats text_start's partial content in the first text_delta.

    Reading both makes every reply open with a stuttered token ("ThisThis repo…"),
    which is glaring in speech. Shapes below are copied from a real run.
    """
    start = msg({"type": "text_start", "contentIndex": 0,
                 "partial": {"content": [{"index": 0, "type": "text", "text": "I"}]}})
    first = msg({"type": "text_delta", "contentIndex": 0, "delta": "I"})
    second = msg({"type": "text_delta", "contentIndex": 0, "delta": "'ll look."})

    assert text_delta(start) is None
    assert collect_text([start, first, second]) == "I'll look."


def test_thinking_and_toolcall_are_not_speakable():
    assert text_delta(msg({"type": "thinking_delta", "delta": "hmm"})) is None
    assert text_delta(msg({"type": "toolcall_delta", "delta": '{"cmd":'})) is None
    assert thinking_delta(msg({"type": "thinking_delta", "delta": "hmm"})) == "hmm"


def test_tool_start_is_narratable():
    event = {"type": "tool_execution_start", "toolCallId": "t1",
             "toolName": "bash", "args": {"command": "ls -la"}}
    assert tool_start(event) == ("bash", {"command": "ls -la"})
    assert tool_start({"type": "tool_execution_end"}) is None


def test_run_finished_is_agent_settled():
    assert is_run_finished({"type": "agent_settled"})
    assert not is_run_finished({"type": "agent_end"})


def test_non_message_events_are_ignored():
    for event in ({"type": "turn_start"}, {"type": "queue_update"}, {}):
        assert text_delta(event) is None
        assert thinking_delta(event) is None


if __name__ == "__main__":
    passed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
            passed += 1
    print(f"\n{passed} passed")
