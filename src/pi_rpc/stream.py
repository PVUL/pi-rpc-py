"""Helpers for reading pi's streaming events.

The RPC stream is fine-grained: a single run emits ``agent_start``, then per-turn
``turn_start`` / ``message_start`` / many ``message_update`` / ``message_end`` /
``turn_end``, interleaved with ``tool_execution_*``, and finally ``agent_end`` and
``agent_settled``.

The text actually worth *speaking* lives inside ``message_update`` events, whose
``assistantMessageEvent.type`` is one of::

    text_start   text_delta   text_end          <- the reply; speak this
    thinking_start thinking_delta thinking_end  <- reasoning; do NOT speak
    toolcall_start toolcall_delta toolcall_end  <- tool arguments; do NOT speak

These helpers exist so front-ends don't each re-derive that distinction — speaking
thinking or tool-call JSON aloud is the obvious failure mode.
"""

from __future__ import annotations

from typing import Any, Iterable

__all__ = [
    "TEXT_EVENTS",
    "THINKING_EVENTS",
    "TOOLCALL_EVENTS",
    "text_delta",
    "thinking_delta",
    "tool_start",
    "is_run_finished",
    "collect_text",
]

TEXT_EVENTS = frozenset({"text_start", "text_delta", "text_end"})
THINKING_EVENTS = frozenset({"thinking_start", "thinking_delta", "thinking_end"})
TOOLCALL_EVENTS = frozenset({"toolcall_start", "toolcall_delta", "toolcall_end"})

Event = dict[str, Any]


def _sub(event: Event) -> dict[str, Any] | None:
    if event.get("type") != "message_update":
        return None
    sub = event.get("assistantMessageEvent")
    return sub if isinstance(sub, dict) else None


def text_delta(event: Event) -> str | None:
    """The next chunk of speakable assistant text, or ``None``.

    Only ``text_delta`` is consulted. ``text_start`` appears to carry the opening of
    the reply in its ``partial`` content, but that text is *repeated* by the first
    ``text_delta`` — reading both duplicates the first token of every message.
    """
    sub = _sub(event)
    if sub is None or sub.get("type") != "text_delta":
        return None
    delta = sub.get("delta")
    return delta if isinstance(delta, str) else None


def thinking_delta(event: Event) -> str | None:
    """The next chunk of reasoning text — useful to display, never to speak."""
    sub = _sub(event)
    if sub is None or sub.get("type") != "thinking_delta":
        return None
    delta = sub.get("delta")
    return delta if isinstance(delta, str) else None


def tool_start(event: Event) -> tuple[str, dict[str, Any]] | None:
    """``(tool_name, args)`` when a tool begins running — good narration material."""
    if event.get("type") != "tool_execution_start":
        return None
    name = event.get("toolName")
    if not isinstance(name, str):
        return None
    args = event.get("args")
    return name, args if isinstance(args, dict) else {}


def is_run_finished(event: Event) -> bool:
    """True for ``agent_settled``: pi has finished and is idle."""
    return event.get("type") == "agent_settled"


def collect_text(events: Iterable[Event]) -> str:
    """Reassemble the full assistant reply from a run's events."""
    return "".join(filter(None, (text_delta(e) for e in events)))
