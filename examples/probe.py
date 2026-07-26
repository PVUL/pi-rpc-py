#!/usr/bin/env python3
"""Probe a real pi over RPC and report what a voice front-end needs to know.

This exists to retire risk before any audio stack is written. It answers, with
measurements rather than guesses:

1. **Streaming** — which events arrive, and *when*. Time-to-first-event and
   time-to-first-assistant-text decide whether a voice UI can start speaking early
   or has to fill silence.
2. **Steering** — does ``steer`` actually redirect a run already in flight? That's
   barge-in, the whole reason this design beats a terminal extension.
3. **Abort** — does ``abort`` stop a run promptly? (In the pi-voice TUI extension,
   Escape did not.)

Every raw event is written to a JSONL file so the event *shapes* can be inspected
afterwards — the client deliberately does not guess at how to extract assistant text
until we've seen real payloads.

Usage:
    python examples/probe.py --cwd /path/to/repo
    python examples/probe.py --only stream --events /tmp/pi-events.jsonl
"""

from __future__ import annotations

import argparse
import asyncio
import json
import pathlib
import sys
import time
from collections import Counter
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from pi_rpc import PiRpcClient, UiRequest, text_delta, tool_start  # noqa: E402

# A prompt that is slow enough to interrupt and that exercises tool use.
SLOW_PROMPT = (
    "List the files in the current directory, then write a detailed paragraph "
    "about what this project appears to do."
)


class Recorder:
    """Timestamp every event, tally the types, and tee them to a file."""

    def __init__(self, path: pathlib.Path | None) -> None:
        self.path = path
        self.handle = path.open("w", encoding="utf-8") if path else None
        self.counts: Counter[str] = Counter()
        self.timeline: list[tuple[float, str]] = []
        self.first_text: float | None = None
        self.first_tool: float | None = None
        self.t0 = time.monotonic()

    def reset(self) -> None:
        self.t0 = time.monotonic()
        self.timeline.clear()
        self.first_text = None
        self.first_tool = None

    def __call__(self, event: dict[str, Any]) -> None:
        elapsed = time.monotonic() - self.t0
        etype = str(event.get("type", "?"))
        self.counts[etype] += 1
        self.timeline.append((elapsed, etype))
        if text_delta(event) is not None and self.first_text is None:
            self.first_text = elapsed
        if tool_start(event) is not None and self.first_tool is None:
            self.first_tool = elapsed
        if self.handle:
            self.handle.write(json.dumps({"t": round(elapsed, 3), **event}) + "\n")
            self.handle.flush()

    def first(self, *types: str) -> float | None:
        for elapsed, etype in self.timeline:
            if etype in types:
                return elapsed
        return None

    def close(self) -> None:
        if self.handle:
            self.handle.close()


def banner(title: str) -> None:
    print(f"\n\033[1m{'─' * 3} {title} {'─' * max(0, 56 - len(title))}\033[0m")


async def probe_stream(pi: PiRpcClient, rec: Recorder) -> None:
    banner("1. streaming + latency")
    rec.reset()
    started = time.monotonic()
    await pi.prompt(SLOW_PROMPT)
    try:
        await pi.wait_for_idle(timeout=180)
    except Exception as exc:
        print(f"  ! did not settle: {exc}")
    total = time.monotonic() - started

    print(f"  time to first event      : {_fmt(rec.timeline[0][0] if rec.timeline else None)}")
    print(f"  time to first spoken word: {_fmt(rec.first_text)}")
    print(f"  time to first tool call  : {_fmt(rec.first_tool)}")
    print(f"  total run                : {total:.2f}s")
    print(f"  events                   : {sum(rec.counts.values())}")
    for etype, n in rec.counts.most_common():
        print(f"      {etype:<34} {n}")
    if rec.first_text is not None:
        print(
            f"  \033[33m→ speaking on stream starts at {rec.first_text:.1f}s; "
            f"waiting for completion means {total:.1f}s of silence.\033[0m"
        )


async def probe_steer(pi: PiRpcClient, rec: Recorder, delay: float) -> None:
    banner(f"2. steer (barge-in) after {delay}s")
    rec.reset()
    await pi.prompt(SLOW_PROMPT)
    await asyncio.sleep(delay)

    state = await pi.get_state()
    print(f"  streaming when we steered: {state.get('isStreaming')}")
    sent = time.monotonic()
    await pi.steer("Stop that. Instead, just reply with the single word: PINEAPPLE.")
    try:
        await pi.wait_for_idle(timeout=180)
    except Exception as exc:
        print(f"  ! did not settle: {exc}")
    print(f"  settled after steer      : {time.monotonic() - sent:.2f}s")

    text = (await pi.get_last_assistant_text()) or ""
    hit = "PINEAPPLE" in text.upper()
    print(f"  redirected               : {_yn(hit)}")
    print(f"  last assistant text      : {text.strip()[:160]!r}")


async def probe_abort(pi: PiRpcClient, rec: Recorder, delay: float) -> None:
    banner(f"3. abort after {delay}s")
    rec.reset()
    await pi.prompt(SLOW_PROMPT)
    await asyncio.sleep(delay)

    before = await pi.get_state()
    print(f"  streaming before abort   : {before.get('isStreaming')}")
    sent = time.monotonic()
    await pi.abort()

    stopped_at: float | None = None
    for _ in range(100):
        await asyncio.sleep(0.1)
        if not (await pi.get_state()).get("isStreaming"):
            stopped_at = time.monotonic() - sent
            break
    print(f"  stopped                  : {_yn(stopped_at is not None)}")
    if stopped_at is not None:
        print(f"  time to stop             : {stopped_at:.2f}s")


def _fmt(value: float | None) -> str:
    return f"{value:.2f}s" if value is not None else "—"


def _yn(value: bool) -> str:
    return "\033[32myes\033[0m" if value else "\033[31mno\033[0m"


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cwd", default=".", help="working directory for pi")
    parser.add_argument("--pi-bin", default="pi")
    parser.add_argument("--provider")
    parser.add_argument("--model")
    parser.add_argument("--delay", type=float, default=4.0, help="seconds before steer/abort")
    parser.add_argument("--events", type=pathlib.Path, help="write raw events here")
    parser.add_argument(
        "--only",
        choices=["stream", "steer", "abort"],
        action="append",
        help="run only these probes (repeatable)",
    )
    parser.add_argument("--stderr", action="store_true", help="mirror pi's stderr")
    parser.add_argument(
        "--allow-writes",
        action="store_true",
        help="do not disable pi's edit/write tools (off by default)",
    )
    opts = parser.parse_args()

    which = set(opts.only or ["stream", "steer", "abort"])
    rec = Recorder(opts.events)

    print(f"pi     : {opts.pi_bin}")
    print(f"cwd    : {pathlib.Path(opts.cwd).resolve()}")
    if opts.events:
        print(f"events : {opts.events}")

    # A probe must not be able to modify anything: pi's own tool guard may well be
    # off, so we disable the mutating tools at the CLI instead of trusting config.
    guard = [] if opts.allow_writes else ["--exclude-tools", "edit,write"]
    client = PiRpcClient(
        pi_bin=opts.pi_bin,
        cwd=opts.cwd,
        provider=opts.provider,
        model=opts.model,
        args=guard,
        forward_stderr=opts.stderr,
    )
    # Any extension asking for confirmation gets a "no" — a probe should never
    # authorise anything on its own.
    client.set_ui_handler(lambda req: print(f"  [ui] {req} → cancelled") or UiRequest.cancel())

    try:
        await client.start()
    except Exception as exc:
        print(f"\n\033[31mFailed to start pi:\033[0m {exc}")
        return 1

    client.on_event(rec)

    try:
        state = await client.get_state()
        model = state.get("model") or {}
        print(f"model  : {model.get('id') or model.get('name') or '?'}")
        print(f"session: {state.get('sessionId')}  ({state.get('sessionFile')})")

        if "stream" in which:
            await probe_stream(client, rec)
        if "steer" in which:
            await probe_steer(client, rec, opts.delay)
        if "abort" in which:
            await probe_abort(client, rec, opts.delay)
    finally:
        await client.stop()
        rec.close()

    banner("verdict")
    print("  Feed these numbers into the LiveKit design decision:")
    print("  · long gap before first output  → narrate tool calls, don't sit silent")
    print("  · steer redirects mid-run       → real barge-in is available")
    print("  · abort stops promptly          → 'stop talking' can also stop thinking")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
