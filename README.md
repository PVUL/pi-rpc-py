# pi-rpc

An async Python client for the [pi coding agent](https://www.npmjs.com/package/@earendil-works/pi-coding-agent)'s
headless RPC mode.

pi can run as a long-lived process speaking JSON Lines over stdin/stdout
(`pi --mode rpc`). That makes it drivable by any front-end — a voice interface, a
web app, a bot, a test harness — while remaining *actually pi*: your extensions,
skills, model providers, context files, and session history all still apply,
because you are running pi rather than reimplementing it.

pi ships a TypeScript `RpcClient` in the box. This is the Python half.

- **Zero runtime dependencies** — stdlib only.
- **asyncio-native**, so it drops into async frameworks.
- **Ported 1:1** from pi's own `dist/modes/rpc/rpc-client.js`, so semantics match.

```python
import asyncio
from pi_rpc import PiRpcClient, text_delta

async def main():
    async with PiRpcClient(cwd="/path/to/repo") as pi:
        pi.on_event(lambda e: print(text_delta(e) or "", end="", flush=True))
        await pi.prompt("What does this project do?")
        await pi.wait_for_idle(timeout=180)

asyncio.run(main())
```

## Install

```bash
pip install -e .
```

Requires Python 3.10+ and `pi` on your `PATH`.

## What you get

Conversation: `prompt`, `steer`, `follow_up`, `abort`.
Session: `get_state`, `new_session`, `switch_session`, `fork`, `clone`, `get_entries`,
`get_messages`, `get_last_assistant_text`, `set_session_name`, `get_session_stats`,
`export_html`.
Model & behaviour: `set_model`, `cycle_model`, `get_available_models`,
`set_thinking_level`, `cycle_thinking_level`, `set_steering_mode`, `set_follow_up_mode`,
`compact`, `set_auto_compaction`, `set_auto_retry`, `get_commands`.
Shell: `bash`, `abort_bash`.
Waiting: `wait_for_idle`, `collect_events`, `prompt_and_wait`.

### Two deliberate differences from pi's TypeScript client

**Extension UI requests are answered.** pi's TS client forwards
`extension_ui_request` to event listeners and never replies, so an extension calling
`ui.confirm()` blocks until it times out. Here they're routed to a handler and
*always* answered — defaulting to cancelled, which fails safe:

```python
def handle(req):
    if req.method == "confirm":
        return req.confirm(ask_the_user_somehow(req.payload["message"]))
    return req.cancel()

pi.set_ui_handler(handle)
```

**Async iteration** is available alongside callbacks: `async for event in pi.events()`.

## Reading the stream

A run emits `agent_start`, then per turn `turn_start` / `message_start` / many
`message_update` / `message_end` / `turn_end`, interleaved with `tool_execution_*`,
ending with `agent_end` and `agent_settled`.

The text worth *speaking or displaying* lives inside `message_update`, whose
`assistantMessageEvent.type` distinguishes three interleaved streams:

| sub-type | meaning | show it? |
|---|---|---|
| `text_start` / `text_delta` / `text_end` | the reply | **yes** |
| `thinking_start` / `thinking_delta` / `thinking_end` | reasoning | usually not |
| `toolcall_start` / `toolcall_delta` / `toolcall_end` | tool arguments | no |

`pi_rpc.stream` provides `text_delta()`, `thinking_delta()`, `tool_start()`,
`is_run_finished()`, and `collect_text()` so front-ends don't re-derive this.
Speaking thinking tokens or tool-call JSON aloud is the obvious failure mode.

## Measured behaviour

From `examples/probe.py` against `claude-opus-5`, thinking `xhigh`, on a task with
10 tool calls (your numbers will differ, the *shape* is the point):

| | |
|---|---|
| time to first event | ~0.0s |
| time to first speakable word | **3.2s** |
| first tool call | 4.7s |
| total run | **48s** |
| events in the run | ~300 |

**`abort` stops a run in ~0.33s.**

**`steer` redirects but does not stop.** Steering at t=5.2s during a tool loop: the
message was accepted immediately, pi kept calling tools until t=29.8s, and settled at
t=53s — with a final answer that *did* honour the steer.

> **If you are building a voice or interactive UI:** interrupt with **`abort()` then
> `prompt()`**, not `steer()`. Use `steer()` to add context to a run you want to
> continue ("also check the tests"), not to cut one off. And speak incrementally from
> `text_delta` — waiting for `agent_settled` meant 48 seconds of silence here.

## Probe

`examples/probe.py` measures all of the above against your own setup and dumps every
raw event to JSONL for inspection:

```bash
python examples/probe.py --cwd /path/to/repo --events /tmp/pi-events.jsonl
python examples/probe.py --only steer --only abort --delay 5
```

It disables pi's `edit`/`write` tools unless you pass `--allow-writes`, and cancels
every extension UI request — a probe should not authorise anything on its own.

## Tests

```bash
python tests/test_framing.py      # no pi process needed
# or: pytest
```

The framing tests are the ones that matter. pi frames records as **LF-only** JSON
Lines and is deliberate about it: its own reader avoids Node's `readline` because
that splits on U+2028/U+2029, which are legal inside JSON strings. Python's
universal-newline text mode has the same flaw, so this client reads bytes and splits
on `\n` alone.

## License

MIT
