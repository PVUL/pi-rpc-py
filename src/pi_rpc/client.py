"""Async Python client for pi's RPC mode.

pi (the coding agent) can run headless as a long-lived process speaking JSON Lines
over stdin/stdout: ``pi --mode rpc``. Commands go in, responses and session events
come out. That makes pi drivable by *any* front-end — this client is the Python
half, so a voice UI, a web app, or a script can hold a real pi session with
streaming output, mid-run steering, and abort.

Ported 1:1 from pi's own TypeScript `RpcClient` (`dist/modes/rpc/rpc-client.js`) so
behaviour matches the reference implementation, with two deliberate additions:

* **Extension UI requests are answered.** pi's TS client forwards
  ``extension_ui_request`` to event listeners and never replies, so an extension
  that calls ``ui.confirm()`` waits for a timeout that may never come. Here they're
  routed to a handler (see :meth:`PiRpcClient.set_ui_handler`) and always answered —
  defaulting to *cancelled*, which fails safe rather than hanging.
* **Async iteration.** ``async for event in client.events()`` alongside callbacks.

Stdlib only, asyncio-native.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
import logging
import os
import sys
from typing import Any, AsyncIterator, Awaitable, Callable, Sequence

from .framing import JsonlFramer, serialize_json_line

__all__ = [
    "PiRpcClient",
    "PiRpcError",
    "PiProcessError",
    "PiCommandError",
    "PiTimeoutError",
    "UiRequest",
]

log = logging.getLogger("pi_rpc")

DEFAULT_REQUEST_TIMEOUT = 30.0
DEFAULT_IDLE_TIMEOUT = 60.0

#: Extension UI methods that block an extension until answered. The rest
#: (``notify``, ``setStatus``, ``setWidget``, ``setTitle``, ``set_editor_text``)
#: are fire-and-forget presentation hints.
BLOCKING_UI_METHODS = frozenset({"select", "confirm", "input", "editor"})

Event = dict[str, Any]
EventListener = Callable[[Event], Any]
UiHandler = Callable[["UiRequest"], Awaitable[dict[str, Any] | None] | dict[str, Any] | None]


class PiRpcError(Exception):
    """Base class for every failure this client raises."""


class PiProcessError(PiRpcError):
    """The pi process died, failed to start, or its stdin went away."""


class PiCommandError(PiRpcError):
    """pi answered a command with ``success: false``."""


class PiTimeoutError(PiRpcError):
    """A command response or an idle wait exceeded its deadline."""


class UiRequest:
    """An ``extension_ui_request`` awaiting an answer.

    Reply with exactly one of :meth:`value`, :meth:`confirm`, or :meth:`cancel`.
    """

    def __init__(self, payload: Event) -> None:
        self.payload = payload
        self.id: str = payload["id"]
        self.method: str = payload["method"]

    @property
    def blocking(self) -> bool:
        return self.method in BLOCKING_UI_METHODS

    @staticmethod
    def value(text: str) -> dict[str, Any]:
        """Answer a ``select`` / ``input`` / ``editor`` request."""
        return {"value": text}

    @staticmethod
    def confirm(confirmed: bool) -> dict[str, Any]:
        """Answer a ``confirm`` request."""
        return {"confirmed": confirmed}

    @staticmethod
    def cancel() -> dict[str, Any]:
        """Decline the request."""
        return {"cancelled": True}

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<UiRequest {self.method} {self.payload.get('title')!r}>"


class PiRpcClient:
    """Spawn and drive ``pi --mode rpc``.

    Typical use::

        async with PiRpcClient(cwd="/Users/me/repos/thing") as pi:
            pi.on_event(lambda e: print(e["type"]))
            await pi.prompt("what does this repo do?")
            await pi.wait_for_idle()
            print(await pi.get_last_assistant_text())

    Args:
        pi_bin: Executable to spawn. Defaults to ``pi`` on ``PATH``.
        cli_path: Spawn ``node <cli_path>`` instead — matches how pi's own client
            starts the agent, useful against a source checkout.
        node_bin: Interpreter used with ``cli_path``.
        cwd: Working directory for pi. This matters more than it looks: pi keys
            sessions, ``AGENTS.md``/``CLAUDE.md`` discovery, and trust off the cwd.
        env: Extra environment variables, merged over ``os.environ``.
        provider / model: Forwarded as ``--provider`` / ``--model``.
        args: Extra CLI arguments (e.g. ``["--session-id", "voice-1"]``).
        request_timeout: Seconds to wait for a command response.
        forward_stderr: Mirror pi's stderr to our own, as pi's client does.
    """

    def __init__(
        self,
        *,
        pi_bin: str = "pi",
        cli_path: str | None = None,
        node_bin: str = "node",
        cwd: str | os.PathLike[str] | None = None,
        env: dict[str, str] | None = None,
        provider: str | None = None,
        model: str | None = None,
        args: Sequence[str] | None = None,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
        forward_stderr: bool = False,
    ) -> None:
        self.pi_bin = pi_bin
        self.cli_path = cli_path
        self.node_bin = node_bin
        self.cwd = os.fspath(cwd) if cwd is not None else None
        self.env = env or {}
        self.provider = provider
        self.model = model
        self.extra_args = list(args or [])
        self.request_timeout = request_timeout
        self.forward_stderr = forward_stderr

        self._proc: asyncio.subprocess.Process | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._pending: dict[str, asyncio.Future[Event]] = {}
        self._ids = itertools.count(1)
        self._listeners: list[EventListener] = []
        self._queues: list[asyncio.Queue[Event | None]] = []
        self._ui_handler: UiHandler | None = None
        self._stderr = bytearray()
        self._exit_error: PiProcessError | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _argv(self) -> list[str]:
        args = ["--mode", "rpc"]
        if self.provider:
            args += ["--provider", self.provider]
        if self.model:
            args += ["--model", self.model]
        args += self.extra_args
        if self.cli_path:
            return [self.node_bin, self.cli_path, *args]
        return [self.pi_bin, *args]

    async def start(self) -> None:
        """Spawn pi and begin reading its stream."""
        if self._proc is not None:
            raise PiRpcError("Client already started")
        self._exit_error = None

        argv = self._argv()
        log.debug("spawning: %s (cwd=%s)", " ".join(argv), self.cwd)
        try:
            self._proc = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.cwd,
                env={**os.environ, **self.env},
            )
        except FileNotFoundError as exc:
            raise PiProcessError(f"Could not spawn {argv[0]!r}: {exc}") from exc

        self._tasks = [
            asyncio.create_task(self._read_stdout(), name="pi-rpc-stdout"),
            asyncio.create_task(self._read_stderr(), name="pi-rpc-stderr"),
        ]

        # pi's own client sleeps 100ms and checks for an early exit; a bad flag or a
        # missing binary shows up here rather than as a mystifying hang later.
        await asyncio.sleep(0.1)
        if self._proc.returncode is not None:
            raise self._process_error(self._proc.returncode)

    async def stop(self) -> None:
        """Terminate pi, escalating to SIGKILL after 1s (as pi's client does)."""
        proc, self._proc = self._proc, None
        if proc is None:
            return
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()

        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()

        self._fail_pending(PiProcessError("Client stopped"))
        for queue in self._queues:
            queue.put_nowait(None)
        self._queues.clear()

    async def __aenter__(self) -> "PiRpcClient":
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    @property
    def stderr(self) -> str:
        """Everything pi has written to stderr so far (debugging aid)."""
        return self._stderr.decode("utf-8", errors="replace")

    # ------------------------------------------------------------------
    # Stream plumbing
    # ------------------------------------------------------------------

    async def _read_stdout(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        framer = JsonlFramer()
        stream = self._proc.stdout
        while True:
            chunk = await stream.read(65536)
            if not chunk:
                for line in framer.flush():
                    self._handle_line(line)
                break
            for line in framer.feed(chunk):
                self._handle_line(line)

        rc = self._proc.returncode if self._proc else None
        self._fail_pending(self._process_error(rc))
        for queue in self._queues:
            queue.put_nowait(None)

    async def _read_stderr(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        stream = self._proc.stderr
        while True:
            chunk = await stream.read(65536)
            if not chunk:
                break
            self._stderr.extend(chunk)
            if self.forward_stderr:
                sys.stderr.buffer.write(chunk)
                sys.stderr.buffer.flush()

    def _handle_line(self, line: str) -> None:
        if not line.strip():
            return
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            # pi's client ignores non-JSON lines; so do we, but leave a trace.
            log.debug("ignoring non-JSON line: %.200s", line)
            return
        if not isinstance(data, dict):
            return

        # A response to a command we're waiting on...
        if data.get("type") == "response":
            request_id = data.get("id")
            future = self._pending.pop(request_id, None) if request_id else None
            if future is not None and not future.done():
                future.set_result(data)
                return

        # ...otherwise it's an event.
        if data.get("type") == "extension_ui_request":
            asyncio.create_task(self._handle_ui_request(data))

        for listener in list(self._listeners):
            try:
                result = listener(data)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception:
                log.exception("event listener raised")
        for queue in self._queues:
            queue.put_nowait(data)

    def _process_error(self, code: int | None) -> PiProcessError:
        if self._exit_error is not None:
            return self._exit_error
        detail = self.stderr.strip()
        suffix = f" Stderr: {detail}" if detail else ""
        self._exit_error = PiProcessError(f"pi exited with code {code}.{suffix}")
        return self._exit_error

    def _fail_pending(self, error: Exception) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def on_event(self, listener: EventListener) -> Callable[[], None]:
        """Register a listener; returns a function that unregisters it."""
        self._listeners.append(listener)

        def unsubscribe() -> None:
            with contextlib.suppress(ValueError):
                self._listeners.remove(listener)

        return unsubscribe

    async def events(self) -> AsyncIterator[Event]:
        """Iterate events until the process ends: ``async for e in client.events()``."""
        queue: asyncio.Queue[Event | None] = asyncio.Queue()
        self._queues.append(queue)
        try:
            while True:
                item = await queue.get()
                if item is None:
                    return
                yield item
        finally:
            with contextlib.suppress(ValueError):
                self._queues.remove(queue)

    def set_ui_handler(self, handler: UiHandler | None) -> None:
        """Handle extension UI requests (``confirm``, ``input``, ``select``, ``editor``).

        The handler receives a :class:`UiRequest` and returns one of
        ``UiRequest.value(...)`` / ``UiRequest.confirm(...)`` / ``UiRequest.cancel()``,
        or ``None`` to cancel. Without a handler every blocking request is cancelled,
        so extensions fail fast instead of hanging.
        """
        self._ui_handler = handler

    async def _handle_ui_request(self, payload: Event) -> None:
        request = UiRequest(payload)
        if not request.blocking:
            return  # notify / setStatus / setWidget / setTitle — nothing to answer

        answer: dict[str, Any] | None = None
        if self._ui_handler is not None:
            try:
                result = self._ui_handler(request)
                answer = await result if asyncio.iscoroutine(result) else result
            except Exception:
                log.exception("UI handler raised; cancelling request")
                answer = None
        if answer is None:
            answer = UiRequest.cancel()

        with contextlib.suppress(PiRpcError):
            await self._write({"type": "extension_ui_response", "id": request.id, **answer})

    # ------------------------------------------------------------------
    # Transport
    # ------------------------------------------------------------------

    async def _write(self, payload: dict[str, Any]) -> None:
        proc = self._proc
        if proc is None or proc.stdin is None:
            raise PiProcessError("Client not started")
        if self._exit_error is not None:
            raise self._exit_error
        if proc.returncode is not None:
            raise self._process_error(proc.returncode)
        try:
            proc.stdin.write(serialize_json_line(payload))
            await proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError) as exc:
            raise PiProcessError(f"pi stdin closed: {exc}. Stderr: {self.stderr}") from exc

    async def _call(self, command: dict[str, Any], *, timeout: float | None = None) -> Any:
        """Send a command, await its response, return its ``data`` (or ``None``)."""
        request_id = f"req_{next(self._ids)}"
        future: asyncio.Future[Event] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future

        payload = {k: v for k, v in command.items() if v is not None}
        payload["id"] = request_id
        try:
            await self._write(payload)
            response = await asyncio.wait_for(
                future, timeout=self.request_timeout if timeout is None else timeout
            )
        except asyncio.TimeoutError as exc:
            self._pending.pop(request_id, None)
            raise PiTimeoutError(
                f"Timed out waiting for response to {command['type']!r}. Stderr: {self.stderr}"
            ) from exc
        except BaseException:
            self._pending.pop(request_id, None)
            raise

        if not response.get("success", False):
            raise PiCommandError(response.get("error") or f"{command['type']} failed")
        return response.get("data")

    # ------------------------------------------------------------------
    # Commands — conversation
    # ------------------------------------------------------------------

    async def prompt(self, message: str, images: list[dict[str, Any]] | None = None) -> None:
        """Send a prompt. Returns as soon as pi accepts it — watch events for output."""
        await self._call({"type": "prompt", "message": message, "images": images})

    async def steer(self, message: str, images: list[dict[str, Any]] | None = None) -> None:
        """Interrupt an in-flight run with a new instruction (barge-in)."""
        await self._call({"type": "steer", "message": message, "images": images})

    async def follow_up(self, message: str, images: list[dict[str, Any]] | None = None) -> None:
        """Queue a message to be handled once the current run finishes."""
        await self._call({"type": "follow_up", "message": message, "images": images})

    async def abort(self) -> None:
        """Stop the current run."""
        await self._call({"type": "abort"})

    # ------------------------------------------------------------------
    # Commands — session
    # ------------------------------------------------------------------

    async def new_session(self, parent_session: str | None = None) -> dict[str, Any]:
        return await self._call({"type": "new_session", "parentSession": parent_session})

    async def get_state(self) -> dict[str, Any]:
        return await self._call({"type": "get_state"})

    async def switch_session(self, session_path: str) -> dict[str, Any]:
        return await self._call({"type": "switch_session", "sessionPath": session_path})

    async def fork(self, entry_id: str) -> dict[str, Any]:
        return await self._call({"type": "fork", "entryId": entry_id})

    async def clone(self) -> dict[str, Any]:
        return await self._call({"type": "clone"})

    async def get_entries(self, since: str | None = None) -> dict[str, Any]:
        return await self._call({"type": "get_entries", "since": since})

    async def get_tree(self) -> dict[str, Any]:
        return await self._call({"type": "get_tree"})

    async def get_messages(self) -> list[Any]:
        data = await self._call({"type": "get_messages"})
        return data["messages"]

    async def get_fork_messages(self) -> list[Any]:
        data = await self._call({"type": "get_fork_messages"})
        return data["messages"]

    async def get_last_assistant_text(self) -> str | None:
        data = await self._call({"type": "get_last_assistant_text"})
        return data["text"]

    async def set_session_name(self, name: str) -> None:
        await self._call({"type": "set_session_name", "name": name})

    async def get_session_stats(self) -> dict[str, Any]:
        return await self._call({"type": "get_session_stats"})

    async def export_html(self, output_path: str | None = None) -> dict[str, Any]:
        return await self._call({"type": "export_html", "outputPath": output_path})

    # ------------------------------------------------------------------
    # Commands — model & behaviour
    # ------------------------------------------------------------------

    async def set_model(self, provider: str, model_id: str) -> dict[str, Any]:
        return await self._call({"type": "set_model", "provider": provider, "modelId": model_id})

    async def cycle_model(self) -> dict[str, Any] | None:
        return await self._call({"type": "cycle_model"})

    async def get_available_models(self) -> list[dict[str, Any]]:
        data = await self._call({"type": "get_available_models"})
        return data["models"]

    async def set_thinking_level(self, level: str) -> None:
        await self._call({"type": "set_thinking_level", "level": level})

    async def cycle_thinking_level(self) -> dict[str, Any] | None:
        return await self._call({"type": "cycle_thinking_level"})

    async def get_available_thinking_levels(self) -> list[str]:
        data = await self._call({"type": "get_available_thinking_levels"})
        return data["levels"]

    async def set_steering_mode(self, mode: str) -> None:
        """``"all"`` or ``"one-at-a-time"``."""
        await self._call({"type": "set_steering_mode", "mode": mode})

    async def set_follow_up_mode(self, mode: str) -> None:
        """``"all"`` or ``"one-at-a-time"``."""
        await self._call({"type": "set_follow_up_mode", "mode": mode})

    async def compact(self, custom_instructions: str | None = None) -> dict[str, Any]:
        return await self._call(
            {"type": "compact", "customInstructions": custom_instructions},
            timeout=max(self.request_timeout, 120.0),
        )

    async def set_auto_compaction(self, enabled: bool) -> None:
        await self._call({"type": "set_auto_compaction", "enabled": enabled})

    async def set_auto_retry(self, enabled: bool) -> None:
        await self._call({"type": "set_auto_retry", "enabled": enabled})

    async def abort_retry(self) -> None:
        await self._call({"type": "abort_retry"})

    async def get_commands(self) -> list[dict[str, Any]]:
        data = await self._call({"type": "get_commands"})
        return data["commands"]

    # ------------------------------------------------------------------
    # Commands — bash
    # ------------------------------------------------------------------

    async def bash(self, command: str, exclude_from_context: bool = False) -> dict[str, Any]:
        return await self._call(
            {
                "type": "bash",
                "command": command,
                "excludeFromContext": exclude_from_context or None,
            },
            timeout=max(self.request_timeout, 120.0),
        )

    async def abort_bash(self) -> None:
        await self._call({"type": "abort_bash"})

    # ------------------------------------------------------------------
    # Waiting helpers
    # ------------------------------------------------------------------

    async def wait_for_idle(self, timeout: float = DEFAULT_IDLE_TIMEOUT) -> None:
        """Block until pi emits ``agent_settled``."""
        await self._wait_settled(timeout, collect=False)

    async def collect_events(self, timeout: float = DEFAULT_IDLE_TIMEOUT) -> list[Event]:
        """Gather every event up to and including ``agent_settled``."""
        return await self._wait_settled(timeout, collect=True)

    async def prompt_and_wait(
        self,
        message: str,
        images: list[dict[str, Any]] | None = None,
        timeout: float = DEFAULT_IDLE_TIMEOUT,
    ) -> list[Event]:
        """Prompt and return every event of the resulting run."""
        task = asyncio.create_task(self.collect_events(timeout))
        await asyncio.sleep(0)  # let the collector subscribe before we prompt
        await self.prompt(message, images)
        return await task

    async def _wait_settled(self, timeout: float, *, collect: bool) -> Any:
        loop = asyncio.get_running_loop()
        done: asyncio.Future[None] = loop.create_future()
        collected: list[Event] = []

        def listener(event: Event) -> None:
            if collect:
                collected.append(event)
            if event.get("type") == "agent_settled" and not done.done():
                done.set_result(None)

        unsubscribe = self.on_event(listener)
        try:
            await asyncio.wait_for(done, timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise PiTimeoutError(
                f"Timed out after {timeout}s waiting for the agent to settle. "
                f"Stderr: {self.stderr}"
            ) from exc
        finally:
            unsubscribe()
        return collected if collect else None
