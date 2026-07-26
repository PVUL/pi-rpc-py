"""pi-rpc — an async Python client for pi's headless RPC mode.

    from pi_rpc import PiRpcClient

    async with PiRpcClient(cwd="/path/to/repo") as pi:
        await pi.prompt("summarise this repo")
        await pi.wait_for_idle()
        print(await pi.get_last_assistant_text())
"""

from .client import (
    BLOCKING_UI_METHODS,
    PiCommandError,
    PiProcessError,
    PiRpcClient,
    PiRpcError,
    PiTimeoutError,
    UiRequest,
)
from .framing import JsonlFramer, serialize_json_line
from .stream import collect_text, is_run_finished, text_delta, thinking_delta, tool_start

__version__ = "0.1.0"

__all__ = [
    "PiRpcClient",
    "PiRpcError",
    "PiProcessError",
    "PiCommandError",
    "PiTimeoutError",
    "UiRequest",
    "BLOCKING_UI_METHODS",
    "JsonlFramer",
    "serialize_json_line",
    "text_delta",
    "thinking_delta",
    "tool_start",
    "is_run_finished",
    "collect_text",
    "__version__",
]
