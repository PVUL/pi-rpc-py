"""Strict JSONL framing for the pi RPC protocol.

pi frames its RPC stream as **LF-only** JSON Lines, and is deliberate about it: its
own reader (`dist/modes/rpc/jsonl.js`) avoids Node's `readline` because readline
splits on additional Unicode separators — U+2028 (LINE SEPARATOR) and U+2029
(PARAGRAPH SEPARATOR) — which are perfectly legal *inside* a JSON string. Splitting
on those corrupts records.

Python has the same trap: text-mode iteration applies universal newlines and will
split on ``\\r`` and ``\\u2028`` too (the latter when reading `str`). So we read
**bytes** and split on ``b"\\n"`` alone, mirroring pi exactly. A trailing ``\\r`` is
stripped per record so CRLF-ish producers still work.

`JsonlFramer` is pure and synchronous, which keeps it trivially unit-testable
without spawning a subprocess.
"""

from __future__ import annotations

__all__ = ["JsonlFramer", "serialize_json_line"]


def serialize_json_line(value: object) -> bytes:
    """Serialize one record the way pi's `serializeJsonLine` does.

    `ensure_ascii=False` keeps the payload compact and UTF-8 clean; the separators
    match `JSON.stringify`'s lack of padding so the wire format is byte-comparable.
    """
    import json

    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"


class JsonlFramer:
    """Accumulate byte chunks and emit complete LF-delimited records."""

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, chunk: bytes) -> list[str]:
        """Add a chunk; return whatever complete records it completed."""
        self._buf.extend(chunk)
        lines: list[str] = []
        while True:
            idx = self._buf.find(b"\n")
            if idx == -1:
                return lines
            raw = bytes(self._buf[:idx])
            del self._buf[: idx + 1]
            lines.append(self._decode(raw))

    def flush(self) -> list[str]:
        """Emit any trailing partial record at end-of-stream (pi does this too)."""
        if not self._buf:
            return []
        raw = bytes(self._buf)
        self._buf.clear()
        return [self._decode(raw)]

    @staticmethod
    def _decode(raw: bytes) -> str:
        text = raw.decode("utf-8", errors="replace")
        return text[:-1] if text.endswith("\r") else text
