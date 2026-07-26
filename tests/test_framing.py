"""Framing tests — pure, no pi process required.

The interesting cases are the ones a naive `readline` implementation gets wrong.
"""

from __future__ import annotations

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from pi_rpc.framing import JsonlFramer, serialize_json_line  # noqa: E402


def test_splits_on_lf():
    framer = JsonlFramer()
    assert framer.feed(b'{"a":1}\n{"b":2}\n') == ['{"a":1}', '{"b":2}']


def test_partial_records_are_buffered():
    framer = JsonlFramer()
    assert framer.feed(b'{"a":') == []
    assert framer.feed(b'1}\n') == ['{"a":1}']


def test_split_mid_multibyte_character():
    """A chunk boundary inside a UTF-8 sequence must not corrupt the record."""
    payload = '{"text":"café ☕"}'.encode("utf-8") + b"\n"
    framer = JsonlFramer()
    out = framer.feed(payload[:10]) + framer.feed(payload[10:])
    assert json.loads(out[0])["text"] == "café ☕"


def test_unicode_line_separators_do_not_split():
    """U+2028 / U+2029 are legal inside JSON strings — pi splits on LF only.

    This is the exact bug pi's `jsonl.js` avoids by not using Node's readline, and
    the one Python's universal-newline text mode would reproduce.
    """
    text = "line one line two line three"
    record = serialize_json_line({"text": text})
    # The raw separators must reach the wire, or this test proves nothing.
    assert b"\xe2\x80\xa8" in record and b"\xe2\x80\xa9" in record
    framer = JsonlFramer()
    lines = framer.feed(record)
    assert len(lines) == 1, "record must not be split on U+2028/U+2029"
    assert json.loads(lines[0])["text"] == text


def test_carriage_return_is_stripped():
    framer = JsonlFramer()
    assert framer.feed(b'{"a":1}\r\n') == ['{"a":1}']


def test_embedded_escaped_newline_does_not_split():
    record = serialize_json_line({"text": "a\nb"})
    framer = JsonlFramer()
    lines = framer.feed(record)
    assert len(lines) == 1
    assert json.loads(lines[0])["text"] == "a\nb"


def test_flush_emits_trailing_partial():
    framer = JsonlFramer()
    assert framer.feed(b'{"a":1}') == []
    assert framer.flush() == ['{"a":1}']
    assert framer.flush() == []


def test_serialize_round_trip():
    record = serialize_json_line({"type": "prompt", "message": "hi", "id": "req_1"})
    assert record.endswith(b"\n")
    assert json.loads(record.decode("utf-8")) == {
        "type": "prompt",
        "message": "hi",
        "id": "req_1",
    }


if __name__ == "__main__":
    passed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
            passed += 1
    print(f"\n{passed} passed")
