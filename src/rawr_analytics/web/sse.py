from __future__ import annotations

import json


def format_sse_event(
    *,
    event: str,
    data: object,
    event_id: str | None = None,
) -> str:
    payload = json.dumps(data, separators=(",", ":"))
    lines: list[str] = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    for line in payload.splitlines() or [payload]:
        lines.append(f"data: {line}")
    lines.append("")
    return "\n".join(lines) + "\n"


def format_sse_comment(comment: str) -> str:
    return f": {comment}\n\n"


def sse_headers() -> dict[str, str]:
    return {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
