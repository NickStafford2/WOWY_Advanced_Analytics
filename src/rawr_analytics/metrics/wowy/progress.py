from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

type WowyProgressSink = Callable[["WowyProgressUpdate"], None]


@dataclass(frozen=True)
class WowyProgressUpdate:
    phase: str
    current: int
    total: int
    detail: str
    percent: int


def _percent(*, current: int, total: int) -> int:
    if total <= 0:
        return 0
    bounded_current = min(max(current, 0), total)
    return int((bounded_current / total) * 100)


def emit_wowy_progress(
    sink: WowyProgressSink | None,
    *,
    phase: str,
    current: int,
    total: int,
    detail: str,
) -> None:
    if sink is None:
        return
    sink(
        WowyProgressUpdate(
            phase=phase,
            current=current,
            total=total,
            detail=detail,
            percent=_percent(current=current, total=total),
        )
    )
