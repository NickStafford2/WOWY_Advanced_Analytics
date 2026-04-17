from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.shared.season import Season

type RawrProgressSink = Callable[["RawrProgressUpdate"], None]


@dataclass(frozen=True)
class RawrProgressUpdate:
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


def emit_rawr_progress(
    sink: RawrProgressSink | None,
    *,
    phase: str,
    current: int,
    total: int,
    detail: str,
) -> None:
    if sink is None:
        return
    sink(
        RawrProgressUpdate(
            phase=phase,
            current=current,
            total=total,
            detail=detail,
            percent=_percent(current=current, total=total),
        )
    )


def emit_rawr_season_progress(
    sink: RawrProgressSink | None,
    *,
    phase: str,
    current: int,
    total: int,
    season: Season,
) -> None:
    emit_rawr_progress(
        sink,
        phase=phase,
        current=current,
        total=total,
        detail=season.year_string_nba_api,
    )
