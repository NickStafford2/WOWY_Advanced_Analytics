from __future__ import annotations

from rawr_analytics.basketball.nba_api.cache import load_or_fetch_box_score_cache
from rawr_analytics.basketball.nba_api.models import SourceBoxScore
from rawr_analytics.basketball.nba_api.parsers import parse_box_score_payload
from rawr_analytics.shared.common import LogFn


def load_or_fetch_box_score(
    game_id: str,
    log_fn: LogFn | None = print,
) -> tuple[SourceBoxScore, str]:
    box_score_payload, box_score_source = load_or_fetch_box_score_cache(game_id, log_fn)

    box_score = parse_box_score_payload(
        box_score_payload,
        game_id=game_id,
    )
    return box_score, box_score_source
