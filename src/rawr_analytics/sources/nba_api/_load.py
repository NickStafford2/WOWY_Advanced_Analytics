from __future__ import annotations

from collections.abc import Callable

from rawr_analytics.sources.nba_api.cache import (
    DEFAULT_NBA_API_DATA_DIR,
    box_score_payload_is_empty,
    load_cached_payload,
)
from rawr_analytics.sources.nba_api.parsers import parse_box_score_payload


def load_player_names_from_cache(
    log: Callable[[str], None] | None = None,
) -> dict[int, str]:
    player_names: dict[int, str] = {}
    for cache_path in sorted((DEFAULT_NBA_API_DATA_DIR / "boxscores").glob("*.json")):
        payload = load_cached_payload(
            cache_path,
            validator=lambda cached_payload: not box_score_payload_is_empty(cached_payload),
            log_fn=log,
        )
        if payload is None:
            continue
        game_id = cache_path.stem.split("_", maxsplit=1)[0]
        parsed_box_score = parse_box_score_payload(payload, game_id=game_id)
        for player in parsed_box_score.players:
            if player.player is None:
                continue
            player_name = player.player.player_name.strip()
            if player_name == "":
                continue
            player_names[player.player.player_id] = player_name
    return player_names


__all__ = [
    "load_player_names_from_cache",
]
