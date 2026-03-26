from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Callable

from nba_api.live.nba.endpoints import boxscore as live_boxscore
from nba_api.stats.endpoints import (
    boxscoretraditionalv2,
    boxscoretraditionalv3,
    leaguegamefinder,
)

from rawr_analytics.nba.errors import BoxScoreFetchError, LeagueGamesFetchError
from rawr_analytics.nba.season_types import canonicalize_season_type
from rawr_analytics.nba.seasons import canonicalize_season_string

DEFAULT_SOURCE_DATA_DIR = Path("data/source/nba")
_LEAGUE_GAMES_REQUEST_RETRIES = 3
_LEAGUE_GAMES_RETRY_BACKOFF_SECONDS = 2.0
_LEAGUE_GAMES_REQUEST_DELAY_SECONDS = 0.6
LEAGUE_GAMES_REQUEST_TIMEOUT_SECONDS = 60
_BOX_SCORE_REQUEST_RETRIES = 5
_BOX_SCORE_RETRY_BACKOFF_SECONDS = 2.0
_BOX_SCORE_REQUEST_DELAY_SECONDS = 0.6
BOX_SCORE_REQUEST_TIMEOUT_SECONDS = 60
LogFn = Callable[[str], None]
PayloadValidator = Callable[[dict], bool]


def load_or_fetch_league_games_with_source(
    team_id: int,
    team_abbreviation: str,
    season: str,
    season_type: str,
    source_data_dir: Path,
    log: LogFn | None = print,
) -> tuple[dict, str]:
    season = canonicalize_season_string(season)
    season_type = canonicalize_season_type(season_type)
    cache_path = league_games_cache_path(
        team_abbreviation=team_abbreviation,
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
    )
    cached_payload = load_cached_payload(
        cache_path,
        validator=league_games_payload_is_valid,
        log=log,
    )
    if cached_payload is not None:
        return cached_payload, "cached"

    for attempt in range(1, _LEAGUE_GAMES_REQUEST_RETRIES + 1):
        time.sleep(_LEAGUE_GAMES_REQUEST_DELAY_SECONDS)
        if log is not None:
            log(f"api league_games {team_abbreviation} {season} {season_type} attempt={attempt}")
        finder = leaguegamefinder.LeagueGameFinder(
            team_id_nullable=str(team_id),
            season_nullable=season,
            season_type_nullable=season_type,
            timeout=LEAGUE_GAMES_REQUEST_TIMEOUT_SECONDS,
        )
        payload = finder.get_dict()
        if league_games_payload_is_valid(payload):
            write_cached_payload(cache_path, payload)
            return payload, "fetched"
        if attempt < _LEAGUE_GAMES_REQUEST_RETRIES:
            time.sleep(_LEAGUE_GAMES_RETRY_BACKOFF_SECONDS * attempt)

    raise LeagueGamesFetchError(
        message=(
            f"Failed to fetch league games for {team_abbreviation} {season} "
            f"{season_type} after {_LEAGUE_GAMES_REQUEST_RETRIES} attempts: "
            "ValueError: endpoint returned empty data"
        ),
        resource="league_games",
        identifier=f"{team_abbreviation}:{season}:{season_type}",
        attempts=_LEAGUE_GAMES_REQUEST_RETRIES,
        last_error_type="ValueError",
        last_error_message="endpoint returned empty data",
        team=team_abbreviation,
        season=season,
        season_type=season_type,
    )


def load_or_fetch_box_score_with_source(
    game_id: str,
    source_data_dir: Path,
    log: LogFn | None = print,
) -> tuple[dict, str]:
    for cache_path in box_score_cache_paths(game_id, source_data_dir=source_data_dir):
        cached_payload = load_cached_payload(
            cache_path,
            validator=lambda payload: not box_score_payload_is_empty(payload),
            log=log,
        )
        if cached_payload is None:
            continue
        return cached_payload, "cached"

    for attempt in range(1, _BOX_SCORE_REQUEST_RETRIES + 1):
        time.sleep(_BOX_SCORE_REQUEST_DELAY_SECONDS)
        if log is not None:
            log(f"api box_score {game_id} attempt={attempt}")
        box_score = boxscoretraditionalv2.BoxScoreTraditionalV2(
            game_id=game_id,
            timeout=BOX_SCORE_REQUEST_TIMEOUT_SECONDS,
        )
        payload = box_score.get_dict()
        if not box_score_payload_is_empty(payload):
            write_cached_payload(
                box_score_cache_path(game_id, source_data_dir=source_data_dir),
                payload,
            )
            return payload, "fetched"
        if log is not None:
            log(f"api box_score {game_id} attempt={attempt} empty-v2 fallback=v3")
        v3_payload = boxscoretraditionalv3.BoxScoreTraditionalV3(
            game_id=game_id,
            timeout=BOX_SCORE_REQUEST_TIMEOUT_SECONDS,
        ).get_dict()
        if not box_score_payload_is_empty(v3_payload):
            write_cached_payload(
                box_score_v3_cache_path(game_id, source_data_dir=source_data_dir),
                v3_payload,
            )
            return v3_payload, "fetched"
        if log is not None:
            log(f"api box_score {game_id} attempt={attempt} empty-v3 fallback=live")
        live_payload = live_boxscore.BoxScore(
            game_id=game_id,
            timeout=BOX_SCORE_REQUEST_TIMEOUT_SECONDS,
        ).get_dict()
        if not box_score_payload_is_empty(live_payload):
            write_cached_payload(
                box_score_live_cache_path(game_id, source_data_dir=source_data_dir),
                live_payload,
            )
            return live_payload, "fetched"
        if attempt < _BOX_SCORE_REQUEST_RETRIES:
            time.sleep(_BOX_SCORE_RETRY_BACKOFF_SECONDS * attempt)

    raise BoxScoreFetchError(
        message=(
            f"Failed to fetch box score for game {game_id} after "
            f"{_BOX_SCORE_REQUEST_RETRIES} attempts: "
            "ValueError: endpoint returned empty data"
        ),
        resource="box_score",
        identifier=game_id,
        attempts=_BOX_SCORE_REQUEST_RETRIES,
        last_error_type="ValueError",
        last_error_message="endpoint returned empty data",
        game_id=game_id,
    )


def league_games_cache_path(
    team_abbreviation: str,
    season: str,
    season_type: str,
    source_data_dir: Path,
) -> Path:
    season = canonicalize_season_string(season)
    season_type_slug = canonicalize_season_type(season_type).lower().replace(" ", "_")
    filename = f"{team_abbreviation}_{season}_{season_type_slug}_leaguegamefinder.json"
    return source_data_dir / "team_seasons" / filename


def box_score_cache_path(game_id: str, source_data_dir: Path) -> Path:
    return source_data_dir / "boxscores" / f"{game_id}_boxscoretraditionalv2.json"


def box_score_v3_cache_path(game_id: str, source_data_dir: Path) -> Path:
    return source_data_dir / "boxscores" / f"{game_id}_boxscoretraditionalv3.json"


def box_score_live_cache_path(game_id: str, source_data_dir: Path) -> Path:
    return source_data_dir / "boxscores" / f"{game_id}_boxscorelive.json"


def box_score_cache_paths(game_id: str, source_data_dir: Path) -> tuple[Path, Path, Path]:
    return (
        box_score_cache_path(game_id, source_data_dir=source_data_dir),
        box_score_v3_cache_path(game_id, source_data_dir=source_data_dir),
        box_score_live_cache_path(game_id, source_data_dir=source_data_dir),
    )


def load_cached_payload(
    cache_path: Path,
    *,
    validator: PayloadValidator | None = None,
    log: LogFn | None = print,
) -> dict | None:
    return _load_cached_payload(cache_path, validator=validator, log=log)


def discard_invalid_cached_payload(
    cache_path: Path,
    *,
    reason: str,
    log: LogFn | None = print,
) -> None:
    _discard_cache_file(cache_path, reason=reason, log=log)


def _load_cached_payload(
    cache_path: Path,
    *,
    validator: PayloadValidator | None = None,
    log: LogFn | None = print,
) -> dict | None:
    if not cache_path.exists():
        return None
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"Cached payload must be a JSON object: {cache_path}")
    if validator is not None and not validator(payload):
        _discard_cache_file(cache_path, reason="invalid_or_empty_payload", log=log)
        return None
    return payload


def _discard_cache_file(
    cache_path: Path,
    *,
    reason: str,
    log: LogFn | None = print,
) -> None:
    cache_path.unlink(missing_ok=True)
    if log is not None:
        log(f"cache discard {cache_path} reason={reason}")


def write_cached_payload(cache_path: Path, payload: dict) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = cache_path.parent / f"{cache_path.name}.tmp-{os.getpid()}"
    temp_path.write_text(json.dumps(payload), encoding="utf-8")
    temp_path.replace(cache_path)


def league_games_payload_is_valid(payload: dict) -> bool:
    result_sets = payload.get("resultSets")
    if not isinstance(result_sets, list) or not result_sets:
        return False
    first_result_set = result_sets[0]
    if not isinstance(first_result_set, dict):
        return False
    row_set = first_result_set.get("rowSet")
    return isinstance(row_set, list) and len(row_set) > 0


def box_score_payload_is_empty(payload: dict) -> bool:
    game_payload = payload.get("game")
    if isinstance(game_payload, dict):
        home_players = (game_payload.get("homeTeam") or {}).get("players") or []
        away_players = (game_payload.get("awayTeam") or {}).get("players") or []
        return not home_players and not away_players

    result_sets = payload.get("resultSets")
    if isinstance(result_sets, list):
        named_sets = {
            str(result_set.get("name", "")): result_set
            for result_set in result_sets
            if isinstance(result_set, dict)
        }
        player_set = named_sets.get("PlayerStats")
        if player_set is None and result_sets:
            player_set = result_sets[0] if isinstance(result_sets[0], dict) else None
        row_set = (player_set or {}).get("rowSet")
        return not isinstance(row_set, list) or len(row_set) == 0
    if isinstance(result_sets, dict):
        player_set = result_sets.get("PlayerStats")
        if not isinstance(player_set, dict):
            return True
        data_rows = player_set.get("data")
        if isinstance(data_rows, list):
            return len(data_rows) == 0
        row_set = player_set.get("rowSet")
        return not isinstance(row_set, list) or len(row_set) == 0
    return True


__all__ = [
    "BOX_SCORE_REQUEST_TIMEOUT_SECONDS",
    "DEFAULT_SOURCE_DATA_DIR",
    "LEAGUE_GAMES_REQUEST_TIMEOUT_SECONDS",
    "box_score_payload_is_empty",
    "box_score_cache_path",
    "box_score_cache_paths",
    "box_score_live_cache_path",
    "box_score_v3_cache_path",
    "discard_invalid_cached_payload",
    "league_games_payload_is_valid",
    "league_games_cache_path",
    "load_cached_payload",
    "load_or_fetch_box_score_with_source",
    "load_or_fetch_league_games_with_source",
    "write_cached_payload",
]
