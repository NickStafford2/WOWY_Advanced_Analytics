from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from pathlib import Path

from nba_api.live.nba.endpoints import boxscore as live_boxscore
from nba_api.stats.endpoints import (
    boxscoretraditionalv2,
    boxscoretraditionalv3,
    leaguegamefinder,
)

from rawr_analytics.shared.common import LogFn
from rawr_analytics.shared.ingest import BoxScoreFetchError, LeagueGamesFetchError
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team

DEFAULT_NBA_API_DATA_DIR = Path("data/source/nba_api")
_LEAGUE_GAMES_REQUEST_RETRIES = 3
_LEAGUE_GAMES_RETRY_BACKOFF_SECONDS = 2.0
_LEAGUE_GAMES_REQUEST_DELAY_SECONDS = 0.6
LEAGUE_GAMES_REQUEST_TIMEOUT_SECONDS = 60
_BOX_SCORE_REQUEST_RETRIES = 5
_BOX_SCORE_RETRY_BACKOFF_SECONDS = 2.0
_BOX_SCORE_REQUEST_DELAY_SECONDS = 0.6
BOX_SCORE_REQUEST_TIMEOUT_SECONDS = 60
PayloadValidator = Callable[[dict], bool]


def _fetch(
    team: Team,
    season: Season,
    attempt: int,
    log_fn: LogFn | None = print,
) -> dict:
    time.sleep(_LEAGUE_GAMES_REQUEST_DELAY_SECONDS)
    if log_fn is not None:
        log_fn(f"api league_games {team.abbreviation(season=season)} {season} attempt={attempt}")
    finder = leaguegamefinder.LeagueGameFinder(
        team_id_nullable=str(team.team_id),
        season_nullable=season.to_nba_api_format(),
        season_type_nullable=season.season_type.to_nba_format(),
        timeout=LEAGUE_GAMES_REQUEST_TIMEOUT_SECONDS,
    )
    return finder.get_dict()


def load_or_fetch_league_games(
    team: Team,
    season: Season,
    log_fn: LogFn | None = print,
) -> tuple[dict, str]:
    cache_path = _league_games_cache_path(
        team=team,
        season=season,
    )
    cached_payload = load_cached_payload(
        cache_path,
        validator=_league_games_payload_is_valid,
        log_fn=log_fn,
    )
    if cached_payload is not None:
        return cached_payload, "cached"

    for attempt in range(1, _LEAGUE_GAMES_REQUEST_RETRIES + 1):
        payload = _fetch(team, season, attempt, log_fn)
        if _league_games_payload_is_valid(payload):
            _write_cached_payload(cache_path, payload)
            return payload, "fetched"
        if attempt < _LEAGUE_GAMES_REQUEST_RETRIES:
            time.sleep(_LEAGUE_GAMES_RETRY_BACKOFF_SECONDS * attempt)

    raise LeagueGamesFetchError(
        message=(
            f"Failed to fetch league games for {team.abbreviation(season=season)} {season} "
            f"{season.season_type} after {_LEAGUE_GAMES_REQUEST_RETRIES} attempts: "
            "ValueError: endpoint returned empty data"
        ),
        resource="league_games",
        identifier=f"{team.abbreviation(season=season)}:{season}",
        attempts=_LEAGUE_GAMES_REQUEST_RETRIES,
        last_error_type="ValueError",
        last_error_message="endpoint returned empty data",
        team=team,
        season=season,
    )


def load_or_fetch_box_score_cache(
    game_id: str,
    log_fn: LogFn | None = print,
) -> tuple[dict, str]:
    for cache_path in _box_score_cache_paths(game_id):
        cached_payload = load_cached_payload(
            cache_path,
            validator=lambda payload: not box_score_payload_is_empty(payload),
            log_fn=log_fn,
        )
        if cached_payload is None:
            continue
        return cached_payload, "cached"

    for attempt in range(1, _BOX_SCORE_REQUEST_RETRIES + 1):
        time.sleep(_BOX_SCORE_REQUEST_DELAY_SECONDS)
        if log_fn is not None:
            log_fn(f"api box_score {game_id} attempt={attempt}")
        box_score = boxscoretraditionalv2.BoxScoreTraditionalV2(
            game_id=game_id,
            timeout=BOX_SCORE_REQUEST_TIMEOUT_SECONDS,
        )
        payload = box_score.get_dict()
        if not box_score_payload_is_empty(payload):
            _write_cached_payload(
                _box_score_cache_path(game_id),
                payload,
            )
            return payload, "fetched"
        if log_fn is not None:
            log_fn(f"api box_score {game_id} attempt={attempt} empty-v2 fallback=v3")
        v3_payload = boxscoretraditionalv3.BoxScoreTraditionalV3(
            game_id=game_id,
            timeout=BOX_SCORE_REQUEST_TIMEOUT_SECONDS,
        ).get_dict()
        if not box_score_payload_is_empty(v3_payload):
            _write_cached_payload(
                _box_score_v3_cache_path(game_id),
                v3_payload,
            )
            return v3_payload, "fetched"
        if log_fn is not None:
            log_fn(f"api box_score {game_id} attempt={attempt} empty-v3 fallback=live")
        live_payload = live_boxscore.BoxScore(
            game_id=game_id,
            timeout=BOX_SCORE_REQUEST_TIMEOUT_SECONDS,
        ).get_dict()
        if not box_score_payload_is_empty(live_payload):
            _write_cached_payload(
                _box_score_live_cache_path(game_id),
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


def _league_games_cache_path(
    team: Team,
    season: Season,
) -> Path:
    filename = (
        f"{team.abbreviation(season=season)}_{season}_"
        f"{season.season_type.to_nba_format().lower().replace(' ', '_')}_leaguegamefinder.json"
    )
    return DEFAULT_NBA_API_DATA_DIR / "team_seasons" / filename


def _box_score_cache_path(game_id: str) -> Path:
    return DEFAULT_NBA_API_DATA_DIR / "boxscores" / f"{game_id}_boxscoretraditionalv2.json"


def _box_score_v3_cache_path(game_id: str) -> Path:
    return DEFAULT_NBA_API_DATA_DIR / "boxscores" / f"{game_id}_boxscoretraditionalv3.json"


def _box_score_live_cache_path(game_id: str) -> Path:
    return DEFAULT_NBA_API_DATA_DIR / "boxscores" / f"{game_id}_boxscorelive.json"


def _box_score_cache_paths(game_id: str) -> tuple[Path, Path, Path]:
    return (
        _box_score_cache_path(game_id),
        _box_score_v3_cache_path(game_id),
        _box_score_live_cache_path(game_id),
    )


def load_cached_payload(
    cache_path: Path,
    *,
    validator: PayloadValidator | None = None,
    log_fn: LogFn | None = print,
) -> dict | None:
    return _load_cached_payload(cache_path, validator=validator, log_fn=log_fn)


def _discard_invalid_cached_payload(
    cache_path: Path,
    *,
    reason: str,
    log: LogFn | None = print,
) -> None:
    _discard_cache_file(cache_path, reason=reason, log_fn=log)


def _load_cached_payload(
    cache_path: Path,
    *,
    validator: PayloadValidator | None = None,
    log_fn: LogFn | None = print,
) -> dict | None:
    if not cache_path.exists():
        return None
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"Cached payload must be a JSON object: {cache_path}")
    if validator is not None and not validator(payload):
        _discard_cache_file(cache_path, reason="invalid_or_empty_payload", log_fn=log_fn)
        return None
    return payload


def _discard_cache_file(
    cache_path: Path,
    *,
    reason: str,
    log_fn: LogFn | None = print,
) -> None:
    cache_path.unlink(missing_ok=True)
    if log_fn is not None:
        log_fn(f"cache discard {cache_path} reason={reason}")


def _write_cached_payload(cache_path: Path, payload: dict) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = cache_path.parent / f"{cache_path.name}.tmp-{os.getpid()}"
    temp_path.write_text(json.dumps(payload), encoding="utf-8")
    temp_path.replace(cache_path)


def _league_games_payload_is_valid(payload: dict) -> bool:
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
    "DEFAULT_NBA_API_DATA_DIR",
    "LEAGUE_GAMES_REQUEST_TIMEOUT_SECONDS",
    "_box_score_cache_path",
    "_box_score_cache_paths",
    "_box_score_live_cache_path",
    "_box_score_v3_cache_path",
    "_discard_invalid_cached_payload",
    "_league_games_cache_path",
    "_league_games_payload_is_valid",
    "_write_cached_payload",
    "box_score_payload_is_empty",
    "load_cached_payload",
    "load_or_fetch_box_score_cache",
    "load_or_fetch_league_games",
]
