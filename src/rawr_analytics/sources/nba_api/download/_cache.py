from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal, TypedDict, cast

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
_LEAGUE_GAMES_REQUEST_TIMEOUT_SECONDS = 60
_BOX_SCORE_REQUEST_RETRIES = 5
_BOX_SCORE_RETRY_BACKOFF_SECONDS = 2.0
_BOX_SCORE_REQUEST_DELAY_SECONDS = 0.6
_BOX_SCORE_REQUEST_TIMEOUT_SECONDS = 60

JSONDict = dict[str, Any]
CacheSource = Literal["cached", "fetched"]
_PayloadValidator = Callable[[JSONDict], bool]


class ResultSetPayload(TypedDict, total=False):
    name: str
    rowSet: list[Any]
    data: list[Any]


class LeagueGameFinderResult(TypedDict):
    resultSets: list[ResultSetPayload]


class TeamPlayersPayload(TypedDict, total=False):
    players: list[Any]


class LiveGamePayload(TypedDict, total=False):
    homeTeam: TeamPlayersPayload
    awayTeam: TeamPlayersPayload


class BoxScoreResultSetsDict(TypedDict, total=False):
    PlayerStats: ResultSetPayload


class BoxScorePayload(TypedDict, total=False):
    game: LiveGamePayload
    resultSets: list[ResultSetPayload] | BoxScoreResultSetsDict


def _as_json_dict(value: Any) -> JSONDict | None:
    if isinstance(value, dict):
        return cast(JSONDict, value)
    return None


def _as_result_set_payload(value: Any) -> ResultSetPayload | None:
    if isinstance(value, dict):
        return cast(ResultSetPayload, value)
    return None


def _as_result_set_payload_list(value: object) -> list[ResultSetPayload] | None:
    if not isinstance(value, list):
        return None

    raw_items = cast(list[object], value)
    result: list[ResultSetPayload] = []
    for item in raw_items:
        result_set = _as_result_set_payload(item)
        if result_set is None:
            return None
        result.append(result_set)
    return result


def _as_list(value: Any) -> list[Any] | None:
    if isinstance(value, list):
        return cast(list[Any], value)
    return None


def _load_json_object(cache_path: Path) -> JSONDict | None:
    if not cache_path.exists():
        return None

    raw = json.loads(cache_path.read_text(encoding="utf-8"))
    payload = _as_json_dict(raw)
    if payload is None:
        raise AssertionError(f"Cached payload must be a JSON object: {cache_path}")
    return payload


def _fetch(
    team: Team,
    season: Season,
    attempt: int,
    log_fn: LogFn | None = print,
) -> LeagueGameFinderResult:
    time.sleep(_LEAGUE_GAMES_REQUEST_DELAY_SECONDS)
    if log_fn is not None:
        log_fn(f"api league_games {team.abbreviation(season=season)} {season} attempt={attempt}")

    finder = leaguegamefinder.LeagueGameFinder(
        team_id_nullable=str(team.team_id),
        season_nullable=season.to_nba_api_format(),
        season_type_nullable=season.season_type.to_nba_format(),
        timeout=_LEAGUE_GAMES_REQUEST_TIMEOUT_SECONDS,
    )

    raw = finder.get_dict()
    return cast(LeagueGameFinderResult, raw)


def _fetch_box_score_v2(
    game_id: str,
    *,
    log_fn: LogFn | None = print,
    attempt: int,
) -> BoxScorePayload:
    if log_fn is not None:
        log_fn(f"api box_score {game_id} attempt={attempt}")

    raw = boxscoretraditionalv2.BoxScoreTraditionalV2(
        game_id=game_id,
        timeout=_BOX_SCORE_REQUEST_TIMEOUT_SECONDS,
    ).get_dict()
    return cast(BoxScorePayload, raw)


def _fetch_box_score_v3(
    game_id: str,
    *,
    log_fn: LogFn | None = print,
    attempt: int,
) -> BoxScorePayload:
    if log_fn is not None:
        log_fn(f"api box_score {game_id} attempt={attempt} empty-v2 fallback=v3")

    raw = boxscoretraditionalv3.BoxScoreTraditionalV3(
        game_id=game_id,
        timeout=_BOX_SCORE_REQUEST_TIMEOUT_SECONDS,
    ).get_dict()
    return cast(BoxScorePayload, raw)


def _fetch_box_score_live(
    game_id: str,
    *,
    log_fn: LogFn | None = print,
    attempt: int,
) -> BoxScorePayload:
    if log_fn is not None:
        log_fn(f"api box_score {game_id} attempt={attempt} empty-v3 fallback=live")

    return cast(
        BoxScorePayload,
        live_boxscore.BoxScore(
            game_id=game_id,
            timeout=_BOX_SCORE_REQUEST_TIMEOUT_SECONDS,
        ).get_dict(),
    )


def load_or_fetch_league_games(
    team: Team,
    season: Season,
    log_fn: LogFn | None = print,
) -> tuple[LeagueGameFinderResult, CacheSource]:
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
        return cast(LeagueGameFinderResult, cached_payload), "cached"

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
) -> tuple[BoxScorePayload, CacheSource]:
    for cache_path in _box_score_cache_paths(game_id):
        cached_payload = load_cached_payload(
            cache_path,
            validator=lambda payload: not box_score_payload_is_empty(
                cast(BoxScorePayload, payload)
            ),
            log_fn=log_fn,
        )
        if cached_payload is None:
            continue
        return cast(BoxScorePayload, cached_payload), "cached"

    for attempt in range(1, _BOX_SCORE_REQUEST_RETRIES + 1):
        time.sleep(_BOX_SCORE_REQUEST_DELAY_SECONDS)

        payload = _fetch_box_score_v2(
            game_id,
            log_fn=log_fn,
            attempt=attempt,
        )
        if not box_score_payload_is_empty(payload):
            _write_cached_payload(
                _box_score_cache_path(game_id),
                payload,
            )
            return payload, "fetched"

        v3_payload = _fetch_box_score_v3(
            game_id,
            log_fn=log_fn,
            attempt=attempt,
        )
        if not box_score_payload_is_empty(v3_payload):
            _write_cached_payload(
                _box_score_v3_cache_path(game_id),
                v3_payload,
            )
            return v3_payload, "fetched"

        live_payload = _fetch_box_score_live(
            game_id,
            log_fn=log_fn,
            attempt=attempt,
        )
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
    validator: _PayloadValidator | None = None,
    log_fn: LogFn | None = print,
) -> JSONDict | None:
    return _load_cached_payload(cache_path, validator=validator, log_fn=log_fn)


def _load_cached_payload(
    cache_path: Path,
    *,
    validator: _PayloadValidator | None = None,
    log_fn: LogFn | None = print,
) -> JSONDict | None:
    payload = _load_json_object(cache_path)
    if payload is None:
        return None
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


def _write_cached_payload(cache_path: Path, payload: JSONDict) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = cache_path.parent / f"{cache_path.name}.tmp-{os.getpid()}"
    temp_path.write_text(json.dumps(payload), encoding="utf-8")
    temp_path.replace(cache_path)


def _league_games_payload_is_valid(payload: JSONDict) -> bool:
    result_sets = _as_result_set_payload_list(payload.get("resultSets"))
    if not result_sets:
        return False

    first_result_set = result_sets[0]
    row_set = _as_list(first_result_set.get("rowSet"))
    return row_set is not None and len(row_set) > 0


def box_score_payload_is_empty(payload: JSONDict) -> bool:
    game_payload = _as_json_dict(payload.get("game"))
    if game_payload is not None:
        home_team = _as_json_dict(game_payload.get("homeTeam"))
        away_team = _as_json_dict(game_payload.get("awayTeam"))

        home_players = _as_list(home_team.get("players")) if home_team is not None else None
        away_players = _as_list(away_team.get("players")) if away_team is not None else None

        return not home_players and not away_players

    result_sets_value = payload.get("resultSets")

    result_sets_list = _as_result_set_payload_list(result_sets_value)
    if result_sets_list is not None:
        player_set: ResultSetPayload | None = None

        for result_set in result_sets_list:
            if result_set.get("name") == "PlayerStats":
                player_set = result_set
                break

        if player_set is None and result_sets_list:
            player_set = result_sets_list[0]

        if player_set is None:
            return True

        row_set = _as_list(player_set.get("rowSet"))
        return row_set is None or len(row_set) == 0

    result_sets_dict = _as_json_dict(result_sets_value)
    if result_sets_dict is not None:
        player_set = _as_result_set_payload(result_sets_dict.get("PlayerStats"))
        if player_set is None:
            return True

        data_rows = _as_list(player_set.get("data"))
        if data_rows is not None:
            return len(data_rows) == 0

        row_set = _as_list(player_set.get("rowSet"))
        return row_set is None or len(row_set) == 0

    return True


__all__ = [
    "DEFAULT_NBA_API_DATA_DIR",
    "box_score_payload_is_empty",
    "load_cached_payload",
    "load_or_fetch_box_score_cache",
    "load_or_fetch_league_games",
]
