from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Callable

from nba_api.stats.endpoints import (
    boxscoretraditionalv2,
    boxscoretraditionalv3,
    leaguegamefinder,
)
from nba_api.live.nba.endpoints import boxscore as live_boxscore
from requests import RequestException

from wowy.nba.errors import BoxScoreFetchError, LeagueGamesFetchError
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.season_types import canonicalize_season_type

DEFAULT_SOURCE_DATA_DIR = Path("data/source/nba")
LEAGUE_GAMES_REQUEST_RETRIES = 3
LEAGUE_GAMES_RETRY_BACKOFF_SECONDS = 2.0
LEAGUE_GAMES_REQUEST_DELAY_SECONDS = 0.6
LEAGUE_GAMES_REQUEST_TIMEOUT_SECONDS = 60
BOX_SCORE_REQUEST_RETRIES = 5
BOX_SCORE_RETRY_BACKOFF_SECONDS = 2.0
BOX_SCORE_REQUEST_DELAY_SECONDS = 0.6
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
        validator=_league_games_payload_is_valid,
        log=log,
    )
    if cached_payload is not None:
        return cached_payload, "cached"

    last_error: Exception | None = None

    for attempt in range(1, LEAGUE_GAMES_REQUEST_RETRIES + 1):
        try:
            time.sleep(LEAGUE_GAMES_REQUEST_DELAY_SECONDS)
            if log is not None:
                log(
                    f"api league_games {team_abbreviation} {season} {season_type} "
                    f"attempt={attempt}"
                )
            finder = leaguegamefinder.LeagueGameFinder(
                team_id_nullable=str(team_id),
                season_nullable=season,
                season_type_nullable=season_type,
                timeout=LEAGUE_GAMES_REQUEST_TIMEOUT_SECONDS,
            )
            payload = finder.get_dict()
            if not _league_games_payload_is_valid(payload):
                raise ValueError(
                    f"League games endpoint returned empty data for "
                    f"{team_abbreviation} {season} {season_type}"
                )
            write_cached_payload(cache_path, payload)
            return payload, "fetched"
        except (json.JSONDecodeError, RequestException, ValueError) as exc:
            last_error = exc
            if attempt == LEAGUE_GAMES_REQUEST_RETRIES:
                break
            time.sleep(LEAGUE_GAMES_RETRY_BACKOFF_SECONDS * attempt)

    if last_error is not None:
        raise LeagueGamesFetchError(
            message=(
                f"Failed to fetch league games for {team_abbreviation} {season} "
                f"{season_type} after {LEAGUE_GAMES_REQUEST_RETRIES} attempts: "
                f"{type(last_error).__name__}: {last_error}"
            ),
            resource="league_games",
            identifier=f"{team_abbreviation}:{season}:{season_type}",
            attempts=LEAGUE_GAMES_REQUEST_RETRIES,
            last_error_type=type(last_error).__name__,
            last_error_message=str(last_error),
            team=team_abbreviation,
            season=season,
            season_type=season_type,
        ) from last_error

    raise RuntimeError(
        f"Failed to fetch league games for {team_abbreviation!r} in {season!r}"
    )


def load_or_fetch_league_games(
    team_id: int,
    team_abbreviation: str,
    season: str,
    season_type: str,
    source_data_dir: Path,
    log: LogFn | None = print,
) -> dict:
    payload, _ = load_or_fetch_league_games_with_source(
        team_id=team_id,
        team_abbreviation=team_abbreviation,
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
        log=log,
    )
    return payload


def load_or_fetch_box_score_with_source(
    game_id: str,
    source_data_dir: Path,
    log: LogFn | None = print,
) -> tuple[dict, str]:
    for cache_path in box_score_cache_paths(game_id, source_data_dir=source_data_dir):
        cached_payload = load_cached_payload(
            cache_path,
            validator=lambda payload: not _box_score_payload_is_empty(payload),
            log=log,
        )
        if cached_payload is None:
            continue
        return cached_payload, "cached"

    last_error: Exception | None = None

    for attempt in range(1, BOX_SCORE_REQUEST_RETRIES + 1):
        try:
            time.sleep(BOX_SCORE_REQUEST_DELAY_SECONDS)
            if log is not None:
                log(f"api box_score {game_id} attempt={attempt}")
            box_score = boxscoretraditionalv2.BoxScoreTraditionalV2(
                game_id=game_id,
                timeout=BOX_SCORE_REQUEST_TIMEOUT_SECONDS,
            )
            payload = box_score.get_dict()
            if not _box_score_payload_is_empty(payload):
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
            if _box_score_payload_is_empty(v3_payload):
                if log is not None:
                    log(f"api box_score {game_id} attempt={attempt} empty-v3 fallback=live")
                live_payload = live_boxscore.BoxScore(
                    game_id=game_id,
                    timeout=BOX_SCORE_REQUEST_TIMEOUT_SECONDS,
                ).get_dict()
                if _box_score_payload_is_empty(live_payload):
                    raise ValueError(
                        f"Box score endpoint returned empty data for game {game_id!r}"
                    )
                write_cached_payload(
                    box_score_live_cache_path(game_id, source_data_dir=source_data_dir),
                    live_payload,
                )
                return live_payload, "fetched"
            write_cached_payload(
                box_score_v3_cache_path(game_id, source_data_dir=source_data_dir),
                v3_payload,
            )
            return v3_payload, "fetched"
        except (json.JSONDecodeError, RequestException, ValueError) as exc:
            last_error = exc
            if attempt == BOX_SCORE_REQUEST_RETRIES:
                break
            time.sleep(BOX_SCORE_RETRY_BACKOFF_SECONDS * attempt)

    if last_error is not None:
        raise BoxScoreFetchError(
            message=(
                f"Failed to fetch box score for game {game_id} after "
                f"{BOX_SCORE_REQUEST_RETRIES} attempts: "
                f"{type(last_error).__name__}: {last_error}"
            ),
            resource="box_score",
            identifier=game_id,
            attempts=BOX_SCORE_REQUEST_RETRIES,
            last_error_type=type(last_error).__name__,
            last_error_message=str(last_error),
            game_id=game_id,
        ) from last_error

    raise RuntimeError(f"Failed to fetch box score for game {game_id!r}")


def load_or_fetch_box_score(
    game_id: str,
    source_data_dir: Path,
    log: LogFn | None = print,
) -> dict:
    payload, _ = load_or_fetch_box_score_with_source(
        game_id=game_id,
        source_data_dir=source_data_dir,
        log=log,
    )
    return payload


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
    log: LogFn | None = None,
) -> dict | None:
    return _load_cached_payload(cache_path, validator=validator, log=log)


def _load_cached_payload(
    cache_path: Path,
    *,
    validator: PayloadValidator | None = None,
    log: LogFn | None = None,
) -> dict | None:
    if not cache_path.exists():
        return None

    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        _discard_invalid_cached_payload(
            cache_path,
            reason=f"{type(exc).__name__}: {exc}",
            log=log,
        )
        return None

    if not isinstance(payload, dict):
        _discard_invalid_cached_payload(
            cache_path,
            reason=f"invalid_payload_type={type(payload).__name__}",
            log=log,
        )
        return None

    if validator is not None and not validator(payload):
        _discard_invalid_cached_payload(
            cache_path,
            reason="invalid_or_empty_payload",
            log=log,
        )
        return None
    return payload


def write_cached_payload(cache_path: Path, payload: dict) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = cache_path.with_suffix(f"{cache_path.suffix}.tmp-{os.getpid()}")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    temp_path.replace(cache_path)


def _box_score_payload_is_empty(payload: dict) -> bool:
    game_payload = payload.get("game")
    if isinstance(game_payload, dict):
        home_team = game_payload.get("homeTeam", {})
        away_team = game_payload.get("awayTeam", {})
        home_players = home_team.get("players", []) if isinstance(home_team, dict) else []
        away_players = away_team.get("players", []) if isinstance(away_team, dict) else []
        return len(home_players) == 0 or len(away_players) == 0

    result_sets = payload.get("resultSets")
    if isinstance(result_sets, list):
        named_sets = {
            str(result_set.get("name", "")): result_set
            for result_set in result_sets
            if isinstance(result_set, dict)
        }
        if "PlayerStats" not in named_sets or "TeamStats" not in named_sets:
            return not any(
                len(result_set.get("rowSet", [])) > 0
                for result_set in result_sets
                if isinstance(result_set, dict)
            )
        player_rows = named_sets.get("PlayerStats", {}).get("rowSet", [])
        team_rows = named_sets.get("TeamStats", {}).get("rowSet", [])
        return len(player_rows) == 0 or len(team_rows) == 0
    if isinstance(result_sets, dict):
        player_rows = result_sets.get("PlayerStats", {}).get("data", [])
        team_rows = result_sets.get("TeamStats", {}).get("data", [])
        return len(player_rows) == 0 or len(team_rows) == 0
    return True


def _league_games_payload_is_valid(payload: dict) -> bool:
    result_sets = payload.get("resultSets")
    if isinstance(result_sets, list):
        for result_set in result_sets:
            if not isinstance(result_set, dict):
                continue
            row_set = result_set.get("rowSet", [])
            if len(row_set) > 0:
                return True
        return False
    if isinstance(result_sets, dict):
        for result_set in result_sets.values():
            if not isinstance(result_set, dict):
                continue
            if len(result_set.get("rowSet", [])) > 0:
                return True
            if len(result_set.get("data", [])) > 0:
                return True
        return False
    return False


def _discard_invalid_cached_payload(
    cache_path: Path,
    *,
    reason: str,
    log: LogFn | None,
) -> None:
    try:
        cache_path.unlink()
    except FileNotFoundError:
        return
    except OSError as exc:
        if log is not None:
            log(
                f"cache discard failed path={cache_path} "
                f"reason={reason} unlink_error={type(exc).__name__}: {exc}"
            )
        return
    if log is not None:
        log(f"cache discard path={cache_path} reason={reason}")
