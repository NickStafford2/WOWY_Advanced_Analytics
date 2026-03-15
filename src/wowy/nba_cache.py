from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Callable

from requests import RequestException
from nba_api.stats.endpoints import boxscoretraditionalv2, leaguegamefinder

DEFAULT_SOURCE_DATA_DIR = Path("data/source/nba")
LEAGUE_GAMES_REQUEST_RETRIES = 3
LEAGUE_GAMES_RETRY_BACKOFF_SECONDS = 2.0
BOX_SCORE_REQUEST_RETRIES = 3
BOX_SCORE_RETRY_BACKOFF_SECONDS = 2.0
BOX_SCORE_REQUEST_DELAY_SECONDS = 0.6
LogFn = Callable[[str], None]


def load_or_fetch_league_games_with_source(
    team_id: int,
    team_abbreviation: str,
    season: str,
    season_type: str,
    source_data_dir: Path,
    log: LogFn | None = print,
) -> tuple[dict, str]:
    """Load or fetch league games and report whether the cache was used."""

    cache_path = league_games_cache_path(
        team_abbreviation=team_abbreviation,
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
    )
    cached_payload = load_cached_payload(cache_path)
    if cached_payload is not None:
        return cached_payload, "cached"

    last_error: Exception | None = None

    for attempt in range(1, LEAGUE_GAMES_REQUEST_RETRIES + 1):
        try:
            if log is not None:
                log(
                    f"api league_games {team_abbreviation} {season} {season_type} "
                    f"attempt={attempt}"
                )
            finder = leaguegamefinder.LeagueGameFinder(
                team_id_nullable=str(team_id),
                season_nullable=season,
                season_type_nullable=season_type,
            )
            payload = finder.get_dict()
            write_cached_payload(cache_path, payload)
            return payload, "fetched"
        except RequestException as exc:
            last_error = exc
            if attempt == LEAGUE_GAMES_REQUEST_RETRIES:
                break
            time.sleep(LEAGUE_GAMES_RETRY_BACKOFF_SECONDS * attempt)

    if last_error is not None:
        raise last_error

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
    """Load or fetch a box score and report whether the cache was used."""

    cache_path = box_score_cache_path(game_id, source_data_dir=source_data_dir)
    cached_payload = load_cached_payload(cache_path)
    if cached_payload is not None:
        return cached_payload, "cached"

    last_error: Exception | None = None

    for attempt in range(1, BOX_SCORE_REQUEST_RETRIES + 1):
        try:
            time.sleep(BOX_SCORE_REQUEST_DELAY_SECONDS)
            if log is not None:
                log(f"api box_score {game_id} attempt={attempt}")
            box_score = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
            payload = box_score.get_dict()
            write_cached_payload(cache_path, payload)
            return payload, "fetched"
        except RequestException as exc:
            last_error = exc
            if attempt == BOX_SCORE_REQUEST_RETRIES:
                break
            time.sleep(BOX_SCORE_RETRY_BACKOFF_SECONDS * attempt)

    if last_error is not None:
        raise last_error

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
    season_type_slug = season_type.lower().replace(" ", "_")
    filename = f"{team_abbreviation}_{season}_{season_type_slug}_leaguegamefinder.json"
    return source_data_dir / "team_seasons" / filename


def box_score_cache_path(game_id: str, source_data_dir: Path) -> Path:
    return source_data_dir / "boxscores" / f"{game_id}_boxscoretraditionalv2.json"


def load_cached_payload(cache_path: Path) -> dict | None:
    if not cache_path.exists():
        return None

    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return None


def write_cached_payload(cache_path: Path, payload: dict) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = cache_path.with_suffix(f"{cache_path.suffix}.tmp-{os.getpid()}")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    temp_path.replace(cache_path)
