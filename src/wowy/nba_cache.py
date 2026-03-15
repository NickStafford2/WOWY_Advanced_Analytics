from __future__ import annotations

import json
import time
from pathlib import Path

from requests import RequestException
from nba_api.stats.endpoints import boxscoretraditionalv2, leaguegamefinder

DEFAULT_SOURCE_DATA_DIR = Path("data/source/nba")
BOX_SCORE_REQUEST_RETRIES = 3
BOX_SCORE_RETRY_BACKOFF_SECONDS = 2.0
BOX_SCORE_REQUEST_DELAY_SECONDS = 0.6


def load_or_fetch_league_games(
    team_id: int,
    team_abbreviation: str,
    season: str,
    season_type: str,
    source_data_dir: Path,
) -> dict:
    """Load a cached team-season response or fetch and cache it from the NBA API."""

    cache_path = league_games_cache_path(
        team_abbreviation=team_abbreviation,
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
    )
    cached_payload = load_cached_payload(cache_path)
    if cached_payload is not None:
        print(f"cache league_games {team_abbreviation} {season} {season_type}")
        return cached_payload

    print(f"api league_games {team_abbreviation} {season} {season_type}")
    finder = leaguegamefinder.LeagueGameFinder(
        team_id_nullable=str(team_id),
        season_nullable=season,
        season_type_nullable=season_type,
    )
    payload = finder.get_dict()
    write_cached_payload(cache_path, payload)
    return payload


def load_or_fetch_box_score(game_id: str, source_data_dir: Path) -> dict:
    """Load a cached box score response or fetch and cache it from the NBA API."""

    cache_path = box_score_cache_path(game_id, source_data_dir=source_data_dir)
    cached_payload = load_cached_payload(cache_path)
    if cached_payload is not None:
        print(f"cache box_score {game_id}")
        return cached_payload

    last_error: Exception | None = None

    for attempt in range(1, BOX_SCORE_REQUEST_RETRIES + 1):
        try:
            time.sleep(BOX_SCORE_REQUEST_DELAY_SECONDS)
            print(f"api box_score {game_id} attempt={attempt}")
            box_score = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
            payload = box_score.get_dict()
            write_cached_payload(cache_path, payload)
            return payload
        except RequestException as exc:
            last_error = exc
            if attempt == BOX_SCORE_REQUEST_RETRIES:
                break
            time.sleep(BOX_SCORE_RETRY_BACKOFF_SECONDS * attempt)

    if last_error is not None:
        raise last_error

    raise RuntimeError(f"Failed to fetch box score for game {game_id!r}")


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

    with open(cache_path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_cached_payload(cache_path: Path, payload: dict) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
