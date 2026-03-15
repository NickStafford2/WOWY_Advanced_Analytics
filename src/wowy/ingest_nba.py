from __future__ import annotations

import csv
import json
import time
from pathlib import Path

import pandas as pd
from requests import RequestException
from nba_api.stats.endpoints import boxscoretraditionalv2, leaguegamefinder
from nba_api.stats.static import teams

from wowy.types import GameRecord

SOURCE_DATA_DIR = Path("data/source/nba")
BOX_SCORE_REQUEST_RETRIES = 3
BOX_SCORE_RETRY_BACKOFF_SECONDS = 2.0
BOX_SCORE_REQUEST_DELAY_SECONDS = 0.6


def fetch_team_season_games(
    team_abbreviation: str,
    season: str,
    season_type: str = "Regular Season",
) -> list[GameRecord]:
    """Fetch one NBA team-season and return rows in the existing game CSV shape.

    Each returned record matches the current WOWY input model:
    one row per game from one team's perspective with `game_id`, `team`,
    `margin`, and the set of NBA player ids who appeared in that game.
    """

    team = teams.find_team_by_abbreviation(team_abbreviation.upper())
    if team is None:
        raise ValueError(f"Unknown NBA team abbreviation: {team_abbreviation!r}")

    finder_payload = _load_or_fetch_league_games(
        team_id=team["id"],
        team_abbreviation=team["abbreviation"],
        season=season,
        season_type=season_type,
    )
    games_df = _result_set_to_data_frame(finder_payload["resultSets"][0])

    if games_df.empty:
        return []

    records: list[GameRecord] = []

    for game_id in games_df["GAME_ID"].drop_duplicates().tolist():
        records.append(
            _fetch_game_record(game_id=game_id, team_abbreviation=team["abbreviation"])
        )

    return records


def write_team_season_games_csv(
    team_abbreviation: str,
    season: str,
    csv_path: Path | str,
    season_type: str = "Regular Season",
) -> None:
    """Fetch one NBA team-season and write it as the existing `games.csv` format."""

    games = fetch_team_season_games(
        team_abbreviation=team_abbreviation,
        season=season,
        season_type=season_type,
    )

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["game_id", "team", "margin", "players"])
        writer.writeheader()

        for game in games:
            writer.writerow(
                {
                    "game_id": game["game_id"],
                    "team": game["team"],
                    "margin": game["margin"],
                    "players": ";".join(
                        str(player_id) for player_id in sorted(game["players"])
                    ),
                }
            )


def _fetch_game_record(game_id: str, team_abbreviation: str) -> GameRecord:
    """Fetch one NBA game and normalize the selected team into a `GameRecord`."""

    box_score_payload = _load_or_fetch_box_score(game_id)
    player_stats_df = _result_set_to_data_frame(box_score_payload["resultSets"][0])
    team_stats_df = _result_set_to_data_frame(box_score_payload["resultSets"][1])

    team_rows = team_stats_df.loc[
        team_stats_df["TEAM_ABBREVIATION"] == team_abbreviation,
    ]
    if team_rows.empty:
        raise ValueError(
            f"Team {team_abbreviation!r} not found in box score for game {game_id!r}"
        )

    player_rows = player_stats_df.loc[
        player_stats_df["TEAM_ABBREVIATION"] == team_abbreviation,
    ]
    players = _extract_players_who_appeared(player_rows["PLAYER_ID"], player_rows["MIN"])
    if not players:
        raise ValueError(
            f"No active players found for team {team_abbreviation!r} in game {game_id!r}"
        )

    plus_minus = float(team_rows.iloc[0]["PLUS_MINUS"])
    return {
        "game_id": game_id,
        "team": team_abbreviation,
        "margin": plus_minus,
        "players": players,
    }


def _extract_players_who_appeared(player_ids, minutes_played) -> set[int]:
    """Return the NBA player ids that logged non-zero minutes in the game."""

    players: set[int] = set()

    for player_id, minutes in zip(player_ids.tolist(), minutes_played.tolist(), strict=True):
        if player_id is None:
            continue
        if not _played_in_game(minutes):
            continue
        players.add(int(player_id))

    return players


def _played_in_game(minutes: object) -> bool:
    """Return whether the NBA box score minute value indicates game participation."""

    if minutes is None:
        return False

    minute_text = str(minutes).strip()
    if not minute_text:
        return False
    if minute_text in {"0", "0:00", "0.0"}:
        return False

    return True


def _load_or_fetch_league_games(
    team_id: int,
    team_abbreviation: str,
    season: str,
    season_type: str,
) -> dict:
    """Load a cached team-season response or fetch and cache it from the NBA API."""

    cache_path = _league_games_cache_path(
        team_abbreviation=team_abbreviation,
        season=season,
        season_type=season_type,
    )
    cached_payload = _load_cached_payload(cache_path)
    if cached_payload is not None:
        print(f"cache league_games {team_abbreviation} {season} {season_type}")
        return cached_payload

    print(f"api league_games {team_abbreviation} {season} {season_type}")
    finder = leaguegamefinder.LeagueGameFinder(
        team_id_nullable=team_id,
        season_nullable=season,
        season_type_nullable=season_type,
    )
    payload = finder.get_dict()
    _write_cached_payload(cache_path, payload)
    return payload


def _load_or_fetch_box_score(game_id: str) -> dict:
    """Load a cached box score response or fetch and cache it from the NBA API."""

    cache_path = _box_score_cache_path(game_id)
    cached_payload = _load_cached_payload(cache_path)
    if cached_payload is not None:
        print(f"cache box_score {game_id}")
        return cached_payload

    last_error: Exception | None = None

    for attempt in range(1, BOX_SCORE_REQUEST_RETRIES + 1):
        try:
            # Pace live requests so repeated game fetches are less likely to be rate limited.
            time.sleep(BOX_SCORE_REQUEST_DELAY_SECONDS)
            print(f"api box_score {game_id} attempt={attempt}")
            box_score = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
            payload = box_score.get_dict()
            _write_cached_payload(cache_path, payload)
            return payload
        except RequestException as exc:
            last_error = exc
            if attempt == BOX_SCORE_REQUEST_RETRIES:
                break
            time.sleep(BOX_SCORE_RETRY_BACKOFF_SECONDS * attempt)

    if last_error is not None:
        raise last_error

    raise RuntimeError(f"Failed to fetch box score for game {game_id!r}")


def _league_games_cache_path(
    team_abbreviation: str,
    season: str,
    season_type: str,
) -> Path:
    """Return the cache path for one team-season league game finder response."""

    season_type_slug = season_type.lower().replace(" ", "_")
    filename = f"{team_abbreviation}_{season}_{season_type_slug}_leaguegamefinder.json"
    return SOURCE_DATA_DIR / "team_seasons" / filename


def _box_score_cache_path(game_id: str) -> Path:
    """Return the cache path for one game box score response."""

    return SOURCE_DATA_DIR / "boxscores" / f"{game_id}_boxscoretraditionalv2.json"


def _load_cached_payload(cache_path: Path) -> dict | None:
    """Load a cached JSON payload if it exists."""

    if not cache_path.exists():
        return None

    with open(cache_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_cached_payload(cache_path: Path, payload: dict) -> None:
    """Write a JSON payload to the local source-data cache."""

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _result_set_to_data_frame(result_set: dict) -> pd.DataFrame:
    """Build a pandas DataFrame from an NBA API result set payload."""

    return pd.DataFrame(result_set["rowSet"], columns=result_set["headers"])
