from __future__ import annotations

from pathlib import Path
from typing import Callable

from nba_api.stats.static import teams

from wowy.apps.wowy.derive import derive_wowy_games
from wowy.data.game_cache_db import (
    replace_team_season_normalized_rows,
)
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.build_models import (
    TeamSeasonArtifacts,
    TeamSeasonBuildResult,
    TeamSeasonRunSummary,
)
from wowy.nba.cache import DEFAULT_SOURCE_DATA_DIR, load_or_fetch_league_games_with_source
from wowy.nba.errors import TeamSeasonConsistencyError
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from wowy.nba.normalize import (
    fetch_normalized_game_data_with_source,
    load_player_names_from_cache as load_cached_player_names,
    result_set_to_data_frame,
)
from wowy.nba.team_seasons import TeamSeasonScope
from wowy.nba.validation import validate_team_season_records
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.season_types import canonicalize_season_type


ProgressFn = Callable[[dict], None]

TEAM_ABBREVIATION_ALIASES = {
    "CHH": "NOP",
    "NJN": "BKN",
    "NOH": "NOP",
    "NOK": "NOP",
    "SEA": "OKC",
    "VAN": "MEM",
    "WSB": "WAS",
}


def season_type_slug(season_type: str) -> str:
    return canonicalize_season_type(season_type).lower().replace(" ", "_")


def resolve_team_lookup_abbreviation(team_abbreviation: str) -> str:
    normalized = team_abbreviation.upper()
    return TEAM_ABBREVIATION_ALIASES.get(normalized, normalized)


def fetch_team_season_data(
    team_abbreviation: str,
    season: str,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    log: Callable[[str], None] | None = print,
    progress: ProgressFn | None = None,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    season = canonicalize_season_string(season)
    season_type = canonicalize_season_type(season_type)
    result = build_team_season_artifacts(
        team_abbreviation=team_abbreviation,
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
        log=log,
        progress=progress,
    )
    return result.artifacts.normalized_games, result.artifacts.normalized_game_players


def build_team_season_artifacts(
    team_abbreviation: str,
    season: str,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    log: Callable[[str], None] | None = print,
    progress: ProgressFn | None = None,
) -> TeamSeasonBuildResult:
    season = canonicalize_season_string(season)
    season_type = canonicalize_season_type(season_type)
    requested_team_abbreviation = team_abbreviation.upper()
    team = teams.find_team_by_abbreviation(
        resolve_team_lookup_abbreviation(requested_team_abbreviation)
    )
    if team is None:
        raise ValueError(f"Unknown NBA team abbreviation: {team_abbreviation!r}")

    finder_payload, league_games_source = load_or_fetch_league_games_with_source(
        team_id=team["id"],
        team_abbreviation=requested_team_abbreviation,
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
        log=log,
    )
    games_df = result_set_to_data_frame(finder_payload["resultSets"][0])

    if games_df.empty:
        return TeamSeasonBuildResult(
            artifacts=TeamSeasonArtifacts([], [], []),
            summary=TeamSeasonRunSummary(
                team=requested_team_abbreviation,
                season=season,
                season_type=season_type,
                league_games_source=league_games_source,
                total_games=0,
                processed_games=0,
                skipped_games=0,
                fetched_box_scores=0,
                cached_box_scores=0,
            ),
        )

    normalized_games: list[NormalizedGameRecord] = []
    normalized_game_players: list[NormalizedGamePlayerRecord] = []
    fetched_box_scores = 0
    cached_box_scores = 0
    skipped_games = 0

    unique_games_df = games_df.drop_duplicates(subset=["GAME_ID"])
    total_games = len(unique_games_df)
    for game_index, (_, game_row) in enumerate(unique_games_df.iterrows(), start=1):
        game_id = str(game_row["GAME_ID"])
        try:
            normalized_game, game_players, box_score_source = (
                fetch_normalized_game_data_with_source(
                    game_id=game_id,
                    team_abbreviation=requested_team_abbreviation,
                    season=season,
                    game_date=extract_game_date(game_row),
                    opponent=extract_opponent(game_row, requested_team_abbreviation),
                    is_home=extract_is_home(game_row, requested_team_abbreviation),
                    season_type=season_type,
                    source_data_dir=source_data_dir,
                    log=log,
                )
            )
        except ValueError as exc:
            skipped_games += 1
            if log is not None:
                log(
                    f"skip game {game_id} {requested_team_abbreviation} {season} reason={exc}"
                )
            if progress is not None:
                progress(
                    {
                        "team": requested_team_abbreviation,
                        "season": season,
                        "game_id": game_id,
                        "current": game_index,
                        "total": total_games,
                        "status": "skipped",
                    }
                )
            continue

        if box_score_source == "fetched":
            fetched_box_scores += 1
        else:
            cached_box_scores += 1
        normalized_games.append(normalized_game)
        normalized_game_players.extend(game_players)
        if progress is not None:
            progress(
                {
                    "team": requested_team_abbreviation,
                    "season": season,
                    "game_id": game_id,
                    "current": game_index,
                    "total": total_games,
                    "status": "ok",
                }
            )

    artifacts = TeamSeasonArtifacts(
        normalized_games=normalized_games,
        normalized_game_players=normalized_game_players,
        wowy_games=derive_wowy_games(normalized_games, normalized_game_players),
    )
    summary = TeamSeasonRunSummary(
        team=requested_team_abbreviation,
        season=season,
        season_type=season_type,
        league_games_source=league_games_source,
        total_games=total_games,
        processed_games=len(normalized_games),
        skipped_games=skipped_games,
        fetched_box_scores=fetched_box_scores,
        cached_box_scores=cached_box_scores,
    )
    return TeamSeasonBuildResult(artifacts=artifacts, summary=summary)

def cache_team_season_data(
    team_abbreviation: str,
    season: str,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    log: Callable[[str], None] | None = print,
    progress: ProgressFn | None = None,
) -> TeamSeasonRunSummary:
    season = canonicalize_season_string(season)
    season_type = canonicalize_season_type(season_type)
    normalized_games_source_path = (
        f"sqlite://normalized_games/"
        f"{team_abbreviation.upper()}_{season}_{season_type_slug(season_type)}"
    )
    result = build_team_season_artifacts(
        team_abbreviation=team_abbreviation,
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
        log=log,
        progress=progress,
    )
    replace_team_season_normalized_rows(
        player_metrics_db_path,
        team=team_abbreviation.upper(),
        season=season,
        season_type=season_type,
        games=result.artifacts.normalized_games,
        game_players=result.artifacts.normalized_game_players,
        source_path=normalized_games_source_path,
        source_snapshot="ingest-build",
        source_kind="nba-api",
    )
    consistency = validate_team_season_records(
        result.artifacts.normalized_games,
        result.artifacts.normalized_game_players,
        result.artifacts.wowy_games,
    )
    if consistency != "ok":
        raise TeamSeasonConsistencyError(
            message=(
                f"Inconsistent team-season cache for {team_abbreviation.upper()} "
                f"{season}: {consistency}"
            ),
            team=team_abbreviation.upper(),
            season=season,
            reason=consistency,
        )
    return result.summary


def load_player_names_from_cache(
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
) -> dict[int, str]:
    return load_cached_player_names(source_data_dir)


def extract_game_date(game_row) -> str:
    return str(game_row["GAME_DATE"])


def extract_opponent(game_row, team_abbreviation: str) -> str:
    matchup = str(game_row["MATCHUP"])
    if " vs. " in matchup:
        left, right = matchup.split(" vs. ", maxsplit=1)
    elif " @ " in matchup:
        left, right = matchup.split(" @ ", maxsplit=1)
    else:
        raise ValueError(f"Unrecognized matchup string {matchup!r}")

    if team_abbreviation == left:
        return right
    if team_abbreviation == right:
        return left
    raise ValueError(f"Failed to parse opponent from matchup {matchup!r}")


def extract_is_home(game_row, team_abbreviation: str) -> bool:
    matchup = str(game_row["MATCHUP"])
    if " vs. " in matchup:
        left, right = matchup.split(" vs. ", maxsplit=1)
        if team_abbreviation == left:
            return True
        if team_abbreviation == right:
            return False
    elif " @ " in matchup:
        left, right = matchup.split(" @ ", maxsplit=1)
        if team_abbreviation == left:
            return False
        if team_abbreviation == right:
            return True

    raise ValueError(f"Unrecognized matchup string {matchup!r}")
