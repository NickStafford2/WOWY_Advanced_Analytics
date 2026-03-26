from __future__ import annotations

import re
from pathlib import Path
from typing import Callable

from rawr_analytics.data.game_cache.repository import replace_team_season_normalized_rows
from rawr_analytics.data.player_metrics_db.constants import DEFAULT_PLAYER_METRICS_DB_PATH
from rawr_analytics.nba.build_models import (
    TeamSeasonArtifacts,
    TeamSeasonBuildResult,
    TeamSeasonRunSummary,
)
from rawr_analytics.nba.errors import (
    GameNormalizationFailure,
    PartialTeamSeasonError,
)
from rawr_analytics.nba.models import (
    NormalizedGamePlayerRecord,
    NormalizedGameRecord,
    NormalizedTeamSeasonBatch,
)
from rawr_analytics.nba.normalize.normalize_game import normalize_source_game
from rawr_analytics.nba.normalize.validation import validate_normalized_team_season_batch
from rawr_analytics.nba.season_types import canonicalize_season_type
from rawr_analytics.nba.seasons import canonicalize_season_string
from rawr_analytics.nba.source.cache import (
    DEFAULT_SOURCE_DATA_DIR,
    box_score_cache_paths,
    box_score_payload_is_empty,
    league_games_cache_path,
    league_games_payload_is_valid,
    load_cached_payload,
    load_or_fetch_box_score_with_source,
    load_or_fetch_league_games_with_source,
)
from rawr_analytics.nba.source.load import (
    load_player_names_from_cache as load_cached_player_names,
)
from rawr_analytics.nba.source.parsers import (
    dedupe_schedule_games,
    parse_box_score_payload,
    parse_league_schedule_payload,
)
from rawr_analytics.nba.team_identity import resolve_team_id

ProgressFn = Callable[[dict], None]


def season_type_slug(season_type: str) -> str:
    return canonicalize_season_type(season_type).lower().replace(" ", "_")


def load_normalized_team_season_records(
    team_abbreviation: str,
    season: str,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    log: Callable[[str], None] | None = print,
    progress: ProgressFn | None = None,
    cached_only: bool = False,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    result = ingest_team_season(
        team_abbreviation=team_abbreviation,
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
        log=log,
        progress=progress,
        cached_only=cached_only,
    )
    return result.artifacts.normalized_games, result.artifacts.normalized_game_players


def ingest_team_season(
    team_abbreviation: str,
    season: str,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    log: Callable[[str], None] | None = print,
    progress: ProgressFn | None = None,
    cached_only: bool = False,
) -> TeamSeasonBuildResult:
    season = canonicalize_season_string(season)
    season_type = canonicalize_season_type(season_type)
    requested_team = team_abbreviation.strip().upper()
    requested_team_id = resolve_team_id(requested_team, season=season)

    league_games_payload, league_games_source = _load_team_season_payload(
        team_abbreviation=requested_team,
        team_id=requested_team_id,
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
        log=log,
        cached_only=cached_only,
    )
    schedule = parse_league_schedule_payload(
        league_games_payload,
        requested_team=requested_team,
        season=season,
        season_type=season_type,
    )
    schedule_games = dedupe_schedule_games(schedule.games)
    if not schedule_games:
        return TeamSeasonBuildResult(
            artifacts=TeamSeasonArtifacts([], []),
            summary=TeamSeasonRunSummary(
                team=requested_team,
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
    failed_game_ids: list[str] = []
    failed_game_details: list[GameNormalizationFailure] = []
    failure_reason_counts: dict[str, int] = {}
    failure_reason_examples: dict[str, list[str]] = {}
    fetched_box_scores = 0
    cached_box_scores = 0

    total_games = len(schedule_games)
    for game_index, schedule_game in enumerate(schedule_games, start=1):
        try:
            box_score_payload, box_score_source = _load_box_score_payload(
                game_id=schedule_game.game_id,
                source_data_dir=source_data_dir,
                log=log,
                cached_only=cached_only,
            )
            parsed_box_score = parse_box_score_payload(
                box_score_payload,
                game_id=schedule_game.game_id,
            )
            normalized_game, game_players = normalize_source_game(
                schedule_game=schedule_game,
                box_score=parsed_box_score,
                season=season,
                season_type=season_type,
            )
        except ValueError as exc:
            _record_game_failure(
                game_id=schedule_game.game_id,
                exc=exc,
                failed_game_ids=failed_game_ids,
                failed_game_details=failed_game_details,
                failure_reason_counts=failure_reason_counts,
                failure_reason_examples=failure_reason_examples,
            )
            if log is not None:
                log(f"failed game {schedule_game.game_id} {requested_team} {season} reason={exc}")
            if progress is not None:
                progress(
                    {
                        "team": requested_team,
                        "season": season,
                        "game_id": schedule_game.game_id,
                        "current": game_index,
                        "total": total_games,
                        "status": "failed",
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
                    "team": requested_team,
                    "season": season,
                    "game_id": schedule_game.game_id,
                    "current": game_index,
                    "total": total_games,
                    "status": "ok",
                }
            )

    if failed_game_ids:
        raise PartialTeamSeasonError(
            message=(
                f"Incomplete team-season ingest for {requested_team} {season} {season_type}: "
                f"{len(failed_game_ids)}/{total_games} games failed normalization"
            ),
            team=requested_team,
            season=season,
            season_type=season_type,
            failed_game_ids=failed_game_ids,
            total_games=total_games,
            failed_games=len(failed_game_ids),
            failed_game_details=failed_game_details,
            failure_reason_counts=dict(sorted(failure_reason_counts.items())),
            failure_reason_examples={
                reason: examples[:] for reason, examples in sorted(failure_reason_examples.items())
            },
        )

    batch = NormalizedTeamSeasonBatch(
        team=requested_team,
        team_id=requested_team_id,
        season=season,
        season_type=season_type,
        games=normalized_games,
        game_players=normalized_game_players,
    )
    validate_normalized_team_season_batch(batch)
    return TeamSeasonBuildResult(
        artifacts=TeamSeasonArtifacts(
            normalized_games=normalized_games,
            normalized_game_players=normalized_game_players,
        ),
        summary=TeamSeasonRunSummary(
            team=requested_team,
            season=season,
            season_type=season_type,
            league_games_source=league_games_source,
            total_games=total_games,
            processed_games=len(normalized_games),
            skipped_games=0,
            fetched_box_scores=fetched_box_scores,
            cached_box_scores=cached_box_scores,
        ),
    )


def refresh_normalized_team_season_cache(
    team_abbreviation: str,
    season: str,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    log: Callable[[str], None] | None = print,
    progress: ProgressFn | None = None,
    cached_only: bool = False,
) -> TeamSeasonRunSummary:
    season = canonicalize_season_string(season)
    season_type = canonicalize_season_type(season_type)
    result = ingest_team_season(
        team_abbreviation=team_abbreviation,
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
        log=log,
        progress=progress,
        cached_only=cached_only,
    )
    team = team_abbreviation.upper()
    team_id = resolve_team_id(team, season=season)
    replace_team_season_normalized_rows(
        player_metrics_db_path,
        team=team,
        team_id=team_id,
        season=season,
        season_type=season_type,
        games=result.artifacts.normalized_games,
        game_players=result.artifacts.normalized_game_players,
        source_path=f"sqlite://normalized_games/{team}_{season}_{season_type_slug(season_type)}",
        source_snapshot="ingest-build",
        source_kind="nba-api",
        expected_games_row_count=result.summary.total_games,
        skipped_games_row_count=result.summary.skipped_games,
    )
    return result.summary


def load_player_names_from_cache(
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
) -> dict[int, str]:
    return load_cached_player_names(source_data_dir)


def _load_team_season_payload(
    *,
    team_abbreviation: str,
    team_id: int,
    season: str,
    season_type: str,
    source_data_dir: Path,
    log: Callable[[str], None] | None,
    cached_only: bool,
) -> tuple[dict, str]:
    if not cached_only:
        return load_or_fetch_league_games_with_source(
            team_id=team_id,
            team_abbreviation=team_abbreviation,
            season=season,
            season_type=season_type,
            source_data_dir=source_data_dir,
            log=log,
        )

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
    if cached_payload is None:
        raise ValueError(f"Missing valid cached league games payload: {cache_path}")
    return cached_payload, "cached"


def _load_box_score_payload(
    *,
    game_id: str,
    source_data_dir: Path,
    log: Callable[[str], None] | None,
    cached_only: bool,
) -> tuple[dict, str]:
    if not cached_only:
        return load_or_fetch_box_score_with_source(
            game_id=game_id,
            source_data_dir=source_data_dir,
            log=log,
        )

    for cache_path in box_score_cache_paths(game_id, source_data_dir=source_data_dir):
        cached_payload = load_cached_payload(
            cache_path,
            validator=lambda payload: not box_score_payload_is_empty(payload),
            log=log,
        )
        if cached_payload is not None:
            return cached_payload, "cached"
    raise ValueError(f"Missing valid cached box score payload for game {game_id!r}")


def _record_game_failure(
    *,
    game_id: str,
    exc: Exception,
    failed_game_ids: list[str],
    failed_game_details: list[GameNormalizationFailure],
    failure_reason_counts: dict[str, int],
    failure_reason_examples: dict[str, list[str]],
) -> None:
    failed_game_ids.append(game_id)
    failure = GameNormalizationFailure(
        game_id=game_id,
        error_type=type(exc).__name__,
        message=str(exc),
    )
    failed_game_details.append(failure)
    reason_key = _summarize_game_failure_reason(failure)
    failure_reason_counts[reason_key] = failure_reason_counts.get(reason_key, 0) + 1
    failure_reason_examples.setdefault(reason_key, [])
    if len(failure_reason_examples[reason_key]) < 5:
        failure_reason_examples[reason_key].append(game_id)


def _summarize_game_failure_reason(failure: GameNormalizationFailure) -> str:
    message = failure.message.split("; nba_api_", maxsplit=1)[0]
    message = re.sub(r"game ['\"][^'\"]+['\"]", "game <game_id>", message)
    return f"{failure.error_type}: {message}"


__all__ = [
    "ProgressFn",
    "refresh_normalized_team_season_cache",
    "load_normalized_team_season_records",
    "ingest_team_season",
    "load_player_names_from_cache",
    "season_type_slug",
]
