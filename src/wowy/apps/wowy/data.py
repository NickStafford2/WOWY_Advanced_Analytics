from __future__ import annotations

from pathlib import Path
from typing import Callable

from wowy.apps.wowy.analysis import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    compute_wowy,
    compute_wowy_shrinkage_score,
    filter_results,
)
from wowy.apps.wowy.models import (
    WowyGameRecord,
    WowyPlayerSeasonRecord,
    WowyPlayerStats,
)
from wowy.data.player_metrics_db import (
    DEFAULT_PLAYER_METRICS_DB_PATH,
    PlayerSeasonMetricRow,
)
from wowy.nba.prepare import (
    prepare_canonical_scope_records,
    prepare_wowy_game_records,
)
from wowy.shared.minutes import build_player_minute_stats, passes_minute_filters


type WowyPlayerSeasonRow = dict[str, str | int | float | None]
type LoadPlayerNamesFn = Callable[[Path], dict[int, str]]

WOWY_METRIC = "wowy"
WOWY_SHRUNK_METRIC = "wowy_shrunk"


def load_player_minute_stats(
    teams: list[str] | None,
    seasons: list[str] | None,
    season_type: str = "Regular Season",
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    team_ids: list[int] | None = None,
) -> dict[int, tuple[float, float]]:
    _games, game_players = prepare_canonical_scope_records(
        teams=teams,
        seasons=seasons,
        team_ids=team_ids,
        season_type=season_type,
        player_metrics_db_path=player_metrics_db_path,
        include_opponents_for_team_scope=False,
    )
    return build_player_minute_stats(game_players)


def load_player_season_minute_stats(
    teams: list[str] | None,
    seasons: list[str] | None,
    season_type: str = "Regular Season",
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    team_ids: list[int] | None = None,
) -> dict[tuple[str, int], tuple[float, float]]:
    totals: dict[tuple[str, int], float] = {}
    counts: dict[tuple[str, int], int] = {}

    games, game_players = prepare_canonical_scope_records(
        teams=teams,
        seasons=seasons,
        team_ids=team_ids,
        season_type=season_type,
        player_metrics_db_path=player_metrics_db_path,
        include_opponents_for_team_scope=False,
    )
    seasons_by_game_id = {game.game_id: game.season for game in games}
    for player in game_players:
        season = seasons_by_game_id.get(player.game_id)
        if season is None or not player.appeared or player.minutes is None or player.minutes <= 0.0:
            continue
        key = (season, player.player_id)
        totals[key] = totals.get(key, 0.0) + player.minutes
        counts[key] = counts.get(key, 0) + 1

    return {
        key: (totals[key] / counts[key], totals[key])
        for key in totals
    }


def filter_results_by_minutes(
    results: dict[int, WowyPlayerStats],
    player_minute_stats: dict[int, tuple[float, float]] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> dict[int, WowyPlayerStats]:
    if player_minute_stats is None:
        return results
    if min_average_minutes is None and min_total_minutes is None:
        return results

    return {
        player_id: stats
        for player_id, stats in results.items()
        if passes_minute_filters(
            player_minute_stats.get(player_id),
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
    }


def attach_minute_stats(
    results: dict[int, WowyPlayerStats],
    player_minute_stats: dict[int, tuple[float, float]] | None,
) -> dict[int, WowyPlayerStats]:
    if player_minute_stats is None:
        return results

    updated = {}
    for player_id, stats in results.items():
        average_minutes, total_minutes = player_minute_stats.get(
            player_id,
            (None, None),
        )
        updated[player_id] = WowyPlayerStats(
            games_with=stats.games_with,
            games_without=stats.games_without,
            avg_margin_with=stats.avg_margin_with,
            avg_margin_without=stats.avg_margin_without,
            wowy_score=stats.wowy_score,
            average_minutes=average_minutes,
            total_minutes=total_minutes,
        )
    return updated


def build_wowy_player_season_records(
    games: list[WowyGameRecord],
    min_games_with: int,
    min_games_without: int,
    player_names: dict[int, str] | None = None,
    player_season_minute_stats: dict[tuple[str, int], tuple[float, float]] | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> list[WowyPlayerSeasonRecord]:
    player_names = player_names or {}
    games_by_season: dict[str, list[WowyGameRecord]] = {}
    for game in games:
        games_by_season.setdefault(game.season, []).append(game)

    records: list[WowyPlayerSeasonRecord] = []
    for season in sorted(games_by_season):
        results = compute_wowy(games_by_season[season])
        results = filter_results(
            results,
            min_games_with=min_games_with,
            min_games_without=min_games_without,
        )

        season_minute_stats = None
        if player_season_minute_stats is not None:
            season_minute_stats = {
                player_id: stats
                for (row_season, player_id), stats in player_season_minute_stats.items()
                if row_season == season
            }

        results = filter_results_by_minutes(
            results,
            player_minute_stats=season_minute_stats,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
        results = attach_minute_stats(results, season_minute_stats)

        ranked = sorted(
            results.items(),
            key=lambda item: item[1].wowy_score if item[1].wowy_score is not None else float("-inf"),
            reverse=True,
        )
        for player_id, stats in ranked:
            if (
                stats.avg_margin_with is None
                or stats.avg_margin_without is None
                or stats.wowy_score is None
            ):
                continue
            records.append(
                WowyPlayerSeasonRecord(
                    season=season,
                    player_id=player_id,
                    player_name=player_names.get(player_id, str(player_id)),
                    games_with=stats.games_with,
                    games_without=stats.games_without,
                    avg_margin_with=stats.avg_margin_with,
                    avg_margin_without=stats.avg_margin_without,
                    wowy_score=stats.wowy_score,
                    average_minutes=stats.average_minutes,
                    total_minutes=stats.total_minutes,
                )
            )

    return records


def serialize_wowy_player_season_records(
    records: list[WowyPlayerSeasonRecord],
) -> list[WowyPlayerSeasonRow]:
    return [
        {
            "season": record.season,
            "player_id": record.player_id,
            "player_name": record.player_name,
            "games_with": record.games_with,
            "games_without": record.games_without,
            "avg_margin_with": record.avg_margin_with,
            "avg_margin_without": record.avg_margin_without,
            "wowy_score": record.wowy_score,
            "average_minutes": record.average_minutes,
            "total_minutes": record.total_minutes,
        }
        for record in records
    ]


def available_wowy_seasons(
    records: list[WowyPlayerSeasonRecord],
) -> list[str]:
    return sorted({record.season for record in records})


def prepare_wowy_player_season_records(
    teams: list[str] | None,
    seasons: list[str] | None,
    season_type: str,
    source_data_dir: Path,
    min_games_with: int,
    min_games_without: int,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    team_ids: list[int] | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    load_player_names_fn: LoadPlayerNamesFn | None = None,
) -> list[WowyPlayerSeasonRecord]:
    del source_data_dir, load_player_names_fn
    games, player_names = prepare_wowy_game_records(
        teams=teams,
        seasons=seasons,
        team_ids=team_ids,
        season_type=season_type,
        player_metrics_db_path=player_metrics_db_path,
    )
    player_season_minute_stats = load_player_season_minute_stats(
        teams=teams,
        seasons=seasons,
        team_ids=team_ids,
        season_type=season_type,
        player_metrics_db_path=player_metrics_db_path,
    )
    return build_wowy_player_season_records(
        games,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        player_names=player_names,
        player_season_minute_stats=player_season_minute_stats,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )


def build_wowy_metric_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: str,
    source_data_dir: Path,
    db_path: Path,
    teams: list[str] | None,
    team_ids: list[int] | None,
    rawr_ridge_alpha: float,
) -> list[PlayerSeasonMetricRow]:
    del rawr_ridge_alpha
    records = prepare_wowy_player_season_records(
        teams=teams,
        team_ids=team_ids,
        seasons=None,
        season_type=season_type,
        source_data_dir=source_data_dir,
        player_metrics_db_path=db_path,
        min_games_with=0,
        min_games_without=0,
        min_average_minutes=None,
        min_total_minutes=None,
    )
    return [
        PlayerSeasonMetricRow(
            metric=WOWY_METRIC,
            metric_label="WOWY",
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type,
            season=record.season,
            player_id=record.player_id,
            player_name=record.player_name,
            value=record.wowy_score,
            sample_size=record.games_with,
            secondary_sample_size=record.games_without,
            average_minutes=record.average_minutes,
            total_minutes=record.total_minutes,
            details={
                "games_with": record.games_with,
                "games_without": record.games_without,
                "avg_margin_with": record.avg_margin_with,
                "avg_margin_without": record.avg_margin_without,
            },
        )
        for record in records
    ]


def build_wowy_shrunk_metric_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: str,
    source_data_dir: Path,
    db_path: Path,
    teams: list[str] | None,
    team_ids: list[int] | None,
    rawr_ridge_alpha: float,
) -> list[PlayerSeasonMetricRow]:
    del rawr_ridge_alpha
    records = prepare_wowy_player_season_records(
        teams=teams,
        team_ids=team_ids,
        seasons=None,
        season_type=season_type,
        source_data_dir=source_data_dir,
        player_metrics_db_path=db_path,
        min_games_with=0,
        min_games_without=0,
        min_average_minutes=None,
        min_total_minutes=None,
    )
    return [
        PlayerSeasonMetricRow(
            metric=WOWY_SHRUNK_METRIC,
            metric_label="WOWY Shrunk",
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type,
            season=record.season,
            player_id=record.player_id,
            player_name=record.player_name,
            value=compute_wowy_shrinkage_score(
                games_with=record.games_with,
                games_without=record.games_without,
                wowy_score=record.wowy_score,
                prior_games=DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
            ),
            sample_size=record.games_with,
            secondary_sample_size=record.games_without,
            average_minutes=record.average_minutes,
            total_minutes=record.total_minutes,
            details={
                "games_with": record.games_with,
                "games_without": record.games_without,
                "avg_margin_with": record.avg_margin_with,
                "avg_margin_without": record.avg_margin_without,
                "raw_wowy_score": record.wowy_score,
            },
        )
        for record in records
    ]
