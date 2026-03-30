from __future__ import annotations

from rawr_analytics.data.player_metrics_db.builders import build_wowy_player_season_metric_rows
from rawr_analytics.data.player_metrics_db.models import (
    PlayerSeasonMetricRow,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.wowy.analysis import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    compute_wowy,
    compute_wowy_shrinkage_score,
    filter_results,
)
from rawr_analytics.metrics.wowy.inputs import load_wowy_game_records
from rawr_analytics.metrics.wowy.minutes import (
    attach_minute_stats,
    filter_results_by_minutes,
    load_player_season_minute_stats,
)
from rawr_analytics.metrics.wowy.models import (
    WowyGameRecord,
    WowyPlayerSeasonRecord,
)
from rawr_analytics.shared.season import SeasonType

type WowyPlayerSeasonRow = dict[str, str | int | float | None]

__all__ = [
    "WowyPlayerSeasonRow",
    "available_wowy_seasons",
    "build_wowy_metric_rows",
    "build_wowy_player_season_records",
    "build_wowy_span_chart_rows",
    "build_wowy_shrunk_metric_rows",
    "prepare_wowy_player_season_records",
    "serialize_wowy_player_season_records",
]


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
            key=lambda item: item[1].wowy_score
            if item[1].wowy_score is not None
            else float("-inf"),
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


def build_wowy_span_chart_rows(
    records: list[WowyPlayerSeasonRecord],
    *,
    start_season: str,
    end_season: str,
    top_n: int = 30,
) -> list[dict[str, str | int | float | list[dict[str, str | float | None]]]]:
    if top_n <= 0:
        raise ValueError("top_n must be positive")
    if start_season > end_season:
        raise ValueError("start_season must be less than or equal to end_season")

    span_records = [record for record in records if start_season <= record.season <= end_season]
    if not span_records:
        return []

    score_totals: dict[int, float] = {}
    season_counts: dict[int, int] = {}
    player_names: dict[int, str] = {}
    season_scores: dict[int, dict[str, float]] = {}

    for record in span_records:
        score_totals[record.player_id] = score_totals.get(record.player_id, 0.0) + record.wowy_score
        season_counts[record.player_id] = season_counts.get(record.player_id, 0) + 1
        player_names[record.player_id] = record.player_name
        season_scores.setdefault(record.player_id, {})[record.season] = record.wowy_score

    ranked_player_ids = sorted(
        score_totals,
        key=lambda player_id: (
            score_totals[player_id],
            player_names[player_id],
        ),
        reverse=True,
    )[:top_n]
    seasons = sorted({record.season for record in span_records})
    span_length = len(seasons)

    return [
        {
            "player_id": player_id,
            "player_name": player_names[player_id],
            "span_average_value": score_totals[player_id] / span_length,
            "season_count": season_counts[player_id],
            "points": [
                {
                    "season": season,
                    "value": season_scores[player_id].get(season),
                }
                for season in seasons
            ],
        }
        for player_id in ranked_player_ids
    ]


def prepare_wowy_player_season_records(
    teams: list[str] | None,
    seasons: list[str] | None,
    season_type: SeasonType,
    min_games_with: int,
    min_games_without: int,
    team_ids: list[int] | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> list[WowyPlayerSeasonRecord]:
    games, player_names = load_wowy_game_records(
        teams=teams,
        seasons=seasons,
        team_ids=team_ids,
        season_type=season_type,
    )
    player_season_minute_stats = load_player_season_minute_stats(
        teams=teams,
        seasons=seasons,
        team_ids=team_ids,
        season_type=season_type,
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
    season_type: SeasonType,
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
        min_games_with=0,
        min_games_without=0,
        min_average_minutes=None,
        min_total_minutes=None,
    )
    return build_wowy_player_season_metric_rows(
        scope_key=scope_key,
        team_filter=team_filter,
        season_type=season_type,
        metric=Metric.WOWY,
        metric_label="WOWY",
        records=records,
    )


def build_wowy_shrunk_metric_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: SeasonType,
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
        min_games_with=0,
        min_games_without=0,
        min_average_minutes=None,
        min_total_minutes=None,
    )
    values_by_player_season = {
        (record.season, record.player_id): compute_wowy_shrinkage_score(
            games_with=record.games_with,
            games_without=record.games_without,
            wowy_score=record.wowy_score,
            prior_games=DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
        )
        for record in records
    }
    return build_wowy_player_season_metric_rows(
        scope_key=scope_key,
        team_filter=team_filter,
        season_type=season_type,
        metric=Metric.WOWY_SHRUNK,
        metric_label="WOWY Shrunk",
        records=records,
        values_by_player_season=values_by_player_season,
        include_raw_wowy_score=True,
    )
