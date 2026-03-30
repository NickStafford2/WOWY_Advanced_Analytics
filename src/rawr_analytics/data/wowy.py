from __future__ import annotations

from collections import defaultdict
from typing import Any

from rawr_analytics.data.game_cache.repository import load_normalized_scope_records_from_db
from rawr_analytics.data.scope_resolver import resolve_team_seasons
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.wowy import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    build_player_season_records,
    compute_wowy_shrinkage_score,
    describe_metric,
)
from rawr_analytics.metrics.wowy.models import (
    WowyGame,
    WowyPlayerContext,
    WowyPlayerSeasonRecord,
    WowyRequest,
    WowySeasonInput,
)
from rawr_analytics.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


def load_wowy_season_inputs(
    teams: list[Team] | None,
    seasons: list[Season] | None,
    *,
    season_type: SeasonType,
) -> list[WowySeasonInput]:
    team_seasons = resolve_team_seasons(teams, seasons, season_type=season_type)
    if not team_seasons:
        raise ValueError("No cached data matched the requested scope")

    games, game_players = load_normalized_scope_records_from_db(team_seasons)
    player_names = _build_player_names(game_players)
    minute_stats = _build_player_season_minute_stats(games, game_players)
    games_by_season = _derive_wowy_games_by_season(games, game_players)

    season_inputs: list[WowySeasonInput] = []
    for season in sorted(games_by_season, key=lambda item: item.id):
        player_ids = sorted(
            {
                player_id
                for game in games_by_season[season]
                for player_id in game.players
            }
        )
        season_inputs.append(
            WowySeasonInput(
                season=season,
                games=games_by_season[season],
                players=[
                    WowyPlayerContext(
                        player_id=player_id,
                        player_name=player_names.get(player_id, str(player_id)),
                        average_minutes=minute_stats.get((season, player_id), (None, None))[0],
                        total_minutes=minute_stats.get((season, player_id), (None, None))[1],
                    )
                    for player_id in player_ids
                ],
            )
        )
    return season_inputs


def prepare_wowy_player_season_records(
    *,
    teams: list[Team] | None,
    seasons: list[Season] | None,
    season_type: SeasonType,
    min_games_with: int,
    min_games_without: int,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> list[WowyPlayerSeasonRecord]:
    return build_player_season_records(
        WowyRequest(
            season_inputs=load_wowy_season_inputs(
                teams=teams,
                seasons=seasons,
                season_type=season_type,
            ),
            min_games_with=min_games_with,
            min_games_without=min_games_without,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
    )


def build_wowy_custom_query(
    metric: Metric,
    *,
    teams: list[Team] | None,
    seasons: list[Season] | None,
    season_type: SeasonType,
    min_games_with: int,
    min_games_without: int,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> dict[str, Any]:
    records = prepare_wowy_player_season_records(
        teams=teams,
        seasons=seasons,
        season_type=season_type,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    return {
        "metric": metric.value,
        "metric_label": describe_metric(metric).label,
        "rows": [_build_wowy_query_row(metric, record) for record in records],
    }


def _build_wowy_query_row(
    metric: Metric,
    record: WowyPlayerSeasonRecord,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "season": record.season,
        "player_id": record.player_id,
        "player_name": record.player_name,
        "sample_size": record.games_with,
        "secondary_sample_size": record.games_without,
        "games_with": record.games_with,
        "games_without": record.games_without,
        "avg_margin_with": record.avg_margin_with,
        "avg_margin_without": record.avg_margin_without,
        "average_minutes": record.average_minutes,
        "total_minutes": record.total_minutes,
    }
    if metric == Metric.WOWY:
        row["value"] = record.wowy_score
        return row
    if metric == Metric.WOWY_SHRUNK:
        row["value"] = compute_wowy_shrinkage_score(
            games_with=record.games_with,
            games_without=record.games_without,
            wowy_score=record.wowy_score,
            prior_games=DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
        )
        row["raw_wowy_score"] = record.wowy_score
        return row
    raise ValueError(f"Unknown WOWY metric: {metric}")


def _build_player_names(game_players: list[NormalizedGamePlayerRecord]) -> dict[int, str]:
    return {player.player_id: player.player_name for player in game_players}


def _derive_wowy_games_by_season(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> dict[Season, list[WowyGame]]:
    players_by_game_team: dict[tuple[str, int], set[int]] = defaultdict(set)
    for player in game_players:
        if not player.appeared:
            continue
        players_by_game_team[(player.game_id, player.identity_team)].add(player.player_id)

    games_by_season: dict[Season, list[WowyGame]] = defaultdict(list)
    for game in games:
        players = players_by_game_team.get((game.game_id, game.identity_team), set())
        if not players:
            raise ValueError(
                f"No appeared players found for game {game.game_id!r} and team "
                f"{game.team.abbreviation(season=game.season)!r}"
            )
        games_by_season[game.season].append(
            WowyGame(
                game_id=game.game_id,
                margin=game.margin,
                players=frozenset(players),
                team=game.team,
            )
        )
    return dict(games_by_season)


def _build_player_season_minute_stats(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> dict[tuple[Season, int], tuple[float, float]]:
    totals: dict[tuple[Season, int], float] = {}
    counts: dict[tuple[Season, int], int] = {}
    season_by_game_id = {game.game_id: game.season for game in games}
    for player in game_players:
        season = season_by_game_id.get(player.game_id)
        if season is None or not player.appeared or player.minutes is None or player.minutes <= 0.0:
            continue
        key = (season, player.player_id)
        totals[key] = totals.get(key, 0.0) + player.minutes
        counts[key] = counts.get(key, 0) + 1
    return {key: (totals[key] / counts[key], totals[key]) for key in totals}
