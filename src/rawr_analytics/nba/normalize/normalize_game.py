from __future__ import annotations

from rawr_analytics.nba.normalize.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.nba.source.models import (
    SourceBoxScore,
    SourceBoxScorePlayer,
    SourceBoxScoreTeam,
    SourceLeagueGame,
)
from rawr_analytics.nba.source.rules import (
    classify_source_player_row,
    format_source_row,
    format_source_rows,
    parse_box_score_numeric_value,
    parse_minutes_to_float,
    source_player_played_in_game,
)
from rawr_analytics.shared.player import PlayerSummary
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


def normalize_source_league_game(
    *,
    source_league_game: SourceLeagueGame,
    box_score: SourceBoxScore,
    season: Season,
    source: str = "nba_api",  # todo remove
) -> tuple[NormalizedGameRecord, list[NormalizedGamePlayerRecord]]:
    team_stat, opponent_stat = _resolve_game_teams(
        schedule_game=source_league_game,
        box_score=box_score,
    )
    player_rows = [
        player for player in box_score.players if player.team.team_id == team_stat.team.team_id
    ]

    players = _normalize_players(
        game_id=source_league_game.game_id,
        team=team_stat.team,
        player_rows=player_rows,
    )
    if not any(player.appeared for player in players):
        team_abbreviation = team_stat.team.abbreviation(season=season)
        raise ValueError(
            f"No active players found for team {team_abbreviation!r} in game "
            f"{source_league_game.game_id!r}; "
            "nba_api_box_score_player_rows="
            f"{format_source_rows([row.raw_row for row in player_rows])}"
        )

    margin = _resolve_margin(
        team_stat=team_stat,
        opponent_stat=opponent_stat,
        game_id=source_league_game.game_id,
    )
    game = NormalizedGameRecord(
        game_id=source_league_game.game_id,
        season=season,
        game_date=source_league_game.game_date,
        is_home=_extract_is_home(
            source_league_game.matchup, team_stat.team.abbreviation(season=season)
        ),
        margin=margin,
        source=source,
        team=team_stat.team,
        opponent_team=opponent_stat.team,
    )
    return game, players


def _extract_opponent(matchup: str, team_abbreviation: str) -> str:
    left, right, _ = _split_matchup(matchup)
    if _validate_matchup(left, team_abbreviation):
        return right
    if _validate_matchup(right, team_abbreviation):
        return left
    raise ValueError(f"Failed to parse opponent from matchup {matchup!r}")


def _extract_is_home(matchup: str, team_abbreviation: str) -> bool:
    left, right, separator = _split_matchup(matchup)
    if separator == "vs.":
        if _validate_matchup(left, team_abbreviation):
            return True
        if _validate_matchup(right, team_abbreviation):
            return False
    else:
        if _validate_matchup(left, team_abbreviation):
            return False
        if _validate_matchup(right, team_abbreviation):
            return True
    raise ValueError(f"Failed to parse home/away from matchup {matchup!r}")


def _normalize_players(
    *,
    game_id: str,
    team: Team,
    player_rows: list[SourceBoxScorePlayer],
) -> list[NormalizedGamePlayerRecord]:
    players: list[NormalizedGamePlayerRecord] = []
    for row in player_rows:
        classification = classify_source_player_row(row)
        if classification.should_skip:
            continue

        player = row.player
        if player is None or player.player_id <= 0:
            raise ValueError(
                "Invalid box score player row with missing PLAYER_ID; "
                f"nba_api_box_score_player_row={format_source_row(row.raw_row)}"
            )
        minutes = parse_minutes_to_float(row.minutes_raw)
        player_name = player.player_name.strip()
        if player_name == "":
            raise ValueError(
                "Invalid box score player row with blank PLAYER_NAME; "
                f"nba_api_box_score_player_row={format_source_row(row.raw_row)}"
            )
        if minutes is None:
            raise ValueError(
                "Invalid box score player row with unparseable MIN value; "
                f"nba_api_box_score_player_row={format_source_row(row.raw_row)}"
            )
        assert source_player_played_in_game(row)
        players.append(
            NormalizedGamePlayerRecord(
                game_id=game_id,
                player=PlayerSummary(
                    player_id=player.player_id,
                    player_name=player_name,
                ),
                appeared=True,
                minutes=minutes,
                team=team,
            )
        )
    return players


def _resolve_game_teams(
    *,
    schedule_game: SourceLeagueGame,
    box_score: SourceBoxScore,
) -> tuple[SourceBoxScoreTeam, SourceBoxScoreTeam]:
    matched_teams = [
        team for team in box_score.teams if Team.are_same(team.team, schedule_game.team)
    ]

    if not matched_teams:
        raise ValueError(
            f"Team {schedule_game.team.abbreviation(game_date=schedule_game.game_date)!r} "
            f"not found in box score for game "
            f"{schedule_game.game_id!r}; "
            "nba_api_box_score_team_rows="
            f"{format_source_rows([team.raw_row for team in box_score.teams])}"
        )
    if len(matched_teams) > 1:
        raise ValueError(
            f"Multiple team rows matched "
            f"{schedule_game.team.abbreviation(game_date=schedule_game.game_date)!r} for game "
            f"{schedule_game.game_id!r}; "
            "nba_api_box_score_team_rows="
            f"{format_source_rows([team.raw_row for team in matched_teams])}"
        )

    opponent_teams = [team for team in box_score.teams if team is not matched_teams[0]]
    if len(opponent_teams) != 1:
        raise ValueError(
            "Expected exactly one opponent row in box score for game "
            f"{schedule_game.game_id!r}; "
            f"found {len(opponent_teams)}; "
            "nba_api_box_score_team_rows="
            f"{format_source_rows([team.raw_row for team in box_score.teams])}"
        )
    return matched_teams[0], opponent_teams[0]


def _resolve_margin(
    *,
    team_stat: SourceBoxScoreTeam,
    opponent_stat: SourceBoxScoreTeam,
    game_id: str,
) -> float:
    plus_minus = parse_box_score_numeric_value(team_stat.plus_minus_raw)
    if plus_minus is not None:
        return plus_minus

    team_points = parse_box_score_numeric_value(team_stat.points_raw)
    opponent_points = parse_box_score_numeric_value(opponent_stat.points_raw)
    if team_points is None or opponent_points is None:
        raise ValueError(
            f"Could not derive margin from team stats for game {game_id!r}; "
            f"nba_api_team_row={format_source_row(team_stat.raw_row)} "
            f"nba_api_opponent_team_row={format_source_row(opponent_stat.raw_row)}"
        )
    return team_points - opponent_points


def _split_matchup(matchup: str) -> tuple[str, str, str]:
    raw_matchup = matchup.strip()
    if " vs. " in raw_matchup:
        left, right = raw_matchup.split(" vs. ", maxsplit=1)
        return left.strip(), right.strip(), "vs."
    if " @ " in raw_matchup:
        left, right = raw_matchup.split(" @ ", maxsplit=1)
        return left.strip(), right.strip(), "@"
    raise ValueError(f"Unrecognized matchup string {matchup!r}")


def _validate_matchup(side: str, team_abbreviation: str) -> bool:
    return side == team_abbreviation


__all__ = [
    "_extract_is_home",
    "normalize_source_league_game",
]
