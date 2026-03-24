from __future__ import annotations

from wowy.nba.models import CanonicalGamePlayerRecord, CanonicalGameRecord
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.season_types import canonicalize_season_type
from wowy.nba.source_models import SourceBoxScore, SourceBoxScorePlayer, SourceBoxScoreTeam, SourceLeagueGame
from wowy.nba.ingest.source_rules import (
    classify_source_player_row,
    format_source_row,
    format_source_rows,
    parse_box_score_numeric_value,
    parse_minutes_to_float,
    played_in_game,
)
from wowy.nba.team_identity import TeamIdentity, canonical_team_lookup_abbreviation, resolve_source_team_identity


def normalize_source_game(
    *,
    schedule_game: SourceLeagueGame,
    box_score: SourceBoxScore,
    season: str,
    season_type: str,
    source: str = "nba_api",
) -> tuple[CanonicalGameRecord, list[CanonicalGamePlayerRecord]]:
    season = canonicalize_season_string(season)
    season_type = canonicalize_season_type(season_type)
    team_stat, opponent_stat = _resolve_game_teams(schedule_game=schedule_game, box_score=box_score)

    team_identity = resolve_source_team_identity(
        team_id=team_stat.team_id,
        team_abbreviation=team_stat.team_abbreviation,
        fallback_team_id=schedule_game.team_id,
        fallback_abbreviation=schedule_game.team_abbreviation,
    )
    opponent_identity = resolve_source_team_identity(
        team_id=opponent_stat.team_id,
        team_abbreviation=opponent_stat.team_abbreviation,
    )

    player_rows = [
        player
        for player in box_score.players
        if _source_row_matches_team(
            row_team_id=player.team_id,
            row_team_abbreviation=player.team_abbreviation,
            expected_team_id=team_identity.team_id,
            expected_team_abbreviation=team_identity.abbreviation,
        )
    ]
    players = _normalize_players(
        game_id=schedule_game.game_id,
        team_identity=team_identity,
        player_rows=player_rows,
    )
    if not any(player.appeared for player in players):
        raise ValueError(
            f"No active players found for team {team_identity.abbreviation!r} in game "
            f"{schedule_game.game_id!r}; "
            f"nba_api_box_score_player_rows={format_source_rows([row.raw_row for row in player_rows])}"
        )

    margin = _resolve_margin(team_stat=team_stat, opponent_stat=opponent_stat, game_id=schedule_game.game_id)
    game = CanonicalGameRecord(
        game_id=schedule_game.game_id,
        season=season,
        game_date=schedule_game.game_date,
        team=team_identity.abbreviation,
        opponent=opponent_identity.abbreviation,
        is_home=extract_is_home(schedule_game.matchup, team_identity.abbreviation),
        margin=margin,
        season_type=season_type,
        source=source,
        team_id=team_identity.team_id,
        opponent_team_id=opponent_identity.team_id,
    )
    return game, players


def extract_opponent(matchup: str, team_abbreviation: str) -> str:
    left, right, _separator = _split_matchup(matchup)
    if _matchup_side_matches_requested_team(left, team_abbreviation):
        return right
    if _matchup_side_matches_requested_team(right, team_abbreviation):
        return left
    raise ValueError(f"Failed to parse opponent from matchup {matchup!r}")


def extract_is_home(matchup: str, team_abbreviation: str) -> bool:
    left, right, separator = _split_matchup(matchup)
    if separator == "vs.":
        if _matchup_side_matches_requested_team(left, team_abbreviation):
            return True
        if _matchup_side_matches_requested_team(right, team_abbreviation):
            return False
    else:
        if _matchup_side_matches_requested_team(left, team_abbreviation):
            return False
        if _matchup_side_matches_requested_team(right, team_abbreviation):
            return True
    raise ValueError(f"Failed to parse home/away from matchup {matchup!r}")


def _normalize_players(
    *,
    game_id: str,
    team_identity: TeamIdentity,
    player_rows: list[SourceBoxScorePlayer],
) -> list[CanonicalGamePlayerRecord]:
    players: list[CanonicalGamePlayerRecord] = []
    for row in player_rows:
        classification = classify_source_player_row(row)
        if classification.should_skip:
            continue

        player_id = row.player_id
        if player_id is None or player_id <= 0:
            raise ValueError(
                "Invalid box score player row with missing PLAYER_ID; "
                f"nba_api_box_score_player_row={format_source_row(row.raw_row)}"
            )
        minutes = parse_minutes_to_float(row.minutes_raw)
        if row.player_name.strip() == "":
            raise ValueError(
                "Invalid box score player row with blank PLAYER_NAME; "
                f"nba_api_box_score_player_row={format_source_row(row.raw_row)}"
            )
        if minutes is None:
            raise ValueError(
                "Invalid box score player row with unparseable MIN value; "
                f"nba_api_box_score_player_row={format_source_row(row.raw_row)}"
            )
        player_name = row.player_name.strip()
        players.append(
            CanonicalGamePlayerRecord(
                game_id=game_id,
                team=team_identity.abbreviation,
                player_id=player_id,
                player_name=player_name,
                appeared=played_in_game(row.minutes_raw),
                minutes=minutes,
                team_id=team_identity.team_id,
            )
        )
    return players


def _resolve_game_teams(
    *,
    schedule_game: SourceLeagueGame,
    box_score: SourceBoxScore,
) -> tuple[SourceBoxScoreTeam, SourceBoxScoreTeam]:
    matched_teams = [
        team
        for team in box_score.teams
        if _source_row_matches_team(
            row_team_id=team.team_id,
            row_team_abbreviation=team.team_abbreviation,
            expected_team_id=schedule_game.team_id,
            expected_team_abbreviation=schedule_game.team_abbreviation,
        )
    ]
    if not matched_teams:
        raise ValueError(
            f"Team {schedule_game.team_abbreviation!r} not found in box score for game "
            f"{schedule_game.game_id!r}; "
            f"nba_api_box_score_team_rows={format_source_rows([team.raw_row for team in box_score.teams])}"
        )
    if len(matched_teams) > 1:
        raise ValueError(
            f"Multiple team rows matched {schedule_game.team_abbreviation!r} for game "
            f"{schedule_game.game_id!r}; "
            f"nba_api_box_score_team_rows={format_source_rows([team.raw_row for team in matched_teams])}"
        )

    opponent_teams = [team for team in box_score.teams if team is not matched_teams[0]]
    if len(opponent_teams) != 1:
        raise ValueError(
            f"Expected exactly one opponent row in box score for game {schedule_game.game_id!r}; "
            f"found {len(opponent_teams)}; "
            f"nba_api_box_score_team_rows={format_source_rows([team.raw_row for team in box_score.teams])}"
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


def _source_row_matches_team(
    *,
    row_team_id: int | None,
    row_team_abbreviation: str,
    expected_team_id: int,
    expected_team_abbreviation: str,
) -> bool:
    if row_team_id is not None:
        return row_team_id == expected_team_id
    return canonical_team_lookup_abbreviation(row_team_abbreviation) == canonical_team_lookup_abbreviation(
        expected_team_abbreviation
    )


def _split_matchup(matchup: str) -> tuple[str, str, str]:
    raw_matchup = matchup.strip()
    if " vs. " in raw_matchup:
        left, right = raw_matchup.split(" vs. ", maxsplit=1)
        return left.strip(), right.strip(), "vs."
    if " @ " in raw_matchup:
        left, right = raw_matchup.split(" @ ", maxsplit=1)
        return left.strip(), right.strip(), "@"
    raise ValueError(f"Unrecognized matchup string {matchup!r}")


def _matchup_side_matches_requested_team(side: str, team_abbreviation: str) -> bool:
    return canonical_team_lookup_abbreviation(side) == canonical_team_lookup_abbreviation(
        team_abbreviation
    )
