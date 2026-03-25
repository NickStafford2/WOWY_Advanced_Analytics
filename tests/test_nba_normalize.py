from __future__ import annotations

import pytest

from rawr_analytics.nba.normalize.normalize_game import (
    extract_is_home,
    extract_opponent,
    normalize_source_game,
)
from rawr_analytics.nba.source.models import (
    SourceBoxScore,
    SourceBoxScorePlayer,
    SourceBoxScoreTeam,
    SourceLeagueGame,
)


def test_extract_matchup_helpers_support_historical_aliases() -> None:
    assert extract_opponent("NJN @ BOS", "BKN") == "BOS"
    assert extract_is_home("NJN @ BOS", "BKN") is False
    assert extract_is_home("SEA vs. BOS", "OKC") is True


def test_normalize_source_game_uses_point_diff_when_plus_minus_missing() -> None:
    game, players = normalize_source_game(
        schedule_game=_schedule_game(),
        box_score=_box_score(team_plus_minus=None, opponent_plus_minus=None),
        season="2002-03",
        season_type="Regular Season",
    )

    assert game.margin == 5.0
    assert game.team == "MEM"
    assert game.opponent == "BOS"
    assert len(players) == 5


def test_normalize_source_game_raises_when_schedule_team_missing_from_box_score() -> None:
    with pytest.raises(ValueError, match="not found in box score"):
        normalize_source_game(
            schedule_game=_schedule_game(),
            box_score=SourceBoxScore(
                game_id="0001",
                players=[],
                teams=[
                    SourceBoxScoreTeam(
                        team_id=1610612738,
                        team_abbreviation="BOS",
                        plus_minus_raw=-5,
                        points_raw=95,
                        raw_row={"TEAM_ID": 1610612738, "TEAM_ABBREVIATION": "BOS"},
                    )
                ],
            ),
            season="2002-03",
            season_type="Regular Season",
        )


def test_normalize_source_game_raises_when_no_active_players_remain() -> None:
    with pytest.raises(ValueError, match="No active players found"):
        normalize_source_game(
            schedule_game=_schedule_game(),
            box_score=SourceBoxScore(
                game_id="0001",
                teams=_box_score().teams,
                players=[
                    SourceBoxScorePlayer(
                        game_id="0001",
                        team_id=1610612763,
                        team_abbreviation="MEM",
                        player_id=2001,
                        player_name="Inactive Player",
                        minutes_raw=None,
                        raw_row={"COMMENT": "DNP - Coach's Decision"},
                    )
                ],
            ),
            season="2002-03",
            season_type="Regular Season",
        )


def _schedule_game() -> SourceLeagueGame:
    return SourceLeagueGame(
        game_id="0001",
        game_date="2003-03-10",
        matchup="MEM vs. BOS",
        team_id=1610612763,
        team_abbreviation="MEM",
        raw_row={"GAME_ID": "0001"},
    )


def _box_score(
    *,
    team_plus_minus: object = 5,
    opponent_plus_minus: object = -5,
) -> SourceBoxScore:
    team_players = [
        SourceBoxScorePlayer(
            game_id="0001",
            team_id=1610612763,
            team_abbreviation="MEM",
            player_id=index,
            player_name=f"P{index}",
            minutes_raw="48:00",
            raw_row={"PLAYER_ID": index, "PLAYER_NAME": f"P{index}", "MIN": "48:00"},
        )
        for index in range(1, 6)
    ]
    opponent_player = SourceBoxScorePlayer(
        game_id="0001",
        team_id=1610612738,
        team_abbreviation="BOS",
        player_id=10,
        player_name="Opp",
        minutes_raw="48:00",
        raw_row={"PLAYER_ID": 10, "PLAYER_NAME": "Opp", "MIN": "48:00"},
    )
    return SourceBoxScore(
        game_id="0001",
        players=[*team_players, opponent_player],
        teams=[
            SourceBoxScoreTeam(
                team_id=1610612763,
                team_abbreviation="MEM",
                plus_minus_raw=team_plus_minus,
                points_raw=100,
                raw_row={"TEAM_ID": 1610612763, "TEAM_ABBREVIATION": "MEM", "PTS": 100},
            ),
            SourceBoxScoreTeam(
                team_id=1610612738,
                team_abbreviation="BOS",
                plus_minus_raw=opponent_plus_minus,
                points_raw=95,
                raw_row={"TEAM_ID": 1610612738, "TEAM_ABBREVIATION": "BOS", "PTS": 95},
            ),
        ],
    )
