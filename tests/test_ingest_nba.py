from __future__ import annotations

from pathlib import Path

import pytest

from wowy.nba.errors import PartialTeamSeasonError
from wowy.nba.ingest import ingest_team_season
from wowy.nba.ingest.normalize import normalize_source_game
from wowy.nba.ingest.parsers import (
    dedupe_schedule_games,
    parse_box_score_payload,
    parse_league_schedule_payload,
)
from wowy.nba.source_models import SourceBoxScorePlayer, SourceBoxScoreTeam, SourceLeagueGame
from wowy.nba.ingest.source_rules import (
    CANONICAL_SCHEDULE_SOURCE_ROW,
    CANONICAL_TEAM_SOURCE_ROW,
    INACTIVE_PLAYER_STATUS_ROW,
    PLAYER_DID_NOT_PLAY_PLACEHOLDER,
    classify_source_player_row,
    classify_source_schedule_row,
    classify_source_team_row,
)


SOURCE_DATA_DIR = Path("data/source/nba")


def _sample_cached_team_seasons() -> list[tuple[str, str]]:
    cache_paths = sorted(SOURCE_DATA_DIR.glob("team_seasons/*_regular_season_leaguegamefinder.json"))
    assert cache_paths, "Expected cached NBA team-season payloads under data/source/nba/team_seasons"
    latest_season = max(path.stem.split("_", maxsplit=2)[1] for path in cache_paths)
    cache_paths = [
        path
        for path in cache_paths
        if path.stem.split("_", maxsplit=2)[1] != latest_season
    ]
    sample_indices = sorted({0, len(cache_paths) // 2, len(cache_paths) - 1})
    samples: list[tuple[str, str]] = []
    for index in sample_indices:
        stem = cache_paths[index].stem.removesuffix("_leaguegamefinder")
        team, season, _season_type_slug = stem.split("_", maxsplit=2)
        samples.append((team, season))
    return samples


def _latest_cached_scope() -> tuple[str, str] | None:
    cache_paths = sorted(SOURCE_DATA_DIR.glob("team_seasons/*_regular_season_leaguegamefinder.json"))
    if not cache_paths:
        return None
    latest_path = max(cache_paths, key=lambda path: path.stem.split("_", maxsplit=2)[1])
    team, season, _season_type_slug = latest_path.stem.removesuffix("_leaguegamefinder").split(
        "_",
        maxsplit=2,
    )
    return team, season


@pytest.mark.parametrize(("team", "season"), _sample_cached_team_seasons())
def test_ingest_team_season_from_cached_nba_source(team: str, season: str) -> None:
    try:
        result = ingest_team_season(
            team_abbreviation=team,
            season=season,
            source_data_dir=SOURCE_DATA_DIR,
            log=None,
            cached_only=True,
        )
    except PartialTeamSeasonError as exc:
        assert exc.failed_games > 0
        assert exc.failed_games <= exc.total_games
        assert exc.failed_game_details
        assert "nba_api_" in exc.failed_game_details[0].message
        return

    assert result.summary.league_games_source == "cached"
    assert result.summary.total_games > 0
    assert result.summary.processed_games == result.summary.total_games
    assert result.summary.fetched_box_scores == 0
    assert result.summary.cached_box_scores == result.summary.total_games
    assert len(result.artifacts.canonical_games) == result.summary.total_games
    assert len(result.artifacts.wowy_games) == len(result.artifacts.canonical_games)
    assert len(result.artifacts.canonical_game_players) > len(result.artifacts.canonical_games) * 5


def test_latest_cached_scope_is_explicitly_reported_if_partial() -> None:
    latest_scope = _latest_cached_scope()
    if latest_scope is None:
        pytest.skip("No cached NBA team-season payloads found")

    team, season = latest_scope
    try:
        ingest_team_season(
            team_abbreviation=team,
            season=season,
            source_data_dir=SOURCE_DATA_DIR,
            log=None,
            cached_only=True,
        )
    except PartialTeamSeasonError as exc:
        assert exc.failed_games > 0
        assert exc.failed_games < exc.total_games
        return


def test_ingest_team_season_cached_only_rejects_empty_cached_box_score(
    tmp_path: Path,
) -> None:
    team_season_dir = tmp_path / "team_seasons"
    team_season_dir.mkdir(parents=True, exist_ok=True)
    (team_season_dir / "BOS_2023-24_regular_season_leaguegamefinder.json").write_text(
        """
        {
          "resultSets": [
            {
              "headers": ["GAME_ID", "GAME_DATE", "MATCHUP", "TEAM_ID", "TEAM_ABBREVIATION"],
              "rowSet": [["0001", "2024-01-01", "BOS vs. LAL", 1610612738, "BOS"]]
            }
          ]
        }
        """.strip(),
        encoding="utf-8",
    )
    boxscores_dir = tmp_path / "boxscores"
    boxscores_dir.mkdir(parents=True, exist_ok=True)
    (boxscores_dir / "0001_boxscoretraditionalv2.json").write_text(
        """
        {
          "resultSets": [
            {"name": "PlayerStats", "headers": ["A"], "rowSet": []},
            {"name": "TeamStats", "headers": ["B"], "rowSet": []}
          ]
        }
        """.strip(),
        encoding="utf-8",
    )

    with pytest.raises(PartialTeamSeasonError) as exc_info:
        ingest_team_season(
            team_abbreviation="BOS",
            season="2023-24",
            source_data_dir=tmp_path,
            log=None,
            cached_only=True,
        )

    assert exc_info.value.failed_game_ids == ["0001"]
    assert "Missing valid cached box score payload" in exc_info.value.failed_game_details[0].message
    assert not (boxscores_dir / "0001_boxscoretraditionalv2.json").exists()


def test_ingest_team_season_groups_partial_failures_by_stable_reason(
    tmp_path: Path,
) -> None:
    team_season_dir = tmp_path / "team_seasons"
    team_season_dir.mkdir(parents=True, exist_ok=True)
    (team_season_dir / "BOS_2023-24_regular_season_leaguegamefinder.json").write_text(
        """
        {
          "resultSets": [
            {
              "headers": ["GAME_ID", "GAME_DATE", "MATCHUP", "TEAM_ID", "TEAM_ABBREVIATION"],
              "rowSet": [
                ["0001", "2024-01-01", "BOS vs. LAL", 1610612738, "BOS"],
                ["0002", "2024-01-03", "BOS vs. NYK", 1610612738, "BOS"]
              ]
            }
          ]
        }
        """.strip(),
        encoding="utf-8",
    )
    boxscores_dir = tmp_path / "boxscores"
    boxscores_dir.mkdir(parents=True, exist_ok=True)
    for game_id in ("0001", "0002"):
        (boxscores_dir / f"{game_id}_boxscoretraditionalv2.json").write_text(
            """
            {
              "resultSets": [
                {"name": "PlayerStats", "headers": ["A"], "rowSet": []},
                {"name": "TeamStats", "headers": ["B"], "rowSet": []}
              ]
            }
            """.strip(),
            encoding="utf-8",
        )

    with pytest.raises(PartialTeamSeasonError) as exc_info:
        ingest_team_season(
            team_abbreviation="BOS",
            season="2023-24",
            source_data_dir=tmp_path,
            log=None,
            cached_only=True,
        )

    assert exc_info.value.failed_game_ids == ["0001", "0002"]
    assert exc_info.value.failure_reason_counts == {
        "ValueError: Missing valid cached box score payload for game <game_id>": 2
    }
    assert exc_info.value.failure_reason_examples == {
        "ValueError: Missing valid cached box score payload for game <game_id>": [
            "0001",
            "0002",
        ]
    }


def test_ingest_team_season_cached_only_rejects_empty_cached_league_games(
    tmp_path: Path,
) -> None:
    team_season_dir = tmp_path / "team_seasons"
    team_season_dir.mkdir(parents=True, exist_ok=True)
    league_games_path = team_season_dir / "BOS_2023-24_regular_season_leaguegamefinder.json"
    league_games_path.write_text(
        """
        {
          "resultSets": [
            {
              "headers": ["GAME_ID", "GAME_DATE", "MATCHUP", "TEAM_ID", "TEAM_ABBREVIATION"],
              "rowSet": []
            }
          ]
        }
        """.strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Missing valid cached league games payload"):
        ingest_team_season(
            team_abbreviation="BOS",
            season="2023-24",
            source_data_dir=tmp_path,
            log=None,
            cached_only=True,
        )

    assert not league_games_path.exists()


def test_normalize_source_game_skips_sentinel_player_id_zero_rows() -> None:
    schedule = parse_league_schedule_payload(
        {
            "resultSets": [
                {
                    "headers": ["GAME_ID", "GAME_DATE", "MATCHUP", "TEAM_ID", "TEAM_ABBREVIATION"],
                    "rowSet": [["0001", "2003-03-10", "MEM vs. BOS", 1610612763, "MEM"]],
                }
            ]
        },
        requested_team="MEM",
        season="2002-03",
        season_type="Regular Season",
    )
    box_score = parse_box_score_payload(
        {
            "resultSets": [
                {
                    "name": "PlayerStats",
                    "headers": ["GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION", "PLAYER_ID", "PLAYER_NAME", "MIN"],
                    "rowSet": [
                        ["0001", 1610612763, "MEM", 0, None, None],
                        ["0001", 1610612763, "MEM", 1, "P1", "48:00"],
                        ["0001", 1610612763, "MEM", 2, "P2", "48:00"],
                        ["0001", 1610612763, "MEM", 3, "P3", "48:00"],
                        ["0001", 1610612763, "MEM", 4, "P4", "48:00"],
                        ["0001", 1610612763, "MEM", 5, "P5", "48:00"],
                        ["0001", 1610612738, "BOS", 10, "Opp", "48:00"],
                    ],
                },
                {
                    "name": "TeamStats",
                    "headers": ["TEAM_ID", "TEAM_ABBREVIATION", "PLUS_MINUS", "PTS"],
                    "rowSet": [
                        [1610612763, "MEM", 5, 100],
                        [1610612738, "BOS", -5, 95],
                    ],
                },
            ]
        },
        game_id="0001",
    )

    game, players = normalize_source_game(
        schedule_game=schedule.games[0],
        box_score=box_score,
        season="2002-03",
        season_type="Regular Season",
    )

    assert game.team_id == 1610612763
    assert [player.player_id for player in players] == [1, 2, 3, 4, 5]


def test_normalize_source_game_skips_player_did_not_play_placeholder_rows() -> None:
    schedule = parse_league_schedule_payload(
        {
            "resultSets": [
                {
                    "headers": ["GAME_ID", "GAME_DATE", "MATCHUP", "TEAM_ID", "TEAM_ABBREVIATION"],
                    "rowSet": [["0001", "2003-03-10", "MEM vs. BOS", 1610612763, "MEM"]],
                }
            ]
        },
        requested_team="MEM",
        season="2002-03",
        season_type="Regular Season",
    )
    box_score = parse_box_score_payload(
        {
            "resultSets": [
                {
                    "name": "PlayerStats",
                    "headers": [
                        "GAME_ID",
                        "TEAM_ID",
                        "TEAM_ABBREVIATION",
                        "PLAYER_ID",
                        "PLAYER_NAME",
                        "MIN",
                        "COMMENT",
                        "AST",
                        "BLK",
                        "DREB",
                        "FG3A",
                        "FG3M",
                        "FG3_PCT",
                        "FGA",
                        "FGM",
                        "FG_PCT",
                        "FTA",
                        "FTM",
                        "FT_PCT",
                        "OREB",
                        "PF",
                        "PLUS_MINUS",
                        "PTS",
                        "REB",
                        "STL",
                        "TO",
                    ],
                    "rowSet": [
                        ["0001", 1610612763, "MEM", 1337, None, None, " ", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
                        ["0001", 1610612763, "MEM", 1, "P1", "48:00", "", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
                        ["0001", 1610612763, "MEM", 2, "P2", "48:00", "", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
                        ["0001", 1610612763, "MEM", 3, "P3", "48:00", "", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
                        ["0001", 1610612763, "MEM", 4, "P4", "48:00", "", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
                        ["0001", 1610612763, "MEM", 5, "P5", "48:00", "", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
                        ["0001", 1610612738, "BOS", 10, "Opp", "48:00", "", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
                    ],
                },
                {
                    "name": "TeamStats",
                    "headers": ["TEAM_ID", "TEAM_ABBREVIATION", "PLUS_MINUS", "PTS"],
                    "rowSet": [
                        [1610612763, "MEM", 5, 100],
                        [1610612738, "BOS", -5, 95],
                    ],
                },
            ]
        },
        game_id="0001",
    )

    game, players = normalize_source_game(
        schedule_game=schedule.games[0],
        box_score=box_score,
        season="2002-03",
        season_type="Regular Season",
    )

    assert game.team_id == 1610612763
    assert [player.player_id for player in players] == [1, 2, 3, 4, 5]


def test_classify_source_player_row_names_known_skip_patterns() -> None:
    placeholder = SourceBoxScorePlayer(
        game_id="0001",
        team_id=1610612763,
        team_abbreviation="MEM",
        player_id=1337,
        player_name="",
        minutes_raw=None,
        raw_row={
            "PLAYER_ID": 1337,
            "PLAYER_NAME": None,
            "MIN": None,
            "COMMENT": " ",
            "AST": None,
            "BLK": None,
            "DREB": None,
            "FG3A": None,
            "FG3M": None,
            "FG3_PCT": None,
            "FGA": None,
            "FGM": None,
            "FG_PCT": None,
            "FTA": None,
            "FTM": None,
            "FT_PCT": None,
            "OREB": None,
            "PF": None,
            "PLUS_MINUS": None,
            "PTS": None,
            "REB": None,
            "STL": None,
            "TO": None,
        },
    )
    inactive = SourceBoxScorePlayer(
        game_id="0001",
        team_id=1610612763,
        team_abbreviation="MEM",
        player_id=2001,
        player_name="Inactive Player",
        minutes_raw=None,
        raw_row={
            "PLAYER_ID": 2001,
            "PLAYER_NAME": "Inactive Player",
            "MIN": None,
            "COMMENT": "DNP - Coach's Decision",
            "AST": None,
            "BLK": None,
            "DREB": None,
            "FG3A": None,
            "FG3M": None,
            "FG3_PCT": None,
            "FGA": None,
            "FGM": None,
            "FG_PCT": None,
            "FTA": None,
            "FTM": None,
            "FT_PCT": None,
            "OREB": None,
            "PF": None,
            "PLUS_MINUS": None,
            "PTS": None,
            "REB": None,
            "STL": None,
            "TO": None,
        },
    )
    inactive_blank_status = SourceBoxScorePlayer(
        game_id="0001",
        team_id=1610612763,
        team_abbreviation="MEM",
        player_id=1628418,
        player_name="Thomas Bryant",
        minutes_raw=None,
        raw_row={
            "PLAYER_ID": 1628418,
            "PLAYER_NAME": "Thomas Bryant",
            "MIN": None,
            "COMMENT": "",
            "AST": None,
            "BLK": None,
            "DREB": None,
            "FG3A": None,
            "FG3M": None,
            "FG3_PCT": None,
            "FGA": None,
            "FGM": None,
            "FG_PCT": None,
            "FTA": None,
            "FTM": None,
            "FT_PCT": None,
            "OREB": None,
            "PF": None,
            "PLUS_MINUS": None,
            "PTS": None,
            "REB": None,
            "STL": None,
            "TO": None,
        },
    )

    assert classify_source_player_row(placeholder) == PLAYER_DID_NOT_PLAY_PLACEHOLDER
    assert classify_source_player_row(inactive) == INACTIVE_PLAYER_STATUS_ROW
    assert classify_source_player_row(inactive_blank_status) == INACTIVE_PLAYER_STATUS_ROW


def test_classify_source_team_row_defaults_to_canonical_team_source_row() -> None:
    team_row = SourceBoxScoreTeam(
        team_id=1610612763,
        team_abbreviation="MEM",
        plus_minus_raw=5,
        points_raw=100,
        raw_row={
            "TEAM_ID": 1610612763,
            "TEAM_ABBREVIATION": "MEM",
            "PLUS_MINUS": 5,
            "PTS": 100,
        },
    )

    assert classify_source_team_row(team_row) == CANONICAL_TEAM_SOURCE_ROW


def test_classify_source_schedule_row_defaults_to_canonical_schedule_source_row() -> None:
    schedule_row = {
        "GAME_ID": "0001",
        "GAME_DATE": "2003-03-10",
        "MATCHUP": "MEM vs. BOS",
        "TEAM_ID": 1610612763,
        "TEAM_ABBREVIATION": "MEM",
    }

    assert classify_source_schedule_row(schedule_row) == CANONICAL_SCHEDULE_SOURCE_ROW


def test_dedupe_schedule_games_raises_on_conflicting_duplicate_rows() -> None:
    with pytest.raises(ValueError, match="Conflicting duplicate schedule rows for game '0001'"):
        dedupe_schedule_games(
            [
                SourceLeagueGame(
                    game_id="0001",
                    game_date="2003-03-10",
                    matchup="MEM vs. BOS",
                    team_id=1610612763,
                    team_abbreviation="MEM",
                    raw_row={
                        "GAME_ID": "0001",
                        "GAME_DATE": "2003-03-10",
                        "MATCHUP": "MEM vs. BOS",
                        "TEAM_ID": 1610612763,
                        "TEAM_ABBREVIATION": "MEM",
                    },
                ),
                SourceLeagueGame(
                    game_id="0001",
                    game_date="2003-03-11",
                    matchup="MEM @ BOS",
                    team_id=1610612763,
                    team_abbreviation="MEM",
                    raw_row={
                        "GAME_ID": "0001",
                        "GAME_DATE": "2003-03-11",
                        "MATCHUP": "MEM @ BOS",
                        "TEAM_ID": 1610612763,
                        "TEAM_ABBREVIATION": "MEM",
                    },
                ),
            ]
        )


def test_normalize_source_game_skips_inactive_player_status_rows() -> None:
    schedule = parse_league_schedule_payload(
        {
            "resultSets": [
                {
                    "headers": ["GAME_ID", "GAME_DATE", "MATCHUP", "TEAM_ID", "TEAM_ABBREVIATION"],
                    "rowSet": [["0001", "2003-03-10", "MEM vs. BOS", 1610612763, "MEM"]],
                }
            ]
        },
        requested_team="MEM",
        season="2002-03",
        season_type="Regular Season",
    )
    box_score = parse_box_score_payload(
        {
            "resultSets": [
                {
                    "name": "PlayerStats",
                    "headers": [
                        "GAME_ID",
                        "TEAM_ID",
                        "TEAM_ABBREVIATION",
                        "PLAYER_ID",
                        "PLAYER_NAME",
                        "MIN",
                        "COMMENT",
                        "AST",
                        "BLK",
                        "DREB",
                        "FG3A",
                        "FG3M",
                        "FG3_PCT",
                        "FGA",
                        "FGM",
                        "FG_PCT",
                        "FTA",
                        "FTM",
                        "FT_PCT",
                        "OREB",
                        "PF",
                        "PLUS_MINUS",
                        "PTS",
                        "REB",
                        "STL",
                        "TO",
                    ],
                    "rowSet": [
                        ["0001", 1610612763, "MEM", 1337, "Inactive Player", None, "DNP - Coach's Decision", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
                        ["0001", 1610612763, "MEM", 1, "P1", "48:00", "", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
                        ["0001", 1610612763, "MEM", 2, "P2", "48:00", "", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
                        ["0001", 1610612763, "MEM", 3, "P3", "48:00", "", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
                        ["0001", 1610612763, "MEM", 4, "P4", "48:00", "", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
                        ["0001", 1610612763, "MEM", 5, "P5", "48:00", "", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
                        ["0001", 1610612738, "BOS", 10, "Opp", "48:00", "", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
                    ],
                },
                {
                    "name": "TeamStats",
                    "headers": ["TEAM_ID", "TEAM_ABBREVIATION", "PLUS_MINUS", "PTS"],
                    "rowSet": [
                        [1610612763, "MEM", 5, 100],
                        [1610612738, "BOS", -5, 95],
                    ],
                },
            ]
        },
        game_id="0001",
    )

    game, players = normalize_source_game(
        schedule_game=schedule.games[0],
        box_score=box_score,
        season="2002-03",
        season_type="Regular Season",
    )

    assert game.team_id == 1610612763
    assert [player.player_id for player in players] == [1, 2, 3, 4, 5]


def test_parse_box_score_payload_accepts_v3_result_set_shape() -> None:
    box_score = parse_box_score_payload(
        {
            "resultSets": {
                "PlayerStats": {
                    "headers": [
                        "gameId",
                        "teamId",
                        "teamTricode",
                        "personId",
                        "firstName",
                        "familyName",
                        "minutes",
                    ],
                    "data": [
                        ["0001", 1610612737, "ATL", 101, "Test", "Player", "12:00"],
                        ["0001", 1610612738, "BOS", 201, "Other", "Player", "DNP - COACH'S DECISION"],
                    ],
                },
                "TeamStats": {
                    "headers": [
                        "gameId",
                        "teamId",
                        "teamTricode",
                        "points",
                        "plusMinusPoints",
                    ],
                    "data": [
                        ["0001", 1610612737, "ATL", 110, 5],
                        ["0001", 1610612738, "BOS", 105, -5],
                    ],
                },
            }
        },
        game_id="0001",
    )

    assert [(player.player_id, player.player_name, player.minutes_raw) for player in box_score.players] == [
        (101, "Test Player", "12:00"),
        (201, "Other Player", "DNP - COACH'S DECISION"),
    ]
    assert [(team.team_id, team.team_abbreviation, team.points_raw) for team in box_score.teams] == [
        (1610612737, "ATL", 110),
        (1610612738, "BOS", 105),
    ]


def test_parse_box_score_payload_raises_with_raw_row_for_blank_player_name() -> None:
    with pytest.raises(ValueError, match='Missing PLAYER_NAME; nba_api_box_score_player_row=.*"PLAYER_ID": 1'):
        parse_box_score_payload(
            {
                "resultSets": [
                    {
                        "name": "PlayerStats",
                        "headers": ["GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION", "PLAYER_ID", "PLAYER_NAME", "MIN"],
                        "rowSet": [
                            ["0001", 1610612763, "MEM", 1, "", "48:00"],
                            ["0001", 1610612738, "BOS", 10, "Opp", "48:00"],
                        ],
                    },
                    {
                        "name": "TeamStats",
                        "headers": ["TEAM_ID", "TEAM_ABBREVIATION", "PLUS_MINUS", "PTS"],
                        "rowSet": [
                            [1610612763, "MEM", 5, 100],
                            [1610612738, "BOS", -5, 95],
                        ],
                    },
                ]
            },
            game_id="0001",
        )


def test_parse_box_score_payload_raises_with_raw_row_for_unparseable_minutes() -> None:
    with pytest.raises(ValueError, match='Unparseable MIN value; nba_api_box_score_player_row=.*"MIN": "bogus"'):
        parse_box_score_payload(
            {
                "resultSets": [
                    {
                        "name": "PlayerStats",
                        "headers": ["GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION", "PLAYER_ID", "PLAYER_NAME", "MIN"],
                        "rowSet": [
                            ["0001", 1610612763, "MEM", 1, "P1", "bogus"],
                            ["0001", 1610612738, "BOS", 10, "Opp", "48:00"],
                        ],
                    },
                    {
                        "name": "TeamStats",
                        "headers": ["TEAM_ID", "TEAM_ABBREVIATION", "PLUS_MINUS", "PTS"],
                        "rowSet": [
                            [1610612763, "MEM", 5, 100],
                            [1610612738, "BOS", -5, 95],
                        ],
                    },
                ]
            },
            game_id="0001",
        )


def test_parse_box_score_payload_raises_with_raw_row_for_missing_minutes_outside_known_skip_category() -> None:
    with pytest.raises(ValueError, match='Unparseable MIN value; nba_api_box_score_player_row=.*"PTS": 12'):
        parse_box_score_payload(
            {
                "resultSets": [
                    {
                        "name": "PlayerStats",
                        "headers": [
                            "GAME_ID",
                            "TEAM_ID",
                            "TEAM_ABBREVIATION",
                            "PLAYER_ID",
                            "PLAYER_NAME",
                            "MIN",
                            "PTS",
                        ],
                        "rowSet": [
                            ["0001", 1610612763, "MEM", 1, "P1", None, 12],
                            ["0001", 1610612738, "BOS", 10, "Opp", "48:00", 10],
                        ],
                    },
                    {
                        "name": "TeamStats",
                        "headers": ["TEAM_ID", "TEAM_ABBREVIATION", "PLUS_MINUS", "PTS"],
                        "rowSet": [
                            [1610612763, "MEM", 5, 100],
                            [1610612738, "BOS", -5, 95],
                        ],
                    },
                ]
            },
            game_id="0001",
        )


def test_parse_box_score_payload_skips_historical_inactive_status_rows_with_null_minutes() -> None:
    box_score = parse_box_score_payload(
        {
            "resultSets": [
                {
                    "name": "PlayerStats",
                    "headers": [
                        "GAME_ID",
                        "TEAM_ID",
                        "TEAM_ABBREVIATION",
                        "PLAYER_ID",
                        "PLAYER_NAME",
                        "COMMENT",
                        "MIN",
                        "PTS",
                    ],
                    "rowSet": [
                        ["0001", 1610612763, "MEM", 1, "Active Player", "", "48:00", 12],
                        ["0001", 1610612763, "MEM", 2, "Inactive DNT", "DNT - Sore back", None, None],
                        [
                            "0001",
                            1610612763,
                            "MEM",
                            3,
                            "Inactive Make Trip",
                            "DN Make Trip - Oral surgery",
                            None,
                            None,
                        ],
                        ["0001", 1610612738, "BOS", 10, "Opp", "", "48:00", 10],
                    ],
                },
                {
                    "name": "TeamStats",
                    "headers": ["TEAM_ID", "TEAM_ABBREVIATION", "PLUS_MINUS", "PTS"],
                    "rowSet": [
                        [1610612763, "MEM", 5, 100],
                        [1610612738, "BOS", -5, 95],
                    ],
                },
            ]
        },
        game_id="0001",
    )

    assert [(player.player_id, player.player_name, player.minutes_raw) for player in box_score.players] == [
        (1, "Active Player", "48:00"),
        (2, "Inactive DNT", None),
        (3, "Inactive Make Trip", None),
        (10, "Opp", "48:00"),
    ]


def test_parse_league_schedule_payload_raises_with_raw_row_for_conflicting_team_identity() -> None:
    with pytest.raises(
        ValueError,
        match='Conflicting source team identity values: TEAM_ABBREVIATION=\'LAL\' expected=\'MEM\' for TEAM_ID=1610612763; nba_api_league_schedule_row=',
    ):
        parse_league_schedule_payload(
            {
                "resultSets": [
                    {
                        "headers": ["GAME_ID", "GAME_DATE", "MATCHUP", "TEAM_ID", "TEAM_ABBREVIATION"],
                        "rowSet": [["0001", "2003-03-10", "MEM vs. BOS", 1610612763, "LAL"]],
                    }
                ]
            },
            requested_team="MEM",
            season="2002-03",
            season_type="Regular Season",
        )


def test_parse_league_schedule_payload_rejects_historically_wrong_team_label() -> None:
    with pytest.raises(
        ValueError,
        match='TEAM_ABBREVIATION=\'NOP\' expected=\'NOH\' for TEAM_ID=1610612740',
    ):
        parse_league_schedule_payload(
            {
                "resultSets": [
                    {
                        "headers": ["GAME_ID", "GAME_DATE", "MATCHUP", "TEAM_ID", "TEAM_ABBREVIATION"],
                        "rowSet": [["0001", "2003-03-10", "NOP vs. BOS", 1610612740, "NOP"]],
                    }
                ]
            },
            requested_team="NOP",
            season="2002-03",
            season_type="Regular Season",
        )


def test_parse_box_score_payload_raises_with_raw_row_for_conflicting_player_team_identity() -> None:
    with pytest.raises(
        ValueError,
        match='Conflicting source team identity values: TEAM_ID=1610612763 TEAM_ABBREVIATION=\'LAL\'',
    ):
        parse_box_score_payload(
            {
                "resultSets": [
                    {
                        "name": "PlayerStats",
                        "headers": ["GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION", "PLAYER_ID", "PLAYER_NAME", "MIN"],
                        "rowSet": [
                            ["0001", 1610612763, "LAL", 1, "P1", "48:00"],
                            ["0001", 1610612738, "BOS", 10, "Opp", "48:00"],
                        ],
                    },
                    {
                        "name": "TeamStats",
                        "headers": ["TEAM_ID", "TEAM_ABBREVIATION", "PLUS_MINUS", "PTS"],
                        "rowSet": [
                            [1610612763, "MEM", 5, 100],
                            [1610612738, "BOS", -5, 95],
                        ],
                    },
                ]
            },
            game_id="0001",
        )


def test_parse_box_score_payload_raises_with_raw_values_for_conflicting_team_identity() -> None:
    schedule = parse_league_schedule_payload(
        {
            "resultSets": [
                {
                    "headers": ["GAME_ID", "GAME_DATE", "MATCHUP", "TEAM_ID", "TEAM_ABBREVIATION"],
                    "rowSet": [["0001", "2003-03-10", "MEM vs. BOS", 1610612763, "MEM"]],
                }
            ]
        },
        requested_team="MEM",
        season="2002-03",
        season_type="Regular Season",
    )
    assert schedule.games[0].team_id == 1610612763

    with pytest.raises(
        ValueError,
        match="Conflicting source team identity values: TEAM_ID=1610612738 TEAM_ABBREVIATION='LAL'",
    ):
        parse_box_score_payload(
            {
                "resultSets": [
                    {
                        "name": "PlayerStats",
                        "headers": ["GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION", "PLAYER_ID", "PLAYER_NAME", "MIN"],
                        "rowSet": [
                            ["0001", 1610612763, "MEM", 1, "P1", "48:00"],
                            ["0001", 1610612763, "MEM", 2, "P2", "48:00"],
                            ["0001", 1610612763, "MEM", 3, "P3", "48:00"],
                            ["0001", 1610612763, "MEM", 4, "P4", "48:00"],
                            ["0001", 1610612763, "MEM", 5, "P5", "48:00"],
                            ["0001", 1610612738, "BOS", 10, "Opp", "48:00"],
                        ],
                    },
                    {
                        "name": "TeamStats",
                        "headers": ["TEAM_ID", "TEAM_ABBREVIATION", "PLUS_MINUS", "PTS"],
                        "rowSet": [
                            [1610612763, "MEM", 5, 100],
                            [1610612738, "LAL", -5, 95],
                        ],
                    },
                ]
            },
            game_id="0001",
        )
