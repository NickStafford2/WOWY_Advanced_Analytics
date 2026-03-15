from __future__ import annotations

import csv
from pathlib import Path

import pytest

from wowy.derive_wowy import derive_wowy_games, write_wowy_games_csv
from wowy.derive_wowy_cli import main
from wowy.types import GameRecord, NormalizedGamePlayerRecord, NormalizedGameRecord


def test_derive_wowy_games_builds_legacy_records():
    games = [
        NormalizedGameRecord(
            game_id="1",
            season="2023-24",
            game_date="2024-04-01",
            team="BOS",
            opponent="MIL",
            is_home=True,
            margin=8.0,
            season_type="Regular Season",
            source="nba_api",
        )
    ]
    game_players = [
        NormalizedGamePlayerRecord(
            game_id="1",
            team="BOS",
            player_id=101,
            player_name="Player 101",
            appeared=True,
            minutes=None,
        ),
        NormalizedGamePlayerRecord(
            game_id="1",
            team="BOS",
            player_id=102,
            player_name="Player 102",
            appeared=False,
            minutes=None,
        ),
        NormalizedGamePlayerRecord(
            game_id="1",
            team="BOS",
            player_id=103,
            player_name="Player 103",
            appeared=True,
            minutes=None,
        ),
    ]

    assert derive_wowy_games(games, game_players) == [
        GameRecord(game_id="1", team="BOS", margin=8.0, players={101, 103})
    ]


def test_derive_wowy_games_rejects_missing_appeared_players():
    games = [
        NormalizedGameRecord(
            game_id="1",
            season="2023-24",
            game_date="2024-04-01",
            team="BOS",
            opponent="MIL",
            is_home=True,
            margin=8.0,
            season_type="Regular Season",
            source="nba_api",
        )
    ]

    with pytest.raises(ValueError, match="No appeared players found"):
        derive_wowy_games(games, [])


def test_write_wowy_games_csv(tmp_path: Path):
    csv_path = tmp_path / "games.csv"
    write_wowy_games_csv(
        csv_path,
        [GameRecord(game_id="1", team="BOS", margin=8.0, players={103, 101})],
    )

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows == [
        ["game_id", "team", "margin", "players"],
        ["1", "BOS", "8.0", "101;103"],
    ]


def test_derive_wowy_cli_writes_output(tmp_path: Path):
    games_csv = tmp_path / "normalized_games.csv"
    games_csv.write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,8,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    game_players_csv = tmp_path / "normalized_game_players.csv"
    game_players_csv.write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,\n"
            "1,BOS,103,Player 103,true,\n"
        ),
        encoding="utf-8",
    )
    output_path = tmp_path / "combined" / "games.csv"

    exit_code = main(
        [
            "--games-csv",
            str(games_csv),
            "--game-players-csv",
            str(game_players_csv),
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    assert output_path.exists()
