from __future__ import annotations

from pathlib import Path

import pytest

from wowy.normalized_io import (
    load_normalized_game_players_from_csv,
    load_normalized_games_from_csv,
    write_normalized_game_players_csv,
    write_normalized_games_csv,
)
from wowy.types import NormalizedGamePlayerRecord, NormalizedGameRecord


def test_write_and_load_normalized_games_csv(tmp_path: Path):
    csv_path = tmp_path / "games.csv"
    games = [
        NormalizedGameRecord(
            game_id="1",
            season="2023-24",
            game_date="2024-04-01",
            team="BOS",
            opponent="MIL",
            is_home=True,
            margin=12.0,
            season_type="Regular Season",
            source="nba_api",
        )
    ]

    write_normalized_games_csv(csv_path, games)

    assert load_normalized_games_from_csv(csv_path) == games
    assert not list(tmp_path.glob("*.tmp-*"))


def test_write_and_load_normalized_game_players_csv(tmp_path: Path):
    csv_path = tmp_path / "game_players.csv"
    players = [
        NormalizedGamePlayerRecord(
            game_id="1",
            team="BOS",
            player_id=1628369,
            player_name="Jayson Tatum",
            appeared=True,
            minutes=None,
        )
    ]

    write_normalized_game_players_csv(csv_path, players)

    assert load_normalized_game_players_from_csv(csv_path) == players
    assert not list(tmp_path.glob("*.tmp-*"))


def test_load_normalized_games_rejects_invalid_is_home(tmp_path: Path):
    csv_path = tmp_path / "games.csv"
    csv_path.write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,yes,10,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid is_home"):
        load_normalized_games_from_csv(csv_path)


def test_load_normalized_game_players_rejects_invalid_minutes(tmp_path: Path):
    csv_path = tmp_path / "game_players.csv"
    csv_path.write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,1628369,Jayson Tatum,true,thirty\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid minutes"):
        load_normalized_game_players_from_csv(csv_path)
