from __future__ import annotations

import csv
from pathlib import Path

import pytest

from wowy.derive_wowy import derive_wowy_games, write_wowy_games_csv
from wowy.types import (
    NormalizedGamePlayerRecord,
    NormalizedGameRecord,
    WowyGameRecord,
)


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
        WowyGameRecord(
            game_id="1",
            season="2023-24",
            team="BOS",
            margin=8.0,
            players={101, 103},
        )
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
        [
            WowyGameRecord(
                game_id="1",
                season="2023-24",
                team="BOS",
                margin=8.0,
                players={103, 101},
            )
        ],
    )

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows == [
        ["game_id", "season", "team", "margin", "players"],
        ["1", "2023-24", "BOS", "8.0", "101;103"],
    ]
    assert not list(tmp_path.glob("*.tmp-*"))
