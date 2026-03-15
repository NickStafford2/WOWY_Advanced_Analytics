from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "cache_season_status.py"
SPEC = importlib.util.spec_from_file_location("cache_season_status", SCRIPT_PATH)
assert SPEC is not None
assert SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def write_csv(path: Path, header: list[str], rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [",".join(header)]
    for row in rows:
        lines.append(",".join(str(value) for value in row))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_main_reports_missing_cache_for_requested_team(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(MODULE, "resolve_requested_teams", lambda team_codes: ["ATL"])

    exit_code = MODULE.main(
        [
            "2022-23",
            "--source-data-dir",
            str(tmp_path / "source"),
            "--normalized-games-dir",
            str(tmp_path / "normalized_games"),
            "--normalized-game-players-dir",
            str(tmp_path / "normalized_players"),
            "--wowy-dir",
            str(tmp_path / "wowy"),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "ATL" in captured.out
    assert "missing" in captured.out
    assert "norm_games" in captured.out
    assert "norm_players" in captured.out


def test_main_reports_current_cache_state(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(MODULE, "resolve_requested_teams", lambda team_codes: ["ATL"])

    source_dir = tmp_path / "source"
    normalized_games_dir = tmp_path / "normalized_games"
    normalized_players_dir = tmp_path / "normalized_players"
    wowy_dir = tmp_path / "wowy"

    team_season_path = source_dir / "team_seasons" / "ATL_2022-23_regular_season_leaguegamefinder.json"
    team_season_path.parent.mkdir(parents=True, exist_ok=True)
    team_season_path.write_text(
        json.dumps(
            {
                "resultSets": [
                    {
                        "headers": ["GAME_ID"],
                        "rowSet": [["0001"], ["0002"]],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    boxscores_dir = source_dir / "boxscores"
    boxscores_dir.mkdir(parents=True, exist_ok=True)
    for game_id in ("0001", "0002"):
        (boxscores_dir / f"{game_id}_boxscoretraditionalv2.json").write_text(
            json.dumps(
                {
                    "resultSets": [
                        {"headers": ["A"], "rowSet": [[1]]},
                        {"headers": ["B"], "rowSet": [[1]]},
                    ]
                }
            ),
            encoding="utf-8",
        )

    write_csv(
        normalized_games_dir / "ATL_2022-23.csv",
        MODULE.NORMALIZED_GAMES_HEADER,
        [
            ["0001", "2022-23", "2022-10-01", "ATL", "BOS", "true", "5", "Regular Season", "nba_api"],
            ["0002", "2022-23", "2022-10-03", "ATL", "NYK", "false", "-2", "Regular Season", "nba_api"],
        ],
    )
    write_csv(
        normalized_players_dir / "ATL_2022-23.csv",
        MODULE.NORMALIZED_GAME_PLAYERS_HEADER,
        [
            ["0001", "ATL", "101", "Player 101", "true", "30.0"],
            ["0002", "ATL", "102", "Player 102", "true", "32.0"],
        ],
    )
    write_csv(
        wowy_dir / "ATL_2022-23.csv",
        MODULE.WOWY_HEADER,
        [
            ["0001", "2022-23", "ATL", "5.0", "101"],
            ["0002", "2022-23", "ATL", "-2.0", "102"],
        ],
    )

    exit_code = MODULE.main(
        [
            "2022-23",
            "--source-data-dir",
            str(source_dir),
            "--normalized-games-dir",
            str(normalized_games_dir),
            "--normalized-game-players-dir",
            str(normalized_players_dir),
            "--wowy-dir",
            str(wowy_dir),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "ATL" in captured.out
    assert "ok" in captured.out
    assert "games" in captured.out
    assert "miss" in captured.out
    assert "empty" in captured.out
    assert "bad" in captured.out
    assert "ATL  ok" in captured.out
    assert " 2  2" in captured.out
    assert "current (2)" in captured.out


def test_main_reports_stale_wowy_cache(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(MODULE, "resolve_requested_teams", lambda team_codes: ["ATL"])

    source_dir = tmp_path / "source"
    normalized_games_dir = tmp_path / "normalized_games"
    normalized_players_dir = tmp_path / "normalized_players"
    wowy_dir = tmp_path / "wowy"

    team_season_path = source_dir / "team_seasons" / "ATL_2022-23_regular_season_leaguegamefinder.json"
    team_season_path.parent.mkdir(parents=True, exist_ok=True)
    team_season_path.write_text(
        json.dumps({"resultSets": [{"headers": ["GAME_ID"], "rowSet": [["0001"]]}]}),
        encoding="utf-8",
    )
    boxscores_dir = source_dir / "boxscores"
    boxscores_dir.mkdir(parents=True, exist_ok=True)
    (boxscores_dir / "0001_boxscoretraditionalv2.json").write_text(
        json.dumps(
            {
                "resultSets": [
                    {"headers": ["A"], "rowSet": [[1]]},
                    {"headers": ["B"], "rowSet": [[1]]},
                ]
            }
        ),
        encoding="utf-8",
    )

    normalized_games_path = normalized_games_dir / "ATL_2022-23.csv"
    normalized_players_path = normalized_players_dir / "ATL_2022-23.csv"
    wowy_path = wowy_dir / "ATL_2022-23.csv"

    write_csv(
        normalized_games_path,
        MODULE.NORMALIZED_GAMES_HEADER,
        [["0001", "2022-23", "2022-10-01", "ATL", "BOS", "true", "5", "Regular Season", "nba_api"]],
    )
    write_csv(
        normalized_players_path,
        MODULE.NORMALIZED_GAME_PLAYERS_HEADER,
        [["0001", "ATL", "101", "Player 101", "true", "30.0"]],
    )
    write_csv(
        wowy_path,
        MODULE.WOWY_HEADER,
        [["0001", "2022-23", "ATL", "5.0", "101"]],
    )

    os.utime(wowy_path, (1, 1))
    os.utime(normalized_games_path, (2, 2))
    os.utime(normalized_players_path, (2, 2))

    exit_code = MODULE.main(
        [
            "2022-23",
            "--source-data-dir",
            str(source_dir),
            "--normalized-games-dir",
            str(normalized_games_dir),
            "--normalized-game-players-dir",
            str(normalized_players_dir),
            "--wowy-dir",
            str(wowy_dir),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "stale (1)" in captured.out


def test_main_reports_corrupt_source_cache(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(MODULE, "resolve_requested_teams", lambda team_codes: ["ATL"])

    source_dir = tmp_path / "source"
    team_season_path = source_dir / "team_seasons" / "ATL_2022-23_regular_season_leaguegamefinder.json"
    team_season_path.parent.mkdir(parents=True, exist_ok=True)
    team_season_path.write_text("{", encoding="utf-8")

    exit_code = MODULE.main(
        [
            "2022-23",
            "--source-data-dir",
            str(source_dir),
            "--normalized-games-dir",
            str(tmp_path / "normalized_games"),
            "--normalized-game-players-dir",
            str(tmp_path / "normalized_players"),
            "--wowy-dir",
            str(tmp_path / "wowy"),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "ATL" in captured.out
    assert "corrupt" in captured.out
