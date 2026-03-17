from __future__ import annotations

import importlib.util
from pathlib import Path

from wowy.nba.build_models import TeamSeasonRunSummary


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "cache_season_data.py"
SPEC = importlib.util.spec_from_file_location("cache_season_data", SCRIPT_PATH)
assert SPEC is not None
assert SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_run_exits_cleanly_on_keyboard_interrupt(
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.setattr(MODULE, "resolve_teams", lambda team_codes: ["ATL"])

    def raise_keyboard_interrupt(**kwargs) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(MODULE, "write_team_season_games_csv", raise_keyboard_interrupt)

    exit_code = MODULE.run(["2022-23"])
    captured = capsys.readouterr()

    assert exit_code == 130
    assert "Interrupted. Shutting down cleanly." in captured.err


def test_main_prints_team_summary(capsys, monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(MODULE, "resolve_teams", lambda team_codes: ["ATL"])
    monkeypatch.setattr(MODULE, "combine_normalized_files", lambda **kwargs: None)
    monkeypatch.setattr(MODULE, "combine_wowy_csvs", lambda *args, **kwargs: None)
    monkeypatch.setattr(MODULE, "DEFAULT_WOWY_GAMES_DIR", tmp_path / "wowy")
    monkeypatch.setattr(MODULE, "DEFAULT_NORMALIZED_GAMES_DIR", tmp_path / "games")
    monkeypatch.setattr(
        MODULE,
        "DEFAULT_NORMALIZED_GAME_PLAYERS_DIR",
        tmp_path / "game_players",
    )

    def fake_write_team_season_games_csv(**kwargs):
        csv_path = kwargs["csv_path"]
        normalized_games_path = kwargs["normalized_games_csv_path"]
        normalized_game_players_path = kwargs["normalized_game_players_csv_path"]
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        normalized_games_path.parent.mkdir(parents=True, exist_ok=True)
        normalized_game_players_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_text("game_id,season,team,margin,players\n", encoding="utf-8")
        normalized_games_path.write_text("game_id\n", encoding="utf-8")
        normalized_game_players_path.write_text("game_id\n", encoding="utf-8")
        return TeamSeasonRunSummary(
            team="ATL",
            season="2022-23",
            season_type="Regular Season",
            league_games_source="cached",
            total_games=82,
            processed_games=80,
            skipped_games=2,
            fetched_box_scores=3,
            cached_box_scores=77,
        )

    monkeypatch.setattr(MODULE, "write_team_season_games_csv", fake_write_team_season_games_csv)

    exit_code = MODULE.main(["2022-23"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "[ 1/1] ATL 2022-23 80/82" in captured.out
    assert "league=cached" in captured.out
    assert "boxscores=3 fetched, 77 cached" in captured.out
    assert "skipped=2" in captured.out


def test_main_reports_consistency_failure_cleanly(capsys, monkeypatch) -> None:
    monkeypatch.setattr(MODULE, "resolve_teams", lambda team_codes: ["ATL"])

    def raise_consistency_error(**kwargs) -> None:
        raise ValueError("Inconsistent team-season cache for ATL 2022-23: wowy_data")

    monkeypatch.setattr(MODULE, "write_team_season_games_csv", raise_consistency_error)

    exit_code = MODULE.main(["2022-23", "--skip-combine"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "[ 1/1] ATL 2022-23 failed consistency=wowy_data" in captured.out
    assert "Inconsistent cache for ATL 2022-23: wowy_data" in captured.err


def test_main_uses_season_type_specific_paths_for_playoffs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(MODULE, "resolve_teams", lambda team_codes: ["ATL"])
    monkeypatch.setattr(MODULE, "combine_normalized_files", lambda **kwargs: None)
    monkeypatch.setattr(MODULE, "combine_wowy_csvs", lambda *args, **kwargs: None)
    monkeypatch.setattr(MODULE, "DEFAULT_WOWY_GAMES_DIR", tmp_path / "wowy")
    monkeypatch.setattr(MODULE, "DEFAULT_NORMALIZED_GAMES_DIR", tmp_path / "games")
    monkeypatch.setattr(
        MODULE,
        "DEFAULT_NORMALIZED_GAME_PLAYERS_DIR",
        tmp_path / "game_players",
    )

    captured_paths: dict[str, Path] = {}

    def fake_write_team_season_games_csv(**kwargs):
        captured_paths["csv_path"] = kwargs["csv_path"]
        captured_paths["normalized_games_csv_path"] = kwargs["normalized_games_csv_path"]
        captured_paths["normalized_game_players_csv_path"] = kwargs[
            "normalized_game_players_csv_path"
        ]
        return TeamSeasonRunSummary(
            team="ATL",
            season="2022-23",
            season_type="Playoffs",
            league_games_source="cached",
            total_games=6,
            processed_games=6,
            skipped_games=0,
            fetched_box_scores=0,
            cached_box_scores=6,
        )

    monkeypatch.setattr(MODULE, "write_team_season_games_csv", fake_write_team_season_games_csv)

    exit_code = MODULE.main(["2022-23", "--season-type", "Playoffs", "--skip-combine"])

    assert exit_code == 0
    assert captured_paths["csv_path"].name == "ATL_2022-23_playoffs.csv"
    assert (
        captured_paths["normalized_games_csv_path"].name
        == "ATL_2022-23_playoffs.csv"
    )
    assert (
        captured_paths["normalized_game_players_csv_path"].name
        == "ATL_2022-23_playoffs.csv"
    )
