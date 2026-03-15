from __future__ import annotations

import importlib.util
from pathlib import Path


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
