from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "cache_all_seasons.py"
SPEC = importlib.util.spec_from_file_location("cache_all_seasons", SCRIPT_PATH)
assert SPEC is not None
assert SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_build_season_strings_counts_backward_inclusive() -> None:
    assert MODULE.build_season_strings(2024, 2022) == [
        "2024-25",
        "2023-24",
        "2022-23",
    ]


def test_build_season_strings_rejects_inverted_range() -> None:
    with pytest.raises(ValueError, match="greater than or equal"):
        MODULE.build_season_strings(2022, 2024)


def test_build_command_passes_through_optional_flags() -> None:
    assert MODULE.build_command(
        season="2024-25",
        season_type="Playoffs",
        teams=["BOS", "NYK"],
        skip_combine=True,
    ) == [
        MODULE.sys.executable,
        "scripts/cache_season_data.py",
        "2024-25",
        "--season-type",
        "Playoffs",
        "--teams",
        "BOS",
        "NYK",
        "--skip-combine",
    ]


def test_run_exits_cleanly_on_keyboard_interrupt(
    capsys,
    monkeypatch,
) -> None:
    def raise_keyboard_interrupt(*args, **kwargs) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(MODULE.subprocess, "run", raise_keyboard_interrupt)

    exit_code = MODULE.run(["--start-year", "2024", "--first-year", "2024"])
    captured = capsys.readouterr()

    assert exit_code == 130
    assert "Interrupted. Shutting down cleanly." in captured.err
