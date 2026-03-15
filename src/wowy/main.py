from __future__ import annotations

from wowy.analysis import compute_wowy, filter_results
from wowy.cli import build_parser, main
from wowy.formatting import format_results_table, print_results, sort_score
from wowy.io import load_games_from_csv
from wowy.types import GameRecord, PlayerStats


__all__ = [
    "GameRecord",
    "PlayerStats",
    "load_games_from_csv",
    "compute_wowy",
    "filter_results",
    "sort_score",
    "format_results_table",
    "print_results",
    "build_parser",
    "main",
]

if __name__ == "__main__":
    raise SystemExit(main())
