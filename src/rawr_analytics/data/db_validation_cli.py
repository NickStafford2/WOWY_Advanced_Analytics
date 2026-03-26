from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from rawr_analytics.data.db_validation import (
    audit_player_metrics_db,
    render_validation_summary,
    summarize_validation_report,
)
from rawr_analytics.data.player_metrics_db.constants import DEFAULT_PLAYER_METRICS_DB_PATH

_LAST_PROGRESS_LINE_LENGTH = 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit the SQLite cache and summarize errors.")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_PLAYER_METRICS_DB_PATH,
        help="SQLite database path to audit.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of top error trends to print in text mode.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the summary as JSON.",
    )
    return parser


def write_progress_line(line: str) -> None:
    global _LAST_PROGRESS_LINE_LENGTH
    padding = max(0, _LAST_PROGRESS_LINE_LENGTH - len(line))
    sys.stderr.write(f"\r{line}{' ' * padding}")
    sys.stderr.flush()
    _LAST_PROGRESS_LINE_LENGTH = len(line)


def clear_progress_line() -> None:
    global _LAST_PROGRESS_LINE_LENGTH
    if _LAST_PROGRESS_LINE_LENGTH == 0:
        return
    sys.stderr.write(f"\r{' ' * _LAST_PROGRESS_LINE_LENGTH}\r")
    sys.stderr.flush()
    _LAST_PROGRESS_LINE_LENGTH = 0


def render_progress(current: int, total: int, label: str) -> None:
    filled = total if total == 0 else int((current / total) * 20)
    bar = "#" * filled + "-" * (20 - filled)
    write_progress_line(f"[{current:>2}/{total}] [{bar}] {label}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    progress = None if args.json else render_progress
    try:
        report = audit_player_metrics_db(args.db_path, progress=progress)
    finally:
        if progress is not None:
            clear_progress_line()
    summary = summarize_validation_report(report)
    if args.json:
        print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))
    else:
        print(render_validation_summary(summary, top_n=args.top))
    return 0 if summary.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
