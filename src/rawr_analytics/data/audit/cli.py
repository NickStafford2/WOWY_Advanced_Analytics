from __future__ import annotations

import argparse
import json
import sys

from rawr_analytics.data.audit.audit import audit_player_metrics_db
from rawr_analytics.data.audit.reporting import (
    render_validation_summary,
    summarize_validation_report,
)

_last_progress_line_length = 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit the SQLite cache and summarize errors.")
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


def _write_progress_line(line: str) -> None:
    global _last_progress_line_length
    padding = max(0, _last_progress_line_length - len(line))
    sys.stderr.write(f"\r{line}{' ' * padding}")
    sys.stderr.flush()
    _last_progress_line_length = len(line)


def _clear_progress_line() -> None:
    global _last_progress_line_length
    if _last_progress_line_length == 0:
        return
    sys.stderr.write(f"\r{' ' * _last_progress_line_length}\r")
    sys.stderr.flush()
    _last_progress_line_length = 0


def _render_progress(current: int, total: int, label: str) -> None:
    filled = total if total == 0 else int((current / total) * 20)
    bar = "#" * filled + "-" * (20 - filled)
    _write_progress_line(f"[{current:>2}/{total}] [{bar}] {label}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    progress_callback = None if args.json else _render_progress
    try:
        report = audit_player_metrics_db(progress=progress_callback)
    finally:
        if progress_callback is not None:
            _clear_progress_line()
    summary = summarize_validation_report(report)
    if args.json:
        print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))
    else:
        print(render_validation_summary(summary, top_n=args.top))
    return 0 if summary.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
