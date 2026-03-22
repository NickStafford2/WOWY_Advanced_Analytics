from __future__ import annotations

import argparse
import json
from pathlib import Path

from wowy.data.db_validation import (
    audit_player_metrics_db,
    render_validation_summary,
    summarize_validation_report,
)
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH


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


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    report = audit_player_metrics_db(args.db_path)
    summary = summarize_validation_report(report)
    if args.json:
        print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))
    else:
        print(render_validation_summary(summary, top_n=args.top))
    return 0 if summary.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
