from __future__ import annotations

import argparse
import subprocess
import sys

from rawr_analytics.data.constants import DB_PATH
from rawr_analytics.shared.season import SeasonType

DEFAULT_START_YEAR = 2025
DEFAULT_FIRST_YEAR = 1998


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild the full player metrics SQLite database from scratch: "
            "normalized NBA cache, web metric store, and validation."
        )
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=DEFAULT_START_YEAR,
        help=f"Latest season start year to rebuild (default: {DEFAULT_START_YEAR})",
    )
    parser.add_argument(
        "--first-year",
        type=int,
        default=DEFAULT_FIRST_YEAR,
        help=f"Earliest season start year to rebuild (default: {DEFAULT_FIRST_YEAR})",
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="NBA season type to rebuild.",
    )
    parser.add_argument(
        "--teams",
        nargs="*",
        default=None,
        help="Optional team abbreviations to restrict the rebuild scope.",
    )
    parser.add_argument(
        "--metric",
        action="append",
        choices=["wowy", "wowy_shrunk", "rawr"],
        help="Optional web metric to refresh. Repeat to select multiple. Defaults to all.",
    )
    parser.add_argument(
        "--keep-existing-db",
        action="store_true",
        help="Do not delete the existing database before rebuilding.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.start_year < args.first_year:
        raise ValueError("Start year must be greater than or equal to first year")

    if not args.keep_existing_db and DB_PATH.exists():
        print(f"Deleting existing database: {DB_PATH}")
        DB_PATH.unlink()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    run_step(
        "Rebuilding normalized cache",
        build_cache_command(
            start_year=args.start_year,
            first_year=args.first_year,
            season_type=args.season_type,
            teams=args.teams,
        ),
    )
    run_step(
        "Refreshing web metric store",
        build_refresh_command(
            season_type=args.season_type,
            metrics=args.metric,
        ),
    )
    run_step(
        "Validating rebuilt database",
        build_validate_command(),
    )
    print(f"Rebuild complete: {DB_PATH}")
    return 0


def run_step(label: str, command: list[str]) -> None:
    print(f"\n== {label} ==")
    print(" ".join(command))
    subprocess.run(command, check=True)


def build_cache_command(
    *,
    start_year: int,
    first_year: int,
    season_type: SeasonType,
    teams: list[str] | None,
) -> list[str]:
    command = [
        sys.executable,
        "scripts/cache_season_data.py",
        "--all-seasons",
        "--start-year",
        str(start_year),
        "--first-year",
        str(first_year),
    ]
    if season_type != SeasonType.REGULAR:
        command.extend(["--season-type", season_type.value])
    if teams:
        command.extend(["--teams", *teams])
    return command


def build_refresh_command(
    *,
    season_type: SeasonType,
    metrics: list[str] | None,
) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "wowy.web.refresh_cli",
    ]
    if season_type != SeasonType.REGULAR:
        command.extend(["--season-type", season_type.value])
    for metric in metrics or []:
        command.extend(["--metric", metric])
    return command


def build_validate_command() -> list[str]:
    return [
        sys.executable,
        "-m",
        "wowy.data.db_validation_cli",
    ]


def run(argv: list[str] | None = None) -> int:
    try:
        return main(argv)
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted. Shutting down cleanly.\n")
        sys.stderr.flush()
        return 130


if __name__ == "__main__":
    raise SystemExit(run())
