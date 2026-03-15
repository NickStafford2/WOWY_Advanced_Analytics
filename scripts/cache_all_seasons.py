from __future__ import annotations

import argparse
import subprocess
import sys


DEFAULT_START_YEAR = 2024
DEFAULT_FIRST_YEAR = 1946


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run cache_season_data.py for every season from a start year backward."
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=DEFAULT_START_YEAR,
        help=f"First season start year to cache (default: {DEFAULT_START_YEAR})",
    )
    parser.add_argument(
        "--first-year",
        type=int,
        default=DEFAULT_FIRST_YEAR,
        help=(
            "Earliest season start year to cache, inclusive "
            f"(default: {DEFAULT_FIRST_YEAR})"
        ),
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="Season type to pass through to cache_season_data.py",
    )
    parser.add_argument(
        "--teams",
        nargs="*",
        default=None,
        help="Optional team abbreviations to pass through to cache_season_data.py",
    )
    parser.add_argument(
        "--skip-combine",
        action="store_true",
        help="Pass --skip-combine through to cache_season_data.py",
    )
    return parser


def season_string(start_year: int) -> str:
    end_year = (start_year + 1) % 100
    return f"{start_year}-{end_year:02d}"


def build_season_strings(start_year: int, first_year: int) -> list[str]:
    if start_year < first_year:
        raise ValueError("Start year must be greater than or equal to first year")
    return [season_string(year) for year in range(start_year, first_year - 1, -1)]


def build_command(
    season: str,
    season_type: str,
    teams: list[str] | None,
    skip_combine: bool,
) -> list[str]:
    command = [sys.executable, "scripts/cache_season_data.py", season]
    if season_type != "Regular Season":
        command.extend(["--season-type", season_type])
    if teams:
        command.extend(["--teams", *teams])
    if skip_combine:
        command.append("--skip-combine")
    return command


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    seasons = build_season_strings(args.start_year, args.first_year)
    total = len(seasons)
    for index, season in enumerate(seasons, start=1):
        print(f"[{index}/{total}] caching {season}")
        subprocess.run(
            build_command(
                season=season,
                season_type=args.season_type,
                teams=args.teams,
                skip_combine=args.skip_combine,
            ),
            check=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
