from __future__ import annotations

import argparse
import subprocess
import sys

from rawr_analytics.shared.season import SeasonType

DEFAULT_START_YEAR = 2024
DEFAULT_FIRST_YEAR = 1946


def _build_parser() -> argparse.ArgumentParser:
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
        help=(f"Earliest season start year to cache, inclusive (default: {DEFAULT_FIRST_YEAR})"),
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


# todo: move/merge with season.py
def _season_string(start_year: int) -> str:
    end_year = (start_year + 1) % 100
    return f"{start_year}-{end_year:02d}"


def _build_season_strings(start_year: int, first_year: int) -> list[str]:
    # todo: move/merge with season.py
    if start_year < first_year:
        raise ValueError("Start year must be greater than or equal to first year")
    return [_season_string(year) for year in range(start_year, first_year - 1, -1)]


def _build_command(
    season: str,
    season_type: SeasonType,
    teams: list[str] | None,
    skip_combine: bool,
) -> list[str]:
    command = [sys.executable, "scripts/cache_season_data.py", season]
    if season_type != SeasonType.REGULAR:
        command.extend(["--season-type", season_type.value])
    if teams:
        command.extend(["--teams", *teams])
    if skip_combine:
        command.append("--skip-combine")
    return command


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    seasons = _build_season_strings(args.start_year, args.first_year)
    total = len(seasons)
    failed_seasons: list[str] = []
    for index, season in enumerate(seasons, start=1):
        print(f"[{index}/{total}] caching {season}")
        try:
            season_type = SeasonType.parse(args.season_type)
            subprocess.run(
                _build_command(
                    season, season_type, teams=args.teams, skip_combine=args.skip_combine
                ),
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            failed_seasons.append(season)
            sys.stderr.write(
                f"Season caching failed for {season} with exit status {exc.returncode}.\n"
            )
            sys.stderr.flush()
            continue

    if failed_seasons:
        sys.stderr.write(
            f"Completed with failures in {len(failed_seasons)}/{total} seasons: "
            f"{', '.join(failed_seasons)}\n"
        )
        sys.stderr.flush()
        return 1
    return 0


def run(argv: list[str] | None = None) -> int:
    try:
        return main(argv)
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted. Shutting down cleanly.\n")
        sys.stderr.flush()
        return 130


if __name__ == "__main__":
    raise SystemExit(run())
