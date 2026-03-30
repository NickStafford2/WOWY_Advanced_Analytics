from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import cache_season_data
else:
    from . import cache_season_data

DEFAULT_START_YEAR = 2024
DEFAULT_END_YEAR = 1946


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compatibility wrapper for caching every season through cache_season_data.py."
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=DEFAULT_START_YEAR,
        help=f"First season start year to cache (default: {DEFAULT_START_YEAR})",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=DEFAULT_END_YEAR,
        help=(f"End season start year to cache, inclusive (default: {DEFAULT_END_YEAR})"),
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


def _build_forwarded_argv(args: argparse.Namespace) -> list[str]:
    forwarded = [
        "--start-year",
        str(args.start_year),
        "--end-year",
        str(args.end_year),
    ]
    if args.season_type != "Regular Season":
        forwarded.extend(["--season-type", args.season_type])
    if args.teams:
        forwarded.extend(["--teams", *args.teams])
    if args.skip_combine:
        forwarded.append("--skip-combine")
    return forwarded


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return cache_season_data.main(_build_forwarded_argv(args))


def run(argv: list[str] | None = None) -> int:
    try:
        return main(argv)
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted. Shutting down cleanly.\n")
        sys.stderr.flush()
        return 130


if __name__ == "__main__":
    raise SystemExit(run())
