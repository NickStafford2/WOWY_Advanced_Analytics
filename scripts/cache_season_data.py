from __future__ import annotations

import argparse
import sys
from pathlib import Path

from nba_api.stats.static import teams as nba_teams

from wowy.nba.ingest import (
    DEFAULT_SOURCE_DATA_DIR,
    cache_team_season_data,
)
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.season_types import canonicalize_season_type
from wowy.nba.team_seasons import TeamSeasonScope


_LAST_STATUS_LINE_LENGTH = 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch, normalize, and cache one NBA season for many teams."
    )
    parser.add_argument(
        "season",
        help="NBA season string, for example 2023-24",
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="NBA season type, for example 'Regular Season' or 'Playoffs'",
    )
    parser.add_argument(
        "--teams",
        nargs="*",
        default=None,
        help="Optional team abbreviations. If omitted, fetches all NBA teams.",
    )
    parser.add_argument(
        "--skip-combine",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--player-metrics-db-path",
        type=Path,
        default=Path("data/app/player_metrics.sqlite3"),
        help="SQLite cache path for normalized team-season rows.",
    )
    return parser


def resolve_teams(team_codes: list[str] | None) -> list[str]:
    if team_codes:
        return [team_code.upper() for team_code in team_codes]
    return sorted(team["abbreviation"] for team in nba_teams.get_teams())


def render_progress_line(
    team_index: int,
    team_total: int,
    payload: dict,
) -> None:
    current = payload["current"]
    total = payload["total"]
    status = payload["status"]
    team = payload["team"]
    season = payload["season"]
    game_id = payload["game_id"]
    filled = 20 if total == 0 else int((current / total) * 20)
    bar = "#" * filled + "-" * (20 - filled)
    line = (
        f"  [{team_index:>2}/{team_total}] {team} {season} "
        f"{current}/{total} [{bar}] {status:<7} {game_id}"
    )
    write_status_line(line)


def quiet_log(_: str) -> None:
    return None


def write_status_line(line: str) -> None:
    global _LAST_STATUS_LINE_LENGTH
    padding = max(0, _LAST_STATUS_LINE_LENGTH - len(line))
    sys.stdout.write(f"\r{line}{' ' * padding}")
    sys.stdout.flush()
    _LAST_STATUS_LINE_LENGTH = len(line)


def render_team_complete_line(
    team_index: int,
    team_total: int,
    summary,
) -> None:
    line = (
        f"  [{team_index:>2}/{team_total}] {summary.team} {summary.season} "
        f"{summary.processed_games}/{summary.total_games} "
        f"league={'cached' if summary.league_games_source == 'cached' else 'fetched'} "
        f"boxscores={summary.fetched_box_scores} fetched, {summary.cached_box_scores} cached "
        f"skipped={summary.skipped_games}"
    )
    write_status_line(line)


def render_team_failed_line(
    team_index: int,
    team_total: int,
    team: str,
    season: str,
    reason: str,
) -> None:
    line = f"  [{team_index:>2}/{team_total}] {team} {season} failed consistency={reason}"
    write_status_line(line)


def parse_consistency_failure(message: str) -> str | None:
    prefix = "Inconsistent team-season cache for "
    if not message.startswith(prefix):
        return None
    _, _, reason = message.rpartition(": ")
    return reason or None


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    season = canonicalize_season_string(args.season)
    season_type = canonicalize_season_type(args.season_type)

    team_codes = resolve_teams(args.teams)
    team_total = len(team_codes)
    for team_index, team_code in enumerate(team_codes, start=1):
        team_season = TeamSeasonScope(team=team_code, season=season)
        try:
            summary = cache_team_season_data(
                team_abbreviation=team_code,
                season=season,
                season_type=season_type,
                source_data_dir=DEFAULT_SOURCE_DATA_DIR,
                player_metrics_db_path=args.player_metrics_db_path,
                log=quiet_log,
                progress=lambda payload, team_index=team_index: render_progress_line(
                    team_index,
                    team_total,
                    payload,
                ),
            )
        except ValueError as exc:
            reason = parse_consistency_failure(str(exc))
            if reason is None:
                raise
            render_team_failed_line(
                team_index=team_index,
                team_total=team_total,
                team=team_code,
                season=season,
                reason=reason,
            )
            sys.stdout.write("\n")
            sys.stderr.write(
                f"Inconsistent cache for {team_code} {season}: {reason}\n"
            )
            sys.stderr.flush()
            return 1
        render_team_complete_line(team_index, team_total, summary)
        sys.stdout.write("\n")
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
