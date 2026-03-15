from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

from nba_api.stats.static import teams as nba_teams

from wowy.atomic_io import atomic_text_writer
from wowy.combine_games_cli import combine_normalized_files
from wowy.ingest_nba import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
    write_team_season_games_csv,
)


WOWY_HEADER = ["game_id", "season", "team", "margin", "players"]
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
        help="Only fetch team-season files and skip combined outputs.",
    )
    parser.add_argument(
        "--combined-wowy-csv",
        type=Path,
        default=Path("data/combined/wowy/games.csv"),
        help="Combined WOWY games CSV path",
    )
    parser.add_argument(
        "--combined-regression-games-csv",
        type=Path,
        default=Path("data/combined/regression/games.csv"),
        help="Combined normalized games CSV path",
    )
    parser.add_argument(
        "--combined-regression-game-players-csv",
        type=Path,
        default=Path("data/combined/regression/game_players.csv"),
        help="Combined normalized game-player CSV path",
    )
    return parser


def resolve_teams(team_codes: list[str] | None) -> list[str]:
    if team_codes:
        return [team_code.upper() for team_code in team_codes]
    return sorted(team["abbreviation"] for team in nba_teams.get_teams())


def combine_wowy_csvs(input_paths: list[Path], output_path: Path) -> None:
    with atomic_text_writer(output_path, newline="") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(WOWY_HEADER)

        for input_path in input_paths:
            with open(input_path, "r", encoding="utf-8", newline="") as input_file:
                reader = csv.reader(input_file)
                header = next(reader, None)
                if header != WOWY_HEADER:
                    raise ValueError(
                        f"Unexpected WOWY CSV header in {input_path}: {header!r}"
                    )
                for row in reader:
                    writer.writerow(row)


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


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    team_codes = resolve_teams(args.teams)
    normalized_games_paths: list[Path] = []
    normalized_game_players_paths: list[Path] = []
    wowy_csv_paths: list[Path] = []

    team_total = len(team_codes)
    for team_index, team_code in enumerate(team_codes, start=1):
        wowy_csv_path = DEFAULT_WOWY_GAMES_DIR / f"{team_code}_{args.season}.csv"
        normalized_games_path = (
            DEFAULT_NORMALIZED_GAMES_DIR / f"{team_code}_{args.season}.csv"
        )
        normalized_game_players_path = (
            DEFAULT_NORMALIZED_GAME_PLAYERS_DIR / f"{team_code}_{args.season}.csv"
        )
        summary = write_team_season_games_csv(
            team_abbreviation=team_code,
            season=args.season,
            csv_path=wowy_csv_path,
            normalized_games_csv_path=normalized_games_path,
            normalized_game_players_csv_path=normalized_game_players_path,
            season_type=args.season_type,
            source_data_dir=DEFAULT_SOURCE_DATA_DIR,
            log=quiet_log,
            progress=lambda payload, team_index=team_index: render_progress_line(
                team_index,
                team_total,
                payload,
            ),
        )
        render_team_complete_line(team_index, team_total, summary)
        sys.stdout.write("\n")
        normalized_games_paths.append(normalized_games_path)
        normalized_game_players_paths.append(normalized_game_players_path)
        wowy_csv_paths.append(wowy_csv_path)

    if args.skip_combine:
        return 0

    combine_normalized_files(
        games_input_paths=normalized_games_paths,
        game_players_input_paths=normalized_game_players_paths,
        games_output_path=args.combined_regression_games_csv,
        game_players_output_path=args.combined_regression_game_players_csv,
    )
    combine_wowy_csvs(wowy_csv_paths, args.combined_wowy_csv)
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
