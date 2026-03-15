from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path

from nba_api.stats.static import teams as nba_teams

from wowy.nba.cache_sync import wowy_cache_is_current
from wowy.nba.team_seasons import TeamSeasonScope
from wowy.cache_validation import validate_team_season_consistency
from wowy.apps.wowy.derive import WOWY_HEADER
from wowy.nba.ingest import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
)
from wowy.nba.cache import league_games_cache_path
from wowy.normalized_io import (
    NORMALIZED_GAME_PLAYERS_HEADER,
    NORMALIZED_GAMES_HEADER,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Report source, normalized, and WOWY cache status for a season."
    )
    parser.add_argument(
        "season",
        help="NBA season string, for example 2025-26",
    )
    parser.add_argument(
        "--teams",
        nargs="*",
        default=None,
        help="Optional team abbreviations to limit the report",
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="NBA season type used for source cache lookup",
    )
    parser.add_argument(
        "--source-data-dir",
        type=Path,
        default=DEFAULT_SOURCE_DATA_DIR,
        help="Path to cached source data",
    )
    parser.add_argument(
        "--normalized-games-dir",
        type=Path,
        default=DEFAULT_NORMALIZED_GAMES_DIR,
        help="Path to normalized games CSVs",
    )
    parser.add_argument(
        "--normalized-game-players-dir",
        type=Path,
        default=DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
        help="Path to normalized game-player CSVs",
    )
    parser.add_argument(
        "--wowy-dir",
        type=Path,
        default=DEFAULT_WOWY_GAMES_DIR,
        help="Path to per-team WOWY CSVs",
    )
    return parser


def load_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def resolve_requested_teams(team_codes: list[str] | None) -> list[str]:
    if team_codes:
        return sorted(team_code.upper() for team_code in team_codes)
    return sorted(team["abbreviation"] for team in nba_teams.get_teams())


def classify_box_score_cache(box_dir: Path, game_id: str) -> str:
    cache_path = box_dir / f"{game_id}_boxscoretraditionalv2.json"
    if not cache_path.exists():
        return "missing"

    payload = load_json(cache_path)
    if payload is None:
        return "corrupt"

    result_sets = payload.get("resultSets", [])
    if len(result_sets) < 2:
        return "corrupt"

    player_rows = result_sets[0].get("rowSet", [])
    team_rows = result_sets[1].get("rowSet", [])
    if not player_rows or not team_rows:
        return "empty"

    return "ok"


def summarize_source_cache(
    team: str,
    season: str,
    season_type: str,
    source_data_dir: Path,
) -> dict:
    cache_path = league_games_cache_path(
        team_abbreviation=team,
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
    )
    if not cache_path.exists():
        return {
            "league_games": "missing",
            "total_games": 0,
            "boxscores_ok": 0,
            "boxscores_empty": 0,
            "boxscores_missing": 0,
            "boxscores_corrupt": 0,
        }

    payload = load_json(cache_path)
    if payload is None:
        return {
            "league_games": "corrupt",
            "total_games": 0,
            "boxscores_ok": 0,
            "boxscores_empty": 0,
            "boxscores_missing": 0,
            "boxscores_corrupt": 0,
        }

    try:
        result_set = payload["resultSets"][0]
        headers = result_set["headers"]
        rows = [dict(zip(headers, row)) for row in result_set["rowSet"]]
    except (IndexError, KeyError, TypeError):
        return {
            "league_games": "corrupt",
            "total_games": 0,
            "boxscores_ok": 0,
            "boxscores_empty": 0,
            "boxscores_missing": 0,
            "boxscores_corrupt": 0,
        }

    unique_game_ids = {str(row["GAME_ID"]) for row in rows if "GAME_ID" in row}
    counts: Counter[str] = Counter()
    for game_id in unique_game_ids:
        counts[classify_box_score_cache(source_data_dir / "boxscores", game_id)] += 1

    return {
        "league_games": "ok",
        "total_games": len(unique_game_ids),
        "boxscores_ok": counts["ok"],
        "boxscores_empty": counts["empty"],
        "boxscores_missing": counts["missing"],
        "boxscores_corrupt": counts["corrupt"],
    }


def classify_csv(path: Path, expected_header: list[str]) -> tuple[str, int]:
    if not path.exists():
        return "missing", 0

    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header != expected_header:
                return "corrupt", 0
            row_count = sum(1 for _ in reader)
    except OSError:
        return "corrupt", 0

    return "ok", row_count


def summarize_wowy_cache(
    team_season: TeamSeasonScope,
    normalized_games_dir: Path,
    normalized_game_players_dir: Path,
    wowy_dir: Path,
) -> tuple[str, int]:
    wowy_path = wowy_dir / f"{team_season.team}_{team_season.season}.csv"
    normalized_games_path = normalized_games_dir / f"{team_season.team}_{team_season.season}.csv"
    normalized_game_players_path = (
        normalized_game_players_dir / f"{team_season.team}_{team_season.season}.csv"
    )

    status, row_count = classify_csv(wowy_path, WOWY_HEADER)
    if status != "ok":
        return status, row_count
    if not normalized_games_path.exists() or not normalized_game_players_path.exists():
        return "orphaned", row_count
    if not wowy_cache_is_current(
        wowy_path=wowy_path,
        normalized_games_path=normalized_games_path,
        normalized_game_players_path=normalized_game_players_path,
    ):
        return "stale", row_count
    return "current", row_count


def summarize_team_season(
    team: str,
    season: str,
    season_type: str,
    source_data_dir: Path,
    normalized_games_dir: Path,
    normalized_game_players_dir: Path,
    wowy_dir: Path,
) -> dict:
    team_season = TeamSeasonScope(team=team, season=season)
    source_summary = summarize_source_cache(
        team=team,
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
    )
    normalized_games_status, normalized_games_rows = classify_csv(
        normalized_games_dir / f"{team}_{season}.csv",
        NORMALIZED_GAMES_HEADER,
    )
    normalized_players_status, normalized_players_rows = classify_csv(
        normalized_game_players_dir / f"{team}_{season}.csv",
        NORMALIZED_GAME_PLAYERS_HEADER,
    )
    wowy_status, wowy_rows = summarize_wowy_cache(
        team_season=team_season,
        normalized_games_dir=normalized_games_dir,
        normalized_game_players_dir=normalized_game_players_dir,
        wowy_dir=wowy_dir,
    )
    return {
        "team": team,
        "source": source_summary["league_games"],
        "games": source_summary["total_games"],
        "box_ok": source_summary["boxscores_ok"],
        "box_missing": source_summary["boxscores_missing"],
        "box_empty": source_summary["boxscores_empty"],
        "box_corrupt": source_summary["boxscores_corrupt"],
        "normalized_games": (
            f"{normalized_games_status}"
            + (f" ({normalized_games_rows})" if normalized_games_status == "ok" else "")
        ),
        "normalized_players": (
            f"{normalized_players_status}"
            + (
                f" ({normalized_players_rows})"
                if normalized_players_status == "ok"
                else ""
            )
        ),
        "wowy": f"{wowy_status}" + (f" ({wowy_rows})" if wowy_rows else ""),
        "consistency": (
            validate_team_season_consistency(
                team=team,
                season=season,
                normalized_games_input_dir=normalized_games_dir,
                normalized_game_players_input_dir=normalized_game_players_dir,
                wowy_output_dir=wowy_dir,
            )
            if normalized_games_status == "ok"
            and normalized_players_status == "ok"
            and wowy_status in {"current", "stale"}
            else "-"
        ),
    }


def format_summary(rows: list[dict], season: str) -> str:
    if not rows:
        return "No teams matched the requested scope."

    sep = "  "
    team_width = max(len("team"), *(len(row["team"]) for row in rows))
    source_width = max(len("source"), *(len(row["source"]) for row in rows))
    games_width = max(len("games"), *(len(str(row["games"])) for row in rows))
    box_ok_width = max(len("ok"), *(len(str(row["box_ok"])) for row in rows))
    box_missing_width = max(
        len("miss"),
        *(len(str(row["box_missing"])) for row in rows),
    )
    box_empty_width = max(
        len("empty"),
        *(len(str(row["box_empty"])) for row in rows),
    )
    box_corrupt_width = max(
        len("bad"),
        *(len(str(row["box_corrupt"])) for row in rows),
    )
    normalized_games_width = max(
        len("norm_games"),
        *(len(row["normalized_games"]) for row in rows),
    )
    normalized_players_width = max(
        len("norm_players"),
        *(len(row["normalized_players"]) for row in rows),
    )
    wowy_width = max(len("wowy"), *(len(row["wowy"]) for row in rows))
    consistency_width = max(
        len("consistency"),
        *(len(row["consistency"]) for row in rows),
    )

    header = sep.join(
        [
            f"{'team':<{team_width}}",
            f"{'source':<{source_width}}",
            f"{'games':>{games_width}}",
            f"{'ok':>{box_ok_width}}",
            f"{'miss':>{box_missing_width}}",
            f"{'empty':>{box_empty_width}}",
            f"{'bad':>{box_corrupt_width}}",
            f"{'norm_games':<{normalized_games_width}}",
            f"{'norm_players':<{normalized_players_width}}",
            f"{'wowy':<{wowy_width}}",
            f"{'consistency':<{consistency_width}}",
        ]
    )
    divider = "-" * len(header)
    lines = [f"Cache status for {season}", divider, header, divider]

    for row in rows:
        lines.append(
            sep.join(
                [
                    f"{row['team']:<{team_width}}",
                    f"{row['source']:<{source_width}}",
                    f"{row['games']:>{games_width}}",
                    f"{row['box_ok']:>{box_ok_width}}",
                    f"{row['box_missing']:>{box_missing_width}}",
                    f"{row['box_empty']:>{box_empty_width}}",
                    f"{row['box_corrupt']:>{box_corrupt_width}}",
                    f"{row['normalized_games']:>{normalized_games_width}}",
                    f"{row['normalized_players']:>{normalized_players_width}}",
                    f"{row['wowy']:>{wowy_width}}",
                    f"{row['consistency']:<{consistency_width}}",
                ]
            )
        )

    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    rows = [
        summarize_team_season(
            team=team,
            season=args.season,
            season_type=args.season_type,
            source_data_dir=args.source_data_dir,
            normalized_games_dir=args.normalized_games_dir,
            normalized_game_players_dir=args.normalized_game_players_dir,
            wowy_dir=args.wowy_dir,
        )
        for team in resolve_requested_teams(args.teams)
    ]
    print(format_summary(rows, season=args.season))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
