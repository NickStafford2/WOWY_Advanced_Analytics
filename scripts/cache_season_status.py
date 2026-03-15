from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from wowy.nba_cache import DEFAULT_SOURCE_DATA_DIR


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Report how much of a season is cached locally."
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
        "--source-data-dir",
        type=Path,
        default=DEFAULT_SOURCE_DATA_DIR,
        help="Path to cached source data",
    )
    return parser


def load_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def find_team_season_cache_paths(
    season: str,
    source_data_dir: Path,
    team_codes: list[str] | None,
) -> list[Path]:
    paths = sorted(
        (source_data_dir / "team_seasons").glob(
            f"*_{season}_regular_season_leaguegamefinder.json"
        )
    )
    if not team_codes:
        return paths

    allowed = {team_code.upper() for team_code in team_codes}
    return [path for path in paths if path.name.split("_", maxsplit=1)[0] in allowed]


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


def summarize_team_season_cache(team_cache_path: Path, source_data_dir: Path) -> dict:
    payload = load_json(team_cache_path)
    if payload is None:
        raise ValueError(f"Corrupt team-season cache: {team_cache_path}")

    result_set = payload["resultSets"][0]
    headers = result_set["headers"]
    rows = [dict(zip(headers, row)) for row in result_set["rowSet"]]
    unique_rows_by_game_id = {}
    for row in rows:
        unique_rows_by_game_id.setdefault(str(row["GAME_ID"]), row)

    counts: Counter[str] = Counter()
    for game_id in unique_rows_by_game_id:
        counts[classify_box_score_cache(source_data_dir / "boxscores", game_id)] += 1

    total = len(unique_rows_by_game_id)
    complete = counts["ok"]
    percent = (complete / total * 100.0) if total else 0.0

    return {
        "team": team_cache_path.name.split("_", maxsplit=1)[0],
        "total_games": total,
        "ok": counts["ok"],
        "empty": counts["empty"],
        "missing": counts["missing"],
        "corrupt": counts["corrupt"],
        "percent_ok": percent,
    }


def format_summary(rows: list[dict]) -> str:
    if not rows:
        return "No cached team-season files found for the requested season."

    totals = {
        "team": "TOTAL",
        "total_games": sum(row["total_games"] for row in rows),
        "ok": sum(row["ok"] for row in rows),
        "empty": sum(row["empty"] for row in rows),
        "missing": sum(row["missing"] for row in rows),
        "corrupt": sum(row["corrupt"] for row in rows),
        "percent_ok": 0.0,
    }
    if totals["total_games"]:
        totals["percent_ok"] = totals["ok"] / totals["total_games"] * 100.0

    name_width = max(len("team"), *(len(row["team"]) for row in rows), len("TOTAL"))
    lines = [
        "Season cache status",
        "-" * (name_width + 49),
        (
            f"{'team':<{name_width}} {'games':>6} {'ok':>6} {'empty':>7} "
            f"{'missing':>8} {'corrupt':>8} {'ok_%':>7}"
        ),
        "-" * (name_width + 49),
    ]

    for row in rows + [totals]:
        lines.append(
            f"{row['team']:<{name_width}} "
            f"{row['total_games']:>6} "
            f"{row['ok']:>6} "
            f"{row['empty']:>7} "
            f"{row['missing']:>8} "
            f"{row['corrupt']:>8} "
            f"{row['percent_ok']:>6.1f}%"
        )

    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    team_cache_paths = find_team_season_cache_paths(
        season=args.season,
        source_data_dir=args.source_data_dir,
        team_codes=args.teams,
    )
    rows = [
        summarize_team_season_cache(team_cache_path, args.source_data_dir)
        for team_cache_path in team_cache_paths
    ]
    print(format_summary(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
