from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


DEFAULT_INPUT_PATH = Path("data/combined/wowy/player_seasons.csv")
DEFAULT_OUTPUT_PATH = Path("data/combined/wowy/player_history.png")


@dataclass(frozen=True)
class PlayerSeasonPoint:
    season: str
    player_id: int
    player_name: str
    wowy_score: float
    games_with: int
    games_without: int
    average_minutes: float | None
    total_minutes: float | None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plot WOWY player history from a player-season CSV export."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT_PATH,
        help=f"Player-season WOWY CSV path (default: {DEFAULT_INPUT_PATH})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output PNG path (default: {DEFAULT_OUTPUT_PATH})",
    )
    parser.add_argument(
        "--season",
        action="append",
        default=None,
        help="Filter to a season string. Repeat to include multiple seasons.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of players to plot, ranked by average WOWY over the selected seasons",
    )
    parser.add_argument(
        "--min-seasons",
        type=int,
        default=1,
        help="Minimum number of season points required for a player to be eligible",
    )
    parser.add_argument(
        "--title",
        default="WOWY Player History",
        help="Chart title",
    )
    return parser


def load_player_season_points(csv_path: Path | str) -> list[PlayerSeasonPoint]:
    points: list[PlayerSeasonPoint] = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required_columns = {
            "season",
            "player_id",
            "player_name",
            "games_with",
            "games_without",
            "wowy_score",
            "average_minutes",
            "total_minutes",
        }
        missing = required_columns - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing required CSV columns: {sorted(missing)}")

        for row_number, row in enumerate(reader, start=2):
            points.append(
                PlayerSeasonPoint(
                    season=require_text(row["season"], "season", row_number),
                    player_id=parse_int(row["player_id"], "player_id", row_number),
                    player_name=require_text(
                        row["player_name"], "player_name", row_number
                    ),
                    wowy_score=parse_float(
                        row["wowy_score"], "wowy_score", row_number
                    ),
                    games_with=parse_int(row["games_with"], "games_with", row_number),
                    games_without=parse_int(
                        row["games_without"], "games_without", row_number
                    ),
                    average_minutes=parse_optional_float(
                        row["average_minutes"], "average_minutes", row_number
                    ),
                    total_minutes=parse_optional_float(
                        row["total_minutes"], "total_minutes", row_number
                    ),
                )
            )
    return points


def filter_points_by_seasons(
    points: list[PlayerSeasonPoint],
    seasons: list[str] | None,
) -> list[PlayerSeasonPoint]:
    if not seasons:
        return points
    selected = set(seasons)
    return [point for point in points if point.season in selected]


def select_top_players(
    points: list[PlayerSeasonPoint],
    top_n: int,
    min_seasons: int,
) -> list[int]:
    if top_n < 0:
        raise ValueError("Top-n must be non-negative")
    if min_seasons < 1:
        raise ValueError("Minimum seasons must be at least 1")

    points_by_player: dict[int, list[PlayerSeasonPoint]] = {}
    for point in points:
        points_by_player.setdefault(point.player_id, []).append(point)

    ranked = []
    for player_id, player_points in points_by_player.items():
        if len(player_points) < min_seasons:
            continue
        ranked.append(
            (
                average(point.wowy_score for point in player_points),
                len(player_points),
                player_points[0].player_name,
                player_id,
            )
        )

    ranked.sort(key=lambda item: (-item[0], -item[1], item[2], item[3]))
    return [player_id for _, _, _, player_id in ranked[:top_n]]


def build_plot_series(
    points: list[PlayerSeasonPoint],
    selected_player_ids: list[int],
) -> list[tuple[str, list[PlayerSeasonPoint]]]:
    selected = set(selected_player_ids)
    series_by_player: dict[int, list[PlayerSeasonPoint]] = {}
    names_by_player: dict[int, str] = {}
    for point in points:
        if point.player_id not in selected:
            continue
        series_by_player.setdefault(point.player_id, []).append(point)
        names_by_player[point.player_id] = point.player_name

    ordered_series = []
    for player_id in selected_player_ids:
        player_points = sorted(
            series_by_player.get(player_id, []),
            key=lambda point: point.season,
        )
        if not player_points:
            continue
        ordered_series.append((names_by_player[player_id], player_points))
    return ordered_series


def plot_player_history(
    series: list[tuple[str, list[PlayerSeasonPoint]]],
    output_path: Path | str,
    title: str,
) -> None:
    if not series:
        raise ValueError("No player series matched the current filters")

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    all_seasons = sorted({point.season for _, points in series for point in points})
    season_index = {season: index for index, season in enumerate(all_seasons)}

    fig, ax = plt.subplots(figsize=(12, 7))
    for player_name, points in series:
        xs = [season_index[point.season] for point in points]
        ys = [point.wowy_score for point in points]
        ax.plot(xs, ys, marker="o", linewidth=2, label=player_name)

    ax.set_title(title)
    ax.set_xlabel("Season")
    ax.set_ylabel("WOWY score")
    ax.set_xticks(range(len(all_seasons)))
    ax.set_xticklabels(all_seasons, rotation=45, ha="right")
    ax.axhline(0.0, color="0.7", linewidth=1, linestyle="--")
    ax.grid(axis="y", alpha=0.3)
    ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5), frameon=False)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def average(values) -> float:
    values = list(values)
    if not values:
        raise ValueError("Expected at least one value")
    return sum(values) / len(values)


def require_text(value: str | None, field_name: str, row_number: int) -> str:
    text = (value or "").strip()
    if not text:
        raise ValueError(f"Invalid {field_name} at row {row_number}: {value!r}")
    return text


def parse_int(value: str | None, field_name: str, row_number: int) -> int:
    try:
        return int(value or "")
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name} at row {row_number}: {value!r}") from exc


def parse_float(value: str | None, field_name: str, row_number: int) -> float:
    try:
        return float(value or "")
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name} at row {row_number}: {value!r}") from exc


def parse_optional_float(
    value: str | None,
    field_name: str,
    row_number: int,
) -> float | None:
    text = (value or "").strip()
    if not text:
        return None
    return parse_float(text, field_name, row_number)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    points = load_player_season_points(args.input)
    points = filter_points_by_seasons(points, args.season)
    if not points:
        raise ValueError("No player-season rows matched the requested seasons")

    selected_player_ids = select_top_players(
        points,
        top_n=args.top_n,
        min_seasons=args.min_seasons,
    )
    series = build_plot_series(points, selected_player_ids)
    plot_player_history(series, args.output, title=args.title)
    print(f"wrote player history chart to {args.output}")
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
