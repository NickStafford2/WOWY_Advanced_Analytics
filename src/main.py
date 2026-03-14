from pathlib import Path
import csv


def load_games_from_csv(csv_path):
    games = []

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        required_columns = {"game_id", "team", "margin", "players"}
        missing = required_columns - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing required CSV columns: {sorted(missing)}")

        for row in reader:
            players = {
                player.strip() for player in row["players"].split(";") if player.strip()
            }

            game = {
                "game_id": row["game_id"],
                "team": row["team"],
                "margin": float(row["margin"]),
                "players": players,
            }
            games.append(game)

    return games


def compute_wowy(games):
    all_players = set()
    for game in games:
        all_players.update(game["players"])

    results = {}

    for player in sorted(all_players):
        margins_with = []
        margins_without = []

        for game in games:
            if player in game["players"]:
                margins_with.append(game["margin"])
            else:
                margins_without.append(game["margin"])

        avg_with = sum(margins_with) / len(margins_with) if margins_with else None
        avg_without = (
            sum(margins_without) / len(margins_without) if margins_without else None
        )

        wowy_score = None
        if avg_with is not None and avg_without is not None:
            wowy_score = avg_with - avg_without

        results[player] = {
            "games_with": len(margins_with),
            "games_without": len(margins_without),
            "avg_margin_with": avg_with,
            "avg_margin_without": avg_without,
            "wowy_score": wowy_score,
        }

    return results


def filter_results(results, min_games_with=1, min_games_without=1):
    filtered = {}

    for player, stats in results.items():
        if stats["games_with"] < min_games_with:
            continue
        if stats["games_without"] < min_games_without:
            continue
        if stats["wowy_score"] is None:
            continue
        filtered[player] = stats

    return filtered


def print_results(results):
    print("WOWY results (Version 1)")
    print("-" * 72)
    print(
        f"{'player':<12} {'with':>6} {'without':>8} "
        f"{'avg_with':>12} {'avg_without':>14} {'score':>10}"
    )
    print("-" * 72)

    ranked = sorted(
        results.items(),
        key=lambda item: item[1]["wowy_score"],
        reverse=True,
    )

    for player, stats in ranked:
        print(
            f"{player:<12} "
            f"{stats['games_with']:>6} "
            f"{stats['games_without']:>8} "
            f"{stats['avg_margin_with']:>12.2f} "
            f"{stats['avg_margin_without']:>14.2f} "
            f"{stats['wowy_score']:>10.2f}"
        )


if __name__ == "__main__":
    script_dir = Path(__file__).resolve().parent
    csv_path = script_dir / "games.csv"

    games = load_games_from_csv(csv_path)
    results = compute_wowy(games)

    filtered_results = filter_results(
        results,
        min_games_with=2,
        min_games_without=2,
    )

    print_results(filtered_results)
