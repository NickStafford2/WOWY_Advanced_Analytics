from __future__ import annotations

import sys

from rawr_analytics.sources.kaggle.ingest import ingest_kaggle_dataset


def main(argv: list[str] | None = None) -> int:
    del argv
    result = ingest_kaggle_dataset()
    sys.stdout.write(
        "\n".join(
            [
                f"source_root: {result.source_root}",
                f"scopes: {result.scope_count}",
                f"games: {result.game_count}",
                f"game_players: {result.game_player_count}",
                f"skipped_games: {result.skipped_game_count}",
                f"skipped_game_types: {', '.join(result.skipped_game_types)}",
            ]
        )
        + "\n"
    )
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
