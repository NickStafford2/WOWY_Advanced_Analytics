from __future__ import annotations

from pathlib import Path

from wowy.data.player_metrics_db import (
    PlayerSeasonMetricRow,
    list_metric_seasons,
    load_metric_rows,
    load_metric_store_metadata,
    replace_metric_rows,
)


def test_replace_metric_rows_persists_metadata_and_queryable_rows(tmp_path: Path):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"

    replace_metric_rows(
        db_path,
        metric="wowy",
        metric_label="WOWY",
        source_fingerprint="fingerprint-1",
        rows=[
            PlayerSeasonMetricRow(
                metric="wowy",
                metric_label="WOWY",
                season="2022-23",
                player_id=101,
                player_name="Player 101",
                value=12.0,
                games_with=2,
                games_without=1,
                average_minutes=34.0,
                total_minutes=68.0,
                details={"avg_margin_with": 7.0},
            ),
            PlayerSeasonMetricRow(
                metric="wowy",
                metric_label="WOWY",
                season="2023-24",
                player_id=101,
                player_name="Player 101",
                value=2.0,
                games_with=2,
                games_without=1,
                average_minutes=34.0,
                total_minutes=68.0,
                details={"avg_margin_with": 3.0},
            ),
            PlayerSeasonMetricRow(
                metric="wowy",
                metric_label="WOWY",
                season="2023-24",
                player_id=103,
                player_name="Player 103",
                value=6.5,
                games_with=2,
                games_without=1,
                average_minutes=30.0,
                total_minutes=60.0,
                details={"avg_margin_with": 4.5},
            ),
        ],
    )

    metadata = load_metric_store_metadata(db_path, "wowy")
    assert metadata is not None
    assert metadata.metric_label == "WOWY"
    assert metadata.source_fingerprint == "fingerprint-1"
    assert metadata.row_count == 3
    assert list_metric_seasons(db_path, "wowy") == ["2022-23", "2023-24"]

    rows = load_metric_rows(
        db_path,
        metric="wowy",
        seasons=["2023-24"],
        min_games_with=2,
        min_games_without=1,
        min_average_minutes=30.0,
        min_total_minutes=60.0,
    )
    assert [(row.season, row.player_id, row.value) for row in rows] == [
        ("2023-24", 103, 6.5),
        ("2023-24", 101, 2.0),
    ]
    assert rows[0].details == {"avg_margin_with": 4.5}
