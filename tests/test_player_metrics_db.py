from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from wowy.data.player_metrics_db import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    PlayerSeasonMetricRow,
    initialize_player_metrics_db,
    list_metric_seasons,
    load_metric_full_span_points_map,
    load_metric_full_span_series_rows,
    load_metric_rows,
    load_metric_scope_catalog_row,
    load_metric_store_metadata,
    replace_metric_full_span_rows,
    replace_metric_rows,
    replace_metric_scope_catalog_row,
)


def test_replace_metric_rows_persists_metadata_and_queryable_rows(tmp_path: Path):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"

    replace_metric_rows(
        db_path,
        metric="wowy",
        scope_key="teams=all-teams|season_type=Regular Season",
        metric_label="WOWY",
        build_version="v1",
        source_fingerprint="fingerprint-1",
        rows=[
            PlayerSeasonMetricRow(
                metric="wowy",
                metric_label="WOWY",
                scope_key="teams=all-teams|season_type=Regular Season",
                team_filter="",
                season_type="Regular Season",
                season="2022-23",
                player_id=101,
                player_name="Player 101",
                value=12.0,
                sample_size=2,
                secondary_sample_size=1,
                average_minutes=34.0,
                total_minutes=68.0,
                details={"avg_margin_with": 7.0},
            ),
            PlayerSeasonMetricRow(
                metric="wowy",
                metric_label="WOWY",
                scope_key="teams=all-teams|season_type=Regular Season",
                team_filter="",
                season_type="Regular Season",
                season="2023-24",
                player_id=101,
                player_name="Player 101",
                value=2.0,
                sample_size=2,
                secondary_sample_size=1,
                average_minutes=34.0,
                total_minutes=68.0,
                details={"avg_margin_with": 3.0},
            ),
            PlayerSeasonMetricRow(
                metric="wowy",
                metric_label="WOWY",
                scope_key="teams=all-teams|season_type=Regular Season",
                team_filter="",
                season_type="Regular Season",
                season="2023-24",
                player_id=103,
                player_name="Player 103",
                value=6.5,
                sample_size=2,
                secondary_sample_size=1,
                average_minutes=30.0,
                total_minutes=60.0,
                details={"avg_margin_with": 4.5},
            ),
        ],
    )

    metadata = load_metric_store_metadata(
        db_path,
        "wowy",
        "teams=all-teams|season_type=Regular Season",
    )
    assert metadata is not None
    assert metadata.metric_label == "WOWY"
    assert metadata.build_version == "v1"
    assert metadata.source_fingerprint == "fingerprint-1"
    assert metadata.row_count == 3
    assert list_metric_seasons(
        db_path,
        "wowy",
        "teams=all-teams|season_type=Regular Season",
    ) == ["2022-23", "2023-24"]

    rows = load_metric_rows(
        db_path,
        metric="wowy",
        scope_key="teams=all-teams|season_type=Regular Season",
        seasons=["2023-24"],
        min_sample_size=2,
        min_secondary_sample_size=1,
        min_average_minutes=30.0,
        min_total_minutes=60.0,
    )
    assert [(row.season, row.player_id, row.value) for row in rows] == [
        ("2023-24", 103, 6.5),
        ("2023-24", 101, 2.0),
    ]
    assert rows[0].details == {"avg_margin_with": 4.5}


def test_full_span_rows_and_scope_catalog_are_queryable(tmp_path: Path):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"

    replace_metric_scope_catalog_row(
        db_path,
        row=MetricScopeCatalogRow(
            metric="wowy",
            scope_key="teams=all-teams|season_type=Regular Season",
            metric_label="WOWY",
            team_filter="",
            season_type="Regular Season",
            available_seasons=["2022-23", "2023-24"],
            available_teams=["BOS", "NYK"],
            full_span_start_season="2022-23",
            full_span_end_season="2023-24",
            updated_at="2026-03-16T00:00:00+00:00",
        ),
    )
    replace_metric_full_span_rows(
        db_path,
        metric="wowy",
        scope_key="teams=all-teams|season_type=Regular Season",
        series_rows=[
            MetricFullSpanSeriesRow(
                metric="wowy",
                scope_key="teams=all-teams|season_type=Regular Season",
                player_id=101,
                player_name="Player 101",
                span_average_value=7.0,
                season_count=2,
                rank_order=1,
            )
        ],
        point_rows=[
            MetricFullSpanPointRow(
                metric="wowy",
                scope_key="teams=all-teams|season_type=Regular Season",
                player_id=101,
                season="2022-23",
                value=12.0,
            ),
            MetricFullSpanPointRow(
                metric="wowy",
                scope_key="teams=all-teams|season_type=Regular Season",
                player_id=101,
                season="2023-24",
                value=2.0,
            ),
        ],
    )

    catalog_row = load_metric_scope_catalog_row(
        db_path,
        "wowy",
        "teams=all-teams|season_type=Regular Season",
    )
    assert catalog_row is not None
    assert catalog_row.available_teams == ["BOS", "NYK"]
    assert catalog_row.full_span_end_season == "2023-24"

    series_rows = load_metric_full_span_series_rows(
        db_path,
        metric="wowy",
        scope_key="teams=all-teams|season_type=Regular Season",
        top_n=1,
    )
    assert series_rows == [
        MetricFullSpanSeriesRow(
            metric="wowy",
            scope_key="teams=all-teams|season_type=Regular Season",
            player_id=101,
            player_name="Player 101",
            span_average_value=7.0,
            season_count=2,
            rank_order=1,
        )
    ]

    assert load_metric_full_span_points_map(
        db_path,
        metric="wowy",
        scope_key="teams=all-teams|season_type=Regular Season",
        player_ids=[101],
    ) == {101: {"2022-23": 12.0, "2023-24": 2.0}}


def test_initialize_player_metrics_db_migrates_legacy_wowy_shrinkage_metric_names(
    tmp_path: Path,
):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"
    initialize_player_metrics_db(db_path)

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO metric_player_season_values (
                metric,
                metric_label,
                scope_key,
                team_filter,
                season_type,
                season,
                player_id,
                player_name,
                value,
                sample_size,
                secondary_sample_size,
                average_minutes,
                total_minutes,
                details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "shrinkage_wowy",
                "Shrinkage WOWY",
                "teams=all-teams|season_type=Regular Season",
                "",
                "Regular Season",
                "2023-24",
                101,
                "Player 101",
                1.5,
                2,
                1,
                34.0,
                68.0,
                '{"raw_wowy_score": 12.0}',
            ),
        )
        connection.execute(
            """
            INSERT INTO metric_store_metadata_v2 (
                metric,
                scope_key,
                metric_label,
                build_version,
                source_fingerprint,
                row_count,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "shrinkage_wowy",
                "teams=all-teams|season_type=Regular Season",
                "Shrinkage WOWY",
                "v1",
                "fingerprint-1",
                1,
                "2026-03-17T00:00:00+00:00",
            ),
        )
        connection.execute(
            """
            INSERT INTO metric_scope_catalog (
                metric,
                scope_key,
                metric_label,
                team_filter,
                season_type,
                available_seasons_json,
                available_teams_json,
                full_span_start_season,
                full_span_end_season,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "shrinkage_wowy",
                "teams=all-teams|season_type=Regular Season",
                "Shrinkage WOWY",
                "",
                "Regular Season",
                '["2023-24"]',
                '["BOS"]',
                "2023-24",
                "2023-24",
                "2026-03-17T00:00:00+00:00",
            ),
        )
        connection.execute(
            """
            INSERT INTO metric_full_span_series (
                metric,
                scope_key,
                player_id,
                player_name,
                span_average_value,
                season_count,
                rank_order
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "shrinkage_wowy",
                "teams=all-teams|season_type=Regular Season",
                101,
                "Player 101",
                1.5,
                1,
                1,
            ),
        )
        connection.execute(
            """
            INSERT INTO metric_full_span_points (
                metric,
                scope_key,
                player_id,
                season,
                value
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                "shrinkage_wowy",
                "teams=all-teams|season_type=Regular Season",
                101,
                "2023-24",
                1.5,
            ),
        )
        connection.commit()

    initialize_player_metrics_db(db_path)

    rows = load_metric_rows(
        db_path,
        metric="wowy_shrunk",
        scope_key="teams=all-teams|season_type=Regular Season",
    )
    metadata = load_metric_store_metadata(
        db_path,
        "wowy_shrunk",
        "teams=all-teams|season_type=Regular Season",
    )
    catalog_row = load_metric_scope_catalog_row(
        db_path,
        "wowy_shrunk",
        "teams=all-teams|season_type=Regular Season",
    )
    series_rows = load_metric_full_span_series_rows(
        db_path,
        metric="wowy_shrunk",
        scope_key="teams=all-teams|season_type=Regular Season",
    )
    point_map = load_metric_full_span_points_map(
        db_path,
        metric="wowy_shrunk",
        scope_key="teams=all-teams|season_type=Regular Season",
        player_ids=[101],
    )

    assert len(rows) == 1
    assert rows[0].metric == "wowy_shrunk"
    assert rows[0].metric_label == "WOWY Shrunk"
    assert metadata is not None
    assert metadata.metric == "wowy_shrunk"
    assert metadata.metric_label == "WOWY Shrunk"
    assert catalog_row is not None
    assert catalog_row.metric == "wowy_shrunk"
    assert catalog_row.metric_label == "WOWY Shrunk"
    assert series_rows[0].metric == "wowy_shrunk"
    assert point_map == {101: {"2023-24": 1.5}}


def test_initialize_player_metrics_db_uses_expected_primary_keys(tmp_path: Path):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"
    initialize_player_metrics_db(db_path)

    with sqlite3.connect(db_path) as connection:
        metric_rows = connection.execute(
            "PRAGMA table_info(metric_player_season_values)"
        ).fetchall()
        full_span_points = connection.execute(
            "PRAGMA table_info(metric_full_span_points)"
        ).fetchall()

    assert [row[1] for row in metric_rows if row[5] > 0] == [
        "metric",
        "scope_key",
        "season",
        "player_id",
    ]
    assert [row[1] for row in full_span_points if row[5] > 0] == [
        "metric",
        "scope_key",
        "player_id",
        "season",
    ]


def test_replace_metric_rows_rejects_non_canonical_scope_values(
    tmp_path: Path,
):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"

    with pytest.raises(ValueError, match="non-canonical season_type"):
        replace_metric_rows(
            db_path,
            metric="wowy",
            scope_key="teams=BOS|season_type=regular season",
            metric_label="WOWY",
            build_version="v1",
            source_fingerprint="fingerprint-1",
            rows=[
                PlayerSeasonMetricRow(
                    metric="wowy",
                    metric_label="WOWY",
                    scope_key="teams=BOS|season_type=regular season",
                    team_filter="BOS",
                    season_type="regular season",
                    season="2023-24",
                    player_id=101,
                    player_name="Player 101",
                    value=2.5,
                    sample_size=3,
                    average_minutes=30.0,
                    total_minutes=90.0,
                    details={"games": 3},
                )
            ],
        )

    with pytest.raises(ValueError, match="non-canonical season_type"):
        replace_metric_rows(
            db_path,
            metric="wowy",
            scope_key="teams=BOS|season_type=Regular Season",
            metric_label="WOWY",
            build_version="v1",
            source_fingerprint="fingerprint-1",
            rows=[
                PlayerSeasonMetricRow(
                    metric="wowy",
                    metric_label="WOWY",
                    scope_key="teams=BOS|season_type=Regular Season",
                    team_filter="BOS",
                    season_type="regular season",
                    season="2023-24",
                    player_id=101,
                    player_name="Player 101",
                    value=2.5,
                    sample_size=3,
                    average_minutes=30.0,
                    total_minutes=90.0,
                    details={"games": 3},
                )
            ],
        )

    with pytest.raises(ValueError, match="non-canonical team_filter"):
        replace_metric_rows(
            db_path,
            metric="wowy",
            scope_key="teams=BOS|season_type=Regular Season",
            metric_label="WOWY",
            build_version="v1",
            source_fingerprint="fingerprint-1",
            rows=[
                PlayerSeasonMetricRow(
                    metric="wowy",
                    metric_label="WOWY",
                    scope_key="teams=BOS|season_type=Regular Season",
                    team_filter="bos",
                    season_type="Regular Season",
                    season="2023-24",
                    player_id=101,
                    player_name="Player 101",
                    value=2.5,
                    sample_size=3,
                    average_minutes=30.0,
                    total_minutes=90.0,
                    details={"games": 3},
                )
            ],
        )


def test_metric_catalog_and_full_span_writes_reject_inconsistent_shapes(tmp_path: Path):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"

    with pytest.raises(ValueError, match="available_seasons must be unique and sorted"):
        replace_metric_scope_catalog_row(
            db_path,
            row=MetricScopeCatalogRow(
                metric="wowy",
                scope_key="teams=all-teams|season_type=Regular Season",
                metric_label="WOWY",
                team_filter="",
                season_type="Regular Season",
                available_seasons=["2023-24", "2022-23"],
                available_teams=["BOS", "NYK"],
                full_span_start_season="2022-23",
                full_span_end_season="2023-24",
                updated_at="2026-03-16T00:00:00+00:00",
            ),
        )

    with pytest.raises(ValueError, match="rank_order values must be unique and contiguous"):
        replace_metric_full_span_rows(
            db_path,
            metric="wowy",
            scope_key="teams=all-teams|season_type=Regular Season",
            series_rows=[
                MetricFullSpanSeriesRow(
                    metric="wowy",
                    scope_key="teams=all-teams|season_type=Regular Season",
                    player_id=101,
                    player_name="Player 101",
                    span_average_value=7.0,
                    season_count=2,
                    rank_order=2,
                )
            ],
            point_rows=[
                MetricFullSpanPointRow(
                    metric="wowy",
                    scope_key="teams=all-teams|season_type=Regular Season",
                    player_id=101,
                    season="2022-23",
                    value=12.0,
                ),
                MetricFullSpanPointRow(
                    metric="wowy",
                    scope_key="teams=all-teams|season_type=Regular Season",
                    player_id=101,
                    season="2023-24",
                    value=2.0,
                ),
            ],
        )

    with pytest.raises(ValueError, match="expected 2 season points but found 1"):
        replace_metric_full_span_rows(
            db_path,
            metric="wowy",
            scope_key="teams=all-teams|season_type=Regular Season",
            series_rows=[
                MetricFullSpanSeriesRow(
                    metric="wowy",
                    scope_key="teams=all-teams|season_type=Regular Season",
                    player_id=101,
                    player_name="Player 101",
                    span_average_value=7.0,
                    season_count=2,
                    rank_order=1,
                )
            ],
            point_rows=[
                MetricFullSpanPointRow(
                    metric="wowy",
                    scope_key="teams=all-teams|season_type=Regular Season",
                    player_id=101,
                    season="2022-23",
                    value=12.0,
                )
            ],
        )
