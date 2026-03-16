from __future__ import annotations

from pathlib import Path

from wowy.data.player_metrics_db import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    PlayerSeasonMetricRow,
    load_metric_full_span_points_map,
    load_metric_full_span_series_rows,
    load_metric_scope_catalog_row,
    list_metric_seasons,
    load_metric_rows,
    load_metric_store_metadata,
    replace_metric_full_span_rows,
    replace_metric_scope_catalog_row,
    replace_metric_rows,
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
