from __future__ import annotations

import sqlite3
from pathlib import Path

from tests.support import game, player
from wowy.data.db_validation import (
    DatabaseValidationReport,
    ValidationIssue,
    assert_valid_player_metrics_db,
    audit_player_metrics_db,
    normalize_issue_message,
    render_validation_summary,
    summarize_validation_report,
)
from wowy.data.db_validation_cli import main as db_validation_cli_main
from wowy.data.game_cache import (
    build_normalized_cache_fingerprint,
    replace_team_season_normalized_rows,
)
from wowy.data.player_metrics_db import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    PlayerSeasonMetricRow,
    replace_metric_full_span_rows,
    replace_metric_rows,
    replace_metric_scope_catalog_row,
)
from wowy.nba.team_identity import resolve_team_id


def test_audit_player_metrics_db_accepts_valid_seed_data(tmp_path: Path):
    db_path = _seed_valid_db(tmp_path)

    report = audit_player_metrics_db(db_path)

    assert report.ok is True
    assert report.issues == []
    assert_valid_player_metrics_db(db_path)


def test_audit_player_metrics_db_reports_normalized_cache_count_mismatch(tmp_path: Path):
    db_path = _seed_valid_db(tmp_path)

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            UPDATE normalized_cache_loads
            SET games_row_count = 99
            WHERE team_id = 1610612738 AND season = '2023-24' AND season_type = 'Regular Season'
            """
        )
        connection.commit()

    report = audit_player_metrics_db(db_path)

    assert report.ok is False
    assert any(
        issue.table == "normalized_cache_loads"
        and "games_row_count does not match normalized_games count" in issue.message
        for issue in report.issues
    )


def test_audit_player_metrics_db_reports_metric_metadata_count_mismatch(tmp_path: Path):
    db_path = _seed_valid_db(tmp_path)
    scope_key = f"team_ids={resolve_team_id('BOS', season='2023-24')}|season_type=Regular Season"

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            UPDATE metric_store_metadata_v2
            SET row_count = 7
            WHERE metric = 'wowy' AND scope_key = ?
            """,
            (scope_key,),
        )
        connection.commit()

    report = audit_player_metrics_db(db_path)

    assert report.ok is False
    assert any(
        issue.table == "metric_store_metadata_v2"
        and "row_count does not match metric rows" in issue.message
        for issue in report.issues
    )


def test_audit_player_metrics_db_reports_noncanonical_persisted_catalog_values(tmp_path: Path):
    db_path = _seed_valid_db(tmp_path)
    scope_key = f"team_ids={resolve_team_id('BOS', season='2023-24')}|season_type=Regular Season"

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            UPDATE metric_scope_catalog
            SET team_filter = 'bos'
            WHERE metric = 'wowy' AND scope_key = ?
            """,
            (scope_key,),
        )
        connection.commit()

    report = audit_player_metrics_db(db_path)

    assert report.ok is False
    assert any(
        issue.table == "metric_scope_catalog"
        and "Invalid team_id filter value" in issue.message
        for issue in report.issues
    )


def test_summarize_validation_report_groups_similar_errors():
    report = DatabaseValidationReport(
        issues=[
            ValidationIssue(
                table="normalized_game_players",
                key="row-1",
                message=(
                    "Normalized player row for game '0020301176' player_id=445 "
                    "player_name='Wesley Person' "
                    "has invalid minutes nan"
                ),
            ),
            ValidationIssue(
                table="normalized_game_players",
                key="row-2",
                message=(
                    "Normalized player row for game '0020301180' player_id=1442 "
                    "player_name='Zeljko Rebraca' "
                    "has invalid minutes nan"
                ),
            ),
        ]
    )

    summary = summarize_validation_report(report)

    assert summary.issue_count == 2
    assert summary.table_counts == {"normalized_game_players": 2}
    assert len(summary.trends) == 1
    assert summary.trends[0].count == 2
    assert "has invalid minutes nan" in summary.trends[0].signature


def test_render_validation_summary_and_cli_show_top_error_trends(
    tmp_path: Path,
    capsys,
):
    db_path = _seed_valid_db(tmp_path)

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            UPDATE normalized_cache_loads
            SET games_row_count = 99
            WHERE team_id = 1610612738 AND season = '2023-24' AND season_type = 'Regular Season'
            """
        )
        connection.commit()

    report = audit_player_metrics_db(db_path)
    summary = summarize_validation_report(report)
    rendered = render_validation_summary(summary, top_n=5)

    assert "Issues by table:" in rendered
    assert "Top 2 error trends:" in rendered

    exit_code = db_validation_cli_main(["--db-path", str(db_path), "--top", "5"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Database validation status: invalid" in captured.out
    assert "normalized_cache_loads" in captured.out
    assert "games_row_count does not match normalized_games count" in captured.out
    assert "source_fingerprint does not match normalized cache" in captured.out
    assert "Validating metric store relations" in captured.err


def test_normalize_issue_message_replaces_embedded_ids():
    normalized = normalize_issue_message(
        "Normalized player row for game '0020301176' player_id=445 player_name='Wesley Person' "
        "has invalid minutes nan"
    )

    assert normalized == (
        "Normalized player row for game '<value>' player_id=<num> player_name='<value>' "
        "has invalid minutes nan"
    )


def test_db_validation_cli_json_mode_omits_progress_output(tmp_path: Path, capsys):
    db_path = _seed_valid_db(tmp_path)

    exit_code = db_validation_cli_main(["--db-path", str(db_path), "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "\"ok\": true" in captured.out
    assert captured.err == ""


def test_audit_player_metrics_db_reports_non_reciprocal_game_margins(tmp_path: Path):
    db_path = _seed_valid_db(tmp_path)

    replace_team_season_normalized_rows(
        db_path,
        team="LAL",
        team_id=resolve_team_id("LAL", season="2023-24"),
        season="2023-24",
        season_type="Regular Season",
        games=[
            game("0001", "2023-24", "2024-04-01", "LAL", "BOS", False, -7.0),
        ],
        game_players=[
            player("0001", "LAL", 201, "Player 201", True, 48.0),
            player("0001", "LAL", 202, "Player 202", True, 48.0),
            player("0001", "LAL", 203, "Player 203", True, 48.0),
            player("0001", "LAL", 204, "Player 204", True, 48.0),
            player("0001", "LAL", 205, "Player 205", True, 48.0),
        ],
        source_path="test://LAL_2023-24",
        source_snapshot="seed",
        source_kind="test",
    )

    report = audit_player_metrics_db(db_path)

    assert report.ok is False
    assert any(
        issue.table == "normalized_games"
        and "paired game rows must have opposite margins" in issue.message
        for issue in report.issues
    )


def _seed_valid_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "app" / "player_metrics.sqlite3"
    team_id = resolve_team_id("BOS", season="2023-24")
    scope_key = f"team_ids={team_id}|season_type=Regular Season"
    team_filter = str(team_id)

    replace_team_season_normalized_rows(
        db_path,
        team="BOS",
        team_id=team_id,
        season="2023-24",
        season_type="Regular Season",
        games=[
            game("0001", "2023-24", "2024-04-01", "BOS", "LAL", True, 8.0),
        ],
        game_players=[
            player("0001", "BOS", 101, "Player 101", True, 48.0),
            player("0001", "BOS", 102, "Player 102", True, 48.0),
            player("0001", "BOS", 103, "Player 103", True, 48.0),
            player("0001", "BOS", 104, "Player 104", True, 48.0),
            player("0001", "BOS", 105, "Player 105", True, 48.0),
        ],
        source_path="test://BOS_2023-24",
        source_snapshot="seed",
        source_kind="test",
    )

    replace_metric_rows(
        db_path,
        metric="wowy",
        scope_key=scope_key,
        metric_label="WOWY",
        build_version="v1",
        source_fingerprint=build_normalized_cache_fingerprint(
            db_path,
            season_type="Regular Season",
        ),
        rows=[
            PlayerSeasonMetricRow(
                metric="wowy",
                metric_label="WOWY",
                scope_key=scope_key,
                team_filter=team_filter,
                season_type="Regular Season",
                season="2023-24",
                player_id=101,
                player_name="Player 101",
                value=2.5,
                sample_size=3,
                average_minutes=36.0,
                total_minutes=108.0,
                details={"games": 3},
            )
        ],
    )
    replace_metric_scope_catalog_row(
        db_path,
        row=MetricScopeCatalogRow(
            metric="wowy",
            scope_key=scope_key,
            metric_label="WOWY",
            team_filter=team_filter,
            season_type="Regular Season",
            available_seasons=["2023-24"],
            available_teams=["BOS"],
            full_span_start_season="2023-24",
            full_span_end_season="2023-24",
            updated_at="2026-03-19T00:00:00+00:00",
        ),
    )
    replace_metric_full_span_rows(
        db_path,
        metric="wowy",
        scope_key=scope_key,
        series_rows=[
            MetricFullSpanSeriesRow(
                metric="wowy",
                scope_key=scope_key,
                player_id=101,
                player_name="Player 101",
                span_average_value=2.5,
                season_count=1,
                rank_order=1,
            )
        ],
        point_rows=[
            MetricFullSpanPointRow(
                metric="wowy",
                scope_key=scope_key,
                player_id=101,
                season="2023-24",
                value=2.5,
            )
        ],
    )

    return db_path
