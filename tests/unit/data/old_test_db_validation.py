from __future__ import annotations

from rawr_analytics.data._validation_issue import ValidationIssue
from rawr_analytics.data.audit.reporting import (
    DatabaseValidationReport,
    render_validation_summary,
    summarize_validation_report,
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


def test_render_validation_summary_lists_tables_and_trends():
    report = DatabaseValidationReport(
        issues=[
            ValidationIssue(
                table="normalized_cache_loads",
                key="BOS|2023-24|Regular Season",
                message="games_row_count does not match normalized_games count",
            ),
            ValidationIssue(
                table="normalized_cache_loads",
                key="LAL|2023-24|Regular Season",
                message="source_fingerprint does not match normalized cache",
            ),
        ]
    )

    summary = summarize_validation_report(report)
    rendered = render_validation_summary(summary, top_n=5)

    assert "Issues by table:" in rendered
    assert "Top 2 error trends:" in rendered
    assert "normalized_cache_loads" in rendered


def test_normalize_issue_message_replaces_embedded_ids():
    normalized = _normalize_issue_message(
        "Normalized player row for game '0020301176' player_id=445 player_name='Wesley Person' "
        "has invalid minutes nan"
    )

    assert normalized == (
        "Normalized player row for game '<value>' player_id=<num> player_name='<value>' "
        "has invalid minutes nan"
    )
