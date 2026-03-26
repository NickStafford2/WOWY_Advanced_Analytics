from __future__ import annotations

from rawr_analytics.data.db_validation import (
    assert_valid_player_metrics_db,
    audit_player_metrics_db,
    render_validation_summary,
    summarize_validation_report,
)
from rawr_analytics.data.db_validation_cli import main as db_validation_cli_main


def test_audit_player_metrics_db_validates_real_database():
    report = audit_player_metrics_db()

    assert report.ok is True
    assert report.issues == []
    assert_valid_player_metrics_db()


def test_db_validation_cli_json_mode_omits_progress_output(capsys):
    report = audit_player_metrics_db()

    exit_code = db_validation_cli_main(["--json"])
    captured = capsys.readouterr()

    assert exit_code == (0 if report.ok else 1)
    assert f'"ok": {"true" if report.ok else "false"}' in captured.out
    assert captured.err == ""


def test_db_validation_cli_text_mode_matches_report(capsys):
    report = audit_player_metrics_db()
    summary = summarize_validation_report(report)

    exit_code = db_validation_cli_main(["--top", "3"])
    captured = capsys.readouterr()

    assert exit_code == (0 if report.ok else 1)
    assert captured.out == f"{render_validation_summary(summary, top_n=3)}\n"
    assert "Validating metric store relations" in captured.err
