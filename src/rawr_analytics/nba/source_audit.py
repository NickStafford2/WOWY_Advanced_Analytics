from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from rawr_analytics.nba.source.cache import DEFAULT_SOURCE_DATA_DIR
from rawr_analytics.nba.source.dedupe import dedupe_schedule_games
from rawr_analytics.nba.source.parsers import (
    parse_box_score_payload,
    parse_league_schedule_payload,
)
from rawr_analytics.nba.source.rules import (
    classify_source_player_row,
    classify_source_schedule_row,
    classify_source_team_row,
)
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

AuditProgressFn = Callable[[int, int, str], None]
_LAST_PROGRESS_LINE_LENGTH = 0


@dataclass(frozen=True)
class SourceAuditFailure:
    category: str
    source_path: str
    message: str


@dataclass(frozen=True)
class SourceAuditReport:
    scanned_schedule_files: int
    scanned_box_score_files: int
    classification_counts: dict[str, int]
    failure_counts: dict[str, int]
    failure_examples: list[SourceAuditFailure]

    @property
    def ok(self) -> bool:
        return not self.failure_counts

    def to_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "scanned_schedule_files": self.scanned_schedule_files,
            "scanned_box_score_files": self.scanned_box_score_files,
            "classification_counts": self.classification_counts,
            "failure_counts": self.failure_counts,
            "failure_examples": [
                {
                    "category": failure.category,
                    "source_path": failure.source_path,
                    "message": failure.message,
                }
                for failure in self.failure_examples
            ],
        }


def _audit_nba_source(
    source_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    *,
    progress: AuditProgressFn | None = None,
) -> SourceAuditReport:
    source_dir = Path(source_dir)
    schedule_paths = sorted((source_dir / "team_seasons").glob("*_leaguegamefinder.json"))
    box_score_paths = sorted((source_dir / "boxscores").glob("*.json"))
    total_files = len(schedule_paths) + len(box_score_paths)
    current = 0

    classification_counts: Counter[str] = Counter()
    failure_counts: Counter[str] = Counter()
    failure_examples: list[SourceAuditFailure] = []

    def report_progress(label: str) -> None:
        if progress is not None:
            progress(current, total_files, label)

    for schedule_path in schedule_paths:
        current += 1
        report_progress(f"schedule {schedule_path.name}")
        try:
            payload = _load_json_payload(schedule_path)
            team, season = _scope_from_schedule_path(schedule_path)
            schedule = parse_league_schedule_payload(
                payload,
                team=team,
                season=season,
            )
            for game in schedule.games:
                classification = classify_source_schedule_row(game.raw_row)
                if classification.kind != "canonical_schedule_source_row":
                    classification_counts[classification.kind] += 1
            dedupe_schedule_games(schedule.games)
        except ValueError as exc:
            _record_failure(
                failure_counts=failure_counts,
                failure_examples=failure_examples,
                category="schedule_error",
                source_path=schedule_path,
                message=str(exc),
            )

    for box_score_path in box_score_paths:
        current += 1
        report_progress(f"boxscore {box_score_path.name}")
        try:
            payload = _load_json_payload(box_score_path)
            game_id = box_score_path.stem.split("_", maxsplit=1)[0]
            box_score = parse_box_score_payload(payload, game_id=game_id)
            for player in box_score.players:
                classification = classify_source_player_row(player)
                if classification.kind != "canonical_player_source_row":
                    classification_counts[classification.kind] += 1
            for team in box_score.teams:
                classification = classify_source_team_row(team)
                if classification.kind != "canonical_team_source_row":
                    classification_counts[classification.kind] += 1
        except ValueError as exc:
            _record_failure(
                failure_counts=failure_counts,
                failure_examples=failure_examples,
                category="box_score_error",
                source_path=box_score_path,
                message=str(exc),
            )

    return SourceAuditReport(
        scanned_schedule_files=len(schedule_paths),
        scanned_box_score_files=len(box_score_paths),
        classification_counts=dict(sorted(classification_counts.items())),
        failure_counts=dict(sorted(failure_counts.items())),
        failure_examples=failure_examples,
    )


def _render_source_audit_report(report: SourceAuditReport) -> str:
    lines = [
        f"Source audit status: {'ok' if report.ok else 'invalid'}",
        f"Scanned schedule files: {report.scanned_schedule_files}",
        f"Scanned box score files: {report.scanned_box_score_files}",
    ]
    if report.classification_counts:
        lines.append("")
        lines.append("Known source classifications:")
        for kind, count in report.classification_counts.items():
            lines.append(f"- {kind}: {count}")
    if report.failure_counts:
        lines.append("")
        lines.append("Hard failures:")
        for category, count in report.failure_counts.items():
            lines.append(f"- {category}: {count}")
        lines.append("")
        lines.append("Failure examples:")
        for failure in report.failure_examples[:10]:
            lines.append(f"- {failure.category} [{failure.source_path}]: {failure.message}")
    return "\n".join(lines)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit cached NBA source payloads for known anomalies and hard failures."
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=DEFAULT_SOURCE_DATA_DIR,
        help="Root source cache directory to audit.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the summary as JSON.",
    )
    return parser


def _write_progress_line(line: str) -> None:
    global _LAST_PROGRESS_LINE_LENGTH
    padding = max(0, _LAST_PROGRESS_LINE_LENGTH - len(line))
    sys.stderr.write(f"\r{line}{' ' * padding}")
    sys.stderr.flush()
    _LAST_PROGRESS_LINE_LENGTH = len(line)


def _clear_progress_line() -> None:
    global _LAST_PROGRESS_LINE_LENGTH
    if _LAST_PROGRESS_LINE_LENGTH == 0:
        return
    sys.stderr.write(f"\r{' ' * _LAST_PROGRESS_LINE_LENGTH}\r")
    sys.stderr.flush()
    _LAST_PROGRESS_LINE_LENGTH = 0


def _render_progress(current: int, total: int, label: str) -> None:
    total = max(total, 1)
    filled = int((current / total) * 20)
    bar = "#" * filled + "-" * (20 - filled)
    _write_progress_line(f"[{current:>4}/{total}] [{bar}] {label}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    progress = None if args.json else _render_progress
    try:
        report = _audit_nba_source(args.source_dir, progress=progress)
    finally:
        if progress is not None:
            _clear_progress_line()

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        print(_render_source_audit_report(report))
    return 0 if report.ok else 1


def _load_json_payload(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Corrupt cached JSON: {exc}") from exc


def _scope_from_schedule_path(path: Path) -> tuple[Team, Season]:
    team_abbreviation, season_id, season_type_slug = path.stem.removesuffix("_leaguegamefinder").split(
        "_",
        maxsplit=2,
    )
    season_type = SeasonType.parse(season_type_slug.replace("_", " ").title())
    season = Season(season_id, season_type.value)
    return Team.from_abbreviation(team_abbreviation, season=season), season


def _record_failure(
    *,
    failure_counts: Counter[str],
    failure_examples: list[SourceAuditFailure],
    category: str,
    source_path: Path,
    message: str,
) -> None:
    failure_counts[category] += 1
    if len([failure for failure in failure_examples if failure.category == category]) >= 5:
        return
    failure_examples.append(
        SourceAuditFailure(
            category=category,
            source_path=str(source_path),
            message=message,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
