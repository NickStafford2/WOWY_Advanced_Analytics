from __future__ import annotations

import subprocess

import pytest

from scripts import cache_all_seasons, cache_season_data
from wowy.nba.build_models import TeamSeasonRunSummary
from wowy.nba.errors import BoxScoreFetchError


def test_cache_season_data_continues_after_team_failure(monkeypatch, capsys):
    calls: list[str] = []

    def fake_cache_team_season_data(**kwargs):
        team = kwargs["team_abbreviation"]
        calls.append(team)
        if team == "BOS":
            raise BoxScoreFetchError(
                message="failed box score fetch",
                resource="box_score",
                identifier="0001",
                attempts=5,
                last_error_type="RequestException",
                last_error_message="temporary timeout",
                game_id="0001",
            )
        return TeamSeasonRunSummary(
            team=team,
            season=kwargs["season"],
            season_type=kwargs["season_type"],
            league_games_source="cached",
            total_games=82,
            processed_games=82,
            skipped_games=0,
            fetched_box_scores=0,
            cached_box_scores=82,
        )

    logged_failures: list[tuple[str, str]] = []

    monkeypatch.setattr(cache_season_data, "cache_team_season_data", fake_cache_team_season_data)
    monkeypatch.setattr(
        cache_season_data,
        "append_ingest_failure_log",
        lambda **kwargs: logged_failures.append((kwargs["team"], kwargs["failure_kind"])),
    )
    monkeypatch.setattr(cache_season_data, "resolve_teams", lambda teams: ["BOS", "LAL"])

    exit_code = cache_season_data.main(["2023-24"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert calls == ["BOS", "LAL"]
    assert logged_failures == [("BOS", "fetch_error")]
    assert "Completed with failures across 1 team-seasons: fetch_error=1" in captured.err
    assert "Failed scopes: BOS 2023-24" in captured.err


def test_cache_all_seasons_continues_after_season_failure(monkeypatch, capsys):
    calls: list[str] = []

    def fake_run(command: list[str], check: bool):
        assert check is True
        season = command[-1]
        calls.append(season)
        if season == "2023-24":
            raise subprocess.CalledProcessError(returncode=1, cmd=command)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cache_all_seasons.subprocess, "run", fake_run)

    exit_code = cache_all_seasons.main(["--start-year", "2024", "--first-year", "2023"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert calls == ["2024-25", "2023-24"]
    assert "Season caching failed for 2023-24 with exit status 1." in captured.err
    assert "Completed with failures in 1/2 seasons: 2023-24" in captured.err
