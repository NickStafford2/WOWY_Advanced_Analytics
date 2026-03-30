from __future__ import annotations

from rawr_analytics.nba.build_models import TeamSeasonRunSummary
from rawr_analytics.nba.errors import (
    BoxScoreFetchError,
    GameNormalizationFailure,
    PartialTeamSeasonError,
)
from scripts import cache_season_data


def test_cache_season_data_continues_after_team_failure(monkeypatch, capsys):
    calls: list[str] = []

    def fake_refresh_normalized_team_season_cache(**kwargs):
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

    monkeypatch.setattr(
        cache_season_data,
        "refresh_normalized_team_season_cache",
        fake_refresh_normalized_team_season_cache,
    )
    monkeypatch.setattr(
        cache_season_data,
        "append_ingest_failure_log",
        lambda **kwargs: logged_failures.append((kwargs["team"], kwargs["failure_kind"])),
    )
    monkeypatch.setattr(cache_season_data, "_resolve_teams", lambda teams, season: ["BOS", "LAL"])
    monkeypatch.setattr(cache_season_data, "team_is_active_for_season", lambda team, season: True)

    exit_code = cache_season_data.main(["2023-24"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert calls == ["BOS", "LAL"]
    assert logged_failures == [("BOS", "fetch_error")]
    assert "ERROR: season cache finished with 1 failed team-seasons" in captured.err
    assert "Completed with failures across 1 team-seasons: fetch_error=1" in captured.err
    assert "Failed scopes: BOS 2023-24" in captured.err

def test_filtered_log_only_emits_actionable_cache_messages(capsys):
    cache_season_data._filtered_log("api box_score 0001 attempt=1")
    cache_season_data._filtered_log("cache discard path=foo reason=invalid_or_empty_payload")
    cache_season_data._filtered_log("cache skip path=bar reason=unparseable_box_score_payload")

    captured = capsys.readouterr()
    assert "api box_score" not in captured.err
    assert "cache discard path=foo reason=invalid_or_empty_payload" in captured.err
    assert "cache skip path=bar reason=unparseable_box_score_payload" in captured.err


def test_cache_season_data_skips_requested_team_not_active_in_season(monkeypatch, capsys):
    called = False

    def fake_refresh_normalized_team_season_cache():
        nonlocal called
        called = True
        raise AssertionError("inactive team-season should not be fetched")

    monkeypatch.setattr(
        cache_season_data,
        "refresh_normalized_team_season_cache",
        fake_refresh_normalized_team_season_cache,
    )
    monkeypatch.setattr(cache_season_data, "team_is_active_for_season", lambda team, _: False)

    exit_code = cache_season_data.main(["2002-03", "--teams", "CHA"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert called is False
    assert "CHA 2002-03 skipped not-active-in-season" in captured.out


def test_render_partial_failure_details_includes_short_per_game_examples() -> None:
    error = PartialTeamSeasonError(
        message="partial",
        team="NYK",
        season="2006-07",
        season_type="Regular Season",
        failed_game_ids=["0020600382", "0020600368"],
        total_games=82,
        failed_games=2,
        failed_game_details=[
            GameNormalizationFailure(
                game_id="0020600382",
                error_type="ValueError",
                message=(
                    "Unparseable MIN value; nba_api_box_score_player_row="
                    '{"COMMENT": "OUT - Sprained ankle", "MIN": null, '
                    '"PLAYER_NAME": "David Newble", "TEAM_ABBREVIATION": "CHI"}'
                ),
            ),
            GameNormalizationFailure(
                game_id="0020600368",
                error_type="ValueError",
                message=(
                    "Unparseable MIN value; nba_api_box_score_player_row="
                    '{"MIN": "bogus", "PLAYER_NAME": "Example Player", '
                    '"TEAM_ABBREVIATION": "CHA"}'
                ),
            ),
        ],
        failure_reason_counts={"ValueError: Unparseable MIN value": 2},
        failure_reason_examples={
            "ValueError: Unparseable MIN value": ["0020600382", "0020600368"],
        },
    )

    rendered = cache_season_data._render_partial_failure_details(error)

    assert "Failure reasons:" in rendered
    assert "  - 2 games: ValueError: Unparseable MIN value" in rendered
    assert (
        "    0020600382: Unparseable MIN value "
        "(player='David Newble', min=None, comment='OUT - Sprained ankle', team_abbreviation='CHI')"
        in rendered
    )
    assert (
        "    0020600368: Unparseable MIN value "
        "(player='Example Player', min='bogus', team_abbreviation='CHA')" in rendered
    )
    assert "examples=" not in rendered
    assert "nba_api_box_score_player_row" not in rendered
