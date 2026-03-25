from __future__ import annotations

import csv
from io import StringIO
from pathlib import Path

from tests.support import (
    TeamSeasonSeed,
    game,
    player,
    seed_db_from_team_seasons,
)
from wowy.data.game_cache.fingerprints import build_normalized_cache_fingerprint
from wowy.data.player_metrics_db import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    PlayerSeasonMetricRow,
    load_metric_rows,
    load_metric_store_metadata,
    replace_metric_scope_store,
)
from wowy.web.app import create_app
from wowy.web.metric_store import (
    RAWR_METRIC,
    WOWY_METRIC,
    WOWY_SHRUNK_METRIC,
    RefreshMetricStoreResult,
    RefreshScopeResult,
    build_scope_key,
    refresh_metric_store,
)
from wowy.web.refresh_cli import main as refresh_cli_main


def _refresh_wowy_store(tmp_path: Path, team_seasons: list[TeamSeasonSeed]) -> Path:
    player_metrics_db_path = tmp_path / "app" / "player_metrics.sqlite3"
    seed_db_from_team_seasons(player_metrics_db_path, team_seasons)
    refresh_metric_store(
        "wowy",
        season_type="Regular Season",
        db_path=player_metrics_db_path,
        source_data_dir=tmp_path / "source",
    )
    return player_metrics_db_path


def _refresh_rawr_store(tmp_path: Path, team_seasons: list[TeamSeasonSeed]) -> Path:
    player_metrics_db_path = tmp_path / "app" / "player_metrics.sqlite3"
    seed_db_from_team_seasons(player_metrics_db_path, team_seasons)
    refresh_metric_store(
        "rawr",
        season_type="Regular Season",
        db_path=player_metrics_db_path,
        source_data_dir=tmp_path / "source",
    )
    return player_metrics_db_path


def _seed_rawr_cache_inputs(
    monkeypatch,
) -> list[TeamSeasonSeed]:
    monkeypatch.setattr(
        "wowy.apps.rawr.data.list_expected_rawr_teams_for_season",
        lambda _season: ["BOS", "LAL", "MIL", "NYK"],
    )
    return [
        (
            "BOS",
            "2023-24",
            [
                game("1", "2023-24", "2024-04-01", "BOS", "MIL", True, 2.0),
                game("2", "2023-24", "2024-04-03", "BOS", "NYK", False, -2.0),
                game("3", "2023-24", "2024-04-05", "BOS", "LAL", True, 0.0),
            ],
            [
                player("1", "BOS", 101, "Player 101", True, 48.0),
                player("2", "BOS", 102, "Player 102", True, 48.0),
                player("3", "BOS", 101, "Player 101", True, 24.0),
                player("3", "BOS", 102, "Player 102", True, 24.0),
            ],
        ),
        (
            "MIL",
            "2023-24",
            [game("1", "2023-24", "2024-04-01", "MIL", "BOS", False, -2.0)],
            [player("1", "MIL", 201, "Player 201", True, 48.0)],
        ),
        (
            "NYK",
            "2023-24",
            [game("2", "2023-24", "2024-04-03", "NYK", "BOS", True, 2.0)],
            [player("2", "NYK", 202, "Player 202", True, 48.0)],
        ),
        (
            "LAL",
            "2023-24",
            [game("3", "2023-24", "2024-04-05", "LAL", "BOS", False, 0.0)],
            [
                player("3", "LAL", 201, "Player 201", True, 24.0),
                player("3", "LAL", 202, "Player 202", True, 24.0),
            ],
        ),
    ]


def _wowy_options_seed() -> list[TeamSeasonSeed]:
    return [
        (
            "BOS",
            "2022-23",
            [game("1", "2022-23", "2023-04-01", "BOS", "MIL", True, 10.0)],
            [player("1", "BOS", 101, "Player 101", True, 34.0)],
        ),
        (
            "BOS",
            "2023-24",
            [game("2", "2023-24", "2024-04-01", "BOS", "MIL", True, 8.0)],
            [player("2", "BOS", 101, "Player 101", True, 35.0)],
        ),
        (
            "NYK",
            "2023-24",
            [game("3", "2023-24", "2024-04-01", "NYK", "BOS", True, 4.0)],
            [player("3", "NYK", 201, "Player 201", True, 33.0)],
        ),
    ]


def _wowy_single_season_seed() -> list[TeamSeasonSeed]:
    return [
        (
            "BOS",
            "2022-23",
            [
                game("1", "2022-23", "2023-04-01", "BOS", "MIL", True, 10.0),
                game("2", "2022-23", "2023-04-03", "BOS", "NYK", False, -5.0),
                game("3", "2022-23", "2023-04-05", "BOS", "LAL", True, 4.0),
            ],
            [
                player("1", "BOS", 101, "Player 101", True, 34.0),
                player("1", "BOS", 102, "Player 102", True, 31.0),
                player("2", "BOS", 102, "Player 102", True, 31.0),
                player("3", "BOS", 101, "Player 101", True, 34.0),
            ],
        ),
    ]


def _wowy_two_season_seed() -> list[TeamSeasonSeed]:
    return _wowy_single_season_seed() + [
        (
            "BOS",
            "2023-24",
            [
                game("4", "2023-24", "2024-04-01", "BOS", "MIL", True, 8.0),
                game("5", "2023-24", "2024-04-03", "BOS", "NYK", False, -2.0),
                game("6", "2023-24", "2024-04-05", "BOS", "LAL", True, 1.0),
            ],
            [
                player("4", "BOS", 101, "Player 101", True, 35.0),
                player("4", "BOS", 103, "Player 103", True, 30.0),
                player("5", "BOS", 101, "Player 101", True, 33.0),
                player("6", "BOS", 103, "Player 103", True, 30.0),
            ],
        ),
    ]


def _wowy_historical_continuity_seed() -> list[TeamSeasonSeed]:
    return [
        (
            "NOH",
            "2002-03",
            [
                game("10", "2002-03", "2003-04-01", "NOH", "BOS", True, 10.0),
                game("11", "2002-03", "2003-04-03", "NOH", "MIL", False, -5.0),
                game("12", "2002-03", "2003-04-05", "NOH", "LAL", True, 4.0),
            ],
            [
                player("10", "NOH", 301, "Player 301", True, 36.0),
                player("10", "NOH", 302, "Player 302", True, 30.0),
                player("11", "NOH", 302, "Player 302", True, 30.0),
                player("12", "NOH", 301, "Player 301", True, 34.0),
            ],
        ),
        (
            "NOP",
            "2013-14",
            [
                game("13", "2013-14", "2014-04-01", "NOP", "LAL", True, 8.0),
                game("14", "2013-14", "2014-04-03", "NOP", "NYK", False, -2.0),
                game("15", "2013-14", "2014-04-05", "NOP", "BOS", True, 1.0),
            ],
            [
                player("13", "NOP", 301, "Player 301", True, 35.0),
                player("13", "NOP", 303, "Player 303", True, 29.0),
                player("14", "NOP", 301, "Player 301", True, 33.0),
                player("15", "NOP", 303, "Player 303", True, 29.0),
            ],
        ),
    ]


def test_refresh_metric_store_builds_rawr_player_season_rows(
    tmp_path: Path,
    monkeypatch,
):
    team_seasons = _seed_rawr_cache_inputs(monkeypatch)
    player_metrics_db_path = _refresh_rawr_store(tmp_path, team_seasons)
    scope_key, _team_filter = build_scope_key(
        team_ids=None,
        season_type="Regular Season",
    )
    rows = load_metric_rows(
        player_metrics_db_path,
        metric="rawr",
        scope_key=scope_key,
        min_sample_size=1,
    )

    assert {row.player_name for row in rows} == {
        "Player 101",
        "Player 102",
        "Player 201",
        "Player 202",
    }
    assert all(row.metric_label == "RAWR" for row in rows)
    assert all(row.season == "2023-24" for row in rows)
    assert all(row.sample_size and row.sample_size >= 1 for row in rows)
    assert all(row.details == {"games": row.sample_size} for row in rows)


def test_refresh_metric_store_skips_incomplete_rawr_seasons_without_scraping(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    team_seasons = _seed_rawr_cache_inputs(monkeypatch)
    player_metrics_db_path = tmp_path / "app" / "player_metrics.sqlite3"
    seed_db_from_team_seasons(player_metrics_db_path, team_seasons[:-1])
    refresh_metric_store(
        "rawr",
        season_type="Regular Season",
        db_path=player_metrics_db_path,
        source_data_dir=tmp_path / "source",
    )
    scope_key, _team_filter = build_scope_key(
        team_ids=None,
        season_type="Regular Season",
    )
    rows = load_metric_rows(
        player_metrics_db_path,
        metric="rawr",
        scope_key=scope_key,
        min_sample_size=1,
    )
    captured = capsys.readouterr()

    assert rows == []
    assert "RAWR warning: skipped incomplete seasons" in captured.out
    assert (
        "2023-24: ERROR: MetaData incomplete/out of date. "
        "Run `poetry run python scripts/cache_season_data.py 2023-24` "
        "or repopulate DB."
    ) not in captured.out
    assert "2023-24: missing team-seasons: LAL" in captured.out
    assert load_metric_store_metadata(player_metrics_db_path, "rawr", scope_key) is None


def test_refresh_metric_store_warns_about_incomplete_rawr_seasons_even_when_cached(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    team_seasons = _seed_rawr_cache_inputs(monkeypatch)
    player_metrics_db_path = tmp_path / "app" / "player_metrics.sqlite3"
    seed_db_from_team_seasons(player_metrics_db_path, team_seasons[:-1])

    refresh_metric_store(
        "rawr",
        season_type="Regular Season",
        db_path=player_metrics_db_path,
        source_data_dir=tmp_path / "source",
    )
    first_run = capsys.readouterr()

    refresh_metric_store(
        "rawr",
        season_type="Regular Season",
        db_path=player_metrics_db_path,
        source_data_dir=tmp_path / "source",
    )
    second_run = capsys.readouterr()

    assert "RAWR warning: skipped incomplete seasons" in first_run.out
    assert "RAWR warning: skipped incomplete seasons" in second_run.out
    assert "2023-24: missing team-seasons: LAL" in second_run.out


def test_refresh_metric_store_can_skip_team_scopes(
    tmp_path: Path,
):
    player_metrics_db_path = tmp_path / "app" / "player_metrics.sqlite3"
    seed_db_from_team_seasons(player_metrics_db_path, _wowy_options_seed())
    refresh_metric_store(
        WOWY_METRIC,
        season_type="Regular Season",
        db_path=player_metrics_db_path,
        source_data_dir=tmp_path / "source",
        include_team_scopes=False,
    )

    all_scope_key, _team_filter = build_scope_key(
        team_ids=None,
        season_type="Regular Season",
    )
    team_scope_key, _team_filter = build_scope_key(
        team_ids=[1610612738],
        season_type="Regular Season",
    )

    assert load_metric_store_metadata(
        player_metrics_db_path,
        WOWY_METRIC,
        all_scope_key,
    ) is not None
    assert (
        load_metric_store_metadata(
            player_metrics_db_path,
            WOWY_METRIC,
            team_scope_key,
        )
        is None
    )


def test_refresh_metric_store_builds_wowy_shrunk_rows(
    tmp_path: Path,
):
    player_metrics_db_path = tmp_path / "app" / "player_metrics.sqlite3"
    seed_db_from_team_seasons(player_metrics_db_path, _wowy_single_season_seed())
    refresh_metric_store(
        WOWY_SHRUNK_METRIC,
        season_type="Regular Season",
        db_path=player_metrics_db_path,
        source_data_dir=tmp_path / "source",
    )
    scope_key, _team_filter = build_scope_key(
        team_ids=None,
        season_type="Regular Season",
    )
    rows = load_metric_rows(
        player_metrics_db_path,
        metric=WOWY_SHRUNK_METRIC,
        scope_key=scope_key,
        min_sample_size=1,
        min_secondary_sample_size=1,
    )
    raw_rows = load_metric_rows(
        player_metrics_db_path,
        metric=WOWY_METRIC,
        scope_key=scope_key,
        min_sample_size=1,
        min_secondary_sample_size=1,
    )

    assert {row.player_name for row in rows} == {"Player 101", "Player 102"}
    assert all(row.metric_label == "WOWY Shrunk" for row in rows)
    assert all(abs(row.details["raw_wowy_score"]) > abs(row.value) for row in rows)
    assert all(row.details["raw_wowy_score"] * row.value >= 0 for row in rows)
    assert raw_rows == []


def test_refresh_cli_refreshes_all_metrics_by_default(monkeypatch, tmp_path: Path, capsys):
    calls: list[str] = []

    def fake_refresh_metric_store(
        metric: str,
        *,
        season_type: str,
        db_path: Path,
        source_data_dir: Path,
        rawr_ridge_alpha: float,
        include_team_scopes: bool,
        progress,
    ) -> RefreshMetricStoreResult:
        calls.append(metric)
        progress(1, 1, "done")
        return RefreshMetricStoreResult(
            metric=metric,
            scope_results=[
                RefreshScopeResult(
                    scope_key="team_ids=all-teams|season_type=Regular Season",
                    scope_label="all-teams",
                    row_count=1,
                    status="built",
                )
            ],
            warnings=[],
        )

    monkeypatch.setattr("wowy.web.refresh_cli.refresh_metric_store", fake_refresh_metric_store)

    exit_code = refresh_cli_main(
        ["--player-metrics-db-path", str(tmp_path / "app" / "player_metrics.sqlite3")]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert calls == [WOWY_METRIC, WOWY_SHRUNK_METRIC, RAWR_METRIC]
    assert "refreshed wowy store" in captured.out
    assert "refreshed wowy_shrunk store" in captured.out
    assert "refreshed rawr store" in captured.out


def test_refresh_cli_fails_for_empty_all_teams_rawr_store(
    monkeypatch,
    tmp_path: Path,
    capsys,
):
    def fake_refresh_metric_store(
        metric: str,
        *,
        season_type: str,
        db_path: Path,
        source_data_dir: Path,
        rawr_ridge_alpha: float,
        include_team_scopes: bool,
        progress,
    ) -> RefreshMetricStoreResult:
        progress(1, 1, "empty all-teams")
        if metric != RAWR_METRIC:
            return RefreshMetricStoreResult(
                metric=metric,
                scope_results=[
                    RefreshScopeResult(
                        scope_key="team_ids=all-teams|season_type=Regular Season",
                        scope_label="all-teams",
                        row_count=5,
                        status="built",
                    )
                ],
                warnings=[],
            )
        return RefreshMetricStoreResult(
            metric=metric,
            scope_results=[
                RefreshScopeResult(
                    scope_key="team_ids=all-teams|season_type=Regular Season",
                    scope_label="all-teams",
                    row_count=0,
                    status="empty",
                )
            ],
            warnings=["2023-24: missing team-seasons: LAL"],
            failure_message=(
                "RAWR refresh produced no all-teams rows. "
                "The normalized cache is incomplete, so the web store was not updated."
            ),
        )

    monkeypatch.setattr("wowy.web.refresh_cli.refresh_metric_store", fake_refresh_metric_store)

    exit_code = refresh_cli_main(
        ["--player-metrics-db-path", str(tmp_path / "app" / "player_metrics.sqlite3")]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "failed to refresh rawr store" in captured.out
    assert "RAWR refresh produced no all-teams rows." in captured.out


def test_wowy_shrunk_options_endpoint_returns_wowy_style_filters(
    tmp_path: Path,
):
    player_metrics_db_path = tmp_path / "app" / "player_metrics.sqlite3"
    seed_db_from_team_seasons(player_metrics_db_path, _wowy_options_seed())
    refresh_metric_store(
        WOWY_SHRUNK_METRIC,
        season_type="Regular Season",
        db_path=player_metrics_db_path,
        source_data_dir=tmp_path / "source",
    )

    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get(
        "/api/metrics/wowy_shrunk/options",
        query_string={"team_id": "1610612738"},
    )

    assert response.status_code == 200
    assert response.get_json() == {
        "metric": "wowy_shrunk",
        "metric_label": "WOWY Shrunk",
        "available_teams": ["BOS", "NYK"],
        "team_options": [
            {"team_id": 1610612738, "label": "BOS", "available_seasons": ["2022-23", "2023-24"]},
            {"team_id": 1610612752, "label": "NYK", "available_seasons": ["2023-24"]},
        ],
        "available_seasons": ["2022-23", "2023-24"],
        "available_teams_by_season": {
            "2022-23": ["BOS"],
            "2023-24": ["BOS", "NYK"],
        },
        "filters": {
            "team": None,
            "team_id": [1610612738],
            "season_type": "Regular Season",
            "min_games_with": 15,
            "min_games_without": 2,
            "min_average_minutes": 30.0,
            "min_total_minutes": 600.0,
            "top_n": 30,
        },
    }


def test_options_endpoint_canonicalizes_lowercase_season_type(
    tmp_path: Path,
):
    player_metrics_db_path = tmp_path / "app" / "player_metrics.sqlite3"
    seed_db_from_team_seasons(player_metrics_db_path, _wowy_options_seed())
    refresh_metric_store(
        WOWY_METRIC,
        season_type="regular season",
        db_path=player_metrics_db_path,
        source_data_dir=tmp_path / "source",
    )

    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get(
        "/api/metrics/wowy/options",
        query_string={"season_type": "regular season"},
    )

    assert response.status_code == 200
    assert response.get_json()["filters"]["season_type"] == "Regular Season"


def test_refresh_metric_store_skips_empty_historical_rawr_team_seasons(
    tmp_path: Path,
    monkeypatch,
):
    team_seasons = _seed_rawr_cache_inputs(monkeypatch)
    team_seasons.append(("BKN", "2008-09", [], []))
    player_metrics_db_path = _refresh_rawr_store(tmp_path, team_seasons)
    scope_key, _team_filter = build_scope_key(
        team_ids=[1610612751],
        season_type="Regular Season",
    )
    rows = load_metric_rows(
        player_metrics_db_path,
        metric="rawr",
        scope_key=scope_key,
        min_sample_size=1,
    )

    assert rows == []


def test_rawr_options_endpoint_returns_metric_specific_filters(
    tmp_path: Path,
    monkeypatch,
):
    team_seasons = _seed_rawr_cache_inputs(monkeypatch)
    player_metrics_db_path = _refresh_rawr_store(tmp_path, team_seasons)

    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get("/api/metrics/rawr/options", query_string={"team_id": "1610612738"})

    assert response.status_code == 200
    assert response.get_json() == {
        "metric": "rawr",
        "metric_label": "RAWR",
        "available_teams": ["BOS", "LAL", "MIL", "NYK"],
        "team_options": [
            {"team_id": 1610612738, "label": "BOS", "available_seasons": ["2023-24"]},
            {"team_id": 1610612747, "label": "LAL", "available_seasons": ["2023-24"]},
            {"team_id": 1610612749, "label": "MIL", "available_seasons": ["2023-24"]},
            {"team_id": 1610612752, "label": "NYK", "available_seasons": ["2023-24"]},
        ],
        "available_seasons": ["2023-24"],
        "available_teams_by_season": {
            "2023-24": ["BOS", "LAL", "MIL", "NYK"],
        },
        "filters": {
            "team": None,
            "team_id": [1610612738],
            "season_type": "Regular Season",
            "min_games": 35,
            "ridge_alpha": 10.0,
            "min_average_minutes": 30.0,
            "min_total_minutes": 600.0,
            "top_n": 30,
        },
    }


def test_rawr_player_seasons_endpoint_accepts_metric_specific_filters(
    tmp_path: Path,
    monkeypatch,
):
    team_seasons = _seed_rawr_cache_inputs(monkeypatch)
    player_metrics_db_path = _refresh_rawr_store(tmp_path, team_seasons)

    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get(
        "/api/metrics/rawr/player-seasons",
        query_string={
            "min_games": "1",
            "ridge_alpha": "2.5",
            "min_average_minutes": "0",
            "min_total_minutes": "0",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["metric"] == "rawr"
    assert payload["metric_label"] == "RAWR"
    assert payload["filters"] == {
        "team": None,
        "team_id": None,
        "season": None,
        "season_type": "Regular Season",
        "min_games": 1,
        "ridge_alpha": 2.5,
        "min_average_minutes": 0.0,
        "min_total_minutes": 0.0,
        "top_n": 30,
    }
    assert {row["player_name"] for row in payload["rows"]} == {
        "Player 101",
        "Player 102",
        "Player 201",
        "Player 202",
    }
    assert all(row["season"] == "2023-24" for row in payload["rows"])
    assert all(row["sample_size"] == 2 for row in payload["rows"])
    assert all(row["secondary_sample_size"] is None for row in payload["rows"])
    assert all(row["games"] == 2 for row in payload["rows"])
    assert all(row["average_minutes"] == 36.0 for row in payload["rows"])
    assert all(row["total_minutes"] == 72.0 for row in payload["rows"])


def test_rawr_player_seasons_endpoint_rejects_invalid_filters(
    tmp_path: Path,
):
    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=tmp_path / "app" / "player_metrics.sqlite3",
    )
    client = app.test_client()

    response = client.get(
        "/api/metrics/rawr/player-seasons",
        query_string={"min_games": "-1"},
    )

    assert response.status_code == 400
    assert response.get_json() == {"error": "Minimum games filter must be non-negative"}


def test_rawr_cached_leaderboard_endpoint_returns_cached_series(
    tmp_path: Path,
    monkeypatch,
):
    team_seasons = _seed_rawr_cache_inputs(monkeypatch)
    player_metrics_db_path = _refresh_rawr_store(tmp_path, team_seasons)

    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get(
        "/api/metrics/rawr/cached-leaderboard",
        query_string={
            "top_n": "2",
            "min_games": "1",
            "ridge_alpha": "7.5",
            "min_average_minutes": "0",
            "min_total_minutes": "0",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["metric"] == "rawr"
    assert payload["mode"] == "cached"
    assert payload["span"] == {
        "start_season": "2023-24",
        "end_season": "2023-24",
        "available_seasons": ["2023-24"],
        "top_n": 2,
    }
    assert len(payload["table_rows"]) == 2
    assert len(payload["series"]) == 2


def test_rawr_custom_query_endpoint_recalculates_requested_span(
    tmp_path: Path,
    monkeypatch,
):
    team_seasons = _seed_rawr_cache_inputs(monkeypatch)
    seed_db_from_team_seasons(
        tmp_path / "app" / "player_metrics.sqlite3",
        team_seasons,
    )

    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=tmp_path / "app" / "player_metrics.sqlite3",
    )
    client = app.test_client()

    response = client.get(
        "/api/metrics/rawr/custom-query",
        query_string={
            "team_id": "1610612738",
            "season": ["2023-24"],
            "top_n": "3",
            "min_games": "1",
            "ridge_alpha": "4.0",
            "min_average_minutes": "0",
            "min_total_minutes": "0",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["metric"] == "rawr"
    assert payload["metric_label"] == "RAWR"
    assert payload["mode"] == "custom"
    assert payload["filters"] == {
        "team": None,
        "team_id": [1610612738],
        "season": ["2023-24"],
        "season_type": "Regular Season",
        "min_games": 1,
        "ridge_alpha": 4.0,
        "min_average_minutes": 0.0,
        "min_total_minutes": 0.0,
        "top_n": 3,
    }
    assert payload["span"] == {
        "start_season": "2023-24",
        "end_season": "2023-24",
        "available_seasons": ["2023-24"],
        "top_n": 3,
    }
    assert len(payload["table_rows"]) == 3
    assert all(row["season_count"] == 1 for row in payload["table_rows"])
    assert all(
        point["season"] == "2023-24"
        for row in payload["series"]
        for point in row["points"]
    )


def test_rawr_custom_query_endpoint_rejects_invalid_filters(tmp_path: Path):
    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=tmp_path / "app" / "player_metrics.sqlite3",
    )
    client = app.test_client()

    response = client.get(
        "/api/metrics/rawr/custom-query",
        query_string={"min_games": "-1"},
    )

    assert response.status_code == 400
    assert response.get_json() == {"error": "Minimum games filter must be non-negative"}


def test_rawr_custom_query_skips_seasons_without_qualifying_players(
    tmp_path: Path,
    monkeypatch,
):
    team_seasons = _seed_rawr_cache_inputs(monkeypatch)
    team_seasons.extend(
        [
            (
                "BOS",
                "2024-25",
                [game("4", "2024-25", "2025-04-01", "BOS", "MIL", True, 3.0)],
                [player("4", "BOS", 101, "Player 101", True, 36.0)],
            ),
            (
                "MIL",
                "2024-25",
                [game("4", "2024-25", "2025-04-01", "MIL", "BOS", False, -3.0)],
                [player("4", "MIL", 201, "Player 201", True, 36.0)],
            ),
        ]
    )
    seed_db_from_team_seasons(
        tmp_path / "app" / "player_metrics.sqlite3",
        team_seasons,
    )

    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=tmp_path / "app" / "player_metrics.sqlite3",
    )
    client = app.test_client()

    response = client.get(
        "/api/metrics/rawr/custom-query",
        query_string={
            "team_id": "1610612738",
            "season": ["2023-24", "2024-25"],
            "top_n": "3",
            "min_games": "2",
            "ridge_alpha": "3.0",
            "min_average_minutes": "0",
            "min_total_minutes": "0",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["mode"] == "custom"
    assert payload["filters"]["team"] is None
    assert payload["filters"]["team_id"] == [1610612738]
    assert payload["filters"]["ridge_alpha"] == 3.0
    assert payload["span"] == {
        "start_season": "2023-24",
        "end_season": "2023-24",
        "available_seasons": ["2023-24"],
        "top_n": 3,
    }
    assert len(payload["table_rows"]) == 3


def test_wowy_options_endpoint_returns_cached_teams_and_seasons(
    tmp_path: Path,
):
    player_metrics_db_path = _refresh_wowy_store(tmp_path, _wowy_options_seed())

    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get("/api/wowy/options", query_string={"team_id": "1610612738"})

    assert response.status_code == 200
    assert response.get_json() == {
        "metric": "wowy",
        "metric_label": "WOWY",
        "available_teams": ["BOS", "NYK"],
        "team_options": [
            {"team_id": 1610612738, "label": "BOS", "available_seasons": ["2022-23", "2023-24"]},
            {"team_id": 1610612752, "label": "NYK", "available_seasons": ["2023-24"]},
        ],
        "available_seasons": ["2022-23", "2023-24"],
        "available_teams_by_season": {
            "2022-23": ["BOS"],
            "2023-24": ["BOS", "NYK"],
        },
        "filters": {
            "team": None,
            "team_id": [1610612738],
            "season_type": "Regular Season",
            "min_games_with": 15,
            "min_games_without": 2,
            "min_average_minutes": 30.0,
            "min_total_minutes": 600.0,
            "top_n": 30,
        },
    }


def test_wowy_player_seasons_endpoint_returns_rows_from_cache(
    tmp_path: Path,
):
    player_metrics_db_path = _refresh_wowy_store(tmp_path, _wowy_single_season_seed())

    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get(
        "/api/wowy/player-seasons",
        query_string={
            "team_id": "1610612738",
            "min_games_with": "1",
            "min_games_without": "1",
            "min_average_minutes": "0",
            "min_total_minutes": "0",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload == {
        "metric": "wowy",
        "metric_label": "WOWY",
        "filters": {
            "team": None,
            "team_id": [1610612738],
            "season": None,
            "season_type": "Regular Season",
            "min_games_with": 1,
            "min_games_without": 1,
            "min_average_minutes": 0.0,
            "min_total_minutes": 0.0,
            "top_n": 30,
        },
        "rows": [
            {
                "season": "2022-23",
                "player_id": 101,
                "player_name": "Player 101",
                "value": 12.0,
                "sample_size": 2,
                "secondary_sample_size": 1,
                "games_with": 2,
                "games_without": 1,
                "avg_margin_with": 7.0,
                "avg_margin_without": -5.0,
                "average_minutes": 34.0,
                "total_minutes": 68.0,
            },
            {
                "season": "2022-23",
                "player_id": 102,
                "player_name": "Player 102",
                "value": -1.5,
                "sample_size": 2,
                "secondary_sample_size": 1,
                "games_with": 2,
                "games_without": 1,
                "avg_margin_with": 2.5,
                "avg_margin_without": 4.0,
                "average_minutes": 31.0,
                "total_minutes": 62.0,
            },
        ],
    }


def test_wowy_player_seasons_endpoint_returns_bad_request_for_invalid_filters(
    tmp_path: Path,
):
    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=tmp_path / "app" / "player_metrics.sqlite3",
    )
    client = app.test_client()

    response = client.get(
        "/api/wowy/player-seasons",
        query_string={"min_games_with": "-1"},
    )

    assert response.status_code == 400
    assert response.get_json() == {"error": "Minimum game filters must be non-negative"}


def test_wowy_options_endpoint_requires_prebuilt_store(tmp_path: Path):
    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=tmp_path / "app" / "player_metrics.sqlite3",
    )
    client = app.test_client()

    response = client.get("/api/wowy/options")

    assert response.status_code == 400
    assert response.get_json() == {
        "error": "Metric store has not been built for the requested scope"
    }


def test_wowy_span_chart_endpoint_returns_series_for_selected_span(
    tmp_path: Path,
):
    player_metrics_db_path = _refresh_wowy_store(tmp_path, _wowy_two_season_seed())

    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get(
        "/api/wowy/span-chart",
        query_string={
            "team_id": "1610612738",
            "top_n": "2",
            "min_games_with": "1",
            "min_games_without": "1",
            "min_average_minutes": "0",
            "min_total_minutes": "0",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["metric"] == "wowy"
    assert payload["metric_label"] == "WOWY"
    assert payload["span"] == {
        "start_season": "2022-23",
        "end_season": "2023-24",
        "available_seasons": ["2022-23", "2023-24"],
        "top_n": 2,
    }
    assert payload["series"] == [
        {
            "player_id": 101,
            "player_name": "Player 101",
            "span_average_value": 7.0,
            "season_count": 2,
            "points": [
                {"season": "2022-23", "value": 12.0},
                {"season": "2023-24", "value": 2.0},
            ],
        },
        {
            "player_id": 103,
            "player_name": "Player 103",
            "span_average_value": 3.25,
            "season_count": 1,
            "points": [
                {"season": "2022-23", "value": None},
                {"season": "2023-24", "value": 6.5},
            ],
        },
    ]


def test_wowy_cached_leaderboard_endpoint_returns_server_ranked_rows(
    tmp_path: Path,
):
    player_metrics_db_path = _refresh_wowy_store(tmp_path, _wowy_two_season_seed())

    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get(
        "/api/wowy/cached-leaderboard",
        query_string={
            "team_id": "1610612738",
            "top_n": "2",
            "min_games_with": "1",
            "min_games_without": "1",
            "min_average_minutes": "0",
            "min_total_minutes": "0",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["mode"] == "cached"
    assert payload["span"] == {
        "start_season": "2022-23",
        "end_season": "2023-24",
        "available_seasons": ["2022-23", "2023-24"],
        "top_n": 2,
    }
    assert payload["table_rows"][0]["player_id"] == 101
    assert payload["table_rows"][0]["rank"] == 1
    assert payload["series"][0]["player_id"] == 101
    assert payload["series"][0]["points"] == [
        {"season": "2022-23", "value": 12.0},
        {"season": "2023-24", "value": 2.0},
    ]


def test_wowy_cached_leaderboard_csv_exports_all_players_ignoring_top_n(
    tmp_path: Path,
):
    player_metrics_db_path = _refresh_wowy_store(tmp_path, _wowy_two_season_seed())

    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get(
        "/api/metrics/wowy/cached-leaderboard.csv",
        query_string={
            "team_id": "1610612738",
            "top_n": "1",
            "min_games_with": "1",
            "min_games_without": "1",
            "min_average_minutes": "0",
            "min_total_minutes": "0",
        },
    )

    assert response.status_code == 200
    assert response.mimetype == "text/csv"
    assert "wowy-all-players.csv" in response.headers["Content-Disposition"]
    rows = list(csv.reader(StringIO(response.get_data(as_text=True))))
    assert rows[0] == [
        "Rank",
        "Player ID",
        "Player",
        "WOWY",
        "Avg Min",
        "Tot Min",
        "With",
        "Without",
        "Avg With",
        "Avg Without",
        "Seasons",
        "Points",
    ]
    assert len(rows) == 4
    assert [row[2] for row in rows[1:]] == ["Player 101", "Player 103", "Player 102"]


def test_wowy_custom_query_endpoint_recalculates_requested_span(
    tmp_path: Path,
):
    seed_db_from_team_seasons(
        tmp_path / "app" / "player_metrics.sqlite3",
        _wowy_two_season_seed(),
    )

    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=tmp_path / "app" / "player_metrics.sqlite3",
    )
    client = app.test_client()

    response = client.get(
        "/api/wowy/custom-query",
        query_string={
            "team_id": "1610612738",
            "season": ["2022-23"],
            "top_n": "5",
            "min_games_with": "1",
            "min_games_without": "1",
            "min_average_minutes": "0",
            "min_total_minutes": "0",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["mode"] == "custom"
    assert payload["filters"]["team"] is None
    assert payload["filters"]["team_id"] == [1610612738]
    assert payload["span"] == {
        "start_season": "2022-23",
        "end_season": "2022-23",
        "available_seasons": ["2022-23"],
        "top_n": 5,
    }
    assert payload["table_rows"] == [
        {
            "rank": 1,
            "player_id": 101,
            "player_name": "Player 101",
            "span_average_value": 12.0,
            "average_minutes": 34.0,
            "total_minutes": 68.0,
            "games_with": 2,
            "games_without": 1,
            "avg_margin_with": 7.0,
            "avg_margin_without": -5.0,
            "season_count": 1,
            "points": [{"season": "2022-23", "value": 12.0}],
        },
        {
            "rank": 2,
            "player_id": 102,
            "player_name": "Player 102",
            "span_average_value": -1.5,
            "average_minutes": 31.0,
            "total_minutes": 62.0,
            "games_with": 2,
            "games_without": 1,
            "avg_margin_with": 2.5,
            "avg_margin_without": 4.0,
            "season_count": 1,
            "points": [{"season": "2022-23", "value": -1.5}],
        },
    ]


def test_rawr_custom_query_csv_exports_all_players_ignoring_top_n(
    tmp_path: Path,
    monkeypatch,
):
    team_seasons = _seed_rawr_cache_inputs(monkeypatch)
    seed_db_from_team_seasons(
        tmp_path / "app" / "player_metrics.sqlite3",
        team_seasons,
    )

    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=tmp_path / "app" / "player_metrics.sqlite3",
    )
    client = app.test_client()

    response = client.get(
        "/api/metrics/rawr/custom-query.csv",
        query_string={
            "team_id": "1610612738",
            "season": ["2023-24"],
            "top_n": "1",
            "min_games": "1",
            "ridge_alpha": "4.0",
            "min_average_minutes": "0",
            "min_total_minutes": "0",
        },
    )

    assert response.status_code == 200
    assert response.mimetype == "text/csv"
    assert "rawr-all-players.csv" in response.headers["Content-Disposition"]
    rows = list(csv.reader(StringIO(response.get_data(as_text=True))))
    assert rows[0] == [
        "Rank",
        "Player ID",
        "Player",
        "RAWR",
        "Avg Min",
        "Tot Min",
        "With",
        "Without",
        "Avg With",
        "Avg Without",
        "Seasons",
        "Points",
    ]
    assert len(rows) == 5
    assert {row[2] for row in rows[1:]} == {
        "Player 101",
        "Player 102",
        "Player 201",
        "Player 202",
    }


def test_wowy_options_endpoint_returns_team_id_team_options_for_frontend(
    tmp_path: Path,
):
    player_metrics_db_path = tmp_path / "app" / "player_metrics.sqlite3"
    seed_db_from_team_seasons(player_metrics_db_path, _wowy_historical_continuity_seed())
    scope_key, _team_filter = build_scope_key(
        team_ids=None,
        season_type="Regular Season",
    )
    replace_metric_scope_store(
        player_metrics_db_path,
        metric=WOWY_METRIC,
        scope_key=scope_key,
        metric_label="WOWY",
        build_version="test-options",
        source_fingerprint=build_normalized_cache_fingerprint(
            player_metrics_db_path,
            season_type="Regular Season",
        ),
        rows=[
            PlayerSeasonMetricRow(
                metric=WOWY_METRIC,
                metric_label="WOWY",
                scope_key=scope_key,
                team_filter="",
                season_type="Regular Season",
                season="2002-03",
                player_id=301,
                player_name="Player 301",
                value=1.0,
                sample_size=1,
                secondary_sample_size=1,
                average_minutes=36.0,
                total_minutes=36.0,
                details={},
            )
        ],
        catalog_row=MetricScopeCatalogRow(
            metric=WOWY_METRIC,
            scope_key=scope_key,
            metric_label="WOWY",
            team_filter="",
            season_type="Regular Season",
            available_seasons=["2002-03", "2013-14"],
            available_teams=["NOH", "NOP"],
            full_span_start_season="2002-03",
            full_span_end_season="2013-14",
            updated_at="2026-03-24T00:00:00+00:00",
        ),
        series_rows=[
            MetricFullSpanSeriesRow(
                metric=WOWY_METRIC,
                scope_key=scope_key,
                player_id=301,
                player_name="Player 301",
                span_average_value=1.0,
                season_count=1,
                rank_order=1,
            )
        ],
        point_rows=[
            MetricFullSpanPointRow(
                metric=WOWY_METRIC,
                scope_key=scope_key,
                player_id=301,
                season="2002-03",
                value=1.0,
            )
        ],
    )

    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get("/api/wowy/options")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["team_options"] == [
        {"team_id": 1610612740, "label": "NOP", "available_seasons": ["2002-03", "2013-14"]},
    ]


def test_wowy_custom_query_endpoint_accepts_team_id_for_historical_multi_season_scope(
    tmp_path: Path,
):
    seed_db_from_team_seasons(
        tmp_path / "app" / "player_metrics.sqlite3",
        _wowy_historical_continuity_seed(),
    )

    app = create_app(
        source_data_dir=tmp_path / "source",
        player_metrics_db_path=tmp_path / "app" / "player_metrics.sqlite3",
    )
    client = app.test_client()

    response = client.get(
        "/api/wowy/custom-query",
        query_string={
            "team_id": "1610612740",
            "season": ["2002-03", "2013-14"],
            "top_n": "5",
            "min_games_with": "1",
            "min_games_without": "1",
            "min_average_minutes": "0",
            "min_total_minutes": "0",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["filters"]["team"] is None
    assert payload["filters"]["team_id"] == [1610612740]
    assert payload["span"] == {
        "start_season": "2002-03",
        "end_season": "2013-14",
        "available_seasons": ["2002-03", "2013-14"],
        "top_n": 5,
    }
    assert {row["player_id"] for row in payload["table_rows"]} == {301, 302, 303}
