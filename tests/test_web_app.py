from __future__ import annotations

from pathlib import Path

from wowy.data.player_metrics_db import load_metric_rows
from wowy.web.app import create_app
from wowy.web.service import build_scope_key, refresh_metric_store


def _refresh_wowy_store(tmp_path: Path) -> Path:
    player_metrics_db_path = tmp_path / "app" / "player_metrics.sqlite3"
    refresh_metric_store(
        "wowy",
        season_type="Regular Season",
        db_path=player_metrics_db_path,
        source_data_dir=tmp_path / "source",
        normalized_games_input_dir=tmp_path / "normalized_games",
        normalized_game_players_input_dir=tmp_path / "normalized_game_players",
        wowy_output_dir=tmp_path / "team_games",
        combined_wowy_csv=tmp_path / "combined" / "games.csv",
    )
    return player_metrics_db_path


def _refresh_rawr_store(tmp_path: Path) -> Path:
    player_metrics_db_path = tmp_path / "app" / "player_metrics.sqlite3"
    refresh_metric_store(
        "rawr",
        season_type="Regular Season",
        db_path=player_metrics_db_path,
        source_data_dir=tmp_path / "source",
        normalized_games_input_dir=tmp_path / "normalized_games",
        normalized_game_players_input_dir=tmp_path / "normalized_game_players",
        wowy_output_dir=tmp_path / "team_games",
        combined_wowy_csv=tmp_path / "combined" / "wowy" / "games.csv",
        combined_rawr_games_csv=tmp_path / "combined" / "rawr" / "games.csv",
        combined_rawr_game_players_csv=tmp_path / "combined" / "rawr" / "game_players.csv",
    )
    return player_metrics_db_path


def _seed_rawr_cache_inputs(
    tmp_path: Path,
    monkeypatch,
) -> tuple[Path, Path]:
    normalized_games_dir = tmp_path / "normalized_games"
    normalized_games_dir.mkdir()
    (normalized_games_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,2,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,BOS,NYK,false,-2,Regular Season,nba_api\n"
            "3,2023-24,2024-04-05,BOS,LAL,true,0,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    (normalized_games_dir / "MIL_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,MIL,BOS,false,-2,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    (normalized_games_dir / "NYK_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "2,2023-24,2024-04-03,NYK,BOS,true,2,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    (normalized_games_dir / "LAL_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "3,2023-24,2024-04-05,LAL,BOS,false,0,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    normalized_players_dir = tmp_path / "normalized_game_players"
    normalized_players_dir.mkdir()
    (normalized_players_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,48\n"
            "2,BOS,102,Player 102,true,48\n"
            "3,BOS,101,Player 101,true,24\n"
            "3,BOS,102,Player 102,true,24\n"
        ),
        encoding="utf-8",
    )
    (normalized_players_dir / "MIL_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,MIL,201,Player 201,true,48\n"
        ),
        encoding="utf-8",
    )
    (normalized_players_dir / "NYK_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "2,NYK,202,Player 202,true,48\n"
        ),
        encoding="utf-8",
    )
    (normalized_players_dir / "LAL_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "3,LAL,201,Player 201,true,24\n"
            "3,LAL,202,Player 202,true,24\n"
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "wowy.nba.prepare.ensure_team_season_data",
        lambda **_kwargs: None,
    )
    return normalized_games_dir, normalized_players_dir


def test_refresh_metric_store_builds_rawr_player_season_rows(
    tmp_path: Path,
    monkeypatch,
):
    _seed_rawr_cache_inputs(tmp_path, monkeypatch)

    player_metrics_db_path = _refresh_rawr_store(tmp_path)
    scope_key, _team_filter = build_scope_key(
        teams=None,
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


def test_rawr_options_endpoint_returns_metric_specific_filters(
    tmp_path: Path,
    monkeypatch,
):
    normalized_games_dir, normalized_players_dir = _seed_rawr_cache_inputs(
        tmp_path,
        monkeypatch,
    )
    player_metrics_db_path = _refresh_rawr_store(tmp_path)

    app = create_app(
        source_data_dir=tmp_path / "source",
        normalized_games_input_dir=normalized_games_dir,
        normalized_game_players_input_dir=normalized_players_dir,
        wowy_output_dir=tmp_path / "team_games",
        combined_wowy_csv=tmp_path / "combined" / "games.csv",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get("/api/metrics/rawr/options", query_string={"team": "BOS"})

    assert response.status_code == 200
    assert response.get_json() == {
        "metric": "rawr",
        "metric_label": "RAWR",
        "available_teams": ["BOS", "LAL", "MIL", "NYK"],
        "available_seasons": ["2023-24"],
        "filters": {
            "team": ["BOS"],
            "season_type": "Regular Season",
            "min_games": 35,
            "min_average_minutes": 30.0,
            "min_total_minutes": 600.0,
            "top_n": 30,
        },
    }


def test_rawr_player_seasons_endpoint_accepts_metric_specific_filters(
    tmp_path: Path,
    monkeypatch,
):
    normalized_games_dir, normalized_players_dir = _seed_rawr_cache_inputs(
        tmp_path,
        monkeypatch,
    )
    player_metrics_db_path = _refresh_rawr_store(tmp_path)

    app = create_app(
        source_data_dir=tmp_path / "source",
        normalized_games_input_dir=normalized_games_dir,
        normalized_game_players_input_dir=normalized_players_dir,
        wowy_output_dir=tmp_path / "team_games",
        combined_wowy_csv=tmp_path / "combined" / "games.csv",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get(
        "/api/metrics/rawr/player-seasons",
        query_string={
            "min_games": "1",
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
        "season": None,
        "season_type": "Regular Season",
        "min_games": 1,
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
        normalized_games_input_dir=tmp_path / "normalized_games",
        normalized_game_players_input_dir=tmp_path / "normalized_game_players",
        wowy_output_dir=tmp_path / "team_games",
        combined_wowy_csv=tmp_path / "combined" / "games.csv",
        player_metrics_db_path=tmp_path / "app" / "player_metrics.sqlite3",
    )
    client = app.test_client()

    response = client.get(
        "/api/metrics/rawr/player-seasons",
        query_string={"min_games": "-1"},
    )

    assert response.status_code == 400
    assert response.get_json() == {
        "error": "Minimum games filter must be non-negative"
    }


def test_rawr_cached_leaderboard_endpoint_returns_cached_series(
    tmp_path: Path,
    monkeypatch,
):
    normalized_games_dir, normalized_players_dir = _seed_rawr_cache_inputs(
        tmp_path,
        monkeypatch,
    )
    player_metrics_db_path = _refresh_rawr_store(tmp_path)

    app = create_app(
        source_data_dir=tmp_path / "source",
        normalized_games_input_dir=normalized_games_dir,
        normalized_game_players_input_dir=normalized_players_dir,
        wowy_output_dir=tmp_path / "team_games",
        combined_wowy_csv=tmp_path / "combined" / "games.csv",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get(
        "/api/metrics/rawr/cached-leaderboard",
        query_string={
            "top_n": "2",
            "min_games": "1",
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


def test_wowy_options_endpoint_returns_cached_teams_and_seasons(
    tmp_path: Path,
    monkeypatch,
):
    normalized_games_dir = tmp_path / "normalized_games"
    normalized_games_dir.mkdir()
    (normalized_games_dir / "BOS_2022-23.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2022-23,2023-04-01,BOS,MIL,true,10,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    (normalized_games_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "2,2023-24,2024-04-01,BOS,MIL,true,8,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    (normalized_games_dir / "NYK_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "3,2023-24,2024-04-01,NYK,BOS,true,4,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    normalized_players_dir = tmp_path / "normalized_game_players"
    normalized_players_dir.mkdir()
    (normalized_players_dir / "BOS_2022-23.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,34.0\n"
        ),
        encoding="utf-8",
    )
    (normalized_players_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "2,BOS,101,Player 101,true,35.0\n"
        ),
        encoding="utf-8",
    )
    (normalized_players_dir / "NYK_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "3,NYK,201,Player 201,true,33.0\n"
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "wowy.nba.prepare.load_player_names_from_cache",
        lambda _: {101: "Player 101", 201: "Player 201"},
    )
    player_metrics_db_path = _refresh_wowy_store(tmp_path)

    app = create_app(
        source_data_dir=tmp_path / "source",
        normalized_games_input_dir=normalized_games_dir,
        normalized_game_players_input_dir=normalized_players_dir,
        wowy_output_dir=tmp_path / "team_games",
        combined_wowy_csv=tmp_path / "combined" / "games.csv",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get("/api/wowy/options", query_string={"team": "BOS"})

    assert response.status_code == 200
    assert response.get_json() == {
        "metric": "wowy",
        "metric_label": "WOWY",
        "available_teams": ["BOS", "NYK"],
        "available_seasons": ["2022-23", "2023-24"],
        "filters": {
            "team": ["BOS"],
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
    monkeypatch,
):
    normalized_games_dir = tmp_path / "normalized_games"
    normalized_games_dir.mkdir()
    (normalized_games_dir / "BOS_2022-23.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2022-23,2023-04-01,BOS,MIL,true,10,Regular Season,nba_api\n"
            "2,2022-23,2023-04-03,BOS,NYK,false,-5,Regular Season,nba_api\n"
            "3,2022-23,2023-04-05,BOS,LAL,true,4,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    normalized_players_dir = tmp_path / "normalized_game_players"
    normalized_players_dir.mkdir()
    (normalized_players_dir / "BOS_2022-23.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,34.0\n"
            "1,BOS,102,Player 102,true,31.0\n"
            "2,BOS,102,Player 102,true,31.0\n"
            "3,BOS,101,Player 101,true,34.0\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "wowy.nba.prepare.load_player_names_from_cache",
        lambda _: {101: "Player 101", 102: "Player 102"},
    )
    player_metrics_db_path = _refresh_wowy_store(tmp_path)

    app = create_app(
        source_data_dir=tmp_path / "source",
        normalized_games_input_dir=normalized_games_dir,
        normalized_game_players_input_dir=normalized_players_dir,
        wowy_output_dir=tmp_path / "team_games",
        combined_wowy_csv=tmp_path / "combined" / "games.csv",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get(
        "/api/wowy/player-seasons",
        query_string={
            "team": "BOS",
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
            "team": ["BOS"],
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
        normalized_games_input_dir=tmp_path / "normalized_games",
        normalized_game_players_input_dir=tmp_path / "normalized_game_players",
        wowy_output_dir=tmp_path / "team_games",
        combined_wowy_csv=tmp_path / "combined" / "games.csv",
        player_metrics_db_path=tmp_path / "app" / "player_metrics.sqlite3",
    )
    client = app.test_client()

    response = client.get(
        "/api/wowy/player-seasons",
        query_string={"min_games_with": "-1"},
    )

    assert response.status_code == 400
    assert response.get_json() == {
        "error": "Minimum game filters must be non-negative"
    }


def test_wowy_options_endpoint_requires_prebuilt_store(tmp_path: Path):
    app = create_app(
        source_data_dir=tmp_path / "source",
        normalized_games_input_dir=tmp_path / "normalized_games",
        normalized_game_players_input_dir=tmp_path / "normalized_game_players",
        wowy_output_dir=tmp_path / "team_games",
        combined_wowy_csv=tmp_path / "combined" / "games.csv",
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
    monkeypatch,
):
    normalized_games_dir = tmp_path / "normalized_games"
    normalized_games_dir.mkdir()
    (normalized_games_dir / "BOS_2022-23.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2022-23,2023-04-01,BOS,MIL,true,10,Regular Season,nba_api\n"
            "2,2022-23,2023-04-03,BOS,NYK,false,-5,Regular Season,nba_api\n"
            "3,2022-23,2023-04-05,BOS,LAL,true,4,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    (normalized_games_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "4,2023-24,2024-04-01,BOS,MIL,true,8,Regular Season,nba_api\n"
            "5,2023-24,2024-04-03,BOS,NYK,false,-2,Regular Season,nba_api\n"
            "6,2023-24,2024-04-05,BOS,LAL,true,1,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    normalized_players_dir = tmp_path / "normalized_game_players"
    normalized_players_dir.mkdir()
    (normalized_players_dir / "BOS_2022-23.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,34.0\n"
            "1,BOS,102,Player 102,true,31.0\n"
            "2,BOS,102,Player 102,true,31.0\n"
            "3,BOS,101,Player 101,true,34.0\n"
        ),
        encoding="utf-8",
    )
    (normalized_players_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "4,BOS,101,Player 101,true,35.0\n"
            "4,BOS,103,Player 103,true,30.0\n"
            "5,BOS,101,Player 101,true,33.0\n"
            "6,BOS,103,Player 103,true,30.0\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "wowy.nba.prepare.load_player_names_from_cache",
        lambda _: {101: "Player 101", 102: "Player 102", 103: "Player 103"},
    )
    player_metrics_db_path = _refresh_wowy_store(tmp_path)

    app = create_app(
        source_data_dir=tmp_path / "source",
        normalized_games_input_dir=normalized_games_dir,
        normalized_game_players_input_dir=normalized_players_dir,
        wowy_output_dir=tmp_path / "team_games",
        combined_wowy_csv=tmp_path / "combined" / "games.csv",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get(
        "/api/wowy/span-chart",
        query_string={
            "team": "BOS",
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
    monkeypatch,
):
    normalized_games_dir = tmp_path / "normalized_games"
    normalized_games_dir.mkdir()
    (normalized_games_dir / "BOS_2022-23.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2022-23,2023-04-01,BOS,MIL,true,10,Regular Season,nba_api\n"
            "2,2022-23,2023-04-03,BOS,NYK,false,-5,Regular Season,nba_api\n"
            "3,2022-23,2023-04-05,BOS,LAL,true,4,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    (normalized_games_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "4,2023-24,2024-04-01,BOS,MIL,true,8,Regular Season,nba_api\n"
            "5,2023-24,2024-04-03,BOS,NYK,false,-2,Regular Season,nba_api\n"
            "6,2023-24,2024-04-05,BOS,LAL,true,1,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    normalized_players_dir = tmp_path / "normalized_game_players"
    normalized_players_dir.mkdir()
    (normalized_players_dir / "BOS_2022-23.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,34.0\n"
            "1,BOS,102,Player 102,true,31.0\n"
            "2,BOS,102,Player 102,true,31.0\n"
            "3,BOS,101,Player 101,true,34.0\n"
        ),
        encoding="utf-8",
    )
    (normalized_players_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "4,BOS,101,Player 101,true,35.0\n"
            "4,BOS,103,Player 103,true,30.0\n"
            "5,BOS,101,Player 101,true,33.0\n"
            "6,BOS,103,Player 103,true,30.0\n"
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "wowy.nba.prepare.load_player_names_from_cache",
        lambda _: {101: "Player 101", 102: "Player 102", 103: "Player 103"},
    )
    player_metrics_db_path = _refresh_wowy_store(tmp_path)

    app = create_app(
        source_data_dir=tmp_path / "source",
        normalized_games_input_dir=normalized_games_dir,
        normalized_game_players_input_dir=normalized_players_dir,
        wowy_output_dir=tmp_path / "team_games",
        combined_wowy_csv=tmp_path / "combined" / "games.csv",
        player_metrics_db_path=player_metrics_db_path,
    )
    client = app.test_client()

    response = client.get(
        "/api/wowy/cached-leaderboard",
        query_string={
            "team": "BOS",
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


def test_wowy_custom_query_endpoint_recalculates_requested_span(
    tmp_path: Path,
    monkeypatch,
):
    normalized_games_dir = tmp_path / "normalized_games"
    normalized_games_dir.mkdir()
    (normalized_games_dir / "BOS_2022-23.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2022-23,2023-04-01,BOS,MIL,true,10,Regular Season,nba_api\n"
            "2,2022-23,2023-04-03,BOS,NYK,false,-5,Regular Season,nba_api\n"
            "3,2022-23,2023-04-05,BOS,LAL,true,4,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    (normalized_games_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "4,2023-24,2024-04-01,BOS,MIL,true,8,Regular Season,nba_api\n"
            "5,2023-24,2024-04-03,BOS,NYK,false,-2,Regular Season,nba_api\n"
            "6,2023-24,2024-04-05,BOS,LAL,true,1,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    normalized_players_dir = tmp_path / "normalized_game_players"
    normalized_players_dir.mkdir()
    (normalized_players_dir / "BOS_2022-23.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,34.0\n"
            "1,BOS,102,Player 102,true,31.0\n"
            "2,BOS,102,Player 102,true,31.0\n"
            "3,BOS,101,Player 101,true,34.0\n"
        ),
        encoding="utf-8",
    )
    (normalized_players_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "4,BOS,101,Player 101,true,35.0\n"
            "4,BOS,103,Player 103,true,30.0\n"
            "5,BOS,101,Player 101,true,33.0\n"
            "6,BOS,103,Player 103,true,30.0\n"
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "wowy.nba.prepare.load_player_names_from_cache",
        lambda _: {101: "Player 101", 102: "Player 102", 103: "Player 103"},
    )

    app = create_app(
        source_data_dir=tmp_path / "source",
        normalized_games_input_dir=normalized_games_dir,
        normalized_game_players_input_dir=normalized_players_dir,
        wowy_output_dir=tmp_path / "team_games",
        combined_wowy_csv=tmp_path / "combined" / "games.csv",
        player_metrics_db_path=tmp_path / "app" / "player_metrics.sqlite3",
    )
    client = app.test_client()

    response = client.get(
        "/api/wowy/custom-query",
        query_string={
            "team": "BOS",
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
