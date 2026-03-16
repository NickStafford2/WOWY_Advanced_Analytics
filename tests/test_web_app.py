from __future__ import annotations

from pathlib import Path

from wowy.web.app import create_app


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

    app = create_app(
        source_data_dir=tmp_path / "source",
        normalized_games_input_dir=normalized_games_dir,
        normalized_game_players_input_dir=normalized_players_dir,
        wowy_output_dir=tmp_path / "team_games",
        combined_wowy_csv=tmp_path / "combined" / "games.csv",
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
        "filters": {
            "team": ["BOS"],
            "season": None,
            "season_type": "Regular Season",
            "min_games_with": 1,
            "min_games_without": 1,
            "min_average_minutes": 0.0,
            "min_total_minutes": 0.0,
        },
        "rows": [
            {
                "season": "2022-23",
                "player_id": 101,
                "player_name": "Player 101",
                "games_with": 2,
                "games_without": 1,
                "avg_margin_with": 7.0,
                "avg_margin_without": -5.0,
                "wowy_score": 12.0,
                "average_minutes": 34.0,
                "total_minutes": 68.0,
            },
            {
                "season": "2022-23",
                "player_id": 102,
                "player_name": "Player 102",
                "games_with": 2,
                "games_without": 1,
                "avg_margin_with": 2.5,
                "avg_margin_without": 4.0,
                "wowy_score": -1.5,
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

    app = create_app(
        source_data_dir=tmp_path / "source",
        normalized_games_input_dir=normalized_games_dir,
        normalized_game_players_input_dir=normalized_players_dir,
        wowy_output_dir=tmp_path / "team_games",
        combined_wowy_csv=tmp_path / "combined" / "games.csv",
    )
    client = app.test_client()

    response = client.get(
        "/api/wowy/span-chart",
        query_string={
            "team": "BOS",
            "start_season": "2022-23",
            "end_season": "2023-24",
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
            "average_wowy_score": 7.0,
            "season_count": 2,
            "points": [
                {"season": "2022-23", "wowy_score": 12.0},
                {"season": "2023-24", "wowy_score": 2.0},
            ],
        },
        {
            "player_id": 103,
            "player_name": "Player 103",
            "average_wowy_score": 6.5,
            "season_count": 1,
            "points": [
                {"season": "2022-23", "wowy_score": None},
                {"season": "2023-24", "wowy_score": 6.5},
            ],
        },
    ]
