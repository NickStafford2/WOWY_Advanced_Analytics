from __future__ import annotations

import sqlite3
from pathlib import Path

from wowy.nba.cache_migration import normalize_cache_season_keys


def test_normalize_cache_season_keys_rewrites_files_and_db(tmp_path: Path):
    source_data_dir = tmp_path / "source"
    normalized_games_dir = tmp_path / "normalized" / "games"
    normalized_game_players_dir = tmp_path / "normalized" / "game_players"
    wowy_output_dir = tmp_path / "raw" / "team_games"
    combined_wowy_csv = tmp_path / "combined" / "wowy" / "games.csv"
    combined_regression_games_csv = tmp_path / "combined" / "regression" / "games.csv"
    db_path = tmp_path / "app" / "player_metrics.sqlite3"

    team_season_json = source_data_dir / "team_seasons" / "BOS_2014_regular_season_leaguegamefinder.json"
    team_season_json.parent.mkdir(parents=True, exist_ok=True)
    team_season_json.write_text('{"resultSets": []}', encoding="utf-8")

    normalized_games_path = normalized_games_dir / "BOS_2014.csv"
    normalized_games_path.parent.mkdir(parents=True, exist_ok=True)
    normalized_games_path.write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2014,2015-04-01,BOS,ATL,true,5,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )

    normalized_game_players_path = normalized_game_players_dir / "BOS_2014.csv"
    normalized_game_players_path.parent.mkdir(parents=True, exist_ok=True)
    normalized_game_players_path.write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,34.0\n"
        ),
        encoding="utf-8",
    )

    wowy_path = wowy_output_dir / "BOS_2014.csv"
    wowy_path.parent.mkdir(parents=True, exist_ok=True)
    wowy_path.write_text(
        (
            "game_id,season,team,margin,players\n"
            "1,2014,BOS,5,101\n"
        ),
        encoding="utf-8",
    )

    combined_wowy_csv.parent.mkdir(parents=True, exist_ok=True)
    combined_wowy_csv.write_text(
        (
            "game_id,season,team,margin,players\n"
            "1,2014,BOS,5,101\n"
        ),
        encoding="utf-8",
    )

    combined_regression_games_csv.parent.mkdir(parents=True, exist_ok=True)
    combined_regression_games_csv.write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2014,2015-04-01,BOS,ATL,true,5,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )

    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.executescript(
        """
        CREATE TABLE metric_player_season_values (
            metric TEXT NOT NULL,
            metric_label TEXT NOT NULL,
            scope_key TEXT NOT NULL,
            team_filter TEXT NOT NULL DEFAULT '',
            season_type TEXT NOT NULL DEFAULT 'Regular Season',
            season TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            player_name TEXT NOT NULL,
            value REAL NOT NULL,
            sample_size INTEGER,
            secondary_sample_size INTEGER,
            average_minutes REAL,
            total_minutes REAL,
            details_json TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY (metric, scope_key, season, player_id)
        );
        CREATE TABLE metric_full_span_points (
            metric TEXT NOT NULL,
            scope_key TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            season TEXT NOT NULL,
            value REAL NOT NULL,
            PRIMARY KEY (metric, scope_key, player_id, season)
        );
        CREATE TABLE metric_scope_catalog (
            metric TEXT NOT NULL,
            scope_key TEXT NOT NULL,
            metric_label TEXT NOT NULL,
            team_filter TEXT NOT NULL DEFAULT '',
            season_type TEXT NOT NULL DEFAULT 'Regular Season',
            available_seasons_json TEXT NOT NULL DEFAULT '[]',
            available_teams_json TEXT NOT NULL DEFAULT '[]',
            full_span_start_season TEXT,
            full_span_end_season TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (metric, scope_key)
        );
        CREATE TABLE player_season_metrics (
            metric TEXT NOT NULL,
            metric_label TEXT NOT NULL,
            season TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            player_name TEXT NOT NULL,
            value REAL NOT NULL,
            games_with INTEGER,
            games_without INTEGER,
            average_minutes REAL,
            total_minutes REAL,
            details_json TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY (metric, season, player_id)
        );
        """
    )
    connection.execute(
        """
        INSERT INTO metric_player_season_values VALUES
        ('wowy', 'WOWY', 'teams=all-teams|season_type=Regular Season', '', 'Regular Season', '2014', 101, 'Player 101', 3.5, 20, 5, 33.0, 660.0, '{}')
        """
    )
    connection.execute(
        """
        INSERT INTO metric_full_span_points VALUES
        ('wowy', 'teams=all-teams|season_type=Regular Season', 101, '2014', 3.5)
        """
    )
    connection.execute(
        """
        INSERT INTO metric_scope_catalog VALUES
        ('wowy', 'teams=all-teams|season_type=Regular Season', 'WOWY', '', 'Regular Season', '["2014"]', '["BOS"]', '2014', '2014', '2026-03-16T00:00:00+00:00')
        """
    )
    connection.execute(
        """
        INSERT INTO player_season_metrics VALUES
        ('wowy', 'WOWY', '2014', 101, 'Player 101', 3.5, 20, 5, 33.0, 660.0, '{}')
        """
    )
    connection.commit()
    connection.close()

    summary = normalize_cache_season_keys(
        source_data_dir=source_data_dir,
        normalized_games_input_dir=normalized_games_dir,
        normalized_game_players_input_dir=normalized_game_players_dir,
        wowy_output_dir=wowy_output_dir,
        combined_wowy_csv=combined_wowy_csv,
        combined_regression_games_csv=combined_regression_games_csv,
        player_metrics_db_path=db_path,
        log=None,
    )

    assert summary.renamed_files == 4
    assert summary.rewritten_files == 4
    assert summary.updated_db_rows == 4

    assert not team_season_json.exists()
    assert (source_data_dir / "team_seasons" / "BOS_2014-15_regular_season_leaguegamefinder.json").exists()
    assert (normalized_games_dir / "BOS_2014-15.csv").read_text(encoding="utf-8").splitlines()[1].split(",")[1] == "2014-15"
    assert (normalized_game_players_dir / "BOS_2014-15.csv").exists()
    assert (wowy_output_dir / "BOS_2014-15.csv").read_text(encoding="utf-8").splitlines()[1].split(",")[1] == "2014-15"
    assert combined_wowy_csv.read_text(encoding="utf-8").splitlines()[1].split(",")[1] == "2014-15"
    assert combined_regression_games_csv.read_text(encoding="utf-8").splitlines()[1].split(",")[1] == "2014-15"

    connection = sqlite3.connect(db_path)
    assert connection.execute(
        "SELECT season FROM metric_player_season_values"
    ).fetchone()[0] == "2014-15"
    assert connection.execute(
        "SELECT season FROM metric_full_span_points"
    ).fetchone()[0] == "2014-15"
    scope_row = connection.execute(
        "SELECT available_seasons_json, full_span_start_season, full_span_end_season FROM metric_scope_catalog"
    ).fetchone()
    assert scope_row[0] == '["2014-15"]'
    assert scope_row[1] == "2014-15"
    assert scope_row[2] == "2014-15"
    assert connection.execute(
        "SELECT season FROM player_season_metrics"
    ).fetchone()[0] == "2014-15"
    connection.close()
