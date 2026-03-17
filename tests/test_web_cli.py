from __future__ import annotations

from pathlib import Path

from wowy.web import cli, refresh_cli


def test_refresh_cli_accepts_rawr_metric(monkeypatch, capsys):
    calls: list[dict] = []

    monkeypatch.setattr(
        "wowy.web.refresh_cli.refresh_metric_store",
        lambda metric, **kwargs: calls.append({"metric": metric, **kwargs}),
    )

    exit_code = refresh_cli.main(
        [
            "--metric",
            "rawr",
            "--player-metrics-db-path",
            "tmp/player_metrics.sqlite3",
        ]
    )

    assert exit_code == 0
    assert calls[0]["metric"] == "rawr"
    assert calls[0]["combined_rawr_games_csv"] == Path("data/combined/rawr/games.csv")
    assert calls[0]["combined_rawr_game_players_csv"] == Path(
        "data/combined/rawr/game_players.csv"
    )
    assert calls[0]["rawr_ridge_alpha"] == 10.0
    captured = capsys.readouterr()
    assert "refreshed rawr store" in captured.out
    assert "Web Store Refresh" in captured.err


def test_web_cli_refresh_store_accepts_multiple_metrics(monkeypatch, capsys):
    refresh_calls: list[dict] = []
    app_calls: list[dict] = []

    monkeypatch.setattr(
        "wowy.web.cli.refresh_metric_store",
        lambda metric, **kwargs: refresh_calls.append({"metric": metric, **kwargs}),
    )

    class FakeApp:
        def run(self, *, host: str, port: int, debug: bool) -> None:
            app_calls.append({"host": host, "port": port, "debug": debug})

    monkeypatch.setattr("wowy.web.cli.create_app", lambda **_kwargs: FakeApp())

    exit_code = cli.main(
        [
            "--refresh-store",
            "--refresh-metric",
            "wowy",
            "--refresh-metric",
            "rawr",
        ]
    )

    assert exit_code == 0
    assert [call["metric"] for call in refresh_calls] == ["wowy", "rawr"]
    assert all(
        call["combined_rawr_games_csv"] == Path("data/combined/rawr/games.csv")
        for call in refresh_calls
    )
    assert all(
        call["combined_rawr_game_players_csv"]
        == Path("data/combined/rawr/game_players.csv")
        for call in refresh_calls
    )
    assert all(call["rawr_ridge_alpha"] == 10.0 for call in refresh_calls)
    assert app_calls == [{"host": "127.0.0.1", "port": 5000, "debug": False}]
    output = capsys.readouterr().out
    assert "refreshing wowy web store" in output
    assert "refreshing rawr web store" in output


def test_web_cli_refresh_store_defaults_to_all_frontend_metrics(monkeypatch, capsys):
    refresh_calls: list[dict] = []
    app_calls: list[dict] = []

    monkeypatch.setattr(
        "wowy.web.cli.refresh_metric_store",
        lambda metric, **kwargs: refresh_calls.append({"metric": metric, **kwargs}),
    )

    class FakeApp:
        def run(self, *, host: str, port: int, debug: bool) -> None:
            app_calls.append({"host": host, "port": port, "debug": debug})

    monkeypatch.setattr("wowy.web.cli.create_app", lambda **_kwargs: FakeApp())

    exit_code = cli.main(["--refresh-store"])

    assert exit_code == 0
    assert [call["metric"] for call in refresh_calls] == ["wowy", "rawr"]
    assert all(call["rawr_ridge_alpha"] == 10.0 for call in refresh_calls)
    assert app_calls == [{"host": "127.0.0.1", "port": 5000, "debug": False}]
    output = capsys.readouterr().out
    assert "refreshing wowy web store" in output
    assert "refreshing rawr web store" in output
