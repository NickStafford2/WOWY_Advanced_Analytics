from __future__ import annotations

from typing import Any

import pytest
from werkzeug.test import TestResponse

from rawr_analytics.web.app import create_app

_GSW_TEAM_ID = "1610612744"
_SEASON_ID = "2025-26:REGULAR"
_SEASON_LABEL = "2025-26"


def test_rawr_custom_query_uses_real_db() -> None:
    client = create_app().test_client()

    response = client.get(
        "/api/metrics/rawr/custom-query",
        query_string={
            "team_id": _GSW_TEAM_ID,
            "season": _SEASON_ID,
            "top_n": "5",
            "min_average_minutes": "0",
            "min_total_minutes": "0",
            "min_games": "5",
            "ridge_alpha": "10",
        },
    )

    payload = _assert_live_leaderboard_response(response, metric="rawr")
    first_row = _first_table_row(payload)
    assert first_row["games"] >= 5
    assert first_row["average_minutes"] is not None
    assert first_row["total_minutes"] is not None


@pytest.mark.parametrize(
    ("route", "metric"),
    [
        ("/api/metrics/wowy/custom-query", "wowy"),
        ("/api/metrics/wowy_shrunk/custom-query", "wowy_shrunk"),
    ],
)
def test_wowy_custom_query_uses_real_db(route: str, metric: str) -> None:
    client = create_app().test_client()

    response = client.get(
        route,
        query_string={
            "team_id": _GSW_TEAM_ID,
            "season": _SEASON_ID,
            "top_n": "5",
            "min_average_minutes": "0",
            "min_total_minutes": "0",
            "min_games_with": "5",
            "min_games_without": "2",
        },
    )

    payload = _assert_live_leaderboard_response(response, metric=metric)
    first_row = _first_table_row(payload)
    assert first_row["games_with"] >= 5
    assert first_row["games_without"] >= 2
    assert first_row["avg_margin_with"] is not None
    assert first_row["avg_margin_without"] is not None


def _assert_live_leaderboard_response(response: TestResponse, *, metric: str) -> dict[str, Any]:
    assert response.status_code == 200, response.get_data(as_text=True)

    payload = response.get_json()
    assert isinstance(payload, dict)
    assert payload["metric"] == metric
    assert payload["mode"] == "live"
    span = _as_dict(payload["span"])
    assert span["available_seasons"] == [_SEASON_LABEL]
    assert span["top_n"] == 5

    table_rows = _as_list(payload["table_rows"])
    assert 0 < len(table_rows) <= 5
    assert len(_as_list(payload["series"])) == len(table_rows)

    first_row = _as_dict(table_rows[0])
    assert first_row["rank"] == 1
    assert first_row["player_id"] > 0
    assert first_row["player_name"]
    assert first_row["season_count"] == 1
    assert first_row["span_average_value"] is not None
    assert first_row["points"] == [
        {
            "season": _SEASON_LABEL,
            "value": first_row["span_average_value"],
        }
    ]
    return payload


def _first_table_row(payload: dict[str, Any]) -> dict[str, Any]:
    table_rows = _as_list(payload["table_rows"])
    return _as_dict(table_rows[0])


def _as_dict(value: object) -> dict[str, Any]:
    assert isinstance(value, dict)
    return value


def _as_list(value: object) -> list[Any]:
    assert isinstance(value, list)
    return value
