from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from werkzeug.test import TestResponse

from rawr_analytics.web.app import create_app

_GSW_TEAM_ID = "1610612744"
_TOP_N = "5"


@dataclass(frozen=True)
class _MetricSeasonCase:
    label: str
    season_id: str
    season_label: str
    min_rawr_games: str
    min_wowy_games_with: str
    min_wowy_games_without: str


_SEASON_CASES = [
    _MetricSeasonCase(
        label="regular",
        season_id="2025-26:REGULAR",
        season_label="2025-26",
        min_rawr_games="5",
        min_wowy_games_with="5",
        min_wowy_games_without="2",
    ),
    _MetricSeasonCase(
        label="playoffs",
        season_id="2024-25:PLAYOFFS",
        season_label="2024-25",
        min_rawr_games="2",
        min_wowy_games_with="2",
        min_wowy_games_without="1",
    ),
    _MetricSeasonCase(
        label="preseason",
        season_id="2024-25:PRESEASON",
        season_label="2024-25",
        min_rawr_games="1",
        min_wowy_games_with="1",
        min_wowy_games_without="1",
    ),
]


@pytest.mark.parametrize("season_case", _SEASON_CASES, ids=lambda season_case: season_case.label)
def test_rawr_custom_query_uses_real_db(season_case: _MetricSeasonCase) -> None:
    client = create_app().test_client()

    response = client.get(
        "/api/metrics/rawr/custom-query",
        query_string={
            "team_id": _GSW_TEAM_ID,
            "season": season_case.season_id,
            "top_n": _TOP_N,
            "min_average_minutes": "0",
            "min_total_minutes": "0",
            "min_games": season_case.min_rawr_games,
            "ridge_alpha": "10",
        },
    )

    payload = _assert_live_leaderboard_response(
        response,
        metric="rawr",
        season_label=season_case.season_label,
    )
    first_row = _first_table_row(payload)
    filters = _assert_common_filter_payload(payload, season_case=season_case)
    assert filters["min_games"] == int(season_case.min_rawr_games)
    assert filters["ridge_alpha"] == 10.0
    assert filters["recalculate"] is True
    assert first_row["games"] >= int(season_case.min_rawr_games)
    assert first_row["average_minutes"] is not None
    assert first_row["total_minutes"] is not None


@pytest.mark.parametrize(
    ("route", "metric", "season_case"),
    [
        ("/api/metrics/wowy/custom-query", "wowy", season_case)
        for season_case in _SEASON_CASES
    ]
    + [
        ("/api/metrics/wowy_shrunk/custom-query", "wowy_shrunk", season_case)
        for season_case in _SEASON_CASES
    ],
    ids=lambda value: value.label if isinstance(value, _MetricSeasonCase) else value,
)
def test_wowy_custom_query_uses_real_db(
    route: str,
    metric: str,
    season_case: _MetricSeasonCase,
) -> None:
    client = create_app().test_client()

    response = client.get(
        route,
        query_string={
            "team_id": _GSW_TEAM_ID,
            "season": season_case.season_id,
            "top_n": _TOP_N,
            "min_average_minutes": "0",
            "min_total_minutes": "0",
            "min_games_with": season_case.min_wowy_games_with,
            "min_games_without": season_case.min_wowy_games_without,
        },
    )

    payload = _assert_live_leaderboard_response(
        response,
        metric=metric,
        season_label=season_case.season_label,
    )
    first_row = _first_table_row(payload)
    filters = _assert_common_filter_payload(payload, season_case=season_case)
    assert filters["min_games_with"] == int(season_case.min_wowy_games_with)
    assert filters["min_games_without"] == int(season_case.min_wowy_games_without)
    assert first_row["games_with"] >= int(season_case.min_wowy_games_with)
    assert first_row["games_without"] >= int(season_case.min_wowy_games_without)
    assert first_row["avg_margin_with"] is not None
    assert first_row["avg_margin_without"] is not None


def _assert_live_leaderboard_response(
    response: TestResponse,
    *,
    metric: str,
    season_label: str,
) -> dict[str, Any]:
    assert response.status_code == 200, response.get_data(as_text=True)

    payload = response.get_json()
    assert isinstance(payload, dict)
    assert payload["metric"] == metric
    assert payload["mode"] == "live"
    span = _as_dict(payload["span"])
    assert span["available_seasons"] == [season_label]
    assert span["top_n"] == int(_TOP_N)

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
            "season": season_label,
            "value": first_row["span_average_value"],
        }
    ]
    return payload


def _assert_common_filter_payload(
    payload: dict[str, Any],
    *,
    season_case: _MetricSeasonCase,
) -> dict[str, Any]:
    filters = _as_dict(payload["filters"])
    assert filters["team_id_filter"] == [int(_GSW_TEAM_ID)]
    assert filters["season_filter"] == [season_case.season_label]
    assert filters["top_n"] == int(_TOP_N)
    assert filters["min_average_minutes"] == 0.0
    assert filters["min_total_minutes"] == 0.0
    return filters


def _first_table_row(payload: dict[str, Any]) -> dict[str, Any]:
    table_rows = _as_list(payload["table_rows"])
    return _as_dict(table_rows[0])


def _as_dict(value: object) -> dict[str, Any]:
    assert isinstance(value, dict)
    return value


def _as_list(value: object) -> list[Any]:
    assert isinstance(value, list)
    return value
