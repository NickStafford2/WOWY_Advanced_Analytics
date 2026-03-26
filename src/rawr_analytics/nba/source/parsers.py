from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Callable, NoReturn

from rawr_analytics.nba.season_types import canonicalize_season_type
from rawr_analytics.nba.seasons import canonicalize_season_string
from rawr_analytics.nba.source.cache import (
    box_score_payload_is_empty,
    load_cached_payload,
)
from rawr_analytics.nba.source.models import (
    SourceBoxScore,
    SourceBoxScorePlayer,
    SourceBoxScoreTeam,
    SourceLeagueGame,
    SourceLeagueSchedule,
)
from rawr_analytics.nba.source.rules import (
    classify_source_player_row,
    classify_source_schedule_row,
    classify_source_team_row,
    format_source_row,
    parse_box_score_numeric_value,
    source_player_played_in_game,
)
from rawr_analytics.nba.team_identity import resolve_source_team_identity

_ROW_VALUE_ALIASES: dict[str, tuple[str, ...]] = {
    "GAME_ID": ("gameId",),
    "TEAM_ID": ("teamId",),
    "TEAM_ABBREVIATION": ("teamTricode",),
    "PLAYER_ID": ("personId",),
    "PLAYER_NAME": ("playerName",),
    "MIN": ("minutes",),
    "PTS": ("points",),
    "PLUS_MINUS": ("plusMinusPoints",),
}
_INTEGER_TEXT_RE = re.compile(r"^[+-]?\d+(?:\.0+)?$")
_PLAYER_ROW_LABEL = "nba_api_box_score_player_row"
_TEAM_ROW_LABEL = "nba_api_box_score_team_row"
_SCHEDULE_ROW_LABEL = "nba_api_league_schedule_row"


def load_player_names_from_cache(
    source_data_dir: Path,
    log: Callable[[str], None] | None = None,
) -> dict[int, str]:
    player_names: dict[int, str] = {}
    for cache_path in sorted((source_data_dir / "boxscores").glob("*.json")):
        payload = load_cached_payload(
            cache_path,
            validator=lambda cached_payload: not box_score_payload_is_empty(cached_payload),
            log=log,
        )
        if payload is None:
            continue
        game_id = cache_path.stem.split("_", maxsplit=1)[0]
        parsed_box_score = parse_box_score_payload(payload, game_id=game_id)
        for player in parsed_box_score.players:
            if player.player_id is None:
                continue
            player_name = player.player_name.strip()
            if player_name == "":
                continue
            player_names[player.player_id] = player_name
    return player_names


def parse_league_schedule_payload(
    payload: dict,
    *,
    requested_team: str,
    season: str,
    season_type: str,
) -> SourceLeagueSchedule:
    rows = _result_set_rows(_first_result_set(payload, label="league schedule"))
    games: list[SourceLeagueGame] = []
    for row in rows:
        game = _parse_schedule_row(row, season=season)
        if classify_source_schedule_row(row).should_skip:
            continue
        games.append(game)
    return SourceLeagueSchedule(
        requested_team=requested_team.strip().upper(),
        season=canonicalize_season_string(season),
        season_type=canonicalize_season_type(season_type),
        games=games,
    )


def dedupe_schedule_games(games: list[SourceLeagueGame]) -> list[SourceLeagueGame]:
    deduped: list[SourceLeagueGame] = []
    seen_games_by_id: dict[str, SourceLeagueGame] = {}
    for game in games:
        existing_game = seen_games_by_id.get(game.game_id)
        if existing_game is not None:
            if (
                existing_game.game_date != game.game_date
                or existing_game.matchup != game.matchup
                or existing_game.team_id != game.team_id
                or existing_game.team_abbreviation != game.team_abbreviation
            ):
                raise ValueError(
                    f"Conflicting duplicate schedule rows for game {game.game_id!r}; "
                    f"first_row={format_source_row(existing_game.raw_row)} "
                    f"second_row={format_source_row(game.raw_row)}"
                )
            continue
        deduped.append(game)
        seen_games_by_id[game.game_id] = game
    return deduped


def parse_box_score_payload(payload: dict, *, game_id: str) -> SourceBoxScore:
    game_payload = payload.get("game")
    if isinstance(game_payload, dict):
        return _parse_live_box_score_payload(game_payload, game_id=game_id)

    result_sets = payload.get("resultSets")
    if isinstance(result_sets, list):
        return _parse_list_result_set_box_score(result_sets, game_id=game_id)
    if isinstance(result_sets, dict):
        return _parse_mapping_result_set_box_score(result_sets, game_id=game_id)
    raise ValueError(f"Box score payload is missing resultSets for game {game_id!r}")


def _parse_list_result_set_box_score(
    result_sets: list[object],
    *,
    game_id: str,
) -> SourceBoxScore:
    named_sets = {
        str(result_set.get("name", "")): result_set
        for result_set in result_sets
        if isinstance(result_set, dict)
    }
    player_set = named_sets.get("PlayerStats")
    team_set = named_sets.get("TeamStats")
    if player_set is None or team_set is None:
        if len(result_sets) < 2:
            raise ValueError(f"Box score payload is incomplete for game {game_id!r}")
        player_set = _require_result_set(result_sets[0], game_id=game_id)
        team_set = _require_result_set(result_sets[1], game_id=game_id)
    return _parse_result_set_box_score(player_set=player_set, team_set=team_set, game_id=game_id)


def _parse_mapping_result_set_box_score(
    result_sets: dict[str, object],
    *,
    game_id: str,
) -> SourceBoxScore:
    player_set = result_sets.get("PlayerStats")
    team_set = result_sets.get("TeamStats")
    if not isinstance(player_set, dict) or not isinstance(team_set, dict):
        raise ValueError(
            f"Box score payload is missing PlayerStats/TeamStats for game {game_id!r}; "
            f"nba_api_box_score_payload={format_source_row(result_sets)}"
        )
    return _parse_result_set_box_score(player_set=player_set, team_set=team_set, game_id=game_id)


def _parse_result_set_box_score(
    *,
    player_set: dict[str, object],
    team_set: dict[str, object],
    game_id: str,
) -> SourceBoxScore:
    player_rows = _result_set_rows(player_set)
    team_rows = _result_set_rows(team_set)
    players = [_parse_result_set_player_row(row, game_id=game_id) for row in player_rows]
    teams = [_parse_result_set_team_row(row) for row in team_rows]
    return SourceBoxScore(game_id=game_id, players=players, teams=teams)


def _parse_live_box_score_payload(
    game_payload: dict[str, object],
    *,
    game_id: str,
) -> SourceBoxScore:
    players: list[SourceBoxScorePlayer] = []
    teams: list[SourceBoxScoreTeam] = []
    for team_payload in _live_team_payloads(game_payload):
        teams.append(_parse_live_team_row(team_payload))
        for player_payload in _live_player_payloads(team_payload):
            players.append(
                _parse_live_player_row(
                    player_payload,
                    team_payload=team_payload,
                    game_id=game_id,
                )
            )
    return SourceBoxScore(game_id=game_id, players=players, teams=teams)


def _live_team_payloads(game_payload: dict[str, object]) -> list[dict[str, object]]:
    teams: list[dict[str, object]] = []
    for team_key in ("homeTeam", "awayTeam"):
        team_payload = game_payload.get(team_key)
        assert team_payload is None or isinstance(team_payload, dict), (
            f"Live box score {team_key} must be a dict"
        )
        if isinstance(team_payload, dict):
            teams.append(team_payload)
    return teams


def _live_player_payloads(team_payload: dict[str, object]) -> list[dict[str, object]]:
    raw_players = team_payload.get("players", [])
    assert isinstance(raw_players, list), "Live box score team players must be a list"
    players: list[dict[str, object]] = []
    for raw_player in raw_players:
        assert isinstance(raw_player, dict), "Live box score player payload must be a dict"
        players.append(raw_player)
    return players


def _parse_live_team_row(team_payload: dict[str, object]) -> SourceBoxScoreTeam:
    statistics = _optional_mapping(team_payload.get("statistics"))
    parsed_row = SourceBoxScoreTeam(
        team_id=_optional_int(team_payload, "teamId", row_label=_TEAM_ROW_LABEL),
        team_abbreviation=_optional_text(team_payload, "teamTricode"),
        plus_minus_raw=_optional_plus_minus(
            statistics,
            "plusMinusPoints",
            row_label=_TEAM_ROW_LABEL,
        ),
        points_raw=team_payload.get("score"),
        raw_row=dict(team_payload),
    )
    _validate_source_team_row(parsed_row)
    return parsed_row


def _parse_live_player_row(
    player_payload: dict[str, object],
    *,
    team_payload: dict[str, object],
    game_id: str,
) -> SourceBoxScorePlayer:
    statistics = _optional_mapping(player_payload.get("statistics"))
    minutes_raw = statistics.get("minutesCalculated")
    if minutes_raw in {None, ""}:
        minutes_raw = statistics.get("minutes")
    parsed_row = SourceBoxScorePlayer(
        game_id=game_id,
        team_id=_optional_int(team_payload, "teamId", row_label=_PLAYER_ROW_LABEL),
        team_abbreviation=_optional_text(team_payload, "teamTricode"),
        player_id=_optional_int(player_payload, "personId", row_label=_PLAYER_ROW_LABEL),
        player_name=_build_live_player_name(player_payload),
        minutes_raw=_optional_minutes_value(
            minutes_raw,
            key="minutes",
            row_label=_PLAYER_ROW_LABEL,
            row=player_payload,
        ),
        raw_row=dict(player_payload),
    )
    _validate_source_player_row(parsed_row)
    return parsed_row


def _parse_schedule_row(row: dict[str, object], *, season: str) -> SourceLeagueGame:
    game = SourceLeagueGame(
        game_id=_required_text(row, "GAME_ID", row_label=_SCHEDULE_ROW_LABEL),
        game_date=_required_text(row, "GAME_DATE", row_label=_SCHEDULE_ROW_LABEL),
        matchup=_required_text(row, "MATCHUP", row_label=_SCHEDULE_ROW_LABEL),
        team_id=_required_int(row, "TEAM_ID", row_label=_SCHEDULE_ROW_LABEL),
        team_abbreviation=_required_text(row, "TEAM_ABBREVIATION", row_label=_SCHEDULE_ROW_LABEL),
        raw_row=row,
    )
    resolve_source_team_identity(
        team_id=game.team_id,
        team_abbreviation=game.team_abbreviation,
        season=season,
        game_date=game.game_date,
        error_context=_row_context(_SCHEDULE_ROW_LABEL, row),
    )
    return game


def _first_result_set(payload: dict[str, object], *, label: str) -> dict[str, object]:
    result_sets = payload.get("resultSets")
    if not isinstance(result_sets, list) or not result_sets:
        raise ValueError(f"{label.title()} payload is missing resultSets")
    return _require_result_set(result_sets[0], game_id=label)


def _require_result_set(result_set: object, *, game_id: str) -> dict[str, object]:
    if not isinstance(result_set, dict):
        raise ValueError(f"Box score payload has invalid result set for game {game_id!r}")
    return result_set


def _result_set_rows(result_set: dict[str, object]) -> list[dict[str, object]]:
    headers = result_set.get("headers")
    assert isinstance(headers, list) and headers, "Result set is missing headers"
    assert all(isinstance(header, str) for header in headers), "Result set headers must be strings"

    raw_rows = result_set.get("rowSet", result_set.get("data"))
    assert isinstance(raw_rows, list), "Result set row data must be a list"

    rows: list[dict[str, object]] = []
    for raw_row in raw_rows:
        assert isinstance(raw_row, list), "Result set row must be a list"
        rows.append(dict(zip(headers, raw_row, strict=False)))
    return rows


def _player_name_from_row(row: dict[str, object]) -> str:
    explicit_name = _optional_text_any(row, ("PLAYER_NAME",))
    if explicit_name:
        return explicit_name
    first_name = _optional_text_any(row, ("firstName",))
    family_name = _optional_text_any(row, ("familyName",))
    return " ".join(part for part in (first_name, family_name) if part)


def _build_live_player_name(player_payload: dict[str, object]) -> str:
    first_name = _optional_text_any(player_payload, ("firstName",))
    family_name = _optional_text_any(player_payload, ("familyName",))
    return " ".join(part for part in (first_name, family_name) if part)


def _required_text(row: dict[str, object], key: str, *, row_label: str) -> str:
    value = _optional_text(row, key)
    if value == "":
        _fail_on_row(row_label, row, f"Missing required {key}")
    return value


def _optional_text(row: dict[str, object], key: str) -> str:
    value = _row_value(row, key)
    if value is None:
        return ""
    text = str(value).strip()
    if key.endswith("ABBREVIATION"):
        return text.upper()
    return text


def _required_int(row: dict[str, object], key: str, *, row_label: str) -> int:
    value = _optional_int(row, key, row_label=row_label)
    if value is None:
        _fail_on_row(row_label, row, f"Missing required {key}")
        assert False, "unreachable after _fail_on_row"
    return value


def _optional_int(
    row: dict[str, object],
    key: str,
    *,
    row_label: str,
) -> int | None:
    value = _row_value(row, key)
    if value in {None, ""}:
        return None
    if isinstance(value, bool):
        _fail_on_row(row_label, row, f"Invalid integer value for {key}: {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            _fail_on_row(row_label, row, f"Invalid integer value for {key}: {value!r}")
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not _INTEGER_TEXT_RE.fullmatch(stripped):
            _fail_on_row(row_label, row, f"Invalid integer value for {key}: {value!r}")
        return int(float(stripped))
    _fail_on_row(row_label, row, f"Invalid integer value for {key}: {value!r}")
    assert False, "unreachable after _fail_on_row"


def _optional_minutes_value(
    value: object,
    *,
    key: str,
    row_label: str,
    row: dict[str, object],
) -> str | int | None:
    if value in {None, ""}:
        return None
    if isinstance(value, int):
        if value == 0:
            return None
        return value
    if isinstance(value, str):
        return value
    _fail_on_row(row_label, row, f"Invalid MIN value for {key}: {value!r}")
    assert False, "unreachable after _fail_on_row"


def _optional_minutes(
    row: dict[str, object],
    key: str,
    *,
    row_label: str,
) -> str | int | None:
    return _optional_minutes_value(
        _row_value(row, key),
        key=key,
        row_label=row_label,
        row=row,
    )


def _optional_plus_minus(
    row: dict[str, object],
    key: str,
    *,
    row_label: str,
) -> int | float | None:
    value = _row_value(row, key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        _fail_on_row(row_label, row, f"Invalid PLUS_MINUS value for {key}: {value!r}")
        assert False, "unreachable after _fail_on_row"
    return value


def _row_value(row: dict[str, object], key: str) -> object | None:
    if key in row:
        return row[key]
    for alias in _ROW_VALUE_ALIASES.get(key, ()):
        if alias in row:
            return row[alias]
    return None


def _optional_text_any(row: dict[str, object], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = _row_value(row, key)
        if value is None:
            continue
        return str(value).strip()
    return ""


def _parse_result_set_player_row(
    row: dict[str, object],
    *,
    game_id: str,
) -> SourceBoxScorePlayer:
    parsed_row = SourceBoxScorePlayer(
        game_id=_optional_text(row, "GAME_ID") or game_id,
        team_id=_optional_int(row, "TEAM_ID", row_label=_PLAYER_ROW_LABEL),
        team_abbreviation=_optional_text(row, "TEAM_ABBREVIATION"),
        player_id=_optional_int(row, "PLAYER_ID", row_label=_PLAYER_ROW_LABEL),
        player_name=_player_name_from_row(row),
        minutes_raw=_optional_minutes(row, "MIN", row_label=_PLAYER_ROW_LABEL),
        raw_row=row,
    )
    _validate_source_player_row(parsed_row)
    return parsed_row


def _parse_result_set_team_row(row: dict[str, object]) -> SourceBoxScoreTeam:
    parsed_row = SourceBoxScoreTeam(
        team_id=_optional_int(row, "TEAM_ID", row_label=_TEAM_ROW_LABEL),
        team_abbreviation=_optional_text(row, "TEAM_ABBREVIATION"),
        plus_minus_raw=_optional_plus_minus(row, "PLUS_MINUS", row_label=_TEAM_ROW_LABEL),
        points_raw=_row_value(row, "PTS"),
        raw_row=row,
    )
    _validate_source_team_row(parsed_row)
    return parsed_row


def _validate_source_player_row(player: SourceBoxScorePlayer) -> None:
    classification = classify_source_player_row(player)
    if classification.should_skip:
        return

    row_context = _row_context(_PLAYER_ROW_LABEL, player.raw_row)
    resolve_source_team_identity(
        team_id=player.team_id,
        team_abbreviation=player.team_abbreviation,
        error_context=row_context,
    )

    if player.game_id.strip() == "":
        raise ValueError(f"Missing GAME_ID; {row_context}")
    if player.player_id is None or player.player_id <= 0:
        raise ValueError(f"Missing PLAYER_ID; {row_context}")
    if player.player_name.strip() == "":
        raise ValueError(f"Missing PLAYER_NAME; {row_context}")
    assert source_player_played_in_game(player), row_context


def _validate_source_team_row(row: SourceBoxScoreTeam) -> None:
    classification = classify_source_team_row(row)
    if classification.should_skip:
        return

    row_context = _row_context(_TEAM_ROW_LABEL, row.raw_row)
    resolve_source_team_identity(
        team_id=row.team_id,
        team_abbreviation=row.team_abbreviation,
        error_context=row_context,
    )

    if row.team_id is None or row.team_id <= 0:
        raise ValueError(f"Missing TEAM_ID; {row_context}")
    if row.team_abbreviation.strip() == "":
        raise ValueError(f"Missing TEAM_ABBREVIATION; {row_context}")
    if parse_box_score_numeric_value(row.points_raw) is None:
        raise ValueError(f"Unparseable PTS value; {row_context}")


def _optional_mapping(value: object) -> dict[str, object]:
    if value is None:
        return {}
    assert isinstance(value, dict), "Expected mapping payload from nba_api"
    return value


def _row_context(row_label: str, row: dict[str, object]) -> str:
    return f"{row_label}={format_source_row(row)}"


def _fail_on_row(row_label: str, row: dict[str, object], message: str) -> NoReturn:
    raise ValueError(f"{message}; {_row_context(row_label, row)}")


__all__ = [
    "dedupe_schedule_games",
    "load_player_names_from_cache",
    "parse_box_score_payload",
    "parse_league_schedule_payload",
]
