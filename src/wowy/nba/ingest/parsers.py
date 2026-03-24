from __future__ import annotations

from pathlib import Path
from typing import Callable

from wowy.nba.ingest.cache import (
    _box_score_payload_is_empty,
    discard_invalid_cached_payload,
    load_cached_payload,
)
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.season_types import canonicalize_season_type
from wowy.nba.source_models import (
    SourceBoxScore,
    SourceBoxScorePlayer,
    SourceBoxScoreTeam,
    SourceLeagueGame,
    SourceLeagueSchedule,
)
from wowy.nba.ingest.source_rules import (
    classify_source_player_row,
    classify_source_schedule_row,
    classify_source_team_row,
    format_source_row,
    parse_box_score_numeric_value,
    parse_minutes_to_float,
)
from wowy.nba.team_identity import resolve_source_team_identity


def load_player_names_from_cache(
    source_data_dir: Path,
    log: Callable[[str], None] | None = None,
) -> dict[int, str]:
    player_names: dict[int, str] = {}
    for cache_path in sorted((source_data_dir / "boxscores").glob("*.json")):
        payload = load_cached_payload(
            cache_path,
            validator=lambda cached_payload: not _box_score_payload_is_empty(cached_payload),
            log=log,
        )
        if payload is None:
            continue
        game_id = cache_path.stem.split("_", maxsplit=1)[0]
        try:
            parsed_box_score = parse_box_score_payload(payload, game_id=game_id)
        except ValueError as exc:
            discard_invalid_cached_payload(
                cache_path,
                reason=f"unparseable_box_score_payload={exc}",
                log=log,
            )
            continue
        for player in parsed_box_score.players:
            if player.player_id is None or not player.player_name.strip():
                continue
            player_names[player.player_id] = player.player_name.strip()
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
        try:
            game_id = _required_text(row, "GAME_ID")
            game_date = _required_text(row, "GAME_DATE")
            matchup = _required_text(row, "MATCHUP")
            team_id = _required_int(row, "TEAM_ID")
            team_abbreviation = _required_text(row, "TEAM_ABBREVIATION")
            resolve_source_team_identity(
                team_id=team_id,
                team_abbreviation=team_abbreviation,
                season=season,
                game_date=game_date,
            )
        except ValueError as exc:
            raise ValueError(
                f"{exc}; nba_api_league_schedule_row={format_source_row(row)}"
            ) from exc
        classification = classify_source_schedule_row(row)
        if classification.should_skip:
            continue
        games.append(
            SourceLeagueGame(
                game_id=game_id,
                game_date=game_date,
                matchup=matchup,
                team_id=team_id,
                team_abbreviation=team_abbreviation,
                raw_row=row,
            )
        )
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
            player_set = result_sets[0]
            team_set = result_sets[1]
        return _parse_result_set_box_score(
            player_set=player_set,
            team_set=team_set,
            game_id=game_id,
        )

    if isinstance(result_sets, dict):
        player_set = result_sets.get("PlayerStats")
        team_set = result_sets.get("TeamStats")
        if not isinstance(player_set, dict) or not isinstance(team_set, dict):
            raise ValueError(
                f"Box score payload is missing PlayerStats/TeamStats for game {game_id!r}; "
                f"nba_api_box_score_payload={format_source_row(payload)}"
            )
        return _parse_result_set_box_score(
            player_set=player_set,
            team_set=team_set,
            game_id=game_id,
        )

    raise ValueError(f"Box score payload is missing resultSets for game {game_id!r}")


def _parse_result_set_box_score(
    *,
    player_set: dict,
    team_set: dict,
    game_id: str,
) -> SourceBoxScore:
    players = [_parse_result_set_player_row(row, game_id=game_id) for row in _result_set_rows(player_set)]
    teams = [_parse_result_set_team_row(row) for row in _result_set_rows(team_set)]
    return SourceBoxScore(game_id=game_id, players=players, teams=teams)


def _parse_live_box_score_payload(game_payload: dict, *, game_id: str) -> SourceBoxScore:
    players: list[SourceBoxScorePlayer] = []
    teams: list[SourceBoxScoreTeam] = []
    for team_key in ("homeTeam", "awayTeam"):
        team_payload = game_payload.get(team_key)
        if not isinstance(team_payload, dict):
            continue
        teams.append(
            SourceBoxScoreTeam(
                team_id=_optional_int(team_payload, "teamId"),
                team_abbreviation=_optional_text(team_payload, "teamTricode"),
                plus_minus_raw=team_payload.get("statistics", {}).get("plusMinusPoints"),
                points_raw=team_payload.get("score"),
                raw_row=dict(team_payload),
            )
        )
        for player_payload in team_payload.get("players", []):
            if not isinstance(player_payload, dict):
                continue
            statistics = player_payload.get("statistics", {})
            minutes_raw = None
            if isinstance(statistics, dict):
                minutes_raw = statistics.get("minutesCalculated") or statistics.get("minutes")
            players.append(
                SourceBoxScorePlayer(
                    game_id=game_id,
                    team_id=_optional_int(team_payload, "teamId"),
                    team_abbreviation=_optional_text(team_payload, "teamTricode"),
                    player_id=_optional_int(player_payload, "personId"),
                    player_name=_build_live_player_name(player_payload),
                    minutes_raw=minutes_raw,
                    raw_row=dict(player_payload),
                )
            )
    for player in players:
        _validate_source_player_row(player)
    for team in teams:
        _validate_source_team_row(team)
    return SourceBoxScore(game_id=game_id, players=players, teams=teams)


def _first_result_set(payload: dict, *, label: str) -> dict:
    result_sets = payload.get("resultSets")
    if not isinstance(result_sets, list) or not result_sets:
        raise ValueError(f"{label.title()} payload is missing resultSets")
    first_result_set = result_sets[0]
    if not isinstance(first_result_set, dict):
        raise ValueError(f"{label.title()} payload has invalid result set")
    return first_result_set


def _result_set_rows(result_set: dict) -> list[dict[str, object]]:
    headers = result_set.get("headers")
    if not isinstance(headers, list) or not headers:
        raise ValueError("Result set is missing headers")

    if "rowSet" in result_set:
        raw_rows = result_set["rowSet"]
    elif "data" in result_set:
        raw_rows = result_set["data"]
    else:
        raise ValueError("Result set is missing row data")
    if not isinstance(raw_rows, list):
        raise ValueError("Result set row data must be a list")

    rows: list[dict[str, object]] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, list):
            raise ValueError("Result set row must be a list")
        rows.append(dict(zip(headers, raw_row, strict=False)))
    return rows


def _player_name_from_row(row: dict[str, object]) -> str:
    explicit_name = _optional_text_any(row, ("PLAYER_NAME",))
    if explicit_name:
        return explicit_name
    first_name = _optional_text_any(row, ("firstName",))
    family_name = _optional_text_any(row, ("familyName",))
    return " ".join(part for part in [first_name, family_name] if part)


def _build_live_player_name(player_payload: dict[str, object]) -> str:
    first_name = _optional_text_any(player_payload, ("firstName",))
    family_name = _optional_text_any(player_payload, ("familyName",))
    return " ".join(part for part in [first_name, family_name] if part)


def _required_text(row: dict[str, object], key: str) -> str:
    value = _optional_text(row, key)
    if not value:
        raise ValueError(f"Missing required {key}")
    return value


def _optional_text(row: dict[str, object], key: str) -> str:
    value = _row_value(row, key)
    if value is None:
        return ""
    return str(value).strip().upper() if key.endswith("ABBREVIATION") else str(value).strip()


def _required_int(row: dict[str, object], key: str) -> int:
    value = _optional_int(row, key)
    if value is None:
        raise ValueError(f"Missing required {key}")
    return value


def _optional_int(row: dict[str, object], key: str) -> int | None:
    value = _row_value(row, key)
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError(f"Invalid integer value for {key}: {value!r}")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid integer value for {key}: {value!r}") from exc


def _row_value(row: dict[str, object], key: str) -> object | None:
    if key in row:
        return row[key]
    alias_map = {
        "GAME_ID": ("gameId",),
        "TEAM_ID": ("teamId",),
        "TEAM_ABBREVIATION": ("teamTricode",),
        "PLAYER_ID": ("personId",),
        "MIN": ("minutes",),
        "PTS": ("points",),
        "PLUS_MINUS": ("plusMinusPoints",),
    }
    for alias in alias_map.get(key, ()):
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
    try:
        parsed_row = SourceBoxScorePlayer(
            game_id=_optional_text(row, "GAME_ID") or game_id,
            team_id=_optional_int(row, "TEAM_ID"),
            team_abbreviation=_optional_text(row, "TEAM_ABBREVIATION"),
            player_id=_optional_int(row, "PLAYER_ID"),
            player_name=_player_name_from_row(row),
            minutes_raw=_row_value(row, "MIN"),
            raw_row=row,
        )
    except ValueError as exc:
        raise ValueError(
            f"{exc}; nba_api_box_score_player_row={format_source_row(row)}"
        ) from exc
    _validate_source_player_row(parsed_row)
    return parsed_row


def _parse_result_set_team_row(row: dict[str, object]) -> SourceBoxScoreTeam:
    try:
        parsed_row = SourceBoxScoreTeam(
            team_id=_optional_int(row, "TEAM_ID"),
            team_abbreviation=_optional_text(row, "TEAM_ABBREVIATION"),
            plus_minus_raw=_row_value(row, "PLUS_MINUS"),
            points_raw=_row_value(row, "PTS"),
            raw_row=row,
        )
    except ValueError as exc:
        raise ValueError(
            f"{exc}; nba_api_box_score_team_row={format_source_row(row)}"
        ) from exc
    _validate_source_team_row(parsed_row)
    return parsed_row


def _validate_source_player_row(row: SourceBoxScorePlayer) -> None:
    classification = classify_source_player_row(row)
    if classification.should_skip:
        return
    if row.team_id is None and row.team_abbreviation.strip() == "":
        raise ValueError(
            "Missing TEAM_ID/TEAM_ABBREVIATION; "
            f"nba_api_box_score_player_row={format_source_row(row.raw_row)}"
        )
    resolve_source_team_identity(
        team_id=row.team_id,
        team_abbreviation=row.team_abbreviation or None,
    )
    if row.player_id is None or row.player_id <= 0:
        raise ValueError(
            "Missing or invalid PLAYER_ID; "
            f"nba_api_box_score_player_row={format_source_row(row.raw_row)}"
        )
    if row.player_name.strip() == "":
        raise ValueError(
            "Missing PLAYER_NAME; "
            f"nba_api_box_score_player_row={format_source_row(row.raw_row)}"
        )
    minutes = parse_minutes_to_float(row.minutes_raw)
    if minutes is None:
        raise ValueError(
            "Unparseable MIN value; "
            f"nba_api_box_score_player_row={format_source_row(row.raw_row)}"
        )


def _validate_source_team_row(row: SourceBoxScoreTeam) -> None:
    classification = classify_source_team_row(row)
    if classification.should_skip:
        return
    if row.team_id is None and row.team_abbreviation.strip() == "":
        raise ValueError(
            "Missing TEAM_ID/TEAM_ABBREVIATION; "
            f"nba_api_box_score_team_row={format_source_row(row.raw_row)}"
        )
    resolve_source_team_identity(
        team_id=row.team_id,
        team_abbreviation=row.team_abbreviation or None,
    )
    if row.plus_minus_raw is not None and parse_box_score_numeric_value(row.plus_minus_raw) is None:
        raise ValueError(
            "Unparseable PLUS_MINUS value; "
            f"nba_api_box_score_team_row={format_source_row(row.raw_row)}"
        )
    if row.points_raw is not None and parse_box_score_numeric_value(row.points_raw) is None:
        raise ValueError(
            "Unparseable PTS value; "
            f"nba_api_box_score_team_row={format_source_row(row.raw_row)}"
        )
