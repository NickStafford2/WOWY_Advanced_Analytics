from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

from wowy.nba.cache import load_cached_payload
from wowy.nba.models import CanonicalGamePlayerRecord, CanonicalGameRecord
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.season_types import canonicalize_season_type
from wowy.nba.source_models import (
    SourceBoxScore,
    SourceBoxScorePlayer,
    SourceBoxScoreTeam,
    SourceLeagueGame,
    SourceLeagueSchedule,
)
from wowy.nba.team_identity import (
    canonical_team_lookup_abbreviation,
    resolve_source_team_identity,
)


@dataclass(frozen=True)
class SourcePlayerRowClassification:
    kind: str
    should_skip: bool = False


@dataclass(frozen=True)
class SourceTeamRowClassification:
    kind: str
    should_skip: bool = False


@dataclass(frozen=True)
class SourceScheduleRowClassification:
    kind: str
    should_skip: bool = False


PLAYER_DID_NOT_PLAY_PLACEHOLDER = SourcePlayerRowClassification(
    kind="player_did_not_play_placeholder",
    should_skip=True,
)
INACTIVE_PLAYER_STATUS_ROW = SourcePlayerRowClassification(
    kind="inactive_player_status_row",
    should_skip=True,
)
CANONICAL_PLAYER_SOURCE_ROW = SourcePlayerRowClassification(
    kind="canonical_player_source_row",
    should_skip=False,
)
CANONICAL_TEAM_SOURCE_ROW = SourceTeamRowClassification(
    kind="canonical_team_source_row",
    should_skip=False,
)
CANONICAL_SCHEDULE_SOURCE_ROW = SourceScheduleRowClassification(
    kind="canonical_schedule_source_row",
    should_skip=False,
)


def load_player_names_from_cache(source_data_dir: Path) -> dict[int, str]:
    player_names: dict[int, str] = {}
    for cache_path in sorted((source_data_dir / "boxscores").glob("*.json")):
        payload = load_cached_payload(cache_path)
        if payload is None:
            continue
        parsed_box_score = parse_box_score_payload(
            payload,
            game_id=cache_path.stem.split("_", maxsplit=1)[0],
        )
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
            )
        except ValueError as exc:
            raise ValueError(
                f"{exc}; nba_api_league_schedule_row={_format_source_row(row)}"
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
                    f"first_row={_format_source_row(existing_game.raw_row)} "
                    f"second_row={_format_source_row(game.raw_row)}"
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
                f"nba_api_box_score_payload={_format_source_row(payload)}"
            )
        return _parse_result_set_box_score(player_set=player_set, team_set=team_set, game_id=game_id)

    raise ValueError(f"Box score payload is missing resultSets for game {game_id!r}")


def normalize_source_game(
    *,
    schedule_game: SourceLeagueGame,
    box_score: SourceBoxScore,
    season: str,
    season_type: str,
    source: str = "nba_api",
) -> tuple[CanonicalGameRecord, list[CanonicalGamePlayerRecord]]:
    season = canonicalize_season_string(season)
    season_type = canonicalize_season_type(season_type)
    team_stat, opponent_stat = _resolve_game_teams(schedule_game=schedule_game, box_score=box_score)

    team_identity = resolve_source_team_identity(
        team_id=team_stat.team_id,
        team_abbreviation=team_stat.team_abbreviation,
        fallback_team_id=schedule_game.team_id,
        fallback_abbreviation=schedule_game.team_abbreviation,
    )
    opponent_identity = resolve_source_team_identity(
        team_id=opponent_stat.team_id,
        team_abbreviation=opponent_stat.team_abbreviation,
    )

    player_rows = [
        player
        for player in box_score.players
        if _source_row_matches_team(
            row_team_id=player.team_id,
            row_team_abbreviation=player.team_abbreviation,
            expected_team_id=team_identity.team_id,
            expected_team_abbreviation=team_identity.abbreviation,
        )
    ]
    players = _normalize_players(
        game_id=schedule_game.game_id,
        team_identity=team_identity,
        player_rows=player_rows,
    )
    if not any(player.appeared for player in players):
        raise ValueError(
            f"No active players found for team {team_identity.abbreviation!r} in game "
            f"{schedule_game.game_id!r}; "
            f"nba_api_box_score_player_rows={_format_source_rows([row.raw_row for row in player_rows])}"
        )

    margin = _resolve_margin(team_stat=team_stat, opponent_stat=opponent_stat, game_id=schedule_game.game_id)
    game = CanonicalGameRecord(
        game_id=schedule_game.game_id,
        season=season,
        game_date=schedule_game.game_date,
        team=team_identity.abbreviation,
        opponent=opponent_identity.abbreviation,
        is_home=extract_is_home(schedule_game.matchup, team_identity.abbreviation),
        margin=margin,
        season_type=season_type,
        source=source,
        team_id=team_identity.team_id,
        opponent_team_id=opponent_identity.team_id,
    )
    return game, players


def extract_opponent(matchup: str, team_abbreviation: str) -> str:
    left, right, _separator = _split_matchup(matchup)
    if _matchup_side_matches_requested_team(left, team_abbreviation):
        return right
    if _matchup_side_matches_requested_team(right, team_abbreviation):
        return left
    raise ValueError(f"Failed to parse opponent from matchup {matchup!r}")


def extract_is_home(matchup: str, team_abbreviation: str) -> bool:
    left, right, separator = _split_matchup(matchup)
    if separator == "vs.":
        if _matchup_side_matches_requested_team(left, team_abbreviation):
            return True
        if _matchup_side_matches_requested_team(right, team_abbreviation):
            return False
    else:
        if _matchup_side_matches_requested_team(left, team_abbreviation):
            return False
        if _matchup_side_matches_requested_team(right, team_abbreviation):
            return True
    raise ValueError(f"Failed to parse home/away from matchup {matchup!r}")


def parse_box_score_numeric_value(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        numeric_value = float(value)
        if not math.isfinite(numeric_value):
            return None
        return numeric_value

    text = str(value).strip()
    if not text:
        return None
    try:
        numeric_value = float(text)
    except ValueError:
        return None
    if not math.isfinite(numeric_value):
        return None
    return numeric_value


def parse_minutes_to_float(minutes: object) -> float | None:
    if minutes is None or isinstance(minutes, bool):
        return None
    if isinstance(minutes, int | float):
        numeric_minutes = float(minutes)
        if not math.isfinite(numeric_minutes):
            return None
        return numeric_minutes

    minute_text = str(minutes).strip()
    if not minute_text:
        return None
    if minute_text.startswith("PT"):
        return _parse_iso_duration_minutes(minute_text)
    if _is_known_inactive_status(minute_text):
        return None
    if minute_text in {"0", "0:00", "0.0"}:
        return 0.0
    if ":" not in minute_text:
        try:
            numeric_minutes = float(minute_text)
        except ValueError:
            return None
        if not math.isfinite(numeric_minutes):
            return None
        return numeric_minutes

    whole_minutes, seconds = minute_text.split(":", maxsplit=1)
    try:
        parsed_minutes = float(whole_minutes) + (float(seconds) / 60.0)
    except ValueError:
        return None
    if not math.isfinite(parsed_minutes):
        return None
    return parsed_minutes


def played_in_game(minutes: object) -> bool:
    parsed_minutes = parse_minutes_to_float(minutes)
    if parsed_minutes is None:
        return False
    return parsed_minutes > 0.0


def _parse_iso_duration_minutes(minute_text: str) -> float | None:
    body = minute_text.removeprefix("PT")
    if not body:
        return None

    minutes_value = 0.0
    seconds_value = 0.0
    if "M" in body:
        minute_part, body = body.split("M", maxsplit=1)
        if minute_part:
            try:
                minutes_value = float(minute_part)
            except ValueError:
                return None
    if body.endswith("S"):
        second_part = body[:-1]
        if second_part:
            try:
                seconds_value = float(second_part)
            except ValueError:
                return None

    parsed_minutes = minutes_value + (seconds_value / 60.0)
    if not math.isfinite(parsed_minutes):
        return None
    return parsed_minutes


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
            players.append(
                SourceBoxScorePlayer(
                    game_id=game_id,
                    team_id=_optional_int(team_payload, "teamId"),
                    team_abbreviation=_optional_text(team_payload, "teamTricode"),
                    player_id=_optional_int(player_payload, "personId"),
                    player_name=_build_live_player_name(player_payload),
                    minutes_raw=player_payload.get("statistics", {}).get("minutes"),
                    raw_row=dict(player_payload),
                )
            )
    for player in players:
        _validate_source_player_row(player)
    for team in teams:
        _validate_source_team_row(team)
    return SourceBoxScore(game_id=game_id, players=players, teams=teams)


def _normalize_players(
    *,
    game_id: str,
    team_identity,
    player_rows: list[SourceBoxScorePlayer],
) -> list[CanonicalGamePlayerRecord]:
    players: list[CanonicalGamePlayerRecord] = []
    for row in player_rows:
        player_id = row.player_id
        classification = classify_source_player_row(row)
        if classification.should_skip:
            continue
        if _is_skip_player_row(row):
            continue
        if player_id is None or player_id <= 0:
            raise ValueError(
                "Invalid box score player row with missing PLAYER_ID; "
                f"nba_api_box_score_player_row={_format_source_row(row.raw_row)}"
            )
        minutes = parse_minutes_to_float(row.minutes_raw)
        if row.player_name.strip() == "":
            raise ValueError(
                "Invalid box score player row with blank PLAYER_NAME; "
                f"nba_api_box_score_player_row={_format_source_row(row.raw_row)}"
            )
        if minutes is None and not _is_explicit_inactive_player(row.minutes_raw):
            raise ValueError(
                "Invalid box score player row with unparseable MIN value; "
                f"nba_api_box_score_player_row={_format_source_row(row.raw_row)}"
            )
        player_name = row.player_name.strip()
        players.append(
            CanonicalGamePlayerRecord(
                game_id=game_id,
                team=team_identity.abbreviation,
                player_id=player_id,
                player_name=player_name,
                appeared=played_in_game(row.minutes_raw),
                minutes=minutes,
                team_id=team_identity.team_id,
            )
        )
    return players


def _resolve_game_teams(
    *,
    schedule_game: SourceLeagueGame,
    box_score: SourceBoxScore,
) -> tuple[SourceBoxScoreTeam, SourceBoxScoreTeam]:
    matched_teams = [
        team
        for team in box_score.teams
        if _source_row_matches_team(
            row_team_id=team.team_id,
            row_team_abbreviation=team.team_abbreviation,
            expected_team_id=schedule_game.team_id,
            expected_team_abbreviation=schedule_game.team_abbreviation,
        )
    ]
    if not matched_teams:
        raise ValueError(
            f"Team {schedule_game.team_abbreviation!r} not found in box score for game "
            f"{schedule_game.game_id!r}; "
            f"nba_api_box_score_team_rows={_format_source_rows([team.raw_row for team in box_score.teams])}"
        )
    if len(matched_teams) > 1:
        raise ValueError(
            f"Multiple team rows matched {schedule_game.team_abbreviation!r} for game "
            f"{schedule_game.game_id!r}; "
            f"nba_api_box_score_team_rows={_format_source_rows([team.raw_row for team in matched_teams])}"
        )

    opponent_teams = [team for team in box_score.teams if team is not matched_teams[0]]
    if len(opponent_teams) != 1:
        raise ValueError(
            f"Expected exactly one opponent row in box score for game {schedule_game.game_id!r}; "
            f"found {len(opponent_teams)}; "
            f"nba_api_box_score_team_rows={_format_source_rows([team.raw_row for team in box_score.teams])}"
        )
    return matched_teams[0], opponent_teams[0]


def _resolve_margin(
    *,
    team_stat: SourceBoxScoreTeam,
    opponent_stat: SourceBoxScoreTeam,
    game_id: str,
) -> float:
    plus_minus = parse_box_score_numeric_value(team_stat.plus_minus_raw)
    if plus_minus is not None:
        return plus_minus

    team_points = parse_box_score_numeric_value(team_stat.points_raw)
    opponent_points = parse_box_score_numeric_value(opponent_stat.points_raw)
    if team_points is None or opponent_points is None:
        raise ValueError(
            f"Could not derive margin from team stats for game {game_id!r}; "
            f"nba_api_team_row={_format_source_row(team_stat.raw_row)} "
            f"nba_api_opponent_team_row={_format_source_row(opponent_stat.raw_row)}"
        )
    return team_points - opponent_points


def _source_row_matches_team(
    *,
    row_team_id: int | None,
    row_team_abbreviation: str,
    expected_team_id: int,
    expected_team_abbreviation: str,
) -> bool:
    if row_team_id is not None:
        return row_team_id == expected_team_id
    return canonical_team_lookup_abbreviation(row_team_abbreviation) == canonical_team_lookup_abbreviation(
        expected_team_abbreviation
    )


def _split_matchup(matchup: str) -> tuple[str, str, str]:
    raw_matchup = matchup.strip()
    if " vs. " in raw_matchup:
        left, right = raw_matchup.split(" vs. ", maxsplit=1)
        return left.strip(), right.strip(), "vs."
    if " @ " in raw_matchup:
        left, right = raw_matchup.split(" @ ", maxsplit=1)
        return left.strip(), right.strip(), "@"
    raise ValueError(f"Unrecognized matchup string {matchup!r}")


def _matchup_side_matches_requested_team(side: str, team_abbreviation: str) -> bool:
    return canonical_team_lookup_abbreviation(side) == canonical_team_lookup_abbreviation(
        team_abbreviation
    )


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
            f"{exc}; nba_api_box_score_player_row={_format_source_row(row)}"
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
            f"{exc}; nba_api_box_score_team_row={_format_source_row(row)}"
        ) from exc
    _validate_source_team_row(parsed_row)
    return parsed_row


def _validate_source_player_row(row: SourceBoxScorePlayer) -> None:
    classification = classify_source_player_row(row)
    if classification.should_skip:
        return
    if _is_skip_player_row(row):
        return
    if row.team_id is None and row.team_abbreviation.strip() == "":
        raise ValueError(
            "Missing TEAM_ID/TEAM_ABBREVIATION; "
            f"nba_api_box_score_player_row={_format_source_row(row.raw_row)}"
        )
    resolve_source_team_identity(
        team_id=row.team_id,
        team_abbreviation=row.team_abbreviation or None,
    )
    if row.player_id is None or row.player_id <= 0:
        raise ValueError(
            "Missing or invalid PLAYER_ID; "
            f"nba_api_box_score_player_row={_format_source_row(row.raw_row)}"
        )
    if row.player_name.strip() == "":
        raise ValueError(
            "Missing PLAYER_NAME; "
            f"nba_api_box_score_player_row={_format_source_row(row.raw_row)}"
        )
    minutes = parse_minutes_to_float(row.minutes_raw)
    if minutes is None:
        raise ValueError(
            "Unparseable MIN value; "
            f"nba_api_box_score_player_row={_format_source_row(row.raw_row)}"
        )


def _validate_source_team_row(row: SourceBoxScoreTeam) -> None:
    classification = classify_source_team_row(row)
    if classification.should_skip:
        return
    if row.team_id is None and row.team_abbreviation.strip() == "":
        raise ValueError(
            "Missing TEAM_ID/TEAM_ABBREVIATION; "
            f"nba_api_box_score_team_row={_format_source_row(row.raw_row)}"
        )
    resolve_source_team_identity(
        team_id=row.team_id,
        team_abbreviation=row.team_abbreviation or None,
    )
    if row.plus_minus_raw is not None and parse_box_score_numeric_value(row.plus_minus_raw) is None:
        raise ValueError(
            "Unparseable PLUS_MINUS value; "
            f"nba_api_box_score_team_row={_format_source_row(row.raw_row)}"
        )
    if row.points_raw is not None and parse_box_score_numeric_value(row.points_raw) is None:
        raise ValueError(
            "Unparseable PTS value; "
            f"nba_api_box_score_team_row={_format_source_row(row.raw_row)}"
        )


def classify_source_team_row(
    row: SourceBoxScoreTeam,
) -> SourceTeamRowClassification:
    # Team rows are currently simpler than player rows: the cached source scan did
    # not reveal a repeated team-row anomaly that should be silently skipped. We
    # keep the classifier anyway so future source patterns can be named in one
    # place without spreading more boolean checks through the pipeline.
    return CANONICAL_TEAM_SOURCE_ROW


def classify_source_schedule_row(
    row: dict[str, object],
) -> SourceScheduleRowClassification:
    # Schedule rows are currently simpler than player rows: the cached source scan
    # has not shown a repeated schedule-row anomaly that should be silently
    # skipped. We keep the classifier anyway so future source patterns can be
    # named in one place without spreading more boolean checks through the
    # pipeline.
    return CANONICAL_SCHEDULE_SOURCE_ROW


def _is_skip_player_row(row: SourceBoxScorePlayer) -> bool:
    if row.player_id not in {None, 0}:
        return False
    return row.player_name.strip() == "" and row.minutes_raw in {None, "", 0, "0", "0:00", "0.0"}


def classify_source_player_row(
    row: SourceBoxScorePlayer,
) -> SourcePlayerRowClassification:
    if _is_player_did_not_play_placeholder(row):
        return PLAYER_DID_NOT_PLAY_PLACEHOLDER
    if _is_inactive_player_status_row(row):
        return INACTIVE_PLAYER_STATUS_ROW
    return CANONICAL_PLAYER_SOURCE_ROW


def _is_player_did_not_play_placeholder(row: SourceBoxScorePlayer) -> bool:
    # Older NBA v2 box scores sometimes emit inactive roster placeholder rows with a
    # real PLAYER_ID but no PLAYER_NAME, no minutes, and no box score stats. Those
    # rows are not usable canonical player appearances, so we drop them before
    # normalization instead of inventing player identity data.
    if row.player_id is None or row.player_id <= 0:
        return False
    if row.player_name.strip() != "":
        return False
    if row.minutes_raw is not None:
        return False

    if _row_has_any_box_score_stats(row.raw_row):
        return False

    comment = str(row.raw_row.get("COMMENT", "")).strip().upper()
    if comment not in {"", "DNP - COACH'S DECISION"}:
        return False
    return True


def _is_inactive_player_status_row(row: SourceBoxScorePlayer) -> bool:
    # The NBA feed also emits inactive player status rows with a resolved player
    # identity plus a status comment like DNP/DND/NWT, but no minutes or box
    # score stats. Those rows are not appearances and should never enter the DB.
    if row.player_id is None or row.player_id <= 0:
        return False
    if row.player_name.strip() == "":
        return False
    if _row_has_any_box_score_stats(row.raw_row):
        return False

    minute_text = str(row.minutes_raw).strip() if row.minutes_raw is not None else ""
    comment = str(row.raw_row.get("COMMENT", "")).strip()
    if minute_text and _is_known_inactive_status(minute_text):
        return True
    if not comment:
        return False
    return _is_known_inactive_status(comment)


def _row_has_any_box_score_stats(raw_row: dict[str, object]) -> bool:
    stat_keys = (
        "AST",
        "BLK",
        "DREB",
        "FG3A",
        "FG3M",
        "FG3_PCT",
        "FGA",
        "FGM",
        "FG_PCT",
        "FTA",
        "FTM",
        "FT_PCT",
        "OREB",
        "PF",
        "PLUS_MINUS",
        "PTS",
        "REB",
        "STL",
        "TO",
    )
    return any(raw_row.get(key) is not None for key in stat_keys)


def _is_known_inactive_status(minute_text: str) -> bool:
    normalized = minute_text.strip().upper()
    inactive_prefixes = (
        "DNP",
        "DND",
        "NWT",
        "DID NOT",
        "NOT WITH TEAM",
        "SUSPENDED",
        "INACTIVE",
    )
    return normalized.startswith(inactive_prefixes)


def _format_source_row(row: dict[str, object]) -> str:
    return json.dumps(row, sort_keys=True, default=str)


def _format_source_rows(rows: list[dict[str, object]]) -> str:
    return json.dumps(rows, sort_keys=True, default=str)
