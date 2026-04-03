from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from rawr_analytics.shared.game import (
    NormalizedGamePlayerRecord,
    NormalizedGameRecord,
    NormalizedTeamSeasonBatch,
)
from rawr_analytics.shared.player import PlayerSummary
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team
from rawr_analytics.sources.kaggle.download._dataset import LOCAL_DATASET_DIR
from rawr_analytics.sources.kaggle.ingest._models import (
    KaggleIngestResult,
    KaggleIngestScopeSummary,
)
from rawr_analytics.sources.kaggle.ingest._store import store_team_season
from rawr_analytics.sources.kaggle.ingest._validation import validate_normalized_team_season_batch

_SUPPORTED_GAME_TYPE_MAP = {
    "Regular Season": "Regular Season",
    "NBA Emirates Cup": "Regular Season",
    "Emirates NBA Cup": "Regular Season",
    "NBA Cup": "Regular Season",
    "Playoffs": "Playoffs",
    "Preseason": "Preseason",
    "Pre Season": "Preseason",
}
_SKIPPED_GAME_TYPES = {
    "",
    "All-Star Game",
    "Play-in Tournament",
    "in-season-knockout",
}
_PLAYER_ACTIVITY_FIELDS = (
    "points",
    "assists",
    "blocks",
    "steals",
    "fieldGoalsAttempted",
    "fieldGoalsMade",
    "threePointersAttempted",
    "threePointersMade",
    "freeThrowsAttempted",
    "freeThrowsMade",
    "reboundsDefensive",
    "reboundsOffensive",
    "reboundsTotal",
    "foulsPersonal",
    "turnovers",
    "plusMinusPoints",
)


@dataclass(frozen=True)
class _KaggleScopeData:
    scope: TeamSeasonScope
    games: list[NormalizedGameRecord]
    game_players: list[NormalizedGamePlayerRecord]
    expected_games_row_count: int
    skipped_games_row_count: int


def ingest_kaggle_dataset(
    source_root: Path = LOCAL_DATASET_DIR,
) -> KaggleIngestResult:
    source_root = Path(source_root)
    games_path = source_root / "Games.csv"
    player_statistics_path = source_root / "PlayerStatistics.csv"
    players_path = source_root / "Players.csv"
    game_rows = _load_game_rows(games_path)
    scope_data_by_key = _build_scope_games(game_rows)
    player_names_by_id = _load_player_names(players_path)
    _load_player_rows(player_statistics_path, scope_data_by_key, game_rows, player_names_by_id)
    source_snapshot = _build_source_snapshot(games_path, player_statistics_path, players_path)

    scope_summaries: list[KaggleIngestScopeSummary] = []
    total_games = 0
    total_game_players = 0
    total_skipped_games = 0

    for scope_key in sorted(scope_data_by_key, key=_scope_sort_key):
        scope_data = scope_data_by_key[scope_key]
        games, game_players = _filter_scope_player_coverage(
            scope_data.games,
            scope_data.game_players,
        )
        if not games:
            continue
        batch = NormalizedTeamSeasonBatch(
            scope=scope_data.scope,
            games=games,
            game_players=game_players,
        )
        validate_normalized_team_season_batch(batch)
        store_team_season(
            team=scope_data.scope.team,
            season=scope_data.scope.season,
            games=games,
            game_players=game_players,
            source_path=str(source_root),
            source_snapshot=source_snapshot,
            expected_games_row_count=scope_data.expected_games_row_count,
            skipped_games_row_count=scope_data.skipped_games_row_count,
        )
        total_games += len(games)
        total_game_players += len(game_players)
        total_skipped_games += scope_data.skipped_games_row_count
        scope_summaries.append(
            KaggleIngestScopeSummary(
                team_id=scope_data.scope.team.team_id,
                season_id=scope_data.scope.season.id,
                season_type=scope_data.scope.season.season_type.to_nba_format(),
                games=len(games),
                game_players=len(game_players),
                skipped_games=scope_data.skipped_games_row_count,
            )
        )

    return KaggleIngestResult(
        source_root=source_root,
        games_path=games_path,
        player_statistics_path=player_statistics_path,
        source_snapshot=source_snapshot,
        scope_count=len(scope_summaries),
        game_count=total_games,
        game_player_count=total_game_players,
        skipped_game_count=total_skipped_games,
        skipped_game_types=tuple(sorted(_SKIPPED_GAME_TYPES)),
        scopes=tuple(scope_summaries),
    )


def _load_game_rows(games_path: Path) -> dict[str, dict[str, str]]:
    with games_path.open(newline="", encoding="utf-8-sig") as file:
        rows = list(csv.DictReader(file))
    rows_by_game_id: dict[str, dict[str, str]] = {}
    for row in rows:
        game_id = _required_text(row, "gameId")
        existing = rows_by_game_id.get(game_id)
        if existing is not None:
            _assert_same_game_row(existing, row)
            continue
        rows_by_game_id[game_id] = row
    return rows_by_game_id


def _build_scope_games(
    game_rows: dict[str, dict[str, str]],
) -> dict[tuple[int, str, str], _KaggleScopeData]:
    supported_games_by_scope: dict[tuple[int, str, str], list[NormalizedGameRecord]] = defaultdict(list)
    raw_game_counts: dict[tuple[int, str, str], int] = defaultdict(int)
    skipped_game_counts: dict[tuple[int, str, str], int] = defaultdict(int)

    for game_row in game_rows.values():
        normalized_game_type = _normalize_game_type(_required_text(game_row, "gameType"))
        game_date = _parse_game_date(_required_text(game_row, "gameDateTimeEst"))

        home_team = _optional_team_from_game_row(game_row, "hometeamId")
        away_team = _optional_team_from_game_row(game_row, "awayteamId")

        if normalized_game_type is None:
            for team in [home_team, away_team]:
                if team is None:
                    continue
                skipped_key = _scope_key(team=team, game_date=game_date, season_type="Regular Season")
                skipped_game_counts[skipped_key] += 1
            continue
        if home_team is None or away_team is None:
            for team in [home_team, away_team]:
                if team is None:
                    continue
                skipped_key = _scope_key(
                    team=team,
                    game_date=game_date,
                    season_type=normalized_game_type,
                )
                skipped_game_counts[skipped_key] += 1
            continue

        home_season = Season(str(_season_start_year_from_game_date(game_date)), normalized_game_type)
        away_season = Season(str(_season_start_year_from_game_date(game_date)), normalized_game_type)

        home_key = (home_team.team_id, home_season.id, home_season.season_type.to_nba_format())
        away_key = (away_team.team_id, away_season.id, away_season.season_type.to_nba_format())
        raw_game_counts[home_key] += 1
        raw_game_counts[away_key] += 1

        home_score = _required_int(game_row, "homeScore")
        away_score = _required_int(game_row, "awayScore")
        game_id = _required_text(game_row, "gameId")

        supported_games_by_scope[home_key].append(
            NormalizedGameRecord(
                game_id=game_id,
                game_date=game_date,
                season=home_season,
                team=home_team,
                opponent_team=away_team,
                is_home=True,
                margin=float(home_score - away_score),
                source="kaggle",
            )
        )
        supported_games_by_scope[away_key].append(
            NormalizedGameRecord(
                game_id=game_id,
                game_date=game_date,
                season=away_season,
                team=away_team,
                opponent_team=home_team,
                is_home=False,
                margin=float(away_score - home_score),
                source="kaggle",
            )
        )

    scope_data_by_key: dict[tuple[int, str, str], _KaggleScopeData] = {}
    for scope_key, games in supported_games_by_scope.items():
        team = Team.from_id(scope_key[0])
        season = Season(scope_key[1], scope_key[2])
        deduped_games = _dedupe_scope_games(games)
        scope_data_by_key[scope_key] = _KaggleScopeData(
            scope=TeamSeasonScope(team=team, season=season),
            games=deduped_games,
            game_players=[],
            expected_games_row_count=raw_game_counts.get(scope_key, len(deduped_games)),
            skipped_games_row_count=skipped_game_counts.get(scope_key, 0),
        )
    return scope_data_by_key


def _load_player_rows(
    player_statistics_path: Path,
    scope_data_by_key: dict[tuple[int, str, str], _KaggleScopeData],
    game_rows: dict[str, dict[str, str]],
    player_names_by_id: dict[int, str],
) -> None:
    player_keys_by_scope: dict[tuple[int, str, str], set[tuple[str, int, int]]] = defaultdict(set)
    with player_statistics_path.open(newline="", encoding="utf-8-sig") as file:
        for row in csv.DictReader(file):
            game_id = _required_text(row, "gameId")
            game_row = game_rows.get(game_id)
            if game_row is None:
                continue

            normalized_game_type = _normalize_game_type(_required_text(game_row, "gameType"))
            if normalized_game_type is None:
                continue
            home_team = _optional_team_from_game_row(game_row, "hometeamId")
            away_team = _optional_team_from_game_row(game_row, "awayteamId")
            if home_team is None or away_team is None:
                continue

            game_date = _parse_game_date(_required_text(game_row, "gameDateTimeEst"))
            is_home = _parse_home_flag(_required_text(row, "home"))
            team = home_team if is_home else away_team
            season = Season(str(_season_start_year_from_game_date(game_date)), normalized_game_type)
            scope_key = (team.team_id, season.id, season.season_type.to_nba_format())
            scope_data = scope_data_by_key.get(scope_key)
            assert scope_data is not None, f"Missing kaggle scope for player row {scope_key!r}"

            player_id = _required_int(row, "personId")
            player_name = _build_player_name(row, player_names_by_id=player_names_by_id)
            minutes = _parse_minutes(row.get("numMinutes", ""))
            player_key = (game_id, team.team_id, player_id)
            if player_key in player_keys_by_scope[scope_key]:
                raise ValueError(f"Duplicate kaggle canonical player row for {player_key!r}")
            player_keys_by_scope[scope_key].add(player_key)
            scope_data.game_players.append(
                NormalizedGamePlayerRecord(
                    game_id=game_id,
                    player=PlayerSummary(
                        player_id=player_id,
                        player_name=player_name,
                    ),
                    appeared=_did_player_appear(row, minutes=minutes),
                    minutes=minutes,
                    team=team,
                )
            )


def _dedupe_scope_games(games: list[NormalizedGameRecord]) -> list[NormalizedGameRecord]:
    games_by_key: dict[tuple[str, int], NormalizedGameRecord] = {}
    for game in games:
        key = (game.game_id, game.team.team_id)
        existing = games_by_key.get(key)
        if existing is not None:
            _assert_same_normalized_game(existing, game)
            continue
        games_by_key[key] = game
    return [games_by_key[key] for key in sorted(games_by_key, key=lambda item: (item[0], item[1]))]


def _filter_scope_player_coverage(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    appeared_counts_by_key: dict[tuple[str, int], int] = defaultdict(int)
    for player in game_players:
        if not player.appeared:
            continue
        key = (player.game_id, player.team.team_id)
        appeared_counts_by_key[key] += 1

    kept_game_keys = {
        (game.game_id, game.team.team_id)
        for game in games
        if appeared_counts_by_key.get((game.game_id, game.team.team_id), 0) >= 5
    }
    filtered_games = [
        game for game in games if (game.game_id, game.team.team_id) in kept_game_keys
    ]
    filtered_game_players = [
        player
        for player in game_players
        if (player.game_id, player.team.team_id) in kept_game_keys
    ]
    return filtered_games, filtered_game_players


def _normalize_game_type(raw_game_type: str) -> str | None:
    normalized = raw_game_type.strip()
    if normalized in _SUPPORTED_GAME_TYPE_MAP:
        return _SUPPORTED_GAME_TYPE_MAP[normalized]
    if normalized in _SKIPPED_GAME_TYPES:
        return None
    raise ValueError(f"Unsupported kaggle gameType {raw_game_type!r}")


def _parse_game_date(raw_datetime: str) -> str:
    return _required_text_value(raw_datetime, "gameDateTimeEst").split(" ", maxsplit=1)[0]


def _season_start_year_from_game_date(game_date: str) -> int:
    year_text, month_text, _ = game_date.split("-")
    year = int(year_text)
    month = int(month_text)
    return year if month >= 7 else year - 1


def _scope_key(*, team: Team, game_date: str, season_type: str) -> tuple[int, str, str]:
    season = Season(str(_season_start_year_from_game_date(game_date)), season_type)
    return team.team_id, season.id, season.season_type.to_nba_format()


def _build_source_snapshot(
    games_path: Path,
    player_statistics_path: Path,
    players_path: Path,
) -> str:
    parts = []
    for path in [games_path, player_statistics_path, players_path]:
        stat = path.stat()
        parts.append(f"{path.name}:{stat.st_size}:{stat.st_mtime_ns}")
    return "kaggle-v1:" + "|".join(parts)


def _load_player_names(players_path: Path) -> dict[int, str]:
    player_names_by_id: dict[int, str] = {}
    with players_path.open(newline="", encoding="utf-8-sig") as file:
        for row in csv.DictReader(file):
            player_id_text = row.get("personId", "").strip()
            if player_id_text == "":
                continue
            player_name = f"{row.get('firstName', '').strip()} {row.get('lastName', '').strip()}".strip()
            if player_name == "":
                continue
            player_names_by_id[int(player_id_text)] = player_name
    return player_names_by_id


def _build_player_name(row: dict[str, str], *, player_names_by_id: dict[int, str]) -> str:
    first_name = row.get("firstName", "").strip()
    last_name = row.get("lastName", "").strip()
    player_name = f"{first_name} {last_name}".strip()
    if player_name == "":
        person_id = _required_int(row, "personId")
        player_name = player_names_by_id.get(person_id, "")
    if player_name == "":
        raise ValueError(f"Blank player name in kaggle row for personId={row.get('personId')!r}")
    return player_name


def _parse_home_flag(raw_value: str) -> bool:
    normalized = raw_value.strip()
    if normalized == "1":
        return True
    if normalized == "0":
        return False
    raise ValueError(f"Invalid kaggle home flag {raw_value!r}")


def _required_text(row: dict[str, str], field_name: str) -> str:
    return _required_text_value(row.get(field_name, ""), field_name)


def _required_text_value(raw_value: str, field_name: str) -> str:
    normalized = raw_value.strip()
    if normalized == "":
        raise ValueError(f"Missing kaggle field {field_name!r}")
    return normalized


def _required_int(row: dict[str, str], field_name: str) -> int:
    return int(_required_text(row, field_name))


def _optional_team_from_game_row(row: dict[str, str], field_name: str) -> Team | None:
    team_id = _required_int(row, field_name)
    if team_id <= 0:
        return None
    try:
        return Team.from_id(team_id)
    except AssertionError:
        return None


def _required_float(row: dict[str, str], field_name: str) -> float:
    return float(_required_text(row, field_name))


def _parse_minutes(raw_value: str) -> float:
    normalized = raw_value.strip()
    if normalized == "":
        return 0.0
    minutes = float(normalized)
    if minutes < 0.0:
        return 0.0
    return minutes


def _did_player_appear(row: dict[str, str], *, minutes: float) -> bool:
    if minutes > 0.0:
        return True
    for field_name in _PLAYER_ACTIVITY_FIELDS:
        if _parse_optional_float(row.get(field_name, "")) != 0.0:
            return True
    return False


def _parse_optional_float(raw_value: str) -> float:
    normalized = raw_value.strip()
    if normalized == "":
        return 0.0
    return float(normalized)


def _assert_same_game_row(existing: dict[str, str], current: dict[str, str]) -> None:
    if existing != current:
        raise ValueError(
            f"Conflicting kaggle game rows for game_id={existing.get('gameId')!r}"
        )


def _assert_same_normalized_game(
    existing: NormalizedGameRecord,
    current: NormalizedGameRecord,
) -> None:
    if existing != current:
        raise ValueError(
            f"Conflicting kaggle canonical game rows for "
            f"game_id={existing.game_id!r} team_id={existing.team.team_id!r}"
        )


def _scope_sort_key(scope_key: tuple[int, str, str]) -> tuple[str, str, int]:
    team_id, season_id, season_type = scope_key
    return season_type, season_id, team_id


__all__ = [
    "KaggleIngestResult",
    "KaggleIngestScopeSummary",
    "ingest_kaggle_dataset",
]
