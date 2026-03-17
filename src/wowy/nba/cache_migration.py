from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from wowy.atomic_io import atomic_text_writer
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.ingest import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
)
from wowy.nba.seasons import canonicalize_season_string, season_sort_key


LogFn = Callable[[str], None]


@dataclass(frozen=True)
class CacheSeasonMigrationSummary:
    renamed_files: int = 0
    rewritten_files: int = 0
    updated_db_rows: int = 0


def normalize_cache_season_keys(
    *,
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
    normalized_game_players_input_dir: Path = DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    wowy_output_dir: Path = DEFAULT_WOWY_GAMES_DIR,
    combined_wowy_csv: Path = Path("data/combined/wowy/games.csv"),
    combined_rawr_games_csv: Path = Path("data/combined/rawr/games.csv"),
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    log: LogFn | None = print,
) -> CacheSeasonMigrationSummary:
    renamed_files = 0
    rewritten_files = 0
    updated_db_rows = 0

    renamed, rewritten = normalize_team_season_json_cache(
        source_data_dir / "team_seasons",
        log=log,
    )
    renamed_files += renamed
    rewritten_files += rewritten

    renamed, rewritten = normalize_team_season_csv_cache(
        normalized_games_input_dir,
        rewrite_season_column=True,
        log=log,
    )
    renamed_files += renamed
    rewritten_files += rewritten

    renamed, rewritten = normalize_team_season_csv_cache(
        normalized_game_players_input_dir,
        rewrite_season_column=False,
        log=log,
    )
    renamed_files += renamed
    rewritten_files += rewritten

    renamed, rewritten = normalize_team_season_csv_cache(
        wowy_output_dir,
        rewrite_season_column=True,
        log=log,
    )
    renamed_files += renamed
    rewritten_files += rewritten

    rewritten_files += normalize_combined_csv(
        combined_wowy_csv,
        season_column="season",
        log=log,
    )
    rewritten_files += normalize_combined_csv(
        combined_rawr_games_csv,
        season_column="season",
        log=log,
    )
    updated_db_rows += normalize_player_metrics_db(player_metrics_db_path, log=log)

    return CacheSeasonMigrationSummary(
        renamed_files=renamed_files,
        rewritten_files=rewritten_files,
        updated_db_rows=updated_db_rows,
    )


def normalize_team_season_json_cache(
    directory: Path,
    *,
    log: LogFn | None,
) -> tuple[int, int]:
    if not directory.exists():
        return 0, 0

    renamed_files = 0
    for path in sorted(directory.glob("*.json")):
        team, season, remainder = parse_team_season_json_filename(path.name)
        canonical_season = canonicalize_season_string(season)
        target_name = f"{team}_{canonical_season}_{remainder}"
        if target_name == path.name:
            continue
        rename_with_conflict_check(path, path.with_name(target_name))
        renamed_files += 1
        if log is not None:
            log(f"rename {path.name} -> {target_name}")
    return renamed_files, 0


def normalize_team_season_csv_cache(
    directory: Path,
    *,
    rewrite_season_column: bool,
    log: LogFn | None,
) -> tuple[int, int]:
    if not directory.exists():
        return 0, 0

    renamed_files = 0
    rewritten_files = 0
    for path in sorted(directory.glob("*.csv")):
        team, season = parse_team_season_csv_filename(path.name)
        canonical_season = canonicalize_season_string(season)
        if rewrite_season_column:
            if rewrite_csv_season_column(path, season_column="season"):
                rewritten_files += 1
                if log is not None:
                    log(f"rewrite season column {path.name}")
        target_name = f"{team}_{canonical_season}.csv"
        if target_name == path.name:
            continue
        rename_with_conflict_check(path, path.with_name(target_name))
        renamed_files += 1
        if log is not None:
            log(f"rename {path.name} -> {target_name}")
    return renamed_files, rewritten_files


def normalize_combined_csv(
    csv_path: Path,
    *,
    season_column: str,
    log: LogFn | None,
) -> int:
    if not csv_path.exists():
        return 0
    rewritten = rewrite_csv_season_column(csv_path, season_column=season_column)
    if rewritten and log is not None:
        log(f"rewrite season column {csv_path}")
    return 1 if rewritten else 0


def rewrite_csv_season_column(csv_path: Path, *, season_column: str) -> bool:
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    if season_column not in fieldnames:
        return False

    changed = False
    for row in rows:
        original = (row.get(season_column) or "").strip()
        canonical = canonicalize_season_string(original)
        if canonical != original:
            row[season_column] = canonical
            changed = True

    if not changed:
        return False

    with atomic_text_writer(csv_path, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return True


def normalize_player_metrics_db(
    db_path: Path,
    *,
    log: LogFn | None,
) -> int:
    if not db_path.exists():
        return 0

    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    updated_rows = 0
    try:
        table_names = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        connection.execute("BEGIN")
        if "metric_player_season_values" in table_names:
            updated_rows += rewrite_table_with_canonical_seasons(
                connection,
                table_name="metric_player_season_values",
                key_fields=("metric", "scope_key", "season", "player_id"),
            )
        if "metric_full_span_points" in table_names:
            updated_rows += rewrite_table_with_canonical_seasons(
                connection,
                table_name="metric_full_span_points",
                key_fields=("metric", "scope_key", "player_id", "season"),
            )
        if "player_season_metrics" in table_names:
            updated_rows += rewrite_table_with_canonical_seasons(
                connection,
                table_name="player_season_metrics",
                key_fields=("metric", "season", "player_id"),
            )
        if "metric_scope_catalog" in table_names:
            updated_rows += rewrite_metric_scope_catalog(connection)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    if updated_rows > 0 and log is not None:
        log(f"updated {updated_rows} SQLite rows in {db_path}")
    return updated_rows


def rewrite_table_with_canonical_seasons(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    key_fields: tuple[str, ...],
) -> int:
    rows = connection.execute(f"SELECT * FROM {table_name}").fetchall()
    if not rows:
        return 0

    columns = rows[0].keys()
    transformed_rows = []
    changed = 0
    deduped: dict[tuple[object, ...], tuple[object, ...]] = {}
    for row in rows:
        values = dict(row)
        original = values["season"]
        canonical = canonicalize_season_string(original)
        if canonical != original:
            values["season"] = canonical
            changed += 1
        tuple_values = tuple(values[column] for column in columns)
        key = tuple(values[field] for field in key_fields)
        existing = deduped.get(key)
        if existing is not None and existing != tuple_values:
            raise ValueError(
                f"Season canonicalization would create conflicting rows in {table_name} for key {key!r}"
            )
        deduped[key] = tuple_values

    if changed == 0:
        return 0

    placeholders = ",".join("?" for _ in columns)
    connection.execute(f"DELETE FROM {table_name}")
    connection.executemany(
        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
        list(deduped.values()),
    )
    return changed


def rewrite_metric_scope_catalog(connection: sqlite3.Connection) -> int:
    rows = connection.execute("SELECT * FROM metric_scope_catalog").fetchall()
    if not rows:
        return 0

    changed = 0
    for row in rows:
        available_seasons = json.loads(row["available_seasons_json"])
        canonical_seasons = sorted(
            {canonicalize_season_string(season) for season in available_seasons},
            key=season_sort_key,
        )
        start_season = canonicalize_nullable_season(row["full_span_start_season"])
        end_season = canonicalize_nullable_season(row["full_span_end_season"])
        if canonical_seasons:
            start_season = canonical_seasons[0]
            end_season = canonical_seasons[-1]

        available_seasons_json = json.dumps(canonical_seasons)
        if (
            available_seasons_json != row["available_seasons_json"]
            or start_season != row["full_span_start_season"]
            or end_season != row["full_span_end_season"]
        ):
            connection.execute(
                """
                UPDATE metric_scope_catalog
                SET available_seasons_json = ?,
                    full_span_start_season = ?,
                    full_span_end_season = ?
                WHERE metric = ? AND scope_key = ?
                """,
                (
                    available_seasons_json,
                    start_season,
                    end_season,
                    row["metric"],
                    row["scope_key"],
                ),
            )
            changed += 1
    return changed


def parse_team_season_csv_filename(filename: str) -> tuple[str, str]:
    stem = Path(filename).stem
    team, separator, season = stem.partition("_")
    if not separator or not team or not season:
        raise ValueError(
            f"Unexpected team-season filename {filename!r}. Expected TEAM_SEASON.csv."
        )
    return team.upper(), season


def parse_team_season_json_filename(filename: str) -> tuple[str, str, str]:
    if not filename.endswith(".json"):
        raise ValueError(f"Unexpected source cache filename {filename!r}")
    stem = filename.removesuffix(".json")
    team, separator, remainder = stem.partition("_")
    if not separator or not team or not remainder:
        raise ValueError(f"Unexpected source cache filename {filename!r}")
    parts = remainder.split("_", maxsplit=1)
    if len(parts) != 2:
        raise ValueError(f"Unexpected source cache filename {filename!r}")
    season, suffix = parts
    return team.upper(), season, f"{suffix}.json"


def rename_with_conflict_check(source_path: Path, target_path: Path) -> None:
    if source_path == target_path:
        return
    if target_path.exists():
        if source_path.read_bytes() == target_path.read_bytes():
            source_path.unlink()
            return
        source_empty = path_is_effectively_empty(source_path)
        target_empty = path_is_effectively_empty(target_path)
        if source_empty and not target_empty:
            source_path.unlink()
            return
        if target_empty and not source_empty:
            target_path.unlink()
            source_path.replace(target_path)
            return
        raise ValueError(
            f"Cannot rename {source_path} to {target_path}: target already exists with different contents"
        )
    source_path.replace(target_path)


def canonicalize_nullable_season(value: str | None) -> str | None:
    if value is None:
        return None
    return canonicalize_season_string(value)


def path_is_effectively_empty(path: Path) -> bool:
    if path.suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        result_sets = payload.get("resultSets", [])
        if not result_sets:
            return True
        return len(result_sets[0].get("rowSet", [])) == 0
    if path.suffix == ".csv":
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            next(reader, None)
            return next(reader, None) is None
    raise ValueError(f"Unsupported cache file for emptiness check: {path}")
