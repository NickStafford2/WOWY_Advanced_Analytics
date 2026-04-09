from pathlib import Path

import pandas as pd


_QUARTER_COLUMNS = ["q1Points", "q2Points", "q3Points", "q4Points"]


def _team_statistics_path() -> Path:
    path = (
        Path(__file__).resolve().parents[1]
        / "data"
        / "source"
        / "eoinamoore"
        / "TeamStatistics.csv"
    )
    assert path.exists(), path
    return path


def _empty_result() -> dict[str, object]:
    return {"gameId": None, "gameDateTimeEst": None, "teamCity": None, "teamName": None}


def _update_oldest(
    current_oldest: dict[str, object],
    candidate_row: pd.Series,
) -> dict[str, object]:
    candidate_date = candidate_row["gameDateTimeEst"]
    current_date = current_oldest["gameDateTimeEst"]

    if current_date is None or candidate_date < current_date:
        return {
            "gameId": int(candidate_row["gameId"]),
            "gameDateTimeEst": candidate_date,
            "teamCity": candidate_row["teamCity"],
            "teamName": candidate_row["teamName"],
        }

    return current_oldest


def _scan_quarter_coverage(path: Path) -> tuple[dict[str, dict[str, object]], dict[str, object]]:
    use_columns = ["gameId", "gameDateTimeEst", "teamCity", "teamName", *_QUARTER_COLUMNS]
    oldest_by_quarter = {column: _empty_result() for column in _QUARTER_COLUMNS}
    oldest_complete_game = _empty_result()

    for chunk in pd.read_csv(
        path,
        usecols=use_columns,
        parse_dates=["gameDateTimeEst"],
        chunksize=100_000,
        low_memory=False,
    ):
        for quarter_column in _QUARTER_COLUMNS:
            quarter_rows = chunk.loc[chunk[quarter_column].notna()]
            if quarter_rows.empty:
                continue

            candidate_row = quarter_rows.loc[quarter_rows["gameDateTimeEst"].idxmin()]
            oldest_by_quarter[quarter_column] = _update_oldest(
                oldest_by_quarter[quarter_column],
                candidate_row,
            )

        complete_rows = chunk.loc[chunk[_QUARTER_COLUMNS].notna().all(axis=1)]
        if complete_rows.empty:
            continue

        candidate_row = complete_rows.loc[complete_rows["gameDateTimeEst"].idxmin()]
        oldest_complete_game = _update_oldest(oldest_complete_game, candidate_row)

    return oldest_by_quarter, oldest_complete_game


def _print_result(label: str, result: dict[str, object]) -> None:
    print(label)
    print(f"  gameId: {result['gameId']}")
    print(f"  gameDateTimeEst: {result['gameDateTimeEst']}")
    print(f"  team: {result['teamCity']} {result['teamName']}")


def _main() -> None:
    path = _team_statistics_path()
    oldest_by_quarter, oldest_complete_game = _scan_quarter_coverage(path)

    print(f"path: {path}")
    print()

    for quarter_column in _QUARTER_COLUMNS:
        _print_result(f"earliest_{quarter_column}:", oldest_by_quarter[quarter_column])
        print()

    _print_result("earliest_game_with_all_four_quarters:", oldest_complete_game)


if __name__ == "__main__":
    _main()
