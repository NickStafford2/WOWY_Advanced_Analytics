from pathlib import Path

import pyarrow.parquet as pq


def _parquet_path() -> Path:
    path = (
        Path(__file__).resolve().parents[1]
        / "data"
        / "source"
        / "eoinamoore"
        / "PlayByPlay.parquet"
    )
    assert path.exists(), path
    return path


def _print_parquet_overview(parquet_file: pq.ParquetFile, path: Path) -> None:
    metadata = parquet_file.metadata
    sample = next(parquet_file.iter_batches(batch_size=5)).to_pandas()

    print(f"path: {path}")
    print(f"rows: {metadata.num_rows}")
    print(f"columns: {metadata.num_columns}")
    print(f"row_groups: {metadata.num_row_groups}")
    print(sample.head())
    print(list(sample.columns))
    print(sample.dtypes)


def _find_oldest_game(parquet_file: pq.ParquetFile) -> tuple[str, object]:
    oldest_game_id = None
    oldest_game_date = None

    for batch in parquet_file.iter_batches(
        batch_size=50_000,
        columns=["gameId", "gameDateTimeEst"],
    ):
        batch_frame = batch.to_pandas().dropna(subset=["gameDateTimeEst"])
        if batch_frame.empty:
            continue

        batch_oldest = batch_frame.loc[batch_frame["gameDateTimeEst"].idxmin()]

        if oldest_game_date is None or batch_oldest["gameDateTimeEst"] < oldest_game_date:
            oldest_game_id = str(batch_oldest["gameId"])
            oldest_game_date = batch_oldest["gameDateTimeEst"]

    assert oldest_game_id is not None
    assert oldest_game_date is not None
    return oldest_game_id, oldest_game_date


def _main() -> None:
    path = _parquet_path()
    parquet_file = pq.ParquetFile(path)

    # _print_parquet_overview(parquet_file, path)

    oldest_game_id, oldest_game_date = _find_oldest_game(parquet_file)
    print(f"oldest_game_id: {oldest_game_id}")
    print(f"oldest_game_date_est: {oldest_game_date}")


if __name__ == "__main__":
    _main()
