from pathlib import Path

import pandas as pd


def _eoinamoore_dir() -> Path:
    path = Path(__file__).resolve().parents[1] / "data" / "source" / "eoinamoore"
    assert path.exists(), path
    return path


def _csv_paths(root: Path) -> list[Path]:
    paths = sorted(root.glob("*.csv"))
    assert paths, root
    return paths


def _read_schema_sample(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, nrows=1_000, low_memory=False)


def _print_schema(path: Path) -> None:
    sample = _read_schema_sample(path)

    print(path.name)
    print(f"sample_rows: {len(sample)}")
    print(f"columns: {len(sample.columns)}")

    for column_name, dtype in sample.dtypes.items():
        print(f"  {column_name}: {dtype}")

    print()


def _main() -> None:
    root = _eoinamoore_dir()

    for path in _csv_paths(root):
        _print_schema(path)


if __name__ == "__main__":
    _main()
