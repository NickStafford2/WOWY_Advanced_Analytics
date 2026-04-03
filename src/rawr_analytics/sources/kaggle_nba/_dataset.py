from __future__ import annotations

import importlib
from dataclasses import dataclass
from pathlib import Path
from shutil import copy2

_DATASET_HANDLE = "eoinamoore/historical-nba-data-and-player-box-scores"
_LOCAL_DATASET_DIR = Path("data/source/eoinamoore")


@dataclass(frozen=True)
class KaggleDatasetSnapshot:
    dataset_handle: str
    kaggle_cache_path: Path
    local_root_path: Path
    csv_paths: tuple[Path, ...]


def download_dataset_snapshot() -> KaggleDatasetSnapshot:
    kagglehub = importlib.import_module("kagglehub")
    kaggle_cache_path = Path(kagglehub.dataset_download(_DATASET_HANDLE))
    local_root_path = _copy_dataset_to_local_source(
        source_root=kaggle_cache_path,
        target_root=_LOCAL_DATASET_DIR,
    )
    csv_paths = tuple(sorted(local_root_path.rglob("*.csv")))
    if not csv_paths:
        raise ValueError(
            "Kaggle dataset "
            f"{_DATASET_HANDLE} downloaded to {local_root_path} "
            "but no CSV files were found."
        )
    return KaggleDatasetSnapshot(
        dataset_handle=_DATASET_HANDLE,
        kaggle_cache_path=kaggle_cache_path,
        local_root_path=local_root_path,
        csv_paths=csv_paths,
    )


def _copy_dataset_to_local_source(*, source_root: Path, target_root: Path) -> Path:
    target_root.mkdir(parents=True, exist_ok=True)
    for source_path in sorted(source_root.rglob("*")):
        if not source_path.is_file():
            continue
        relative_path = source_path.relative_to(source_root)
        target_path = target_root / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        copy2(source_path, target_path)
    return target_root


__all__ = ["KaggleDatasetSnapshot", "download_dataset_snapshot"]
