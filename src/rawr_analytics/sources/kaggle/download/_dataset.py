from __future__ import annotations

import importlib
from pathlib import Path
from shutil import copy2

DATASET_HANDLE = "eoinamoore/historical-nba-data-and-player-box-scores"
LOCAL_DATASET_DIR = Path("data/source/eoinamoore")


def download_dataset_to_repo() -> tuple[Path, Path]:
    kagglehub = importlib.import_module("kagglehub")
    cache_root_path = Path(kagglehub.dataset_download(DATASET_HANDLE))
    local_root_path = _copy_dataset_to_local_source(
        source_root=cache_root_path,
        target_root=LOCAL_DATASET_DIR,
    )
    return cache_root_path, local_root_path


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


__all__ = [
    "DATASET_HANDLE",
    "LOCAL_DATASET_DIR",
    "download_dataset_to_repo",
]
