from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from rawr_analytics.download.kaggle._dataset import DATASET_HANDLE, download_dataset_to_repo


@dataclass(frozen=True)
class KaggleDownloadResult:
    dataset_handle: str
    kaggle_cache_path: Path
    local_root_path: Path
    csv_paths: tuple[Path, ...]


def download_dataset() -> KaggleDownloadResult:
    kaggle_cache_path, local_root_path = download_dataset_to_repo()
    csv_paths = tuple(sorted(local_root_path.rglob("*.csv")))
    if not csv_paths:
        raise ValueError(
            f"Downloaded {DATASET_HANDLE} into {local_root_path}, but no CSV files were found."
        )
    return KaggleDownloadResult(
        dataset_handle=DATASET_HANDLE,
        kaggle_cache_path=kaggle_cache_path,
        local_root_path=local_root_path,
        csv_paths=csv_paths,
    )


__all__ = [
    "KaggleDownloadResult",
    "download_dataset",
]
