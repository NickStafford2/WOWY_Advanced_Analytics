from __future__ import annotations

import sys

from rawr_analytics.sources.kaggle.download import download_dataset


def main(argv: list[str] | None = None) -> int:
    del argv
    result = download_dataset()
    sys.stdout.write(
        "\n".join(
            [
                f"dataset: {result.dataset_handle}",
                f"kaggle_cache: {result.kaggle_cache_path}",
                f"local_root: {result.local_root_path}",
                f"csv_files: {len(result.csv_paths)}",
            ]
        )
        + "\n"
    )
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
