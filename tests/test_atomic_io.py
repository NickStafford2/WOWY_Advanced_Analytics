from __future__ import annotations

from pathlib import Path

import pytest

from rawr_analytics.atomic_io import atomic_text_writer


def test_atomic_text_writer_replaces_file_atomically(tmp_path: Path) -> None:
    path = tmp_path / "output.csv"
    path.write_text("old\n", encoding="utf-8")

    with atomic_text_writer(path, newline="") as f:
        f.write("new\n")

    assert path.read_text(encoding="utf-8") == "new\n"
    assert not list(tmp_path.glob("*.tmp-*"))


def test_atomic_text_writer_cleans_up_temp_file_on_error(tmp_path: Path) -> None:
    path = tmp_path / "output.csv"
    path.write_text("old\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="boom"):
        with atomic_text_writer(path, newline="") as f:
            f.write("new\n")
            raise RuntimeError("boom")

    assert path.read_text(encoding="utf-8") == "old\n"
    assert not list(tmp_path.glob("*.tmp-*"))
