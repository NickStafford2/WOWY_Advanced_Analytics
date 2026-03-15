from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, TextIO


@contextmanager
def atomic_text_writer(path: Path | str, newline: str | None = None) -> Iterator[TextIO]:
    """Write a text file atomically by replacing the destination after a flush."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp-{os.getpid()}")

    try:
        with open(temp_path, "w", encoding="utf-8", newline=newline) as f:
            yield f
            f.flush()
            os.fsync(f.fileno())
        temp_path.replace(path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
