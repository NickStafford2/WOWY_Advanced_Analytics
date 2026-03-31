from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TextIO


# TODO This is essential to be used. check to ensure this is being used.
@contextmanager
def _atomic_text_writer(path: Path | str, newline: str | None = None) -> Iterator[TextIO]:
    """Write a text file atomically by replacing the destination after a flush."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_fd, temp_name = tempfile.mkstemp(
        prefix=f"{path.name}.tmp-{os.getpid()}-",
        dir=path.parent,
        text=True,
    )
    os.close(temp_fd)
    temp_path = Path(temp_name)

    try:
        with open(temp_path, "w", encoding="utf-8", newline=newline) as f:
            yield f
            f.flush()
            os.fsync(f.fileno())
        temp_path.replace(path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
