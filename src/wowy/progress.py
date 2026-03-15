from __future__ import annotations

import sys
from dataclasses import dataclass


@dataclass
class TerminalProgressBar:
    label: str
    total: int
    width: int = 32
    stream = sys.stderr

    def __post_init__(self) -> None:
        self.total = max(1, self.total)
        self._last_line: str | None = None

    def update(self, current: int, detail: str | None = None) -> None:
        current = min(max(current, 0), self.total)
        filled = int((current / self.total) * self.width)
        bar = "#" * filled + "-" * (self.width - filled)
        percent = int((current / self.total) * 100)
        suffix = f" {detail}" if detail else ""
        line = f"\r{self.label} [{bar}] {percent:>3}% ({current}/{self.total}){suffix}"
        if line == self._last_line:
            return
        self.stream.write(line)
        self.stream.flush()
        self._last_line = line

    def finish(self, detail: str | None = None) -> None:
        self.update(self.total, detail=detail)
        self.stream.write("\n")
        self.stream.flush()
        self._last_line = None
