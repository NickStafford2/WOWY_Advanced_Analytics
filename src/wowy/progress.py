from __future__ import annotations

import sys
from dataclasses import dataclass
from textwrap import wrap
from typing import TextIO


@dataclass
class TerminalProgressBar:
    label: str
    total: int
    width: int = 32
    stream: TextIO | None = None

    def __post_init__(self) -> None:
        self.total = max(1, self.total)
        if self.stream is None:
            self.stream = sys.stderr
        self._last_line: str | None = None

    def update(self, current: int, detail: str | None = None) -> None:
        assert self.stream is not None
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
        assert self.stream is not None
        self.update(self.total, detail=detail)
        self.stream.write("\n")
        self.stream.flush()
        self._last_line = None


def print_status_box(
    title: str,
    lines: list[str],
    *,
    width: int = 78,
    stream: TextIO | None = None,
) -> None:
    if stream is None:
        stream = sys.stderr
    assert stream is not None
    content_width = max(24, width - 4)
    wrapped_lines: list[str] = []
    for line in lines:
        wrapped_lines.extend(wrap(line, width=content_width) or [""])

    box_width = min(
        width,
        max(
            len(title) + 4,
            *(len(line) + 4 for line in wrapped_lines),
        ),
    )
    inner_width = box_width - 4

    stream.write("+" + "-" * (box_width - 2) + "+\n")
    stream.write(f"| {title[:inner_width].ljust(inner_width)} |\n")
    stream.write("|" + "-" * (box_width - 2) + "|\n")
    for line in wrapped_lines:
        stream.write(f"| {line[:inner_width].ljust(inner_width)} |\n")
    stream.write("+" + "-" * (box_width - 2) + "+\n")
    stream.flush()
