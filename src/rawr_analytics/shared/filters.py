from __future__ import annotations


def validate_top_n_and_minutes(
    top_n: int | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> None:
    if top_n is not None and top_n < 0:
        raise ValueError("Top-n filter must be non-negative")
    if min_average_minutes is not None and min_average_minutes < 0:
        raise ValueError("Minimum average minutes filter must be non-negative")
    if min_total_minutes is not None and min_total_minutes < 0:
        raise ValueError("Minimum total minutes filter must be non-negative")
