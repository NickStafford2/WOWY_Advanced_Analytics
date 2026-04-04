from __future__ import annotations

from datetime import datetime


def validate_required_text(value: str, label: str) -> None:
    if not value.strip():
        raise ValueError(f"{label} must not be empty")


def validate_optional_non_negative_int(value: int | None, label: str) -> None:
    if value is None:
        return
    if value < 0:
        raise ValueError(f"{label} must be non-negative")


def validate_optional_non_negative_float(value: float | None, label: str) -> None:
    if value is None:
        return
    if value < 0.0:
        raise ValueError(f"{label} must be non-negative")


def validate_iso_datetime(value: str, label: str) -> None:
    validate_required_text(value, label)
    datetime.fromisoformat(value)
