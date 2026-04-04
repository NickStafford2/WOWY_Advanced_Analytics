from __future__ import annotations

from enum import Enum


class RawrShrinkageMode(str, Enum):
    UNIFORM = "uniform"
    GAME_COUNT = "game-count"
    MINUTES = "minutes"

    def __str__(self) -> str:
        return self.value

    @staticmethod
    def parse(value: RawrShrinkageMode | str) -> RawrShrinkageMode:
        if isinstance(value, RawrShrinkageMode):
            return value
        normalized = value.strip().lower()
        for mode in RawrShrinkageMode:
            if mode.value == normalized:
                return mode
        raise ValueError("Shrinkage mode must be 'uniform', 'game-count', or 'minutes'")

    @staticmethod
    def validate(
        shrinkage_mode: RawrShrinkageMode | str,
        shrinkage_strength: float,
        shrinkage_minute_scale: float,
    ) -> RawrShrinkageMode:
        mode = RawrShrinkageMode.parse(shrinkage_mode)
        if shrinkage_strength < 0:
            raise ValueError("Shrinkage strength must be non-negative")
        if shrinkage_minute_scale <= 0:
            raise ValueError("Shrinkage minute scale must be positive")
        return mode
