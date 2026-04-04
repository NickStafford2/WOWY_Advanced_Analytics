from dataclasses import dataclass
from enum import Enum


class Metric(Enum):
    WOWY = "wowy"
    WOWY_SHRUNK = "wowy_shrunk"
    RAWR = "rawr"

    @classmethod
    def parse(cls, value: str) -> "Metric":
        metric_type = value.strip().lower()
        if metric_type in ["wowy", "with or without you"]:
            return cls.WOWY
        if metric_type in [
            "wowy_shrunk",
            "wowy shrunk",
            "with or without you shrunk",
            "shrunk wowy",
            "shrunk with or without you",
        ]:
            return cls.WOWY_SHRUNK
        if metric_type in ["rawr"]:
            return cls.RAWR
        raise ValueError(f"Invalid metric type {value!r}.")


@dataclass(frozen=True)
class MetricSummary:
    metric: Metric
    build_version: str
