from dataclasses import dataclass
from enum import Enum


class Metric(Enum):
    WOWY = "WOWY"
    WOWY_SHRUNK = "WOWY_SHRUNK"
    RAWR = "RAWR"


@dataclass(frozen=True)
class MetricSummary:
    metric: Metric
    label: str
    build_version: str

    @staticmethod
    def parse(value: str) -> Metric:
        metric_type = value.strip().lower()
        if metric_type in ["wowy", "with or without you"]:
            return Metric.WOWY
        if metric_type in [
            "wowy shrunk",
            "with or without you shrunk",
            "shrunk wowy",
            "shrunk with or without you",
        ]:
            return Metric.WOWY_SHRUNK
        if metric_type in ["rawr"]:
            return Metric.RAWR
        assert False, f"Invalid Metric type {value!r}."
