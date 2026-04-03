from __future__ import annotations

import sys

from rawr_analytics.cli._metric_query_cli import (
    build_metric_query_parser,
    run_metric_query_cli,
)
from rawr_analytics.metrics.constants import Metric


def _build_parser():
    return build_metric_query_parser(
        description="Run the WOWY custom query used by the web app.",
        include_rawr_options=False,
    )


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return run_metric_query_cli(
        args,
        metric=Metric.WOWY,
        title="WOWY CLI",
        details=[
            "Running the same custom-query service path used by the web app.",
            "Output is a terminal leaderboard built from the shared service export rows.",
        ],
    )


def run(argv: list[str] | None = None) -> int:
    try:
        return main(argv)
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted. Shutting down cleanly.\n")
        sys.stderr.flush()
        return 130


if __name__ == "__main__":
    raise SystemExit(run())
