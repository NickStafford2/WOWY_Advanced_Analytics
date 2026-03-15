from __future__ import annotations

from wowy.regression_types import RegressionResult


def format_regression_results(
    result: RegressionResult,
    top_n: int | None = None,
) -> str:
    ranked = sorted(
        result.estimates,
        key=lambda estimate: estimate.coefficient,
        reverse=True,
    )
    if top_n is not None:
        ranked = ranked[:top_n]

    name_width = max(len("player"), *(len(estimate.player_name) for estimate in ranked))
    player_id_width = max(
        len("player_id"),
        *(len(str(estimate.player_id)) for estimate in ranked),
    )

    lines = [
        "Regression results (Game-level player model)",
        (
            f"observations={result.observations} players={result.players} "
            f"intercept={result.intercept:.4f} home_court={result.home_court_advantage:.4f}"
        ),
        "-" * (name_width + player_id_width + 24),
        f"{'player':<{name_width}} {'player_id':<{player_id_width}} {'games':>6} {'coef':>10}",
        "-" * (name_width + player_id_width + 24),
    ]

    for estimate in ranked:
        lines.append(
            f"{estimate.player_name:<{name_width}} "
            f"{estimate.player_id:<{player_id_width}} "
            f"{estimate.games:>6} "
            f"{estimate.coefficient:>10.4f}"
        )

    return "\n".join(lines)
