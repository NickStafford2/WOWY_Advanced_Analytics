from __future__ import annotations

from rawr_analytics.sources.nba_api.download._models import SourceLeagueGame
from rawr_analytics.sources.nba_api.download._rules import format_source_row


def dedupe_schedule_games(games: list[SourceLeagueGame]) -> list[SourceLeagueGame]:
    deduped: list[SourceLeagueGame] = []
    seen_games_by_id: dict[str, SourceLeagueGame] = {}
    for game in games:
        existing_game = seen_games_by_id.get(game.game_id)
        if existing_game is not None:
            if (
                existing_game.game_date != game.game_date
                or existing_game.matchup != game.matchup
                or existing_game.team.team_id != game.team.team_id
                or existing_game.team.abbreviation(game_date=existing_game.game_date)
                != game.team.abbreviation(game_date=game.game_date)
            ):
                raise ValueError(
                    f"Conflicting duplicate schedule rows for game {game.game_id!r}; "
                    f"first_row={format_source_row(existing_game.raw_row)} "
                    f"second_row={format_source_row(game.raw_row)}"
                )
            continue
        deduped.append(game)
        seen_games_by_id[game.game_id] = game
    return deduped


__all__ = [
    "dedupe_schedule_games",
]
