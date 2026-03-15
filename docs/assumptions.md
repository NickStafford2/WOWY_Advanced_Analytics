# Assumptions

This project is currently a simple game-level WOWY prototype.

## Current assumptions

- Each row in `games.csv` represents one game from one team's perspective.
- `margin` is the final point differential for that team.
- `players` is the set of players who appeared in that game.
- The current model only uses whole-game participation.
- The current model does not use minutes played.
- The current model does not use starting lineup data.
- The current model does not use in-game substitutions or lineup stints.
- The current model is not RAPM and not a possession-level plus-minus model.

## Phase 1 normalized-data assumptions

- The canonical normalized layer is still game-level.
- Opponent context should be recorded in normalized game data even though the current WOWY path does not use it.
- `minutes` may exist in normalized player rows but is not used yet.
- The current WOWY CSV remains a derived compatibility format, not the long-term canonical source.

## Current interpretation

The current WOWY score is:

`average margin when player played - average margin when player did not play`

This should be treated as a simple prototype metric, not a final adjusted player rating.
