# Assumptions

This project currently contains:

- A game-level WOWY baseline
- A separate game-level regression path on normalized data

## Current assumptions

- Each row in `games.csv` represents one game from one team's perspective.
- `margin` is the final point differential for that team.
- `players` is the set of players who appeared in that game.
- The WOWY baseline only uses whole-game participation.
- The WOWY baseline does not use minutes played.
- The current model does not use starting lineup data.
- The current model does not use in-game substitutions or lineup stints.
- The current model is not RAPM and not a possession-level plus-minus model.

## Phase 1 normalized-data assumptions

- The canonical normalized layer is still game-level.
- Opponent context is recorded in normalized game data.
- `minutes` is used by the regression path for player weighting and output qualification filters.
- The WOWY path still does not use `minutes`.
- The current WOWY CSV remains a derived compatibility format, not the long-term canonical source.

## Current interpretations

The current WOWY score is:

`average margin when player played - average margin when player did not play`

This should be treated as a simple prototype metric, not a final adjusted player rating.

The current regression path is a game-level ridge model with player, home-court, team-season, and opponent team-season terms.
