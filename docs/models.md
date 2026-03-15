# Models

The project currently has two analysis paths.

## WOWY

Input:

- derived `games.csv`
- one row per game from one team's perspective

Score:

`average margin when player played - average margin when player did not play`

Notes:

- uses whole-game participation only
- does not use substitutions or lineup stints
- does not use minutes in the score itself
- can use minutes for output filtering when running from cache-managed normalized inputs
- should be treated as a simple baseline

## Regression

Input:

- normalized games
- normalized game-player rows

Model:

- game-level ridge regression
- player coefficients
- home-court term
- team-season terms
- opponent team-season terms

Notes:

- player features are minute-weighted
- `min-games` is a pre-fit inclusion rule
- minute thresholds are output filters
- this is not RAPM or a possession-level model

## Shared scope

Both paths are game-level only.

Neither path currently models:

- play by play
- substitution-level lineups
- starting lineup structure
