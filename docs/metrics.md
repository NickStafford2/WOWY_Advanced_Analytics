# Models

The project currently has two analysis paths, and both are game-level only.

## WOWY

Input: derived WOWY game records in the app database, one row per game from one team's perspective.

Score:

`average margin when player played - average margin when player did not play`

Notes:

- uses whole-game participation only
- does not use substitutions or lineup stints
- does not use minutes in the score itself
- can use minutes for output filtering when running from cache-managed canonical inputs
- should be treated as a simple baseline

## RAWR

Input: validated canonical games and canonical game-player rows persisted in SQLite.

Model:

- game-level ridge regression
- player coefficients
- home-court term
- team-season terms
- opponent team-season terms

Notes:

- RAWR stands for Real Adjusted WOWY Regression
- player features are minute-weighted
- `min-games` is a pre-fit inclusion rule
- minute thresholds are output filters
- this is not RAPM or a possession-level model

Neither path currently models:

- play by play
- substitution-level lineups
- starting lineup structure
