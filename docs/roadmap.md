# Roadmap
I do not plan to implement play by play tracking for quite some time. Most important is game data. The goal is for rough analysis for historical players, where there is not much data.

## Current version

- Load normalized game-level data from CSV
- Compute a simple game-level WOWY baseline
- Filter players by minimum games with and without
- Support local NBA team-season ingestion and CSV combination

## Current interpretation

- The current WOWY score is a baseline only
- It is useful for debugging the data pipeline and comparing simple outputs
- It is not expected to be a strong standalone player metric on pooled real NBA data

## Planned next steps

- Build a regression-based player matrix on normalized game-level data
- Add ridge regression for more stable player estimates
- Decide whether player identity should move from player name to stable player id
- Compare results to RAPM-style models

## Phase 1 status

- Keep the existing WOWY CLI and derived `games.csv` path intact
- Add canonical normalized game and game-player schemas
- Record opponent context in the normalized layer
- Reserve `minutes` in the normalized player schema without using it yet
- Add a derivation step from normalized tables to WOWY `games.csv`

## Phase 2 status

- Make NBA ingestion write canonical normalized team-season CSVs directly
- Continue deriving the existing WOWY team-season CSV from those normalized outputs
- Keep the current WOWY CLI unchanged while shifting ingestion toward the normalized layer
- Combine normalized team-season files into regression-ready multi-team inputs

## Phase 3 status

- Add a separate regression CLI over combined normalized game-level inputs
- Use one observation per team-game
- Use player appearance indicators only for the first model
- Keep opponent context in the observation data even though the first coefficients are player-only

## Not in scope yet

- possession-level data
- play-by-play parsing
- substitution-level lineup tracking
- complex production infrastructure

The current goal is to keep the data pipeline simple and understandable while moving the modeling work from raw WOWY toward regression.
