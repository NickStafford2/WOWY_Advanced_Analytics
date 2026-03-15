# Roadmap
I do not plan to implement play by play tracking for quite some time. Most important is game data. The goal is for rough analysis for historical players, where there is not much data.

## Current version

- Load normalized game-level data from CSV
- Compute a simple game-level WOWY baseline
- Support WOWY output filtering by games and minutes
- Fit a separate game-level ridge regression model on normalized data
- Use minute-weighted player features plus home-court, team-season, and opponent team-season terms
- Support ridge tuning from the regression CLI
- Support local NBA team-season ingestion, cache status reporting, and CSV combination

## Current interpretation

- The current WOWY score is a baseline only
- It is useful for debugging the data pipeline and comparing simple outputs
- It is not expected to be a strong standalone player metric on pooled real NBA data

## Planned next steps

- Add clearer separation between pre-fit and post-fit qualification rules
- Improve cache fetch behavior so missing data is fetched eagerly even for broader cached-scope flows
- Improve evaluation of stability and sanity across parameter choices
- Compare results to RAPM-style models

## Not in scope yet

- possession-level data
- play-by-play parsing
- substitution-level lineup tracking
- complex production infrastructure
