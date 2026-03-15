# Roadmap
I do not plan to implement play by play tracking for quite some time. Most important is game data. The goal is for rough analysis for historical players, where there is not much data.

## Current version

- Load normalized game-level data from CSV
- Compute a simple game-level WOWY baseline
- Filter players by minimum games with and without
- Fit a separate game-level ridge regression model on normalized data
- Support local NBA team-season ingestion and CSV combination

## Current interpretation

- The current WOWY score is a baseline only
- It is useful for debugging the data pipeline and comparing simple outputs
- It is not expected to be a strong standalone player metric on pooled real NBA data

## Planned next steps

- Add clearer separation between pre-fit and post-fit qualification rules
- Add tuning workflows for ridge alpha and future pre-fit filters
- Improve evaluation of stability and sanity across parameter choices
- Compare results to RAPM-style models


- Add a separate regression CLI over combined normalized game-level inputs
- Use one observation per full game with both team perspectives represented
- Use minute-weighted player features
- Include team-season and opponent team-season context in the model

## Not in scope yet

- possession-level data
- play-by-play parsing
- substitution-level lineup tracking
- complex production infrastructure
