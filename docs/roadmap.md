# Roadmap
I do not plan to implement play by play tracking for quite some time. Most important is game data. The goal is for rough analysis for historical players, where there is not much data.

## Current version

- Two analysis paths: WOWY and regression
- Game-level data only
- Local NBA team-season ingestion and cache tooling
- Regression uses minute-weighted player features and ridge tuning

## Current interpretation

- WOWY is a baseline only
- Regression is the main path for more adjusted analysis
- Neither path is meant to be a finished possession-level metric

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
