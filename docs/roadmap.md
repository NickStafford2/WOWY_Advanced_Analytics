# Roadmap

## Current version

- Load game data from `games.csv`
- Compute a simple game-level WOWY score
- Filter players by minimum games with and without

## Planned next steps

- Add minutes played to the input data
- Build a minutes-weighted version of the model
- Replace simple WOWY with a regression-based player matrix
- Add ridge regression for more stable player estimates
- Compare results to RAPM-style models

## Not in scope yet

- possession-level data
- play-by-play parsing
- substitution-level lineup tracking
- complex production infrastructure

The current goal is to keep the project simple and understandable while building toward a more advanced model.
