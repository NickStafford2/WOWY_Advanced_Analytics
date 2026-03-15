# WOWY

Python project for experimenting with historical basketball impact metrics.

## Goal

This project is a first step toward recreating a simple version of a "With or Without You" (WOWY) style plus-minus model.

The current version estimates a player's impact by comparing:

- the average point differential in games when the player played
- versus the average point differential in games when the player did not play

This is intentionally a simple starting point before moving to more advanced adjusted models like ridge regression or RAPM-style methods.

The current WOWY implementation should be treated as a baseline, not a final player evaluation model. On pooled real NBA data it is expected to be noisy and heavily confounded by team, season, and rotation context.

## Current model

Version 1 computes:

wowy_score = average margin when player played - average margin when player did not play

Where:
- `margin` is the final point differential from one team's perspective
- `players` is the set of NBA player ids who appeared in that game for that team

This is a game-level presence model, not a possession-level or substitution-level plus-minus model.

The normalized game CSV format is the stable data contract for this project. New ingestion or modeling work should continue to read and write this same game-level shape unless there is a deliberate format change.

## Input data

The program reads a CSV file named `games.csv`.

Expected columns:

- `game_id`
- `team`
- `margin`
- `players`

`players` should contain semicolon-separated NBA `PLAYER_ID` values.

Example:

```csv
game_id,team,margin,players
1,team_1,10,"1628369;1627759;1628401;201143;1629057"
2,team_1,6,"1628369;1627759;1628401;201143;203935"
```

## Real NBA data

The planned real-data path uses `nba_api` to fetch NBA game-level box score data and convert it into the same `games.csv` format above.

The WOWY model stays unchanged:

- one row per game from one team's perspective
- `margin` remains final game point differential
- `players` remains the semicolon-separated list of NBA `PLAYER_ID` values for that team

Generate a normalized team-season CSV with:

```bash
poetry run wowy-ingest-nba
```

This currently defaults to `BOS`, `2023-24`, and writes:

```text
data/raw/nba/team_games/BOS_2023-24.csv
```

You can override the defaults:

```bash
poetry run wowy-ingest-nba NYK 2022-23 --csv games.csv --season-type "Regular Season"
```

Combine local normalized CSVs into one analysis file with:

```bash
poetry run wowy-combine-games
```

This writes:

```text
data/combined/games.csv
```


## Install

Install dependencies with:

```bash
poetry install
```

## Tests

Run tests with:

```bash
poetry run pytest
```

## Example output

```text
WOWY results (Version 1)
------------------------------------------------------------------------
player_id     with  without     avg_with    avg_without      score
------------------------------------------------------------------------
1628369          4         2         9.00          1.00       8.00
```

## Next model direction

The next phase of the project is a regression-based player matrix built on the same normalized game-level data. The current WOWY score remains useful as a simple baseline and debugging reference, but future model development is expected to move away from direct with-or-without averages.
