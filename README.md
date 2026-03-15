# WOWY

Python project for experimenting with historical basketball impact metrics.

## Goal

This project is a first step toward recreating a simple version of a "With or Without You" (WOWY) style plus-minus model.

The current version estimates a player's impact by comparing:

- the average point differential in games when the player played
- versus the average point differential in games when the player did not play

This is intentionally a simple starting point before moving to more advanced adjusted models like ridge regression or RAPM-style methods.

## Current model

Version 1 computes:

wowy_score = average margin when player played - average margin when player did not play

Where:
- `margin` is the final point differential from one team's perspective
- `players` is the set of players who appeared in that game for that team

This is a game-level presence model, not a possession-level or substitution-level plus-minus model.

## Input data

The program reads a CSV file named `games.csv`.

Expected columns:

- `game_id`
- `team`
- `margin`
- `players`

Example:

```csv
game_id,team,margin,players
1,team_1,10,"player_A;player_B;player_C;player_D;player_E"
2,team_1,6,"player_A;player_B;player_C;player_D;player_F"
```

## Real NBA data

The planned real-data path uses `nba_api` to fetch NBA game-level box score data and convert it into the same `games.csv` format above.

The WOWY model stays unchanged:

- one row per game from one team's perspective
- `margin` remains final game point differential
- `players` remains the semicolon-separated list of players who appeared for that team


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
player         with  without     avg_with    avg_without      score
------------------------------------------------------------------------
player_A          4         2         9.00          1.00       8.00
```
