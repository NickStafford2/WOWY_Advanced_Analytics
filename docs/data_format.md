# Data format

The repository has two game-level CSV contracts in active use:

- a canonical normalized layer for ingestion and regression modeling
- a derived WOWY compatibility layer for the existing WOWY CLI

## Canonical normalized games

`normalized_games.csv` should contain one row per game from one team's perspective.

### Columns

- `game_id`
- `season`
- `game_date`
- `team`
- `opponent`
- `is_home`
- `margin`
- `season_type`
- `source`

This file carries the game-level context needed by the current regression path, including opponent identity and home/away status.

## Canonical normalized game players

`normalized_game_players.csv` should contain one row per player appearance record for a team-game.

### Columns

- `game_id`
- `team`
- `player_id`
- `player_name`
- `appeared`
- `minutes`

`minutes` is not part of the current WOWY computation. It is used by the regression path to build minute-weighted player features and to support post-fit output qualification filters.

Rows with `appeared = false` are allowed in the normalized layer. The derived WOWY format should include only players with `appeared = true`.

## Derived WOWY compatibility format

`games.csv` should contain one row per game from one team's perspective.

## Columns

- `game_id`: unique game identifier
- `season`: season string
- `team`: team name or id
- `margin`: final point differential for that team
- `players`: semicolon-separated NBA `PLAYER_ID` values

## Example

```csv
game_id,season,team,margin,players
1,2023-24,team_1,10,"1628369;1627759;1628401;201143;1629057"
2,2023-24,team_1,-5,"1627759;1628401;201143;1629057;203935"
```
