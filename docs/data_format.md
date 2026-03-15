# Data format

The repository now has two game-level CSV contracts:

- a canonical normalized layer for ingestion and future modeling
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

## Canonical normalized game players

`normalized_game_players.csv` should contain one row per player appearance record for a team-game.

### Columns

- `game_id`
- `team`
- `player_id`
- `player_name`
- `appeared`
- `minutes`

`minutes` is included for future use but is not part of the current WOWY computation.

## Derived WOWY compatibility format

`games.csv` should contain one row per game from one team's perspective.

## Columns

- `game_id`: unique game identifier
- `team`: team name or id
- `margin`: final point differential for that team
- `players`: semicolon-separated NBA `PLAYER_ID` values

## Example

```csv
game_id,team,margin,players
1,team_1,10,"1628369;1627759;1628401;201143;1629057"
2,team_1,-5,"1627759;1628401;201143;1629057;203935"
```
