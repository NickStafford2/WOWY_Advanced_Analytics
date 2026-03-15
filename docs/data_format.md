# Data format

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
