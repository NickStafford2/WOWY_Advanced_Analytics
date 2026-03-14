# Data format

`games.csv` should contain one row per game from one team's perspective.

## Columns

- `game_id`: unique game identifier
- `team`: team name or id
- `margin`: final point differential for that team
- `players`: semicolon-separated player list

## Example

```csv
game_id,team,margin,players
1,team_1,10,"player_A;player_B;player_C;player_D;player_E"
2,team_1,-5,"player_B;player_C;player_D;player_E;player_F"
```
