# Missing Player Stats

This list covers player-level data that is not currently present as explicit fields in the
`eoinamoore` source under `data/source/eoinamoore/`.

## Missing and likely needed from elsewhere

### Awards and accolades

- MVP awards
- Finals MVP awards
- Defensive Player of the Year awards
- Rookie of the Year awards
- Most Improved Player awards
- Sixth Man of the Year awards
- All-NBA selections
- All-Defensive selections
- All-Star selections
- championship count / champion-by-season flag

### Common season-value metrics

- PER
- Win Shares
- Offensive Win Shares
- Defensive Win Shares
- Win Shares per 48
- BPM
- OBPM
- DBPM
- VORP

### Other convenience metadata that may still be useful

- Finals appearances
- conference finals appearances
- scoring-title / rebound-title / assist-title flags
- Hall of Fame flag

## Probably do not need an external source for these

These are not explicit columns in the Kaggle files, but they can be derived from the game-level
player logs you already have.

- PPG
- RPG
- APG
- SPG
- BPG
- FG%
- 3P%
- FT%
- games played
- starts, if the source has enough lineup context elsewhere
- regular season / playoff splits for basic box stats
- season and career totals for basic box stats

## Current Kaggle source already includes some advanced game-level stats

The current source already has explicit game-level fields such as:

- offensive rating
- defensive rating
- net rating
- pace
- PIE
- true shooting percentage
- usage percentage
- assist percentage
- rebound percentages

Those should be aggregated or modeled locally instead of fetched from another source unless you
specifically want a third-party canonical version.
