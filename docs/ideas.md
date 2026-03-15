# Ideas

Suggestions for improving regression accuracy while staying at the game level and avoiding play-by-play data.

## Highest-value upgrades

- Add time decay across seasons.
  Recent games should count more than older games in multi-year fits.

- Use player-season coefficients instead of one pooled coefficient per player.
  This avoids blending together very different versions of the same player across years.

- Tune ridge and pre-fit rules systematically.
  Use rolling validation and stability checks instead of relying only on inspection.

- Add sample-size-aware shrinkage.
  Low-minute and low-game players should be shrunk more aggressively than heavily observed players.

- Report uncertainty and stability, not just point estimates.
  Bootstrap samples or rolling windows can show whether top coefficients are robust.

## Other useful improvements

- Weight observations by information quality.
  Some games are less informative because of extreme rotations, rest patterns, or unusual minute distributions.

- Separate regular season and playoffs.
  Pooling them together can blur estimates because they are different environments.

- Keep improving context controls.
  Team-season and opponent team-season effects are already useful; future extensions should preserve that structure.

## Suggested implementation order

1. Player-season coefficients.
2. Time decay across seasons.
3. Better tuning workflow for ridge and inclusion rules.
4. Sample-size-aware shrinkage.
5. Bootstrap and rolling-window stability analysis.

## Key principle

The biggest likely improvement is moving from:
- one coefficient per player across all seasons
to:
- one coefficient per player-season
That should help more than continuing to pool more years into a single player effect without temporal structure.
