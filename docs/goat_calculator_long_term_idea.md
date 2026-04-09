# Summary 

The GOAT calculator is a customizable ranking engine, not a fixed leaderboard.

The user is supposed to decide what “greatness” means to them by assigning importance to broad categories like:

* winning
* sustained excellence / longevity
* peak
* accolades
* impact metrics
* traditional stats
* optional subjective areas like intangibles or era context

Inside each category, there are multiple contributing stats. Each player has raw values for those stats. The calculator normalizes those stats, combines them into a category score, then combines the category scores into one final GOAT score.

So the model is basically:

1. take raw player metrics
2. scale them onto a comparable range
3. combine them into category scores
4. weight those category scores based on user preferences
5. output a final GOAT score and rank

That means the final answer changes depending on the user’s basketball philosophy. A rings-focused user, a peak-focused user, and an impact-metrics user should all be able to get different rankings from the same underlying player database.

I wrote this up as a Markdown document you can use as an implementation reference for AI agents:

[goat_calculator_scoring_model.md](sandbox:/mnt/data/goat_calculator_scoring_model.md)

One important design note: for the public website, I’d recommend separating:

* category weight, which controls how much a user cares about winning vs peak vs impact
* metric weight within category, which controls how a category is built internally

That is a little cleaner than the spreadsheet behavior and will make the site easier to understand.

Next, I’d turn this into a more technical build spec with:

* JSON schema for categories and metrics
* scoring formulas
* API shape
* frontend component plan

# Stats Needed.

I pulled the metric names from the Results sheet and grouped them by the category headers above them. I ignored the Intangibles / Bonus Points section and the Era Competition / League Talent Delusion section, just like you asked.

I also excluded the helper columns like Rank, Points, and %, since those are result columns rather than source metrics.

Winning
- Conference Finals Appearances
- Finals Appearances
- Championships Won
- Winning/Year

Sustained Excellence
- Thinking Basketball Backpicks
- Backpicks Approximate Calculation
- Games Played

Peak
- Thinking Basketball Greatest Peaks Since 77
- Jonny Arnett Peaks
- Greatest Peaks (me) Extrapolation from TB’s 8yr
- Thinking Basketball 8yr Peak Rankings
- Prime WOWYR
- Peak WS/48
- P Peak WS/48
- P WS/48 Improvement

Analyst Rankings / GroupThink
- Hollenger GOAT Pts (2022)
- Bill Simmons Pyramid
- Bruin Sports Analytics Expert Average
- Johnny Arnett GOAT List

Accolades
- MVP Shares
- MVP Shares / GP
- Finals MVP
- All NBA Team Points
- All NBA Team Points / GP
- All Star
- All Star / GP
- Time to enter HOF

Impact on Team Success
- Prime WOWYR
- Career WOWYR
- Scaled WOWYR
- Alt Scaled WOWYR (>20 min played only)
- 10-yr Scaled GPM
- RAPM

Dumber Stats
- Defensive Win Shares
- DWS/GP
- Offensive Win Shares
- OWS/GP
- Win Shares
- WS/GP
- WS/48
- P WS/48
- P WS/48 Improvement
- WS/100 poss (estimate)
- TS%

Outside NBA
- College Years
- College Championships
- Olympic Points

A couple quick notes for turning this into a portfolio feature:
- You have some repeated concepts across sections, especially Prime WOWYR and the playoff/peak WS/48 family.
- This would translate well into two public-facing tools:
  1. GOAT calculator, where users can adjust weights
  2. Metric explorer/calculator, where each metric is defined, sourced, and visualized

The strongest portfolio version is probably one site with two tabs: GOAT Calculator and Metric Lab. That feels more cohesive than splitting it into two separate sites.

I can next turn this into a cleaner implementation plan for your existing analytics project, including which metrics are easiest to port first and how to structure the database/API.

I pulled the metric names from the Results sheet and grouped them by the category headers above them. I ignored the Intangibles / Bonus Points section and the Era Competition / League Talent Delusion section, just like you asked.

I also excluded the helper columns like Rank, Points, and %, since those are result columns rather than source metrics.

Winning
- Conference Finals Appearances
- Finals Appearances
- Championships Won
- Winning/Year

Sustained Excellence
- Thinking Basketball Backpicks
- Backpicks Approximate Calculation
- Games Played

Peak
- Thinking Basketball Greatest Peaks Since 77
- Jonny Arnett Peaks
- Greatest Peaks (me) Extrapolation from TB’s 8yr
- Thinking Basketball 8yr Peak Rankings
- Prime WOWYR
- Peak WS/48
- P Peak WS/48
- P WS/48 Improvement

Analyst Rankings / GroupThink
- Hollenger GOAT Pts (2022)
- Bill Simmons Pyramid
- Bruin Sports Analytics Expert Average
- Johnny Arnett GOAT List

Accolades
- MVP Shares
- MVP Shares / GP
- Finals MVP
- All NBA Team Points
- All NBA Team Points / GP
- All Star
- All Star / GP
- Time to enter HOF

Impact on Team Success
- Prime WOWYR
- Career WOWYR
- Scaled WOWYR
- Alt Scaled WOWYR (>20 min played only)
- 10-yr Scaled GPM
- RAPM

Dumber Stats
- Defensive Win Shares
- DWS/GP
- Offensive Win Shares
- OWS/GP
- Win Shares
- WS/GP
- WS/48
- P WS/48
- P WS/48 Improvement
- WS/100 poss (estimate)
- TS%

Outside NBA
- College Years
- College Championships
- Olympic Points

A couple quick notes for turning this into a portfolio feature:
- You have some repeated concepts across sections, especially Prime WOWYR and the playoff/peak WS/48 family.
- This would translate well into two public-facing tools:
  1. GOAT calculator, where users can adjust weights
  2. Metric explorer/calculator, where each metric is defined, sourced, and visualized




# GOAT Calculator Scoring Model

## Purpose

This document explains how the existing spreadsheet-based NBA GOAT calculator appears to work, based on the formulas in the `Results` sheet. It is written so an AI agent or engineer can use it to recreate the feature in a website or analytics application.

The core idea is:

- A GOAT score is not a fixed truth.
- It is a weighted expression of what a given user values.
- Users should be able to decide how much they care about categories like winning, peak, longevity, advanced impact, accolades, and similar concepts.
- The calculator should then convert those preferences into a numerical GOAT score and ranking.

In other words, the model is a customizable value system, not a claim of a single objective answer.

---

## High-level interpretation

The spreadsheet uses a hierarchical weighting model:

1. Each player has a set of raw metrics.
2. Each raw metric is normalized against the player pool.
3. Normalized metrics are combined into category scores.
4. Category scores are normalized again.
5. Normalized category scores are combined into one final GOAT score.
6. Players are ranked by that final score.

This means the calculator works in two layers:

- Layer 1: metrics within a category
- Layer 2: categories within the final GOAT score

This is a good design for a website because it matches how fans actually think:

- first they care about broad ideas like winning, peak, impact, accolades, longevity
- then each of those ideas is represented by a bundle of more specific stats

---

## What the workbook is doing structurally

On the `Results` sheet:

- Row 1 contains category names and each category's share of the total score.
- Row 2 contains metric names.
- Row 3 contains user-entered weights for metrics and category totals.
- Rows 4+ contain player data and formulas.

The workbook computes:

- a score for each category
- a normalized percentage for each category
- a final overall point total
- a final normalized overall percentage
- an overall rank

---

## Category model

The workbook groups metrics into categories such as:

- Winning
- Sustained Excellence
- Peak
- Analyst Rankings / GroupThink
- Accolades
- Impact on Team Success
- Dumber Stats
- Outside NBA
- Intangibles / Bonus Points
- Era Competition / League Talent Delusion

For the website version, categories should be treated as the main user-facing controls.

A user should be able to say things like:

- Winning matters a lot to me.
- Peak matters more than longevity.
- I care a lot about impact metrics.
- I do not care much about analyst lists.
- I want to ignore intangibles.
- I want to ignore era adjustments.

That is the core product concept.

---

## Metric scoring logic

Within each category, every metric follows the same basic pattern.

### Step 1: store a raw value for each player

Each player has a raw value for the metric, such as:

- championships won
- finals appearances
- MVP shares
- RAPM
- WOWYR
- TS%

### Step 2: normalize that metric across all players

For most metrics, the sheet converts the raw value into a normalized score using min-max normalization:

```text
normalized_metric = (player_metric - min_metric) / (max_metric - min_metric)
```

This maps the metric onto a 0 to 1 scale relative to the player pool in the workbook.

Interpretation:

- 0 means the lowest value in the dataset
- 1 means the highest value in the dataset
- everyone else falls somewhere in between

This is important because it lets very different stat types coexist in one model.

Examples:

- championships and TS% can both be compared after normalization
- RAPM and All-Star selections can both contribute without being on incompatible raw scales

### Step 3: multiply each normalized metric by its metric weight

Each metric has a user-defined weight.

Conceptually:

```text
weighted_metric_contribution = normalized_metric * metric_weight
```

Then the weighted contributions are added together within the category.

---

## Category score logic

Each category score is the sum of its weighted metric contributions.

Conceptually:

```text
raw_category_score = sum(normalized_metric_i * metric_weight_i)
```

Example for a simplified Winning category:

```text
winning_score =
    weight_cf * normalized_conference_finals
  + weight_finals * normalized_finals
  + weight_titles * normalized_titles
  + weight_win_per_year * normalized_win_per_year
```

The workbook then normalizes the category score across all players again.

Conceptually:

```text
normalized_category_score =
    (raw_category_score - min_category_score) / (max_category_score - min_category_score)
```

This means each category becomes a 0 to 1 score before being used in the overall GOAT score.

That second normalization matters because categories may have very different internal metric counts and weight totals.

---

## Category weight logic

Each category has a category-level weight total.

In the current sheet, that category total is effectively the sum of the metric weights inside the category.

Conceptually:

```text
category_weight = sum(metric_weights_in_category)
```

The workbook also shows each category as a fraction of the total weight budget.

Conceptually:

```text
category_share = category_weight / sum(all_category_weights)
```

This tells you how much the current scoring model values each broad area.

For the website, there are two reasonable ways to implement this:

### Option A: preserve the spreadsheet behavior exactly

- Metric weights are editable.
- Category weights are derived automatically as the sum of metric weights.
- Changing a metric weight changes both:
  - the metric's influence inside its category
  - the category's influence on the final score

This matches the workbook.

### Option B: separate metric weighting and category weighting

- Users set category weights directly.
- Users optionally set metric weights inside each category.
- Category weight controls the overall importance of the category.
- Metric weights only control how players are judged within that category.

This is cleaner for a public product.

Recommendation: for a portfolio-quality website, use Option B. It is easier for users to understand and gives better UX.

---

## Final GOAT score logic

The workbook combines normalized category scores into a final total.

Conceptually:

```text
goat_score = sum(normalized_category_score_j * category_weight_j)
```

Then it produces:

- a final point total
- a final normalized percentage relative to the player pool
- a final rank

The workbook's overall normalization is again min-max based:

```text
normalized_goat_score =
    (goat_score - min_goat_score) / (max_goat_score - min_goat_score)
```

Final rank is then based on the overall point total.

---

## Plain English summary of the model

The calculator is trying to answer this question:

> Given the specific things a user cares about, which player scores best on those priorities?

It does that by:

1. taking many player metrics
2. converting them to a common scale
3. combining them into idea-level categories
4. weighting those categories according to the user's values
5. producing a final GOAT score and rank

So the GOAT score is not just raw stats.
It is a weighted philosophical profile.

---

## Suggested product definition for the website

## Product concept

Build an interactive GOAT calculator where the user defines their basketball philosophy and the site outputs a ranking.

### Core user flow

1. User opens the GOAT calculator.
2. User chooses how much they care about each category.
3. User can optionally expand a category and adjust the component metrics.
4. User can choose to include or exclude controversial categories such as intangibles or era adjustments.
5. The site recalculates rankings instantly.
6. The site shows:
   - top players by GOAT score
   - category breakdown for each player
   - why one player beats another under the current weighting system

### Good UI framing

Use language like:

- Build your GOAT formula
- Weight what matters to you
- Compare basketball philosophies
- See how rankings change when you value peak vs longevity

This makes the feature feel thoughtful rather than gimmicky.

---

## Recommended implementation rules for agents

### Data model

Represent the system using three layers:

#### 1. players

Each player has:

- id
- name
- seasons
- metadata as needed

#### 2. metrics

Each metric should have:

- id
- label
- category_id
- raw_value_by_player
- normalization_direction
- description
- source
- enabled_by_default

`normalization_direction` exists in case a future metric is negative when larger, though most current metrics appear to reward higher values.

#### 3. categories

Each category should have:

- id
- label
- description
- default_weight
- optional_flag
- metrics[]

Optional categories should include things like:

- intangibles
- era adjustments

---

## Recommended scoring engine

### Exact spreadsheet-style engine

```text
for each metric:
    normalized_metric[player] = min_max(metric.raw_value[player])

for each category:
    raw_category_score[player] = sum(
        normalized_metric[player, metric] * metric_weight[metric]
        for metric in category.metrics
    )
    normalized_category_score[player] = min_max(raw_category_score[player])

final_goat_score[player] = sum(
    normalized_category_score[player, category] * category_weight[category]
    for category in enabled_categories
)

final_rank = descending_rank(final_goat_score)
```

### Better public-product engine

```text
for each metric:
    normalized_metric[player] = min_max(metric.raw_value[player])

for each category:
    internal_metric_weights = normalize_user_metric_weights_within_category()
    category_score[player] = weighted_average(
        normalized_metric[player, metric],
        internal_metric_weights
    )

final_goat_score[player] = weighted_average(
    category_score[player, category],
    user_category_weights
)

final_rank = descending_rank(final_goat_score)
```

The second version is easier to explain because users can think of category weights and metric weights separately.

---

## Why this model is good for a portfolio project

This is stronger than a static leaderboard because it shows:

- data modeling
- metric design
- normalization logic
- customizable ranking systems
- front-end interactivity
- explainable analytics
- product thinking

It is especially good if the site lets users:

- save presets
- share ranking configurations
- compare preset philosophies

Examples:

- Rings Culture preset
- Peak Purist preset
- Longevity preset
- Impact Metrics preset
- Old School preset
- Analytics Nerd preset

That makes the project feel like a real product, not just a spreadsheet port.

---

## Important implementation cautions

### 1. Normalization is dataset-dependent

Because min-max normalization depends on the player pool, scores can shift when:

- new players are added
- metrics are changed
- outliers are introduced

Agents should make this behavior explicit in the implementation.

### 2. Some categories are more subjective than others

Intangibles and era adjustments are not purely statistical. They should be treated as optional and clearly labeled.

### 3. Duplicative metrics can overweight a concept

Some categories contain related stats that may partially overlap. For example, different impact stats or related win share stats can all reward similar player traits.

The website should document that overlap rather than pretending every input is independent.

### 4. Metric provenance should be documented

Each metric should have:

- a definition
- a source or derivation note
- whether it is directly imported or custom-derived

This is especially important because the spreadsheet mixes:

- direct historical counts
- advanced metrics
- values derived from outside analysts
- custom transformations

---

## Suggested MVP feature set

### GOAT calculator MVP

- category sliders
- optional metric-level advanced controls
- player ranking table
- player detail page with category breakdown
- include/exclude optional categories
- reset to default weights
- shareable URL with current settings

### Nice second-phase features

- preset philosophies
- side-by-side player comparison
- radar chart by category
- explanation panel showing exactly why rankings changed
- metric glossary
- sensitivity analysis showing which categories most affect rank

---

## One-sentence product summary

The GOAT calculator is a customizable weighted ranking engine that lets users define what they value in basketball greatness and then ranks players according to that philosophy.

---

## Practical recommendation

If this is being merged into an existing NBA analytics site, the best structure is:

- `GOAT Calculator` as one main feature
- `Metric Lab` or `Metric Explorer` as a second feature

That way:

- the GOAT calculator is the opinionated ranking product
- the metric lab is the transparent analytics/reference layer underneath it

This gives the project both personality and credibility.
