from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.shared.common import LogFn
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.ingest import IngestProgress, IngestUpdateFn
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team
from rawr_analytics.sources.kaggle_nba._dataset import download_dataset_snapshot


@dataclass(frozen=True)
class KaggleNbaTeamSeasonData:
    games: list[NormalizedGameRecord]
    game_players: list[NormalizedGamePlayerRecord]
    total_games: int
    fetched_box_scores: int
    cached_box_scores: int
    league_games_source: str


def ingest_team_season(
    *,
    team: Team,
    season: Season,
    log_fn: LogFn | None = print,
    update_fn: IngestUpdateFn | None = None,
) -> KaggleNbaTeamSeasonData:
    dataset = download_dataset_snapshot()
    if log_fn is not None:
        log_fn(
            f"kaggle_nba dataset={dataset.dataset_handle} "
            f"kaggle_cache={dataset.kaggle_cache_path} "
            f"local_root={dataset.local_root_path} "
            f"csv_files={len(dataset.csv_paths)}"
        )
    if update_fn is not None:
        update_fn(
            IngestProgress(
                team=team,
                season=season,
                current=0,
                total=0,
                status="dataset-downloaded",
            )
        )
    raise ValueError(
        "Kaggle ingest source is downloaded and selectable, but the CSV-to-normalized-game "
        f"mapping is not implemented yet for {team.abbreviation(season=season)} {season}. "
        "Next step: inspect the downloaded CSV headers and map schedule rows plus player box "
        "score rows into the normalized ingest contract."
    )


__all__ = ["KaggleNbaTeamSeasonData", "ingest_team_season"]
