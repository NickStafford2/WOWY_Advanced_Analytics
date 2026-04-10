export type LeaderboardSeasonType = 'REGULAR' | 'PLAYOFFS' | 'PRESEASON'

export type LeaderboardFilters = {
  startSeason: string
  endSeason: string
  seasonTypes: LeaderboardSeasonType[]
  teamIds: number[] | null
  topN: number
  minGames: number
  ridgeAlpha: number
  minGamesWith: number
  minGamesWithout: number
  minAverageMinutes: number
  minTotalMinutes: number
}

export type LeaderboardNumberField =
  | 'topN'
  | 'minGames'
  | 'ridgeAlpha'
  | 'minGamesWith'
  | 'minGamesWithout'
  | 'minAverageMinutes'
  | 'minTotalMinutes'
