import type { LeaderboardSeasonType } from './leaderboardTypes'

export const DEFAULT_SEASON_TYPES: LeaderboardSeasonType[] = ['REGULAR', 'PLAYOFFS']
const SEASON_TYPE_ORDER: LeaderboardSeasonType[] = ['REGULAR', 'PLAYOFFS', 'PRESEASON']

export function seasonSpan(
  startSeason: string,
  endSeason: string,
  seasons: string[],
): string[] {
  if (!startSeason || !endSeason) {
    return []
  }

  const startIndex = seasons.indexOf(startSeason)
  const endIndex = seasons.indexOf(endSeason)
  if (startIndex === -1 || endIndex === -1) {
    return []
  }

  const lowIndex = Math.min(startIndex, endIndex)
  const highIndex = Math.max(startIndex, endIndex)
  return seasons.slice(lowIndex, highIndex + 1)
}

export function buildSelectedSeasonIds(
  seasons: string[],
  seasonTypes: LeaderboardSeasonType[],
): string[] {
  const selectedSeasonTypes = seasonTypes.length > 0 ? seasonTypes : DEFAULT_SEASON_TYPES
  const seasonIds: string[] = []
  for (const season of seasons) {
    for (const seasonType of selectedSeasonTypes) {
      seasonIds.push(`${season}:${seasonType}`)
    }
  }
  return seasonIds
}

export function toggleLeaderboardSeasonType(
  selectedSeasonTypes: LeaderboardSeasonType[],
  seasonType: LeaderboardSeasonType,
): LeaderboardSeasonType[] {
  if (!selectedSeasonTypes.includes(seasonType)) {
    return _orderSeasonTypes([...selectedSeasonTypes, seasonType])
  }
  if (selectedSeasonTypes.length === 1) {
    return selectedSeasonTypes
  }
  return selectedSeasonTypes.filter((selectedSeasonType) => selectedSeasonType !== seasonType)
}

function _orderSeasonTypes(seasonTypes: LeaderboardSeasonType[]): LeaderboardSeasonType[] {
  return SEASON_TYPE_ORDER.filter((seasonType) => seasonTypes.includes(seasonType))
}
