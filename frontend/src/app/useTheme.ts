import { useEffect, useState } from 'react'
import type { ThemeMode } from './types'

const THEME_STORAGE_KEY = 'wowy-theme'

type UseThemeValue = {
  theme: ThemeMode
  toggleTheme: () => void
}

export function useTheme(): UseThemeValue {
  const [theme, setTheme] = useState<ThemeMode>(_resolveInitialTheme)

  useEffect(() => {
    document.documentElement.dataset.theme = theme
    window.localStorage.setItem(THEME_STORAGE_KEY, theme)
  }, [theme])

  function toggleTheme(): void {
    setTheme((current) => (current === 'light' ? 'dark' : 'light'))
  }

  return {
    theme,
    toggleTheme,
  }
}

function _resolveInitialTheme(): ThemeMode {
  const storedTheme = window.localStorage.getItem(THEME_STORAGE_KEY)
  if (storedTheme === 'light' || storedTheme === 'dark') {
    return storedTheme
  }

  return 'light'
}
