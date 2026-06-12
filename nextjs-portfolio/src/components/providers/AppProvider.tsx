'use client';

import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { SWRConfig } from 'swr';
import type { State } from 'swr';
import type { Locale } from '@/lib/types';
import { dict } from '@/lib/i18n';
import type { TranslateKey } from '@/lib/i18n';

/**
 * Persistent SWR cache: snapshot data survives page reloads in localStorage,
 * so the app opens INSTANTLY with the last-known data and revalidates in the
 * background — no blocking Google-Sheet round-trip on every open.
 */
function localStorageProvider(): Map<string, State<unknown, unknown>> {
  if (typeof window === 'undefined') return new Map();
  let entries: [string, State<unknown, unknown>][] = [];
  try {
    entries = JSON.parse(
      window.localStorage.getItem('pp-swr-cache') ?? '[]',
    ) as [string, State<unknown, unknown>][];
  } catch {
    entries = [];
  }
  const map = new Map<string, State<unknown, unknown>>(entries);
  window.addEventListener('beforeunload', () => {
    try {
      window.localStorage.setItem('pp-swr-cache', JSON.stringify(Array.from(map.entries())));
    } catch {
      /* storage full/blocked — cache is best-effort */
    }
  });
  return map;
}

type Theme = 'light' | 'dark';

interface AppContextValue {
  locale: Locale;
  theme: Theme;
  setLocale: (locale: Locale) => void;
  setTheme: (theme: Theme) => void;
  toggleLocale: () => void;
  toggleTheme: () => void;
  t: (key: TranslateKey) => string;
  paletteOpen: boolean;
  setPaletteOpen: (open: boolean) => void;
  chartLock: boolean;
  setChartLock: (v: boolean) => void;
  demoMode: boolean;
  setDemoMode: (v: boolean) => void;
}

const AppContext = createContext<AppContextValue | null>(null);

const LOCALE_KEY = 'portfolio-os.locale';
const THEME_KEY = 'portfolio-os.theme';

export function AppProvider({ children }: { children: ReactNode }) {
  const [locale, setLocaleState] = useState<Locale>('he');
  const [theme, setThemeState] = useState<Theme>('dark');
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [chartLock, setChartLockState] = useState(false);
  const [demoMode, setDemoModeState] = useState(false);

  // Hydrate persisted state on mount.
  useEffect(() => {
    try {
      const storedLocale = window.localStorage.getItem(LOCALE_KEY) as Locale | null;
      const storedTheme = window.localStorage.getItem(THEME_KEY) as Theme | null;
      if (storedLocale === 'he' || storedLocale === 'en') setLocaleState(storedLocale);
      if (storedTheme === 'light' || storedTheme === 'dark') setThemeState(storedTheme);
      else if (window.matchMedia('(prefers-color-scheme: dark)').matches) setThemeState('dark');
    } catch {
      // ignore — fallback to defaults
    }
  }, []);

  // Sync to <html> attributes whenever they change.
  useEffect(() => {
    const root = document.documentElement;
    root.classList.toggle('dark', theme === 'dark');
    root.lang = locale;
    root.dir = locale === 'he' ? 'rtl' : 'ltr';
    try {
      window.localStorage.setItem(LOCALE_KEY, locale);
      window.localStorage.setItem(THEME_KEY, theme);
    } catch {
      // ignore
    }
  }, [locale, theme]);

  // Demo mode — toggle CSS class that blurs financial values in the UI.
  useEffect(() => {
    document.documentElement.classList.toggle('demo-mode', demoMode);
  }, [demoMode]);

  // Global keyboard: ⌘K / Ctrl+K opens command palette; "g d/t/r/s/q" jumps.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const isMod = e.metaKey || e.ctrlKey;
      if (isMod && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        setPaletteOpen((p) => !p);
      }
      if (e.key === 'Escape') setPaletteOpen(false);
    }
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  const setLocale = useCallback((l: Locale) => setLocaleState(l), []);
  const setTheme = useCallback((tt: Theme) => setThemeState(tt), []);
  const setChartLock = useCallback((v: boolean) => setChartLockState(v), []);
  const setDemoMode = useCallback((v: boolean) => setDemoModeState(v), []);
  const toggleLocale = useCallback(() => setLocaleState((l) => (l === 'he' ? 'en' : 'he')), []);
  const toggleTheme = useCallback(
    () => setThemeState((tt) => (tt === 'dark' ? 'light' : 'dark')),
    [],
  );

  const value = useMemo<AppContextValue>(() => {
    const table = dict(locale);
    return {
      locale,
      theme,
      setLocale,
      setTheme,
      toggleLocale,
      toggleTheme,
      paletteOpen,
      setPaletteOpen,
      chartLock,
      setChartLock,
      demoMode,
      setDemoMode,
      t: (key: TranslateKey) => table[key] ?? String(key),
    };
  }, [locale, theme, setLocale, setTheme, toggleLocale, toggleTheme, paletteOpen, chartLock, setChartLock, demoMode, setDemoMode]);

  return (
    <AppContext.Provider value={value}>
      <SWRConfig value={{ provider: localStorageProvider, revalidateOnMount: true }}>
        {children}
      </SWRConfig>
    </AppContext.Provider>
  );
}

export function useApp(): AppContextValue {
  const ctx = useContext(AppContext);
  if (!ctx) throw new Error('useApp must be used inside <AppProvider>');
  return ctx;
}
