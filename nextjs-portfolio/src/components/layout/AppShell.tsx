'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { useState, useEffect, useCallback } from 'react';
import { useApp } from '@/components/providers/AppProvider';
import { cn } from '@/lib/cn';
import type { Locale } from '@/lib/types';

interface ConnectionStatus {
  configured: boolean;
  urlPreview: string;
  hasToken: boolean;
}

const NAV_ITEMS = [
  { href: '/dashboard', icon: '🏠', heLabel: 'דשבורד', enLabel: 'Dashboard', accent: 'indigo' },
  { href: '/trades', icon: '📁', heLabel: 'ניהול עסקאות', enLabel: 'Trade Management', accent: 'emerald' },
  { href: '/risk', icon: '🛡', heLabel: 'סיכונים ופיפו', enLabel: 'Risk & FIFO', accent: 'rose' },
  { href: '/simulator', icon: '🧮', heLabel: 'סימולטור', enLabel: 'Simulator', accent: 'amber' },
  { href: '/data', icon: '📊', heLabel: 'בקרת נתונים', enLabel: 'Data Quality', accent: 'cyan' },
] as const;

const ACTIVE_STYLES: Record<string, string> = {
  indigo:
    'bg-indigo-50 text-indigo-700 shadow-soft dark:bg-indigo-500/20 dark:text-indigo-200',
  emerald:
    'bg-emerald-50 text-emerald-700 shadow-soft dark:bg-emerald-500/20 dark:text-emerald-200',
  rose: 'bg-rose-50 text-rose-700 shadow-soft dark:bg-rose-500/20 dark:text-rose-200',
  amber:
    'bg-amber-50 text-amber-700 shadow-soft dark:bg-amber-500/20 dark:text-amber-200',
  cyan: 'bg-cyan-50 text-cyan-700 shadow-soft dark:bg-cyan-500/20 dark:text-cyan-200',
};

function navLabel(item: (typeof NAV_ITEMS)[number], locale: Locale) {
  return locale === 'he' ? item.heLabel : item.enLabel;
}

interface SidebarProps {
  locale: Locale;
  theme: string;
  setTheme: (t: 'light' | 'dark') => void;
  setLocale: (l: Locale) => void;
  chartLock: boolean;
  setChartLock: (v: boolean) => void;
  demoMode: boolean;
  setDemoMode: (v: boolean) => void;
  pathname: string | null;
  onClose?: () => void;
}

function SidebarContent({
  locale,
  theme,
  setTheme,
  setLocale,
  chartLock,
  setChartLock,
  demoMode,
  setDemoMode,
  pathname,
  onClose,
}: SidebarProps) {
  const [syncOpen, setSyncOpen] = useState(false);
  const [connOpen, setConnOpen] = useState(false);
  const [connStatus, setConnStatus] = useState<ConnectionStatus | null>(null);
  const [connTesting, setConnTesting] = useState(false);
  const [connError, setConnError] = useState('');

  const fetchStatus = useCallback(async () => {
    try {
      const res = await fetch('/api/connection-status');
      const data = (await res.json()) as ConnectionStatus;
      setConnStatus(data);
    } catch {
      setConnStatus({ configured: false, urlPreview: '', hasToken: false });
    }
  }, []);

  const testConnection = useCallback(async () => {
    setConnTesting(true);
    setConnError('');
    try {
      const res = await fetch('/api/snapshot');
      const json = (await res.json()) as { ok: boolean; error?: string };
      if (!json.ok) setConnError(json.error ?? 'Connection failed');
    } catch (e) {
      setConnError(e instanceof Error ? e.message : 'Connection failed');
    } finally {
      setConnTesting(false);
    }
  }, []);

  useEffect(() => {
    if (syncOpen || connOpen) fetchStatus();
  }, [syncOpen, connOpen, fetchStatus]);

  const isHe = locale === 'he';
  const labelClass = 'text-[11px] font-semibold uppercase tracking-wider text-slate-400 dark:text-slate-500 mb-1';

  return (
    <div className="flex h-full flex-col overflow-y-auto bg-white dark:bg-slate-950 border-e border-slate-200 dark:border-slate-800">
      {/* Logo */}
      <div className="flex items-center gap-2.5 px-4 py-4 border-b border-slate-100 dark:border-slate-800">
        <div className="grid h-9 w-9 shrink-0 place-items-center rounded-xl bg-gradient-to-br from-indigo-500 to-purple-600 shadow-soft">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2.2" strokeLinecap="round">
            <rect x="3" y="3" width="7" height="7" rx="1.5" />
            <rect x="14" y="3" width="7" height="7" rx="1.5" />
            <rect x="3" y="14" width="7" height="7" rx="1.5" />
            <rect x="14" y="14" width="7" height="7" rx="1.5" />
          </svg>
        </div>
        <div className="min-w-0 flex-1">
          <div className="truncate text-sm font-bold text-slate-900 dark:text-white">
            {isHe ? 'מערכת ניהול תיק' : 'Portfolio OS'}
          </div>
          <div className="truncate text-[10px] text-slate-400">
            {isHe ? 'ניהול השקעות חכם' : 'Smart investment management'}
          </div>
        </div>
        {onClose && (
          <button
            onClick={onClose}
            className="ms-auto text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 lg:hidden"
            aria-label="Close menu"
          >
            ✕
          </button>
        )}
      </div>

      {/* Controls */}
      <div className="space-y-3 px-3 py-4 border-b border-slate-100 dark:border-slate-800">
        {/* Language */}
        <div>
          <div className={labelClass}>{isHe ? 'שפה' : 'Language'}</div>
          <select
            value={locale}
            onChange={(e) => setLocale(e.target.value as Locale)}
            className="w-full rounded-lg border border-slate-200 bg-slate-50 px-2 py-1.5 text-xs text-slate-700 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 focus:outline-none"
          >
            <option value="he">עברית</option>
            <option value="en">English</option>
          </select>
        </div>

        {/* Appearance */}
        <div>
          <div className={labelClass}>{isHe ? 'תצוגה' : 'Appearance'}</div>
          <select
            value={theme}
            onChange={(e) => setTheme(e.target.value as 'light' | 'dark')}
            className="w-full rounded-lg border border-slate-200 bg-slate-50 px-2 py-1.5 text-xs text-slate-700 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 focus:outline-none"
          >
            <option value="light">{isHe ? 'בהיר' : 'Light'}</option>
            <option value="dark">{isHe ? 'כהה' : 'Dark'}</option>
          </select>
        </div>

        {/* Checkboxes */}
        <label className="flex cursor-pointer items-center gap-2 text-xs text-slate-600 dark:text-slate-300 select-none">
          <input
            type="checkbox"
            checked={chartLock}
            onChange={(e) => setChartLock(e.target.checked)}
            className="rounded"
          />
          🔒 {isHe ? 'נעילת תרשימים' : 'Lock Charts'}
        </label>
        <label className="flex cursor-pointer items-center gap-2 text-xs text-slate-600 dark:text-slate-300 select-none">
          <input
            type="checkbox"
            checked={demoMode}
            onChange={(e) => setDemoMode(e.target.checked)}
            className="rounded"
          />
          {isHe ? 'מצב הדגמה' : 'Demo Mode'}
        </label>
      </div>

      {/* Navigation */}
      <div className="flex-1 px-2 py-4">
        <div className="mb-2 px-2 text-[11px] font-semibold uppercase tracking-wider text-slate-400 dark:text-slate-500">
          {isHe ? 'ניווט' : 'Navigation'}
        </div>
        <nav className="space-y-0.5">
          {NAV_ITEMS.map((item) => {
            const active = pathname?.startsWith(item.href);
            return (
              <Link
                key={item.href}
                href={item.href}
                onClick={onClose}
                className={cn(
                  'flex items-center gap-3 rounded-xl px-3 py-2.5 text-sm font-medium transition-all',
                  active
                    ? ACTIVE_STYLES[item.accent]
                    : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900 dark:text-slate-300 dark:hover:bg-slate-800 dark:hover:text-white',
                )}
              >
                <span className="text-base leading-none">{item.icon}</span>
                <span>{navLabel(item, locale)}</span>
              </Link>
            );
          })}
        </nav>
      </div>

      {/* Collapsible panels */}
      <div className="border-t border-slate-100 dark:border-slate-800 px-2 py-3 space-y-1">
        {/* Google Sheets sync */}
        <button
          onClick={() => setSyncOpen((v) => !v)}
          className="flex w-full items-center gap-2 rounded-xl px-3 py-2 text-xs font-medium text-slate-500 hover:bg-slate-100 dark:text-slate-400 dark:hover:bg-slate-800 transition-colors"
        >
          <span>☁</span>
          <span className="flex-1 text-start">
            {isHe ? 'סנכרון עם גוגל שיט' : 'Google Sheets Sync'}
          </span>
          <span className={cn('transition-transform', syncOpen && 'rotate-90')}>›</span>
        </button>
        {syncOpen && (
          <div className="mx-1 mb-1 rounded-lg border border-slate-100 bg-slate-50 px-3 py-3 dark:border-slate-800 dark:bg-slate-900/60 space-y-2">
            {/* Status dot */}
            <div className="flex items-center gap-2">
              <span className={cn(
                'h-2 w-2 rounded-full shrink-0',
                connStatus === null ? 'bg-slate-300 dark:bg-slate-600' :
                connStatus.configured ? 'bg-emerald-500' : 'bg-rose-500'
              )} />
              <span className="text-xs text-slate-600 dark:text-slate-300">
                {connStatus === null
                  ? (isHe ? 'בודק…' : 'Checking…')
                  : connStatus.configured
                    ? (isHe ? 'מחובר לגוגל שיט' : 'Connected to Google Sheets')
                    : (isHe ? 'לא מוגדר' : 'Not configured')}
              </span>
            </div>
            {connStatus?.urlPreview && (
              <div className="truncate font-mono text-[10px] text-slate-400 dark:text-slate-500">
                {connStatus.urlPreview}
              </div>
            )}
            {!connStatus?.configured && (
              <p className="text-[10px] leading-snug text-slate-400 dark:text-slate-500">
                {isHe
                  ? 'הגדר APPS_SCRIPT_URL ו-APPS_SCRIPT_TOKEN בקובץ .env.local'
                  : 'Set APPS_SCRIPT_URL and APPS_SCRIPT_TOKEN in .env.local'}
              </p>
            )}
            <button
              onClick={testConnection}
              disabled={connTesting}
              className="h-7 rounded-lg bg-indigo-600 px-3 text-[10px] font-semibold text-white hover:bg-indigo-700 disabled:opacity-60 transition-colors"
            >
              {connTesting
                ? (isHe ? 'בודק…' : 'Testing…')
                : (isHe ? 'בדוק חיבור' : 'Test Connection')}
            </button>
            {connError && (
              <p className="text-[10px] text-rose-500">{connError}</p>
            )}
            {!connTesting && !connError && connStatus?.configured && (
              <p className="text-[10px] text-emerald-600 dark:text-emerald-400">
                {isHe ? '✅ חיבור תקין' : '✅ Connection OK'}
              </p>
            )}
          </div>
        )}

        {/* Connection settings */}
        <button
          onClick={() => setConnOpen((v) => !v)}
          className="flex w-full items-center gap-2 rounded-xl px-3 py-2 text-xs font-medium text-slate-500 hover:bg-slate-100 dark:text-slate-400 dark:hover:bg-slate-800 transition-colors"
        >
          <span>⚙</span>
          <span className="flex-1 text-start">
            {isHe ? 'הגדרות חיבור ונתונים' : 'Connection Settings'}
          </span>
          <span className={cn('transition-transform', connOpen && 'rotate-90')}>›</span>
        </button>
        {connOpen && (
          <div className="mx-1 mb-1 rounded-lg border border-slate-100 bg-slate-50 px-3 py-3 dark:border-slate-800 dark:bg-slate-900/60 space-y-2 text-[10px] text-slate-500 dark:text-slate-400">
            <div className="flex items-center gap-2">
              <span className={cn(
                'h-2 w-2 rounded-full shrink-0',
                connStatus?.configured ? 'bg-emerald-500' : 'bg-rose-500 animate-pulse'
              )} />
              <span className="font-medium">
                {isHe ? 'Apps Script Web App' : 'Apps Script Web App'}
              </span>
            </div>
            {connStatus?.urlPreview ? (
              <div className="truncate font-mono">{connStatus.urlPreview}</div>
            ) : (
              <div className="text-rose-400">
                {isHe ? 'URL לא מוגדר — ערוך .env.local' : 'URL not set — edit .env.local'}
              </div>
            )}
            <div className="flex items-center gap-1.5">
              <span className={connStatus?.hasToken ? 'text-emerald-500' : 'text-rose-400'}>
                {connStatus?.hasToken ? '🔑' : '⚠'}
              </span>
              <span>{isHe ? 'טוקן API' : 'API token'}: {connStatus?.hasToken ? (isHe ? 'מוגדר' : 'set') : (isHe ? 'חסר' : 'missing')}</span>
            </div>
            <p className="leading-snug text-slate-400 dark:text-slate-500">
              {isHe
                ? 'שנה את פרטי החיבור בקובץ nextjs-portfolio/.env.local ואתחל את השרת.'
                : 'Edit nextjs-portfolio/.env.local and restart the server to change connection details.'}
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

export function AppShell({ children }: { children: React.ReactNode }) {
  const { locale, setLocale, theme, setTheme, chartLock, setChartLock, demoMode, setDemoMode } =
    useApp();
  const pathname = usePathname();
  const [mobileOpen, setMobileOpen] = useState(false);

  const sidebarProps: SidebarProps = {
    locale,
    theme,
    setTheme,
    setLocale,
    chartLock,
    setChartLock,
    demoMode,
    setDemoMode,
    pathname,
  };

  return (
    // dir="ltr" keeps sidebar on left even when html dir="rtl" for Hebrew
    <div dir="ltr" className="flex min-h-screen">
      {/* Desktop sidebar (always visible) */}
      <aside className="hidden lg:flex lg:flex-col w-64 shrink-0 fixed inset-y-0 start-0 z-30">
        <SidebarContent {...sidebarProps} />
      </aside>

      {/* Mobile sidebar overlay */}
      {mobileOpen && (
        <div className="fixed inset-0 z-50 lg:hidden">
          <div
            className="absolute inset-0 bg-black/50 backdrop-blur-sm"
            onClick={() => setMobileOpen(false)}
          />
          <aside className="absolute start-0 top-0 h-full w-64 shadow-deep">
            <SidebarContent {...sidebarProps} onClose={() => setMobileOpen(false)} />
          </aside>
        </div>
      )}

      {/* Main content — LTR layout; Hebrew text auto-aligns via BiDi algorithm.
          min-w-0 is critical: without it this flex item's min-width:auto lets
          nowrap children (the tab bar) force it wider than the mobile viewport,
          clipping KPI tiles off the screen edge. */}
      <div className="flex min-h-screen min-w-0 flex-1 flex-col lg:ms-64">
        {/* Mobile top bar */}
        <header className="sticky top-0 z-20 flex items-center gap-3 border-b border-slate-200 bg-white/90 px-4 py-3 backdrop-blur-md dark:border-slate-800 dark:bg-slate-950/90 lg:hidden">
          <button
            onClick={() => setMobileOpen(true)}
            className="grid h-9 w-9 place-items-center rounded-xl text-slate-600 hover:bg-slate-100 dark:text-slate-300 dark:hover:bg-slate-800"
            aria-label="Open menu"
          >
            ☰
          </button>
          <div className="flex items-center gap-2">
            <div className="grid h-7 w-7 place-items-center rounded-lg bg-gradient-to-br from-indigo-500 to-purple-600">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2.5" strokeLinecap="round">
                <rect x="3" y="3" width="7" height="7" rx="1.5" />
                <rect x="14" y="3" width="7" height="7" rx="1.5" />
                <rect x="3" y="14" width="7" height="7" rx="1.5" />
                <rect x="14" y="14" width="7" height="7" rx="1.5" />
              </svg>
            </div>
            <span className="text-sm font-bold text-slate-900 dark:text-white">
              {locale === 'he' ? 'מערכת ניהול תיק' : 'Portfolio OS'}
            </span>
          </div>
        </header>

        {/* Demo-mode banner: the data on screen is synthetic, writes disabled. */}
        {demoMode && (
          <div className="sticky top-0 z-10 border-b border-amber-300/60 bg-amber-50 px-4 py-1.5 text-center text-xs font-semibold text-amber-800 dark:border-amber-500/30 dark:bg-amber-500/10 dark:text-amber-300">
            {locale === 'he'
              ? '🧪 מצב הדגמה — הנתונים סינתטיים, עריכת עסקאות מושבתת'
              : '🧪 Demo mode — synthetic data, trade editing disabled'}
          </div>
        )}

        <main className="flex-1 px-4 py-5 sm:px-6 lg:px-8">{children}</main>

        <footer className="border-t border-slate-200/70 px-4 py-3 text-center text-xs text-slate-400 dark:border-slate-800/70 dark:text-slate-500">
          Portfolio OS · {new Date().getFullYear()}
        </footer>
      </div>
    </div>
  );
}
