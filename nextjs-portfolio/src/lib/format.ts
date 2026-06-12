/** Locale-aware formatters used across the UI. */
import type { Locale } from './types';

const LOCALES: Record<Locale, string> = { he: 'he-IL', en: 'en-US' };

export function formatCurrency(
  value: number,
  currency: 'ILS' | 'USD' = 'ILS',
  locale: Locale = 'he',
  digits = 0,
): string {
  if (!Number.isFinite(value)) return '—';
  return new Intl.NumberFormat(LOCALES[locale], {
    style: 'currency',
    currency,
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(value);
}

export function formatPercent(value: number, locale: Locale = 'he', digits = 2): string {
  if (!Number.isFinite(value)) return '—';
  return new Intl.NumberFormat(LOCALES[locale], {
    style: 'percent',
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(value);
}

export function formatNumber(value: number, locale: Locale = 'he', digits = 2): string {
  if (!Number.isFinite(value)) return '—';
  return new Intl.NumberFormat(LOCALES[locale], {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(value);
}

export function formatDate(value: string, locale: Locale = 'he'): string {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(LOCALES[locale], {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
  }).format(date);
}

export function compactNumber(value: number, locale: Locale = 'he'): string {
  if (!Number.isFinite(value)) return '—';
  return new Intl.NumberFormat(LOCALES[locale], {
    notation: 'compact',
    maximumFractionDigits: 1,
  }).format(value);
}
