import type { ReactNode } from 'react';
import { cn } from '@/lib/cn';

export interface PageHeaderProps {
  title: string;
  subtitle?: string;
  accent?: 'indigo' | 'emerald' | 'rose' | 'amber' | 'cyan';
  actions?: ReactNode;
}

const ACCENT_BAR: Record<NonNullable<PageHeaderProps['accent']>, string> = {
  indigo: 'from-indigo-500 to-indigo-300',
  emerald: 'from-emerald-500 to-emerald-300',
  rose: 'from-rose-500 to-rose-300',
  amber: 'from-amber-500 to-amber-300',
  cyan: 'from-cyan-500 to-cyan-300',
};

const ACCENT_TITLE: Record<NonNullable<PageHeaderProps['accent']>, string> = {
  indigo:
    'bg-gradient-to-r from-indigo-600 to-indigo-400 dark:from-indigo-300 dark:to-indigo-100 bg-clip-text text-transparent',
  emerald:
    'bg-gradient-to-r from-emerald-600 to-emerald-400 dark:from-emerald-300 dark:to-emerald-100 bg-clip-text text-transparent',
  rose: 'bg-gradient-to-r from-rose-600 to-rose-400 dark:from-rose-300 dark:to-rose-100 bg-clip-text text-transparent',
  amber:
    'bg-gradient-to-r from-amber-600 to-amber-400 dark:from-amber-300 dark:to-amber-100 bg-clip-text text-transparent',
  cyan: 'bg-gradient-to-r from-cyan-600 to-cyan-400 dark:from-cyan-300 dark:to-cyan-100 bg-clip-text text-transparent',
};

export function PageHeader({ title, subtitle, accent = 'indigo', actions }: PageHeaderProps) {
  return (
    <div className="mb-5 flex flex-wrap items-end justify-between gap-3">
      <div>
        <div className={cn('mb-2 h-1 w-12 rounded-full bg-gradient-to-r', ACCENT_BAR[accent])} />
        <h1 className={cn('text-2xl font-bold tracking-tight', ACCENT_TITLE[accent])}>{title}</h1>
        {subtitle && <p className="mt-1 text-sm text-slate-500 dark:text-slate-400">{subtitle}</p>}
      </div>
      {actions && <div className="flex items-center gap-2">{actions}</div>}
    </div>
  );
}
