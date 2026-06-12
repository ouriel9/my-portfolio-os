import type { ReactNode } from 'react';
import { cn } from '@/lib/cn';

export interface KpiTileProps {
  label: string;
  value: ReactNode;
  delta?: ReactNode;
  deltaPositive?: boolean;
  accent?: 'indigo' | 'emerald' | 'rose' | 'amber' | 'cyan' | 'slate';
  hint?: string;
}

const ACCENT: Record<NonNullable<KpiTileProps['accent']>, string> = {
  indigo: 'from-indigo-500/15 to-indigo-500/0 dark:from-indigo-400/20',
  emerald: 'from-emerald-500/15 to-emerald-500/0 dark:from-emerald-400/20',
  rose: 'from-rose-500/15 to-rose-500/0 dark:from-rose-400/20',
  amber: 'from-amber-500/15 to-amber-500/0 dark:from-amber-400/20',
  cyan: 'from-cyan-500/15 to-cyan-500/0 dark:from-cyan-400/20',
  slate: 'from-slate-500/10 to-slate-500/0 dark:from-slate-400/10',
};

export function KpiTile({
  label,
  value,
  delta,
  deltaPositive,
  accent = 'indigo',
  hint,
}: KpiTileProps) {
  return (
    <div
      className={cn(
        'card card-hover relative overflow-hidden bg-gradient-to-br',
        ACCENT[accent],
      )}
    >
      <div className="kpi-label">{label}</div>
      <div className="mt-2 kpi-value mono-num" data-ltr>
        {value}
      </div>
      {delta !== undefined && (
        <div
          className={cn(
            'mt-1 text-xs font-medium tabular-nums',
            deltaPositive === undefined
              ? 'text-slate-500 dark:text-slate-400'
              : deltaPositive
                ? 'text-emerald-600 dark:text-emerald-400'
                : 'text-rose-600 dark:text-rose-400',
          )}
          data-ltr
        >
          {delta}
        </div>
      )}
      {hint && (
        <div className="mt-2 text-[11px] leading-snug text-slate-500 dark:text-slate-400">
          {hint}
        </div>
      )}
    </div>
  );
}
