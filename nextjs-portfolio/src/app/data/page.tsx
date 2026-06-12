'use client';

import { useMemo } from 'react';
import { useApp } from '@/components/providers/AppProvider';
import { useSnapshot } from '@/components/hooks/useSnapshot';
import { KpiTile } from '@/components/ui/KpiTile';
import { PageHeader } from '@/components/ui/PageHeader';
import { LoadingBlock, ErrorBlock, EmptyBlock } from '@/components/ui/StateBanners';
import { formatCurrency, formatNumber } from '@/lib/format';
import type { Trade } from '@/lib/types';
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Cell,
  PieChart,
  Pie,
  Legend,
} from 'recharts';

// Fields to audit for completeness
const AUDIT_FIELDS: { key: keyof Trade; label: string; heLabel: string }[] = [
  { key: 'tradeId', label: 'Trade ID', heLabel: 'Trade ID' },
  { key: 'ticker', label: 'Ticker', heLabel: 'טיקר' },
  { key: 'platform', label: 'Platform', heLabel: 'פלטפורמה' },
  { key: 'type', label: 'Asset Type', heLabel: 'סוג נכס' },
  { key: 'currentLocation', label: 'Location', heLabel: 'מיקום' },
  { key: 'purchaseDate', label: 'Purchase Date', heLabel: 'תאריך רכישה' },
  { key: 'quantity', label: 'Quantity', heLabel: 'כמות' },
  { key: 'buyPrice', label: 'Buy Price', heLabel: 'שער קנייה' },
  { key: 'costOrigin', label: 'Origin Cost', heLabel: 'עלות מקור' },
  { key: 'currency', label: 'Currency', heLabel: 'מטבע' },
  { key: 'status', label: 'Status', heLabel: 'סטטוס' },
];

function computeStats(trades: Trade[], isHe: boolean) {
  const total = trades.length;
  if (total === 0) return null;

  // Trade-level issues
  const seenIds = new Set<string>();
  let dupes = 0;
  let missingTicker = 0;
  let missingQty = 0;
  let missingDate = 0;

  for (const r of trades) {
    if (r.tradeId) {
      if (seenIds.has(r.tradeId)) dupes += 1;
      else seenIds.add(r.tradeId);
    }
    if (!r.ticker) missingTicker += 1;
    if (!r.quantity) missingQty += 1;
    if (!r.purchaseDate) missingDate += 1;
  }

  // Overall completeness across all audit fields
  let totalFilled = 0;
  const totalPossible = total * AUDIT_FIELDS.length;
  for (const { key } of AUDIT_FIELDS) {
    for (const t of trades) {
      const v = t[key];
      if (typeof v === 'number' ? v !== 0 : Boolean(v)) totalFilled += 1;
    }
  }
  const ratio = totalPossible === 0 ? 0 : totalFilled / totalPossible;

  // Per-column completeness for chart
  const colData = AUDIT_FIELDS.map(({ key, label, heLabel }) => {
    let filled = 0;
    for (const t of trades) {
      const v = t[key];
      if (typeof v === 'number' ? v !== 0 : Boolean(v)) filled += 1;
    }
    const pct = total === 0 ? 0 : filled / total;
    return { label: isHe ? heLabel : label, pct, pctDisplay: `${(pct * 100).toFixed(0)}%` };
  }).sort((a, b) => a.pct - b.pct);

  // Status distribution
  const statusMap: Record<string, number> = {};
  for (const t of trades) {
    const s = t.status || (isHe ? 'לא ידוע' : 'Unknown');
    statusMap[s] = (statusMap[s] ?? 0) + 1;
  }
  const statusData = Object.entries(statusMap).map(([name, value]) => ({ name, value }));

  return { total, dupes, missingTicker, missingQty, missingDate, ratio, colData, statusData };
}

const STATUS_COLORS = ['#10b981', '#94a3b8', '#f59e0b', '#ef4444'];

export default function DataQualityPage() {
  const { t, locale } = useApp();
  const isHe = locale === 'he';
  const { views, error, isLoading } = useSnapshot();

  const stats = useMemo(
    () => (views ? computeStats(views.trades, isHe) : null),
    [views, isHe],
  );

  if (isLoading) return <LoadingBlock label={t('loading')} />;
  if (error) return <ErrorBlock message={error} />;
  if (!views || !stats) return <EmptyBlock message={t('no_data')} />;

  const issues: string[] = [];
  if (stats.dupes > 0)
    issues.push(
      isHe
        ? `${stats.dupes} רשומות עם Trade_ID כפול — מומלץ למזג בגיליון המקור.`
        : `${stats.dupes} duplicate Trade_ID rows — deduplicate in the source sheet.`,
    );
  if (stats.missingTicker > 0)
    issues.push(
      isHe
        ? `${stats.missingTicker} שורות ללא טיקר — יש להשלים או להסיר.`
        : `${stats.missingTicker} rows with empty Ticker — fill or remove.`,
    );
  if (stats.missingQty > 0)
    issues.push(
      isHe
        ? `${stats.missingQty} שורות עם כמות 0 — יש לוודא או לארכב.`
        : `${stats.missingQty} rows with zero quantity — verify or archive.`,
    );
  if (stats.missingDate > 0)
    issues.push(
      isHe
        ? `${stats.missingDate} שורות עם תאריך רכישה לא תקין — יש לתקן פורמט.`
        : `${stats.missingDate} rows with invalid purchase date — fix date formats.`,
    );
  if (stats.ratio < 0.95)
    issues.push(
      isHe
        ? `שלמות כוללת: ${(stats.ratio * 100).toFixed(1)}% — יעד מומלץ: 95% ומעלה.`
        : `Overall completeness is ${(stats.ratio * 100).toFixed(1)}% — target ≥ 95%.`,
    );

  return (
    <div className="space-y-5">
      <PageHeader title={t('nav_quality')} accent="cyan" />

      {/* KPI tiles */}
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <KpiTile
          label={isHe ? 'שלמות נתונים' : 'Data Completeness'}
          value={`${(stats.ratio * 100).toFixed(1)}%`}
          accent={stats.ratio >= 0.95 ? 'cyan' : 'amber'}
        />
        <KpiTile
          label={isHe ? 'שורות בתמונת מצב' : 'Rows in Snapshot'}
          value={String(stats.total)}
          accent="cyan"
        />
        <KpiTile
          label={isHe ? 'כפילויות Trade_ID' : 'Duplicate Trade IDs'}
          value={String(stats.dupes)}
          accent={stats.dupes > 0 ? 'rose' : 'emerald'}
        />
        <KpiTile
          label={isHe ? 'טיקרים חסרים' : 'Missing Tickers'}
          value={String(stats.missingTicker)}
          accent={stats.missingTicker > 0 ? 'rose' : 'emerald'}
        />
        <KpiTile
          label={isHe ? 'כמות 0' : 'Zero Quantity'}
          value={String(stats.missingQty)}
          accent={stats.missingQty > 0 ? 'amber' : 'emerald'}
        />
        <KpiTile
          label={isHe ? 'תאריך חסר' : 'Missing Date'}
          value={String(stats.missingDate)}
          accent={stats.missingDate > 0 ? 'amber' : 'emerald'}
        />
        <KpiTile
          label={t('open_positions')}
          value={String(views.openTrades.length)}
          accent="cyan"
        />
        <KpiTile
          label={t('closed_positions')}
          value={String(views.closedTrades.length)}
          accent="cyan"
        />
      </div>

      {/* Column-level completeness chart */}
      <section className="card">
        <h2 className="mb-4 text-sm font-bold text-slate-700 dark:text-slate-200">
          {isHe ? 'שלמות נתונים ברמת עמודה' : 'Column-Level Completeness'}
        </h2>
        <div style={{ height: Math.max(220, stats.colData.length * 32 + 40) }} className="w-full">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={stats.colData}
              layout="vertical"
              margin={{ top: 0, right: 40, left: 10, bottom: 0 }}
            >
              <XAxis
                type="number"
                domain={[0, 1]}
                tickFormatter={(v: number) => `${(v * 100).toFixed(0)}%`}
                tick={{ fontSize: 11 }}
              />
              <YAxis
                type="category"
                dataKey="label"
                width={100}
                tick={{ fontSize: 11 }}
              />
              <Tooltip
                formatter={(v: number) => `${(v * 100).toFixed(1)}%`}
                contentStyle={{ fontSize: 12, borderRadius: 8 }}
              />
              <Bar dataKey="pct" radius={[0, 4, 4, 0]} label={{ position: 'right', fontSize: 10, formatter: (v: number) => `${(v * 100).toFixed(0)}%` }}>
                {stats.colData.map((entry, i) => (
                  <Cell
                    key={i}
                    fill={
                      entry.pct >= 0.95
                        ? '#10b981'
                        : entry.pct >= 0.8
                          ? '#f59e0b'
                          : '#ef4444'
                    }
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </section>

      {/* Issues panel */}
      {issues.length === 0 ? (
        <div className="rounded-xl bg-emerald-50 px-4 py-3 text-sm font-medium text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300">
          ✅ {isHe ? 'לא נמצאו בעיות איכות נתונים.' : 'No data-quality issues detected.'}
        </div>
      ) : (
        <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 dark:border-amber-800/50 dark:bg-amber-900/20">
          <div className="mb-2 text-sm font-semibold text-amber-800 dark:text-amber-200">
            ⚠ {isHe ? 'ממצאי איכות נתונים והמלצות תיקון' : 'Data quality issues & suggested fixes'}
          </div>
          <ul className="space-y-1">
            {issues.map((issue, i) => (
              <li key={i} className="text-sm text-amber-700 dark:text-amber-300">
                • {issue}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Status distribution */}
      {stats.statusData.length > 0 && (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          {/* Status table */}
          <section className="card">
            <h2 className="mb-3 text-sm font-bold text-slate-700 dark:text-slate-200">
              {isHe ? 'פיזור סטטוסי עסקאות' : 'Trade Status Distribution'}
            </h2>
            <table className="w-full text-sm">
              <thead className="border-b border-slate-200 dark:border-slate-700">
                <tr className="text-xs text-slate-500 dark:text-slate-400">
                  <th className="px-2 py-2 text-start font-semibold">{isHe ? 'סטטוס' : 'Status'}</th>
                  <th className="px-2 py-2 text-end font-semibold">{isHe ? 'כמות' : 'Count'}</th>
                  <th className="px-2 py-2 text-end font-semibold">%</th>
                </tr>
              </thead>
              <tbody>
                {stats.statusData.map(({ name, value }) => (
                  <tr key={name} className="border-b border-slate-100 dark:border-slate-800">
                    <td className="px-2 py-2">
                      <span className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-medium ${
                        name === 'פתוח' || name === 'open'
                          ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300'
                          : 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-400'
                      }`}>
                        {name}
                      </span>
                    </td>
                    <td className="px-2 py-2 text-end tabular-nums">{value}</td>
                    <td className="px-2 py-2 text-end tabular-nums text-slate-500">
                      {stats.total > 0 ? `${((value / stats.total) * 100).toFixed(1)}%` : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>

          {/* Status pie */}
          <section className="card flex flex-col items-center">
            <h2 className="mb-3 w-full text-sm font-bold text-slate-700 dark:text-slate-200">
              {isHe ? 'פיזור סטטוסי עסקאות' : 'Status Breakdown'}
            </h2>
            <div className="h-52 w-full">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={stats.statusData}
                    dataKey="value"
                    nameKey="name"
                    cx="50%"
                    cy="50%"
                    outerRadius={80}
                    innerRadius={40}
                  >
                    {stats.statusData.map((_, i) => (
                      <Cell key={i} fill={STATUS_COLORS[i % STATUS_COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip contentStyle={{ fontSize: 12, borderRadius: 8 }} />
                  <Legend wrapperStyle={{ fontSize: 11 }} />
                </PieChart>
              </ResponsiveContainer>
            </div>
          </section>
        </div>
      )}

      {/* Recent data table */}
      <section className="card">
        <h2 className="mb-3 text-sm font-bold text-slate-700 dark:text-slate-200">
          {isHe ? 'נתונים אחרונים' : 'Recent Data'}
        </h2>
        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead className="border-b border-slate-200 dark:border-slate-700">
              <tr className="text-slate-500 dark:text-slate-400">
                <th className="px-2 py-2 text-start font-semibold">{t('trade_id')}</th>
                <th className="px-2 py-2 text-start font-semibold">{t('ticker')}</th>
                <th className="px-2 py-2 text-start font-semibold">{t('platform')}</th>
                <th className="px-2 py-2 text-start font-semibold">{t('purchase_date')}</th>
                <th className="px-2 py-2 text-start font-semibold">{t('status')}</th>
                <th className="px-2 py-2 text-end font-semibold">{t('quantity')}</th>
                <th className="px-2 py-2 text-end font-semibold">{t('cost')}</th>
                <th className="px-2 py-2 text-end font-semibold">{t('value')}</th>
                <th className="px-2 py-2 text-end font-semibold">{t('return')}</th>
              </tr>
            </thead>
            <tbody>
              {views.trades.slice(0, 50).map((r, i) => {
                const profit = r.valueIls - r.costIls;
                const ret = r.costIls > 0 ? (profit / r.costIls) * 100 : 0;
                const positive = ret >= 0;
                return (
                  <tr
                    key={`${r.tradeId}-${i}`}
                    className="border-b border-slate-100 hover:bg-slate-50/50 dark:border-slate-800 dark:hover:bg-slate-800/30"
                  >
                    <td className="px-2 py-1.5 font-mono text-[10px] text-slate-400">{r.tradeId}</td>
                    <td className="px-2 py-1.5 font-mono font-bold text-slate-900 dark:text-slate-100">{r.ticker}</td>
                    <td className="px-2 py-1.5 text-slate-600 dark:text-slate-400">{r.platform}</td>
                    <td className="px-2 py-1.5 tabular-nums text-slate-500">{r.purchaseDate}</td>
                    <td className="px-2 py-1.5">
                      <span className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-medium ${
                        r.status === 'פתוח' || r.status === 'open'
                          ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300'
                          : 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-400'
                      }`}>
                        {r.status}
                      </span>
                    </td>
                    <td className="px-2 py-1.5 text-end tabular-nums">{formatNumber(r.quantity, locale, 4)}</td>
                    <td className="px-2 py-1.5 text-end tabular-nums">{formatCurrency(r.costIls, 'ILS', locale)}</td>
                    <td className="px-2 py-1.5 text-end tabular-nums">{formatCurrency(r.valueIls, 'ILS', locale)}</td>
                    <td className={`px-2 py-1.5 text-end font-semibold tabular-nums ${positive ? 'text-emerald-600 dark:text-emerald-400' : 'text-rose-600 dark:text-rose-400'}`}>
                      {ret.toFixed(2)}%
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
