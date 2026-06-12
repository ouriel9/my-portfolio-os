'use client';

import { useMemo, useState } from 'react';
import { useApp } from '@/components/providers/AppProvider';
import { useSnapshot } from '@/components/hooks/useSnapshot';
import { LoadingBlock, ErrorBlock, EmptyBlock } from '@/components/ui/StateBanners';
import { formatCurrency, formatNumber } from '@/lib/format';
import { herfindahlIndex, maxDrawdown } from '@/lib/risk';
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  PieChart,
  Pie,
  Cell,
  Legend,
  BarChart,
  Bar,
} from 'recharts';
import type { CoreViews } from '@/lib/types';

const BRAND_PALETTE = [
  '#4f46e5', '#06b6d4', '#10b981', '#f59e0b', '#ef4444',
  '#8b5cf6', '#ec4899', '#14b8a6', '#f97316', '#6366f1',
  '#84cc16', '#0ea5e9', '#a855f7', '#fb923c', '#22d3ee',
];

type TabId = 'overview' | 'allocation' | 'reports' | 'transactions';

/* ─── Overview tab ─────────────────────────────────────────────────────────── */
const CLOSED_SET = ['סגור', 'closed', 'close', 'sold', 'נמכר'];
const isClosedTrade = (s: string) => CLOSED_SET.includes((s ?? '').toLowerCase().trim());

function downloadBlob(content: string, name: string, mime: string) {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  a.click();
  URL.revokeObjectURL(url);
}

function OverviewTab({ views, locale, chartLock }: { views: CoreViews; locale: string; chartLock: boolean }) {
  const isHe = locale === 'he';

  if (views.history.length === 0) {
    return (
      <div className="card flex items-center justify-center py-16 text-slate-400">
        {isHe ? 'אין נתוני היסטוריה' : 'No history data available'}
      </div>
    );
  }

  return (
    <div className="space-y-5">
      <section className="card">
        <h2 className="mb-4 text-sm font-bold text-slate-700 dark:text-slate-200">
          {isHe ? 'תיק מצטבר' : 'Portfolio Build-Up'}
        </h2>
        <div className="h-72 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={views.history} margin={{ top: 5, right: 10, bottom: 5, left: 10 }}>
              <defs>
                <linearGradient id="valueGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#4f46e5" stopOpacity={0.15} />
                  <stop offset="95%" stopColor="#4f46e5" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="rgba(148,163,184,0.15)" strokeDasharray="3 3" />
              <XAxis dataKey="date" tick={{ fontSize: 10 }} minTickGap={32} />
              <YAxis
                tick={{ fontSize: 10 }}
                tickFormatter={(v: number) => `₪${(v / 1000).toFixed(0)}k`}
              />
              {!chartLock && (
                <Tooltip
                  formatter={(v: number) => [formatCurrency(v, 'ILS', locale as 'he' | 'en'), isHe ? 'שווי' : 'Value']}
                  labelClassName="text-xs"
                />
              )}
              <Area
                type="monotone"
                dataKey="value"
                stroke="#4f46e5"
                strokeWidth={2.5}
                fill="url(#valueGrad)"
                dot={{ r: 3, fill: '#4f46e5' }}
                activeDot={{ r: 5 }}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </section>
    </div>
  );
}

/* ─── Allocation tab ────────────────────────────────────────────────────────── */
function AllocationTab({ views, locale, chartLock }: { views: CoreViews; locale: string; chartLock: boolean }) {
  const isHe = locale === 'he';

  const pnlByTicker = useMemo(
    () =>
      [...views.byTicker]
        .map((r) => ({ key: r.key, profit: r.profit }))
        .sort((a, b) => b.profit - a.profit),
    [views.byTicker],
  );

  const openTrades = views.openTrades;

  return (
    <div className="space-y-5">
      {/* Row: Donut + Bar */}
      <div className="grid gap-4 lg:grid-cols-2">
        {/* Donut pie */}
        <section className="card">
          <h2 className="mb-3 text-sm font-bold text-slate-700 dark:text-slate-200">
            {isHe ? 'הרכב לפי טיקר' : 'Allocation by Ticker'}
          </h2>
          <div className="h-64 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={views.byTicker.slice(0, 15)}
                  dataKey="value"
                  nameKey="key"
                  cx="50%"
                  cy="50%"
                  innerRadius="50%"
                  outerRadius="75%"
                  paddingAngle={2}
                >
                  {views.byTicker.slice(0, 15).map((_, i) => (
                    <Cell key={i} fill={BRAND_PALETTE[i % BRAND_PALETTE.length]} />
                  ))}
                </Pie>
                {!chartLock && (
                  <Tooltip
                    formatter={(v: number, name: string) => [
                      formatCurrency(v, 'ILS', locale as 'he' | 'en'),
                      name,
                    ]}
                  />
                )}
                <Legend wrapperStyle={{ fontSize: 11 }} />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </section>

        {/* P&L bar */}
        <section className="card">
          <h2 className="mb-3 text-sm font-bold text-slate-700 dark:text-slate-200">
            {isHe ? 'רווח/הפסד נטו לפי נכס' : 'Net P&L by Asset'}
          </h2>
          <div className="h-64 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={pnlByTicker} margin={{ top: 5, right: 5, bottom: 20, left: 5 }}>
                <CartesianGrid stroke="rgba(148,163,184,0.12)" strokeDasharray="3 3" />
                <XAxis dataKey="key" tick={{ fontSize: 10 }} angle={-35} textAnchor="end" interval={0} />
                <YAxis
                  tick={{ fontSize: 10 }}
                  tickFormatter={(v: number) => `₪${(v / 1000).toFixed(0)}k`}
                />
                {!chartLock && (
                  <Tooltip
                    formatter={(v: number) => [formatCurrency(v, 'ILS', locale as 'he' | 'en'), isHe ? 'רווח/הפסד' : 'P&L']}
                  />
                )}
                <Bar dataKey="profit" radius={[4, 4, 0, 0]}>
                  {pnlByTicker.map((entry, i) => (
                    <Cell key={i} fill={entry.profit >= 0 ? '#16a34a' : '#dc2626'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </section>
      </div>

      {/* Class allocation */}
      <section className="card">
        <h2 className="mb-3 text-sm font-bold text-slate-700 dark:text-slate-200">
          {isHe ? 'הקצאה לפי קטגוריה' : 'Allocation by Category'}
        </h2>
        <div className="h-44 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={views.byType}
              layout="vertical"
              margin={{ top: 5, right: 20, bottom: 5, left: 80 }}
            >
              <CartesianGrid stroke="rgba(148,163,184,0.12)" strokeDasharray="3 3" horizontal={false} />
              <XAxis
                type="number"
                tick={{ fontSize: 10 }}
                tickFormatter={(v: number) => `₪${(v / 1000).toFixed(0)}k`}
              />
              <YAxis type="category" dataKey="key" tick={{ fontSize: 11 }} width={75} />
              {!chartLock && (
                <Tooltip
                  formatter={(v: number) => [formatCurrency(v, 'ILS', locale as 'he' | 'en'), isHe ? 'שווי' : 'Value']}
                />
              )}
              <Bar dataKey="value" radius={[0, 4, 4, 0]}>
                {views.byType.map((_, i) => (
                  <Cell key={i} fill={BRAND_PALETTE[i % BRAND_PALETTE.length]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </section>

      {/* Exposure table */}
      <section className="card overflow-hidden p-0">
        <div className="border-b border-slate-200 dark:border-slate-700 px-5 py-3">
          <h2 className="text-sm font-bold text-slate-700 dark:text-slate-200">
            {isHe ? 'טבלת חשיפה' : 'Exposure Table'}
          </h2>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead className="bg-slate-50 dark:bg-slate-800/50">
              <tr className="text-slate-500 dark:text-slate-400">
                <th className="px-4 py-2.5 text-start font-semibold">{isHe ? 'טיקר' : 'Ticker'}</th>
                <th className="px-4 py-2.5 text-end font-semibold">{isHe ? 'מחיר נוכחי' : 'Current Price'}</th>
                <th className="px-4 py-2.5 text-end font-semibold">{isHe ? 'כמות פתוחה' : 'Open Qty'}</th>
                <th className="px-4 py-2.5 text-end font-semibold">{isHe ? 'עלות (₪)' : 'Cost (₪)'}</th>
                <th className="px-4 py-2.5 text-end font-semibold">{isHe ? 'שווי (₪)' : 'Value (₪)'}</th>
                <th className="px-4 py-2.5 text-end font-semibold">{isHe ? 'ר/ה (₪)' : 'P&L (₪)'}</th>
                <th className="px-4 py-2.5 text-end font-semibold">{isHe ? 'תשואה %' : 'Yield %'}</th>
              </tr>
            </thead>
            <tbody>
              {openTrades.slice(0, 100).map((t, i) => {
                const pnl = t.valueIls - t.costIls;
                const yieldPct = t.costIls > 0 ? (pnl / t.costIls) * 100 : 0;
                return (
                  <tr
                    key={`${t.tradeId}-${i}`}
                    className="border-t border-slate-100 hover:bg-slate-50/50 dark:border-slate-800 dark:hover:bg-slate-800/30"
                  >
                    <td className="px-4 py-2 font-mono font-bold text-slate-900 dark:text-slate-100">
                      {t.ticker}
                    </td>
                    <td className="px-4 py-2 text-end font-mono tabular-nums text-slate-600 dark:text-slate-300">
                      {formatCurrency(t.spotIls, 'ILS', locale as 'he' | 'en')}
                    </td>
                    <td className="px-4 py-2 text-end font-mono tabular-nums">
                      {formatNumber(t.quantity, locale as 'he' | 'en', 4)}
                    </td>
                    <td className="px-4 py-2 text-end font-mono tabular-nums">
                      {formatCurrency(t.costIls, 'ILS', locale as 'he' | 'en')}
                    </td>
                    <td className="px-4 py-2 text-end font-mono tabular-nums">
                      {formatCurrency(t.valueIls, 'ILS', locale as 'he' | 'en')}
                    </td>
                    <td
                      className={`px-4 py-2 text-end font-mono font-semibold tabular-nums ${pnl >= 0 ? 'text-emerald-600 dark:text-emerald-400' : 'text-rose-600 dark:text-rose-400'}`}
                    >
                      {formatCurrency(pnl, 'ILS', locale as 'he' | 'en')}
                    </td>
                    <td
                      className={`px-4 py-2 text-end font-mono font-semibold tabular-nums ${yieldPct >= 0 ? 'text-emerald-600 dark:text-emerald-400' : 'text-rose-600 dark:text-rose-400'}`}
                    >
                      {yieldPct.toFixed(2)}%
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

/* ─── Reports tab ───────────────────────────────────────────────────────────── */
function ReportsTab({ views, locale, chartLock }: { views: CoreViews; locale: string; chartLock: boolean }) {
  const isHe = locale === 'he';

  const weights = views.byTicker.map((r) => r.weight);
  const hhi = herfindahlIndex(weights);
  const histValues = views.history.map((h) => h.value);
  const mdd = maxDrawdown(histValues);
  const totalReturn = views.totalReturn * 100;

  const metrics = [
    { label: isHe ? 'תשואה כוללת' : 'Total Return', value: `${totalReturn.toFixed(2)}%`, positive: totalReturn >= 0 },
    { label: isHe ? 'ירידה מקסימלית' : 'Max Drawdown', value: `${(mdd * 100).toFixed(2)}%`, positive: mdd === 0 },
    { label: isHe ? 'ריכוזיות (HHI)' : 'Concentration (HHI)', value: hhi.toFixed(4), positive: hhi < 0.25 },
    { label: isHe ? 'מספר פוזיציות פתוחות' : 'Open Positions', value: String(views.openTrades.length), positive: true },
    { label: isHe ? 'מספר פוזיציות סגורות' : 'Closed Positions', value: String(views.closedTrades.length), positive: true },
    { label: isHe ? 'עלות כוללת' : 'Total Cost', value: formatCurrency(views.totalCostIls, 'ILS', locale as 'he' | 'en'), positive: true },
  ];

  return (
    <div className="space-y-5">
      {/* Key metrics grid */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
        {metrics.map((m, i) => (
          <div key={i} className="card">
            <div className="text-xs font-medium text-slate-500 dark:text-slate-400">{m.label}</div>
            <div
              className={`mt-1.5 text-xl font-bold tabular-nums ${
                m.positive ? 'text-slate-900 dark:text-white' : 'text-rose-600 dark:text-rose-400'
              }`}
            >
              {m.value}
            </div>
          </div>
        ))}
      </div>

      {/* Allocation by type chart */}
      <section className="card">
        <h2 className="mb-3 text-sm font-bold text-slate-700 dark:text-slate-200">
          {isHe ? 'הרכב לפי סוג נכס' : 'Allocation by Asset Type'}
        </h2>
        <div className="h-52 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={views.byType}
                dataKey="value"
                nameKey="key"
                cx="50%"
                cy="50%"
                outerRadius="65%"
                label={({ name, percent }: { name: string; percent: number }) =>
                  `${name} ${(percent * 100).toFixed(0)}%`
                }
              >
                {views.byType.map((_, i) => (
                  <Cell key={i} fill={BRAND_PALETTE[i % BRAND_PALETTE.length]} />
                ))}
              </Pie>
              {!chartLock && (
                <Tooltip
                  formatter={(v: number) => [formatCurrency(v, 'ILS', locale as 'he' | 'en'), isHe ? 'שווי' : 'Value']}
                />
              )}
            </PieChart>
          </ResponsiveContainer>
        </div>
      </section>

      {/* Crypto exposure KPIs — mirrors app.py crypto-share report */}
      {(() => {
        const open = views.openTrades;
        const totalVal = views.totalValueIls;
        const isCrypto = (t: (typeof open)[number]) => t.type.includes('קריפטו') || /crypto/i.test(t.type);
        const cryptoVal = open.filter(isCrypto).reduce((s, t) => s + t.valueIls, 0);
        const btcVal = open.filter((t) => t.ticker === 'BTC').reduce((s, t) => s + t.valueIls, 0);
        const pct = (x: number, base: number) => (base > 0 ? ((x / base) * 100).toFixed(1) + '%' : '—');
        const cards = [
          { label: isHe ? 'חלק הקריפטו מהתיק' : 'Crypto share of portfolio', value: pct(cryptoVal, totalVal) },
          { label: isHe ? 'BTC מהתיק' : 'BTC share of portfolio', value: pct(btcVal, totalVal) },
          { label: isHe ? 'BTC מתוך הקריפטו' : 'BTC share of crypto', value: pct(btcVal, cryptoVal) },
        ];
        return (
          <div className="grid grid-cols-3 gap-3">
            {cards.map((c, i) => (
              <div key={i} className="card">
                <div className="text-xs font-medium text-slate-500 dark:text-slate-400">{c.label}</div>
                <div className="mt-1.5 text-xl font-bold tabular-nums text-slate-900 dark:text-white" data-ltr>{c.value}</div>
              </div>
            ))}
          </div>
        );
      })()}

      {/* Net investment by platform — mirrors app.py platform report */}
      <section className="card overflow-hidden">
        <h2 className="mb-3 text-sm font-bold text-slate-700 dark:text-slate-200">
          {isHe ? 'השקעה נטו לפי פלטפורמה' : 'Net Investment by Platform'}
        </h2>
        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead>
              <tr className="text-slate-500 dark:text-slate-400 border-b border-slate-200 dark:border-slate-700">
                <th className="pb-2 text-start font-semibold pe-4">{isHe ? 'פלטפורמה' : 'Platform'}</th>
                <th className="pb-2 text-end font-semibold pe-4">{isHe ? 'עלות (₪)' : 'Cost (₪)'}</th>
                <th className="pb-2 text-end font-semibold pe-4">{isHe ? 'שווי (₪)' : 'Value (₪)'}</th>
                <th className="pb-2 text-end font-semibold">{isHe ? 'רווח/הפסד (₪)' : 'P&L (₪)'}</th>
              </tr>
            </thead>
            <tbody>
              {views.byPlatform.map((r) => (
                <tr key={r.key} className="border-b border-slate-100 dark:border-slate-800">
                  <td className="py-1.5 pe-4 font-semibold">{r.key}</td>
                  <td className="py-1.5 pe-4 text-end font-mono tabular-nums">{formatCurrency(r.cost, 'ILS', locale as 'he' | 'en')}</td>
                  <td className="py-1.5 pe-4 text-end font-mono tabular-nums">{formatCurrency(r.value, 'ILS', locale as 'he' | 'en')}</td>
                  <td className={`py-1.5 text-end font-mono font-semibold tabular-nums ${r.profit >= 0 ? 'text-emerald-600 dark:text-emerald-400' : 'text-rose-600 dark:text-rose-400'}`}>
                    {formatCurrency(r.profit, 'ILS', locale as 'he' | 'en')}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {/* Winner / Loser — best & worst open position by ILS yield */}
      {(() => {
        const withYield = views.openTrades
          .filter((t) => t.costIls > 0)
          .map((t) => ({ t, y: (t.valueIls - t.costIls) / t.costIls }))
          .sort((a, b) => b.y - a.y);
        if (withYield.length === 0) return null;
        const winner = withYield[0]!;
        const loser = withYield[withYield.length - 1]!;
        const row = (label: string, e: { t: (typeof views.openTrades)[number]; y: number }, good: boolean) => (
          <div className="flex items-center justify-between py-2">
            <span className="text-xs font-medium text-slate-500 dark:text-slate-400">{label}</span>
            <span className="font-mono font-bold">{e.t.ticker}</span>
            <span className={`font-mono font-semibold tabular-nums ${good ? 'text-emerald-600 dark:text-emerald-400' : 'text-rose-600 dark:text-rose-400'}`} data-ltr>
              {(e.y * 100).toFixed(2)}%
            </span>
          </div>
        );
        return (
          <section className="card">
            <h2 className="mb-2 text-sm font-bold text-slate-700 dark:text-slate-200">
              {isHe ? 'המוביל והמפגר בתיק' : 'Winner & Loser'}
            </h2>
            <div className="divide-y divide-slate-100 dark:divide-slate-800">
              {row(isHe ? '🏆 מוביל' : '🏆 Winner', winner, true)}
              {row(isHe ? '📉 מפגר' : '📉 Loser', loser, false)}
            </div>
          </section>
        );
      })()}
    </div>
  );
}

/* ─── Transactions tab ──────────────────────────────────────────────────────── */
function TransactionsTab({ views, locale }: { views: CoreViews; locale: string }) {
  const isHe = locale === 'he';

  const [statusFilter, setStatusFilter] = useState<'all' | 'open' | 'closed'>('all');
  const [tickerFilter, setTickerFilter] = useState<string>('all');

  const tickers = useMemo(
    () => Array.from(new Set(views.trades.map((t) => t.ticker))).sort(),
    [views.trades],
  );

  const sorted = useMemo(() => {
    let rows = [...views.trades];
    if (statusFilter !== 'all') {
      rows = rows.filter((t) => (statusFilter === 'closed') === isClosedTrade(t.status));
    }
    if (tickerFilter !== 'all') rows = rows.filter((t) => t.ticker === tickerFilter);
    return rows.sort((a, b) => b.purchaseDate.localeCompare(a.purchaseDate));
  }, [views.trades, statusFilter, tickerFilter]);

  const exportCsv = () => {
    const head = ['Date', 'Ticker', 'Status', 'Platform', 'Qty', 'Cost_ILS', 'Value_ILS', 'Return_%'];
    const rows = sorted.map((t) => [
      t.purchaseDate.slice(0, 10), t.ticker, t.status, t.platform, t.quantity,
      t.costIls, t.valueIls, t.costIls > 0 ? (((t.valueIls - t.costIls) / t.costIls) * 100).toFixed(2) : '',
    ]);
    const csv = '﻿' + [head, ...rows].map((r) => r.join(',')).join('\n');
    downloadBlob(csv, 'transactions.csv', 'text/csv;charset=utf-8');
  };
  const exportJson = () =>
    downloadBlob(JSON.stringify(sorted, null, 2), 'transactions.json', 'application/json');

  const [deposits, setDeposits] = useState<{ platform: string; amount: number }[]>([
    { platform: '', amount: 0 },
  ]);

  const totalDeposits = deposits.reduce((s, d) => s + d.amount, 0);

  return (
    <div className="space-y-5">
      {/* Manual deposits editor */}
      <section className="card">
        <h2 className="mb-3 text-sm font-bold text-slate-700 dark:text-slate-200">
          {isHe ? 'הפקדות ידניות' : 'Manual Deposits'}
        </h2>
        <div className="overflow-x-auto">
          <table className="min-w-[340px] text-xs">
            <thead>
              <tr className="text-slate-500 dark:text-slate-400 border-b border-slate-200 dark:border-slate-700">
                <th className="pb-2 text-start font-semibold pe-4">{isHe ? 'פלטפורמה' : 'Platform'}</th>
                <th className="pb-2 text-end font-semibold">{isHe ? 'סכום (₪)' : 'Amount (₪)'}</th>
              </tr>
            </thead>
            <tbody>
              {deposits.map((d, i) => (
                <tr key={i} className="border-b border-slate-100 dark:border-slate-800">
                  <td className="py-1.5 pe-4">
                    <input
                      value={d.platform}
                      onChange={(e) => {
                        const next = [...deposits];
                        next[i] = { ...next[i], platform: e.target.value };
                        setDeposits(next);
                      }}
                      placeholder={isHe ? 'פלטפורמה' : 'Platform'}
                      className="w-full rounded border border-slate-200 bg-transparent px-2 py-1 text-xs dark:border-slate-700"
                    />
                  </td>
                  <td className="py-1.5">
                    <input
                      type="number"
                      value={d.amount || ''}
                      onChange={(e) => {
                        const next = [...deposits];
                        next[i] = { ...next[i], amount: Number(e.target.value) };
                        setDeposits(next);
                      }}
                      placeholder="0"
                      className="w-full rounded border border-slate-200 bg-transparent px-2 py-1 text-end text-xs dark:border-slate-700"
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="mt-3 flex items-center gap-3">
          <button
            onClick={() => setDeposits((prev) => [...prev, { platform: '', amount: 0 }])}
            className="btn btn-secondary text-xs py-1.5 px-3"
          >
            + {isHe ? 'הוסף שורה' : 'Add Row'}
          </button>
          <button className="btn btn-primary text-xs py-1.5 px-3">
            {isHe ? 'שמור הפקדות' : 'Save Deposits'}
          </button>
          <span className="ms-auto text-xs font-semibold text-slate-600 dark:text-slate-300">
            {isHe ? 'סך הפקדות:' : 'Total Deposits:'}{' '}
            <span className="text-indigo-600 dark:text-indigo-400">
              {formatCurrency(totalDeposits, 'ILS', locale as 'he' | 'en')}
            </span>
          </span>
        </div>
      </section>

      {/* Transactions table */}
      <section className="card overflow-hidden p-0">
        <div className="border-b border-slate-200 dark:border-slate-700 px-5 py-3 flex items-center justify-between">
          <h2 className="text-sm font-bold text-slate-700 dark:text-slate-200">
            {isHe ? 'טבלת עסקאות' : 'Transactions Table'}
          </h2>
          <div className="flex items-center gap-2">
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value as 'all' | 'open' | 'closed')}
              className="rounded-lg border border-slate-200 bg-transparent px-2 py-1 text-xs dark:border-slate-700 dark:bg-slate-900"
            >
              <option value="all">{isHe ? 'הכל' : 'All'}</option>
              <option value="open">{isHe ? 'פתוחות' : 'Open'}</option>
              <option value="closed">{isHe ? 'סגורות' : 'Closed'}</option>
            </select>
            <select
              value={tickerFilter}
              onChange={(e) => setTickerFilter(e.target.value)}
              className="rounded-lg border border-slate-200 bg-transparent px-2 py-1 text-xs dark:border-slate-700 dark:bg-slate-900"
            >
              <option value="all">{isHe ? 'כל הטיקרים' : 'All tickers'}</option>
              {tickers.map((tk) => (
                <option key={tk} value={tk}>{tk}</option>
              ))}
            </select>
            <button onClick={exportCsv} className="btn btn-secondary text-xs py-1 px-2.5">CSV</button>
            <button onClick={exportJson} className="btn btn-secondary text-xs py-1 px-2.5">JSON</button>
            <span className="text-xs text-slate-400">
              {sorted.length} {isHe ? 'שורות' : 'rows'}
            </span>
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead className="bg-slate-50 dark:bg-slate-800/50">
              <tr className="text-slate-500 dark:text-slate-400">
                <th className="px-4 py-2.5 text-start font-semibold">{isHe ? 'תאריך' : 'Date'}</th>
                <th className="px-4 py-2.5 text-start font-semibold">{isHe ? 'טיקר' : 'Ticker'}</th>
                <th className="px-4 py-2.5 text-start font-semibold">{isHe ? 'סטטוס' : 'Status'}</th>
                <th className="px-4 py-2.5 text-start font-semibold">{isHe ? 'פלטפורמה' : 'Platform'}</th>
                <th className="px-4 py-2.5 text-end font-semibold">{isHe ? 'כמות' : 'Qty'}</th>
                <th className="px-4 py-2.5 text-end font-semibold">{isHe ? 'עלות (₪)' : 'Cost (₪)'}</th>
                <th className="px-4 py-2.5 text-end font-semibold">{isHe ? 'שווי (₪)' : 'Value (₪)'}</th>
                <th className="px-4 py-2.5 text-end font-semibold">{isHe ? 'תשואה' : 'Return'}</th>
                <th className="px-4 py-2.5 text-end font-semibold">{isHe ? 'מחיר מכירה' : 'Sell Price'}</th>
              </tr>
            </thead>
            <tbody>
              {sorted.slice(0, 300).map((t, i) => {
                const ret = t.costIls > 0 ? ((t.valueIls - t.costIls) / t.costIls) * 100 : 0;
                const positive = t.valueIls >= t.costIls;
                return (
                  <tr
                    key={`${t.tradeId}-${i}`}
                    className="border-t border-slate-100 hover:bg-slate-50/50 dark:border-slate-800 dark:hover:bg-slate-800/30"
                  >
                    <td className="px-4 py-2 font-mono text-slate-500 dark:text-slate-400">
                      {t.purchaseDate.slice(0, 10)}
                    </td>
                    <td className="px-4 py-2 font-mono font-bold text-slate-900 dark:text-slate-100">
                      {t.ticker}
                    </td>
                    <td className="px-4 py-2 text-slate-600 dark:text-slate-400">{t.status}</td>
                    <td className="px-4 py-2 text-slate-600 dark:text-slate-400">{t.platform}</td>
                    <td className="px-4 py-2 text-end font-mono tabular-nums">
                      {formatNumber(t.quantity, locale as 'he' | 'en', 4)}
                    </td>
                    <td className="px-4 py-2 text-end font-mono tabular-nums">
                      {formatCurrency(t.costIls, 'ILS', locale as 'he' | 'en')}
                    </td>
                    <td className="px-4 py-2 text-end font-mono tabular-nums">
                      {formatCurrency(t.valueIls, 'ILS', locale as 'he' | 'en')}
                    </td>
                    <td
                      className={`px-4 py-2 text-end font-mono font-semibold tabular-nums ${positive ? 'text-emerald-600 dark:text-emerald-400' : 'text-rose-600 dark:text-rose-400'}`}
                    >
                      {ret.toFixed(2)}%
                    </td>
                    <td className="px-4 py-2 text-end font-mono tabular-nums text-slate-500 dark:text-slate-400">
                      {isClosedTrade(t.status) && t.quantity > 0
                        ? formatCurrency(t.valueIls / t.quantity, 'ILS', locale as 'he' | 'en')
                        : '—'}
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

/* ─── Main page ─────────────────────────────────────────────────────────────── */
export default function DashboardPage() {
  const { locale, chartLock } = useApp();
  const { views, error, isLoading, refresh } = useSnapshot();
  const [activeTab, setActiveTab] = useState<TabId>('overview');

  const isHe = locale === 'he';

  if (isLoading) return <LoadingBlock label={isHe ? 'טוען...' : 'Loading...'} />;
  if (error) return <ErrorBlock message={error} />;
  if (!views) return <EmptyBlock message={isHe ? 'אין נתונים זמינים' : 'No data available'} />;

  const totalReturnPct = views.totalReturn * 100;
  const closedCount = views.closedTrades.length;
  const openCount = views.openTrades.length;
  const totalTrades = views.trades.length;

  const tabs: { id: TabId; label: string }[] = [
    { id: 'overview', label: isHe ? 'סקירה כללית' : 'Overview' },
    { id: 'allocation', label: isHe ? 'הרכב התיק' : 'Allocation' },
    { id: 'reports', label: isHe ? 'דוחות ואנליזה' : 'Reports' },
    { id: 'transactions', label: isHe ? 'תנועות ועסקאות' : 'Transactions' },
  ];

  return (
    <div className="space-y-5">
      {/* Banner header card */}
      <div
        className="relative overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-soft dark:border-slate-800 dark:bg-slate-900"
        style={{ borderTop: '3px solid #4f46e5' }}
      >
        <div className="flex flex-col items-center px-5 py-6">
          {/* Page name top corner — physical start (right in RTL) */}
          <span className="absolute start-4 top-4 text-sm font-bold text-indigo-600 dark:text-indigo-400">
            {isHe ? 'דשבורד' : 'Dashboard'}
          </span>

          {/* Icon + title side by side (centered) */}
          <div className="flex items-center gap-3">
            <div className="grid h-12 w-12 shrink-0 place-items-center rounded-xl bg-gradient-to-br from-indigo-500 to-purple-600 shadow-md">
              <svg
                width="22"
                height="22"
                viewBox="0 0 24 24"
                fill="none"
                stroke="white"
                strokeWidth="2"
                strokeLinecap="round"
              >
                <rect x="3" y="3" width="7" height="7" rx="1.5" />
                <rect x="14" y="3" width="7" height="7" rx="1.5" />
                <rect x="3" y="14" width="7" height="7" rx="1.5" />
                <rect x="14" y="14" width="7" height="7" rx="1.5" />
              </svg>
            </div>
            <h1 className="text-2xl font-bold text-slate-900 dark:text-white">
              {isHe ? 'מערכת ניהול תיק' : 'Portfolio OS'}
            </h1>
          </div>

          {/* Sub-badge */}
          <span className="mt-2 inline-flex items-center rounded-full bg-indigo-50 px-3 py-0.5 text-xs font-medium text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-300">
            {isHe ? 'ניהול השקעות חכם' : 'Smart investment management'}
          </span>
        </div>
      </div>

      {/* Stats line + refresh */}
      <div className="flex items-center justify-between">
        <p className="text-xs text-slate-500 dark:text-slate-400">
          {totalTrades} {isHe ? 'רשומות נטענו' : 'records'} &nbsp;|&nbsp; {openCount}{' '}
          {isHe ? 'פתוחות' : 'open'} &nbsp;|&nbsp; {closedCount}{' '}
          {isHe ? 'סגורות' : 'closed'}
        </p>
        <button
          onClick={() => refresh()}
          className="text-xs text-indigo-500 hover:text-indigo-700 dark:text-indigo-400 dark:hover:text-indigo-200"
        >
          {isHe ? '↻ רענן' : '↻ Refresh'}
        </button>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        {/* Total value */}
        <div className="relative card card-hover overflow-hidden">
          <div className="absolute left-0 top-0 bottom-0 w-1.5 bg-indigo-500" />
          <div className="ps-3">
            <div className="kpi-label" dir="auto">{isHe ? 'שווי כולל (₪)' : 'Total Value (₪)'}</div>
            <div className="mt-2 kpi-value mono-num">
              {formatCurrency(views.totalValueIls, 'ILS', locale as 'he' | 'en')}
            </div>
            <div
              className={`mt-1 text-xs font-semibold tabular-nums ${views.totalProfitIls >= 0 ? 'text-emerald-600 dark:text-emerald-400' : 'text-rose-600 dark:text-rose-400'}`}
            >
              {views.totalProfitIls >= 0 ? '↑' : '↓'}{' '}
              {formatCurrency(views.totalProfitIls, 'ILS', locale as 'he' | 'en')}
            </div>
          </div>
        </div>

        {/* Open P&L */}
        <div className="relative card card-hover overflow-hidden">
          <div className="absolute left-0 top-0 bottom-0 w-1.5 bg-indigo-500" />
          <div className="ps-3">
            <div className="kpi-label" dir="auto">{isHe ? 'רווח/הפסד פתוח (₪)' : 'Open P&L (₪)'}</div>
            <div
              className={`mt-2 kpi-value mono-num ${views.totalProfitIls >= 0 ? 'text-emerald-600 dark:text-emerald-400' : 'text-rose-600 dark:text-rose-400'}`}
            >
              {formatCurrency(views.totalProfitIls, 'ILS', locale as 'he' | 'en')}
            </div>
          </div>
        </div>

        {/* Total return */}
        <div className="relative card card-hover overflow-hidden">
          <div className="absolute left-0 top-0 bottom-0 w-1.5 bg-indigo-500" />
          <div className="ps-3">
            <div className="kpi-label" dir="auto">{isHe ? 'תשואה כוללת' : 'Total Return'}</div>
            <div
              className={`mt-2 kpi-value mono-num ${totalReturnPct >= 0 ? 'text-emerald-600 dark:text-emerald-400' : 'text-rose-600 dark:text-rose-400'}`}
            >
              {totalReturnPct.toFixed(2)}%
            </div>
          </div>
        </div>

        {/* Closed positions */}
        <div className="relative card card-hover overflow-hidden">
          <div className="absolute left-0 top-0 bottom-0 w-1.5 bg-indigo-500" />
          <div className="ps-3">
            <div className="kpi-label" dir="auto">{isHe ? 'פוזיציות סגורות' : 'Closed Positions'}</div>
            <div className="mt-2 kpi-value mono-num">{closedCount}</div>
            <div className="mt-1 text-xs font-semibold text-emerald-600 dark:text-emerald-400">
              ↑ {openCount} {isHe ? 'פתוחות' : 'open'}
            </div>
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div>
        <div className="flex gap-0 border-b border-slate-200 dark:border-slate-700 overflow-x-auto">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`whitespace-nowrap px-4 py-2.5 text-sm font-medium transition-all border-b-2 -mb-px ${
                activeTab === tab.id
                  ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400'
                  : 'border-transparent text-slate-600 hover:text-slate-900 dark:text-slate-400 dark:hover:text-slate-200'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        <div className="mt-5">
          {activeTab === 'overview' && <OverviewTab views={views} locale={locale} chartLock={chartLock} />}
          {activeTab === 'allocation' && <AllocationTab views={views} locale={locale} chartLock={chartLock} />}
          {activeTab === 'reports' && <ReportsTab views={views} locale={locale} chartLock={chartLock} />}
          {activeTab === 'transactions' && <TransactionsTab views={views} locale={locale} />}
        </div>
      </div>
    </div>
  );
}
