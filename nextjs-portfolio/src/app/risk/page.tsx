'use client';

import { useMemo, useState } from 'react';
import { useApp } from '@/components/providers/AppProvider';
import { useSnapshot } from '@/components/hooks/useSnapshot';
import { KpiTile } from '@/components/ui/KpiTile';
import { PageHeader } from '@/components/ui/PageHeader';
import { LoadingBlock, ErrorBlock, EmptyBlock } from '@/components/ui/StateBanners';
import { formatCurrency, formatNumber } from '@/lib/format';
import { cn } from '@/lib/cn';
import {
  dailyReturnsFromHistory,
  fifoMetrics,
  herfindahlIndex,
  historicalCvar,
  historicalVar,
  maxDrawdown,
  sharpeRatio,
  sortinoRatio,
} from '@/lib/risk';
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
} from 'recharts';

function annualizedVol(returns: number[]): number {
  if (returns.length < 2) return 0;
  const m = returns.reduce((s, x) => s + x, 0) / returns.length;
  const v = returns.reduce((s, x) => s + (x - m) ** 2, 0) / (returns.length - 1);
  return Math.sqrt(v) * Math.sqrt(252);
}

function cagr(values: number[]): number {
  if (values.length < 2) return 0;
  const start = values[0] ?? 0;
  const end = values[values.length - 1] ?? 0;
  if (start <= 0) return 0;
  const years = values.length / 252;
  return (end / start) ** (1 / years) - 1;
}

export default function RiskPage() {
  const { t, locale, chartLock } = useApp();
  const isHe = locale === 'he';
  const { views, error, isLoading } = useSnapshot();
  const [fifoOpen, setFifoOpen] = useState(false);

  const fifo = useMemo(() => (views ? fifoMetrics(views.trades) : []), [views]);

  const drawdownSeries = useMemo(() => {
    if (!views) return [];
    let peak = 0;
    return views.history.map((p) => {
      if (p.value > peak) peak = p.value;
      const dd = peak === 0 ? 0 : ((p.value - peak) / peak) * 100;
      return { date: p.date, drawdown: dd };
    });
  }, [views]);

  const metrics = useMemo(() => {
    if (!views) return null;
    const values = views.history.map((p) => p.value);
    const ret = dailyReturnsFromHistory(views.history);
    const weights = views.byTicker.map((r) => r.weight);
    return {
      sharpe: sharpeRatio(ret),
      sortino: sortinoRatio(ret),
      vol: annualizedVol(ret),
      mdd: maxDrawdown(values),
      cagrV: cagr(values),
      hhi: herfindahlIndex(weights),
      var95: historicalVar(ret),
      cvar95: historicalCvar(ret),
    };
  }, [views]);

  if (isLoading) return <LoadingBlock label={t('loading')} />;
  if (error) return <ErrorBlock message={error} />;
  if (!views || !metrics) return <EmptyBlock message={t('no_data')} />;

  return (
    <div className="space-y-5">
      <PageHeader title={t('nav_risk')} accent="rose" />

      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <KpiTile
          label={t('sharpe')}
          value={metrics.sharpe.toFixed(2)}
          accent="rose"
          hint="Return / total volatility"
        />
        <KpiTile
          label={t('volatility')}
          value={`${(metrics.vol * 100).toFixed(2)}%`}
          accent="rose"
          hint="Annualised σ of daily returns"
        />
        <KpiTile
          label={t('drawdown')}
          value={`${(metrics.mdd * 100).toFixed(2)}%`}
          accent="rose"
          hint="Worst peak-to-trough"
        />
        <KpiTile
          label="CAGR"
          value={`${(metrics.cagrV * 100).toFixed(2)}%`}
          accent={metrics.cagrV >= 0 ? 'emerald' : 'rose'}
          hint="Compound annual growth rate"
        />
        <KpiTile
          label={t('var')}
          value={`${(metrics.var95 * 100).toFixed(2)}%`}
          accent="rose"
        />
        <KpiTile
          label={t('cvar')}
          value={`${(metrics.cvar95 * 100).toFixed(2)}%`}
          accent="rose"
        />
        <KpiTile label={t('sortino')} value={metrics.sortino.toFixed(2)} accent="rose" />
        <KpiTile
          label={t('hhi')}
          value={metrics.hhi.toFixed(3)}
          accent="rose"
          hint="0 = diversified · 1 = concentrated"
        />
      </div>

      {/* Drawdown chart */}
      <section className="card">
        <h2 className="mb-3 text-sm font-bold text-slate-700 dark:text-slate-200">
          {t('drawdown')} (%)
        </h2>
        <div className="h-72 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={drawdownSeries}>
              <CartesianGrid stroke="rgba(148,163,184,0.18)" strokeDasharray="3 3" />
              <XAxis dataKey="date" tick={{ fontSize: 11 }} minTickGap={28} />
              <YAxis
                tick={{ fontSize: 11 }}
                tickFormatter={(v: number) => `${v.toFixed(0)}%`}
              />
              {!chartLock && <Tooltip formatter={(value: number) => `${value.toFixed(2)}%`} />}
              <Area
                type="monotone"
                dataKey="drawdown"
                stroke="#ef4444"
                strokeWidth={1.6}
                fill="rgba(239,68,68,0.18)"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </section>

      {/* FIFO Engine */}
      <section className="card">
        <h2 className="mb-3 text-sm font-bold text-slate-700 dark:text-slate-200">
          {t('fifo_engine')}
        </h2>

        {/* FIFO educational expander */}
        <div className="mb-4 rounded-xl border border-slate-200 dark:border-slate-700">
          <button
            onClick={() => setFifoOpen((v) => !v)}
            className="flex w-full items-center gap-2 px-3 py-2.5 text-xs font-medium text-slate-500 hover:bg-slate-50 dark:text-slate-400 dark:hover:bg-slate-800/50 rounded-xl transition-colors"
          >
            <span className={cn('transition-transform text-[10px]', fifoOpen && 'rotate-90')}>›</span>
            <span>{isHe ? 'ℹ מה זה FIFO כאן?' : 'ℹ What does FIFO mean here?'}</span>
          </button>
          {fifoOpen && (
            <div className="border-t border-slate-100 px-4 py-3 text-xs leading-relaxed text-slate-600 dark:border-slate-800 dark:text-slate-300 space-y-2">
              {isHe ? (
                <>
                  <p><strong>FIFO = First-In-First-Out (ראשון-שנכנס-ראשון-שיוצא).</strong> כאשר מוכרים חלק מפוזיציה, המערכת מניחה שהלוטים הוותיקים ביותר נמכרים קודם. זוהי שיטת החשבונאות ברירת-המחדל ברוב מדינות העולם (כולל מס רווחי הון בישראל) כי היא נוטה לייצר את הרווח המוכר הגבוה ביותר.</p>
                  <p><strong>למה חשוב לך:</strong></p>
                  <ol className="list-decimal list-inside space-y-1">
                    <li><strong>רווח ממומש (Realized P/L)</strong> — הרווח שכבר &apos;נעלת&apos; על הלוטים שכבר מכרת. זהו הבסיס לחישוב המס.</li>
                    <li><strong>עלות פתוחה (Open Cost)</strong> — עלות הלוטים שעדיין מוחזקים. השוואה מול השווי הנוכחי נותנת רווח לא-ממומש.</li>
                    <li><strong>מחיר קנייה ממוצע</strong> — ממוצע משוקלל של הלוטים הפתוחים. שימושי לבדיקה מהירה של נקודת איזון (break-even).</li>
                  </ol>
                  <p><strong>דוגמה מעשית:</strong> קנית 10 BTC ב-₪50K, ואז עוד 10 BTC ב-₪100K, ואז מכרת 10 BTC ב-₪150K.</p>
                  <ul className="list-disc list-inside space-y-1">
                    <li>FIFO מוכר את 10 הלוטים הראשונים → רווח ממומש = 10 × (150K − 50K) = ₪1,000K.</li>
                    <li>פוזיציה פתוחה שנותרה: 10 BTC בעלות ₪100K כל אחד (סה&quot;כ עלות פתוחה ₪1,000K).</li>
                  </ul>
                </>
              ) : (
                <>
                  <p><strong>FIFO = First-In-First-Out.</strong> When you sell part of a position, the system assumes you are selling the OLDEST lots first. This is the default accounting method for most tax jurisdictions (including Israel&apos;s capital gains tax) because it tends to produce the largest realised gain — and therefore the highest tax bill — when prices have risen.</p>
                  <p><strong>Why you care:</strong></p>
                  <ol className="list-decimal list-inside space-y-1">
                    <li><strong>Realised P/L</strong> — the profit locked in on the lots you&apos;ve already sold. This is what your tax liability is based on.</li>
                    <li><strong>Open Cost</strong> — the remaining cost basis of lots you still hold. Compare it against current value to see unrealised P/L.</li>
                    <li><strong>Average Buy Price</strong> — weighted average of the still-open lots. Useful for quick break-even checks.</li>
                  </ol>
                  <p><strong>Worked example:</strong> you bought 10 BTC at ₪50K, later 10 more at ₪100K, then sold 10 BTC at ₪150K.</p>
                  <ul className="list-disc list-inside space-y-1">
                    <li>FIFO sells the FIRST 10 lots → realised gain = 10 × (150K − 50K) = ₪1,000K.</li>
                    <li>Remaining open position: 10 BTC at cost ₪100K each (total open cost ₪1,000K).</li>
                  </ul>
                </>
              )}
            </div>
          )}
        </div>

        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead className="border-b border-slate-200 dark:border-slate-700">
              <tr className="text-slate-600 dark:text-slate-400">
                <th className="px-2 py-2 font-semibold">{t('ticker')}</th>
                <th className="px-2 py-2 text-right font-semibold">{t('open_qty')}</th>
                <th className="px-2 py-2 text-right font-semibold">{t('avg_cost')}</th>
                <th className="px-2 py-2 text-right font-semibold">{t('market_value')}</th>
                <th className="px-2 py-2 text-right font-semibold">{t('realized_pnl')}</th>
                <th className="px-2 py-2 text-right font-semibold">{t('unrealized_pnl')}</th>
              </tr>
            </thead>
            <tbody>
              {fifo.map((m) => (
                <tr
                  key={m.ticker}
                  className="border-b border-slate-100 hover:bg-slate-50/50 dark:border-slate-800 dark:hover:bg-slate-800/30"
                >
                  <td className="px-2 py-2 font-mono font-bold">{m.ticker}</td>
                  <td className="px-2 py-2 text-right tabular-nums">
                    {formatNumber(m.openQuantity, locale, 4)}
                  </td>
                  <td className="px-2 py-2 text-right tabular-nums">
                    {formatCurrency(m.averageCost, 'ILS', locale)}
                  </td>
                  <td className="px-2 py-2 text-right tabular-nums">
                    {formatCurrency(m.marketValue, 'ILS', locale)}
                  </td>
                  <td
                    className={`px-2 py-2 text-right font-semibold tabular-nums ${
                      m.realizedPnl >= 0
                        ? 'text-emerald-600 dark:text-emerald-400'
                        : 'text-rose-600 dark:text-rose-400'
                    }`}
                  >
                    {formatCurrency(m.realizedPnl, 'ILS', locale)}
                  </td>
                  <td
                    className={`px-2 py-2 text-right font-semibold tabular-nums ${
                      m.unrealizedPnl >= 0
                        ? 'text-emerald-600 dark:text-emerald-400'
                        : 'text-rose-600 dark:text-rose-400'
                    }`}
                  >
                    {formatCurrency(m.unrealizedPnl, 'ILS', locale)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
