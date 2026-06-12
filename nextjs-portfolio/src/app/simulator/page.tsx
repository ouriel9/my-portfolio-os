'use client';

import { useMemo, useState } from 'react';
import { useApp } from '@/components/providers/AppProvider';
import { useSnapshot } from '@/components/hooks/useSnapshot';
import { KpiTile } from '@/components/ui/KpiTile';
import { PageHeader } from '@/components/ui/PageHeader';
import { LoadingBlock, ErrorBlock } from '@/components/ui/StateBanners';
import { SimSection, SimExpander } from '@/components/ui/SimSection';
import { formatCurrency } from '@/lib/format';
import { runFullSimulation } from '@/lib/simulator';
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  Legend,
  Area,
  ComposedChart,
} from 'recharts';

export default function SimulatorPage() {
  const { t, locale, chartLock } = useApp();
  const { views, error, isLoading } = useSnapshot();

  // ── Mode + global controls ─────────────────────────────────────
  const [mode, setMode] = useState<'mine' | 'clean'>('clean');
  const [ageNow, setAgeNow] = useState(30);
  const [ageTarget, setAgeTarget] = useState(67);
  const [swr, setSwr] = useState(4);
  const [inflation, setInflation] = useState(3.0); // parity: app.py annual_inflation=3.0

  // ── Regular taxable portfolio ─────────────────────────────────
  const [regInitial, setRegInitial] = useState(0);
  const [regMonthly, setRegMonthly] = useState(2000);
  const [regReturn, setRegReturn] = useState(7);
  const [regLumpSum, setRegLumpSum] = useState(0);
  const [regLumpMonth, setRegLumpMonth] = useState(0);

  // ── Pension fund ──────────────────────────────────────────────
  const [pensionInitial, setPensionInitial] = useState(0);
  const [pensionMonthly, setPensionMonthly] = useState(1500);
  const [pensionReturn, setPensionReturn] = useState(6);
  const [pensionFeeAcc, setPensionFeeAcc] = useState(0.5);
  const [pensionFeeDep, setPensionFeeDep] = useState(1);

  // ── Education fund ────────────────────────────────────────────
  const [eduInitial, setEduInitial] = useState(0);
  const [eduMonthly, setEduMonthly] = useState(800); // parity: app.py education_monthly=800
  const [eduReturn, setEduReturn] = useState(6); // parity: app.py education_return=6.0
  const [eduFeeAcc, setEduFeeAcc] = useState(0.5);
  const [eduFeeDep, setEduFeeDep] = useState(0.5);

  const liveValue = views?.totalValueIls ?? 0;
  const effectiveRegInitial = mode === 'mine' && liveValue > 0 ? liveValue : regInitial;
  const monthsTotal = Math.max(0, ageTarget - ageNow) * 12;
  const safeLumpMonth = Math.min(Math.max(0, regLumpMonth), monthsTotal);

  const result = useMemo(
    () =>
      runFullSimulation({
        ageNow,
        ageTarget,
        swrPct: swr,
        annualInflationPct: inflation,
        regInitial: effectiveRegInitial,
        regMonthly,
        regReturnPct: regReturn,
        regLumpSum,
        regLumpMonth: safeLumpMonth,
        pensionInitial,
        pensionMonthly,
        pensionReturnPct: pensionReturn,
        pensionFeeAccumulationPct: pensionFeeAcc,
        pensionFeeDepositsPct: pensionFeeDep,
        eduInitial,
        eduMonthly,
        eduReturnPct: eduReturn,
        eduFeeAccumulationPct: eduFeeAcc,
        eduFeeDepositsPct: eduFeeDep,
      }),
    [
      ageNow,
      ageTarget,
      swr,
      inflation,
      effectiveRegInitial,
      regMonthly,
      regReturn,
      regLumpSum,
      safeLumpMonth,
      pensionInitial,
      pensionMonthly,
      pensionReturn,
      pensionFeeAcc,
      pensionFeeDep,
      eduInitial,
      eduMonthly,
      eduReturn,
      eduFeeAcc,
      eduFeeDep,
    ],
  );

  if (isLoading) return <LoadingBlock label={t('loading')} />;
  if (error) return <ErrorBlock message={error} />;

  // SWR per-bucket monthly pension (NET) = bucketBalance * swr/100 / 12
  const swrMonthly = (b: number) => (b * (swr / 100)) / 12;

  return (
    <div className="space-y-5">
      <PageHeader title={t('nav_simulator')} accent="amber" />

      {/* Mode selector */}
      <div className="card border-l-4 border-l-amber-400/80">
        <div className="mb-3 text-sm font-bold text-slate-700 dark:text-slate-200">
          {locale === 'he' ? 'מצב' : 'Mode'}
        </div>
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={() => setMode('clean')}
            className={`rounded-lg px-4 py-2 text-sm font-semibold transition-all ${
              mode === 'clean'
                ? 'bg-amber-500 text-white shadow-soft'
                : 'bg-slate-100 text-slate-700 hover:bg-slate-200 dark:bg-slate-800 dark:text-slate-300'
            }`}
          >
            🧼 {t('sim_mode_clean')}
          </button>
          {liveValue > 0 && (
            <button
              type="button"
              onClick={() => setMode('mine')}
              className={`rounded-lg px-4 py-2 text-sm font-semibold transition-all ${
                mode === 'mine'
                  ? 'bg-amber-500 text-white shadow-soft'
                  : 'bg-slate-100 text-slate-700 hover:bg-slate-200 dark:bg-slate-800 dark:text-slate-300'
              }`}
            >
              💼 {t('sim_mode_my_portfolio')}
            </button>
          )}
        </div>
        {mode === 'mine' && (
          <div className="mt-2 rounded-md bg-amber-50 px-3 py-2 text-xs text-amber-800 dark:bg-amber-950/30 dark:text-amber-300">
            {t('sim_mode_seeded_from')} {formatCurrency(liveValue, 'ILS', locale)}
          </div>
        )}
      </div>

      {/* Section: Horizon & Time */}
      <SimSection title={t('sim_section_horizon')} icon="🕒">
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <NumField
            label={t('age_now')}
            value={ageNow}
            onChange={(v) => setAgeNow(Math.max(10, Math.min(90, v)))}
            step={1}
          />
          <NumField
            label={t('age_target')}
            value={ageTarget}
            onChange={(v) => setAgeTarget(Math.max(ageNow + 1, Math.min(100, v)))}
            step={1}
          />
          <NumField
            label={t('swr')}
            value={swr}
            onChange={(v) => setSwr(Math.max(3, Math.min(5, v)))}
            step={0.25}
          />
          <NumField
            label={t('inflation')}
            value={inflation}
            onChange={(v) => setInflation(Math.max(0, Math.min(20, v)))}
            step={0.25}
          />
        </div>
      </SimSection>

      {/* Section: Regular taxable portfolio */}
      <SimSection title={t('sim_section_regular')} icon="📈">
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <NumField
            label={t('initial_capital')}
            value={effectiveRegInitial}
            onChange={mode === 'mine' ? () => undefined : setRegInitial}
            disabled={mode === 'mine'}
            step={1000}
          />
          <NumField
            label={t('expected_return')}
            value={regReturn}
            onChange={(v) => setRegReturn(Math.max(-20, Math.min(40, v)))}
            step={0.25}
          />
          <NumField
            label={t('monthly_contribution')}
            value={regMonthly}
            onChange={setRegMonthly}
            step={100}
          />
          <NumField label={t('lump_sum')} value={regLumpSum} onChange={setRegLumpSum} step={1000} />
          <NumField
            label={t('lump_month')}
            value={regLumpMonth}
            onChange={(v) => setRegLumpMonth(Math.min(monthsTotal, Math.max(0, v)))}
            step={1}
          />
        </div>
      </SimSection>

      {/* Section: Pension fund (collapsed) */}
      <SimExpander title={t('sim_section_pension')} icon="🏦">
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <NumField
            label={t('sim_fund_initial')}
            value={pensionInitial}
            onChange={setPensionInitial}
            step={1000}
          />
          <NumField
            label={t('sim_fund_return')}
            value={pensionReturn}
            onChange={setPensionReturn}
            step={0.25}
          />
          <NumField
            label={t('sim_fund_monthly')}
            value={pensionMonthly}
            onChange={setPensionMonthly}
            step={100}
          />
          <NumField
            label={t('sim_fee_accumulation')}
            value={pensionFeeAcc}
            onChange={(v) => setPensionFeeAcc(Math.max(0, Math.min(5, v)))}
            step={0.05}
          />
          <NumField
            label={t('sim_fee_deposits')}
            value={pensionFeeDep}
            onChange={(v) => setPensionFeeDep(Math.max(0, Math.min(10, v)))}
            step={0.05}
          />
        </div>
      </SimExpander>

      {/* Section: Education fund (collapsed) */}
      <SimExpander title={t('sim_section_education')} icon="🎓">
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <NumField
            label={t('sim_fund_initial')}
            value={eduInitial}
            onChange={setEduInitial}
            step={1000}
          />
          <NumField
            label={t('sim_fund_return')}
            value={eduReturn}
            onChange={setEduReturn}
            step={0.25}
          />
          <NumField
            label={t('sim_fund_monthly')}
            value={eduMonthly}
            onChange={setEduMonthly}
            step={100}
          />
          <NumField
            label={t('sim_fee_accumulation')}
            value={eduFeeAcc}
            onChange={(v) => setEduFeeAcc(Math.max(0, Math.min(5, v)))}
            step={0.05}
          />
          <NumField
            label={t('sim_fee_deposits')}
            value={eduFeeDep}
            onChange={(v) => setEduFeeDep(Math.max(0, Math.min(10, v)))}
            step={0.05}
          />
        </div>
      </SimExpander>

      {/* Output KPIs (3 main + annual) */}
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <KpiTile
          label={t('sim_kpi_total_net')}
          value={formatCurrency(result.totalNominalNet, 'ILS', locale)}
          delta={`= ${formatCurrency(result.totalContributions, 'ILS', locale)} ${locale === 'he' ? 'הופקדו' : 'contributed'}`}
          accent="amber"
        />
        <KpiTile
          label={t('sim_kpi_real')}
          value={formatCurrency(result.totalRealNet, 'ILS', locale)}
          delta={`${inflation}% ${locale === 'he' ? 'אינפלציה שנתית' : 'annual inflation'}`}
          accent="amber"
        />
        <KpiTile
          label={t('sim_kpi_monthly_pension')}
          value={formatCurrency(result.monthlyPensionNet, 'ILS', locale)}
          delta={`${swr}% SWR`}
          accent="amber"
        />
        <KpiTile
          label={t('sim_kpi_annual_pension')}
          value={formatCurrency(result.annualPensionNet, 'ILS', locale)}
          accent="amber"
        />
      </div>

      {/* Breakdown table */}
      <section className="card border-l-4 border-l-amber-400/80">
        <h2 className="mb-3 text-sm font-bold text-slate-700 dark:text-slate-200">
          {t('sim_breakdown')}
        </h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
              <tr>
                <th className="px-3 py-2 text-start font-semibold">{t('sim_component')}</th>
                <th className="px-3 py-2 text-end font-semibold">{t('sim_final_balance_net')}</th>
                <th className="px-3 py-2 text-end font-semibold">{t('sim_monthly_pension_swr')}</th>
              </tr>
            </thead>
            <tbody className="text-slate-700 dark:text-slate-300">
              <BreakdownRow
                label={`📈 ${locale === 'he' ? 'תיק רגיל (אחרי מס)' : 'Regular (after 25% tax on real gains)'}`}
                balance={result.regular.finalNet}
                monthly={swrMonthly(result.regular.finalNet)}
                locale={locale}
              />
              <BreakdownRow
                label={`🏦 ${locale === 'he' ? 'קרן פנסיה' : 'Pension fund'}`}
                balance={result.pension.finalBalance}
                monthly={swrMonthly(result.pension.finalBalance)}
                locale={locale}
              />
              <BreakdownRow
                label={`🎓 ${locale === 'he' ? 'קרן השתלמות' : 'Education fund'}`}
                balance={result.education.finalBalance}
                monthly={swrMonthly(result.education.finalBalance)}
                locale={locale}
              />
              <tr className="border-t-2 border-amber-300/60 bg-amber-50/60 font-bold dark:border-amber-600/40 dark:bg-amber-950/20">
                <td className="px-3 py-2.5">{t('sim_total_label')}</td>
                <td className="px-3 py-2.5 text-end tabular-nums" data-ltr>
                  {formatCurrency(result.totalNominalNet, 'ILS', locale)}
                </td>
                <td className="px-3 py-2.5 text-end tabular-nums" data-ltr>
                  {formatCurrency(result.monthlyPensionNet, 'ILS', locale)}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      {/* Growth chart — stacked area per fund */}
      <section className="card border-l-4 border-l-amber-400/80">
        <h2 className="mb-3 text-sm font-bold text-slate-700 dark:text-slate-200">
          {t('sim_chart_growth')}
        </h2>
        <div className="h-80 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={result.chart}>
              <CartesianGrid stroke="rgba(148,163,184,0.18)" strokeDasharray="3 3" />
              <XAxis
                dataKey="year"
                tick={{ fontSize: 11 }}
                minTickGap={20}
                label={{
                  value: locale === 'he' ? 'שנים מעכשיו' : 'Years',
                  position: 'insideBottom',
                  offset: -2,
                  style: { fontSize: 11, fill: '#94a3b8' },
                }}
              />
              <YAxis
                tick={{ fontSize: 11 }}
                tickFormatter={(v: number) => `₪${(v / 1000).toFixed(0)}k`}
              />
              {!chartLock && (
                <Tooltip
                  formatter={(value: number) => formatCurrency(value, 'ILS', locale)}
                  contentStyle={{ fontSize: 12, borderRadius: 8 }}
                />
              )}
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Area
                type="monotone"
                dataKey="regular"
                name={`📈 ${locale === 'he' ? 'תיק רגיל' : 'Regular'}`}
                stroke="#f59e0b"
                fill="rgba(245,158,11,0.18)"
                strokeWidth={2}
              />
              <Line
                type="monotone"
                dataKey="pension"
                name={`🏦 ${locale === 'he' ? 'פנסיה' : 'Pension'}`}
                stroke="#10b981"
                strokeWidth={2}
                strokeDasharray="5 4"
                dot={false}
              />
              <Line
                type="monotone"
                dataKey="education"
                name={`🎓 ${locale === 'he' ? 'השתלמות' : 'Education'}`}
                stroke="#06b6d4"
                strokeWidth={2}
                strokeDasharray="3 3"
                dot={false}
              />
              <Line
                type="monotone"
                dataKey="total"
                name={`= ${locale === 'he' ? 'סה"כ' : 'Total'}`}
                stroke="#6366f1"
                strokeWidth={2.5}
                dot={false}
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </section>

      {/* Annual contribution vs growth */}
      <section className="card border-l-4 border-l-amber-400/80">
        <h2 className="mb-3 text-sm font-bold text-slate-700 dark:text-slate-200">
          {t('sim_chart_breakdown')}
        </h2>
        <div className="h-72 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={result.chart}>
              <CartesianGrid stroke="rgba(148,163,184,0.18)" strokeDasharray="3 3" />
              <XAxis dataKey="year" tick={{ fontSize: 11 }} minTickGap={20} />
              <YAxis
                tick={{ fontSize: 11 }}
                tickFormatter={(v: number) => `₪${(v / 1000).toFixed(0)}k`}
              />
              {!chartLock && (
                <Tooltip
                  formatter={(value: number) => formatCurrency(value, 'ILS', locale)}
                  contentStyle={{ fontSize: 12, borderRadius: 8 }}
                />
              )}
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Line
                type="monotone"
                dataKey="contributions"
                name={locale === 'he' ? 'הפקדות מצטברות' : 'Cumulative contributions'}
                stroke="#94a3b8"
                strokeWidth={1.6}
                strokeDasharray="3 3"
                dot={false}
              />
              <Line
                type="monotone"
                dataKey="total"
                name={locale === 'he' ? 'שווי כולל' : 'Total value'}
                stroke="#f59e0b"
                strokeWidth={2.5}
                dot={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </section>

      <p className="rounded-md bg-amber-50/60 px-3 py-2 text-xs leading-relaxed text-amber-900 dark:bg-amber-950/30 dark:text-amber-200">
        ⚠ {t('sim_disclaimer')}
      </p>
    </div>
  );
}

function NumField({
  label,
  value,
  onChange,
  step,
  disabled,
}: {
  label: string;
  value: number;
  onChange: (v: number) => void;
  step: number;
  disabled?: boolean;
}) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-xs font-semibold text-slate-600 dark:text-slate-400">{label}</span>
      <input
        type="number"
        value={value}
        onChange={(e) => onChange(Number(e.target.value) || 0)}
        step={step}
        disabled={disabled}
        className="h-10 rounded-lg border border-slate-200 bg-white px-3 text-sm tabular-nums focus:border-amber-400 focus:outline-none focus:ring-2 focus:ring-amber-200 disabled:opacity-60 dark:border-slate-700 dark:bg-slate-900 dark:focus:ring-amber-800"
      />
    </label>
  );
}

function BreakdownRow({
  label,
  balance,
  monthly,
  locale,
}: {
  label: string;
  balance: number;
  monthly: number;
  locale: 'he' | 'en';
}) {
  return (
    <tr className="border-t border-slate-100 dark:border-slate-800">
      <td className="px-3 py-2.5">{label}</td>
      <td className="px-3 py-2.5 text-end tabular-nums" data-ltr>
        {formatCurrency(balance, 'ILS', locale)}
      </td>
      <td className="px-3 py-2.5 text-end tabular-nums" data-ltr>
        {formatCurrency(monthly, 'ILS', locale)}
      </td>
    </tr>
  );
}
