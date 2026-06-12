/**
 * Simulator math — mirrors app.py's three-fund model.
 *
 * Three sub-portfolios are projected independently:
 *   1. Regular taxable portfolio   → 25% capital-gains tax on REAL
 *                                    (inflation-adjusted) profits.
 *   2. Pension fund (קרן פנסיה)    → tax-exempt at withdrawal.
 *   3. Education fund (קרן השתלמות)→ tax-exempt after vesting.
 *
 * Funds (Pension / Education) factor in TWO management-fee types:
 *   - Fee on accumulation: annual % skimmed off the running balance.
 *   - Fee on deposits: % skimmed off each new deposit.
 *
 * All conversions from annual to monthly use geometric compounding:
 *   r_m = (1 + r_annual)^(1/12) - 1
 */

export const ISRAELI_CAPITAL_GAINS_TAX = 0.25;

// ────────────────────────────────────────────────────────────────────
// Regular taxable portfolio (legacy single-fund projection — still used
// for the dashboard chart with the "with-lump / no-lump / contributions"
// triple line.)
// ────────────────────────────────────────────────────────────────────

export interface SimulatorInput {
  initialCapital: number;
  monthlyContribution: number;
  lumpSum: number;
  lumpMonth: number;
  annualReturnPct: number;
  annualInflationPct: number;
  swrPct: number;
  ageNow: number;
  ageTarget: number;
}

export interface ProjectionPoint {
  month: number;
  contributionsOnly: number;
  noLump: number;
  withLump: number;
  realWithLump: number;
}

export interface SimulatorResult {
  monthsTotal: number;
  finalNominal: number;
  finalReal: number;
  totalContributions: number;
  monthlyPensionNominal: number;
  monthlyPensionReal: number;
  series: ProjectionPoint[];
}

function geomMonthlyRate(annualPct: number): number {
  const r = annualPct / 100;
  if (1 + r <= 0) return -0.999999;
  return Math.pow(1 + r, 1 / 12) - 1;
}

export function runProjection(input: SimulatorInput): SimulatorResult {
  const yearsTotal = Math.max(1, input.ageTarget - input.ageNow);
  const monthsTotal = Math.round(yearsTotal * 12);
  const monthlyRate = geomMonthlyRate(input.annualReturnPct);
  const monthlyInflation = geomMonthlyRate(input.annualInflationPct);

  const series: ProjectionPoint[] = [];
  let contribOnly = input.initialCapital;
  let noLump = input.initialCapital;
  let withLump = input.initialCapital + (input.lumpMonth === 0 ? input.lumpSum : 0);
  series.push({
    month: 0,
    contributionsOnly: contribOnly,
    noLump,
    withLump,
    realWithLump: withLump,
  });
  for (let m = 1; m <= monthsTotal; m += 1) {
    contribOnly += input.monthlyContribution;
    noLump = noLump * (1 + monthlyRate) + input.monthlyContribution;
    withLump = withLump * (1 + monthlyRate) + input.monthlyContribution;
    if (m === input.lumpMonth) withLump += input.lumpSum;
    const realWithLump = withLump / Math.pow(1 + monthlyInflation, m);
    series.push({ month: m, contributionsOnly: contribOnly, noLump, withLump, realWithLump });
  }
  const finalNominal = withLump;
  const finalReal = finalNominal / Math.pow(1 + monthlyInflation, monthsTotal);
  const totalContributions =
    input.initialCapital + input.monthlyContribution * monthsTotal + input.lumpSum;
  const monthlyPensionNominal = (finalNominal * (input.swrPct / 100)) / 12;
  const monthlyPensionReal = (finalReal * (input.swrPct / 100)) / 12;
  return {
    monthsTotal,
    finalNominal,
    finalReal,
    totalContributions,
    monthlyPensionNominal,
    monthlyPensionReal,
    series,
  };
}

// ────────────────────────────────────────────────────────────────────
// Tax-exempt fund (Pension / Education) — with management fees.
// ────────────────────────────────────────────────────────────────────

export interface FundInput {
  initial: number;
  monthlyContribution: number;
  annualReturnPct: number;
  feeAccumulationPct: number;
  feeDepositsPct: number;
  years: number;
}

export interface FundProjection {
  finalBalance: number;
  totalContributions: number;
  series: { month: number; balance: number; contributions: number }[];
}

export function projectFund(input: FundInput): FundProjection {
  const initial = Math.max(0, input.initial || 0);
  const contrib = Math.max(0, input.monthlyContribution || 0);
  const years = Math.max(0, input.years || 0);
  const feeAcc = Math.max(0, input.feeAccumulationPct || 0);
  const feeDep = Math.max(0, input.feeDepositsPct || 0);

  if (years <= 0) {
    return {
      finalBalance: initial,
      totalContributions: initial,
      series: [{ month: 0, balance: initial, contributions: initial }],
    };
  }
  const netAnnual = input.annualReturnPct - feeAcc;
  const r_m = geomMonthlyRate(netAnnual);
  const effectiveMonthly = contrib * (1 - feeDep / 100);
  const n = Math.max(1, Math.round(years * 12));
  let balance = initial;
  let contribs = initial;
  const series = [{ month: 0, balance, contributions: contribs }];
  for (let m = 1; m <= n; m += 1) {
    balance = balance * (1 + r_m) + effectiveMonthly;
    contribs += contrib;
    series.push({ month: m, balance, contributions: contribs });
  }
  return { finalBalance: balance, totalContributions: contribs, series };
}

// ────────────────────────────────────────────────────────────────────
// Real (inflation-adjusted) capital-gains tax — Israeli 25% rule.
// ────────────────────────────────────────────────────────────────────

export function realCapitalGainsTax(
  finalNominal: number,
  initial: number,
  monthly: number,
  months: number,
  lump: number,
  lumpMonth: number,
  inflationPct: number,
  years: number,
): number {
  // Mirrors app.py._real_capital_gains_tax EXACTLY: each contribution is
  // indexed only from its OWN deposit date — initial gets the full horizon,
  // monthly deposit m gets (months-m)/12 years, the lump gets
  // (months-lumpMonth)/12 years. The previous version indexed the whole
  // basis by the full horizon, overstating it and under-charging tax for
  // contribution-heavy plans.
  if (finalNominal <= 0 || years <= 0) return 0;
  const i = Math.max(0, inflationPct) / 100;
  const m = Math.max(0, Math.round(months));
  let indexedBasis = Math.max(0, initial) * Math.pow(1 + i, years);
  if (monthly && m > 0) {
    for (let k = 1; k <= m; k += 1) {
      indexedBasis += monthly * Math.pow(1 + i, (m - k) / 12);
    }
  }
  if (lump) {
    const lm = Math.max(0, Math.min(Math.round(lumpMonth), m));
    indexedBasis += Math.max(0, lump) * Math.pow(1 + i, (m - lm) / 12);
  }
  const realGain = Math.max(0, finalNominal - indexedBasis);
  return ISRAELI_CAPITAL_GAINS_TAX * realGain;
}

// ────────────────────────────────────────────────────────────────────
// Three-fund full simulation — what the simulator page renders.
// ────────────────────────────────────────────────────────────────────

export interface FullSimInput {
  ageNow: number;
  ageTarget: number;
  swrPct: number;
  annualInflationPct: number;
  // Regular taxable portfolio
  regInitial: number;
  regMonthly: number;
  regReturnPct: number;
  regLumpSum: number;
  regLumpMonth: number;
  // Pension
  pensionInitial: number;
  pensionMonthly: number;
  pensionReturnPct: number;
  pensionFeeAccumulationPct: number;
  pensionFeeDepositsPct: number;
  // Education
  eduInitial: number;
  eduMonthly: number;
  eduReturnPct: number;
  eduFeeAccumulationPct: number;
  eduFeeDepositsPct: number;
}

export interface FullSimResult {
  yearsTotal: number;
  monthsTotal: number;

  regular: {
    finalGross: number;
    finalNet: number;
    tax: number;
    totalContributions: number;
    series: ProjectionPoint[];
  };
  pension: FundProjection;
  education: FundProjection;

  totalNominalNet: number;
  totalRealNet: number;
  totalContributions: number;
  monthlyPensionNet: number;
  annualPensionNet: number;

  // Combined chart data — one row per year-mark for area/line plots.
  chart: Array<{
    year: number;
    regular: number;
    pension: number;
    education: number;
    total: number;
    contributions: number;
  }>;
}

export function runFullSimulation(input: FullSimInput): FullSimResult {
  const yearsTotal = Math.max(0, input.ageTarget - input.ageNow);
  const monthsTotal = Math.round(yearsTotal * 12);
  const monthlyInflation = geomMonthlyRate(input.annualInflationPct);

  // Regular portfolio: reuse the three-line projection we already had so the
  // chart can show "with lump / no lump / contributions" semantics.
  const regular = runProjection({
    initialCapital: input.regInitial,
    monthlyContribution: input.regMonthly,
    lumpSum: input.regLumpSum,
    lumpMonth: input.regLumpMonth,
    annualReturnPct: input.regReturnPct,
    annualInflationPct: input.annualInflationPct,
    swrPct: input.swrPct,
    ageNow: input.ageNow,
    ageTarget: input.ageTarget,
  });
  const regContributions =
    input.regInitial + input.regMonthly * monthsTotal + input.regLumpSum;
  const regTax = realCapitalGainsTax(
    regular.finalNominal,
    input.regInitial,
    input.regMonthly,
    monthsTotal,
    input.regLumpSum,
    input.regLumpMonth,
    input.annualInflationPct,
    yearsTotal,
  );
  const regNet = Math.max(0, regular.finalNominal - regTax);

  const pension = projectFund({
    initial: input.pensionInitial,
    monthlyContribution: input.pensionMonthly,
    annualReturnPct: input.pensionReturnPct,
    feeAccumulationPct: input.pensionFeeAccumulationPct,
    feeDepositsPct: input.pensionFeeDepositsPct,
    years: yearsTotal,
  });

  const education = projectFund({
    initial: input.eduInitial,
    monthlyContribution: input.eduMonthly,
    annualReturnPct: input.eduReturnPct,
    feeAccumulationPct: input.eduFeeAccumulationPct,
    feeDepositsPct: input.eduFeeDepositsPct,
    years: yearsTotal,
  });

  const totalNominalNet = regNet + pension.finalBalance + education.finalBalance;
  const totalRealNet = totalNominalNet / Math.pow(1 + monthlyInflation, monthsTotal);
  const totalContributions = regContributions + pension.totalContributions + education.totalContributions;
  const monthlyPensionNet = (totalNominalNet * (input.swrPct / 100)) / 12;
  const annualPensionNet = monthlyPensionNet * 12;

  // Chart data — sample every 12 months (yearly granularity) so legend stays clean.
  const chart: FullSimResult['chart'] = [];
  for (let y = 0; y <= yearsTotal; y += 1) {
    const m = y * 12;
    const regAt = regular.series[m]?.withLump ?? regular.series[regular.series.length - 1]!.withLump;
    const penAt = pension.series[m]?.balance ?? pension.series[pension.series.length - 1]!.balance;
    const eduAt =
      education.series[m]?.balance ?? education.series[education.series.length - 1]!.balance;
    const contribAt =
      (regular.series[m]?.contributionsOnly ?? regular.series[regular.series.length - 1]!.contributionsOnly) +
      (pension.series[m]?.contributions ?? pension.series[pension.series.length - 1]!.contributions) +
      (education.series[m]?.contributions ?? education.series[education.series.length - 1]!.contributions);
    chart.push({
      year: y,
      regular: regAt,
      pension: penAt,
      education: eduAt,
      total: regAt + penAt + eduAt,
      contributions: contribAt,
    });
  }

  return {
    yearsTotal,
    monthsTotal,
    regular: {
      finalGross: regular.finalNominal,
      finalNet: regNet,
      tax: regTax,
      totalContributions: regContributions,
      series: regular.series,
    },
    pension,
    education,
    totalNominalNet,
    totalRealNet,
    totalContributions,
    monthlyPensionNet,
    annualPensionNet,
    chart,
  };
}

// ────────────────────────────────────────────────────────────────────
// Monte-Carlo (unchanged) — exported for future use.
// ────────────────────────────────────────────────────────────────────

export interface MonteCarloPoint {
  year: number;
  median: number;
  p10: number;
  p90: number;
}

export function monteCarlo(
  input: SimulatorInput,
  paths = 200,
  annualVolPct = 15,
): MonteCarloPoint[] {
  const yearsTotal = Math.max(1, input.ageTarget - input.ageNow);
  const monthlyMu = geomMonthlyRate(input.annualReturnPct);
  const monthlySigma = annualVolPct / 100 / Math.sqrt(12);

  const yearly: number[][] = Array.from({ length: yearsTotal + 1 }, () => []);
  for (let p = 0; p < paths; p += 1) {
    let value = input.initialCapital;
    yearly[0]!.push(value);
    for (let y = 1; y <= yearsTotal; y += 1) {
      for (let m = 0; m < 12; m += 1) {
        const u1 = Math.max(1e-9, Math.random());
        const u2 = Math.random();
        const z = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
        const r = monthlyMu + monthlySigma * z;
        value = value * (1 + r) + input.monthlyContribution;
      }
      yearly[y]!.push(value);
    }
  }
  const percentile = (arr: number[], q: number): number => {
    const sorted = [...arr].sort((a, b) => a - b);
    const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor(q * sorted.length)));
    return sorted[idx] ?? 0;
  };
  return yearly.map((vals, year) => ({
    year,
    median: percentile(vals, 0.5),
    p10: percentile(vals, 0.1),
    p90: percentile(vals, 0.9),
  }));
}
