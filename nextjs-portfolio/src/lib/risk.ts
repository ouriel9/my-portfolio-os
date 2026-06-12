/** Pure risk math — mirrors app.py risk helpers (HHI, VaR, CVaR, drawdown). */
import type { FifoLot, FifoMetric, RiskMetrics, Trade } from './types';

export function herfindahlIndex(weights: number[]): number {
  return weights.reduce((s, w) => s + w * w, 0);
}

export function historicalVar(returns: number[], alpha = 0.95): number {
  if (returns.length === 0) return 0;
  const sorted = [...returns].sort((a, b) => a - b);
  const idx = Math.max(0, Math.floor((1 - alpha) * sorted.length));
  return sorted[idx] ?? 0;
}

export function historicalCvar(returns: number[], alpha = 0.95): number {
  if (returns.length === 0) return 0;
  const sorted = [...returns].sort((a, b) => a - b);
  const cutoff = Math.max(1, Math.floor((1 - alpha) * sorted.length));
  const tail = sorted.slice(0, cutoff);
  const mean = tail.reduce((s, x) => s + x, 0) / tail.length;
  return mean;
}

export function maxDrawdown(values: number[]): number {
  if (values.length === 0) return 0;
  let peak = values[0] ?? 0;
  let mdd = 0;
  for (const v of values) {
    if (v > peak) peak = v;
    const dd = peak === 0 ? 0 : (v - peak) / peak;
    if (dd < mdd) mdd = dd;
  }
  return mdd;
}

function mean(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((s, x) => s + x, 0) / values.length;
}

function stddev(values: number[]): number {
  if (values.length < 2) return 0;
  const m = mean(values);
  const variance = values.reduce((s, x) => s + (x - m) ** 2, 0) / (values.length - 1);
  return Math.sqrt(variance);
}

export function sharpeRatio(returns: number[], rfAnnual = 0.02, periods = 252): number {
  const sd = stddev(returns);
  if (sd === 0) return 0;
  const rfPeriod = rfAnnual / periods;
  return ((mean(returns) - rfPeriod) / sd) * Math.sqrt(periods);
}

export function sortinoRatio(returns: number[], rfAnnual = 0.02, periods = 252): number {
  const downside = returns.filter((r) => r < 0);
  const sd = stddev(downside);
  if (sd === 0) return 0;
  const rfPeriod = rfAnnual / periods;
  return ((mean(returns) - rfPeriod) / sd) * Math.sqrt(periods);
}

export function calmarRatio(returns: number[], periods = 252): number {
  if (returns.length === 0) return 0;
  const cumulative = returns.reduce<number[]>((acc, r) => {
    const last = acc[acc.length - 1] ?? 1;
    acc.push(last * (1 + r));
    return acc;
  }, []);
  const mdd = Math.abs(maxDrawdown(cumulative));
  if (mdd === 0) return 0;
  const annualReturn = mean(returns) * periods;
  return annualReturn / mdd;
}

export function computeRisk(weights: number[], dailyReturns: number[]): RiskMetrics {
  return {
    hhi: herfindahlIndex(weights),
    var95: historicalVar(dailyReturns, 0.95),
    cvar95: historicalCvar(dailyReturns, 0.95),
    maxDrawdown: maxDrawdown(
      dailyReturns.reduce<number[]>((acc, r) => {
        const last = acc[acc.length - 1] ?? 1;
        acc.push(last * (1 + r));
        return acc;
      }, []),
    ),
    sharpe: sharpeRatio(dailyReturns),
    sortino: sortinoRatio(dailyReturns),
    calmar: calmarRatio(dailyReturns),
    volatility: stddev(dailyReturns) * Math.sqrt(252),
  };
}

/** Synthesize a daily-returns series from the snapshot's history points. */
export function dailyReturnsFromHistory(history: { date: string; value: number }[]): number[] {
  const out: number[] = [];
  for (let i = 1; i < history.length; i += 1) {
    const prev = history[i - 1]!.value;
    const cur = history[i]!.value;
    if (prev > 0) out.push((cur - prev) / prev);
  }
  return out;
}

/** FIFO P&L per ticker. Mirrors app.py fifo_metrics. */
export function fifoMetrics(trades: Trade[]): FifoMetric[] {
  const byTicker = new Map<string, Trade[]>();
  for (const t of trades) {
    if (!t.ticker) continue;
    const arr = byTicker.get(t.ticker) ?? [];
    arr.push(t);
    byTicker.set(t.ticker, arr);
  }
  const out: FifoMetric[] = [];
  for (const [ticker, list] of byTicker.entries()) {
    const sorted = [...list].sort((a, b) => a.purchaseDate.localeCompare(b.purchaseDate));
    const lots: FifoLot[] = [];
    let realized = 0;
    for (const trade of sorted) {
      const isClosed = ['סגור', 'closed', 'close', 'sold', 'נמכר'].includes(
        trade.status.toLowerCase().trim(),
      );
      if (!isClosed) {
        lots.push({
          date: trade.purchaseDate,
          quantity: trade.quantity,
          unitCost: trade.quantity > 0 ? trade.costIls / trade.quantity : 0,
        });
        continue;
      }
      // Closed lot: realize against earliest open lots.
      let qtyToSell = trade.quantity;
      const proceedsPerUnit = trade.quantity > 0 ? trade.valueIls / trade.quantity : 0;
      while (qtyToSell > 0 && lots.length > 0) {
        const head = lots[0]!;
        const take = Math.min(head.quantity, qtyToSell);
        realized += take * (proceedsPerUnit - head.unitCost);
        head.quantity -= take;
        qtyToSell -= take;
        if (head.quantity <= 1e-9) lots.shift();
      }
      // Single-row closed trades carry both buy cost and proceeds with no
      // separate open lot; realize the unmatched qty against this row's own
      // cost basis. Without this, realizedPnl was always 0 when closed.
      if (qtyToSell > 1e-9) {
        const ownUnitCost = trade.quantity > 0 ? trade.costIls / trade.quantity : 0;
        realized += qtyToSell * (proceedsPerUnit - ownUnitCost);
      }
    }
    const openQty = lots.reduce((s, l) => s + l.quantity, 0);
    const openCost = lots.reduce((s, l) => s + l.quantity * l.unitCost, 0);
    const avg = openQty > 0 ? openCost / openQty : 0;
    const lastTrade = sorted[sorted.length - 1]!;
    const marketValue = openQty * (lastTrade.spotIls || avg);
    out.push({
      ticker,
      openQuantity: openQty,
      realizedPnl: realized,
      averageCost: avg,
      marketValue,
      unrealizedPnl: marketValue - openCost,
    });
  }
  return out.sort((a, b) => b.marketValue - a.marketValue);
}
