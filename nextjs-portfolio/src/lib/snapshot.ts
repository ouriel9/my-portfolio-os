/**
 * Pure helpers that turn the raw Apps Script snapshot payload into the
 * normalized `Trade` model and the `CoreViews` aggregations consumed by the
 * UI. These functions mirror app.py:_normalize_snapshot_df / prepare_core_views.
 */
import type {
  AggregateRow,
  CoreViews,
  HistoryPoint,
  Trade,
} from './types';

const FIELD_ALIASES: Record<keyof Trade, string[]> = {
  tradeId: ['Trade_ID'],
  ticker: ['טיקר', 'Ticker'],
  type: ['סוג נכס', 'Type'],
  platform: ['פלטפורמה', 'Platform'],
  currentLocation: ['מיקום נוכחי', 'Current_Location'],
  purchaseDate: ['תאריך רכישה', 'Purchase_Date'],
  sellDate: ['תאריך מכירה', 'Sell_Date'],
  quantity: ['כמות', 'Quantity'],
  buyPrice: ['שער קנייה', 'Origin_Buy_Price'],
  costOrigin: ['עלות כוללת', 'Cost_Origin'],
  currency: ['מטבע', 'Origin_Currency'],
  commission: ['עמלה', 'Commission'],
  status: ['סטטוס', 'Status'],
  spotUsd: ['שער נוכחי USD'],
  spotIls: ['שער נוכחי ILS'],
  buyUsd: ['שער קנייה USD'],
  buyIls: ['שער קנייה ILS'],
  costUsd: ['עלות USD'],
  costIls: ['עלות ILS'],
  valueUsd: ['שווי USD', 'שווי נוכחי USD'],
  valueIls: ['שווי ILS'],
  sellPrice: ['שער מכירה', 'Sell_Price_Origin'],
  yieldAtSale: ['תשואה במכירה', 'Yield_At_Sale'],
  yieldOrigin: ['תשואה מקור'],
  yieldIls: ['תשואה שקלית'],
};

function clean(value: unknown): string {
  if (value === null || value === undefined) return '';
  return String(value).replace(/[‎‏]/g, '').trim();
}

function toNumber(value: unknown): number {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const s = clean(value)
    .replace(/₪|\$/g, '')
    .replace(/,/g, '')
    .replace(/%/g, '')
    .replace(/\s/g, '');
  if (!s) return 0;
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

function buildIndex(headers: string[]): Record<keyof Trade, number> {
  const cleaned = headers.map(clean);
  const out: Partial<Record<keyof Trade, number>> = {};
  (Object.keys(FIELD_ALIASES) as Array<keyof Trade>).forEach((key) => {
    const aliases = FIELD_ALIASES[key];
    let idx = -1;
    for (const a of aliases) {
      const i = cleaned.indexOf(a);
      if (i >= 0) {
        idx = i;
        break;
      }
    }
    out[key] = idx;
  });
  return out as Record<keyof Trade, number>;
}

function rowValue(row: Array<string | number | null>, idx: number): unknown {
  if (idx < 0 || idx >= row.length) return '';
  return row[idx];
}

function isClosedStatus(status: string): boolean {
  const s = status.toLowerCase().trim();
  return ['סגור', 'closed', 'close', 'sold', 'נמכר'].includes(s);
}

export function normalizeSnapshot(
  headers: string[],
  rows: Array<Array<string | number | null>>,
): Trade[] {
  const index = buildIndex(headers);
  const trades: Trade[] = [];
  for (const r of rows) {
    const ticker = clean(rowValue(r, index.ticker));
    if (!ticker) continue;
    const trade: Trade = {
      tradeId: clean(rowValue(r, index.tradeId)),
      ticker: ticker.toUpperCase(),
      type: clean(rowValue(r, index.type)),
      platform: clean(rowValue(r, index.platform)),
      currentLocation: clean(rowValue(r, index.currentLocation)),
      purchaseDate: clean(rowValue(r, index.purchaseDate)),
      sellDate: clean(rowValue(r, index.sellDate)),
      quantity: toNumber(rowValue(r, index.quantity)),
      buyPrice: toNumber(rowValue(r, index.buyPrice)),
      costOrigin: toNumber(rowValue(r, index.costOrigin)),
      currency: clean(rowValue(r, index.currency)) || 'ILS',
      commission: toNumber(rowValue(r, index.commission)),
      status: clean(rowValue(r, index.status)) || 'פתוח',
      spotUsd: toNumber(rowValue(r, index.spotUsd)),
      spotIls: toNumber(rowValue(r, index.spotIls)),
      buyUsd: toNumber(rowValue(r, index.buyUsd)),
      buyIls: toNumber(rowValue(r, index.buyIls)),
      costUsd: toNumber(rowValue(r, index.costUsd)),
      costIls: toNumber(rowValue(r, index.costIls)),
      valueUsd: toNumber(rowValue(r, index.valueUsd)),
      valueIls: toNumber(rowValue(r, index.valueIls)),
      sellPrice: toNumber(rowValue(r, index.sellPrice)),
      yieldAtSale: toNumber(rowValue(r, index.yieldAtSale)),
      yieldOrigin: toNumber(rowValue(r, index.yieldOrigin)),
      yieldIls: toNumber(rowValue(r, index.yieldIls)),
    };
    trades.push(trade);
  }
  return trades;
}

function aggregate(
  trades: Trade[],
  picker: (t: Trade) => string,
): AggregateRow[] {
  const groups = new Map<string, { cost: number; value: number }>();
  for (const t of trades) {
    const key = picker(t) || '—';
    const g = groups.get(key) ?? { cost: 0, value: 0 };
    g.cost += t.costIls;
    g.value += t.valueIls;
    groups.set(key, g);
  }
  const totalValue = Array.from(groups.values()).reduce((s, g) => s + g.value, 0);
  return Array.from(groups.entries())
    .map(([key, g]) => ({
      key,
      cost: g.cost,
      value: g.value,
      profit: g.value - g.cost,
      weight: totalValue > 0 ? g.value / totalValue : 0,
    }))
    .sort((a, b) => b.value - a.value);
}

/**
 * Synthesizes a portfolio history series. The Apps Script snapshot is
 * point-in-time, so we extrapolate cost-line vs current-value using each
 * open trade's purchase date — this matches the simple line shown in the
 * Streamlit dashboard when full history isn't available.
 */
function buildHistory(trades: Trade[]): HistoryPoint[] {
  const dated = trades
    .filter((t) => t.purchaseDate && t.costIls > 0)
    .map((t) => ({ date: t.purchaseDate.slice(0, 10), cost: t.costIls }))
    .sort((a, b) => a.date.localeCompare(b.date));
  if (dated.length === 0) return [];
  let running = 0;
  const out: HistoryPoint[] = [];
  for (const d of dated) {
    running += d.cost;
    out.push({ date: d.date, value: running });
  }
  // Append "today" with the live total value if it differs.
  const today = new Date().toISOString().slice(0, 10);
  const totalValue = trades.reduce((s, t) => s + t.valueIls, 0);
  if (totalValue > 0) out.push({ date: today, value: totalValue });
  return out;
}

export function prepareCoreViews(trades: Trade[]): CoreViews {
  const open = trades.filter((t) => !isClosedStatus(t.status));
  const closed = trades.filter((t) => isClosedStatus(t.status));
  const totalCost = open.reduce((s, t) => s + t.costIls, 0);
  const totalValue = open.reduce((s, t) => s + t.valueIls, 0);
  const totalProfit = totalValue - totalCost;
  const totalReturn = totalCost > 0 ? totalProfit / totalCost : 0;
  return {
    trades,
    openTrades: open,
    closedTrades: closed,
    totalCostIls: totalCost,
    totalValueIls: totalValue,
    totalProfitIls: totalProfit,
    totalReturn,
    byTicker: aggregate(open, (t) => t.ticker),
    byPlatform: aggregate(open, (t) => t.platform),
    byType: aggregate(open, (t) => t.type),
    history: buildHistory(open),
  };
}
