export type Locale = 'he' | 'en';

export type AssetType = 'קריפטו' | 'שוק ההון' | 'crypto' | 'stock' | string;

export type TradeStatus = 'פתוח' | 'סגור' | 'open' | 'closed' | string;

export interface SnapshotApiResponse {
  ok: boolean;
  error?: string;
  data?: {
    headers: string[];
    rows: Array<Array<string | number | null>>;
  };
}

/** Canonical normalized trade row used everywhere in the UI. */
export interface Trade {
  tradeId: string;
  ticker: string;
  type: AssetType;
  platform: string;
  currentLocation: string;
  purchaseDate: string;
  sellDate: string;
  quantity: number;
  buyPrice: number;
  costOrigin: number;
  currency: string;
  commission: number;
  status: TradeStatus;
  spotUsd: number;
  spotIls: number;
  buyUsd: number;
  buyIls: number;
  costUsd: number;
  costIls: number;
  valueUsd: number;
  valueIls: number;
  sellPrice: number;
  yieldAtSale: number;
  yieldOrigin: number;
  yieldIls: number;
}

export interface CoreViews {
  trades: Trade[];
  openTrades: Trade[];
  closedTrades: Trade[];
  totalCostIls: number;
  totalValueIls: number;
  totalProfitIls: number;
  totalReturn: number;
  byTicker: AggregateRow[];
  byPlatform: AggregateRow[];
  byType: AggregateRow[];
  history: HistoryPoint[];
}

export interface AggregateRow {
  key: string;
  cost: number;
  value: number;
  profit: number;
  weight: number;
}

export interface HistoryPoint {
  date: string;
  value: number;
}

export interface CompletenessStat {
  nonEmpty: number;
  total: number;
  ratio: number;
}

export interface FifoLot {
  date: string;
  quantity: number;
  unitCost: number;
}

export interface FifoMetric {
  ticker: string;
  openQuantity: number;
  realizedPnl: number;
  unrealizedPnl: number;
  averageCost: number;
  marketValue: number;
}

export interface RiskMetrics {
  hhi: number;
  var95: number;
  cvar95: number;
  maxDrawdown: number;
  sharpe: number;
  sortino: number;
  calmar: number;
  volatility: number;
}
