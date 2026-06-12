/**
 * Demo dataset — mirrors app.py's demo_mode semantics: the app runs on
 * realistic synthetic data (readable numbers, safe to show publicly),
 * instead of the user's real portfolio. Served by /api/snapshot?demo=1.
 * Includes closed positions so FIFO / realized-P&L demo correctly.
 */
import type { Trade } from './types';

function t(partial: Partial<Trade>): Trade {
  return {
    tradeId: '',
    ticker: '',
    type: 'מניות',
    platform: '',
    currentLocation: '',
    purchaseDate: '',
    sellDate: '',
    quantity: 0,
    buyPrice: 0,
    costOrigin: 0,
    currency: 'USD',
    commission: 0,
    status: 'פתוח',
    spotUsd: 0,
    spotIls: 0,
    buyUsd: 0,
    buyIls: 0,
    costUsd: 0,
    costIls: 0,
    valueUsd: 0,
    valueIls: 0,
    sellPrice: 0,
    yieldAtSale: 0,
    yieldOrigin: 0,
    yieldIls: 0,
    ...partial,
  };
}

export const DEMO_TRADES: Trade[] = [
  t({ tradeId: 'demo-voo-1', ticker: 'VOO', type: 'שוק ההון', platform: 'אקסלנס', currentLocation: 'אקסלנס',
      purchaseDate: '2025-02-04', quantity: 18, buyPrice: 492, costOrigin: 8856, commission: 6,
      costUsd: 8856, costIls: 32720, spotUsd: 558, spotIls: 2062, valueUsd: 10044, valueIls: 37110,
      yieldOrigin: 0.1341, yieldIls: 0.1342 }),
  t({ tradeId: 'demo-qqq-1', ticker: 'QQQ', type: 'ETF', platform: 'אקסלנס', currentLocation: 'אקסלנס',
      purchaseDate: '2025-03-12', quantity: 12, buyPrice: 478, costOrigin: 5736, commission: 5,
      costUsd: 5736, costIls: 21190, spotUsd: 545, spotIls: 2014, valueUsd: 6540, valueIls: 24170,
      yieldOrigin: 0.1402, yieldIls: 0.1406 }),
  t({ tradeId: 'demo-btc-1', ticker: 'BTC', type: 'קריפטו', platform: 'Bit2C', currentLocation: 'ארנק קר',
      purchaseDate: '2025-05-01', quantity: 0.42, buyPrice: 62500, costOrigin: 26250, commission: 9,
      costUsd: 26250, costIls: 98200, spotUsd: 71800, spotIls: 265300, valueUsd: 30156, valueIls: 111430,
      yieldOrigin: 0.1488, yieldIls: 0.1347 }),
  t({ tradeId: 'demo-eth-1', ticker: 'ETH', type: 'קריפטו', platform: 'Bit2C', currentLocation: 'Bit2C',
      purchaseDate: '2025-06-20', quantity: 3.5, buyPrice: 3100, costOrigin: 10850, commission: 7,
      costUsd: 10850, costIls: 40330, spotUsd: 2840, spotIls: 10495, valueUsd: 9940, valueIls: 36730,
      yieldOrigin: -0.0839, yieldIls: -0.0893 }),
  t({ tradeId: 'demo-nvda-1', ticker: 'NVDA', type: 'מניות', platform: 'אינטראקטיב', currentLocation: 'IBKR',
      purchaseDate: '2025-01-10', quantity: 32, buyPrice: 515, costOrigin: 16480, commission: 4,
      costUsd: 16480, costIls: 61400, spotUsd: 612, spotIls: 2262, valueUsd: 19584, valueIls: 72380,
      yieldOrigin: 0.1883, yieldIls: 0.1788 }),
  t({ tradeId: 'demo-sol-1', ticker: 'SOL', type: 'קריפטו', platform: 'Bit2C', currentLocation: 'Bit2C',
      purchaseDate: '2025-09-02', quantity: 40, buyPrice: 138, costOrigin: 5520, commission: 5,
      costUsd: 5520, costIls: 20420, spotUsd: 151, spotIls: 558, valueUsd: 6040, valueIls: 22330,
      yieldOrigin: 0.0942, yieldIls: 0.0935 }),
  // Closed with profit — exercises FIFO realized P&L + closed filters.
  t({ tradeId: 'demo-aapl-x', ticker: 'AAPL', type: 'מניות', platform: 'אינטראקטיב', currentLocation: 'נמכר',
      purchaseDate: '2024-08-01', sellDate: '2025-07-20', quantity: 15, buyPrice: 175, costOrigin: 2625,
      commission: 4, costUsd: 2625, costIls: 9712, valueUsd: 3225, valueIls: 11930, sellPrice: 215,
      status: 'סגור', yieldAtSale: 0.2286, yieldOrigin: 0.2286, yieldIls: 0.2284 }),
  // Closed with loss.
  t({ tradeId: 'demo-tsla-x', ticker: 'TSLA', type: 'מניות', platform: 'אינטראקטיב', currentLocation: 'נמכר',
      purchaseDate: '2025-02-22', sellDate: '2025-11-05', quantity: 10, buyPrice: 355, costOrigin: 3550,
      commission: 4, costUsd: 3550, costIls: 13230, valueUsd: 3120, valueIls: 11540, sellPrice: 312,
      status: 'סגור', yieldAtSale: -0.1211, yieldOrigin: -0.1211, yieldIls: -0.1277 }),
];
