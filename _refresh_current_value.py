"""
Update stale Current_Value_ILS in portfolio_data.json for Horizon crypto trades
using approximate current market prices. This improves the initial-render accuracy
before live prices are fetched.
Uses yfinance to get current prices.
"""
import json
from pathlib import Path

try:
    import yfinance as yf

    # Fetch current prices
    btc_usd = yf.Ticker("BTC-USD").fast_info.last_price or 102000
    eth_usd = yf.Ticker("ETH-USD").fast_info.last_price or 2200
    sol_usd = yf.Ticker("SOL-USD").fast_info.last_price or 155
    usd_ils = yf.Ticker("USDILS=X").fast_info.last_price or 3.62
except Exception:
    btc_usd, eth_usd, sol_usd, usd_ils = 102000, 2200, 155, 3.62

print(f"Prices: BTC=${btc_usd:,.0f} ETH=${eth_usd:,.0f} SOL=${sol_usd:,.2f} USD/ILS={usd_ils:.4f}")

TICKER_PRICES = {'BTC': btc_usd, 'ETH': eth_usd, 'SOL': sol_usd}

data = json.loads(Path('portfolio_data.json').read_text(encoding='utf-8'))
rows = data['rows']

updated = 0
for row in rows:
    if row.get('Status', '') != 'פתוח':
        continue
    ticker = str(row.get('Ticker', '')).upper().strip()
    if ticker not in TICKER_PRICES:
        continue
    qty = float(row.get('Quantity', 0) or 0)
    if qty <= 0:
        continue
    price = TICKER_PRICES[ticker]
    new_val = round(qty * price * usd_ils, 4)
    old_val = row.get('Current_Value_ILS', 0)
    row['Current_Value_ILS'] = new_val
    updated += 1
    print(f"  {ticker} {str(row.get('Purchase_Date','?'))[:10]}: Current_Value_ILS {old_val:.2f} → {new_val:.2f}")

Path('portfolio_data.json').write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
print(f"\nUpdated {updated} rows in portfolio_data.json")

