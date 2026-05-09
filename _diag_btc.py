import json

data = json.load(open('portfolio_data.json', encoding='utf-8'))
rows = [r for r in data['rows'] if not r.get('_deleted', False)]

btc = [r for r in rows if str(r.get('Ticker','')).upper()=='BTC' and r.get('Status','') not in ['סגור','closed','sold']]

print('=== BTC avg buy price (Cost_ILS / Qty) ===')
total_ils = sum(float(r['Cost_ILS']) for r in btc)
total_qty = sum(float(r['Quantity']) for r in btc)
avg_ils = total_ils / total_qty
print(f'Total Cost_ILS = {total_ils:,.2f} ILS')
print(f'Total Qty      = {total_qty:.8f} BTC')
print(f'Avg Buy Price  = {avg_ils:,.2f} ILS/BTC')
print(f'Avg Buy Price  = {avg_ils/3.65:,.2f} USD/BTC  (at current fx 3.65)')

print()
print('=== per-trade breakdown ===')
for r in btc:
    d = str(r['Purchase_Date'])[:10]
    cur = r['Origin_Currency']
    qty = float(r['Quantity'])
    bp = float(r['Origin_Buy_Price'])
    co = float(r['Cost_Origin'])
    ci = float(r['Cost_ILS'])
    implied_ils = ci / qty if qty else 0
    hist_fx = ci / co if co and cur=='USD' else 'N/A'
    print(f"{d} | {cur} | qty={qty:.6f} | buy_price={bp:,.0f} {cur} | Cost_ILS={ci:,.2f} | ILS/BTC={implied_ils:,.0f} | hist_fx={hist_fx}")

print()
print('=== Horizon 2025-09-07 ANOMALY ===')
h = next(r for r in btc if '2025-09-07' in str(r.get('Purchase_Date','')))
qty = float(h['Quantity'])
ci = float(h['Cost_ILS'])
co = float(h['Cost_Origin'])
bp = float(h['Origin_Buy_Price'])
hist_fx = ci / co
implied_usd = ci / qty / hist_fx
btc_sep_2025_approx = 95000
print(f"  Stored Origin_Buy_Price = ${bp:,.2f} USD/BTC")
print(f"  Cost_ILS={ci:.2f} / Qty={qty:.8f} / hist_fx({hist_fx:.3f}) = ${implied_usd:,.2f} USD/BTC <- same")
print(f"  Nearby Aug 2025 trades imply BTC ~{sum(float(r['Cost_ILS'])/float(r['Quantity']) for r in btc if '2025-08' in str(r['Purchase_Date']))/4:,.0f} ILS/BTC")
print(f"  Sep 2025 BTC should be ~$95,000-$100,000 USD/BTC based on adjacent trades")
print(f"  Ratio: {bp/btc_sep_2025_approx:.3f}x (should be ~1.0)")
print()
print("  CONCLUSION: Origin_Buy_Price=$12,061 and Cost_ILS=717 are ~8x too small vs market.")
print("  Most likely cause: data import error from Horizon platform.")
print("  Impact: Yield_Current shows +566% (mathematically correct given wrong data).")
print()
print("  If CORRECT qty=0.002222 BTC (not 0.017776) then price would be $96,491/BTC - realistic")
print(f"  Or if CORRECT cost_ils=5,678 (not 717) then avg_ils={5678/qty:,.0f} ILS/BTC = ${5678/qty/hist_fx:,.0f}/BTC")

