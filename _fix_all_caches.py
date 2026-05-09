"""
Fix snapshot_cache.csv and bak JSON files to match the corrected portfolio_data.json.
This ensures ALL fallback data sources have correct Horizon trade data.
"""
import json
import csv
import pandas as pd
from pathlib import Path

BASE = Path('C:/Users/ourie/PycharmProjects/my-portfolio-os')

# The 5 Horizon trades that were wrong
HORIZON_FIXES = {
    'f50172f0e47a5d9f': {'cost_origin': 2061.45, 'buy_price': 115970.051267, 'cost_ils_rate': 3.3448},  # BTC Sep
    '8d80385f4bd3': {'cost_origin': 2061.25, 'buy_price': 224.571588, 'cost_ils_rate': 3.4865},          # SOL Sep
    'dcc43c3a0b06': {'cost_origin': 1511.79, 'buy_price': 90256.658249, 'cost_ils_rate': 3.4453},        # BTC Nov
    '4ecbb3956f20': {'cost_origin': 2066.51, 'buy_price': 2886.134749, 'cost_ils_rate': 3.2462},         # ETH Jan
    'cb4ad6315af9': {'cost_origin': 2518.16, 'buy_price': 120.692283, 'cost_ils_rate': 3.2461},          # SOL Jan
}

def find_fix(trade_id):
    tid = str(trade_id)
    for key, fix in HORIZON_FIXES.items():
        if tid.startswith(key) or key.startswith(tid[:12]):
            return fix
    return None

# ── Fix snapshot_cache.csv ──────────────────────────────────────────────────
cache_path = BASE / 'snapshot_cache.csv'
if cache_path.exists():
    df = pd.read_csv(cache_path)
    print(f'snapshot_cache.csv: {len(df)} rows')
    fixed = 0
    for idx, row in df.iterrows():
        fix = find_fix(str(row.get('Trade_ID', '')))
        if fix:
            old_co = row.get('Cost_Origin', 0)
            old_ci = row.get('Cost_ILS', 0)
            qty = float(row.get('Quantity', 0) or 0)
            new_co = fix['cost_origin']
            new_ci = new_co * fix['cost_ils_rate']
            new_bp = fix['buy_price']
            df.at[idx, 'Cost_Origin'] = new_co
            df.at[idx, 'Cost_ILS'] = round(new_ci, 6)
            df.at[idx, 'Origin_Buy_Price'] = new_bp
            # Recompute yields to 0 (will be recomputed by app from live prices)
            df.at[idx, 'Yield_Origin'] = 0.0
            df.at[idx, 'Yield_ILS'] = 0.0
            fixed += 1
            print(f'  Fixed: Trade {str(row.get("Trade_ID","?"))[:12]} CO: {old_co} → {new_co}, CI: {old_ci:.2f} → {new_ci:.2f}')
    df.to_csv(cache_path, index=False)
    print(f'  Fixed {fixed} rows in snapshot_cache.csv')
else:
    print('snapshot_cache.csv not found')

print()

# ── Fix portfolio_data.bak*.json ────────────────────────────────────────────
for bak in ['portfolio_data.bak1.json', 'portfolio_data.bak2.json', 'portfolio_data.bak3.json']:
    bak_path = BASE / bak
    if not bak_path.exists():
        print(f'{bak}: not found, skipping')
        continue
    try:
        data = json.loads(bak_path.read_text(encoding='utf-8'))
        rows = data.get('rows', [])
        fixed = 0
        for row in rows:
            fix = find_fix(str(row.get('Trade_ID', '')))
            if fix:
                old_co = float(row.get('Cost_Origin', 0) or 0)
                qty = float(row.get('Quantity', 0) or 0)
                new_co = fix['cost_origin']
                new_ci = new_co * fix['cost_ils_rate']
                new_bp = fix['buy_price']
                row['Cost_Origin'] = new_co
                row['Cost_ILS'] = round(new_ci, 6)
                row['Origin_Buy_Price'] = round(new_bp, 6)
                fixed += 1
        bak_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        print(f'{bak}: fixed {fixed} trades')
    except Exception as e:
        print(f'{bak}: ERROR - {e}')

print('\nDone.')

