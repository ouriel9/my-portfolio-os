import json
from pathlib import Path

f = Path(r'C:\Users\ourie\PycharmProjects\my-portfolio-os\portfolio_data.json')
data = json.loads(f.read_text(encoding='utf-8'))
rows = data.get('rows', [])
print(f'Total rows: {len(rows)}')
print(f'Non-deleted: {sum(1 for r in rows if not r.get("_deleted", False))}')
print(f'Dirty: {sum(1 for r in rows if r.get("_dirty", False))}')
print('Meta:', json.dumps(data.get('_meta', {}), ensure_ascii=False))
for i, r in enumerate(rows):
    print(f'  Row {i}: Ticker={r.get("Ticker","")} Trade_ID={r.get("Trade_ID","")} dirty={r.get("_dirty")} op={r.get("_dirty_op")} deleted={r.get("_deleted")}')

