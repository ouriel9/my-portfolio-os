keywords = ['gauge', 'Gauge', 'sharpe', 'Sharpe', 'שלמות', 'completeness', 'quality_bar', 'perf_track', 'Build-Up', 'equity_slot', 'fig_gauge', 'fig_quality', 'automargin', 'st.write(']
with open('app.py', encoding='utf-8') as f:
    lines = f.readlines()
for i, line in enumerate(lines, 1):
    for kw in keywords:
        if kw in line:
            print(f"{i}: {line.rstrip()[:130]}")
            break

