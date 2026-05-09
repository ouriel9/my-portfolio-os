import sys
kws = ['gauge','Gauge','sharpe','Sharpe','שלמות','quality_bar','perf_track','Build-Up','fig_quality','automargin','st.write(','_ov_equity','col_complete']
with open('app.py', encoding='utf-8') as f:
    for i,line in enumerate(f,1):
        for k in kws:
            if k in line:
                sys.stdout.write(f"{i}: {line[:120]}")
                break

