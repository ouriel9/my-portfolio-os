"""Find specific patterns of hardcoded Hebrew in UI-visible contexts."""
import re
import sys
sys.stdout.reconfigure(encoding='utf-8')

with open("app.py", encoding="utf-8") as f:
    content = f.read()
    lines = content.splitlines()

def has_he(s):
    return any('\u05d0' <= c <= '\u05ea' for c in s)

print("=== 1. st.metric() with hardcoded Hebrew labels ===")
# Find st.metric( calls where label is Hebrew and not tr()
for i, line in enumerate(lines, 1):
    if 'st.metric(' in line or '.metric(' in line:
        if has_he(line) and 'tr(' not in line:
            print(f"L{i:5d}: {line.strip()[:120]}")

print("\n=== 2. Hardcoded Hebrew in selectbox options ===")
for i, line in enumerate(lines, 1):
    if 'selectbox' in line and has_he(line) and 'tr(' not in line:
        print(f"L{i:5d}: {line.strip()[:120]}")

print("\n=== 3. Hardcoded Hebrew in button/checkbox labels ===")
for i, line in enumerate(lines, 1):
    if re.search(r'\b(st\.button|st\.checkbox|st\.radio)\s*\(', line) and has_he(line) and 'tr(' not in line:
        print(f"L{i:5d}: {line.strip()[:120]}")

print("\n=== 4. Page title / window title ===")
for i, line in enumerate(lines, 1):
    if 'page_title' in line and has_he(line):
        print(f"L{i:5d}: {line.strip()[:120]}")

print("\n=== 5. Hardcoded Hebrew in st.markdown/subheader/header/caption ===")
for i, line in enumerate(lines, 1):
    if re.search(r'st\.(markdown|subheader|header|caption|info|warning|error|success|write)\s*\(', line):
        if has_he(line) and 'tr(' not in line:
            print(f"L{i:5d}: {line.strip()[:120]}")

print("\n=== 6. Hardcoded Hebrew in Plotly titles/axis/labels (not inside tr()) ===")
PLOTLY_KEYS = ['title=', 'xaxis_title=', 'yaxis_title=', 'name=', 'text=', 'annotation_text=', 'hovertemplate=']
for i, line in enumerate(lines, 1):
    stripped = line.strip()
    if stripped.startswith('#'):
        continue
    if 'tr(' in line:
        # Remove tr() bodies (simple, non-nested)
        check = re.sub(r'tr\(\s*["\'].*?["\'],\s*["\'].*?["\']\s*\)', '', line)
    else:
        check = line
    for key in PLOTLY_KEYS:
        if key in check and has_he(check):
            print(f"L{i:5d}: {line.strip()[:120]}")
            break

print("\n=== 7. Column headers used in displayed DataFrames with Hebrew ===")
# Look for rename(columns={...}) with Hebrew keys that are NOT handled by SNAPSHOT_HEADERS
df_he_cols = re.findall(r'"([^"]*[\u05d0-\u05ea][^"]*)"', content)
# Unique
seen = set()
# Find which ones appear in rename() or DataFrame() constructors
for i, line in enumerate(lines, 1):
    if ('rename(columns' in line or 'pd.DataFrame' in line) and has_he(line) and 'tr(' not in line:
        if not line.strip().startswith('#'):
            print(f"L{i:5d}: {line.strip()[:120]}")

