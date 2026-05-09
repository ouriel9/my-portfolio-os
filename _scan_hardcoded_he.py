"""Find Hebrew strings hardcoded in Streamlit calls WITHOUT tr() wrapping."""
import re

with open("app.py", encoding="utf-8") as f:
    lines = f.readlines()

def has_he(s):
    return any('\u05d0' <= c <= '\u05ea' for c in s)

# Streamlit display calls that should be localised
ST_CALLS = re.compile(
    r'\bst\.(markdown|subheader|header|title|caption|info|warning|error|success|write|text|metric|button|checkbox|radio|selectbox|multiselect|number_input|text_input|text_area|expander|toast|balloons|snow|sidebar)\s*\('
)

issues = []
for lineno, line in enumerate(lines, 1):
    # Skip lines that already have tr(
    if 'tr(' in line:
        continue
    # Skip comments
    stripped = line.strip()
    if stripped.startswith('#'):
        continue
    # Look for hebrew in st. calls
    if has_he(line) and ST_CALLS.search(line):
        issues.append((lineno, line.rstrip()))

print(f"\n=== Hardcoded Hebrew in st.* calls (no tr()): {len(issues)} ===")
for lineno, ln in issues:
    print(f"  L{lineno:5d}: {ln[:120]}")

# Also scan for Hebrew in column names / DataFrame labels used in UI
print("\n=== tr() calls where EN looks like it has real Hebrew text (codepoints U+05D0..U+05EA) ===")
pattern = re.compile(r'tr\(\s*(["\'])(.+?)\1\s*,\s*(["\'])(.+?)\3\s*\)')
real_he_issues = []
for lineno, line in enumerate(lines, 1):
    for m in pattern.finditer(line):
        en, he = m.group(2), m.group(4)
        he_in_en = [c for c in en if '\u05d0' <= c <= '\u05ea']
        if he_in_en:
            real_he_issues.append((lineno, en[:80], he[:80]))

print(f"Total tr() calls with real Hebrew in EN string: {len(real_he_issues)}")
for lineno, en, he in real_he_issues:
    print(f"  L{lineno:5d} EN='{en}' | HE='{he}'")

