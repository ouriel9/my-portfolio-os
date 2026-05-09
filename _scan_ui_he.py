"""Smart scan - find Hebrew strings that are likely UI-visible and not in tr()."""
import re
import sys

# Force utf-8 output
sys.stdout.reconfigure(encoding='utf-8')

with open("app.py", encoding="utf-8") as f:
    lines = f.readlines()

def has_real_he(s):
    return any('\u05d0' <= c <= '\u05ea' for c in s)

# Patterns that strongly suggest a UI-visible string
UI_PATTERNS = [
    r'st\.(markdown|subheader|header|title|caption|info|warning|error|success|write|text|metric|button|checkbox|selectbox|expander|toast|sidebar\.markdown|sidebar\.subheader|sidebar\.title)',
    r'name\s*=\s*["\'][^"\']*[\u05d0-\u05ea]',  # plotly trace names in Hebrew
    r'title\s*=\s*["\'][^"\']*[\u05d0-\u05ea]',  # chart titles
    r'xaxis_title\s*=\s*["\'][^"\']*[\u05d0-\u05ea]',
    r'yaxis_title\s*=\s*["\'][^"\']*[\u05d0-\u05ea]',
    r'annotation_text\s*=\s*["\'][^"\']*[\u05d0-\u05ea]',
    r'\[["\'][^"\']*[\u05d0-\u05ea][^"\']*["\']',  # list of hebrew strings like ["פתוח", "סגור"]
]
ui_re = [re.compile(p) for p in UI_PATTERNS]

issues = []
for lineno, line in enumerate(lines, 1):
    stripped = line.strip()
    if stripped.startswith('#'):
        continue
    if 'tr(' in line:
        # Remove tr(...) content to see what's left
        line_check = re.sub(r'tr\([^)]{0,500}\)', '', line)
    else:
        line_check = line
    
    if not has_real_he(line_check):
        continue
    
    for r in ui_re:
        if r.search(line_check):
            issues.append((lineno, line.rstrip()))
            break

print(f"=== UI-visible Hebrew NOT in tr(): {len(issues)} candidates ===")
for lineno, ln in issues:
    print(f"L{lineno:5d}: {ln[:140]}")

