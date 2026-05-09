"""Comprehensive scan for Hebrew strings not wrapped in tr()."""
import re

with open("app.py", encoding="utf-8") as f:
    lines = f.readlines()

def has_real_he(s):
    return any('\u05d0' <= c <= '\u05ea' for c in s)

# All lines with Hebrew that don't have tr() wrapper
print("=== All lines with Hebrew text not inside tr() ===")
issues = []
for lineno, line in enumerate(lines, 1):
    stripped = line.strip()
    if stripped.startswith('#'):
        continue
    if not has_real_he(line):
        continue
    # Count how much of the Hebrew is inside tr() calls
    # Extract tr() call bodies
    tr_bodies = re.findall(r'tr\([^)]{0,400}\)', line)
    line_minus_tr = line
    for body in tr_bodies:
        line_minus_tr = line_minus_tr.replace(body, '', 1)
    # If remaining text has Hebrew, it's outside tr()
    if has_real_he(line_minus_tr):
        # Ignore pure data / shape logic lines
        noisy = ('טיקר', 'ticker', 'Trade_ID', 'FIFO', '#')
        issues.append((lineno, line.rstrip()))

print(f"Total lines: {len(issues)}")
for lineno, ln in issues[:200]:
    print(f"  L{lineno:5d}: {ln[:130]}")

