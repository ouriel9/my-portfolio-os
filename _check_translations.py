"""Scan app.py for tr() calls and report translation issues."""
import re

with open("C:/Users/ourie/PycharmProjects/my-portfolio-os/app.py", encoding="utf-8") as f:
    lines = f.readlines()

# Match tr("EN", "HE") on a single line
pattern = re.compile(r'tr\(\s*(["\'])(.+?)\1\s*,\s*(["\'])(.+?)\3\s*\)')

issues = []
for lineno, line in enumerate(lines, 1):
    for m in pattern.finditer(line):
        en, he = m.group(2), m.group(4)
        has_he_in_en = any(ord(c) > 0x590 for c in en)
        has_ascii_alpha_in_he = any(c.isascii() and c.isalpha() for c in he)
        same = (en.strip().lower() == he.strip().lower())
        if has_he_in_en:
            issues.append((lineno, "EN_HAS_HEBREW", en[:60], he[:60]))
        elif same and len(en.strip()) > 4:
            issues.append((lineno, "NOT_TRANSLATED", en[:60], he[:60]))

print(f"Total issues: {len(issues)}")
for lineno, kind, en, he in issues:
    print(f"  L{lineno:5d} [{kind}] EN='{en}' | HE='{he}'")

