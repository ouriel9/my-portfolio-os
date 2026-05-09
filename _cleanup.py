"""One-time cleanup script — deletes all debug/temp files."""
import os
from pathlib import Path

BASE = Path(r"C:\Users\ourie\PycharmProjects\my-portfolio-os")

TO_DELETE = [
    # Debug / Playwright scripts
    "_check_ancestors.py",
    "_check_app.py",
    "_check_menu.py",
    "_check_pos.py",
    "_check_sidebar_btn.py",
    "_check_translations.py",
    "_debug2.py",
    "_debug_scroll.py",
    "_find_bottom_logos.py",
    "_fix_theme.py",
    "_inspect_menu.py",
    "_inspect_optmenu.py",
    "_inspect_top.py",
    "_zoom_top.py",
    # One-time patch scripts
    "_new_simulator.py",
    "_patch_tx_fragment.py",
    "_audit_flet.py",
    "_syntax_check.py",
    "_chk_syntax.py",
    # Scan/analysis scripts
    "_scan_all_he.py",
    "_scan_hardcoded_he.py",
    "_scan_specific.py",
    "_scan_ui_he.py",
    # Playwright test/screenshot scripts
    "_snap.py",
    "_snap_sim.py",
    "_test_english_pct.py",
    "_test_manage.py",
    "_test_popup.py",
    "_test_sim_crash.py",
    # Empty files
    "chk.py",
    "s.py",
    "search_app.py",
    # Old app.py versions
    "app (1).py",
    "app1.py",
    # Screenshots / debug images
    "_after_desktop.png",
    "_after_mobile.png",
    "_after_mobile_risk.png",
    "_after_mobile_risk_help_open.png",
    "_bottom_mobile.png",
    "_debug_menu.png",
    "_debug_menu2.png",
    "_en_overview.png",
    "_en_ov_bot.png",
    "_en_reports_full.png",
    "_en_rep_bot.png",
    "_full_mobile.png",
    "_full_mobile_bottom.png",
    "_inspect_desktop.png",
    "_inspect_mobile.png",
    "_manage_top.png",
    "_menu_isolated_desktop.png",
    "_menu_isolated_mobile.png",
    "_popup_after_scroll.png",
    "_popup_inline.png",
    "_qa_desktop.png",
    "_qa_mobile_iphone13.png",
    "_qa_mobile_iphone13_risk.png",
    "_qa_mobile_iphone13_simulator.png",
    "_reports_english.png",
    "_sidebar_bottom_mobile.png",
    "_sidebar_collapsed.png",
    "_sidebar_open.png",
    "_sim_after_interact.png",
    "_sim_bottom_mobile.png",
    "_sim_clean_after.png",
    "_sim_initial.png",
    "_sim_top_mobile.png",
    "_top_desktop.png",
    "_top_mobile.png",
    "_very_bottom.png",
    "mobile_check.png",
    "mobile_final.png",
    "WhatsApp Image 2026-04-12 at 18.44.13.jpeg",
    # Docs / reports no longer needed
    "RECONSTRUCTION_PROMPT.txt",
    "RESTORE_PROMPT_APP_PY_HE.txt",
    "executive_summary.md",
    "FINAL_REPORT.md",
    "diff_output.txt",
    "_he_report.txt",
]

deleted = []
not_found = []

for name in TO_DELETE:
    p = BASE / name
    if p.exists():
        p.unlink()
        deleted.append(name)
    else:
        not_found.append(name)

print(f"Deleted {len(deleted)} files:")
for f in deleted:
    print(f"  ✓ {f}")

if not_found:
    print(f"\nNot found ({len(not_found)}):")
    for f in not_found:
        print(f"  ? {f}")

# Self-delete this script after running
Path(__file__).unlink()
print("\nCleanup complete. This script deleted itself.")

