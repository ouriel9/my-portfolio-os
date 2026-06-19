"""Runtime-visual QA harness for the Streamlit app (desktop + mobile).

This is the layer the audit/fix loop was missing: it BOOTS the real app and
DRIVES it in a real browser at desktop and mobile viewports, then makes both
programmatic assertions (header visible? nav switches page? sidebar animated?)
and screenshots for a vision agent to inspect. Pure code-trace and AppTest
cannot see CSS/JS, so runtime-only regressions (hidden header, dead page nav,
missing animation) were invisible until now.

Usage:
  python _visual_check.py                 # full run, writes _visual_shots/ + JSON verdict
  python _visual_check.py --port 8765     # pin the streamlit port

Exit code 0 = all assertions passed; 1 = at least one failed (gate-friendly).
No secrets are read or written. Demo mode is enabled by clicking the sidebar
toggle, so the real Google Sheet is never contacted.
"""
from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import time
from pathlib import Path

# Windows console defaults to cp1252 and crashes printing Hebrew hero/nav text.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

ROOT = Path(__file__).resolve().parent
APP = ROOT / "app.py"
SHOTS = ROOT / "_visual_shots"

NAV_PILLS_HE = ["סקירה", "עסקאות", "סיכון", "סימולטור"]  # Overview / Trades / Risk / Sim


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def _wait_port(port: int, timeout: float = 90.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=2):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def _start_streamlit(port: int) -> subprocess.Popen:
    cmd = [
        sys.executable, "-m", "streamlit", "run", str(APP),
        "--server.port", str(port),
        "--server.address", "127.0.0.1",
        "--server.headless", "true",
        "--browser.gatherUsageStats", "false",
        "--server.runOnSave", "false",
        "--global.developmentMode", "false",
    ]
    return subprocess.Popen(cmd, cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _settle(page, ms: int = 1400):
    """Wait for Streamlit to finish its current rerun, then a beat for CSS."""
    try:
        page.wait_for_selector('[data-testid="stAppViewContainer"]', timeout=30000)
    except Exception:
        pass
    # Streamlit shows a "running" status while computing; wait for it to clear.
    try:
        page.wait_for_function(
            "() => !document.querySelector('[data-testid=\"stStatusWidget\"]')"
            " || document.querySelector('[data-testid=\"stStatusWidget\"]').innerText.indexOf('Running') < 0",
            timeout=20000,
        )
    except Exception:
        pass
    page.wait_for_timeout(ms)


def _enable_demo(page, is_mobile: bool):
    """Click the demo-mode toggle in the sidebar so the app populates without the real sheet."""
    try:
        if is_mobile:
            # Open the collapsed sidebar on mobile.
            for sel in ['[data-testid="stSidebarCollapsedControl"]', '[data-testid="collapsedControl"]']:
                el = page.query_selector(sel)
                if el:
                    el.click()
                    page.wait_for_timeout(700)
                    break
        # The demo toggle has label text "Demo"/"דמו"; click its input via the label.
        toggled = page.evaluate(
            """() => {
                const labels = Array.from(document.querySelectorAll('label, [data-testid="stWidgetLabel"], p, span'));
                const hit = labels.find(l => /demo|דמו/i.test(l.textContent || ''));
                if (!hit) return false;
                const box = hit.closest('[data-testid="stCheckbox"], [data-testid="stToggle"], label');
                const input = (box || hit).querySelector('input') || hit.parentElement.querySelector('input');
                if (input) { input.click(); return true; }
                (box || hit).click(); return true;
            }"""
        )
        page.wait_for_timeout(300)
        return bool(toggled)
    except Exception:
        return False


def _probe(page) -> dict:
    """Programmatic runtime assertions that code-trace cannot make."""
    return page.evaluate(
        """() => {
            const out = {};
            // 1) HEADER visible: the premium hero header must be rendered AND have height.
            const hero = document.querySelector('[class*="pp-hero"], [id*="hero"], h1, [data-testid="stHeading"]');
            out.header_present = !!hero;
            out.header_visible = !!hero && hero.offsetHeight > 8 &&
                getComputedStyle(hero).display !== 'none' &&
                getComputedStyle(hero).visibility !== 'hidden';
            out.header_text = hero ? (hero.innerText || '').slice(0, 60) : '';
            // 2) Premium CSS present: the <style id="premium-polish"> must exist in the DOM.
            out.premium_css_present = !!document.getElementById('premium-polish');
            out.mobile_polish_present = !!document.getElementById('pp-mobile-polish-v2');
            // 3) Sidebar animation: the sidebar element must carry a CSS transition (not 'none').
            const sb = document.querySelector('[data-testid="stSidebar"]');
            const tr = sb ? getComputedStyle(sb).transition : '';
            out.sidebar_present = !!sb;
            out.sidebar_has_transition = !!tr && tr !== 'none' && tr !== 'all 0s ease 0s' && /\\d/.test(tr);
            out.sidebar_transition = tr;
            // 4) Nav pills present.
            const pills = Array.from(document.querySelectorAll('[role="radiogroup"] label')).map(l => (l.innerText||'').trim());
            out.nav_pills = pills;
            return out;
        }"""
    )


def _main_fp(page) -> str:
    """Fingerprint the MAIN content (not the constant brand hero / sidebar / nav).

    Page navigation is proven by a change in the main view's text, since the hero
    banner is the same brand string on every page. We strip the radiogroup (nav)
    and sidebar, then take a length+head signature of the remaining innerText.
    """
    try:
        return page.evaluate(
            """() => {
                const main = document.querySelector('section.main, [data-testid="stAppViewContainer"] [data-testid="stMain"], [data-testid="stMain"]')
                          || document.querySelector('[data-testid="stAppViewContainer"]');
                if (!main) return '';
                let t = main.innerText || '';
                // Drop the nav pill labels so a pill highlight change alone doesn't count.
                ['Overview','Trades','Risk','Sim','סקירה','עסקאות','סיכון','סימולטור'].forEach(w => { t = t.split(w).join(''); });
                return t.length + '|' + t.replace(/\\s+/g,' ').slice(0, 120);
            }"""
        ) or ""
    except Exception:
        return ""


def _click_pill(page, idx: int) -> bool:
    """Click the Nth nav pill by position — language-agnostic (labels may be EN or HE)."""
    return bool(page.evaluate(
        """(i) => {
            const labels = Array.from(document.querySelectorAll('[role="radiogroup"] label'));
            if (i < 0 || i >= labels.length) return false;
            const input = labels[i].querySelector('input');
            (input || labels[i]).click();
            return true;
        }""", idx
    ))


def run():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=0)
    args = ap.parse_args()

    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        print(json.dumps({"error": f"playwright not importable: {e}"}))
        return 1

    SHOTS.mkdir(exist_ok=True)
    port = args.port or _free_port()
    proc = _start_streamlit(port)
    verdict = {"port": port, "desktop": {}, "mobile": {}, "screenshots": [], "failures": []}

    try:
        if not _wait_port(port):
            print(json.dumps({"error": "streamlit did not start in time", "port": port}))
            return 1
        base = f"http://127.0.0.1:{port}"

        with sync_playwright() as pw:
            browser = pw.chromium.launch(channel="chrome", headless=True)

            # ── DESKTOP ──────────────────────────────────────────────
            dctx = browser.new_context(viewport={"width": 1440, "height": 900})
            dp = dctx.new_page()
            dp.goto(base, wait_until="domcontentloaded")
            _settle(dp)
            _enable_demo(dp, is_mobile=False)
            _settle(dp)
            p1 = str(SHOTS / "desktop_overview.png"); dp.screenshot(path=p1, full_page=True)
            verdict["screenshots"].append(p1)
            verdict["desktop"] = _probe(dp)
            dctx.close()

            # ── MOBILE (iPhone-class) ────────────────────────────────
            mctx = browser.new_context(
                viewport={"width": 390, "height": 844},
                device_scale_factor=3, is_mobile=True, has_touch=True,
                user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
                           "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
            )
            mp = mctx.new_page()
            mp.goto(base + "/?mobile=1", wait_until="domcontentloaded")
            _settle(mp)
            _enable_demo(mp, is_mobile=True)
            _settle(mp)

            probe = _probe(mp)
            verdict["mobile"] = probe
            pm = str(SHOTS / "mobile_overview.png"); mp.screenshot(path=pm, full_page=True)
            verdict["screenshots"].append(pm)

            # NAV TEST: click pills by INDEX (language-agnostic), confirm the hero changes.
            # Pills are [Overview(0), Trades(1), Risk(2), Sim(3)]. Start on Overview.
            pill_names = probe.get("nav_pills", []) or ["0", "1", "2", "3"]
            nav_log = []
            before = _main_fp(mp)
            for idx in [1, 2, 0]:  # Trades, Risk, back to Overview
                name = pill_names[idx] if idx < len(pill_names) else str(idx)
                ok_click = _click_pill(mp, idx)
                _settle(mp)
                after = _main_fp(mp)
                changed = after != before
                nav_log.append({"pill": name, "idx": idx, "clicked": ok_click, "changed": changed})
                shot = str(SHOTS / f"mobile_nav_{idx}_{name}.png"); mp.screenshot(path=shot, full_page=False)
                verdict["screenshots"].append(shot)
                before = after
            verdict["mobile"]["nav_log"] = nav_log
            verdict["mobile"]["nav_switches_pages"] = any(n["changed"] for n in nav_log)

            # SIDEBAR animation screenshot (open state).
            try:
                for sel in ['[data-testid="stSidebarCollapsedControl"]', '[data-testid="collapsedControl"]']:
                    el = mp.query_selector(sel)
                    if el:
                        el.click(); mp.wait_for_timeout(500); break
                ssb = str(SHOTS / "mobile_sidebar.png"); mp.screenshot(path=ssb, full_page=False)
                verdict["screenshots"].append(ssb)
            except Exception:
                pass

            mctx.close()
            browser.close()

        # ── GATE ASSERTIONS ──────────────────────────────────────────
        m = verdict["mobile"]
        if not m.get("header_visible"):
            verdict["failures"].append("MOBILE header not visible (offsetHeight<=8 or display:none)")
        if not m.get("premium_css_present"):
            verdict["failures"].append("MOBILE <style id=premium-polish> missing from DOM (injector skipped on rerun)")
        if not m.get("nav_switches_pages"):
            verdict["failures"].append("MOBILE nav pills do not switch pages (hero text never changed)")
        if not m.get("sidebar_has_transition"):
            verdict["failures"].append(f"MOBILE sidebar has no CSS transition (animation missing): {m.get('sidebar_transition')!r}")
        if not verdict["desktop"].get("header_visible"):
            verdict["failures"].append("DESKTOP header not visible")

        verdict["passed"] = not verdict["failures"]
        (SHOTS / "_verdict.json").write_text(json.dumps(verdict, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(verdict, ensure_ascii=False, indent=2))
        return 0 if verdict["passed"] else 1
    finally:
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(run())
