"""Claude-Code-backed Telegram agent — uses your local Claude Code CLI (which is
authenticated with your Claude MAX subscription) instead of a paid Anthropic API
key. Every Telegram message is handed to a headless `claude -p` run inside the
portfolio project, scoped so it can read/write the Google Sheet through
portfolio_cli.py and pull Bit2C trades through bit2c_sync.py — nothing else.

This makes the bot a TRUE agent (free-text in, action taken, answer out) at zero
extra cost, running on your MAX plan. It only works while this PC is on (the
always-on Streamlit server already requires that).

Public API:
  cc_available() -> bool          # is the Claude Code CLI present?
  run_agent_cc(user_text) -> str  # run one turn, return the reply text
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# Candidate locations for the Claude Code executable (npm global install on Windows).
_EXE_CANDIDATES = [
    Path(os.environ.get("APPDATA", "")) / "npm" / "node_modules" / "@anthropic-ai" / "claude-code" / "bin" / "claude.exe",
    Path(os.environ.get("APPDATA", "")) / "npm" / "claude.cmd",
]

# Tools the bot is allowed to use, fully scoped. In headless mode anything not on
# this list is auto-denied (no hang, no arbitrary shell from a Telegram message).
_ALLOWED_TOOLS = (
    "Bash(python portfolio_cli.py:*),"
    "Bash(python bit2c_sync.py:*),"
    "PowerShell(python portfolio_cli.py:*),"
    "PowerShell(python bit2c_sync.py:*),"
    "Read,Glob,Grep"
)

_SYSTEM = (
    "You are Ouriel's personal portfolio assistant, reached over Telegram. You act on his "
    "authority over his own investment data. Work in this project directory. You manage the "
    "portfolio through a CLI that writes to the shared Google Sheet (so every app — Streamlit, "
    "desktop, mobile — updates automatically):\n"
    "  python portfolio_cli.py snapshot                      -> list all holdings (open/closed)\n"
    "  python portfolio_cli.py add --json \"{...}\"            -> add a trade\n"
    "  python portfolio_cli.py edit --json \"{...}\"           -> edit a trade by Trade_ID\n"
    "  python portfolio_cli.py delete --id TRADE_ID           -> delete a trade\n"
    "  python bit2c_sync.py                                   -> pull new Bit2C trades into the sheet\n"
    "Trade JSON fields (Hebrew sheet contract): Platform, Current_Location, Type, Ticker, "
    "Purchase_Date (YYYY-MM-DD), Quantity, Origin_Buy_Price, Cost_Origin, Origin_Currency "
    "(ILS/USD), Commission, Status (פתוח/סגור). Trade_ID is auto-derived on add.\n"
    "RULES: Always reply in Hebrew, short and clear (this goes to a phone). When the user asks "
    "you to record/change a trade, actually run the CLI and confirm what changed. For questions, "
    "run snapshot to get live data — never guess numbers. Before any add/edit/delete, make sure "
    "you have the real values; if something critical is missing, ask one short follow-up instead "
    "of inventing it. Keep replies under ~1500 characters."
)


def _find_exe() -> str | None:
    for cand in _EXE_CANDIDATES:
        try:
            if cand and cand.exists():
                return str(cand)
        except Exception:
            continue
    return None


def cc_available() -> bool:
    return _find_exe() is not None


def _agent_model() -> str:
    """Model for the headless Claude Code agent. Defaults to 'sonnet' (included in
    the Claude MAX subscription). 'fable'/Opus-tier may not be in every MAX plan,
    which causes 'model may not exist or you may not have access' errors.
    Override via telegram_config.json -> "cc_model"."""
    try:
        import json
        cfg = json.loads((ROOT / "telegram_config.json").read_text(encoding="utf-8"))
        m = str(cfg.get("cc_model", "")).strip()
        if m:
            return m
    except Exception:
        pass
    return "sonnet"


def run_agent_cc(user_text: str, timeout: int = 300) -> str:
    """Run one headless Claude Code turn for the given user message and return the reply."""
    exe = _find_exe()
    if not exe:
        return "Claude Code CLI לא נמצא במחשב."
    cmd = [
        exe, "-p",
        "--model", _agent_model(),
        "--output-format", "text",
        "--allowedTools", _ALLOWED_TOOLS,
        "--append-system-prompt", _SYSTEM,
    ]
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    # Don't let a stray API key change billing — force the subscription path.
    env.pop("ANTHROPIC_API_KEY", None)
    try:
        proc = subprocess.run(
            cmd,
            input=user_text,
            cwd=str(ROOT),
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return "הבקשה ארכה יותר מדי (timeout). נסה לנסח קצר יותר."
    except Exception as exc:
        return f"שגיאת הרצת Claude Code: {str(exc)[:200]}"
    out = (proc.stdout or "").strip()
    if not out:
        err = (proc.stderr or "").strip()
        return f"לא התקבלה תשובה. {err[:300]}" if err else "לא התקבלה תשובה מהסוכן."
    return out[:3800]


if __name__ == "__main__":
    import sys
    msg = " ".join(sys.argv[1:]) or "מה מצב התיק שלי כרגע? תן סיכום קצר."
    print(run_agent_cc(msg))
