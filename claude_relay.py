"""Claude relay daemon — lets the PHONE (Streamlit Cloud) use Claude.

Claude on the MAX subscription only works through the local Claude Code CLI, which
needs this PC. This daemon polls the Apps Script backend for Claude requests
submitted by the phone, runs them through the local CLI (which can also run
portfolio_cli.py to add/edit/delete trades), and posts the answer back. So:
"Claude works on the phone whenever the PC is on."

Run:  pythonw claude_relay.py   (auto-started at logon via the Startup .bat)
Keep exactly ONE instance running.
"""
from __future__ import annotations
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# Under pythonw.exe there is no console — sys.stdout/stderr are None and any
# print() would crash the daemon. Redirect everything to a log file.
try:
    _logf = open(ROOT / "claude_relay.log", "a", encoding="utf-8", buffering=1)
    sys.stdout = _logf
    sys.stderr = _logf
except Exception:
    pass
_cfg = json.loads((ROOT / "app_local_config.json").read_text(encoding="utf-8"))
URL = _cfg["web_app_url"]
TOKEN = _cfg["api_token"]


def _call(action: str, **kw):
    payload = {"token": TOKEN, "action": action}
    payload.update(kw)
    req = urllib.request.Request(URL, data=json.dumps(payload).encode("utf-8"),
                                 headers={"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=60).read())


def _claude_exe():
    for c in [
        Path(os.environ.get("APPDATA", "")) / "npm" / "node_modules" / "@anthropic-ai" / "claude-code" / "bin" / "claude.exe",
        Path(os.environ.get("APPDATA", "")) / "npm" / "claude.cmd",
    ]:
        try:
            if c.exists():
                return str(c)
        except Exception:
            continue
    return None


_SYSTEM = (
    "You are Ouriel's portfolio agent (reached from his phone). Reply in HEBREW, concise. "
    "You manage the portfolio via the CLI (writes to the shared Google Sheet → all apps sync):\n"
    "  python portfolio_cli.py snapshot|add --json \"{...}\"|edit --json \"{...}\"|delete --id ID\n"
    "The user's message already includes the live portfolio data and recent conversation. "
    "Before any add/edit/delete, confirm the parsed values in one line, then run the CLI and report what changed. "
    "Never invent numbers."
)


def _run_claude(prompt: str, timeout: int = 240) -> str:
    exe = _claude_exe()
    if not exe:
        return "Claude CLI לא נמצא במחשב."
    cmd = [exe, "-p", "--model", "sonnet", "--output-format", "text",
           "--allowedTools", "Bash(python portfolio_cli.py:*),PowerShell(python portfolio_cli.py:*),Read,Glob,Grep",
           "--append-system-prompt", _SYSTEM]
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"; env["PYTHONIOENCODING"] = "utf-8"
    env.pop("ANTHROPIC_API_KEY", None)
    try:
        proc = subprocess.run(cmd, input=prompt, cwd=str(ROOT), env=env, capture_output=True,
                              text=True, encoding="utf-8", errors="replace", timeout=timeout)
    except subprocess.TimeoutExpired:
        return "Claude לקח יותר מדי זמן (timeout)."
    except Exception as exc:
        return "שגיאת הרצת Claude: " + str(exc)[:200]
    out = (proc.stdout or "").strip()
    return out[:7000] if out else ("לא התקבלה תשובה. " + (proc.stderr or "")[:300])


def _log(m):
    try:
        print(time.strftime("%H:%M:%S"), m, flush=True)
    except Exception:
        pass


def main():
    _log("claude_relay: polling " + URL[:60])
    while True:
        try:
            r = _call("claude_pending")
            req = r.get("request") if isinstance(r, dict) else None
            if req and req.get("prompt"):
                _log("got request %s (%d chars)" % (req.get("id"), len(req["prompt"])))
                ans = _run_claude(req["prompt"])
                _log("claude answered (%d chars); posting" % len(ans))
                _call("claude_answer", id=req.get("id", ""), answer=ans)
                _log("posted answer for %s" % req.get("id"))
        except Exception as e:
            _log("ERROR: " + repr(e)[:200])
        time.sleep(3)


if __name__ == "__main__":
    main()
