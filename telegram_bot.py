"""Telegram ⇆ Portfolio OS bot (long-polling — no webhook/ngrok needed).

Commands (Hebrew or English):
  מצב / status              → portfolio summary
  דוח / report              → monthly-style report
  סנכרן / sync              → pull new Bit2C trades into the sheet
  קניתי BTC 0.01 188000     → add a trade
  עזרה / help               → command list

Every action updates the Google Sheet, so ALL apps (Streamlit/Flet/Next.js)
reflect it. First message you send auto-saves your chat_id (for the monthly
push). Token lives in telegram_config.json (gitignored).

Run:    python telegram_bot.py            # live, polls forever
        python telegram_bot.py --report   # push the monthly report once
"""
from __future__ import annotations

import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

# Windows consoles default to cp1252 and crash when printing Hebrew. Make all
# stdout/stderr writes encoding-safe so logging can never kill the bot.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from whatsapp_bot import handle as _quick_handle, _summary  # quick fixed-commands
import telegram_agent
import telegram_agent_cc  # Claude-Code-backed agent (uses your MAX subscription, no API key)


def handle(text: str) -> str:
    """Full dispatch, in order of preference:
    1. Claude Code CLI (your MAX subscription) — a TRUE free-text agent, zero API cost.
    2. Anthropic API agent — only if an API key is configured.
    3. Fixed quick-commands — always works as a last resort.
    """
    # 1) Claude Code via the local MAX-subscription CLI
    if telegram_agent_cc.cc_available():
        try:
            return telegram_agent_cc.run_agent_cc(text)
        except Exception as exc:
            # fall through to the next backend on any failure
            _log(f"cc agent error: {str(exc)[:160]}")
    # 2) Anthropic API agent (paid key)
    if telegram_agent.agent_available():
        try:
            return telegram_agent.run_agent(text)
        except Exception as exc:
            return f"שגיאת סוכן: {str(exc)[:200]}\n(נופל לפקודות הבסיסיות)\n\n" + _quick_handle(text)
    # 3) Fixed quick-commands
    return _quick_handle(text)


def _log(msg: str) -> None:
    try:
        print(msg, flush=True)
    except Exception:
        pass

ROOT = Path(__file__).resolve().parent
CFG_PATH = ROOT / "telegram_config.json"


def _cfg() -> dict:
    return json.loads(CFG_PATH.read_text(encoding="utf-8"))


def _save_cfg(cfg: dict) -> None:
    CFG_PATH.write_text(json.dumps(cfg, indent=2), encoding="utf-8")


def _api(method: str, params: dict | None = None, timeout: int = 65) -> dict:
    token = _cfg()["token"]
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = urllib.parse.urlencode(params or {}).encode()
    req = urllib.request.Request(url, data=data)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def send(chat_id: str | int, text: str) -> None:
    _api("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})


def send_typing(chat_id: str | int) -> None:
    """Show the 'typing…' indicator while the agent works (calls can take ~60s)."""
    try:
        _api("sendChatAction", {"chat_id": chat_id, "action": "typing"}, timeout=10)
    except Exception:
        pass


def send_report() -> None:
    cfg = _cfg()
    if not cfg.get("chat_id"):
        print("No chat_id yet — message the bot once so it can learn your chat.")
        return
    send(cfg["chat_id"], _summary())
    print("report sent.")


def poll_forever() -> None:
    offset = 0
    _log("Telegram bot live.")
    # greet the saved chat on startup if known
    cfg = _cfg()
    if cfg.get("chat_id"):
        try:
            send(cfg["chat_id"], "🤖 Portfolio OS bot מחובר. שלח 'מצב', 'דוח', 'סנכרן' או 'קניתי BTC 0.01 188000'.")
        except Exception:
            pass
    while True:
        try:
            res = _api("getUpdates", {"offset": offset, "timeout": 50})
        except Exception as exc:
            _log("poll error: " + str(exc)[:120])
            time.sleep(5)
            continue
        for u in res.get("result", []):
            offset = u["update_id"] + 1
            # one bad message must never kill the loop
            try:
                msg = u.get("message") or u.get("edited_message")
                if not msg:
                    continue
                chat_id = msg.get("chat", {}).get("id")
                text = msg.get("text", "")
                # persist chat_id on first contact
                cfg = _cfg()
                if str(cfg.get("chat_id") or "") != str(chat_id):
                    cfg["chat_id"] = chat_id
                    _save_cfg(cfg)
                    _log(f"learned chat_id: {chat_id}")
                send_typing(chat_id)
                try:
                    reply = handle(text)
                except Exception as exc:
                    reply = f"שגיאה: {str(exc)[:200]}"
                send(chat_id, reply)
                _log(f"  handled update {offset}")
            except Exception as exc:
                _log(f"  update error: {str(exc)[:120]}")


if __name__ == "__main__":
    if "--report" in sys.argv:
        send_report()
    else:
        poll_forever()
