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

from whatsapp_bot import handle, _summary  # reuse the command logic

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


def send_report() -> None:
    cfg = _cfg()
    if not cfg.get("chat_id"):
        print("No chat_id yet — message the bot once so it can learn your chat.")
        return
    send(cfg["chat_id"], _summary())
    print("report sent.")


def poll_forever() -> None:
    offset = 0
    print("Telegram bot live. Send it a message…")
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
            print("poll error:", str(exc)[:120])
            time.sleep(5)
            continue
        for u in res.get("result", []):
            offset = u["update_id"] + 1
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
                print("learned chat_id:", chat_id)
            try:
                reply = handle(text)
            except Exception as exc:
                reply = f"שגיאה: {str(exc)[:200]}"
            send(chat_id, reply)
            print(f"  <- {text!r}  -> {reply[:60]!r}")


if __name__ == "__main__":
    if "--report" in sys.argv:
        send_report()
    else:
        poll_forever()
