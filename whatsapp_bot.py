"""WhatsApp ⇆ Portfolio OS bot (Twilio webhook).

Lets you command the portfolio from WhatsApp:
  "מצב" / "status"            → portfolio summary
  "דוח" / "report"            → monthly-style report
  "סנכרן" / "sync"            → pull new Bit2C trades into the sheet
  "קניתי BTC 0.01 188000"     → add a trade (ticker qty price [platform])
  "עזרה" / "help"             → command list

Every action updates the Google Sheet, so ALL apps (Streamlit / Flet / Next.js)
reflect it automatically.

SETUP (one-time, the only thing that needs you):
  1. Create a Twilio account → Messaging → WhatsApp Sandbox (free).
  2. pip install flask twilio
  3. Put creds in whatsapp_config.json (gitignored):
       {"account_sid": "...", "auth_token": "...",
        "from": "whatsapp:+14155238886", "to": "whatsapp:+972XXXXXXXXX"}
  4. Run: python whatsapp_bot.py   (exposes :5005)
  5. Expose it (ngrok http 5005) and set the Twilio sandbox "When a message
     comes in" webhook to  https://<ngrok>/whatsapp .
  6. monthly_report() can be scheduled to push you a summary each month.
"""
from __future__ import annotations

import json
from pathlib import Path

import portfolio_cli  # bridge to the sheet

ROOT = Path(__file__).resolve().parent
CRYPTO = {"BTC", "ETH", "SOL", "XRP", "USDC", "LTC", "BCH"}
CLOSED = {"סגור", "closed", "close", "sold", "נמכר"}


def _summary() -> str:
    data = portfolio_cli.call("read_snapshot")["data"]
    headers = [h.strip() for h in data["headers"]]
    ix = {h: i for i, h in enumerate(headers)}

    def g(r, k):
        i = ix.get(k, -1)
        return str(r[i]).strip() if 0 <= i < len(r) else ""

    def num(x):
        try:
            return float(str(x).replace("₪", "").replace(",", "").strip())
        except ValueError:
            return 0.0

    tv = tc = 0.0
    nopen = nclosed = 0
    for r in data["rows"]:
        if g(r, "סטטוס").lower() in CLOSED:
            nclosed += 1
            continue
        nopen += 1
        tv += num(g(r, "שווי ILS"))
        tc += num(g(r, "עלות ILS"))
    pnl = tv - tc
    ret = (pnl / tc * 100) if tc else 0.0
    return (f"📊 *Portfolio OS*\n"
            f"שווי כולל: ₪{tv:,.0f}\n"
            f"רווח/הפסד: ₪{pnl:,.0f} ({ret:+.2f}%)\n"
            f"פוזיציות: {nopen} פתוחות, {nclosed} סגורות")


def _add_trade(parts: list[str]) -> str:
    # "קניתי BTC 0.01 188000 [Bit2C]"
    try:
        ticker = parts[1].upper()
        qty = float(parts[2])
        price = float(parts[3])
        platform = parts[4] if len(parts) > 4 else "Bit2C"
    except (IndexError, ValueError):
        return "פורמט: קניתי <טיקר> <כמות> <מחיר> [פלטפורמה]"
    import datetime  # local date only for the trade record
    trade = {
        "Platform": platform, "Current_Location": platform,
        "Type": "קריפטו" if ticker in CRYPTO else "שוק ההון",
        "Ticker": ticker, "Purchase_Date": datetime.date.today().isoformat(),
        "Quantity": qty, "Origin_Buy_Price": price,
        "Cost_Origin": round(qty * price, 2),
        "Origin_Currency": "ILS" if platform.lower().startswith(("bit", "ביט")) else "USD",
        "Commission": 0, "Status": "פתוח",
    }
    out = portfolio_cli.call("add", {"trade": trade})
    return f"✅ נוסף {ticker} {qty} @ {price} (id {out.get('trade_id', '')[:8]})"


def handle(message: str) -> str:
    msg = (message or "").strip()
    low = msg.lower()
    parts = msg.split()
    if low in ("status", "מצב", "תיק"):
        return _summary()
    if low in ("report", "דוח"):
        return _summary()  # monthly report = same summary for now
    if low in ("sync", "סנכרן"):
        import bit2c_sync
        miss = [t for t in bit2c_sync.bit2c_trades()
                if (t["Ticker"], t["Purchase_Date"], round(t["Quantity"], 8))
                not in {(r["ticker"], r["date"], round(r["qty"], 8)) for r in bit2c_sync.fetch_sheet_rows()}]
        for t in miss:
            portfolio_cli.call("add", {"trade": t})
        return f"🔄 סונכרן Bit2C — נוספו {len(miss)} עסקאות חדשות"
    if low.startswith(("קניתי", "buy", "add")):
        return _add_trade(parts)
    return ("פקודות:\n• מצב — סיכום תיק\n• דוח — דוח חודשי\n"
            "• סנכרן — משיכת עסקאות Bit2C\n• קניתי BTC 0.01 188000 — הוספת עסקה")


# ── Flask webhook (only imported when actually serving) ──────────────────
def create_app():
    from flask import Flask, request, Response
    app = Flask(__name__)

    @app.route("/whatsapp", methods=["POST"])
    def whatsapp():
        body = request.form.get("Body", "")
        reply = handle(body)
        twiml = f"<?xml version='1.0' encoding='UTF-8'?><Response><Message>{reply}</Message></Response>"
        return Response(twiml, mimetype="application/xml")

    @app.route("/health")
    def health():
        return "ok"

    return app


def send_monthly(text: str | None = None) -> None:
    """Push a message to WhatsApp via Twilio (for the scheduled monthly report)."""
    cfg = json.loads((ROOT / "whatsapp_config.json").read_text(encoding="utf-8"))
    from twilio.rest import Client
    Client(cfg["account_sid"], cfg["auth_token"]).messages.create(
        from_=cfg["from"], to=cfg["to"], body=text or _summary())


if __name__ == "__main__":
    import sys
    if "--send" in sys.argv:
        send_monthly()
    elif "--test" in sys.argv:
        print(handle(" ".join(sys.argv[sys.argv.index("--test") + 1:]) or "מצב"))
    else:
        create_app().run(host="0.0.0.0", port=5005)
