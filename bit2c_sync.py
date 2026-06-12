"""Bit2C → Google-Sheet AUTO-SYNC. Fully automatic (uses bit2c_api.json).

Pulls Bit2C order history, matches each completed trade against the sheet by
(ticker, date, quantity), and adds any that are missing via portfolio_cli's
Apps-Script bridge. Idempotent — re-running never duplicates.

Run:  python bit2c_sync.py            # dry-run (shows what WOULD be added)
      python bit2c_sync.py --apply    # actually add missing trades

Designed to be scheduled (Task Scheduler / cron) for hands-off syncing.
"""
from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

import bit2c_check as b  # private_get / credentials

ROOT = Path(__file__).resolve().parent
CFG = json.loads((ROOT / "app_local_config.json").read_text(encoding="utf-8"))

PAIR_TO_TICKER = {
    "BtcNis": "BTC", "EthNis": "ETH", "SolNis": "SOL", "XrpNis": "XRP",
    "UsdcNis": "USDC", "LtcNis": "LTC", "BchNis": "BCH",
}
PLATFORM = "Bit2C"


def _num(x) -> float:
    try:
        return float(str(x).replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _iso_date(created: str) -> str:
    # Bit2C format "04/06/26 21:55" (DD/MM/YY) -> "2026-06-04"
    d = created.split(" ")[0]
    parts = d.split("/")
    if len(parts) == 3:
        dd, mm, yy = parts
        yyyy = "20" + yy if len(yy) == 2 else yy
        return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
    return d


def fetch_sheet_rows() -> list[dict]:
    body = json.dumps({"token": CFG["api_token"], "action": "read_snapshot"}).encode()
    req = urllib.request.Request(CFG["web_app_url"], data=body,
                                 headers={"Content-Type": "application/json"})
    d = json.loads(urllib.request.urlopen(req, timeout=45).read())
    headers = [h.strip() for h in d["data"]["headers"]]
    ix = {h: i for i, h in enumerate(headers)}

    def g(r, k):
        i = ix.get(k, -1)
        return str(r[i]).strip() if 0 <= i < len(r) else ""

    out = []
    for r in d["data"]["rows"]:
        out.append({
            "ticker": g(r, "טיקר").upper(),
            "date": (g(r, "תאריך רכישה") or "")[:10],
            "qty": _num(g(r, "כמות")),
            "platform": g(r, "פלטפורמה"),
        })
    return out


def bit2c_trades() -> list[dict]:
    trades = []
    for h in b.private_get("/Order/OrderHistory"):
        pair = h.get("pair")
        ticker = PAIR_TO_TICKER.get(pair)
        if not ticker:
            continue
        qty = abs(_num(h.get("firstAmount") or h.get("amount")))
        price = _num(h.get("price"))
        fee = _num(h.get("feeAmount"))
        if qty <= 0 or price <= 0:
            continue
        side = "SELL" if h.get("action") == 1 else "BUY"
        trades.append({
            "Platform": PLATFORM,
            "Current_Location": PLATFORM,
            "Type": "קריפטו",
            "Ticker": ticker,
            "Purchase_Date": _iso_date(h.get("created", "")),
            "Quantity": round(qty, 8),
            "Origin_Buy_Price": price,
            "Cost_Origin": round(qty * price + fee, 2),
            "Origin_Currency": "ILS",
            "Commission": round(fee, 2),
            "Status": "סגור" if side == "SELL" else "פתוח",
        })
    return trades


def main() -> None:
    apply = "--apply" in sys.argv
    existing = fetch_sheet_rows()
    # dedup key tolerant to cost/fee rounding: ticker + date + qty(8dp)
    seen = {(r["ticker"], r["date"], round(r["qty"], 8)) for r in existing
            if r["ticker"] and r["date"]}

    candidates = bit2c_trades()
    missing = [t for t in candidates
               if (t["Ticker"], t["Purchase_Date"], round(t["Quantity"], 8)) not in seen]

    print(f"Bit2C trades: {len(candidates)} | already in sheet: {len(candidates) - len(missing)} | MISSING: {len(missing)}")
    for t in missing:
        print(f"  + {t['Ticker']} {t['Quantity']} @ {t['Origin_Buy_Price']} on {t['Purchase_Date']} ({t['Status']})")

    if not missing:
        print("Sheet is in sync with Bit2C. Nothing to add.")
        return
    if not apply:
        print("\nDRY-RUN. Re-run with --apply to add the above to the sheet.")
        return

    import portfolio_cli
    for t in missing:
        out = portfolio_cli.call("add", {"trade": t})
        print(f"  added {t['Ticker']} {t['Quantity']} -> {out.get('trade_id')}")


if __name__ == "__main__":
    main()
