"""Horizon → Google-Sheet importer. Parses the TradeTransactionReport CSV that
you export from Horizon and adds any missing trades to the sheet.

Run:  python horizon_import.py <path-to-horizon.csv>            # dry-run
      python horizon_import.py <path-to-horizon.csv> --apply    # add missing

Drop the CSV in DATA/ and point this at it; idempotent (matches by
ticker+date+qty, so re-importing the same file adds nothing).
"""
from __future__ import annotations

import csv
import json
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CFG = json.loads((ROOT / "app_local_config.json").read_text(encoding="utf-8"))
PLATFORM = "הורייזון"

# Horizon pairs are <COIN>/USD; map to our ticker + asset type.
CRYPTO = {"BTC", "ETH", "SOL", "XRP", "USDC", "LTC", "BCH"}


def _num(x) -> float:
    try:
        return float(str(x).replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0


def fetch_sheet_keys() -> set:
    body = json.dumps({"token": CFG["api_token"], "action": "read_snapshot"}).encode()
    req = urllib.request.Request(CFG["web_app_url"], data=body,
                                 headers={"Content-Type": "application/json"})
    d = json.loads(urllib.request.urlopen(req, timeout=45).read())
    headers = [h.strip() for h in d["data"]["headers"]]
    ix = {h: i for i, h in enumerate(headers)}

    def g(r, k):
        i = ix.get(k, -1)
        return str(r[i]).strip() if 0 <= i < len(r) else ""

    keys = set()
    for r in d["data"]["rows"]:
        t, dt, q = g(r, "טיקר").upper(), (g(r, "תאריך רכישה") or "")[:10], _num(g(r, "כמות"))
        if t and dt:
            keys.add((t, dt, round(q, 8)))
    return keys


def parse_horizon(path: Path) -> list[dict]:
    out = []
    with path.open(encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            if (row.get("Status") or "").strip().lower() != "complete":
                continue
            pair = (row.get("Pair") or "").strip()
            coin = pair.split("/")[0].upper()
            qty = _num(row.get("Quantity"))
            price = _num(row.get("Price"))
            fee = _num(row.get("Fee"))
            if qty <= 0 or price <= 0:
                continue
            date = (row.get("Date") or "")[:10]
            side = (row.get("Side") or "Buy").strip().lower()
            cur = (row.get("Currency") or "USD").strip().upper()
            out.append({
                "Platform": PLATFORM,
                "Current_Location": PLATFORM,
                "Type": "קריפטו" if coin in CRYPTO else "שוק ההון",
                "Ticker": coin,
                "Purchase_Date": date,
                "Quantity": round(qty, 8),
                "Origin_Buy_Price": price,
                "Cost_Origin": round(qty * price + fee, 2),
                "Origin_Currency": cur,
                "Commission": round(fee, 2),
                "Status": "סגור" if side == "sell" else "פתוח",
            })
    return out


def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        sys.exit("usage: python horizon_import.py <horizon.csv> [--apply]")
    path = Path(args[0])
    if not path.exists():
        sys.exit(f"file not found: {path}")
    apply = "--apply" in sys.argv

    keys = fetch_sheet_keys()
    trades = parse_horizon(path)
    missing = [t for t in trades
               if (t["Ticker"], t["Purchase_Date"], round(t["Quantity"], 8)) not in keys]
    print(f"Horizon trades in file: {len(trades)} | already in sheet: {len(trades) - len(missing)} | MISSING: {len(missing)}")
    for t in missing:
        print(f"  + {t['Ticker']} {t['Quantity']} @ {t['Origin_Buy_Price']} {t['Origin_Currency']} on {t['Purchase_Date']} ({t['Status']})")
    if not missing:
        print("Nothing to add.")
        return
    if not apply:
        print("\nDRY-RUN. Re-run with --apply to add.")
        return
    import portfolio_cli
    for t in missing:
        out = portfolio_cli.call("add", {"trade": t})
        print(f"  added {t['Ticker']} {t['Quantity']} -> {out.get('trade_id')}")


if __name__ == "__main__":
    main()
