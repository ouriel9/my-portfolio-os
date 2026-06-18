"""Portfolio OS — command-line bridge to the Google Sheet (via Apps Script).

This is the integration point for AI-assisted workflows (e.g. "here's a
screenshot of a buy I made — add it"): a small, safe CLI around the same
Code.gs contract the apps use. Uses app_local_config.json for URL + token.

Usage:
  python portfolio_cli.py snapshot              # print holdings summary
  python portfolio_cli.py add --json "{...}"    # add a trade (canonical fields)
  python portfolio_cli.py edit --json "{...}"   # edit by Trade_ID
  python portfolio_cli.py delete --id TRADE_ID  # delete by Trade_ID

Trade JSON fields (Hebrew sheet contract, see Code.gs sanitizeTrade_):
  Platform, Current_Location, Type, Ticker, Purchase_Date (YYYY-MM-DD),
  Quantity, Origin_Buy_Price, Cost_Origin, Origin_Currency (ILS/USD),
  Commission, Status (פתוח/סגור). Trade_ID auto-derived if omitted on add.
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from pathlib import Path

# Windows consoles default to cp1252 and crash when printing Hebrew ticker/status
# values. Make stdout/stderr UTF-8 so the snapshot never dies on encoding.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "app_local_config.json"


def _config() -> dict:
    try:
        cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    except FileNotFoundError:
        sys.exit(f"Config not found: {CONFIG}")
    except json.JSONDecodeError as e:
        sys.exit(f"Config is not valid JSON: {e}")
    if not cfg.get("web_app_url") or not cfg.get("api_token"):
        sys.exit("app_local_config.json missing web_app_url/api_token")
    return cfg


def call(action: str, extra: dict | None = None) -> dict:
    import urllib.error, socket
    cfg = _config()
    payload = {"token": cfg["api_token"], "action": action}
    if extra:
        payload.update(extra)
    req = urllib.request.Request(
        cfg["web_app_url"].rstrip("/"),
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            body = r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        sys.exit(f"HTTP {e.code}: {e.reason}")
    except (urllib.error.URLError, socket.timeout) as e:
        sys.exit(f"Network error: {e}")
    try:
        out = json.loads(body)
    except json.JSONDecodeError:
        sys.exit(f"Server returned non-JSON (likely auth/redirect): {body[:200]}")
    if not out.get("ok"):
        sys.exit(f"API error: {out.get('error')}")
    return out


def cmd_snapshot(_args) -> None:
    data = call("read_snapshot")["data"]
    headers, rows = data["headers"], data["rows"]
    ix = {h: i for i, h in enumerate(headers)}
    closed_set = {"סגור", "closed", "close", "sold", "נמכר"}
    n_open = n_closed = 0
    print(f"{'Ticker':8s} {'Platform':14s} {'Qty':>12s} {'Status':8s} {'Trade_ID':16s}")
    for r in rows:
        get = lambda k: str(r[ix[k]]).strip() if k in ix and ix[k] < len(r) else ""
        status = get("סטטוס")
        closed = status.lower() in closed_set
        n_closed += closed
        n_open += not closed
        print(f"{get('טיקר'):8s} {get('פלטפורמה'):14s} {get('כמות'):>12s} {status:8s} {get('Trade_ID'):16s}")
    print(f"\n{len(rows)} rows | {n_open} open | {n_closed} closed")


def _parse_trade_json(raw: str) -> dict:
    if not raw or not raw.strip():
        sys.exit("--json must not be empty")
    try:
        trade = json.loads(raw)
    except json.JSONDecodeError as e:
        sys.exit(f"--json is not valid JSON: {e}")
    if not isinstance(trade, dict):
        sys.exit("--json must be a JSON object {…}, not a list or primitive")
    return trade


def cmd_add(args) -> None:
    trade = _parse_trade_json(args.json)
    out = call("add", {"trade": trade})
    print(json.dumps(out, ensure_ascii=False, indent=2))


def cmd_edit(args) -> None:
    trade = _parse_trade_json(args.json)
    if not str(trade.get("Trade_ID", "")).strip():
        sys.exit("edit requires a non-empty Trade_ID in the JSON")
    out = call("edit", {"trade": trade})
    print(json.dumps(out, ensure_ascii=False, indent=2))


def cmd_delete(args) -> None:
    out = call("delete", {"trade": {"Trade_ID": args.id}})
    print(json.dumps(out, ensure_ascii=False, indent=2))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("snapshot").set_defaults(fn=cmd_snapshot)
    a = sub.add_parser("add"); a.add_argument("--json", required=True); a.set_defaults(fn=cmd_add)
    e = sub.add_parser("edit"); e.add_argument("--json", required=True); e.set_defaults(fn=cmd_edit)
    d = sub.add_parser("delete"); d.add_argument("--id", required=True); d.set_defaults(fn=cmd_delete)
    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
