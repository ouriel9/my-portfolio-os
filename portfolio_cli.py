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
import datetime
import json
import math
import re
import sys
import urllib.request
import uuid
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
        data=json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            body = r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        sys.exit(f"HTTP {e.code}: {e.reason}")
    except (urllib.error.URLError, socket.timeout) as e:
        if action in ("add", "edit", "delete"):
            sys.exit(f"Network error during {action}: {e}\n"
                     "Write may or may not have completed (no auto-retry, no server idempotency). "
                     "Run `snapshot` to check BEFORE retrying to avoid a duplicate.")
        sys.exit(f"Network error: {e}")
    try:
        out = json.loads(body)
    except json.JSONDecodeError:
        sys.exit(f"Server returned non-JSON (likely auth/redirect): {body[:200]}")
    if not out.get("ok"):
        sys.exit(f"API error: {out.get('error')}")
    return out


def _valid_date(s: str) -> bool:
    try:
        datetime.date.fromisoformat(s)
        return True
    except ValueError:
        return False


def _confirm_write(args, action: str, summary: str, payload: dict) -> None:
    """Echo a human-readable summary and require --yes (or --dry-run) before any write."""
    print(f"{action}: {summary}", file=sys.stderr)
    if getattr(args, "dry_run", False):
        print(json.dumps({"dry_run": True, "action": action, "payload": payload}, ensure_ascii=False, indent=2))
        sys.exit(0)
    if getattr(args, "yes", False):
        return
    if not sys.stdin.isatty():
        sys.exit(f"refusing to {action} without --yes (non-interactive); re-run with --yes or --dry-run")
    try:
        ans = input(f"Proceed with {action}? [y/N] ").strip().lower()
    except EOFError:
        sys.exit(f"refusing to {action} without --yes (no input available); re-run with --yes or --dry-run")
    if ans not in ("y", "yes"):
        sys.exit("aborted")


SNAPSHOT_FIELD_ALIASES = {
    "ticker": ("טיקר", "Ticker"),
    "platform": ("פלטפורמה", "Platform"),
    "qty": ("כמות", "Quantity"),
    "status": ("סטטוס", "Status"),
    "trade_id": ("Trade_ID",),
}


def cmd_snapshot(_args) -> None:
    data = call("read_snapshot").get("data")
    if not isinstance(data, dict) or "headers" not in data or "rows" not in data:
        sys.exit("Malformed snapshot response from server")
    headers, rows = data["headers"], data["rows"]
    if not isinstance(headers, list) or not isinstance(rows, list):
        sys.exit("Malformed snapshot response: headers/rows must be lists")
    ix = {h: i for i, h in enumerate(headers)}
    def col(field):
        for alias in SNAPSHOT_FIELD_ALIASES[field]:
            if alias in ix:
                return ix[alias]
        return None
    if col("trade_id") is None:
        print("warning: snapshot has no Trade_ID column; Trade_ID values will be blank", file=sys.stderr)
    closed_set = {"סגור", "closed", "close", "sold", "נמכר"}
    n_open = n_closed = 0
    print(f"{'Ticker'[:8]:8s} {'Platform'[:14]:14s} {'Qty'[:12]:>12s} {'Status'[:8]:8s} {'Trade_ID'[:16]:16s}")
    for r in rows:
        if not isinstance(r, list):
            sys.exit("Malformed snapshot response: each row must be a list")
        def get(field):
            i = col(field)
            return str(r[i]).strip() if i is not None and i < len(r) else ""
        status = get("status")
        closed = status.lower() in closed_set
        n_closed += closed
        n_open += not closed
        print(f"{get('ticker')[:8]:8s} {get('platform')[:14]:14s} {get('qty')[:12]:>12s} {status[:8]:8s} {get('trade_id')[:16]:16s}")
    print(f"\n{len(rows)} rows | {n_open} open | {n_closed} closed")


CURRENCY_ALLOWLIST = ("ILS", "USD", "EUR", "GBP", "GBX", "ILA")
_MAX_MAGNITUDE = 1e15


def _no_dup_keys(pairs):
    seen = {}
    for k, v in pairs:
        if k in seen:
            raise ValueError(f"duplicate key not allowed: {k!r}")
        seen[k] = v
    return seen


def _parse_trade_json(raw: str) -> dict:
    if not raw or not raw.strip():
        sys.exit("--json must not be empty")
    def _no_const(c):
        raise ValueError(f"non-finite number not allowed: {c}")
    try:
        trade = json.loads(raw, parse_constant=_no_const, object_pairs_hook=_no_dup_keys)
    except json.JSONDecodeError as e:
        sys.exit(f"--json is not valid JSON: {e}")
    except ValueError as e:
        sys.exit(f"--json is not valid JSON: {e}")
    if not isinstance(trade, dict):
        sys.exit("--json must be a JSON object {…}, not a list or primitive")
    for k, v in trade.items():
        if not isinstance(v, (str, int, float, bool)) and v is not None:
            sys.exit(f"--json field {k!r} must be a scalar (string/number/bool/null), not a list/object")
    return trade


def _validate_number(trade: dict, key: str, *, allow_negative: bool, require_positive: bool) -> None:
    if key not in trade or (isinstance(trade.get(key), str) and not str(trade.get(key)).strip()):
        return
    v = trade.get(key)
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        sys.exit(f"{key} must be a number (not bool/string/list)")
    fv = float(v)
    if not math.isfinite(fv):
        sys.exit(f"{key} must be a finite number")
    if abs(fv) > _MAX_MAGNITUDE:
        sys.exit(f"{key} magnitude is implausibly large (max {_MAX_MAGNITUDE:g})")
    if require_positive and fv <= 0:
        sys.exit(f"{key} must be greater than 0")
    if not allow_negative and fv < 0:
        sys.exit(f"{key} must not be negative")


def cmd_add(args) -> None:
    trade = _parse_trade_json(args.json)
    ticker = str(trade.get("Ticker", "")).strip()
    if not ticker:
        sys.exit("add requires a non-empty Ticker")
    qty_raw = trade.get("Quantity")
    if isinstance(qty_raw, bool) or not isinstance(qty_raw, (int, float)):
        sys.exit("add requires a numeric Quantity (number, not bool/string)")
    qty = float(qty_raw)
    if not math.isfinite(qty) or qty <= 0:
        sys.exit("add requires a finite Quantity greater than 0")
    if qty > _MAX_MAGNITUDE:
        sys.exit(f"add Quantity magnitude is implausibly large (max {_MAX_MAGNITUDE:g})")
    pdate = str(trade.get("Purchase_Date", "")).strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", pdate) or not _valid_date(pdate):
        sys.exit("add requires a valid calendar Purchase_Date in YYYY-MM-DD format")
    sdate = str(trade.get("Sell_Date", "")).strip()
    if sdate and (not re.match(r"^\d{4}-\d{2}-\d{2}$", sdate) or not _valid_date(sdate)):
        sys.exit("add requires a valid calendar Sell_Date in YYYY-MM-DD format")
    if "Origin_Currency" in trade:
        cur = str(trade.get("Origin_Currency", "")).strip().upper()
        if cur and cur not in CURRENCY_ALLOWLIST:
            sys.exit(f"add requires Origin_Currency in {{{', '.join(CURRENCY_ALLOWLIST)}}}")
    _validate_number(trade, "Origin_Buy_Price", allow_negative=False, require_positive=False)
    _validate_number(trade, "Cost_Origin", allow_negative=False, require_positive=False)
    _validate_number(trade, "Commission", allow_negative=False, require_positive=False)

    def _num(k):
        v = trade.get(k)
        return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else 0.0
    cost = _num("Cost_Origin")
    price = _num("Origin_Buy_Price")
    if not (cost > 0 or (qty > 0 and price > 0)):
        sys.exit("add requires Cost_Origin>0 or (Quantity>0 and Origin_Buy_Price>0)")
    _confirm_write(args, "add", f"{ticker} qty={qty} date={pdate}", {"trade": trade})
    out = call("add", {"trade": trade, "client_op_id": uuid.uuid4().hex})
    print(json.dumps(out, ensure_ascii=False, indent=2))


EDIT_FULL_FIELDS = (
    "Platform", "Current_Location", "Type", "Ticker", "Purchase_Date",
    "Quantity", "Origin_Buy_Price", "Cost_Origin", "Origin_Currency",
    "Commission", "Status",
)


def cmd_edit(args) -> None:
    trade = _parse_trade_json(args.json)
    tid = str(trade.get("Trade_ID", "")).strip()
    if not tid:
        sys.exit("edit requires a non-empty Trade_ID in the JSON")
    changed = [k for k in trade if k != "Trade_ID" and str(trade.get(k, "")).strip() != ""]
    if not changed:
        sys.exit("edit requires at least one field to change besides Trade_ID "
                 "(a Trade_ID-only payload would zero Quantity/Cost_Origin/Ticker server-side)")
    if "Quantity" in trade and str(trade.get("Quantity", "")).strip() != "":
        _validate_number(trade, "Quantity", allow_negative=False, require_positive=True)
    _validate_number(trade, "Origin_Buy_Price", allow_negative=False, require_positive=False)
    _validate_number(trade, "Cost_Origin", allow_negative=False, require_positive=False)
    _validate_number(trade, "Commission", allow_negative=False, require_positive=False)
    for dk in ("Purchase_Date", "Sell_Date"):
        dv = str(trade.get(dk, "")).strip()
        if dv and (not re.match(r"^\d{4}-\d{2}-\d{2}$", dv) or not _valid_date(dv)):
            sys.exit(f"edit requires a valid calendar {dk} in YYYY-MM-DD format")
    if "Origin_Currency" in trade:
        cur = str(trade.get("Origin_Currency", "")).strip().upper()
        if cur and cur not in CURRENCY_ALLOWLIST:
            sys.exit(f"edit requires Origin_Currency in {{{', '.join(CURRENCY_ALLOWLIST)}}}")
    missing = [f for f in EDIT_FULL_FIELDS
               if f not in trade or str(trade.get(f, "")).strip() == ""]
    if missing and not getattr(args, "force_overwrite", False):
        sys.exit(
            "edit is a FULL-ROW overwrite server-side: omitted fields "
            f"({', '.join(missing)}) would be zeroed/blanked. Supply every field, "
            "or pass --force-overwrite to intentionally overwrite the omitted fields.")
    summary = f"Trade_ID={tid} fields={','.join(changed)}"
    if missing and getattr(args, "force_overwrite", False):
        summary += f" | OVERWRITING (zeroing) omitted: {','.join(missing)}"
    _confirm_write(args, "edit", summary, {"trade": trade})
    out = call("edit", {"trade": trade, "client_op_id": uuid.uuid4().hex})
    print(json.dumps(out, ensure_ascii=False, indent=2))


def cmd_delete(args) -> None:
    tid = str(args.id).strip()
    if not tid:
        sys.exit("delete requires a non-empty Trade_ID")
    _confirm_write(args, "delete", f"Trade_ID={tid}", {"trade": {"Trade_ID": tid}})
    out = call("delete", {"trade": {"Trade_ID": tid}, "client_op_id": uuid.uuid4().hex})
    print(json.dumps(out, ensure_ascii=False, indent=2))
    print("note: verify the server response above identifies the intended row "
          "(row/ticker/qty); a by-id delete that missed may have matched a zeroed row.", file=sys.stderr)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter, allow_abbrev=False)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("snapshot").set_defaults(fn=cmd_snapshot)
    a = sub.add_parser("add", allow_abbrev=False); a.add_argument("--json", required=True); a.add_argument("--yes", action="store_true"); a.add_argument("--dry-run", action="store_true"); a.set_defaults(fn=cmd_add)
    e = sub.add_parser("edit", allow_abbrev=False); e.add_argument("--json", required=True); e.add_argument("--yes", action="store_true"); e.add_argument("--dry-run", action="store_true"); e.add_argument("--force-overwrite", action="store_true"); e.set_defaults(fn=cmd_edit)
    d = sub.add_parser("delete", allow_abbrev=False); d.add_argument("--id", required=True); d.add_argument("--yes", action="store_true"); d.add_argument("--dry-run", action="store_true"); d.set_defaults(fn=cmd_delete)
    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
