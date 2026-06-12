"""AI agent brain for the Telegram bot — turns any free-text prompt into actions.

Uses the Anthropic Messages API with a manual tool-use agentic loop. Claude
decides which portfolio tools to call (read / add / edit / delete / sync /
crypto-exposure), executes them against the Google Sheet via portfolio_cli, and
replies in natural language — exactly like chatting with Claude here.

Activation: put your Anthropic key in telegram_config.json as "anthropic_api_key"
(or set ANTHROPIC_API_KEY). Optional "agent_model" overrides the default.
Without a key, the bot falls back to the fixed quick-commands.
"""
from __future__ import annotations

import json
from pathlib import Path

import portfolio_cli

ROOT = Path(__file__).resolve().parent
CFG_PATH = ROOT / "telegram_config.json"
# Skill-mandated default; override via telegram_config.json "agent_model" (e.g. "claude-fable-5").
DEFAULT_MODEL = "claude-opus-4-8"
CLOSED = {"סגור", "closed", "close", "sold", "נמכר"}
CRYPTO = {"BTC", "ETH", "SOL", "XRP", "USDC", "LTC", "BCH"}


def _cfg() -> dict:
    try:
        return json.loads(CFG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def agent_available() -> bool:
    import os
    return bool(_cfg().get("anthropic_api_key") or os.environ.get("ANTHROPIC_API_KEY"))


# ── Tool implementations (return strings — what Claude sees as tool_result) ──

def _num(x) -> float:
    try:
        return float(str(x).replace("₪", "").replace("$", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _snapshot_rows():
    data = portfolio_cli.call("read_snapshot")["data"]
    headers = [h.strip() for h in data["headers"]]
    ix = {h: i for i, h in enumerate(headers)}
    rows = []
    for r in data["rows"]:
        rows.append({h: (r[ix[h]] if ix[h] < len(r) else "") for h in headers})
    return rows


def tool_get_portfolio(_args) -> str:
    rows = _snapshot_rows()
    tv = tc = 0.0
    nopen = nclosed = 0
    for r in rows:
        if str(r.get("סטטוס", "")).lower() in CLOSED:
            nclosed += 1
            continue
        nopen += 1
        tv += _num(r.get("שווי ILS"))
        tc += _num(r.get("עלות ILS"))
    pnl = tv - tc
    return json.dumps({
        "total_value_ils": round(tv), "total_cost_ils": round(tc),
        "open_pnl_ils": round(pnl), "return_pct": round(pnl / tc * 100, 2) if tc else 0,
        "open_positions": nopen, "closed_positions": nclosed, "total_rows": len(rows),
    }, ensure_ascii=False)


def tool_list_trades(args) -> str:
    rows = _snapshot_rows()
    tkr = (args.get("ticker") or "").upper()
    status = args.get("status")  # open|closed|all
    out = []
    for r in rows:
        is_closed = str(r.get("סטטוס", "")).lower() in CLOSED
        if status == "open" and is_closed:
            continue
        if status == "closed" and not is_closed:
            continue
        if tkr and str(r.get("טיקר", "")).upper() != tkr:
            continue
        out.append({
            "ticker": r.get("טיקר"), "platform": r.get("פלטפורמה"),
            "date": str(r.get("תאריך רכישה"))[:10], "qty": r.get("כמות"),
            "status": r.get("סטטוס"), "value_ils": r.get("שווי ILS"),
            "trade_id": r.get("Trade_ID"),
        })
    return json.dumps(out[:60], ensure_ascii=False)


def tool_add_trade(args) -> str:
    trade = {
        "Platform": args["platform"],
        "Current_Location": args.get("location") or args["platform"],
        "Type": args.get("asset_type") or ("קריפטו" if args["ticker"].upper() in CRYPTO else "שוק ההון"),
        "Ticker": args["ticker"].upper(),
        "Purchase_Date": args["purchase_date"],
        "Quantity": float(args["quantity"]),
        "Origin_Buy_Price": float(args.get("buy_price") or 0),
        "Cost_Origin": float(args.get("cost_origin") or (float(args["quantity"]) * float(args.get("buy_price") or 0))),
        "Origin_Currency": (args.get("currency") or "ILS").upper(),
        "Commission": float(args.get("commission") or 0),
        "Status": args.get("status") or "פתוח",
    }
    return json.dumps(portfolio_cli.call("add", {"trade": trade}), ensure_ascii=False)


def tool_edit_trade(args) -> str:
    if not args.get("trade_id"):
        return "error: trade_id required (get it from list_trades)"
    trade = {"Trade_ID": args["trade_id"]}
    field_map = {"platform": "Platform", "location": "Current_Location", "asset_type": "Type",
                 "ticker": "Ticker", "purchase_date": "Purchase_Date", "quantity": "Quantity",
                 "buy_price": "Origin_Buy_Price", "cost_origin": "Cost_Origin",
                 "currency": "Origin_Currency", "commission": "Commission", "status": "Status"}
    for k, col in field_map.items():
        if k in args and args[k] is not None:
            trade[col] = args[k]
    return json.dumps(portfolio_cli.call("edit", {"trade": trade}), ensure_ascii=False)


def tool_delete_trade(args) -> str:
    if not args.get("trade_id"):
        return "error: trade_id required"
    return json.dumps(portfolio_cli.call("delete", {"trade": {"Trade_ID": args["trade_id"]}}), ensure_ascii=False)


def tool_sync_bit2c(_args) -> str:
    import bit2c_sync
    seen = {(r["ticker"], r["date"], round(r["qty"], 8)) for r in bit2c_sync.fetch_sheet_rows()}
    missing = [t for t in bit2c_sync.bit2c_trades()
               if (t["Ticker"], t["Purchase_Date"], round(t["Quantity"], 8)) not in seen]
    for t in missing:
        portfolio_cli.call("add", {"trade": t})
    return json.dumps({"added": len(missing), "trades": [f"{t['Ticker']} {t['Quantity']}" for t in missing]}, ensure_ascii=False)


def tool_crypto_exposure(_args) -> str:
    rows = _snapshot_rows()
    etf_map = {"BTC": "IBIT", "ETH": "ETHA", "SOL": "BSOL"}
    out = {}
    for coin, etf in etf_map.items():
        direct = sum(_num(r.get("כמות")) for r in rows
                     if str(r.get("טיקר", "")).upper() == coin and str(r.get("סטטוס", "")).lower() not in CLOSED)
        coin_spot = next((_num(r.get("שער נוכחי ILS")) for r in rows
                          if str(r.get("טיקר", "")).upper() == coin and _num(r.get("שער נוכחי ILS")) > 0), 0)
        etf_val = sum(_num(r.get("שווי ILS")) for r in rows
                      if str(r.get("טיקר", "")).upper() == etf and str(r.get("סטטוס", "")).lower() not in CLOSED)
        etf_equiv = etf_val / coin_spot if coin_spot else 0
        out[coin] = {"direct": round(direct, 8), "via_etf": round(etf_equiv, 8), "total": round(direct + etf_equiv, 8)}
    return json.dumps(out, ensure_ascii=False)


TOOL_IMPLS = {
    "get_portfolio": tool_get_portfolio, "list_trades": tool_list_trades,
    "add_trade": tool_add_trade, "edit_trade": tool_edit_trade,
    "delete_trade": tool_delete_trade, "sync_bit2c": tool_sync_bit2c,
    "crypto_exposure": tool_crypto_exposure,
}

TOOLS = [
    {"name": "get_portfolio", "description": "Get portfolio summary: total value, cost, P&L, return %, open/closed counts (ILS).",
     "input_schema": {"type": "object", "properties": {}}},
    {"name": "list_trades", "description": "List trades. Optionally filter by ticker and/or status. Returns trade_id needed for edit/delete.",
     "input_schema": {"type": "object", "properties": {
         "ticker": {"type": "string"}, "status": {"type": "string", "enum": ["open", "closed", "all"]}}}},
    {"name": "add_trade", "description": "Add a new trade to the sheet (syncs to all apps).",
     "input_schema": {"type": "object", "properties": {
         "platform": {"type": "string"}, "location": {"type": "string"}, "asset_type": {"type": "string"},
         "ticker": {"type": "string"}, "purchase_date": {"type": "string", "description": "YYYY-MM-DD"},
         "quantity": {"type": "number"}, "buy_price": {"type": "number"}, "cost_origin": {"type": "number"},
         "currency": {"type": "string", "enum": ["ILS", "USD"]}, "commission": {"type": "number"},
         "status": {"type": "string"}},
         "required": ["platform", "ticker", "purchase_date", "quantity"]}},
    {"name": "edit_trade", "description": "Edit an existing trade by Trade_ID (get it via list_trades).",
     "input_schema": {"type": "object", "properties": {
         "trade_id": {"type": "string"}, "quantity": {"type": "number"}, "buy_price": {"type": "number"},
         "cost_origin": {"type": "number"}, "commission": {"type": "number"}, "status": {"type": "string"},
         "purchase_date": {"type": "string"}}, "required": ["trade_id"]}},
    {"name": "delete_trade", "description": "Delete a trade by Trade_ID.",
     "input_schema": {"type": "object", "properties": {"trade_id": {"type": "string"}}, "required": ["trade_id"]}},
    {"name": "sync_bit2c", "description": "Pull any new trades from the Bit2C exchange into the sheet.",
     "input_schema": {"type": "object", "properties": {}}},
    {"name": "crypto_exposure", "description": "Total BTC/ETH/SOL exposure including ETF-equivalent (IBIT/ETHA/BSOL).",
     "input_schema": {"type": "object", "properties": {}}},
]

SYSTEM = """You are Ouriel's personal AI portfolio manager, reachable over Telegram.
His investment portfolio lives in a Google Sheet that four apps (Streamlit, Flet desktop,
two Next.js apps) all read from — so any change you make appears everywhere automatically.

You can do ANYTHING he asks about his portfolio using your tools: read holdings, add/edit/
delete trades, sync the Bit2C exchange, and compute crypto exposure. You are the owner's
assistant — act on his requests directly; you don't need to ask permission for routine
reads or for trades he clearly asked for. For a destructive/ambiguous action, confirm the
specifics in one line first.

Rules:
- Reply in Hebrew, concise and friendly (this is a phone chat). Use ₪ and thousands separators.
- Trade_ID is derived by the server — never invent one; get it from list_trades for edit/delete.
- Dates are YYYY-MM-DD. Currency is ILS or USD. Bit2C trades are usually ILS; brokers often USD.
- When you add/edit/delete, confirm what changed and the new totals if relevant.
- If asked something you have no tool for, answer helpfully from what you can read."""


def run_agent(user_text: str) -> str:
    """Single-turn agentic loop. Returns the final Hebrew reply text."""
    import os
    import anthropic

    cfg = _cfg()
    api_key = cfg.get("anthropic_api_key") or os.environ.get("ANTHROPIC_API_KEY")
    model = cfg.get("agent_model") or DEFAULT_MODEL
    client = anthropic.Anthropic(api_key=api_key)

    messages = [{"role": "user", "content": user_text}]
    for _ in range(12):  # safety cap on tool-call rounds
        resp = client.messages.create(
            model=model, max_tokens=4096, system=SYSTEM, tools=TOOLS, messages=messages,
        )
        if resp.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": resp.content})
            results = []
            for block in resp.content:
                if block.type == "tool_use":
                    impl = TOOL_IMPLS.get(block.name)
                    try:
                        out = impl(block.input) if impl else f"unknown tool {block.name}"
                    except Exception as exc:
                        out = f"error: {str(exc)[:300]}"
                    results.append({"type": "tool_result", "tool_use_id": block.id, "content": out})
            messages.append({"role": "user", "content": results})
            continue
        # end_turn (or anything else) — return the text
        return "".join(b.text for b in resp.content if b.type == "text").strip() or "✓"
    return "הפעולה דרשה יותר מדי צעדים — נסה לנסח מחדש."
