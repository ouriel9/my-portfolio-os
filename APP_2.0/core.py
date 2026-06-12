from __future__ import annotations

import hashlib
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib import error as urlerror
from urllib import request as urlrequest

import numpy as np
import pandas as pd
import yfinance as yf

try:
    import gspread
except Exception:
    gspread = None


@dataclass
class AppConfig:
    web_app_url: str
    api_token: str
    spreadsheet_ref: str
    worksheet_name: str
    service_account_file: str
    language: str = "עברית"
    demo_mode: bool = False


if getattr(sys, "frozen", False):
    # When running as EXE, look for project data relative to the EXE location.
    # If the EXE is on the desktop (or elsewhere), fall back to the original project dir.
    _exe_dir = Path(sys.executable).resolve().parent
    _project_dir = Path(r"C:\Users\ourie\PycharmProjects\my-portfolio-os")
    if (_exe_dir / "app_local_config.json").exists():
        ROOT_DIR = _exe_dir
    elif _project_dir.exists():
        ROOT_DIR = _project_dir
    else:
        ROOT_DIR = _exe_dir
else:
    ROOT_DIR = Path(__file__).resolve().parent.parent

DEFAULT_CONFIG_FILE = ROOT_DIR / "app_local_config.json"
LOCAL_SNAPSHOT_CACHE_FILE = ROOT_DIR / "snapshot_cache.csv"
VERIFIED_DATA_FALLBACK_FILE = ROOT_DIR / "DATA" / "verified_data.csv"
MANUAL_DEPOSITS_FILE = ROOT_DIR / "manual_deposits_store.json"

NETWORK_TIMEOUT_SECONDS = 20
NETWORK_MAX_RETRIES = 3
NETWORK_RETRY_BACKOFF_SECONDS = 1.3

_DEFAULT_CONFIG = {
    "web_app_url": "",
    "api_token": "",
    "spreadsheet_ref": "",
    "worksheet_name": "תמונת מצב",
    "service_account_file": "service-account.json",
    "language": "עברית",
    "demo_mode": False,
}

_EXPECTED_COLS = [
    "Current_Location",
    "Platform",
    "Type",
    "Ticker",
    "Purchase_Date",
    "Quantity",
    "Origin_Buy_Price",
    "Cost_Origin",
    "Origin_Currency",
    "Commission",
    "Status",
    "Cost_ILS",
    "Current_Value_ILS",
    "Trade_ID",
    "Record_Source",
    "Event_Type",
    "Action",
]

_PRICE_CACHE: Dict[str, Tuple[float, float]] = {}
_PRICE_CACHE_TTL = 300.0

# Canonical closed/sold status labels — must stay identical to
# app.py.CLOSED_STATUS_VALUES and the set in Code.gs.
CLOSED_STATUS_VALUES = frozenset({"סגור", "closed", "close", "sold", "נמכר"})


def _clean_url(value: str) -> str:
    return _clean(value).rstrip("/")


def is_apps_script_web_app_url(url: str) -> bool:
    u = _clean(url).lower()
    return u.startswith("https://script.google.com/macros/s/") and (u.endswith("/exec") or u.endswith("/dev"))


def _extract_sheet_id(sheet_ref: str) -> str:
    ref = _clean(sheet_ref)
    if not ref:
        return ""
    if "docs.google.com/spreadsheets" not in ref:
        return ref
    marker = "/spreadsheets/d/"
    if marker not in ref:
        return ""
    tail = ref.split(marker, 1)[1]
    return tail.split("/", 1)[0].strip()


def _resolve_service_account_path(service_account_file: str) -> Path:
    p = Path(_clean(service_account_file)) if _clean(service_account_file) else Path("service-account.json")
    if not p.is_absolute():
        p = ROOT_DIR / p
    return p


def _can_use_gspread_fallback(spreadsheet_ref: str, service_account_file: str) -> bool:
    if gspread is None:
        return False
    if not _extract_sheet_id(spreadsheet_ref):
        return False
    return _resolve_service_account_path(service_account_file).exists()


def load_google_snapshot_data_via_gspread(spreadsheet_ref: str, worksheet_name: str, service_account_file: str) -> pd.DataFrame:
    if gspread is None:
        raise RuntimeError("gspread is not installed")
    key_path = _resolve_service_account_path(service_account_file)
    if not key_path.exists():
        raise RuntimeError(f"Service account file not found: {key_path}")
    sheet_id = _extract_sheet_id(spreadsheet_ref)
    if not sheet_id:
        raise RuntimeError("Missing Spreadsheet URL/ID for gspread fallback")

    client = gspread.service_account(filename=str(key_path))
    values: list[list[object]] = []
    last_exc: Exception | None = None
    for attempt in range(1, NETWORK_MAX_RETRIES + 1):
        try:
            book = client.open_by_key(sheet_id)
            ws = book.worksheet(_clean(worksheet_name) or "תמונת מצב")
            values = ws.get_all_values()
            break
        except Exception as exc:
            last_exc = exc
            retry = attempt < NETWORK_MAX_RETRIES and _is_transient_network_error(exc)
            if not retry:
                raise
            time.sleep(NETWORK_RETRY_BACKOFF_SECONDS * attempt)
    if last_exc is not None and not values:
        raise last_exc
    if not values:
        return pd.DataFrame(columns=_EXPECTED_COLS)

    headers = [str(h) for h in values[0]]
    rows = [r for r in values[1:] if any(_clean(v) for v in r)]
    return normalize_snapshot_df(pd.DataFrame(rows, columns=headers))


def _clean(value: object) -> str:
    if value is None:
        return ""
    return str(value).replace("\u200e", "").replace("\u200f", "").strip()


def _num(value: object) -> float:
    if value is None:
        return 0.0
    s = _clean(value)
    if not s or s.lower() == "nan":
        return 0.0
    s = s.replace("₪", "").replace("$", "").replace("%", "").replace(",", "")
    if s in {"#VALUE!", "נמכר"}:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def _normalize_currency_code(value: object) -> str:
    # Must match app.py._normalize_currency_code and Code.gs normalizeCurrencyCode_
    # exactly — it feeds the canonical Trade_ID identity tuple.
    raw = _clean(value).upper()
    if raw in {"", "NAN"}:
        return ""
    if raw in {"ILS", "NIS", "₪", "שח", 'ש"ח'}:
        return "ILS"
    if raw in {"USD", "$"}:
        return "USD"
    return raw


def _to_trade_id(row: pd.Series) -> str:
    # CANONICAL 10-field identity — byte-identical to app.py._to_trade_id and
    # Code.gs tradeIdentityRaw_/tradeIdFromRow_. Was a divergent 5-field hash
    # (used Cost_ILS + raw date/qty), which produced different IDs than the
    # other clients and the backend → duplicate rows / broken edit-by-id.
    platform = _clean(row.get("Platform", ""))
    location = _clean(row.get("Current_Location", ""))
    asset_type = _clean(row.get("Type", ""))
    ticker = _clean(row.get("Ticker", "")).upper()
    purchase_raw = row.get("Purchase_Date", "")
    purchase_dt = _parse_dates_flexible(pd.Series([purchase_raw])).iloc[0]
    purchase_date = purchase_dt.strftime("%Y-%m-%d") if pd.notna(purchase_dt) else _clean(purchase_raw)
    qty = _num(row.get("Quantity", 0))
    buy_price = _num(row.get("Origin_Buy_Price", 0))
    cost_origin = _num(row.get("Cost_Origin", 0))
    currency = _normalize_currency_code(row.get("Origin_Currency", ""))
    commission = _num(row.get("Commission", 0))
    raw = (
        f"{platform}|{location}|{asset_type}|{ticker}|{purchase_date}|"
        f"{qty:.12f}|{buy_price:.12f}|{cost_origin:.12f}|{currency}|{commission:.12f}"
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _parse_dates_flexible(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=True)
    missing = parsed.isna()
    if missing.any():
        parsed_alt = pd.to_datetime(series[missing], errors="coerce", dayfirst=False)
        parsed.loc[missing] = parsed_alt
    return parsed


def load_config(path: Optional[Path] = None) -> AppConfig:
    cfg_path = path or DEFAULT_CONFIG_FILE
    if cfg_path.exists():
        raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    else:
        raw = dict(_DEFAULT_CONFIG)
    return AppConfig(
        web_app_url=_clean(raw.get("web_app_url", "")),
        api_token=_clean(raw.get("api_token", "")),
        spreadsheet_ref=_clean(raw.get("spreadsheet_ref", "")),
        worksheet_name=_clean(raw.get("worksheet_name", "תמונת מצב")) or "תמונת מצב",
        service_account_file=_clean(raw.get("service_account_file", "")),
        language=_clean(raw.get("language", "עברית")) or "עברית",
        demo_mode=bool(raw.get("demo_mode", False)),
    )


def _is_timeout_error(exc: Exception) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, urlerror.URLError) and isinstance(getattr(exc, "reason", None), TimeoutError):
        return True
    t = str(exc).lower()
    return "timed out" in t or "timeout" in t


def _is_transient_network_error(exc: Exception) -> bool:
    if _is_timeout_error(exc):
        return True
    if isinstance(exc, urlerror.URLError):
        return True
    t = str(exc).lower()
    return any(x in t for x in ["tempor", "connection reset", "remote end closed", "502", "503", "504"])


def _read_url_with_retries(req: urlrequest.Request) -> str:
    last_exc: Exception | None = None
    for attempt in range(1, NETWORK_MAX_RETRIES + 1):
        try:
            with urlrequest.urlopen(req, timeout=NETWORK_TIMEOUT_SECONDS) as response:
                return response.read().decode("utf-8")
        except Exception as exc:
            last_exc = exc
            retry = attempt < NETWORK_MAX_RETRIES and _is_transient_network_error(exc)
            if not retry:
                raise
            time.sleep(NETWORK_RETRY_BACKOFF_SECONDS * attempt)
    if last_exc:
        raise last_exc
    raise RuntimeError("Network read failed")


def call_apps_script(web_app_url: str, payload: Dict[str, object]) -> Dict[str, object]:
    req = urlrequest.Request(
        web_app_url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    return json.loads(_read_url_with_retries(req))


def normalize_snapshot_df(df: pd.DataFrame) -> pd.DataFrame:
    rename = {
        "מיקום נוכחי": "Current_Location",
        "פלטפורמה": "Platform",
        "סוג נכס": "Type",
        "טיקר": "Ticker",
        "תאריך רכישה": "Purchase_Date",
        "כמות": "Quantity",
        "שער קנייה": "Origin_Buy_Price",
        "עלות כוללת": "Cost_Origin",
        "מטבע": "Origin_Currency",
        "עמלה": "Commission",
        "סטטוס": "Status",
        "עלות ILS": "Cost_ILS",
        "שווי ILS": "Current_Value_ILS",
        "Trade_ID": "Trade_ID",
    }
    out = df.rename(columns=rename).copy()

    for col in ["Platform", "Type", "Ticker", "Status", "Origin_Currency"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].map(_clean)

    for col in ["Quantity", "Origin_Buy_Price", "Cost_Origin", "Commission", "Cost_ILS", "Current_Value_ILS"]:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = out[col].map(_num)

    if "Purchase_Date" not in out.columns:
        out["Purchase_Date"] = pd.NaT
    out["Purchase_Date"] = _parse_dates_flexible(out["Purchase_Date"])

    if "Trade_ID" not in out.columns:
        out["Trade_ID"] = out.apply(_to_trade_id, axis=1)
    else:
        out["Trade_ID"] = out["Trade_ID"].map(_clean)
        missing = out["Trade_ID"] == ""
        if missing.any():
            out.loc[missing, "Trade_ID"] = out.loc[missing].apply(_to_trade_id, axis=1)

    out["Record_Source"] = "STATE_SNAPSHOT"
    out["Event_Type"] = "TRADE"
    # Match app.py: treat the full closed-status set as SELL, not only "סגור".
    # Otherwise a sale labeled נמכר/sold/closed/close becomes a phantom FIFO buy lot.
    out["Action"] = out["Status"].map(lambda x: "SELL" if _clean(x).lower() in CLOSED_STATUS_VALUES else "BUY")

    for col in _EXPECTED_COLS:
        if col not in out.columns:
            out[col] = "" if col in {"Current_Location", "Platform", "Type", "Ticker", "Origin_Currency", "Status", "Trade_ID", "Record_Source", "Event_Type", "Action"} else 0.0
    return out[_EXPECTED_COLS].copy()


def save_snapshot_cache(df: pd.DataFrame) -> None:
    if df.empty:
        return
    out = df.copy()
    if "Purchase_Date" in out.columns:
        out["Purchase_Date"] = pd.to_datetime(out["Purchase_Date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    out.to_csv(LOCAL_SNAPSHOT_CACHE_FILE, index=False, encoding="utf-8-sig")


def load_emergency_snapshot_backup() -> Tuple[pd.DataFrame, str]:
    try:
        if LOCAL_SNAPSHOT_CACHE_FILE.exists():
            cached = pd.read_csv(LOCAL_SNAPSHOT_CACHE_FILE)
            if not cached.empty:
                return normalize_snapshot_df(cached), "local_cache"
    except Exception:
        pass
    try:
        if VERIFIED_DATA_FALLBACK_FILE.exists():
            verified = pd.read_csv(VERIFIED_DATA_FALLBACK_FILE)
            if not verified.empty:
                return normalize_snapshot_df(verified), "verified_fallback"
    except Exception:
        pass
    return pd.DataFrame(columns=_EXPECTED_COLS), "empty"


def load_google_snapshot_data(web_app_url: str, token: str) -> pd.DataFrame:
    parsed = {}
    last_error = ""
    for action_name in ("read_snapshot", "readSnapshot", "read", "snapshot"):
        payload = {"token": token or "", "action": action_name}
        parsed = call_apps_script(web_app_url, payload)
        if bool(parsed.get("ok")):
            break
        err = _clean(parsed.get("error") or "")
        if "unsupported action" in err.lower():
            last_error = err
            continue
        raise RuntimeError(str(parsed.get("error") or parsed))
    if not bool(parsed.get("ok")):
        raise RuntimeError(last_error or str(parsed.get("error") or parsed))

    data = parsed.get("data") or {}
    headers = [str(h) for h in (data.get("headers") or [])]
    rows = data.get("rows") or []
    if not headers:
        return pd.DataFrame(columns=_EXPECTED_COLS)
    return normalize_snapshot_df(pd.DataFrame(rows, columns=headers))


SNAPSHOT_CACHE_FRESH_SECONDS = 6 * 3600  # cache-first window for fast app open


def load_snapshot_data(config: AppConfig, prefer_cache: bool = True) -> Tuple[pd.DataFrame, str]:
    if config.demo_mode:
        return build_demo_snapshot_data(), "demo"

    # FAST OPEN: if the local snapshot cache is fresh, use it instead of a
    # blocking Google round-trip on every launch. A manual refresh (or stale
    # cache) still goes remote, so trades added elsewhere appear within the
    # freshness window at worst.
    if prefer_cache:
        try:
            if LOCAL_SNAPSHOT_CACHE_FILE.exists():
                age = time.time() - LOCAL_SNAPSHOT_CACHE_FILE.stat().st_mtime
                if age < SNAPSHOT_CACHE_FRESH_SECONDS:
                    cached = pd.read_csv(LOCAL_SNAPSHOT_CACHE_FILE)
                    if not cached.empty:
                        return normalize_snapshot_df(cached), "local_cache_fast"
        except Exception:
            pass

    web_url = _clean_url(config.web_app_url)
    if is_apps_script_web_app_url(web_url):
        try:
            remote_df = load_google_snapshot_data(web_url, config.api_token)
            if not remote_df.empty:
                save_snapshot_cache(remote_df)
                return remote_df, "apps_script"
        except Exception:
            pass

    if _can_use_gspread_fallback(config.spreadsheet_ref, config.service_account_file):
        try:
            via_gspread = load_google_snapshot_data_via_gspread(
                config.spreadsheet_ref,
                config.worksheet_name,
                config.service_account_file,
            )
            if not via_gspread.empty:
                save_snapshot_cache(via_gspread)
                return via_gspread, "gspread"
        except Exception:
            pass

    try:
        if web_url and not is_apps_script_web_app_url(web_url):
            remote_df = load_google_snapshot_data(web_url, config.api_token)
            if not remote_df.empty:
                save_snapshot_cache(remote_df)
                return remote_df, "apps_script"
    except Exception:
        pass
    return load_emergency_snapshot_backup()


def prepare_core_views(df: pd.DataFrame) -> Dict[str, object]:
    trades = df[(df["Record_Source"] == "STATE_SNAPSHOT") & (df["Event_Type"] == "TRADE")].copy()
    if "Status" not in trades.columns:
        trades["Status"] = ""
    trades["Status"] = trades["Status"].replace("", "פתוח")
    status_norm = trades["Status"].map(_clean).str.lower()
    closed_mask = status_norm.isin(CLOSED_STATUS_VALUES)
    open_trades = trades[~closed_mask].copy()
    closed_trades = trades[closed_mask].copy()

    total_cost = float(open_trades["Cost_ILS"].sum()) if "Cost_ILS" in open_trades.columns else 0.0
    total_value = float(open_trades["Current_Value_ILS"].sum()) if "Current_Value_ILS" in open_trades.columns else 0.0
    total_profit = total_value - total_cost
    total_return = (total_profit / total_cost) if total_cost else 0.0
    return {
        "trades": trades,
        "open_trades": open_trades,
        "closed_trades": closed_trades,
        "total_cost": total_cost,
        "total_value": total_value,
        "total_profit": total_profit,
        "total_return": total_return,
    }


def _market_symbol(ticker: str) -> str:
    t = _clean(ticker).upper()
    return f"{t}-USD" if t in {"BTC", "ETH", "SOL"} else t


def _download_close_matrix(symbols: Tuple[str, ...], days: int = 365) -> pd.DataFrame:
    clean_symbols = tuple(s for s in symbols if _clean(s))
    if not clean_symbols:
        return pd.DataFrame()
    try:
        raw = yf.download(list(clean_symbols), period=f"{int(days)}d", interval="1d", progress=False, group_by="ticker", auto_adjust=False, threads=True)
    except Exception:
        return pd.DataFrame()
    if raw is None or getattr(raw, "empty", True):
        return pd.DataFrame()

    out: Dict[str, pd.Series] = {}
    if isinstance(raw.columns, pd.MultiIndex):
        level_zero = {str(v) for v in raw.columns.get_level_values(0)}
        if "Close" in level_zero:
            for sym in clean_symbols:
                try:
                    s = pd.to_numeric(raw[("Close", sym)], errors="coerce")
                    if s.notna().any():
                        out[sym] = s.rename(sym)
                except Exception:
                    continue
        else:
            for sym in clean_symbols:
                try:
                    part = raw[sym]
                    if isinstance(part, pd.DataFrame) and "Close" in part.columns:
                        s = pd.to_numeric(part["Close"], errors="coerce")
                        if s.notna().any():
                            out[sym] = s.rename(sym)
                except Exception:
                    continue
    elif isinstance(raw, pd.DataFrame) and "Close" in raw.columns and len(clean_symbols) == 1:
        sym = clean_symbols[0]
        s = pd.to_numeric(raw["Close"], errors="coerce")
        if s.notna().any():
            out[sym] = s.rename(sym)

    if not out:
        return pd.DataFrame()
    return pd.concat(out.values(), axis=1)


def safe_quote(symbol: str) -> float:
    sym = _market_symbol(symbol)
    now = time.time()
    hit = _PRICE_CACHE.get(sym)
    if hit and (now - hit[1]) <= _PRICE_CACHE_TTL:
        return float(hit[0])

    val = 0.0
    try:
        t = yf.Ticker(sym)
        fast_info = getattr(t, "fast_info", None)
        if fast_info:
            for field in ["lastPrice", "regularMarketPrice", "previousClose"]:
                try:
                    maybe = float(fast_info.get(field, 0) or 0)
                    if maybe > 0:
                        val = maybe
                        break
                except Exception:
                    pass
        if val <= 0:
            hist = t.history(period="5d", interval="1d", auto_adjust=False)
            if not hist.empty and "Close" in hist.columns:
                s = pd.to_numeric(hist["Close"], errors="coerce").dropna()
                if not s.empty:
                    val = float(s.iloc[-1])
    except Exception:
        val = 0.0
    _PRICE_CACHE[sym] = (val, now)
    return val


def fetch_prices(tickers: Tuple[str, ...]) -> Dict[str, float]:
    clean = tuple(dict.fromkeys([_clean(t).upper() for t in tickers if _clean(t)]))
    if not clean:
        return {}
    symbol_map = {t: _market_symbol(t) for t in clean}
    close_df = _download_close_matrix(tuple(dict.fromkeys(symbol_map.values())), days=7)
    out: Dict[str, float] = {}
    for t in clean:
        sym = symbol_map[t]
        val = 0.0
        if not close_df.empty and sym in close_df.columns:
            s = pd.to_numeric(close_df[sym], errors="coerce").dropna()
            if not s.empty:
                val = float(s.iloc[-1])
        if val <= 0:
            val = safe_quote(sym)
        out[t] = val if val > 0 else 0.0
    return out


def enrich_open_trades_with_prices(open_trades: pd.DataFrame) -> pd.DataFrame:
    out = open_trades.copy()
    tickers = tuple(sorted(t for t in out["Ticker"].dropna().unique() if _clean(t))) if "Ticker" in out.columns else tuple()
    out["Current_Price"] = out["Ticker"].map(fetch_prices(tickers)).fillna(0.0) if "Ticker" in out.columns else 0.0
    return out


def build_exposure_summary(open_trades: pd.DataFrame) -> pd.DataFrame:
    if open_trades.empty:
        return pd.DataFrame(columns=["Ticker", "Current_Price", "Open_Qty", "Cost_ILS", "Value_ILS", "Net_PnL_ILS", "Yield_ILS"])
    work = enrich_open_trades_with_prices(open_trades)
    out = work.groupby("Ticker", as_index=False).agg(
        Current_Price=("Current_Price", "max"),
        Open_Qty=("Quantity", "sum"),
        Cost_ILS=("Cost_ILS", "sum"),
        Value_ILS=("Current_Value_ILS", "sum"),
    )
    out["Net_PnL_ILS"] = out["Value_ILS"] - out["Cost_ILS"]
    out["Yield_ILS"] = np.where(out["Cost_ILS"] > 0, out["Net_PnL_ILS"] / out["Cost_ILS"], 0.0)
    return out.sort_values("Value_ILS", ascending=False).reset_index(drop=True)


def portfolio_price_history(tickers: Tuple[str, ...], quantities: Tuple[float, ...], days: int = 365) -> pd.Series:
    if not tickers or not quantities:
        return pd.Series(dtype=float)
    qty_by_ticker: Dict[str, float] = {}
    for ticker, qty in zip(tickers, quantities):
        t = _clean(ticker).upper()
        q = float(_num(qty))
        if not t or abs(q) <= 1e-12:
            continue
        qty_by_ticker[t] = qty_by_ticker.get(t, 0.0) + q
    if not qty_by_ticker:
        return pd.Series(dtype=float)
    symbol_by_ticker = {t: _market_symbol(t) for t in qty_by_ticker}
    close_df = _download_close_matrix(tuple(dict.fromkeys(symbol_by_ticker.values())), days=max(int(days), 30))
    if close_df.empty:
        return pd.Series(dtype=float)

    # Convert USD-quoted tickers (anything not a .TA / TASE symbol) to ILS before
    # summing, so the combined series isn't weighted by native-currency notional.
    def _is_usd(ticker: str) -> bool:
        return not symbol_by_ticker[ticker].upper().endswith(".TA")
    needs_fx = any(_is_usd(t) for t in qty_by_ticker)
    fx_series = None
    fx_spot = 0.0
    if needs_fx:
        fx_df = _download_close_matrix(("USDILS=X",), days=max(int(days), 30))
        if not fx_df.empty and "USDILS=X" in fx_df.columns:
            fx_series = pd.to_numeric(fx_df["USDILS=X"], errors="coerce")
        fx_spot = safe_quote("USDILS=X")
        if fx_spot <= 0:
            fx_spot = 3.6

    frames = []
    for ticker, qty in qty_by_ticker.items():
        sym = symbol_by_ticker[ticker]
        if sym not in close_df.columns:
            continue
        s = pd.to_numeric(close_df[sym], errors="coerce").rename(ticker)
        if not s.notna().any():
            continue
        if _is_usd(ticker):
            if fx_series is not None:
                s = s * fx_series.reindex(s.index).ffill().bfill()
            else:
                s = s * fx_spot
        frames.append(s * qty)
    if not frames:
        return pd.Series(dtype=float)
    return pd.concat(frames, axis=1).ffill().fillna(0).sum(axis=1)


def fifo_metrics(trades: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for ticker, tdf in trades.sort_values("Purchase_Date").groupby("Ticker"):
        lots: List[Tuple[float, float]] = []
        realized = 0.0
        for _, row in tdf.iterrows():
            qty = float(_num(row.get("Quantity", 0.0)))
            cost_ils = float(_num(row.get("Cost_ILS", 0.0)))
            unit_cost = abs(cost_ils / qty) if qty else 0.0
            action = _clean(row.get("Action", "BUY")).upper() or "BUY"
            if action == "BUY" and qty > 0:
                lots.append((qty, unit_cost))
            elif action == "SELL" and qty != 0:
                sell_qty = abs(qty)
                sell_price = abs(float(_num(row.get("Current_Value_ILS", 0.0)))) / sell_qty if sell_qty and _num(row.get("Current_Value_ILS", 0.0)) != 0 else unit_cost
                i = 0
                while sell_qty > 1e-9 and i < len(lots):
                    lot_qty, lot_cost = lots[i]
                    used = min(lot_qty, sell_qty)
                    realized += used * (sell_price - lot_cost)
                    lot_qty -= used
                    sell_qty -= used
                    lots[i] = (lot_qty, lot_cost)
                    if lot_qty <= 1e-9:
                        i += 1
                lots = [lot for lot in lots[i:] if lot[0] > 1e-9]
                # Single-row closed trades carry both buy cost and proceeds with
                # no separate BUY lot; realize the unmatched qty against this
                # row's own cost basis (else realized P/L is always 0 when closed).
                if sell_qty > 1e-9:
                    realized += sell_qty * (sell_price - unit_cost)
        open_qty = sum(q for q, _ in lots)
        # Emit when there's an open position OR realized P/L (closed-only tickers).
        if open_qty <= 1e-9 and abs(realized) <= 1e-9:
            continue
        open_cost = sum(q * c for q, c in lots)
        rows.append({
            "Ticker": ticker,
            "Open_Qty_FIFO": open_qty,
            "Open_Cost_ILS": open_cost,
            "Realized_PnL_ILS": realized,
            "Average_Buy_Price": open_cost / open_qty if open_qty else 0.0,
        })
    return pd.DataFrame(rows)


def build_reports(open_trades: pd.DataFrame) -> Dict[str, object]:
    summary = build_exposure_summary(open_trades)
    total_value = float(summary["Value_ILS"].sum()) if not summary.empty else 0.0
    if summary.empty:
        winner_loser = pd.DataFrame(columns=["Category", "Ticker", "Yield_ILS"])
    else:
        winner = summary.loc[summary["Yield_ILS"].idxmax()]
        loser = summary.loc[summary["Yield_ILS"].idxmin()]
        winner_loser = pd.DataFrame([
            {"Category": "Winner", "Ticker": winner["Ticker"], "Yield_ILS": float(winner["Yield_ILS"])},
            {"Category": "Loser", "Ticker": loser["Ticker"], "Yield_ILS": float(loser["Yield_ILS"])},
        ])
    platform_table = open_trades.groupby("Platform", as_index=False).agg(
        Net_Investment_ILS=("Cost_ILS", "sum"),
        Current_Value_ILS=("Current_Value_ILS", "sum"),
    ) if not open_trades.empty else pd.DataFrame(columns=["Platform", "Net_Investment_ILS", "Current_Value_ILS"])
    if not platform_table.empty:
        platform_table["PnL_ILS"] = platform_table["Current_Value_ILS"] - platform_table["Net_Investment_ILS"]
    crypto_mask = open_trades.get("Type", pd.Series(dtype=str)).map(_clean) == "קריפטו"
    crypto_value = float(open_trades.loc[crypto_mask, "Current_Value_ILS"].sum()) if not open_trades.empty else 0.0
    return {
        "crypto_share": (crypto_value / total_value) if total_value else 0.0,
        "winner_loser_table": winner_loser,
        "net_investment_table": platform_table,
        "live_rates": {
            "USD/ILS": safe_quote("USDILS=X"),
            "BTC/USD": safe_quote("BTC-USD"),
            "ETH/USD": safe_quote("ETH-USD"),
            "SOL/USD": safe_quote("SOL-USD"),
        },
    }


def _normalize_manual_rows(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    out = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        platform = _clean(row.get("Platform", ""))
        if not platform or platform in seen:
            continue
        seen.add(platform)
        out.append({"Platform": platform, "Manual_Deposit_ILS": _num(row.get("Manual_Deposit_ILS", 0.0))})
    out.sort(key=lambda r: r["Platform"].lower())
    return out


def load_manual_deposits_store() -> Dict[str, List[Dict[str, object]]]:
    if not MANUAL_DEPOSITS_FILE.exists():
        return {"live": [], "demo": []}
    try:
        raw = json.loads(MANUAL_DEPOSITS_FILE.read_text(encoding="utf-8"))
        return {
            "live": _normalize_manual_rows(raw.get("live", []) if isinstance(raw, dict) else []),
            "demo": _normalize_manual_rows(raw.get("demo", []) if isinstance(raw, dict) else []),
        }
    except Exception:
        return {"live": [], "demo": []}


def save_manual_deposits_store(store: Dict[str, List[Dict[str, object]]]) -> bool:
    try:
        payload = {"live": _normalize_manual_rows(store.get("live", [])), "demo": _normalize_manual_rows(store.get("demo", []))}
        MANUAL_DEPOSITS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False


def load_manual_deposits_remote(web_app_url: str, token: str, mode: str) -> Tuple[bool, List[Dict[str, object]], str]:
    try:
        payload = {"token": token or "", "action": "read_manual_deposits", "mode": "demo" if _clean(mode).lower() == "demo" else "live"}
        parsed = call_apps_script(web_app_url, payload)
        if not bool(parsed.get("ok")):
            return False, [], str(parsed.get("error") or parsed)
        rows = _normalize_manual_rows((parsed.get("data") or {}).get("rows") or [])
        return True, rows, ""
    except Exception as exc:
        return False, [], str(exc)


def save_manual_deposits_remote(web_app_url: str, token: str, mode: str, rows: List[Dict[str, object]]) -> Tuple[bool, str]:
    try:
        payload = {
            "token": token or "",
            "action": "save_manual_deposits",
            "mode": "demo" if _clean(mode).lower() == "demo" else "live",
            "rows": _normalize_manual_rows(rows),
        }
        parsed = call_apps_script(web_app_url, payload)
        if bool(parsed.get("ok")):
            return True, ""
        return False, str(parsed.get("error") or parsed)
    except Exception as exc:
        return False, str(exc)


# ════════════════════════════════════════════════════════════════════════
# SIMULATOR — long-term projection + Safe Withdrawal (pension) model.
# Pure ports of the Streamlit functions in app.py (lines 5801-5906) so that
# the desktop client stays in mathematical lock-step with the web client.
# ════════════════════════════════════════════════════════════════════════
SIM_PREFS_FILE = ROOT_DIR / "sim_user_prefs.json"
SIM_PERSISTED_KEYS = (
    "sim_age_now", "sim_age_target",
    "sim_initial_clean", "sim_annual_return",
    "sim_monthly_contrib", "sim_lump_sum", "sim_lump_month",
    "sim_swr", "sim_annual_inflation", "sim_mode_choice",
)


def load_sim_prefs() -> Dict[str, object]:
    """Load previously saved simulator inputs from disk (per-user persistence)."""
    if not SIM_PREFS_FILE.exists():
        return {}
    try:
        raw = json.loads(SIM_PREFS_FILE.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def save_sim_prefs(values: Dict[str, object]) -> bool:
    """Persist the simulator input snapshot. Silent on failure so a broken disk
    never crashes the UI."""
    try:
        safe = {k: v for k, v in values.items() if k in SIM_PERSISTED_KEYS}
        SIM_PREFS_FILE.write_text(
            json.dumps(safe, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return True
    except Exception:
        return False


def sim_project_portfolio(
    initial_capital: float,
    monthly_contribution: float,
    annual_return_pct: float,
    years: float,
    lump_sum: float = 0.0,
    lump_sum_month: int = 0,
) -> pd.DataFrame:
    """Project portfolio growth with monthly compounding and optional lump-sum.

    Returns a DataFrame with columns:
      month, year, contributions_cum, balance_no_lump, balance_with_lump
    """
    try:
        initial_capital = float(initial_capital or 0.0)
        monthly_contribution = float(monthly_contribution or 0.0)
        annual_return_pct = float(annual_return_pct or 0.0)
        years = float(years or 0.0)
        lump_sum = float(lump_sum or 0.0)
        lump_sum_month = int(lump_sum_month or 0)
    except (TypeError, ValueError):
        initial_capital = monthly_contribution = annual_return_pct = 0.0
        years = lump_sum = 0.0
        lump_sum_month = 0

    n = max(int(round(years * 12)), 0)
    if n == 0:
        return pd.DataFrame(
            [{"month": 0, "year": 0.0, "contributions_cum": initial_capital,
              "balance_no_lump": initial_capital,
              "balance_with_lump": initial_capital + (lump_sum if lump_sum_month == 0 else 0.0)}]
        )

    r_annual = annual_return_pct / 100.0
    if r_annual <= -1.0:
        r_m = -0.999999
    else:
        r_m = (1.0 + r_annual) ** (1.0 / 12.0) - 1.0

    k = max(0, min(lump_sum_month, n))

    balances_nl = np.zeros(n + 1, dtype=float)
    balances_wl = np.zeros(n + 1, dtype=float)
    contribs = np.zeros(n + 1, dtype=float)

    balances_nl[0] = initial_capital
    balances_wl[0] = initial_capital + (lump_sum if k == 0 else 0.0)
    contribs[0] = initial_capital

    for m in range(1, n + 1):
        balances_nl[m] = balances_nl[m - 1] * (1.0 + r_m) + monthly_contribution
        inj = lump_sum if m == k else 0.0
        balances_wl[m] = balances_wl[m - 1] * (1.0 + r_m) + monthly_contribution + inj
        contribs[m] = contribs[m - 1] + monthly_contribution

    months = np.arange(n + 1, dtype=int)
    return pd.DataFrame({
        "month": months,
        "year": months / 12.0,
        "contributions_cum": contribs,
        "balance_no_lump": balances_nl,
        "balance_with_lump": balances_wl,
    })


def sim_safe_withdrawal_monthly(final_balance: float, swr_pct: float) -> float:
    """Trinity-style: monthly pension = final_balance * (swr/100) / 12."""
    try:
        fb = float(final_balance or 0.0)
        sw = float(swr_pct or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, fb * (sw / 100.0) / 12.0)


def sim_years_to_target(
    initial_capital: float,
    monthly_contribution: float,
    annual_return_pct: float,
    target_balance: float,
    lump_sum: float = 0.0,
    max_years: int = 80,
) -> float:
    """Find the first fractional-year point where the projection hits target.

    Returns float('inf') if the target is never reached within max_years.
    """
    try:
        target_balance = float(target_balance or 0.0)
    except (TypeError, ValueError):
        return float("inf")
    if target_balance <= 0:
        return 0.0
    df = sim_project_portfolio(
        initial_capital=initial_capital,
        monthly_contribution=monthly_contribution,
        annual_return_pct=annual_return_pct,
        years=float(max_years),
        lump_sum=lump_sum,
        lump_sum_month=0,
    )
    hits = df[df["balance_with_lump"] >= target_balance]
    if hits.empty:
        return float("inf")
    return float(hits.iloc[0]["year"])


# ════════════════════════════════════════════════════════════════════════
# RISK METRICS — Sharpe / Vol / MDD / CAGR (port of app.py:3065)
# ════════════════════════════════════════════════════════════════════════
RISK_FREE_ANNUAL = 0.04


def risk_metrics(value_series: pd.Series) -> Dict[str, float]:
    if value_series is None or value_series.empty or len(value_series) < 3:
        return {"vol": 0.0, "sharpe": 0.0, "mdd": 0.0, "cagr": 0.0}

    daily = value_series.pct_change().dropna()
    vol = float(daily.std() * np.sqrt(252)) if not daily.empty else 0.0
    rf_daily = (1 + RISK_FREE_ANNUAL) ** (1 / 252) - 1
    sharpe = float(((daily.mean() - rf_daily) / daily.std()) * np.sqrt(252)) if daily.std() > 0 else 0.0

    running_max = value_series.cummax()
    drawdowns = (value_series / running_max) - 1
    mdd = float(drawdowns.min()) if not drawdowns.empty else 0.0

    years = max(len(value_series) / 252.0, 1 / 252.0)
    cagr = float((value_series.iloc[-1] / value_series.iloc[0]) ** (1 / years) - 1) if value_series.iloc[0] > 0 else 0.0

    return {"vol": vol, "sharpe": sharpe, "mdd": mdd, "cagr": cagr}


def build_demo_snapshot_data() -> pd.DataFrame:
    rows = [
        {
            "Current_Location": "",
            "Platform": "Global Prime",
            "Type": "ETF",
            "Ticker": "SPY",
            "Purchase_Date": "2025-02-04",
            "Quantity": 18,
            "Origin_Buy_Price": 492,
            "Cost_Origin": 8856,
            "Origin_Currency": "USD",
            "Commission": 6,
            "Status": "פתוח",
            "Cost_ILS": 32720,
            "Current_Value_ILS": 36940,
        },
        {
            "Current_Location": "",
            "Platform": "Bit2C",
            "Type": "קריפטו",
            "Ticker": "BTC",
            "Purchase_Date": "2025-05-01",
            "Quantity": 0.42,
            "Origin_Buy_Price": 62500,
            "Cost_Origin": 26250,
            "Origin_Currency": "USD",
            "Commission": 9,
            "Status": "פתוח",
            "Cost_ILS": 98200,
            "Current_Value_ILS": 116500,
        },
        {
            "Current_Location": "",
            "Platform": "Interactive",
            "Type": "שוק ההון",
            "Ticker": "NVDA",
            "Purchase_Date": "2025-01-10",
            "Quantity": 32,
            "Origin_Buy_Price": 515,
            "Cost_Origin": 16480,
            "Origin_Currency": "USD",
            "Commission": 4,
            "Status": "סגור",
            "Cost_ILS": 61400,
            "Current_Value_ILS": 70200,
        },
    ]
    return normalize_snapshot_df(pd.DataFrame(rows))
