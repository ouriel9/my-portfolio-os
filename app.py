import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Tuple
from urllib import error as urlerror
from urllib import request as urlrequest

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf

try:
    import gspread
except Exception:
    gspread = None

RISK_FREE_ANNUAL = 0.02
CRYPTO_ETFS = {"IBIT", "ETHA", "BSOL", "MSTR"}
LOCAL_SETTINGS_FILE = Path(__file__).resolve().parent / "app_local_config.json"
DEFAULT_SERVICE_ACCOUNT_FILE = Path(__file__).resolve().parent / "clean-linker-492313-s3-770814e64205.json"
DEFAULT_WORKSHEET_NAME = "תמונת מצב"
DEFAULT_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbyDKgJszq8NWNgG7OQVPLflfN2rufBhAT5-fzmjy8iEVFMmNLZlK_CeI4MFvx1dijZF/exec"


@dataclass
class FifoLot:
    qty: float
    cost_per_unit: float


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
    except ValueError:
        return 0.0


def _to_trade_id(row: pd.Series) -> str:
    raw = f"{row.get('Platform','')}|{row.get('Ticker','')}|{row.get('Purchase_Date','')}|{row.get('Quantity',0)}|{row.get('Cost_ILS',0)}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


@st.cache_data(ttl=300)
def load_verified_data(path_str: str) -> pd.DataFrame:
    path = Path(path_str)
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path)
    df.columns = [_clean(c) for c in df.columns]

    rename = {
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
        "Event_Type": "Event_Type",
        "Action": "Action",
    }
    df = df.rename(columns=rename)

    for col in ["Platform", "Type", "Ticker", "Status", "Origin_Currency", "Action", "Event_Type"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].map(_clean)

    for col in [
        "Quantity",
        "Origin_Buy_Price",
        "Cost_Origin",
        "Cost_ILS",
        "Current_Value_ILS",
        "Commission",
        "raw_match_count",
    ]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = df[col].map(_num)

    for col, default in {
        "Record_Source": "",
        "validation_status": "",
        "source_type": "",
        "source_file": "",
        "raw_sources": "",
        "platform_norm": "",
    }.items():
        if col not in df.columns:
            df[col] = default
        df[col] = df[col].map(_clean)

    if "Purchase_Date" not in df.columns:
        df["Purchase_Date"] = pd.NaT
    df["Purchase_Date"] = pd.to_datetime(df["Purchase_Date"], errors="coerce", dayfirst=True)

    if "Trade_ID" not in df.columns:
        df["Trade_ID"] = df.apply(_to_trade_id, axis=1)

    df["Event_Type"] = df["Event_Type"].replace("", "TRADE")
    df["Action"] = df.apply(
        lambda r: "SELL"
        if _clean(r.get("Action")) == "SELL" or _clean(r.get("Status")) == "סגור"
        else ("BUY" if _clean(r.get("Action")) in {"", "BUY"} else _clean(r.get("Action"))),
        axis=1,
    )
    return df


@st.cache_data(ttl=600)
def fetch_prices(tickers: Tuple[str, ...]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for t in tickers:
        y_ticker = f"{t}-USD" if t in {"BTC", "ETH", "SOL"} else t
        try:
            out[t] = float(yf.Ticker(y_ticker).info.get("regularMarketPrice", 0) or 0)
        except Exception:
            out[t] = 0.0
    return out


@st.cache_data(ttl=900)
def portfolio_price_history(tickers: Tuple[str, ...], quantities: Tuple[float, ...], days: int = 365) -> pd.Series:
    if not tickers:
        return pd.Series(dtype=float)

    frames = []
    for ticker, qty in zip(tickers, quantities):
        symbol = f"{ticker}-USD" if ticker in {"BTC", "ETH", "SOL"} else ticker
        try:
            hist = yf.download(symbol, period=f"{days}d", interval="1d", progress=False)["Close"]
            if isinstance(hist, pd.DataFrame):
                hist = hist.iloc[:, 0]
            hist = pd.Series(hist).rename(ticker) * float(qty)
            frames.append(hist)
        except Exception:
            continue

    if not frames:
        return pd.Series(dtype=float)

    combined = pd.concat(frames, axis=1).ffill().fillna(0)
    return combined.sum(axis=1)


def fifo_metrics(trades: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, float]] = []

    for ticker, tdf in trades.sort_values("Purchase_Date").groupby("Ticker"):
        lots: List[FifoLot] = []
        realized = 0.0
        for _, row in tdf.iterrows():
            qty = float(row["Quantity"])
            cost_ils = float(row["Cost_ILS"] if row["Cost_ILS"] else row["Cost_Origin"])
            unit_cost = abs(cost_ils / qty) if qty else 0.0
            if row["Action"] == "BUY" and qty > 0:
                lots.append(FifoLot(qty=qty, cost_per_unit=unit_cost))
            elif row["Action"] == "SELL" and qty != 0:
                sell_qty = abs(qty)
                sell_price = abs(row["Current_Value_ILS"]) / sell_qty if sell_qty and row["Current_Value_ILS"] else unit_cost
                while sell_qty > 1e-9 and lots:
                    lot = lots[0]
                    used = min(lot.qty, sell_qty)
                    realized += used * (sell_price - lot.cost_per_unit)
                    lot.qty -= used
                    sell_qty -= used
                    if lot.qty <= 1e-9:
                        lots.pop(0)

        open_qty = sum(lot.qty for lot in lots)
        open_cost = sum(lot.qty * lot.cost_per_unit for lot in lots)
        rows.append(
            {
                "Ticker": ticker,
                "כמות פתוחה (FIFO)": open_qty,
                "עלות פתוחה (₪)": open_cost,
                "רווח ממומש (₪)": realized,
                "מחיר ממוצע פתוח (₪)": open_cost / open_qty if open_qty else 0.0,
            }
        )
    return pd.DataFrame(rows)


def risk_metrics(value_series: pd.Series) -> Dict[str, float]:
    if value_series.empty or len(value_series) < 3:
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


def _safe_quote(symbol: str) -> float:
    try:
        return float(yf.Ticker(symbol).info.get("regularMarketPrice", 0) or 0)
    except Exception:
        return 0.0


def _select_or_type(label: str, options: List[str], default: str = "", key_prefix: str = "") -> str:
    cleaned = sorted({_clean(v) for v in options if _clean(v)})
    mode = st.radio(
        f"{label} - אופן הזנה",
        ["בחירה מרשימה", "הקלדה ידנית"],
        horizontal=True,
        key=f"{key_prefix}_{label}_mode",
    )
    if mode == "בחירה מרשימה":
        selectable = cleaned if cleaned else ([default] if _clean(default) else [])
        if not selectable:
            return st.text_input(label, value=default, key=f"{key_prefix}_{label}_fallback")
        index = selectable.index(default) if default in selectable else 0
        return st.selectbox(label, selectable, index=index, key=f"{key_prefix}_{label}_pick")
    return st.text_input(label, value=default, key=f"{key_prefix}_{label}_type")


def build_home_inspired_reports(open_trades: pd.DataFrame) -> Dict[str, object]:
    work = open_trades.copy()
    for col in ["Type", "Ticker", "Platform"]:
        if col not in work.columns:
            work[col] = ""
        work[col] = work[col].map(_clean)

    for col in ["Quantity", "Cost_ILS", "Current_Value_ILS", "Cost_Origin", "Commission"]:
        if col not in work.columns:
            work[col] = 0.0
        work[col] = work[col].map(_num)

    total_value = float(work["Current_Value_ILS"].sum())
    total_cost = float(work["Cost_ILS"].sum())

    crypto_mask = (work["Type"] == "קריפטו") | (work["Ticker"].isin(CRYPTO_ETFS))
    crypto_value = float(work.loc[crypto_mask, "Current_Value_ILS"].sum())
    btc_value = float(work.loc[work["Ticker"].isin(["BTC", "IBIT"]), "Current_Value_ILS"].sum())

    summary = work.groupby("Ticker", as_index=False).agg(
        עלות_שקלית=("Cost_ILS", "sum"),
        שווי_שקלי=("Current_Value_ILS", "sum"),
        עלות_מקור=("Cost_Origin", "sum"),
    )
    if summary.empty:
        winner_loser = pd.DataFrame(columns=["סוג", "טיקר", "תשואה"])
    else:
        summary["תשואה"] = np.where(summary["עלות_שקלית"] > 0, (summary["שווי_שקלי"] - summary["עלות_שקלית"]) / summary["עלות_שקלית"], 0.0)
        winner = summary.loc[summary["תשואה"].idxmax()]
        loser = summary.loc[summary["תשואה"].idxmin()]
        winner_loser = pd.DataFrame(
            [
                {"סוג": "המנצח", "טיקר": winner["Ticker"], "תשואה": float(winner["תשואה"])},
                {"סוג": "המפסיד", "טיקר": loser["Ticker"], "תשואה": float(loser["תשואה"])},
            ]
        )

    platform_summary = work.groupby("Platform", as_index=False).agg(
        עלות_שקלית=("Cost_ILS", "sum"),
        שווי_שקלי=("Current_Value_ILS", "sum"),
    )
    if not platform_summary.empty:
        platform_summary["רווח_הפסד"] = platform_summary["שווי_שקלי"] - platform_summary["עלות_שקלית"]

    asset_map = {"BTC": "IBIT", "ETH": "ETHA", "SOL": "BSOL"}
    concentration_rows: List[Dict[str, float]] = []
    for asset, etf in asset_map.items():
        direct = work[(work["Ticker"] == asset) & (work["Type"] == "קריפטו")]
        etf_df = work[work["Ticker"] == etf]
        direct_qty = float(direct["Quantity"].sum())
        direct_val = float(direct["Current_Value_ILS"].sum())
        etf_qty = float(etf_df["Quantity"].sum())
        etf_val = float(etf_df["Current_Value_ILS"].sum())
        concentration_rows.append(
            {
                "נכס": asset,
                "כמות ישירה": direct_qty,
                "שווי ישיר (₪)": direct_val,
                "כמות בקרן": etf_qty,
                "שווי בקרן (₪)": etf_val,
                "חשיפה כוללת (₪)": direct_val + etf_val,
            }
        )

    live_rates = {
        "USD/ILS": _safe_quote("USDILS=X"),
        "BTC/USD": _safe_quote("BTC-USD"),
        "ETH/USD": _safe_quote("ETH-USD"),
        "SOL/USD": _safe_quote("SOL-USD"),
    }

    return {
        "אחוז_קריפטו": (crypto_value / total_value) if total_value else 0.0,
        "אחוז_ביטקוין_מהתיק": (btc_value / total_value) if total_value else 0.0,
        "אחוז_ביטקוין_מהקריפטו": (btc_value / crypto_value) if crypto_value else 0.0,
        "טבלת_ריכוז": pd.DataFrame(concentration_rows),
        "טבלת_מנצח_מפסיד": winner_loser,
        "טבלת_הפקדות": platform_summary,
        "שערים_חיים": live_rates,
    }


def call_apps_script_(web_app_url: str, payload: Dict[str, object]) -> Dict[str, object]:
    req = urlrequest.Request(
        web_app_url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urlrequest.urlopen(req, timeout=25) as response:
        body = response.read().decode("utf-8")
    return json.loads(body)


def load_local_settings() -> Dict[str, str]:
    if not LOCAL_SETTINGS_FILE.exists():
        return {}
    try:
        raw = json.loads(LOCAL_SETTINGS_FILE.read_text(encoding="utf-8"))
        return {
            "web_app_url": _clean(raw.get("web_app_url", "")),
            "api_token": _clean(raw.get("api_token", "")),
            "spreadsheet_ref": _clean(raw.get("spreadsheet_ref", "")),
            "worksheet_name": _clean(raw.get("worksheet_name", DEFAULT_WORKSHEET_NAME)) or DEFAULT_WORKSHEET_NAME,
            "service_account_file": _clean(raw.get("service_account_file", str(DEFAULT_SERVICE_ACCOUNT_FILE))),
            "language": _clean(raw.get("language", "English")) or "English",
            "demo_mode": str(raw.get("demo_mode", "false")).lower() == "true",
        }
    except Exception:
        return {}


def save_local_settings(
    web_app_url: str,
    api_token: str,
    spreadsheet_ref: str,
    worksheet_name: str,
    service_account_file: str,
    language: str,
    demo_mode: bool,
) -> bool:
    try:
        payload = {
            "web_app_url": _clean(web_app_url),
            "api_token": _clean(api_token),
            "spreadsheet_ref": _clean(spreadsheet_ref),
            "worksheet_name": _clean(worksheet_name) or DEFAULT_WORKSHEET_NAME,
            "service_account_file": _clean(service_account_file),
            "language": _clean(language) or "English",
            "demo_mode": bool(demo_mode),
        }
        LOCAL_SETTINGS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False


def is_apps_script_web_app_url(url: str) -> bool:
    cleaned = _clean(url).lower()
    return cleaned.startswith("https://script.google.com/macros/s/") and (cleaned.endswith("/exec") or cleaned.endswith("/dev"))


def is_google_sheet_url(url: str) -> bool:
    cleaned = _clean(url).lower()
    return "docs.google.com/spreadsheets" in cleaned


def _extract_sheet_id(sheet_ref: str) -> str:
    ref = _clean(sheet_ref)
    if not ref:
        return ""
    if "docs.google.com/spreadsheets" not in ref:
        return ref
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", ref)
    return match.group(1) if match else ""


def _looks_like_private_key_blob(value: str) -> bool:
    v = _clean(value)
    if not v:
        return False
    # Catch accidental paste of service-account private key into API token field.
    if "BEGIN PRIVATE KEY" in v or "-----BEGIN" in v:
        return True
    return len(v) > 300 and v.startswith("MII")


def _service_account_from_runtime_secrets() -> Optional[Dict[str, object]]:
    # Streamlit Cloud usually injects credentials via st.secrets, not local files.
    required_keys = {
        "type",
        "project_id",
        "private_key_id",
        "private_key",
        "client_email",
        "client_id",
        "auth_uri",
        "token_uri",
        "auth_provider_x509_cert_url",
        "client_x509_cert_url",
    }

    def _to_plain_dict(obj: object) -> Optional[Dict[str, object]]:
        if obj is None:
            return None
        if hasattr(obj, "to_dict"):
            try:
                return dict(obj.to_dict())
            except Exception:
                return None
        if isinstance(obj, Mapping):
            return dict(obj)
        if isinstance(obj, dict):
            return dict(obj)
        if isinstance(obj, str):
            try:
                parsed = json.loads(obj)
                return dict(parsed) if isinstance(parsed, dict) else None
            except Exception:
                return None
        return None

    def _normalize(creds: Optional[Dict[str, object]]) -> Optional[Dict[str, object]]:
        if not creds:
            return None
        out = {str(k): v for k, v in creds.items()}
        if not required_keys.issubset(set(out.keys())):
            return None
        # Ensure real newlines in private key for google-auth.
        out["private_key"] = str(out.get("private_key", "")).replace("\\n", "\n")
        return out

    try:
        # Preferred format in Streamlit Cloud: [gcp_service_account] section.
        if "gcp_service_account" in st.secrets:
            parsed = _normalize(_to_plain_dict(st.secrets["gcp_service_account"]))
            if parsed:
                return parsed

        # Also support flat secrets format (top-level keys).
        flat = _to_plain_dict(st.secrets)
        parsed_flat = _normalize(flat)
        if parsed_flat:
            return parsed_flat
    except Exception:
        return None
    return None


def _service_account_from_env() -> Optional[Dict[str, object]]:
    for var_name in ["GCP_SERVICE_ACCOUNT_JSON", "GOOGLE_SERVICE_ACCOUNT_JSON", "SERVICE_ACCOUNT_JSON"]:
        raw = os.getenv(var_name, "")
        if not _clean(raw):
            continue
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            continue
    return None


def _build_gspread_client(service_account_file: str):
    if gspread is None:
        raise RuntimeError("הספריה gspread לא מותקנת בסביבה")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    env_creds = _service_account_from_env()
    if env_creds:
        return gspread.service_account_from_dict(env_creds, scopes=scopes)

    secrets_creds = _service_account_from_runtime_secrets()
    if secrets_creds:
        return gspread.service_account_from_dict(secrets_creds, scopes=scopes)

    key_path = Path(_clean(service_account_file)) if _clean(service_account_file) else DEFAULT_SERVICE_ACCOUNT_FILE
    if not key_path.is_absolute():
        key_path = Path(__file__).resolve().parent / key_path
    if not key_path.exists():
        raise RuntimeError(
            f"קובץ Service Account לא נמצא: {key_path}. "
            "בפריסה (למשל Streamlit Cloud) הגדר credentials תחת st.secrets['gcp_service_account'] "
            "או ENV בשם GCP_SERVICE_ACCOUNT_JSON."
        )

    return gspread.service_account(filename=str(key_path), scopes=scopes)


@st.cache_data(ttl=20)
def load_google_snapshot_data_via_gspread(spreadsheet_ref: str, worksheet_name: str, service_account_file: str) -> pd.DataFrame:
    client = _build_gspread_client(service_account_file)
    sheet_id = _extract_sheet_id(spreadsheet_ref)
    if not sheet_id:
        raise RuntimeError("חסר Spreadsheet ID/URL עבור חיבור gspread")

    book = client.open_by_key(sheet_id)
    ws = book.worksheet(_clean(worksheet_name) or DEFAULT_WORKSHEET_NAME)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    headers = [str(h) for h in values[0]]
    rows = [r for r in values[1:] if any(_clean(v) for v in r)]
    return pd.DataFrame(rows, columns=headers)


@st.cache_data(ttl=20)
def load_google_snapshot_data(web_app_url: str, token: str) -> pd.DataFrame:
    parsed = {}
    last_error = ""
    for action_name in ("read_snapshot", "readSnapshot", "read", "snapshot"):
        payload = {"token": token or "", "action": action_name}
        parsed = call_apps_script_(web_app_url, payload)
        if bool(parsed.get("ok")):
            break
        error_text = _clean(parsed.get("error") or "")
        if "unsupported action" in error_text.lower():
            last_error = error_text
            continue
        raise RuntimeError(str(parsed.get("error") or parsed))

    if not bool(parsed.get("ok")):
        raise RuntimeError(last_error or str(parsed.get("error") or parsed))

    data = parsed.get("data") or {}
    headers = [str(h) for h in (data.get("headers") or [])]
    rows = data.get("rows") or []
    if not headers:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=headers)
    return _normalize_snapshot_df(df)


def _normalize_snapshot_df(df: pd.DataFrame) -> pd.DataFrame:
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
    df = df.rename(columns=rename)

    for col in ["Platform", "Type", "Ticker", "Status", "Origin_Currency"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].map(_clean)

    for col in ["Quantity", "Origin_Buy_Price", "Cost_Origin", "Commission", "Cost_ILS", "Current_Value_ILS"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = df[col].map(_num)

    if "Purchase_Date" not in df.columns:
        df["Purchase_Date"] = pd.NaT
    df["Purchase_Date"] = pd.to_datetime(df["Purchase_Date"], errors="coerce", dayfirst=True)

    if "Trade_ID" not in df.columns:
        df["Trade_ID"] = df.apply(_to_trade_id, axis=1)
    else:
        df["Trade_ID"] = df["Trade_ID"].map(_clean)
        missing = df["Trade_ID"] == ""
        if missing.any():
            df.loc[missing, "Trade_ID"] = df.loc[missing].apply(_to_trade_id, axis=1)

    df["Record_Source"] = "STATE_SNAPSHOT"
    df["Event_Type"] = "TRADE"
    df["Action"] = df["Status"].map(lambda x: "SELL" if _clean(x) == "סגור" else "BUY")
    return df


def build_demo_snapshot_data() -> pd.DataFrame:
    # Non-personal sample portfolio for a richer, presentation-friendly demo.
    demo_rows = [
        {
            "Current_Location": "",
            "Platform": "BrokerOne",
            "Type": "ETF",
            "Ticker": "VOO",
            "Purchase_Date": "2025-03-10",
            "Quantity": 8,
            "Origin_Buy_Price": 470,
            "Cost_Origin": 3760,
            "Origin_Currency": "USD",
            "Commission": 2,
            "Status": "פתוח",
            "Cost_ILS": 13850,
            "Current_Value_ILS": 15400,
        },
        {
            "Current_Location": "",
            "Platform": "BrokerOne",
            "Type": "ETF",
            "Ticker": "QQQ",
            "Purchase_Date": "2025-06-02",
            "Quantity": 12,
            "Origin_Buy_Price": 455,
            "Cost_Origin": 5460,
            "Origin_Currency": "USD",
            "Commission": 3,
            "Status": "פתוח",
            "Cost_ILS": 20120,
            "Current_Value_ILS": 23640,
        },
        {
            "Current_Location": "Cold Wallet",
            "Platform": "CryptoX",
            "Type": "קריפטו",
            "Ticker": "BTC",
            "Purchase_Date": "2025-08-11",
            "Quantity": 0.15,
            "Origin_Buy_Price": 62000,
            "Cost_Origin": 9300,
            "Origin_Currency": "USD",
            "Commission": 12,
            "Status": "פתוח",
            "Cost_ILS": 34300,
            "Current_Value_ILS": 41800,
        },
        {
            "Current_Location": "Trading Wallet",
            "Platform": "CryptoX",
            "Type": "קריפטו",
            "Ticker": "SOL",
            "Purchase_Date": "2025-10-09",
            "Quantity": 40,
            "Origin_Buy_Price": 155,
            "Cost_Origin": 6200,
            "Origin_Currency": "USD",
            "Commission": 9,
            "Status": "פתוח",
            "Cost_ILS": 22750,
            "Current_Value_ILS": 25680,
        },
        {
            "Current_Location": "",
            "Platform": "BrokerOne",
            "Type": "שוק ההון",
            "Ticker": "IBIT",
            "Purchase_Date": "2025-12-17",
            "Quantity": 75,
            "Origin_Buy_Price": 57,
            "Cost_Origin": 4275,
            "Origin_Currency": "USD",
            "Commission": 2,
            "Status": "פתוח",
            "Cost_ILS": 15710,
            "Current_Value_ILS": 18490,
        },
        {
            "Current_Location": "",
            "Platform": "BrokerOne",
            "Type": "ETF",
            "Ticker": "QQQ",
            "Purchase_Date": "2025-11-02",
            "Quantity": 6,
            "Origin_Buy_Price": 510,
            "Cost_Origin": 3060,
            "Origin_Currency": "USD",
            "Commission": 2,
            "Status": "סגור",
            "Cost_ILS": 11200,
            "Current_Value_ILS": 11980,
        },
        {
            "Current_Location": "Trading Wallet",
            "Platform": "CryptoX",
            "Type": "קריפטו",
            "Ticker": "ETH",
            "Purchase_Date": "2026-01-18",
            "Quantity": 2.8,
            "Origin_Buy_Price": 2400,
            "Cost_Origin": 6720,
            "Origin_Currency": "USD",
            "Commission": 8,
            "Status": "פתוח",
            "Cost_ILS": 24800,
            "Current_Value_ILS": 26250,
        },
    ]
    df = pd.DataFrame(demo_rows)
    return _normalize_snapshot_df(df)


def load_snapshot_data(
    web_app_url: str,
    token: str,
    spreadsheet_ref: str,
    worksheet_name: str,
    service_account_file: str,
) -> Tuple[pd.DataFrame, str]:
    clean_url = _clean(web_app_url)
    if clean_url:
        if not is_apps_script_web_app_url(clean_url):
            raise RuntimeError("קישור Web App לא תקין")
        try:
            return load_google_snapshot_data(clean_url, token), "apps_script"
        except Exception as exc:
            # Fallback to read-only gspread when Apps Script is old or token is invalid.
            err_text = str(exc).lower()
            can_fallback = ("unsupported action" in err_text) or ("unauthorized" in err_text)
            if not can_fallback:
                raise
            if not _clean(spreadsheet_ref):
                raise
            raw_df = load_google_snapshot_data_via_gspread(spreadsheet_ref, worksheet_name, service_account_file)
            return _normalize_snapshot_df(raw_df), "gspread"

    raw_df = load_google_snapshot_data_via_gspread(spreadsheet_ref, worksheet_name, service_account_file)
    return _normalize_snapshot_df(raw_df), "gspread"


def sync_trade_to_sheet(web_app_url: str, token: str, action: str, trade_row: Dict[str, object]) -> Tuple[bool, str]:
    if not web_app_url:
        return False, "חסר קישור Web App של Apps Script"

    payload = {"token": token or "", "action": action, "trade": trade_row}

    try:
        parsed = call_apps_script_(web_app_url, payload)
    except urlerror.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else str(exc)
        return False, f"שגיאת HTTP מהשרת: {exc.code} | {details}"
    except Exception as exc:
        return False, f"שגיאת תקשורת ל-Apps Script: {exc}"

    if bool(parsed.get("ok")):
        return True, str(parsed.get("message") or "נשמר בהצלחה בגוגל שיט")
    return False, str(parsed.get("error") or parsed)


def main() -> None:
    st.set_page_config(page_title="מערכת ניהול תיק", page_icon="📈", layout="wide")
    template = "plotly_white"

    settings = load_local_settings()
    language_default = settings.get("language", "English")
    if language_default not in {"English", "עברית"}:
        language_default = "English"

    top_left, top_right = st.columns([9, 1])
    with top_left:
        st.markdown("### Portfolio Manager OS")
    with top_right:
        if hasattr(st, "popover"):
            with st.popover("⋮"):
                language = st.selectbox("Language", ["English", "עברית"], index=0 if language_default == "English" else 1)
                demo_mode = st.checkbox("Demo view", value=bool(settings.get("demo_mode", False)))
        else:
            with st.expander("⋮"):
                language = st.selectbox("Language", ["English", "עברית"], index=0 if language_default == "English" else 1)
                demo_mode = st.checkbox("Demo view", value=bool(settings.get("demo_mode", False)))

    tr = (lambda en, he: he if language == "עברית" else en)

    st.sidebar.title(tr("Navigation", "ניווט"))
    page_dashboard = tr("Dashboard", "דשבורד")
    page_risk = tr("Risk & FIFO", "סיכונים ו-FIFO")
    page_manage = tr("Trade Management", "ניהול עסקאות")
    page_quality = tr("Data Quality", "בקרת נתונים")
    page = st.sidebar.radio(tr("Page", "עמוד"), [page_dashboard, page_risk, page_manage, page_quality])

    with st.sidebar.expander(tr("Connection & Data Settings", "הגדרות חיבור ונתונים"), expanded=False):
        web_app_url = st.text_input("Apps Script Web App URL", value=settings.get("web_app_url", DEFAULT_WEB_APP_URL))
        api_token = st.text_input("API Token", value=settings.get("api_token", ""), type="password")
        st.caption(tr("If Web App is missing, fallback to gspread is available.", "אם אין Web App אפשר לעבוד בקריאה ישירה עם Service Account (gspread)."))
        spreadsheet_ref = st.text_input(
            "Spreadsheet URL or ID",
            value=settings.get("spreadsheet_ref", ""),
            help=tr("Use full Google Sheets URL or only its ID.", "ניתן להדביק URL מלא של Google Sheets או את ה-ID בלבד."),
        )
        worksheet_name = st.text_input(tr("Worksheet name", "שם גיליון"), value=settings.get("worksheet_name", DEFAULT_WORKSHEET_NAME))
        service_account_file = st.text_input(
            tr("Service Account JSON path", "נתיב קובץ Service Account JSON"),
            value=settings.get("service_account_file", str(DEFAULT_SERVICE_ACCOUNT_FILE)),
        )

        if st.button(tr("Save settings on this machine", "שמור חיבור למחשב הזה")):
            ok = save_local_settings(
                web_app_url,
                api_token,
                spreadsheet_ref,
                worksheet_name,
                service_account_file,
                language,
                demo_mode,
            )
            if ok:
                st.success(tr("Settings saved locally", "החיבור נשמר מקומית"))
            else:
                st.error(tr("Failed to save settings", "שמירת החיבור נכשלה"))

        if st.button(tr("Refresh Google data", "רענון נתונים מגוגל")):
            load_google_snapshot_data.clear()
            load_google_snapshot_data_via_gspread.clear()
            st.rerun()

    if _looks_like_private_key_blob(api_token):
        st.error(tr("API Token appears invalid (looks like a private key). Paste your Apps Script API token instead.", "נראה שבשדה API Token הודבק מפתח פרטי. יש להדביק את ה-API Token של Apps Script."))
        st.stop()

    if demo_mode:
        df, source_mode = build_demo_snapshot_data(), "demo"
    else:
        web_url_clean = _clean(web_app_url)
        using_apps_script = bool(web_url_clean)
        if using_apps_script and not is_apps_script_web_app_url(web_url_clean):
            if is_google_sheet_url(web_url_clean):
                st.error(tr("Google Sheets URL was entered instead of Apps Script Web App URL", "הוזן קישור של Google Sheets במקום קישור Web App של Apps Script"))
                st.info(tr("Use a URL that starts with https://script.google.com/macros/s/ and ends with /exec", "צריך להדביק קישור שמתחיל ב-https://script.google.com/macros/s/ ומסתיים ב-/exec"))
            else:
                st.error(tr("Invalid Web App URL", "קישור Web App לא תקין"))
                st.info("https://script.google.com/macros/s/.../exec")
            st.stop()

        if not using_apps_script and not _clean(spreadsheet_ref):
            st.error(tr("Missing Google connection: set Web App URL or Spreadsheet URL/ID", "חסר חיבור לגוגל: הזן Web App URL או Spreadsheet URL/ID עבור gspread"))
            st.stop()

        try:
            df, source_mode = load_snapshot_data(web_url_clean, api_token, spreadsheet_ref, worksheet_name, service_account_file)
        except Exception as exc:
            st.error(f"{tr('Google data read failed', 'קריאת נתונים מגוגל נכשלה')}: {exc}")
            st.stop()

    web_url_clean = _clean(settings.get("web_app_url", DEFAULT_WEB_APP_URL) if demo_mode else web_app_url)

    if source_mode == "gspread":
        st.sidebar.warning(tr("gspread read mode active (write actions disabled).", "מצב קריאה דרך gspread פעיל (ללא Web App פעיל, פעולות עריכה/מחיקה מושבתות)."))
    elif source_mode == "demo":
        st.sidebar.info(tr("Demo mode active - sample data only.", "מצב הדגמה פעיל - נתוני דוגמה בלבד."))
    else:
        st.sidebar.success(tr("Apps Script mode active (read + write).", "חיבור דרך Apps Script פעיל (קריאה + כתיבה)."))

    if df.empty:
        st.warning("לא נמצאו עסקאות ב'תמונת מצב' בגוגל שיט")
        st.stop()

    trades = df[(df["Record_Source"] == "STATE_SNAPSHOT") & (df["Event_Type"] == "TRADE")].copy() if "Record_Source" in df.columns else df.copy()
    trades["Status"] = trades["Status"].replace("", "פתוח")


    open_trades = trades[trades["Status"] != "סגור"].copy()
    closed_trades = trades[trades["Status"] == "סגור"].copy()

    # Revalue open positions from market feed when possible.
    tickers = tuple(sorted(t for t in open_trades["Ticker"].dropna().unique() if _clean(t)))
    live_prices = fetch_prices(tickers)

    open_trades["מחיר שוק"] = open_trades["Ticker"].map(live_prices).fillna(0.0)
    open_trades["שווי שוק (יחסי מטבע מקור)"] = open_trades["Quantity"] * open_trades["מחיר שוק"]

    total_cost = float(open_trades["Cost_ILS"].sum())
    total_value = float(open_trades["Current_Value_ILS"].sum())
    total_profit = total_value - total_cost
    total_return = (total_profit / total_cost) if total_cost else 0.0

    if page == page_dashboard:
        st.title(tr("Portfolio Manager - Advanced Dashboard", "מערכת ניהול תיק - דשבורד מתקדם"))
        if source_mode == "demo":
            st.info(
                tr(
                    "Demo mode is active: diversified sample portfolio, live-style analytics, and storytelling widgets are enabled.",
                    "מצב הדגמה פעיל: פורטפוליו מגוון לדוגמה, אנליטיקה עשירה ותצוגת דמו מרשימה פעילים כרגע.",
                )
            )
        st.caption(
            f"{len(trades):,} {tr('rows loaded', 'רשומות נטענו')} | "
            f"{len(trades[trades['Record_Source'] == 'STATE_SNAPSHOT']):,} {tr('snapshot rows', 'שורות תמונת מצב')} | "
            f"{len(trades[trades['Status'] == 'סגור']):,} {tr('closed', 'סגורות')}"
        )

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("שווי כולל (₪)", f"{total_value:,.0f}", delta=f"{total_profit:,.0f} ₪")
        c2.metric("עלות כוללת (₪)", f"{total_cost:,.0f}")
        c3.metric("תשואה כוללת", f"{total_return:.2%}")
        c4.metric("רווח ממומש (סטטוס סגור)", f"{(closed_trades['Current_Value_ILS'].sum()-closed_trades['Cost_ILS'].sum()):,.0f} ₪")

        summary = open_trades.groupby("Ticker", as_index=False).agg(
            כמות=("Quantity", "sum"),
            עלות=("Cost_ILS", "sum"),
            שווי=("Current_Value_ILS", "sum"),
        )
        summary["רווח/הפסד"] = summary["שווי"] - summary["עלות"]
        summary["תשואה"] = np.where(summary["עלות"] != 0, summary["רווח/הפסד"] / summary["עלות"], 0.0)

        col_a, col_b = st.columns(2)
        with col_a:
            fig_pie = px.pie(summary, names="Ticker", values="שווי", title="חלוקת תיק לפי נכס", hole=0.45, template=template)
            st.plotly_chart(fig_pie, use_container_width=True)
        with col_b:
            fig_bar = px.bar(summary, x="Ticker", y="רווח/הפסד", color="רווח/הפסד", title="רווח/הפסד לפי נכס", template=template)
            st.plotly_chart(fig_bar, use_container_width=True)

        st.subheader("טבלת חשיפה מפורטת")
        st.dataframe(summary.style.format({"כמות": "{:.8f}", "עלות": "₪{:,.0f}", "שווי": "₪{:,.0f}", "רווח/הפסד": "₪{:,.0f}", "תשואה": "{:.2%}"}))

        with st.expander(tr("Full snapshot transactions (including closed)", "רשימת העסקאות המלאה בתמונת מצב כולל סגורות"), expanded=False):
            snapshot_view = trades[trades["Record_Source"] == "STATE_SNAPSHOT"].copy() if "Record_Source" in trades.columns else trades.copy()
            ticker_options = sorted([t for t in snapshot_view["Ticker"].dropna().astype(str).unique() if _clean(t)]) if "Ticker" in snapshot_view.columns else []
            selected_tickers = st.multiselect(tr("Filter by ticker", "סינון לפי טיקר"), ticker_options, default=[])
            if selected_tickers and "Ticker" in snapshot_view.columns:
                snapshot_view = snapshot_view[snapshot_view["Ticker"].isin(selected_tickers)]
            show_cols = [c for c in ["Purchase_Date", "Platform", "Type", "Ticker", "Quantity", "Cost_Origin", "Cost_ILS", "Commission", "Status", "validation_status"] if c in snapshot_view.columns]
            if show_cols:
                snapshot_sort_cols = [c for c in ["Purchase_Date", "Ticker"] if c in snapshot_view.columns]
                if snapshot_sort_cols:
                    snapshot_asc = [False if c == "Purchase_Date" else True for c in snapshot_sort_cols]
                    snapshot_view = snapshot_view.sort_values(snapshot_sort_cols, ascending=snapshot_asc)
                st.dataframe(snapshot_view[show_cols], use_container_width=True)

        st.subheader("תובנות בהשראת דף הבית")
        reports = build_home_inspired_reports(open_trades)
        m1, m2, m3 = st.columns(3)
        m1.metric("אחוז קריפטו מהתיק", f"{reports['אחוז_קריפטו']:.2%}")
        m2.metric("אחוז ביטקוין מהתיק", f"{reports['אחוז_ביטקוין_מהתיק']:.2%}")
        m3.metric("אחוז ביטקוין מסך הקריפטו", f"{reports['אחוז_ביטקוין_מהקריפטו']:.2%}")

        report_choice = st.selectbox(
            "בחר דוח",
            ["ריכוז נכסים", "המנצח והמפסיד", "סך השקעה נטו", "שערי מטבע חיים"],
        )
        if report_choice == "ריכוז נכסים":
            st.dataframe(
                reports["טבלת_ריכוז"].style.format(
                    {
                        "כמות ישירה": "{:.8f}",
                        "שווי ישיר (₪)": "₪{:,.0f}",
                        "כמות בקרן": "{:.8f}",
                        "שווי בקרן (₪)": "₪{:,.0f}",
                        "חשיפה כוללת (₪)": "₪{:,.0f}",
                    }
                )
            )
        elif report_choice == "המנצח והמפסיד":
            st.dataframe(reports["טבלת_מנצח_מפסיד"].style.format({"תשואה": "{:.2%}"}))
        elif report_choice == "סך השקעה נטו":
            st.dataframe(reports["טבלת_הפקדות"].style.format({"עלות_שקלית": "₪{:,.0f}", "שווי_שקלי": "₪{:,.0f}", "רווח_הפסד": "₪{:,.0f}"}))
        else:
            rates = reports["שערים_חיים"]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("USD/ILS", f"{rates['USD/ILS']:.4f}")
            c2.metric("BTC/USD", f"{rates['BTC/USD']:.2f}")
            c3.metric("ETH/USD", f"{rates['ETH/USD']:.2f}")
            c4.metric("SOL/USD", f"{rates['SOL/USD']:.2f}")

    elif page == page_risk:
        st.title(tr("Risk, Performance and FIFO", "סיכונים, ביצועים ועלות FIFO"))
        fifo_df = fifo_metrics(trades)
        st.subheader("מנוע FIFO")
        if fifo_df.empty:
            st.info("אין מספיק נתונים לחישוב FIFO")
        else:
            st.dataframe(
                fifo_df.style.format(
                    {
                        "כמות פתוחה (FIFO)": "{:.8f}",
                        "עלות פתוחה (₪)": "₪{:,.0f}",
                        "רווח ממומש (₪)": "₪{:,.0f}",
                        "מחיר ממוצע פתוח (₪)": "₪{:,.2f}",
                    }
                )
            )

        holdings = open_trades.groupby("Ticker", as_index=False)["Quantity"].sum()
        value_series = portfolio_price_history(tuple(holdings["Ticker"]), tuple(holdings["Quantity"]), days=365)
        metrics = risk_metrics(value_series)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Sharpe", f"{metrics['sharpe']:.2f}")
        c2.metric("תנודתיות שנתית", f"{metrics['vol']:.2%}")
        c3.metric("Max Drawdown", f"{metrics['mdd']:.2%}")
        c4.metric("CAGR", f"{metrics['cagr']:.2%}")

        if not value_series.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=value_series.index, y=value_series.values, mode="lines", name="שווי היסטורי משוער"))
            fig.update_layout(template=template, title="סדרת שווי היסטורית (הערכה לפי מחירי שוק)", xaxis_title="תאריך", yaxis_title="שווי")
            st.plotly_chart(fig, use_container_width=True)

    elif page == page_manage:
        st.title(tr("Trade Management (Add / Edit / Delete)", "ניהול עסקאות (הוספה / עריכה / מחיקה)"))
        st.caption("שמירה מתבצעת ישירות ל-Google Sheets דרך Apps Script (ללא כתיבה ל-CSV).")
        write_enabled = source_mode == "apps_script"
        if not write_enabled:
            st.warning(tr("This page is read-only in current mode. Connect Apps Script Web App to enable write actions.", "הדף במצב קריאה בלבד כי אין Web App URL תקין. כדי לאפשר הוספה/עריכה/מחיקה, חבר Apps Script Web App."))

        st.caption(f"מציג {len(trades):,} עסקאות תמונת מצב, כולל עסקאות סגורות.")
        trade_view = trades.copy()
        status_filter = st.multiselect("סינון סטטוס", sorted([s for s in trade_view["Status"].dropna().astype(str).unique() if s]), default=[])
        if status_filter:
            trade_view = trade_view[trade_view["Status"].isin(status_filter)]
        preview_cols = [c for c in ["Trade_ID", "Purchase_Date", "Platform", "Type", "Ticker", "Quantity", "Cost_Origin", "Cost_ILS", "Status", "validation_status"] if c in trade_view.columns]
        if preview_cols:
            sort_cols = [c for c in ["Purchase_Date", "Ticker"] if c in trade_view.columns]
            if sort_cols:
                asc = [False if c == "Purchase_Date" else True for c in sort_cols]
                trade_view = trade_view.sort_values(sort_cols, ascending=asc)
            st.dataframe(trade_view[preview_cols], use_container_width=True)

        mode = st.radio("פעולה", ["הוספה", "עריכה", "מחיקה"], horizontal=True)
        editable_cols = ["Platform", "Type", "Ticker", "Purchase_Date", "Quantity", "Origin_Buy_Price", "Cost_Origin", "Origin_Currency", "Commission", "Status", "Cost_ILS", "Current_Value_ILS", "Action", "Event_Type", "Trade_ID"]
        platforms = trades["Platform"].dropna().astype(str).tolist() if "Platform" in trades.columns else []
        types = trades["Type"].dropna().astype(str).tolist() if "Type" in trades.columns else []
        tickers = trades["Ticker"].dropna().astype(str).tolist() if "Ticker" in trades.columns else []
        currencies = trades["Origin_Currency"].dropna().astype(str).tolist() if "Origin_Currency" in trades.columns else []

        if mode == "הוספה":
            with st.form("add_form"):
                new_row = {
                    "Platform": _select_or_type("פלטפורמה", platforms, "Bit2C", "add_platform"),
                    "Type": _select_or_type("סוג נכס", types, "קריפטו", "add_type"),
                    "Ticker": _select_or_type("טיקר", tickers, "BTC", "add_ticker").upper(),
                    "Purchase_Date": st.date_input("תאריך רכישה", value=datetime.now()).strftime("%Y-%m-%d"),
                    "Quantity": st.number_input("כמות", value=0.0, format="%.8f"),
                    "Origin_Buy_Price": st.number_input("שער קנייה", value=0.0),
                    "Cost_Origin": st.number_input("עלות מקור", value=0.0),
                    "Origin_Currency": _select_or_type("מטבע מקור", currencies, "USD", "add_currency").upper(),
                    "Commission": st.number_input("עמלה", value=0.0),
                    "Status": st.selectbox("סטטוס", ["פתוח", "סגור"]),
                    "Cost_ILS": st.number_input("עלות ILS", value=0.0),
                    "Current_Value_ILS": st.number_input("שווי ILS", value=0.0),
                    "Action": st.selectbox("פעולה חשבונאית", ["BUY", "SELL"]),
                    "Event_Type": "TRADE",
                }
                new_row["Trade_ID"] = hashlib.sha1(json.dumps(new_row, ensure_ascii=False).encode("utf-8")).hexdigest()[:16]
                submitted = st.form_submit_button("שמור")

            if submitted:
                if not write_enabled:
                    st.error("לא ניתן לשמור במצב gspread. הגדר Web App URL תקין בצד שמאל.")
                else:
                    ok, msg = sync_trade_to_sheet(web_url_clean, api_token, "add", new_row)
                    if ok:
                        st.success("הרשומה נוספה ישירות ל-Google Sheets")
                        st.info(msg)
                        load_google_snapshot_data.clear()
                        st.rerun()
                    else:
                        st.error(f"הוספה נכשלה: {msg}")

        elif mode == "עריכה":
            options = trades["Trade_ID"].dropna().astype(str).tolist()
            if not options:
                st.info("אין Trade_ID זמין לעריכה")
                return
            selected = st.selectbox("בחר Trade_ID", options)
            row_idx = trades.index[trades["Trade_ID"].astype(str) == selected]
            if len(row_idx) == 0:
                st.warning("לא נמצאה רשומה")
            else:
                idx = row_idx[0]
                with st.form("edit_form"):
                    edited = {}
                    for col in editable_cols:
                        if col not in trades.columns:
                            continue
                        val = trades.at[idx, col]
                        if col in {"Quantity", "Origin_Buy_Price", "Cost_Origin", "Commission", "Cost_ILS", "Current_Value_ILS"}:
                            edited[col] = st.number_input(col, value=float(_num(val)), key=f"e_{col}")
                        elif col == "Purchase_Date":
                            d = pd.to_datetime(val, errors="coerce")
                            edited[col] = st.date_input(col, value=(d.date() if pd.notna(d) else datetime.now().date()), key=f"e_{col}").strftime("%Y-%m-%d")
                        elif col == "Platform":
                            edited[col] = _select_or_type("פלטפורמה", platforms, _clean(val), f"edit_{selected}_platform")
                        elif col == "Type":
                            edited[col] = _select_or_type("סוג נכס", types, _clean(val), f"edit_{selected}_type")
                        elif col == "Ticker":
                            edited[col] = _select_or_type("טיקר", tickers, _clean(val), f"edit_{selected}_ticker").upper()
                        elif col == "Origin_Currency":
                            edited[col] = _select_or_type("מטבע מקור", currencies, _clean(val), f"edit_{selected}_currency").upper()
                        elif col == "Status":
                            edited[col] = st.selectbox(col, ["פתוח", "סגור"], index=0 if _clean(val) != "סגור" else 1, key=f"e_{col}")
                        elif col == "Action":
                            edited[col] = st.selectbox(col, ["BUY", "SELL"], index=0 if _clean(val) != "SELL" else 1, key=f"e_{col}")
                        else:
                            edited[col] = st.text_input(col, value=_clean(val), key=f"e_{col}")
                    submitted = st.form_submit_button("עדכן")

                if submitted:
                    if not write_enabled:
                        st.error("לא ניתן לעדכן במצב gspread. הגדר Web App URL תקין בצד שמאל.")
                    else:
                        edited["Trade_ID"] = selected
                        ok, msg = sync_trade_to_sheet(web_url_clean, api_token, "edit", edited)
                        if ok:
                            st.success("הרשומה עודכנה ישירות ב-Google Sheets")
                            st.info(msg)
                            load_google_snapshot_data.clear()
                            st.rerun()
                        else:
                            st.error(f"עדכון נכשל: {msg}")

        else:
            options = trades["Trade_ID"].dropna().astype(str).tolist()
            if not options:
                st.info("אין Trade_ID זמין למחיקה")
                return
            selected = st.selectbox("בחר Trade_ID למחיקה", options)
            if st.button("מחק רשומה"):
                if not write_enabled:
                    st.error("לא ניתן למחוק במצב gspread. הגדר Web App URL תקין בצד שמאל.")
                else:
                    delete_payload = {"Trade_ID": _clean(selected)}
                    ok, msg = sync_trade_to_sheet(web_url_clean, api_token, "delete", delete_payload)
                    if ok:
                        st.success("הרשומה נמחקה ישירות מ-Google Sheets")
                        st.info(msg)
                        load_google_snapshot_data.clear()
                        st.rerun()
                    else:
                        st.error(f"מחיקה נכשלה: {msg}")

    else:
        st.title(tr("Data Quality", "בקרת נתונים ואיכות"))
        status_counts = trades.groupby("Status").size().reset_index(name="count")
        st.dataframe(status_counts)
        fig = px.pie(status_counts, names="Status", values="count", title="פיזור סטטוסי עסקאות", template=template)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("נתונים אחרונים")
        st.dataframe(df.tail(30))


if __name__ == "__main__":
    main()
