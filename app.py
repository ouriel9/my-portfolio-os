import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import gspread
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf

try:
    from google.oauth2.service_account import Credentials
except Exception:  # pragma: no cover - optional for local preview environments
    Credentials = None

DATA_FILE = Path(__file__).resolve().parent / "DATA" / "verified_data.csv"
SHEET_SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]
RISK_FREE_ANNUAL = 0.02
CRYPTO_ETFS = {"IBIT", "ETHA", "BSOL", "MSTR"}


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


def get_gspread_client(credentials_json: Optional[str]):
    if not credentials_json or Credentials is None:
        return None
    try:
        credentials_dict = json.loads(credentials_json)
        creds = Credentials.from_service_account_info(credentials_dict, scopes=SHEET_SCOPE)
        return gspread.authorize(creds)
    except Exception:
        return None


def sync_trade_to_sheet(gc, sheet_url: str, action: str, trade_row: Dict[str, object]) -> Tuple[bool, str]:
    try:
        sh = gc.open_by_url(sheet_url)
        ws = sh.worksheet("תמונת מצב")
        audit = sh.worksheet("תגובות לטופס 1")

        header = ws.row_values(1)
        if not header:
            return False, "הטאב תמונת מצב ריק מכותרות"
        if "Trade_ID" not in header:
            return False, "עמודת Trade_ID חסרה בטאב תמונת מצב"

        tid = str(trade_row.get("Trade_ID", "")).strip()
        if not tid:
            return False, "Trade_ID חסר"

        id_col = header.index("Trade_ID") + 1
        id_values = ws.col_values(id_col)[1:]
        row_index = None
        for i, value in enumerate(id_values, start=2):
            if str(value).strip() == tid:
                row_index = i
                break

        editable = set(trade_row.keys())

        def build_new_row() -> List[object]:
            row = [""] * len(header)
            for col, val in trade_row.items():
                if col in header:
                    row[header.index(col)] = val
            return row

        if action == "add":
            if row_index:
                return False, "הרשומה כבר קיימת. יש לבחור עריכה"
            ws.append_row(build_new_row(), value_input_option="USER_ENTERED")
        elif action == "edit":
            if not row_index:
                return False, "לא נמצאה רשומה לעדכון"
            existing = ws.row_values(row_index)
            if len(existing) < len(header):
                existing += [""] * (len(header) - len(existing))
            for col in editable:
                if col in header:
                    existing[header.index(col)] = trade_row.get(col, "")
            ws.update([existing], f"A{row_index}")
        elif action == "delete":
            if not row_index:
                return False, "לא נמצאה רשומה למחיקה"
            ws.delete_rows(row_index)
        else:
            return False, "פעולה לא נתמכת"

        audit.append_row([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            action,
            tid,
            "OK",
            json.dumps(trade_row, ensure_ascii=False),
        ])
        return True, "סונכרן בהצלחה"
    except Exception as exc:
        return False, f"שגיאת סנכרון: {exc}"


def persist_verified(df: pd.DataFrame) -> None:
    df_to_save = df.copy()
    df_to_save.to_csv(DATA_FILE, index=False, encoding="utf-8-sig")
    load_verified_data.clear()


def main() -> None:
    st.set_page_config(page_title="מערכת ניהול תיק", page_icon="📈", layout="wide")

    st.sidebar.title("הגדרות תצוגה וחיבור")
    theme = st.sidebar.radio("מצב תצוגה", ["כהה", "בהיר"], index=0)
    template = "plotly_dark" if theme == "כהה" else "plotly_white"

    creds_json = st.sidebar.text_area("JSON של Service Account (אופציונלי)", value="", height=120)
    sheet_url = st.sidebar.text_input("קישור Google Sheet (אופציונלי)", value="")
    view_mode = st.sidebar.selectbox(
        "טווח נתונים",
        ["עסקאות תמונת מצב בלבד", "כל הרשומות ב-verified_data"],
        index=0,
    )

    df = load_verified_data(str(DATA_FILE))
    if df.empty:
        st.error("לא נמצא קובץ DATA/verified_data.csv. יש להריץ תחילה את portfolio_validator.py")
        st.stop()

    trades = df[(df["Record_Source"] == "STATE_SNAPSHOT") & (df["Event_Type"] == "TRADE")].copy() if "Record_Source" in df.columns else df.copy()
    trades["Status"] = trades["Status"].replace("", "פתוח")

    page = st.sidebar.radio("ניווט", ["דשבורד", "סיכונים ו-FIFO", "ניהול עסקאות", "בקרת נתונים"])

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

    if page == "דשבורד":
        st.title("מערכת ניהול תיק - דשבורד מתקדם")
        st.caption(f"{len(trades):,} רשומות נטענו | {len(trades[trades['Record_Source'] == 'STATE_SNAPSHOT']):,} שורות תמונת מצב | {len(trades[trades['Status'] == 'סגור']):,} סגורות")

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

        with st.expander("רשימת העסקאות המלאה בתמונת מצב כולל סגורות", expanded=False):
            snapshot_view = trades[trades["Record_Source"] == "STATE_SNAPSHOT"].copy() if "Record_Source" in trades.columns else trades.copy()
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

    elif page == "סיכונים ו-FIFO":
        st.title("סיכונים, ביצועים ועלות FIFO")
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

    elif page == "ניהול עסקאות":
        st.title("ניהול עסקאות (הוספה / עריכה / מחיקה)")

        st.caption(f"מציג {len(trades):,} עסקאות תמונת מצב, כולל עסקאות סגורות.")
        trade_view = df.copy() if view_mode == "כל הרשומות ב-verified_data" else trades.copy()
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

        if mode == "הוספה":
            with st.form("add_form"):
                new_row = {
                    "Platform": st.text_input("פלטפורמה", "Bit2C"),
                    "Type": st.text_input("סוג נכס", "קריפטו"),
                    "Ticker": st.text_input("טיקר", "BTC"),
                    "Purchase_Date": st.date_input("תאריך רכישה", value=datetime.now()).strftime("%Y-%m-%d"),
                    "Quantity": st.number_input("כמות", value=0.0, format="%.8f"),
                    "Origin_Buy_Price": st.number_input("שער קנייה", value=0.0),
                    "Cost_Origin": st.number_input("עלות מקור", value=0.0),
                    "Origin_Currency": st.text_input("מטבע מקור", "USD"),
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
                for col in df.columns:
                    if col not in new_row:
                        new_row[col] = ""
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                persist_verified(df)
                st.success("הרשומה נוספה לקובץ verified_data.csv")

                gc = get_gspread_client(creds_json)
                if gc and sheet_url:
                    ok, msg = sync_trade_to_sheet(gc, sheet_url, "add", new_row)
                    st.info(msg if ok else f"כשל סנכרון: {msg}")

        elif mode == "עריכה":
            options = trades["Trade_ID"].dropna().astype(str).tolist()
            selected = st.selectbox("בחר Trade_ID", options)
            row_idx = df.index[df["Trade_ID"].astype(str) == selected]
            if len(row_idx) == 0:
                st.warning("לא נמצאה רשומה")
            else:
                idx = row_idx[0]
                with st.form("edit_form"):
                    edited = {}
                    for col in editable_cols:
                        if col not in df.columns:
                            continue
                        val = df.at[idx, col]
                        if col in {"Quantity", "Origin_Buy_Price", "Cost_Origin", "Commission", "Cost_ILS", "Current_Value_ILS"}:
                            edited[col] = st.number_input(col, value=float(_num(val)), key=f"e_{col}")
                        elif col == "Purchase_Date":
                            d = pd.to_datetime(val, errors="coerce")
                            edited[col] = st.date_input(col, value=(d.date() if pd.notna(d) else datetime.now().date()), key=f"e_{col}").strftime("%Y-%m-%d")
                        elif col == "Status":
                            edited[col] = st.selectbox(col, ["פתוח", "סגור"], index=0 if _clean(val) != "סגור" else 1, key=f"e_{col}")
                        elif col == "Action":
                            edited[col] = st.selectbox(col, ["BUY", "SELL"], index=0 if _clean(val) != "SELL" else 1, key=f"e_{col}")
                        else:
                            edited[col] = st.text_input(col, value=_clean(val), key=f"e_{col}")
                    submitted = st.form_submit_button("עדכן")

                if submitted:
                    for k, v in edited.items():
                        df.at[idx, k] = v
                    persist_verified(df)
                    st.success("הרשומה עודכנה")
                    gc = get_gspread_client(creds_json)
                    if gc and sheet_url:
                        ok, msg = sync_trade_to_sheet(gc, sheet_url, "edit", edited)
                        st.info(msg if ok else f"כשל סנכרון: {msg}")

        else:
            options = trades["Trade_ID"].dropna().astype(str).tolist()
            selected = st.selectbox("בחר Trade_ID למחיקה", options)
            if st.button("מחק רשומה"):
                row = df[df["Trade_ID"].astype(str) == selected]
                df = df[df["Trade_ID"].astype(str) != selected]
                persist_verified(df)
                st.success("הרשומה נמחקה")
                gc = get_gspread_client(creds_json)
                if gc and sheet_url and not row.empty:
                    ok, msg = sync_trade_to_sheet(gc, sheet_url, "delete", row.iloc[0].to_dict())
                    st.info(msg if ok else f"כשל סנכרון: {msg}")

    else:
        st.title("בקרת נתונים ואיכות")
        if "validation_status" in df.columns:
            status_counts = df[df.get("Record_Source", "") == "STATE_SNAPSHOT"].groupby("validation_status").size().reset_index(name="count")
            st.dataframe(status_counts)
            fig = px.pie(status_counts, names="validation_status", values="count", title="פיזור סטטוסי אימות", template=template)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("לא נמצאו עמודות אימות ב-verified_data.csv")

        st.subheader("נתונים אחרונים")
        st.dataframe(df.tail(30))


if __name__ == "__main__":
    main()
