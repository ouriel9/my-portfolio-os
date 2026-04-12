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
import streamlit.components.v1 as components
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
MANUAL_DEPOSITS_FILE = Path(__file__).resolve().parent / "manual_deposits_store.json"

LANG_EN = "English"
LANG_HE = "עברית"
DEFAULT_LANGUAGE = LANG_HE

COLUMN_LABELS = {
    "Ticker": {LANG_EN: "Ticker", LANG_HE: "טיקר"},
    "Open_Qty": {LANG_EN: "Active Quantity", LANG_HE: "כמות פעילה"},
    "Cost_ILS": {LANG_EN: "Total Cost (ILS)", LANG_HE: "עלות כוללת (₪)"},
    "Value_ILS": {LANG_EN: "Current Value (ILS)", LANG_HE: "שווי עדכני (₪)"},
    "Net_PnL_ILS": {LANG_EN: "Net P/L (ILS)", LANG_HE: "רווח/הפסד נטו (₪)"},
    "Yield_Origin": {LANG_EN: "Net Return (Origin)", LANG_HE: "תשואה נטו (מקור)"},
    "Yield_ILS": {LANG_EN: "Net Return (ILS)", LANG_HE: "תשואה נטו (₪)"},
    "Asset": {LANG_EN: "Target Asset", LANG_HE: "מטבע יעד"},
    "Direct_Qty": {LANG_EN: "Direct Holding (Qty)", LANG_HE: "אחזקה ישירה (כמות)"},
    "Direct_ILS": {LANG_EN: "Direct Holding (ILS)", LANG_HE: "אחזקה ישירה (₪)"},
    "ETF_Qty": {LANG_EN: "ETF Holding (Units)", LANG_HE: "דרך קרן סל (יחידות)"},
    "ETF_ILS": {LANG_EN: "ETF Holding (ILS)", LANG_HE: "דרך קרן סל (₪)"},
    "Total_Exposure_ILS": {LANG_EN: "Total Exposure (ILS)", LANG_HE: "סה\"כ חשיפה (₪)"},
    "Category": {LANG_EN: "Category", LANG_HE: "סוג"},
    "Yield": {LANG_EN: "Return", LANG_HE: "תשואה"},
    "Platform": {LANG_EN: "Platform", LANG_HE: "פלטפורמה"},
    "Net_Investment_ILS": {LANG_EN: "Net Investment (ILS)", LANG_HE: "עלות שקלית"},
    "Current_Value_ILS": {LANG_EN: "Current Value (ILS)", LANG_HE: "שווי שקלי"},
    "PnL_ILS": {LANG_EN: "Net P/L (ILS)", LANG_HE: "רווח/הפסד"},
}

SNAPSHOT_HEADERS = {
    "Purchase_Date": {LANG_EN: "Purchase Date", LANG_HE: "תאריך רכישה"},
    "Platform": {LANG_EN: "Platform", LANG_HE: "פלטפורמה"},
    "Type": {LANG_EN: "Asset Type", LANG_HE: "סוג נכס"},
    "Ticker": {LANG_EN: "Ticker", LANG_HE: "טיקר"},
    "Quantity": {LANG_EN: "Quantity", LANG_HE: "כמות"},
    "Origin_Buy_Price": {LANG_EN: "Buy Price", LANG_HE: "שער קנייה"},
    "Cost_Origin": {LANG_EN: "Cost (Origin)", LANG_HE: "עלות מקור"},
    "Cost_ILS": {LANG_EN: "Cost (ILS)", LANG_HE: "עלות שקלית"},
    "Current_Value_ILS": {LANG_EN: "Current Value (ILS)", LANG_HE: "שווי שקלי"},
    "Commission": {LANG_EN: "Commission", LANG_HE: "עמלה"},
    "Status": {LANG_EN: "Status", LANG_HE: "סטטוס"},
    "validation_status": {LANG_EN: "Validation", LANG_HE: "אימות"},
    "Trade_ID": {LANG_EN: "Trade ID", LANG_HE: "מזהה עסקה"},
    "Yield_Origin": {LANG_EN: "Return (Origin)", LANG_HE: "תשואה (מקור)"},
}

VALUE_LABELS = {
    "Status": {
        "פתוח": {LANG_EN: "Open", LANG_HE: "פתוח"},
        "סגור": {LANG_EN: "Closed", LANG_HE: "סגור"},
    },
    "Type": {
        "קריפטו": {LANG_EN: "Crypto", LANG_HE: "קריפטו"},
        "שוק ההון": {LANG_EN: "Capital Market", LANG_HE: "שוק ההון"},
        "ETF": {LANG_EN: "ETF", LANG_HE: "קרן סל"},
    },
}


def localize_column_name(col: str, language: str) -> str:
    if col in COLUMN_LABELS:
        return COLUMN_LABELS[col][language]
    return col


def localize_dataframe_columns(df: pd.DataFrame, language: str) -> pd.DataFrame:
    out = df.copy()
    out.columns = [localize_column_name(str(c), language) for c in out.columns]
    return out


def localize_snapshot_view(df: pd.DataFrame, language: str) -> pd.DataFrame:
    out = df.copy()
    renamed = {}
    for col in out.columns:
        if col in SNAPSHOT_HEADERS:
            renamed[col] = SNAPSHOT_HEADERS[col][language]
    if renamed:
        out = out.rename(columns=renamed)

    for raw_col, labels in VALUE_LABELS.items():
        visible_col = SNAPSHOT_HEADERS.get(raw_col, {}).get(language, raw_col)
        if visible_col in out.columns:
            out[visible_col] = out[visible_col].map(lambda v: labels.get(_clean(v), {}).get(language, _clean(v)))
    return out


def render_modern_metrics(items: List[Tuple[str, str, str]]) -> None:
    cards = []
    for title, value, delta in items:
        delta_html = f"<div class='pm-delta'>{delta}</div>" if _clean(delta) else ""
        cards.append(
            "<div class='pm-card'>"
            f"<div class='pm-title'>{title}</div>"
            f"<div class='pm-value'>{value}</div>"
            f"{delta_html}"
            "</div>"
        )
    st.markdown(f"<div class='pm-metric-grid'>{''.join(cards)}</div>", unsafe_allow_html=True)


def inject_global_styles(language: str) -> None:
    rtl = language == LANG_HE
    direction = "rtl" if rtl else "ltr"
    align = "right" if rtl else "left"
    css = f"""
    <style>
    .block-container {{padding-top: 3.0rem;}}
    footer,
    footer *,
    [data-testid="stFooter"],
    [data-testid="stFooter"] *,
    [data-testid="stAppCreator"],
    [data-testid="stAppCreator"] * {{
        display: none !important;
        visibility: hidden !important;
        max-height: 0 !important;
        min-height: 0 !important;
        overflow: hidden !important;
        opacity: 0 !important;
        pointer-events: none !important;
    }}
    [data-testid="stDecoration"] {{display: none !important;}}
    [data-testid="stToolbar"] {{display: flex !important; visibility: visible !important;}}
    [data-testid="stToolbarActions"] {{display: flex !important; visibility: visible !important;}}
    [data-testid="stDataFrame"] [role="grid"] {{direction: {direction}; text-align: {align};}}
    [data-testid="stDataFrame"] table {{direction: {direction}; text-align: {align};}}
    [data-testid="stDataFrame"] {{overflow-x: auto;}}
    [data-testid="stMetric"] {{direction: {direction};}}
    [data-baseweb="tab-list"] {{
        display: flex !important;
        overflow-x: auto;
        -webkit-overflow-scrolling: touch;
        scroll-snap-type: x mandatory;
        touch-action: pan-x;
        scrollbar-width: thin;
        white-space: nowrap;
    }}
    [data-baseweb="tab-list"] [data-baseweb="tab"] {{
        flex: 0 0 auto;
        scroll-snap-align: start;
    }}
    [data-testid="stMarkdownContainer"] p {{line-height: 1.35;}}
    [data-testid="collapsedControl"] button svg,
    [data-testid="stSidebarCollapsedControl"] button svg,
    button[aria-label*="sidebar"] svg {{display: none !important;}}
    [data-testid="collapsedControl"] button::before,
    [data-testid="stSidebarCollapsedControl"] button::before,
    button[aria-label*="sidebar"]::before {{
        content: "\2630";
        font-size: 1.28rem;
        font-weight: 700;
        line-height: 1;
        display: inline-block;
    }}
    .modern-card {{
        border: 1px solid rgba(120,120,120,0.18);
        border-radius: 16px;
        padding: 0.9rem 1rem;
        background: linear-gradient(180deg, rgba(250,250,250,0.95), rgba(245,247,250,0.95));
    }}
    .pm-metric-grid {{
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 0.75rem;
        margin: 0.35rem 0 1rem 0;
    }}
    .pm-card {{
        border: 1px solid rgba(120,120,120,0.2);
        border-radius: 16px;
        padding: 0.9rem 1rem;
        background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(245,247,250,0.98));
        box-shadow: 0 6px 18px rgba(10, 20, 30, 0.06);
        color: #1f2937 !important;
    }}
    .pm-title {{font-size: 0.82rem; opacity: 0.8; margin-bottom: 0.35rem; color: #4b5563 !important;}}
    .pm-value {{font-size: 1.35rem; font-weight: 700; line-height: 1.15; color: #111827 !important;}}
    .pm-delta {{font-size: 0.84rem; opacity: 0.75; margin-top: 0.2rem; color: #374151 !important;}}
    @media (max-width: 980px) {{
        .pm-metric-grid {{grid-template-columns: repeat(2, minmax(0, 1fr));}}
    }}
    @media (max-width: 640px) {{
        .pm-metric-grid {{grid-template-columns: 1fr;}}
        h1 {{font-size: 1.62rem !important; line-height: 1.25 !important;}}
        h2 {{font-size: 1.3rem !important; line-height: 1.25 !important;}}
        h3 {{font-size: 1.12rem !important; line-height: 1.25 !important;}}
        [data-testid="stMetricValue"] {{font-size: 1.22rem !important;}}
        [data-testid="stMetricLabel"] {{font-size: 0.8rem !important;}}
        .pm-title {{font-size: 0.76rem;}}
        .pm-value {{font-size: 1.12rem;}}
        .pm-delta {{font-size: 0.75rem;}}
        .block-container {{padding-top: 2.6rem; padding-left: 0.6rem; padding-right: 0.6rem;}}
        footer,
        [data-testid="stFooter"],
        [data-testid="stAppCreator"],
        [data-testid="stDecoration"] {{display: none !important;}}
    }}
    @media (max-width: 420px) {{
        h1 {{font-size: 1.42rem !important;}}
        h2 {{font-size: 1.16rem !important;}}
        h3 {{font-size: 1.0rem !important;}}
    }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


def inject_client_fixes() -> None:
    # Small runtime fixes for mobile UX and branding noise.
    components.html(
        """
        <script>
        (function () {
          const hideCss = `
            footer, footer *, [data-testid="stFooter"], [data-testid="stFooter"] *,
            [data-testid="stAppCreator"], [data-testid="stAppCreator"] *,
            [data-testid="stDecoration"], [role="contentinfo"] {
              display: none !important;
              visibility: hidden !important;
              opacity: 0 !important;
              max-height: 0 !important;
              overflow: hidden !important;
              pointer-events: none !important;
            }
          `;

          function injectHideStyle(targetDoc) {
            if (!targetDoc || !targetDoc.head) return;
            let styleTag = targetDoc.getElementById('pm-hide-streamlit-branding');
            if (!styleTag) {
              styleTag = targetDoc.createElement('style');
              styleTag.id = 'pm-hide-streamlit-branding';
              targetDoc.head.appendChild(styleTag);
            }
            if (styleTag.textContent !== hideCss) {
              styleTag.textContent = hideCss;
            }
          }

          function resolveRootDoc() {
            try {
              if (window.parent && window.parent !== window && window.parent.document) {
                return window.parent.document;
              }
            } catch (err) {
              // Cross-origin or sandbox limitation: fallback to current document.
            }
            return document;
          }

          let rootDoc = resolveRootDoc();
          let rootWin = rootDoc.defaultView || window;

          function removeBranding() {
            rootDoc = resolveRootDoc();
            rootWin = rootDoc.defaultView || window;
            injectHideStyle(document);
            if (rootDoc !== document) injectHideStyle(rootDoc);

            const brandingHosts = [
              'footer',
              '[data-testid="stFooter"]',
              '[data-testid="stAppCreator"]',
              '[data-testid="stDecoration"]',
              '[role="contentinfo"]'
            ];

            brandingHosts.forEach((sel) => {
              rootDoc.querySelectorAll(sel).forEach((el) => {
                el.style.display = 'none';
                el.style.visibility = 'hidden';
                el.style.maxHeight = '0';
                el.style.overflow = 'hidden';
              });
              document.querySelectorAll(sel).forEach((el) => {
                el.style.display = 'none';
                el.style.visibility = 'hidden';
                el.style.maxHeight = '0';
                el.style.overflow = 'hidden';
              });
            });

            rootDoc.querySelectorAll('a[href*="streamlit.io"], a[href*="share.streamlit.io"]').forEach((link) => {
              const holder = link.closest('footer, [data-testid="stFooter"], [data-testid="stAppCreator"], [role="contentinfo"]');
              const text = ((holder && holder.innerText) || link.innerText || '').toLowerCase();
              if (text.includes('streamlit') || text.includes('hosted with') || text.includes('created by')) {
                (holder || link).style.display = 'none';
              }
            });


            // Fallback: hide tiny fixed bottom-right badges that include Streamlit links.
            rootDoc.querySelectorAll('div, section, aside').forEach((el) => {
              const style = rootWin.getComputedStyle ? rootWin.getComputedStyle(el) : null;
              if (!style) return;
              const isFixed = style.position === 'fixed' || style.position === 'sticky';
              const nearBottom = parseFloat(style.bottom || '9999') <= 40;
              if (!isFixed || !nearBottom) return;
              if (el.querySelector('a[href*="streamlit.io"], a[href*="share.streamlit.io"]')) {
                el.style.display = 'none';
              }
            });
          }

          function findDashboardTabList() {
            const lists = Array.from(rootDoc.querySelectorAll('[data-baseweb="tab-list"]'));
            const he = ['סקירה', 'חלוקה', 'דוחות', 'סך הפקדות', 'עסקאות'];
            const en = ['overview', 'allocation', 'reports', 'total deposits', 'transactions'];

            for (const list of lists) {
              const labels = Array.from(list.querySelectorAll('[data-baseweb="tab"]')).map((tab) =>
                (tab.innerText || '').trim().toLowerCase()
              );
              const hasHebrew = he.every((name) => labels.some((lbl) => lbl.indexOf(name) >= 0));
              const hasEnglish = en.every((name) => labels.some((lbl) => lbl.indexOf(name) >= 0));
              if (hasHebrew || hasEnglish) return list;
            }
            return null;
          }

          function setupTabSwipe() {
            if (rootWin.__pmSwipeBound) return;

            const blockedSelector = [
              'input',
              'textarea',
              'select',
              'button',
              'a',
              '[role="button"]',
              '.js-plotly-plot',
              '[data-testid="stDataFrame"]',
              'iframe',
              '[data-no-swipe]'
            ].join(',');

            let startX = 0;
            let startY = 0;
            let shouldHandle = false;

            rootDoc.addEventListener('touchstart', (e) => {
              if (!findDashboardTabList()) return;
              if (!e.touches || !e.touches.length) return;
              if (e.touches.length > 1) return;
              const target = e.target;
              shouldHandle = !(target && target.closest && target.closest(blockedSelector));
              if (!shouldHandle) return;
              startX = e.touches[0].clientX;
              startY = e.touches[0].clientY;
            }, { passive: true });

            rootDoc.addEventListener('touchend', (e) => {
              if (!shouldHandle) return;
              if (!e.changedTouches || !e.changedTouches.length) return;
              const dx = e.changedTouches[0].clientX - startX;
              const dy = e.changedTouches[0].clientY - startY;
              if (Math.abs(dx) < 55 || Math.abs(dx) < Math.abs(dy) * 1.2) return;

              const tabList = findDashboardTabList();
              if (!tabList) return;

              const currentTabs = Array.from(tabList.querySelectorAll('[data-baseweb="tab"]'));
              const active = currentTabs.findIndex((t) => t.getAttribute('aria-selected') === 'true');
              if (active < 0) return;

              const next = dx < 0 ? active + 1 : active - 1;
              if (next >= 0 && next < currentTabs.length) {
                currentTabs[next].click();
                currentTabs[next].scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' });
              }
            }, { passive: true });

            rootWin.__pmSwipeBound = true;
          }

          function run() {
            removeBranding();
            setupTabSwipe();
          }

          run();
          const obs = new MutationObserver(run);
          obs.observe(rootDoc.body, { childList: true, subtree: true });
          rootWin.setInterval(removeBranding, 1200);
          window.setInterval(function () {
            injectHideStyle(document);
            if (rootDoc !== document) injectHideStyle(rootDoc);
          }, 2000);
        })();
        </script>
        """,
        height=0,
        width=0,
    )


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


def _parse_dates_flexible(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=True)
    missing = parsed.isna()
    if missing.any():
        parsed_alt = pd.to_datetime(series[missing], errors="coerce", dayfirst=False)
        parsed.loc[missing] = parsed_alt
    return parsed


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
    df["Purchase_Date"] = _parse_dates_flexible(df["Purchase_Date"])

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


def _select_or_type(label: str, options: List[str], default: str = "", key_prefix: str = "", tr_fn=None) -> str:
    cleaned = sorted({_clean(v) for v in options if _clean(v)})
    tr_local = tr_fn or (lambda en, he: he)
    mode = st.radio(
        f"{label} - {tr_local('Input mode', 'אופן הזנה')}",
        [tr_local("Pick from list", "בחירה מרשימה"), tr_local("Type manually", "הקלדה ידנית")],
        horizontal=True,
        key=f"{key_prefix}_{label}_mode",
    )
    if mode == tr_local("Pick from list", "בחירה מרשימה"):
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

    crypto_mask = (work["Type"] == "קריפטו") | (work["Ticker"].isin(CRYPTO_ETFS))
    crypto_value = float(work.loc[crypto_mask, "Current_Value_ILS"].sum())
    btc_value = float(work.loc[work["Ticker"].isin(["BTC", "IBIT"]), "Current_Value_ILS"].sum())

    fx = _safe_quote("USDILS=X")
    if fx <= 0:
        fx = 3.6

    work["Cost_Origin_With_Fee"] = work["Cost_Origin"] + work["Commission"]
    work["Value_Origin_Est"] = np.where(
        work["Origin_Currency"].str.upper() == "USD",
        work["Current_Value_ILS"] / fx,
        work["Current_Value_ILS"],
    )

    summary = work.groupby("Ticker", as_index=False).agg(
        Cost_ILS=("Cost_ILS", "sum"),
        Value_ILS=("Current_Value_ILS", "sum"),
        Cost_Origin=("Cost_Origin_With_Fee", "sum"),
        Value_Origin=("Value_Origin_Est", "sum"),
    )
    if summary.empty:
        winner_loser = pd.DataFrame(columns=["Category", "Ticker", "Yield"])
    else:
        summary["Yield"] = np.where(summary["Cost_Origin"] > 0, (summary["Value_Origin"] - summary["Cost_Origin"]) / summary["Cost_Origin"], 0.0)
        winner = summary.loc[summary["Yield"].idxmax()]
        loser = summary.loc[summary["Yield"].idxmin()]
        winner_loser = pd.DataFrame(
            [
                {"Category": "Winner", "Ticker": winner["Ticker"], "Yield": float(winner["Yield"])},
                {"Category": "Loser", "Ticker": loser["Ticker"], "Yield": float(loser["Yield"])},
            ]
        )

    platform_summary = work.groupby("Platform", as_index=False).agg(
        Net_Investment_ILS=("Cost_ILS", "sum"),
        Current_Value_ILS=("Current_Value_ILS", "sum"),
    )
    if not platform_summary.empty:
        platform_summary["PnL_ILS"] = platform_summary["Current_Value_ILS"] - platform_summary["Net_Investment_ILS"]

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
                "Asset": asset,
                "Direct_Qty": direct_qty,
                "Direct_ILS": direct_val,
                "ETF_Qty": etf_qty,
                "ETF_ILS": etf_val,
                "Total_Exposure_ILS": direct_val + etf_val,
            }
        )

    live_rates = {
        "USD/ILS": _safe_quote("USDILS=X"),
        "BTC/USD": _safe_quote("BTC-USD"),
        "ETH/USD": _safe_quote("ETH-USD"),
        "SOL/USD": _safe_quote("SOL-USD"),
    }

    return {
        "crypto_share": (crypto_value / total_value) if total_value else 0.0,
        "btc_share_of_portfolio": (btc_value / total_value) if total_value else 0.0,
        "btc_share_of_crypto": (btc_value / crypto_value) if crypto_value else 0.0,
        "concentration_table": pd.DataFrame(concentration_rows),
        "winner_loser_table": winner_loser,
        "net_investment_table": platform_summary,
        "live_rates": live_rates,
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


def load_manual_deposits_store() -> Dict[str, List[Dict[str, object]]]:
    if not MANUAL_DEPOSITS_FILE.exists():
        return {"live": [], "demo": []}
    try:
        raw = json.loads(MANUAL_DEPOSITS_FILE.read_text(encoding="utf-8"))
        out: Dict[str, List[Dict[str, object]]] = {"live": [], "demo": []}
        for mode in ["live", "demo"]:
            rows = raw.get(mode, []) if isinstance(raw, dict) else []
            if not isinstance(rows, list):
                rows = []
            clean_rows = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                platform = _clean(row.get("Platform", ""))
                if not platform:
                    continue
                clean_rows.append({
                    "Platform": platform,
                    "Manual_Deposit_ILS": _num(row.get("Manual_Deposit_ILS", 0.0)),
                })
            out[mode] = clean_rows
        return out
    except Exception:
        return {"live": [], "demo": []}


def save_manual_deposits_store(store: Dict[str, List[Dict[str, object]]]) -> bool:
    try:
        payload = {"live": [], "demo": []}
        for mode in ["live", "demo"]:
            rows = store.get(mode, []) if isinstance(store, dict) else []
            clean_rows = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                platform = _clean(row.get("Platform", ""))
                if not platform:
                    continue
                clean_rows.append({
                    "Platform": platform,
                    "Manual_Deposit_ILS": _num(row.get("Manual_Deposit_ILS", 0.0)),
                })
            payload[mode] = clean_rows
        MANUAL_DEPOSITS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False


def load_manual_deposits_remote(web_app_url: str, token: str, mode: str) -> Tuple[bool, List[Dict[str, object]], str]:
    try:
        payload = {
            "token": token or "",
            "action": "read_manual_deposits",
            "mode": ("demo" if _clean(mode).lower() == "demo" else "live"),
        }
        parsed = call_apps_script_(web_app_url, payload)
        if not bool(parsed.get("ok")):
            return False, [], str(parsed.get("error") or parsed)
        data = parsed.get("data") or {}
        rows = data.get("rows") or []
        clean_rows: List[Dict[str, object]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            platform = _clean(row.get("Platform", ""))
            if not platform:
                continue
            clean_rows.append({
                "Platform": platform,
                "Manual_Deposit_ILS": _num(row.get("Manual_Deposit_ILS", 0.0)),
            })
        return True, clean_rows, ""
    except Exception as exc:
        return False, [], str(exc)


def save_manual_deposits_remote(web_app_url: str, token: str, mode: str, rows: List[Dict[str, object]]) -> Tuple[bool, str]:
    try:
        payload = {
            "token": token or "",
            "action": "save_manual_deposits",
            "mode": ("demo" if _clean(mode).lower() == "demo" else "live"),
            "rows": rows,
        }
        parsed = call_apps_script_(web_app_url, payload)
        if bool(parsed.get("ok")):
            return True, ""
        return False, str(parsed.get("error") or parsed)
    except Exception as exc:
        return False, str(exc)


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
            "language": _clean(raw.get("language", DEFAULT_LANGUAGE)) or DEFAULT_LANGUAGE,
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
            "language": _clean(language) or DEFAULT_LANGUAGE,
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

        # Ignore template/placeholder secrets so local/env fallback can work.
        pk = out["private_key"].strip()
        if (
            not pk
            or "YOUR_PRIVATE_KEY" in pk
            or "BEGIN PRIVATE KEY" not in pk
            or "END PRIVATE KEY" not in pk
        ):
            return None
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
    df["Purchase_Date"] = _parse_dates_flexible(df["Purchase_Date"])

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
    # Synthetic, diversified showcase portfolio used only for demo mode.
    demo_rows = [
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
            "Platform": "Global Prime",
            "Type": "ETF",
            "Ticker": "VXUS",
            "Purchase_Date": "2025-03-18",
            "Quantity": 140,
            "Origin_Buy_Price": 58.4,
            "Cost_Origin": 8176,
            "Origin_Currency": "USD",
            "Commission": 5,
            "Status": "פתוח",
            "Cost_ILS": 30110,
            "Current_Value_ILS": 32280,
        },
        {
            "Current_Location": "",
            "Platform": "Global Prime",
            "Type": "ETF",
            "Ticker": "GLD",
            "Purchase_Date": "2025-04-10",
            "Quantity": 85,
            "Origin_Buy_Price": 211,
            "Cost_Origin": 17935,
            "Origin_Currency": "USD",
            "Commission": 7,
            "Status": "פתוח",
            "Cost_ILS": 66210,
            "Current_Value_ILS": 70850,
        },
        {
            "Current_Location": "",
            "Platform": "Global Prime",
            "Type": "ETF",
            "Ticker": "XLK",
            "Purchase_Date": "2025-05-27",
            "Quantity": 54,
            "Origin_Buy_Price": 219,
            "Cost_Origin": 11826,
            "Origin_Currency": "USD",
            "Commission": 5,
            "Status": "פתוח",
            "Cost_ILS": 43650,
            "Current_Value_ILS": 52200,
        },
        {
            "Current_Location": "Custody Vault",
            "Platform": "Quant Desk",
            "Type": "שוק ההון",
            "Ticker": "MSFT",
            "Purchase_Date": "2025-06-19",
            "Quantity": 34,
            "Origin_Buy_Price": 418,
            "Cost_Origin": 14212,
            "Origin_Currency": "USD",
            "Commission": 4,
            "Status": "פתוח",
            "Cost_ILS": 52420,
            "Current_Value_ILS": 61230,
        },
        {
            "Current_Location": "Custody Vault",
            "Platform": "Quant Desk",
            "Type": "שוק ההון",
            "Ticker": "NVDA",
            "Purchase_Date": "2025-07-11",
            "Quantity": 60,
            "Origin_Buy_Price": 121,
            "Cost_Origin": 7260,
            "Origin_Currency": "USD",
            "Commission": 3,
            "Status": "פתוח",
            "Cost_ILS": 26730,
            "Current_Value_ILS": 40920,
        },
        {
            "Current_Location": "Core Wallet",
            "Platform": "Digital Alpha",
            "Type": "קריפטו",
            "Ticker": "BTC",
            "Purchase_Date": "2025-08-23",
            "Quantity": 0.42,
            "Origin_Buy_Price": 57500,
            "Cost_Origin": 24150,
            "Origin_Currency": "USD",
            "Commission": 18,
            "Status": "פתוח",
            "Cost_ILS": 89120,
            "Current_Value_ILS": 118740,
        },
        {
            "Current_Location": "Core Wallet",
            "Platform": "Digital Alpha",
            "Type": "קריפטו",
            "Ticker": "ETH",
            "Purchase_Date": "2025-09-30",
            "Quantity": 7.5,
            "Origin_Buy_Price": 2520,
            "Cost_Origin": 18900,
            "Origin_Currency": "USD",
            "Commission": 15,
            "Status": "פתוח",
            "Cost_ILS": 69780,
            "Current_Value_ILS": 81840,
        },
        {
            "Current_Location": "Trading Wallet",
            "Platform": "Digital Alpha",
            "Type": "קריפטו",
            "Ticker": "SOL",
            "Purchase_Date": "2025-11-06",
            "Quantity": 220,
            "Origin_Buy_Price": 132,
            "Cost_Origin": 29040,
            "Origin_Currency": "USD",
            "Commission": 20,
            "Status": "פתוח",
            "Cost_ILS": 106920,
            "Current_Value_ILS": 135500,
        },
        {
            "Current_Location": "",
            "Platform": "Global Prime",
            "Type": "ETF",
            "Ticker": "IEF",
            "Purchase_Date": "2025-04-28",
            "Quantity": 180,
            "Origin_Buy_Price": 93,
            "Cost_Origin": 16740,
            "Origin_Currency": "USD",
            "Commission": 6,
            "Status": "פתוח",
            "Cost_ILS": 61890,
            "Current_Value_ILS": 63570,
        },
        {
            "Current_Location": "",
            "Platform": "Global Prime",
            "Type": "ETF",
            "Ticker": "VNQ",
            "Purchase_Date": "2025-10-02",
            "Quantity": 96,
            "Origin_Buy_Price": 85,
            "Cost_Origin": 8160,
            "Origin_Currency": "USD",
            "Commission": 4,
            "Status": "פתוח",
            "Cost_ILS": 30140,
            "Current_Value_ILS": 32620,
        },
        {
            "Current_Location": "",
            "Platform": "Quant Desk",
            "Type": "שוק ההון",
            "Ticker": "ASML",
            "Purchase_Date": "2025-12-09",
            "Quantity": 12,
            "Origin_Buy_Price": 720,
            "Cost_Origin": 8640,
            "Origin_Currency": "USD",
            "Commission": 3,
            "Status": "פתוח",
            "Cost_ILS": 31890,
            "Current_Value_ILS": 37420,
        },
        {
            "Current_Location": "",
            "Platform": "Quant Desk",
            "Type": "שוק ההון",
            "Ticker": "MELI",
            "Purchase_Date": "2026-01-15",
            "Quantity": 5,
            "Origin_Buy_Price": 1580,
            "Cost_Origin": 7900,
            "Origin_Currency": "USD",
            "Commission": 3,
            "Status": "פתוח",
            "Cost_ILS": 29160,
            "Current_Value_ILS": 33800,
        },
        {
            "Current_Location": "",
            "Platform": "Global Prime",
            "Type": "ETF",
            "Ticker": "XLV",
            "Purchase_Date": "2025-07-23",
            "Quantity": 70,
            "Origin_Buy_Price": 148,
            "Cost_Origin": 10360,
            "Origin_Currency": "USD",
            "Commission": 4,
            "Status": "סגור",
            "Cost_ILS": 38220,
            "Current_Value_ILS": 39610,
        },
        {
            "Current_Location": "",
            "Platform": "Quant Desk",
            "Type": "שוק ההון",
            "Ticker": "TSM",
            "Purchase_Date": "2025-09-12",
            "Quantity": 50,
            "Origin_Buy_Price": 168,
            "Cost_Origin": 8400,
            "Origin_Currency": "USD",
            "Commission": 3,
            "Status": "סגור",
            "Cost_ILS": 30970,
            "Current_Value_ILS": 34780,
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


@st.cache_data(ttl=120)
def prepare_core_views(df: pd.DataFrame) -> Dict[str, object]:
    trades = df[(df["Record_Source"] == "STATE_SNAPSHOT") & (df["Event_Type"] == "TRADE")].copy() if "Record_Source" in df.columns else df.copy()
    if "Status" not in trades.columns:
        trades["Status"] = ""
    trades["Status"] = trades["Status"].replace("", "פתוח")

    open_trades = trades[trades["Status"] != "סגור"].copy()
    closed_trades = trades[trades["Status"] == "סגור"].copy()

    total_cost = float(open_trades["Cost_ILS"].sum()) if "Cost_ILS" in open_trades.columns else 0.0
    total_value = float(open_trades["Current_Value_ILS"].sum()) if "Current_Value_ILS" in open_trades.columns else 0.0
    total_profit = total_value - total_cost
    total_return = (total_profit / total_cost) if total_cost else 0.0

    status_counts = trades.groupby("Status").size().reset_index(name="count") if "Status" in trades.columns else pd.DataFrame(columns=["Status", "count"])

    return {
        "trades": trades,
        "open_trades": open_trades,
        "closed_trades": closed_trades,
        "total_cost": total_cost,
        "total_value": total_value,
        "total_profit": total_profit,
        "total_return": total_return,
        "status_counts": status_counts,
    }


def enrich_open_trades_with_prices(open_trades: pd.DataFrame) -> pd.DataFrame:
    out = open_trades.copy()
    tickers = tuple(sorted(t for t in out["Ticker"].dropna().unique() if _clean(t))) if "Ticker" in out.columns else tuple()
    live_prices = fetch_prices(tickers)
    out["מחיר שוק"] = out["Ticker"].map(live_prices).fillna(0.0) if "Ticker" in out.columns else 0.0
    qty_series = out["Quantity"].map(_num) if "Quantity" in out.columns else 0.0
    out["שווי שוק (יחסי מטבע מקור)"] = qty_series * out["מחיר שוק"]
    return out


def dataframe_completeness(df: pd.DataFrame) -> Tuple[int, int, float]:
    if df.empty:
        return 0, 0, 0.0

    total_cells = int(df.shape[0] * df.shape[1])
    non_empty = 0
    for col in df.columns:
        series = df[col]
        if series.dtype == object:
            cleaned = series.map(_clean)
            non_empty += int(cleaned.ne("").sum())
        else:
            non_empty += int(series.notna().sum())

    completeness = (non_empty / total_cells) if total_cells else 0.0
    return non_empty, total_cells, completeness


def main() -> None:
    st.set_page_config(page_title="מערכת ניהול תיק", page_icon="📈", layout="wide", initial_sidebar_state="collapsed")
    template = "plotly_white"

    settings = load_local_settings()
    language_default = DEFAULT_LANGUAGE

    inject_global_styles(language_default)

    st.markdown("### Portfolio Manager OS")

    st.sidebar.markdown("### App")
    language = st.sidebar.selectbox("Language / שפה", [LANG_EN, LANG_HE], index=0 if language_default == LANG_EN else 1)
    demo_mode = st.sidebar.checkbox("Demo view / מצב הדגמה", value=bool(settings.get("demo_mode", False)))

    inject_global_styles(language)
    inject_client_fixes()

    tr = (lambda en, he: he if language == LANG_HE else en)

    st.sidebar.title(tr("Navigation", "ניווט"))
    page_dashboard = tr("Dashboard", "דשבורד")
    page_manage = tr("Trade Management", "ניהול עסקאות")
    page_risk = tr("Risk & FIFO", "סיכונים ו-FIFO")
    page_quality = tr("Data Quality", "בקרת נתונים")
    page = st.sidebar.radio(tr("Page", "עמוד"), [page_dashboard, page_manage, page_risk, page_quality])

    connection_state_box = None
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

        connection_state_box = st.empty()

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

    if connection_state_box is not None:
        if source_mode == "gspread":
            connection_state_box.warning(tr("gspread read mode active (write actions disabled).", "מצב קריאה דרך gspread פעיל (ללא Web App פעיל, פעולות עריכה/מחיקה מושבתות)."))
        elif source_mode == "demo":
            connection_state_box.info(tr("Demo mode active - sample data only.", "מצב הדגמה פעיל - נתוני דוגמה בלבד."))
        else:
            connection_state_box.success(tr("Apps Script mode active (read + write).", "חיבור דרך Apps Script פעיל (קריאה + כתיבה)."))

    if df.empty:
        st.warning(tr("No rows found in Google Sheet snapshot", "לא נמצאו עסקאות ב'תמונת מצב' בגוגל שיט"))
        st.stop()

    core = prepare_core_views(df)
    trades = core["trades"]
    open_trades = core["open_trades"].copy()
    closed_trades = core["closed_trades"].copy()
    total_cost = float(core["total_cost"])
    total_value = float(core["total_value"])
    total_profit = float(core["total_profit"])
    total_return = float(core["total_return"])
    is_demo = source_mode == "demo"

    if page == page_dashboard:
        st.title(tr("Portfolio Manager OS - Advanced Dashboard", "Portfolio Manager OS - דשבורד מתקדם"))
        if is_demo:
            st.markdown(
                """
                <div class='modern-card' style='margin-bottom:0.6rem;'>
                  <div style='font-size:1.05rem;font-weight:700;'>Demo Showcase Portfolio</div>
                  <div style='opacity:0.82;margin-top:0.2rem;'>Institutional-style mix across US equities, global ETFs, defensive assets and digital holdings.</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        st.caption(
            f"{len(trades):,} {tr('rows loaded', 'רשומות נטענו')} | "
            f"{len(trades[trades['Record_Source'] == 'STATE_SNAPSHOT']):,} {tr('snapshot rows', 'שורות תמונת מצב')} | "
            f"{len(trades[trades['Status'] == 'סגור']):,} {tr('closed', 'סגורות')}"
        )

        fx = _safe_quote("USDILS=X")
        if fx <= 0:
            fx = 3.6

        dashboard_df = enrich_open_trades_with_prices(open_trades)
        dashboard_df["Cost_Origin_With_Fee"] = dashboard_df["Cost_Origin"] + dashboard_df["Commission"]
        dashboard_df["Value_Origin_Est"] = np.where(
            dashboard_df["Origin_Currency"].str.upper() == "USD",
            dashboard_df["Current_Value_ILS"] / fx,
            dashboard_df["Current_Value_ILS"],
        )

        summary = dashboard_df.groupby("Ticker", as_index=False).agg(
            Open_Qty=("Quantity", "sum"),
            Cost_ILS=("Cost_ILS", "sum"),
            Value_ILS=("Current_Value_ILS", "sum"),
            Cost_Origin=("Cost_Origin_With_Fee", "sum"),
            Value_Origin=("Value_Origin_Est", "sum"),
        )
        summary["Net_PnL_ILS"] = summary["Value_ILS"] - summary["Cost_ILS"]
        summary["Yield_Origin"] = np.where(summary["Cost_Origin"] > 0, (summary["Value_Origin"] - summary["Cost_Origin"]) / summary["Cost_Origin"], 0.0)
        summary["Yield_ILS"] = np.where(summary["Cost_ILS"] > 0, summary["Net_PnL_ILS"] / summary["Cost_ILS"], 0.0)

        total_value_txt = f"{total_value:,.0f}"
        total_cost_txt = f"{total_cost:,.0f}"
        total_return_txt = f"{total_return:.2%}"
        realized_closed = closed_trades["Current_Value_ILS"].sum() - closed_trades["Cost_ILS"].sum()
        realized_closed_txt = f"{realized_closed:,.0f}"
        render_modern_metrics(
            [
                (tr("Total Value (ILS)", "שווי כולל (₪)"), total_value_txt, f"{total_profit:,.0f} ₪"),
                (tr("Total Cost (ILS)", "עלות כוללת (₪)"), total_cost_txt, ""),
                (tr("Total Return", "תשואה כוללת"), total_return_txt, ""),
                (tr("Realized P/L (Closed)", "רווח ממומש (סגור)"), realized_closed_txt, ""),
            ]
        )

        if is_demo and not open_trades.empty:
            demo_cols = st.columns(2)
            with demo_cols[0]:
                type_mix = open_trades.groupby("Type", as_index=False)["Current_Value_ILS"].sum().sort_values("Current_Value_ILS", ascending=False)
                fig_type_mix = px.treemap(
                    type_mix,
                    path=["Type"],
                    values="Current_Value_ILS",
                    title=tr("Asset Class Composition", "הרכב מחלקות נכסים"),
                    template=template,
                )
                st.plotly_chart(fig_type_mix, use_container_width=True)
            with demo_cols[1]:
                perf_track = open_trades.groupby("Purchase_Date", as_index=False)[["Cost_ILS", "Current_Value_ILS"]].sum().sort_values("Purchase_Date")
                perf_track["Cum_Cost_ILS"] = perf_track["Cost_ILS"].cumsum()
                perf_track["Cum_Value_ILS"] = perf_track["Current_Value_ILS"].cumsum()
                fig_track = go.Figure()
                fig_track.add_trace(go.Scatter(x=perf_track["Purchase_Date"], y=perf_track["Cum_Cost_ILS"], mode="lines+markers", name=tr("Cumulative Cost", "עלות מצטברת")))
                fig_track.add_trace(go.Scatter(x=perf_track["Purchase_Date"], y=perf_track["Cum_Value_ILS"], mode="lines+markers", name=tr("Cumulative Value", "שווי מצטבר")))
                fig_track.update_layout(template=template, title=tr("Portfolio Build-Up", "התפתחות בניית התיק"), xaxis_title=tr("Date", "תאריך"), yaxis_title="ILS")
                st.plotly_chart(fig_track, use_container_width=True)

        tab_overview, tab_allocation, tab_reports, tab_deposits, tab_transactions = st.tabs(
            [
                tr("Overview", "סקירה"),
                tr("Allocation", "חלוקה"),
                tr("Reports", "דוחות"),
                tr("Total Deposits", "סך הפקדות"),
                tr("Transactions", "עסקאות"),
            ]
        )

        with tab_overview:
            col_a, col_b = st.columns(2)
            with col_a:
                fig_pie = px.pie(
                    summary,
                    names="Ticker",
                    values="Value_ILS",
                    title=tr("Portfolio Allocation by Asset", "חלוקת תיק לפי נכס"),
                    hole=0.52,
                    template=template,
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            with col_b:
                fig_bar = px.bar(
                    summary.sort_values("Net_PnL_ILS", ascending=False),
                    x="Ticker",
                    y="Net_PnL_ILS",
                    color="Net_PnL_ILS",
                    title=tr("Net P/L by Asset", "רווח/הפסד לפי נכס"),
                    template=template,
                )
                st.plotly_chart(fig_bar, use_container_width=True)

            st.markdown(f"#### {tr('Exposure Table', 'טבלת חשיפה')}")
            exposure_cols = ["Ticker", "Open_Qty", "Cost_ILS", "Value_ILS", "Net_PnL_ILS", "Yield_Origin", "Yield_ILS"]
            exposure_view = localize_dataframe_columns(summary[exposure_cols], language)
            st.dataframe(
                exposure_view.style.format(
                    {
                        localize_column_name("Open_Qty", language): "{:.8f}",
                        localize_column_name("Cost_ILS", language): "{:,.0f}",
                        localize_column_name("Value_ILS", language): "{:,.0f}",
                        localize_column_name("Net_PnL_ILS", language): "{:,.0f}",
                        localize_column_name("Yield_Origin", language): "{:.2%}",
                        localize_column_name("Yield_ILS", language): "{:.2%}",
                    }
                ),
                use_container_width=True,
            )

        reports = build_home_inspired_reports(open_trades)
        with tab_allocation:
            m1, m2, m3 = st.columns(3)
            m1.metric(tr("Crypto Share of Portfolio", "אחוז קריפטו מהתיק"), f"{reports['crypto_share']:.2%}")
            m2.metric(tr("BTC Share of Portfolio", "אחוז ביטקוין מהתיק"), f"{reports['btc_share_of_portfolio']:.2%}")
            m3.metric(tr("BTC Share of Crypto", "אחוז ביטקוין מסך הקריפטו"), f"{reports['btc_share_of_crypto']:.2%}")

            concentration_view = localize_dataframe_columns(reports["concentration_table"], language)
            st.dataframe(
                concentration_view.style.format(
                    {
                        localize_column_name("Direct_Qty", language): "{:.8f}",
                        localize_column_name("Direct_ILS", language): "{:,.0f}",
                        localize_column_name("ETF_Qty", language): "{:.8f}",
                        localize_column_name("ETF_ILS", language): "{:,.0f}",
                        localize_column_name("Total_Exposure_ILS", language): "{:,.0f}",
                    }
                ),
                use_container_width=True,
            )

        with tab_reports:
            report_options = {
                tr("Crypto Allocation", "חלוקת קריפטו בתיק"): "crypto",
                tr("Asset Concentration", "ריכוז נכסים"): "concentration",
                tr("Winner / Loser", "המנצח והמפסיד"): "winner",
                tr("Net Investment", "סך השקעה נטו"): "net",
                tr("Live FX / Crypto Rates", "שערי מטבע חיים"): "rates",
            }
            report_label = st.selectbox(tr("Choose report", "בחר דוח"), list(report_options.keys()))
            report_key = report_options[report_label]

            if report_key == "crypto":
                ratio_df = pd.DataFrame(
                    [
                        {"Category": tr("Crypto from Portfolio", "קריפטו מכלל התיק"), "Yield": reports["crypto_share"]},
                        {"Category": tr("Bitcoin from Portfolio", "ביטקוין מכלל התיק"), "Yield": reports["btc_share_of_portfolio"]},
                        {"Category": tr("Bitcoin from Crypto", "ביטקוין מסך הקריפטו"), "Yield": reports["btc_share_of_crypto"]},
                    ]
                )
                fig_ratio = px.bar(ratio_df, x="Category", y="Yield", title=report_label, template=template)
                fig_ratio.update_yaxes(tickformat=".0%")
                st.plotly_chart(fig_ratio, use_container_width=True)
                st.dataframe(ratio_df.style.format({"Yield": "{:.2%}"}), use_container_width=True)

            elif report_key == "concentration":
                concentration_view = localize_dataframe_columns(reports["concentration_table"], language)
                st.dataframe(concentration_view, use_container_width=True)

            elif report_key == "winner":
                wl = reports["winner_loser_table"].copy()
                wl["Category"] = wl["Category"].replace({"Winner": tr("Winner", "המנצח"), "Loser": tr("Loser", "המפסיד")})
                wl_view = localize_dataframe_columns(wl, language)
                st.dataframe(wl_view.style.format({localize_column_name("Yield", language): "{:.2%}"}), use_container_width=True)

            elif report_key == "net":
                net_view = localize_dataframe_columns(reports["net_investment_table"], language)
                st.dataframe(
                    net_view.style.format(
                        {
                            localize_column_name("Net_Investment_ILS", language): "{:,.0f}",
                            localize_column_name("Current_Value_ILS", language): "{:,.0f}",
                            localize_column_name("PnL_ILS", language): "{:,.0f}",
                        }
                    ),
                    use_container_width=True,
                )

            else:
                rates = reports["live_rates"]
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("USD/ILS", f"{rates['USD/ILS']:.4f}")
                c2.metric("BTC/USD", f"{rates['BTC/USD']:.2f}")
                c3.metric("ETH/USD", f"{rates['ETH/USD']:.2f}")
                c4.metric("SOL/USD", f"{rates['SOL/USD']:.2f}")

            st.markdown(f"#### {tr('Unified Report Table', 'טבלת דוח מאוחדת')}")
            full_report_view = localize_dataframe_columns(summary[["Ticker", "Open_Qty", "Cost_ILS", "Value_ILS", "Net_PnL_ILS", "Yield_Origin", "Yield_ILS"]], language)
            st.dataframe(
                full_report_view.style.format(
                    {
                        localize_column_name("Open_Qty", language): "{:.8f}",
                        localize_column_name("Cost_ILS", language): "{:,.0f}",
                        localize_column_name("Value_ILS", language): "{:,.0f}",
                        localize_column_name("Net_PnL_ILS", language): "{:,.0f}",
                        localize_column_name("Yield_Origin", language): "{:.2%}",
                        localize_column_name("Yield_ILS", language): "{:.2%}",
                    }
                ),
                use_container_width=True,
            )

        with tab_deposits:
            st.caption(tr("Manual deposits table by platform. This table is informational only and does not affect other calculations.", "טבלת הפקדות ידנית לפי פלטפורמה. הטבלה אינפורמטיבית בלבד ואינה משפיעה על חישובים אחרים."))

            all_platforms = sorted([p for p in trades.get("Platform", pd.Series(dtype=str)).dropna().astype(str).map(_clean).unique() if p])
            if not all_platforms:
                all_platforms = ["Bit2C", "הורייזון", "אקסלנס"]

            deposits_mode = "demo" if is_demo else "live"
            deposits_key = "manual_deposits_table_demo" if is_demo else "manual_deposits_table_live"
            remote_sync_enabled = source_mode == "apps_script" and _clean(web_url_clean) != ""
            if is_demo:
                demo_vals = [120000.0, 85000.0, 43000.0, 26000.0, 14000.0]
                seeded = [demo_vals[i % len(demo_vals)] for i in range(len(all_platforms))]
                base_df = pd.DataFrame({
                    "Platform": all_platforms,
                    "Manual_Deposit_ILS": seeded,
                })
            else:
                base_df = pd.DataFrame({
                    "Platform": all_platforms,
                    "Manual_Deposit_ILS": [0.0] * len(all_platforms),
                })

            if deposits_key not in st.session_state:
                loaded = False
                if remote_sync_enabled:
                    ok_remote, remote_rows, _ = load_manual_deposits_remote(web_url_clean, api_token, deposits_mode)
                    if ok_remote:
                        st.session_state[deposits_key] = pd.DataFrame(remote_rows) if remote_rows else base_df
                        loaded = True

                if not loaded:
                    store = load_manual_deposits_store()
                    persisted = store.get(deposits_mode, [])
                    if persisted:
                        persisted_df = pd.DataFrame(persisted)
                        st.session_state[deposits_key] = persisted_df
                    else:
                        st.session_state[deposits_key] = base_df
            else:
                existing = st.session_state[deposits_key].copy()
                for col, default in {"Platform": "", "Manual_Deposit_ILS": 0.0}.items():
                    if col not in existing.columns:
                        existing[col] = default
                existing["Platform"] = existing["Platform"].map(_clean)
                existing["Manual_Deposit_ILS"] = existing["Manual_Deposit_ILS"].map(_num)
                known = set(existing["Platform"].tolist())
                missing = [p for p in all_platforms if p not in known]
                if missing:
                    existing = pd.concat(
                        [existing, pd.DataFrame({"Platform": missing, "Manual_Deposit_ILS": [0.0] * len(missing)})],
                        ignore_index=True,
                    )
                st.session_state[deposits_key] = existing

            edited_deposits = st.data_editor(
                st.session_state[deposits_key],
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                column_config={
                    "Platform": st.column_config.TextColumn(tr("Platform", "פלטפורמה"), required=True),
                    "Manual_Deposit_ILS": st.column_config.NumberColumn(tr("Manual Deposit (ILS)", "הפקדה ידנית (₪)"), format="%0.2f"),
                },
                key="manual_deposits_editor_demo" if is_demo else "manual_deposits_editor_live",
            )

            edited_deposits = edited_deposits.copy()
            edited_deposits["Platform"] = edited_deposits["Platform"].map(_clean)
            edited_deposits = edited_deposits[edited_deposits["Platform"] != ""]
            edited_deposits["Manual_Deposit_ILS"] = edited_deposits["Manual_Deposit_ILS"].map(_num)
            st.session_state[deposits_key] = edited_deposits

            # Persist deposits across app restarts, separated by live/demo mode.
            store = load_manual_deposits_store()
            store[deposits_mode] = edited_deposits.to_dict("records")
            save_manual_deposits_store(store)

            # Sync across devices when Apps Script mode is available.
            if remote_sync_enabled:
                ok_remote_save, remote_err = save_manual_deposits_remote(
                    web_url_clean,
                    api_token,
                    deposits_mode,
                    edited_deposits.to_dict("records"),
                )
                if not ok_remote_save:
                    st.warning(tr("Could not sync deposits to cloud, kept locally on this device.", "לא ניתן לסנכרן הפקדות לענן, נשמר מקומית במכשיר זה."))

            total_manual_deposits = float(edited_deposits["Manual_Deposit_ILS"].sum()) if not edited_deposits.empty else 0.0
            st.metric(tr("Total Manual Deposits (ILS)", "סך הפקדות ידני (₪)"), f"{total_manual_deposits:,.0f}")

        with tab_transactions:
            with st.expander(tr("Full snapshot transactions (including closed)", "רשימת העסקאות המלאה בתמונת מצב כולל סגורות"), expanded=True):
                snapshot_view = trades[trades["Record_Source"] == "STATE_SNAPSHOT"].copy() if "Record_Source" in trades.columns else trades.copy()
                ticker_options = sorted([t for t in snapshot_view["Ticker"].dropna().astype(str).unique() if _clean(t)]) if "Ticker" in snapshot_view.columns else []
                if hasattr(st, "pills"):
                    selected_tickers = st.pills(
                        tr("Filter by ticker", "סינון לפי טיקר"),
                        ticker_options,
                        selection_mode="multi",
                        default=[],
                        key="snapshot_ticker_pills",
                    )
                    selected_tickers = list(selected_tickers) if selected_tickers else []
                else:
                    selected_tickers = st.multiselect(
                        tr("Filter by ticker", "סינון לפי טיקר"),
                        ticker_options,
                        default=[],
                    )
                if selected_tickers and "Ticker" in snapshot_view.columns:
                    snapshot_view = snapshot_view[snapshot_view["Ticker"].isin(selected_tickers)]

                snapshot_view = snapshot_view.copy()
                fx_local = _safe_quote("USDILS=X")
                if fx_local <= 0:
                    fx_local = 3.6
                snapshot_view["Cost_Origin"] = snapshot_view.get("Cost_Origin", 0).map(_num)
                snapshot_view["Commission"] = snapshot_view.get("Commission", 0).map(_num)
                snapshot_view["Cost_ILS"] = snapshot_view.get("Cost_ILS", 0).map(_num)
                snapshot_view["Current_Value_ILS"] = snapshot_view.get("Current_Value_ILS", 0).map(_num)
                snapshot_view["Origin_Currency"] = snapshot_view.get("Origin_Currency", "").map(_clean)
                snapshot_view["Status"] = snapshot_view.get("Status", "").map(_clean)
                snapshot_view["Cost_Origin_With_Fee"] = snapshot_view["Cost_Origin"] + snapshot_view["Commission"]
                snapshot_view["Value_Origin_Est"] = np.where(
                    snapshot_view["Origin_Currency"].str.upper() == "USD",
                    snapshot_view["Current_Value_ILS"] / fx_local,
                    snapshot_view["Current_Value_ILS"],
                )
                if "Purchase_Date" in snapshot_view.columns:
                    parsed_dates = _parse_dates_flexible(snapshot_view["Purchase_Date"])
                    snapshot_view["Purchase_Date"] = parsed_dates.dt.strftime("%Y-%m-%d").fillna(tr("Missing date", "תאריך חסר"))
                snapshot_view["Yield_Origin"] = np.where(
                    (snapshot_view["Status"] != "סגור") & (snapshot_view["Cost_Origin_With_Fee"] > 0),
                    (snapshot_view["Value_Origin_Est"] - snapshot_view["Cost_Origin_With_Fee"]) / snapshot_view["Cost_Origin_With_Fee"],
                    0.0,
                )

                show_cols = [
                    c
                    for c in [
                        "Purchase_Date",
                        "Ticker",
                        "Platform",
                        "Quantity",
                        "Origin_Buy_Price",
                        "Cost_ILS",
                        "Current_Value_ILS",
                        "Yield_Origin",
                        "Status",
                    ]
                    if c in snapshot_view.columns
                ]
                if show_cols:
                    snapshot_sort_cols = [c for c in ["Purchase_Date", "Ticker"] if c in snapshot_view.columns]
                    if snapshot_sort_cols:
                        snapshot_asc = [False if c == "Purchase_Date" else True for c in snapshot_sort_cols]
                        snapshot_view = snapshot_view.sort_values(snapshot_sort_cols, ascending=snapshot_asc)
                    localized_tx = localize_snapshot_view(snapshot_view[show_cols], language)
                    date_col = SNAPSHOT_HEADERS["Purchase_Date"][language]
                    ticker_col = SNAPSHOT_HEADERS["Ticker"][language]
                    platform_col = SNAPSHOT_HEADERS["Platform"][language]
                    qty_col = SNAPSHOT_HEADERS["Quantity"][language]
                    buy_col = SNAPSHOT_HEADERS["Origin_Buy_Price"][language]
                    cost_col = SNAPSHOT_HEADERS["Cost_ILS"][language]
                    value_col = SNAPSHOT_HEADERS["Current_Value_ILS"][language]
                    yield_col = SNAPSHOT_HEADERS["Yield_Origin"][language]
                    status_col = SNAPSHOT_HEADERS["Status"][language]
                    st.dataframe(
                        localized_tx.style.format(
                            {
                                qty_col: "{:.8f}",
                                buy_col: "{:,.2f}",
                                cost_col: "{:,.0f}",
                                value_col: "{:,.0f}",
                                yield_col: "{:.2%}",
                            }
                        ),
                        column_config={
                            date_col: st.column_config.TextColumn(width="small"),
                            ticker_col: st.column_config.TextColumn(width="small"),
                            platform_col: st.column_config.TextColumn(width="medium"),
                            qty_col: st.column_config.NumberColumn(width="small"),
                            buy_col: st.column_config.NumberColumn(width="small"),
                            cost_col: st.column_config.NumberColumn(width="small"),
                            value_col: st.column_config.NumberColumn(width="small"),
                            yield_col: st.column_config.NumberColumn(width="small"),
                            status_col: st.column_config.TextColumn(width="small"),
                        },
                        use_container_width=True,
                        height=420,
                        hide_index=True,
                    )


    elif page == page_risk:
        st.title(tr("Risk, Performance and FIFO", "סיכונים, ביצועים ועלות FIFO"))
        fifo_df = fifo_metrics(trades)
        st.subheader(tr("FIFO Engine", "מנוע FIFO"))
        if fifo_df.empty:
            st.info(tr("Not enough data for FIFO", "אין מספיק נתונים לחישוב FIFO"))
        else:
            fifo_view = fifo_df.rename(
                columns={
                    "Ticker": tr("Ticker", "טיקר"),
                    "כמות פתוחה (FIFO)": tr("Open Qty (FIFO)", "כמות פתוחה (FIFO)"),
                    "עלות פתוחה (₪)": tr("Open Cost (ILS)", "עלות פתוחה (₪)"),
                    "רווח ממומש (₪)": tr("Realized P/L (ILS)", "רווח ממומש (₪)"),
                    "מחיר ממוצע פתוח (₪)": tr("Avg Open Price (ILS)", "מחיר ממוצע פתוח (₪)"),
                }
            )
            st.dataframe(
                fifo_view.style.format(
                    {
                        tr("Open Qty (FIFO)", "כמות פתוחה (FIFO)"): "{:.8f}",
                        tr("Open Cost (ILS)", "עלות פתוחה (₪)"): "₪{:,.0f}",
                        tr("Realized P/L (ILS)", "רווח ממומש (₪)"): "₪{:,.0f}",
                        tr("Avg Open Price (ILS)", "מחיר ממוצע פתוח (₪)"): "₪{:,.2f}",
                    }
                )
            )

        holdings = open_trades.groupby("Ticker", as_index=False)["Quantity"].sum()
        value_series = portfolio_price_history(tuple(holdings["Ticker"]), tuple(holdings["Quantity"]), days=365)
        metrics = risk_metrics(value_series)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Sharpe", f"{metrics['sharpe']:.2f}")
        c2.metric(tr("Annual Volatility", "תנודתיות שנתית"), f"{metrics['vol']:.2%}")
        c3.metric(tr("Max Drawdown", "משיכה מקסימלית"), f"{metrics['mdd']:.2%}")
        c4.metric("CAGR", f"{metrics['cagr']:.2%}")

        if not value_series.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=value_series.index, y=value_series.values, mode="lines", name=tr("Estimated Historical Value", "שווי היסטורי משוער")))
            fig.update_layout(
                template=template,
                title=tr("Historical Value Series (Market-Based Estimate)", "סדרת שווי היסטורית (הערכה לפי מחירי שוק)"),
                xaxis_title=tr("Date", "תאריך"),
                yaxis_title=tr("Value", "שווי"),
            )
            st.plotly_chart(fig, use_container_width=True)

        if is_demo and total_value > 0:
            st.markdown(f"#### {tr('Scenario Lab (Demo)', 'מעבדת תרחישים (דמו)')}")
            scenario_df = pd.DataFrame(
                [
                    {"Scenario": tr("Calm market", "שוק רגוע"), "Shock": -0.03},
                    {"Scenario": tr("Risk-off week", "שבוע Risk-off"), "Shock": -0.08},
                    {"Scenario": tr("Macro stress", "סטרס מאקרו"), "Shock": -0.15},
                    {"Scenario": tr("Tail event", "אירוע קיצון"), "Shock": -0.25},
                ]
            )
            scenario_df["Estimated Value (ILS)"] = total_value * (1 + scenario_df["Shock"])
            scenario_df["Estimated P/L (ILS)"] = scenario_df["Estimated Value (ILS)"] - total_cost
            st.dataframe(
                scenario_df.style.format(
                    {
                        "Shock": "{:.1%}",
                        "Estimated Value (ILS)": "{:,.0f}",
                        "Estimated P/L (ILS)": "{:,.0f}",
                    }
                ),
                use_container_width=True,
            )

    elif page == page_manage:
        st.title(tr("Trade Management (Add / Edit / Delete)", "ניהול עסקאות (הוספה / עריכה / מחיקה)"))
        st.caption(tr("Changes are saved directly to Google Sheets via Apps Script (no CSV write).", "שמירה מתבצעת ישירות ל-Google Sheets דרך Apps Script (ללא כתיבה ל-CSV)."))
        if is_demo:
            st.info(tr("Demo trade-desk mode: you can safely explore flows and forms without touching your personal portfolio.", "מצב דמו לחדר מסחר: אפשר לבדוק זרימות וטפסים בבטחה בלי לגעת בתיק האישי שלך."))
        write_enabled = source_mode == "apps_script"
        if not write_enabled:
            st.warning(tr("This page is read-only in current mode. Connect Apps Script Web App to enable write actions.", "הדף במצב קריאה בלבד כי אין Web App URL תקין. כדי לאפשר הוספה/עריכה/מחיקה, חבר Apps Script Web App."))

        st.caption(tr(f"Showing {len(trades):,} snapshot transactions, including closed trades.", f"מציג {len(trades):,} עסקאות תמונת מצב, כולל עסקאות סגורות."))
        trade_view = trades.copy()
        status_filter = st.multiselect(tr("Status filter", "סינון סטטוס"), sorted([s for s in trade_view["Status"].dropna().astype(str).unique() if s]), default=[])
        if status_filter:
            trade_view = trade_view[trade_view["Status"].isin(status_filter)]
        preview_cols = [c for c in ["Trade_ID", "Purchase_Date", "Platform", "Type", "Ticker", "Quantity", "Cost_Origin", "Cost_ILS", "Status", "validation_status"] if c in trade_view.columns]
        if preview_cols:
            sort_cols = [c for c in ["Purchase_Date", "Ticker"] if c in trade_view.columns]
            if sort_cols:
                asc = [False if c == "Purchase_Date" else True for c in sort_cols]
                trade_view = trade_view.sort_values(sort_cols, ascending=asc)

            manage_select_df = trade_view[preview_cols].copy()
            selected_trade_id = st.session_state.get("selected_trade_id", "")
            manage_select_df.insert(0, "__select__", manage_select_df["Trade_ID"].astype(str) == str(selected_trade_id))
            manage_select_df = manage_select_df.rename(columns={"__select__": tr("Select", "בחר")})

            edited_manage_df = st.data_editor(
                localize_snapshot_view(manage_select_df, language),
                use_container_width=True,
                hide_index=True,
                key="manage_table_selector",
            )

            select_col = tr("Select", "בחר")
            trade_id_col = SNAPSHOT_HEADERS["Trade_ID"][language]
            selected_rows = edited_manage_df[edited_manage_df[select_col] == True] if select_col in edited_manage_df.columns else pd.DataFrame()
            if not selected_rows.empty and trade_id_col in selected_rows.columns:
                st.session_state["selected_trade_id"] = _clean(selected_rows.iloc[0][trade_id_col])
            selected_trade_id = _clean(st.session_state.get("selected_trade_id", ""))

            if selected_trade_id:
                st.caption(f"{tr('Selected Trade_ID', 'Trade_ID נבחר')}: `{selected_trade_id}`")

        mode_label_map = {
            tr("Add", "הוספה"): "add",
            tr("Edit", "עריכה"): "edit",
            tr("Delete", "מחיקה"): "delete",
        }
        mode_label = st.radio(tr("Action", "פעולה"), list(mode_label_map.keys()), horizontal=True)
        mode = mode_label_map[mode_label]
        editable_cols = ["Platform", "Type", "Ticker", "Purchase_Date", "Quantity", "Origin_Buy_Price", "Cost_Origin", "Origin_Currency", "Commission", "Status", "Cost_ILS", "Current_Value_ILS", "Action", "Event_Type", "Trade_ID"]
        platforms = trades["Platform"].dropna().astype(str).tolist() if "Platform" in trades.columns else []
        types = trades["Type"].dropna().astype(str).tolist() if "Type" in trades.columns else []
        tickers = trades["Ticker"].dropna().astype(str).tolist() if "Ticker" in trades.columns else []
        currencies = trades["Origin_Currency"].dropna().astype(str).tolist() if "Origin_Currency" in trades.columns else []

        if mode == "add":
            with st.form("add_form"):
                new_row = {
                    "Platform": _select_or_type(tr("Platform", "פלטפורמה"), platforms, "Bit2C", "add_platform", tr),
                    "Type": _select_or_type(tr("Asset type", "סוג נכס"), types, "קריפטו", "add_type", tr),
                    "Ticker": _select_or_type(tr("Ticker", "טיקר"), tickers, "BTC", "add_ticker", tr).upper(),
                    "Purchase_Date": st.date_input(tr("Purchase date", "תאריך רכישה"), value=datetime.now()).strftime("%Y-%m-%d"),
                    "Quantity": st.number_input(tr("Quantity", "כמות"), value=0.0, format="%.8f"),
                    "Origin_Buy_Price": st.number_input(tr("Buy price", "שער קנייה"), value=0.0),
                    "Cost_Origin": st.number_input(tr("Origin cost", "עלות מקור"), value=0.0),
                    "Origin_Currency": _select_or_type(tr("Origin currency", "מטבע מקור"), currencies, "USD", "add_currency", tr).upper(),
                    "Commission": st.number_input(tr("Commission", "עמלה"), value=0.0),
                    "Status": st.selectbox(tr("Status", "סטטוס"), ["פתוח", "סגור"]),
                    "Cost_ILS": st.number_input(tr("Cost ILS", "עלות ILS"), value=0.0),
                    "Current_Value_ILS": st.number_input(tr("Value ILS", "שווי ILS"), value=0.0),
                    "Action": st.selectbox(tr("Accounting action", "פעולה חשבונאית"), ["BUY", "SELL"]),
                    "Event_Type": "TRADE",
                }
                new_row["Trade_ID"] = hashlib.sha1(json.dumps(new_row, ensure_ascii=False).encode("utf-8")).hexdigest()[:16]
                submitted = st.form_submit_button(tr("Save", "שמור"))

            if submitted:
                if not write_enabled:
                    st.error(tr("Cannot save in gspread mode. Configure a valid Web App URL on the left.", "לא ניתן לשמור במצב gspread. הגדר Web App URL תקין בצד שמאל."))
                else:
                    ok, msg = sync_trade_to_sheet(web_url_clean, api_token, "add", new_row)
                    if ok:
                        st.success(tr("Trade added directly to Google Sheets", "הרשומה נוספה ישירות ל-Google Sheets"))
                        st.info(msg)
                        load_google_snapshot_data.clear()
                        st.rerun()
                    else:
                        st.error(f"{tr('Add failed', 'הוספה נכשלה')}: {msg}")

        elif mode == "edit":
            selected = _clean(st.session_state.get("selected_trade_id", ""))
            if not selected:
                st.info(tr("Select a row in the table above to edit", "סמן שורה בטבלה למעלה כדי לערוך"))
                return
            row_idx = trades.index[trades["Trade_ID"].astype(str) == selected]
            if len(row_idx) == 0:
                st.warning(tr("Row not found", "לא נמצאה רשומה"))
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
                            edited[col] = _select_or_type(tr("Platform", "פלטפורמה"), platforms, _clean(val), f"edit_{selected}_platform", tr)
                        elif col == "Type":
                            edited[col] = _select_or_type(tr("Asset type", "סוג נכס"), types, _clean(val), f"edit_{selected}_type", tr)
                        elif col == "Ticker":
                            edited[col] = _select_or_type(tr("Ticker", "טיקר"), tickers, _clean(val), f"edit_{selected}_ticker", tr).upper()
                        elif col == "Origin_Currency":
                            edited[col] = _select_or_type(tr("Origin currency", "מטבע מקור"), currencies, _clean(val), f"edit_{selected}_currency", tr).upper()
                        elif col == "Status":
                            edited[col] = st.selectbox(col, ["פתוח", "סגור"], index=0 if _clean(val) != "סגור" else 1, key=f"e_{col}")
                        elif col == "Action":
                            edited[col] = st.selectbox(col, ["BUY", "SELL"], index=0 if _clean(val) != "SELL" else 1, key=f"e_{col}")
                        else:
                            edited[col] = st.text_input(col, value=_clean(val), key=f"e_{col}")
                    submitted = st.form_submit_button(tr("Update", "עדכן"))

                if submitted:
                    if not write_enabled:
                        st.error(tr("Cannot update in gspread mode. Configure a valid Web App URL on the left.", "לא ניתן לעדכן במצב gspread. הגדר Web App URL תקין בצד שמאל."))
                    else:
                        edited["Trade_ID"] = selected
                        ok, msg = sync_trade_to_sheet(web_url_clean, api_token, "edit", edited)
                        if ok:
                            st.success(tr("Trade updated directly in Google Sheets", "הרשומה עודכנה ישירות ב-Google Sheets"))
                            st.info(msg)
                            load_google_snapshot_data.clear()
                            st.rerun()
                        else:
                            st.error(f"{tr('Update failed', 'עדכון נכשל')}: {msg}")

        else:
            selected = _clean(st.session_state.get("selected_trade_id", ""))
            if not selected:
                st.info(tr("Select a row in the table above to delete", "סמן שורה בטבלה למעלה כדי למחוק"))
                return
            if st.button(tr("Delete trade", "מחק רשומה")):
                if not write_enabled:
                    st.error(tr("Cannot delete in gspread mode. Configure a valid Web App URL on the left.", "לא ניתן למחוק במצב gspread. הגדר Web App URL תקין בצד שמאל."))
                else:
                    delete_payload = {"Trade_ID": _clean(selected)}
                    ok, msg = sync_trade_to_sheet(web_url_clean, api_token, "delete", delete_payload)
                    if ok:
                        st.success(tr("Trade deleted directly from Google Sheets", "הרשומה נמחקה ישירות מ-Google Sheets"))
                        st.info(msg)
                        load_google_snapshot_data.clear()
                        st.rerun()
                    else:
                        st.error(f"{tr('Delete failed', 'מחיקה נכשלה')}: {msg}")

    else:
        st.title(tr("Data Quality", "בקרת נתונים ואיכות"))
        if is_demo:
            non_empty_cells, total_cells, completeness = dataframe_completeness(df)
            dupes = int(df.duplicated(subset=["Trade_ID"]).sum()) if "Trade_ID" in df.columns else 0
            cqa1, cqa2, cqa3 = st.columns(3)
            cqa1.metric(tr("Data Completeness", "שלמות נתונים"), f"{completeness:.1%}")
            cqa2.metric(tr("Duplicate Trade IDs", "כפילויות Trade ID"), f"{dupes}")
            cqa3.metric(tr("Rows in Snapshot", "שורות בתמונת מצב"), f"{len(df):,}")

        status_counts = core["status_counts"].copy()
        if status_counts.empty:
            st.info(tr("No status distribution available.", "אין נתוני סטטוס להצגה."))
            st.subheader(tr("Recent Data", "נתונים אחרונים"))
            st.dataframe(localize_snapshot_view(df.tail(30), language))
            return

        status_counts_view = status_counts.copy()
        status_counts_view["Status"] = status_counts_view["Status"].map(lambda v: VALUE_LABELS["Status"].get(_clean(v), {}).get(language, _clean(v)))
        status_counts_view = status_counts_view.rename(columns={"Status": SNAPSHOT_HEADERS["Status"][language], "count": tr("Count", "כמות")})
        st.dataframe(status_counts_view)
        fig = px.pie(status_counts, names="Status", values="count", title=tr("Trade Status Distribution", "פיזור סטטוסי עסקאות"), template=template)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader(tr("Recent Data", "נתונים אחרונים"))
        st.dataframe(localize_snapshot_view(df.tail(30), language))


if __name__ == "__main__":
    main()
