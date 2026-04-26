import hashlib
import json
import os
import re
import socket
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Tuple
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
import yfinance as yf
try:
    from streamlit_option_menu import option_menu
except Exception:
    option_menu = None

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
except Exception:
    AgGrid = None
    GridOptionsBuilder = None
    JsCode = None

try:
    from streamlit_extras.metric_cards import style_metric_cards
except Exception:
    def style_metric_cards(*args, **kwargs):
        return None

try:
    from streamlit_lottie import st_lottie
except Exception:
    st_lottie = None

try:
    import gspread
except Exception:
    gspread = None

RISK_FREE_ANNUAL = 0.02

# ── Auto-detect corporate proxy (Intel WPAD) so yfinance / requests work ──
def _auto_detect_proxy() -> None:
    """If HTTPS_PROXY is not set and we're on an Intel network (WPAD),
    auto-configure os.environ so that all Python HTTP libs go through the proxy."""
    if os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy"):
        return  # already configured
    try:
        import urllib.request
        urllib.request.urlopen("http://wpad.intel.com/wpad.dat", timeout=2)
        os.environ["HTTPS_PROXY"] = "http://proxy-iil.intel.com:912"
        os.environ["HTTP_PROXY"] = "http://proxy-iil.intel.com:912"
    except Exception:
        pass

_auto_detect_proxy()

CRYPTO_ETFS = {"IBIT", "ETHA", "BSOL", "MSTR"}
CRYPTO_SHARE_TICKERS = {"IBIT", "ETHA", "BSOL", "MSTR"}
BTC_SHARE_TICKERS = {"BTC", "IBIT"}
CHART_CRYPTO_TICKERS = {"IBIT", "ETHA", "BSOL", "BTC", "ETH", "SOL", "XRP"}
CHART_STOCK_TICKERS = {"MSTR"}
CHART_FORCED_ETF_TICKERS = {"QQQ", "VOO"}
_BRAND_PALETTE = [
    "#4f46e5", "#06b6d4", "#10b981", "#f59e0b", "#ef4444",
    "#8b5cf6", "#ec4899", "#14b8a6", "#f97316", "#6366f1",
    "#84cc16", "#0ea5e9", "#a855f7", "#fb923c", "#22d3ee",
]
DEFAULT_TRADINGVIEW_WATCHLIST = [
    {"category": "crypto", "ticker": "BTCUSD", "label": "Bitcoin / Dollar", "tv_symbol": "BINANCE:BTCUSDT"},
    {"category": "crypto", "ticker": "ETHUSD", "label": "Ethereum / Dollar", "tv_symbol": "BINANCE:ETHUSDT"},
    {"category": "crypto", "ticker": "SOLUSD", "label": "Solana / Dollar", "tv_symbol": "BINANCE:SOLUSDT"},
    {"category": "crypto", "ticker": "XRPUSD", "label": "Ripple / Dollar", "tv_symbol": "BINANCE:XRPUSDT"},
    {"category": "crypto", "ticker": "BTC.D", "label": "Bitcoin Market Cap Dominance", "tv_symbol": "CRYPTOCAP:BTC.D"},
    {"category": "crypto", "ticker": "IBIT", "label": "iShares Bitcoin Trust ETF", "tv_symbol": "NASDAQ:IBIT"},
    {"category": "crypto", "ticker": "ETHA", "label": "iShares Ethereum Trust ETF", "tv_symbol": "NYSE:ETHA"},
    {"category": "stocks", "ticker": "INTC", "label": "Intel Corporation", "tv_symbol": "NASDAQ:INTC"},
    {"category": "stocks", "ticker": "VOO", "label": "Vanguard S&P 500 ETF", "tv_symbol": "AMEX:VOO"},
    {"category": "stocks", "ticker": "QQQ", "label": "Invesco QQQ Trust (Nasdaq 100)", "tv_symbol": "NASDAQ:QQQ"},
    {"category": "stocks", "ticker": "MAGS", "label": "Roundhill Magnificent Seven ETF", "tv_symbol": "NASDAQ:MAGS"},
    {"category": "stocks", "ticker": "TA125", "label": "TA-125 Index", "tv_symbol": "TVC:TA125"},
    {"category": "stocks", "ticker": "NVDA", "label": "NVIDIA Corporation", "tv_symbol": "NASDAQ:NVDA"},
    {"category": "stocks", "ticker": "AAPL", "label": "Apple Inc.", "tv_symbol": "NASDAQ:AAPL"},
    {"category": "stocks", "ticker": "MSFT", "label": "Microsoft Corp.", "tv_symbol": "NASDAQ:MSFT"},
    {"category": "stocks", "ticker": "AMZN", "label": "Amazon.com, Inc.", "tv_symbol": "NASDAQ:AMZN"},
    {"category": "stocks", "ticker": "GOOG", "label": "Alphabet Inc. (Google) Class C", "tv_symbol": "NASDAQ:GOOG"},
    {"category": "stocks", "ticker": "META", "label": "Meta Platforms, Inc.", "tv_symbol": "NASDAQ:META"},
    {"category": "stocks", "ticker": "TSLA", "label": "Tesla, Inc.", "tv_symbol": "NASDAQ:TSLA"},
    {"category": "stocks", "ticker": "MSTR", "label": "MicroStrategy Inc.", "tv_symbol": "NASDAQ:MSTR"},
    {"category": "stocks", "ticker": "SOXX", "label": "iShares Semiconductor ETF", "tv_symbol": "NASDAQ:SOXX"},
    {"category": "macro", "ticker": "GOLD", "label": "Gold CFD", "tv_symbol": "TVC:GOLD"},
    {"category": "macro", "ticker": "SILVER", "label": "Silver CFD", "tv_symbol": "TVC:SILVER"},
    {"category": "macro", "ticker": "USOIL", "label": "WTI Crude Oil", "tv_symbol": "TVC:USOIL"},
    {"category": "macro", "ticker": "VIX", "label": "S&P 500 Volatility Index", "tv_symbol": "TVC:VIX"},
    {"category": "macro", "ticker": "USDILS", "label": "US Dollar / Israeli Shekel", "tv_symbol": "FX_IDC:USDILS"},
]
LOCAL_SETTINGS_FILE = Path(__file__).resolve().parent / "app_local_config.json"
DEFAULT_SERVICE_ACCOUNT_FILE = Path(__file__).resolve().parent / "clean-linker-492313-s3-770814e64205.json"
DEFAULT_WORKSHEET_NAME = "תמונת מצב"
DEFAULT_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbyDKgJszq8NWNgG7OQVPLflfN2rufBhAT5-fzmjy8iEVFMmNLZlK_CeI4MFvx1dijZF/exec"
MANUAL_DEPOSITS_FILE = Path(__file__).resolve().parent / "manual_deposits_store.json"
LOCAL_SNAPSHOT_CACHE_FILE = Path(__file__).resolve().parent / "snapshot_cache.csv"
VERIFIED_DATA_FALLBACK_FILE = Path(__file__).resolve().parent / "DATA" / "verified_data.csv"
APPS_SCRIPT_COOLDOWN_FILE = Path(__file__).resolve().parent / "apps_script_cooldown.json"
NETWORK_TIMEOUT_SECONDS = 20
NETWORK_MAX_RETRIES = 3
NETWORK_RETRY_BACKOFF_SECONDS = 1.4
APPS_SCRIPT_COOLDOWN_SECONDS = 300
_APPS_SCRIPT_RETRY_AFTER_TS = 0.0

LANG_EN = "English"
LANG_HE = "עברית"
DEFAULT_LANGUAGE = LANG_HE
THEME_SYSTEM = "system"
THEME_LIGHT = "light"
THEME_DARK = "dark"

COLUMN_LABELS = {
    "Ticker": {LANG_EN: "Ticker", LANG_HE: "טיקר"},
    "Current_Price": {LANG_EN: "Current Price", LANG_HE: "שער נוכחי"},
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
    "Estimated_BTC_Qty": {LANG_EN: "Estimated BTC Qty (incl. IBIT/MSTR)", LANG_HE: "כמות BTC מוערכת (כולל IBIT/MSTR)"},
    "Estimated_ETH_Qty": {LANG_EN: "Estimated ETH Qty (incl. ETHA)", LANG_HE: "כמות ETH מוערכת (כולל ETHA)"},
    "Estimated_SOL_Qty": {LANG_EN: "Estimated SOL Qty (incl. BSOL)", LANG_HE: "כמות SOL מוערכת (כולל BSOL)"},
    "Category": {LANG_EN: "Category", LANG_HE: "סוג"},
    "Yield": {LANG_EN: "Return", LANG_HE: "תשואה"},
    "Platform": {LANG_EN: "Platform", LANG_HE: "פלטפורמה"},
    "Net_Investment_ILS": {LANG_EN: "Net Investment (ILS)", LANG_HE: "עלות שקלית"},
    "Current_Value_ILS": {LANG_EN: "Current Value (ILS)", LANG_HE: "שווי שקלי"},
    "PnL_ILS": {LANG_EN: "Net P/L (ILS)", LANG_HE: "רווח/הפסד"},
}

SNAPSHOT_HEADERS = {
    "Purchase_Date": {LANG_EN: "Purchase Date", LANG_HE: "תאריך רכישה"},
    "Sell_Date": {LANG_EN: "Sell Date", LANG_HE: "תאריך מכירה"},
    "Current_Location": {LANG_EN: "Current Location", LANG_HE: "מיקום נוכחי"},
    "Platform": {LANG_EN: "Platform", LANG_HE: "פלטפורמה"},
    "Type": {LANG_EN: "Asset Type", LANG_HE: "סוג נכס"},
    "Ticker": {LANG_EN: "Ticker", LANG_HE: "טיקר"},
    "Quantity": {LANG_EN: "Quantity", LANG_HE: "כמות"},
    "Origin_Buy_Price": {LANG_EN: "Buy Price", LANG_HE: "שער קנייה"},
    "Cost_Origin": {LANG_EN: "Cost (Origin)", LANG_HE: "עלות מקור"},
    "Cost_ILS": {LANG_EN: "Cost ILS", LANG_HE: "עלותILS"},
    "Current_Value_ILS": {LANG_EN: "Value ILS", LANG_HE: "שווי ILS"},
    "Origin_Currency": {LANG_EN: "Purchase Currency", LANG_HE: "מטבע קנייה"},
    "Market_Price_Origin": {LANG_EN: "Current Price", LANG_HE: "שער נוכחי"},
    "Sell_Price_Origin": {LANG_EN: "Sell Price", LANG_HE: "מחיר מכירה"},
    "Yield_Current": {LANG_EN: "Return vs Current", LANG_HE: "תשואה מול שער נוכחי"},
    "Yield_At_Sale": {LANG_EN: "Return at Sale", LANG_HE: "תשואה במכירה"},
    "Current_Asset_Value_Display": {LANG_EN: "Current Asset Value", LANG_HE: "שווי נוכחי לנכס"},
    "Commission": {LANG_EN: "Commission", LANG_HE: "עמלה"},
    "Status": {LANG_EN: "Status", LANG_HE: "סטטוס"},
    "validation_status": {LANG_EN: "Validation", LANG_HE: "אימות"},
    "Trade_ID": {LANG_EN: "Trade ID", LANG_HE: "מזהה עסקה"},
    "Yield_Origin": {LANG_EN: "Return (Origin)", LANG_HE: "תשואה בשער מקור"},
    "Yield_ILS": {LANG_EN: "Return ILS", LANG_HE: "תשואה ILS"},
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
    raw = COLUMN_LABELS[col][language] if col in COLUMN_LABELS else col
    return _flip_currency_header_order(raw)


def _flip_currency_header_order(col_name: str) -> str:
    c = _clean(col_name)
    if not c:
        return c
    c = re.sub(r"\((USD|ILS)\)", lambda mm: mm.group(1).upper(), c, flags=re.IGNORECASE)
    c = re.sub(r"\s+", " ", c).strip()

    suffix = re.match(r"^(.+?)\s+(USD|ILS)$", c, flags=re.IGNORECASE)
    if suffix:
        return f"{suffix.group(2).upper()} {suffix.group(1).strip()}"

    prefix = re.match(r"^(USD|ILS)\s+(.+)$", c, flags=re.IGNORECASE)
    if prefix:
        return f"{prefix.group(2).strip()} {prefix.group(1).upper()}"

    compact_suffix = re.match(r"^(.+?)(USD|ILS)$", c, flags=re.IGNORECASE)
    if compact_suffix:
        return f"{compact_suffix.group(2).upper()} {compact_suffix.group(1).strip()}"

    compact_prefix = re.match(r"^(USD|ILS)(.+)$", c, flags=re.IGNORECASE)
    if compact_prefix:
        return f"{compact_prefix.group(2).strip()} {compact_prefix.group(1).upper()}"
    return c


def localize_dataframe_columns(df: pd.DataFrame, language: str) -> pd.DataFrame:
    out = df.copy()
    out.columns = [localize_column_name(str(c), language) for c in out.columns]
    return out


def localize_snapshot_view(df: pd.DataFrame, language: str) -> pd.DataFrame:
    out = df.copy()
    out = out.drop(columns=[c for c in out.columns if _clean(c).lower() in {"סטטוס מכירה", "sell status", "sale status"}], errors="ignore")
    renamed = {}
    for col in out.columns:
        if col in SNAPSHOT_HEADERS:
            renamed[col] = _flip_currency_header_order(SNAPSHOT_HEADERS[col][language])
    if renamed:
        out = out.rename(columns=renamed)
    out = out.rename(columns=lambda c: _flip_currency_header_order(str(c)))

    for raw_col, labels in VALUE_LABELS.items():
        visible_col = _flip_currency_header_order(SNAPSHOT_HEADERS.get(raw_col, {}).get(language, raw_col))
        if visible_col in out.columns:
            out[visible_col] = out[visible_col].map(lambda v: labels.get(_clean(v), {}).get(language, _clean(v)))
    return out


def _with_calendar_purchase_date(df: pd.DataFrame, language: str, force_uniform: bool = False) -> Tuple[pd.DataFrame, Dict[str, object]]:
    out = df.copy()
    localized_purchase = SNAPSHOT_HEADERS.get("Purchase_Date", {}).get(language, "Purchase_Date")
    localized_sell = SNAPSHOT_HEADERS.get("Sell_Date", {}).get(language, "Sell_Date")
    date_cols: List[str] = []
    for c in [localized_purchase, "Purchase_Date", "תאריך רכישה", localized_sell, "Sell_Date", "תאריך מכירה"]:
        if c in out.columns and c not in date_cols:
            date_cols.append(c)
    if not date_cols:
        return out, {}
    mobile_client = _is_mobile_client() and (not force_uniform)
    column_config: Dict[str, object] = {}
    for purchase_col in date_cols:
        parsed = _parse_dates_flexible(out[purchase_col])
        if mobile_client:
            # Mobile browsers can shift DateColumn by timezone; render as plain date text.
            out[purchase_col] = parsed.dt.strftime("%d/%m/%Y").fillna("")
            column_config[purchase_col] = st.column_config.TextColumn(purchase_col)
        else:
            out[purchase_col] = parsed.dt.date
            column_config[purchase_col] = st.column_config.DateColumn(
                purchase_col,
                format="DD/MM/YYYY",
            )
    return out, column_config


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


def _space(height: int = 16) -> None:
    st.markdown(f"<div style='margin-top:{height}px;'></div>", unsafe_allow_html=True)


@st.cache_data(ttl=21600)
def _load_lottie_json(url: str) -> Optional[Dict[str, object]]:
    try:
        with urlrequest.urlopen(url, timeout=8) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception:
        return None


def _render_premium_sidebar_lottie(language: str) -> None:
    if st_lottie is None:
        return
    # Lightweight loop-free animation for a premium but non-distracting sidebar header.
    lottie_data = _load_lottie_json("https://assets2.lottiefiles.com/packages/lf20_jcikwtux.json")
    if not lottie_data:
        return
    label = "Quick Portfolio Pulse" if language == LANG_EN else "דופק תיק מהיר"
    with st.sidebar:
        st.caption(label)
        st_lottie(lottie_data, height=76, speed=1, loop=False, key=f"pm_sidebar_lottie_{language}")


def _is_mobile_client() -> bool:
    try:
        headers = getattr(st.context, "headers", {}) or {}
        user_agent = _clean(headers.get("user-agent", "")).lower()
        if any(token in user_agent for token in ["android", "iphone", "ipad", "mobile"]):
            return True
    except Exception:
        pass
    try:
        qp = st.query_params if hasattr(st, "query_params") else {}
        if _clean(qp.get("mobile", "")).lower() in {"1", "true", "yes"}:
            return True
    except Exception:
        pass
    return False


def _apply_plotly_theme(fig: go.Figure, is_dark: bool, is_mobile: bool, is_bar: bool = False) -> go.Figure:
    """Apply dark/mobile layout to a Plotly figure."""
    if is_dark:
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(30,41,59,0.5)",
            font_color="#f1f5f9",
        )
    # Always push legend below the chart to avoid overlapping the title
    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.18,
            xanchor="center",
            x=0.5,
            font=dict(size=11 if is_mobile else 12),
        ),
        margin=dict(
            l=6 if is_mobile else 20,
            r=6 if is_mobile else 20,
            t=48,   # enough room for the title
            b=80,   # enough room for the legend below
        ),
    )
    if is_mobile:
        fig.update_layout(
            hoverlabel=dict(font=dict(size=13), bgcolor="rgba(30,30,30,0.85)", font_color="#fff"),
            title_font=dict(size=14),
            font=dict(size=11),
            autosize=True,
            height=320,          # minimum readable height on mobile
            minreducedheight=280,
        )
        if is_bar:
            fig.update_layout(showlegend=False, coloraxis_showscale=False, bargap=0.25)
            fig.update_xaxes(tickangle=45, tickfont=dict(size=9))
            fig.update_yaxes(tickfont=dict(size=9))
        else:
            fig.update_xaxes(tickfont=dict(size=9))
            fig.update_yaxes(tickfont=dict(size=9))
    else:
        if is_bar:
            fig.update_layout(showlegend=False)
    return fig


def _optimize_plotly_for_mobile(fig: go.Figure, is_mobile: bool, is_bar: bool = False) -> go.Figure:
    """Legacy wrapper — kept for call-site compatibility."""
    return _apply_plotly_theme(fig, is_dark=False, is_mobile=is_mobile, is_bar=is_bar)


def _normalize_theme_mode(theme_mode: str) -> str:
    mode = _clean(theme_mode).lower()
    if mode in {THEME_LIGHT, THEME_DARK, THEME_SYSTEM}:
        return mode
    return THEME_SYSTEM


def _resolve_theme_base(theme_mode: str) -> str:
    mode = _normalize_theme_mode(theme_mode)
    if mode == THEME_LIGHT:
        return THEME_LIGHT
    if mode == THEME_DARK:
        return THEME_DARK
    detected = (_clean(st.get_option("theme.base")) or THEME_LIGHT).lower()
    return THEME_DARK if detected == THEME_DARK else THEME_LIGHT


def inject_global_styles(language: str, theme_mode: str = THEME_SYSTEM) -> None:
    rtl = language == LANG_HE
    # ALWAYS use ltr for layout direction — the browser's Unicode BiDi algorithm
    # renders Hebrew text correctly without direction:rtl on CSS containers.
    # Setting direction:rtl on ANY flex ancestor of the sidebar flips its position
    # to the right. Keeping layout 100% LTR keeps the sidebar on the LEFT.
    direction = "ltr"
    align = "right" if rtl else "left"
    theme_base = _resolve_theme_base(theme_mode)
    is_dark = theme_base == "dark"
    sidebar_bg = "#1E1E1E" if is_dark else "#f8f9fa"
    metric_bg = "#2B2B2B" if is_dark else "#ffffff"
    metric_border = "#3a3a3a" if is_dark else "#e8ecf3"
    metric_text = "#f8fafc" if is_dark else "#0f172a"
    metric_label = "#d1d5db" if is_dark else "#475569"
    df_line = "#343434" if is_dark else "#f0f0f0"
    df_header_bg = "#262626" if is_dark else "#fafafa"
    title_color = "#f8fafc" if is_dark else "#0f172a"
    subtitle_color = "#cbd5e1" if is_dark else "#475569"
    # Mobile bottom nav colors
    nav_bg = "rgba(22,22,22,0.96)" if is_dark else "rgba(255,255,255,0.96)"
    nav_border = "rgba(255,255,255,0.09)" if is_dark else "rgba(0,0,0,0.08)"
    nav_inactive = "#94a3b8" if is_dark else "#64748b"
    hamburger_bg = "rgba(30,30,30,0.92)" if is_dark else "rgba(255,255,255,0.92)"
    hamburger_border = "rgba(255,255,255,0.18)" if is_dark else "rgba(203,213,225,0.9)"
    css = f"""
    <style>
    /* Force the Streamlit root layout container to LTR so the sidebar
       always appears on the LEFT regardless of page language / direction.
       RTL direction is applied only to content containers below. */
    [data-testid="stApp"] {{
        direction: ltr !important;
    }}
    /* ── Zero top gap: no scrollable space above title ── */
    .block-container {{
        padding-top: 0 !important;
        margin-top: 0 !important;
    }}
    section.main, .main {{
        padding-top: 0 !important;
    }}
    /* ── Suppress phantom 0-height iframes from `components.html(height=0)`.
       IMPORTANT: limit to `data-testid="stIFrame"` so custom components
       (option_menu, lottie, ...) are NOT hidden — they start at
       height=0 and resize via postMessage; CSS-hiding kills them.       */
    .element-container:has(> div > iframe[data-testid="stIFrame"][height="0"]),
    [data-testid="stElementContainer"]:has(> div > iframe[data-testid="stIFrame"][height="0"]) {{
        display: none !important;
    }}
    /* Collapse "phantom" element-containers that hold ONLY <style>/<script>
       tags (st.markdown CSS injections). The :not(:has(visible-tags)) test
       matches any markdown container without paragraph-level content.     */
    .element-container:has(> .stMarkdown > [data-testid="stMarkdownContainer"]:not(:has(p,h1,h2,h3,h4,h5,h6,ul,ol,li,table,img,svg,hr,button,a,code,pre,blockquote,em,strong,details,summary,figure))),
    [data-testid="stElementContainer"]:has(> .stMarkdown > [data-testid="stMarkdownContainer"]:not(:has(p,h1,h2,h3,h4,h5,h6,ul,ol,li,table,img,svg,hr,button,a,code,pre,blockquote,em,strong,details,summary,figure))) {{
        display: none !important;
    }}
    /* ── Hide Deploy button — not needed for personal/local app, and was
       stacking over the Main Menu (3-dots) at the same top-right slot. */
    [data-testid="stAppDeployButton"],
    [data-testid="stBaseButton-header"][kind="header"]:not([data-testid="stMainMenuButton"]),
    button[kind="header"][data-testid="stBaseButton-header"]:not([data-testid="stMainMenuButton"]) {{
        display: none !important;
        visibility: hidden !important;
        width: 0 !important; height: 0 !important;
        pointer-events: none !important;
    }}
    /* ── Sidebar collapse-arrow buttons — fix the top:-14px clip by
       making them position:fixed so they don't inherit the zero-height
       header's geometry. */
    [data-testid="stExpandSidebarButton"],
    [data-testid="stSidebarCollapseButton"],
    button[data-testid="stExpandSidebarButton"],
    button[data-testid="stSidebarCollapseButton"] {{
        position: fixed !important;
        top: 12px !important;
        z-index: 1000003 !important;
        background: {metric_bg} !important;
        border: 1px solid {metric_border} !important;
        border-radius: 10px !important;
        width: 38px !important; height: 38px !important;
        padding: 6px !important;
        box-shadow: 0 4px 14px -4px rgba(15,23,42,0.22) !important;
        display: inline-flex !important;
        align-items: center !important;
        justify-content: center !important;
        opacity: 1 !important;
        visibility: visible !important;
        pointer-events: auto !important;
        color: {metric_text} !important;
    }}
    [data-testid="stExpandSidebarButton"] svg,
    [data-testid="stSidebarCollapseButton"] svg {{
        width: 22px !important; height: 22px !important;
        color: {metric_text} !important;
        fill: currentColor !important;
    }}
    /* When sidebar collapsed: expand button at the LEFT edge (LTR layout) */
    [data-testid="stExpandSidebarButton"] {{
        left: 12px !important; right: auto !important;
    }}
    /* When sidebar open: collapse button on the LEFT edge of viewport but
       just inside the open sidebar (Streamlit places it inside .stSidebar). */
    [data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"] {{
        position: absolute !important;   /* relative to sidebar, not viewport */
        top: 12px !important;
        right: 8px !important;
        left: auto !important;
        z-index: 99 !important;
    }}

    /* ── 3-dots Main Menu — pin to TOP-RIGHT (desktop) or TOP-LEFT next
       to the sidebar-expand button (mobile). Was previously (a) clipped
       by parent header height:0, and (b) covered by the Deploy button
       at the same position. Now isolated, fixed, above everything.       */
    [data-testid="stMainMenuButton"],
    button[data-testid="stMainMenuButton"] {{
        position: fixed !important;
        top: 8px !important;
        right: 10px !important;
        left: auto !important;
        z-index: 1000001 !important;       /* above EVERYTHING */
        background: {metric_bg} !important;
        border: 1px solid {metric_border} !important;
        border-radius: 10px !important;
        width: 40px !important; height: 40px !important;
        padding: 6px !important;
        box-shadow: 0 6px 18px -4px rgba(15,23,42,0.22) !important;
        display: inline-flex !important;
        align-items: center !important;
        justify-content: center !important;
        opacity: 1 !important;
        visibility: visible !important;
        pointer-events: auto !important;
        color: {metric_text} !important;
    }}
    [data-testid="stMainMenuButton"] svg {{
        width: 22px !important; height: 22px !important;
        color: {metric_text} !important;
        fill: currentColor !important;
    }}
    [data-testid="stMainMenuButton"]:hover {{
        background: {metric_bg} !important;
        filter: brightness(1.05);
        transform: translateY(-1px);
    }}
    /* Mobile: 3-dots Main Menu sits at the SAME vertical level as the
       page-pills bar (top: 12px), in the right-side 56px gutter we
       reserved by giving the pills `margin-right: 56px`. Header z is
       lifted to 1000000 so it stacks above .app-header-wrap (z:9999). */
    @media (max-width: 820px) {{
        header[data-testid="stHeader"] {{
            z-index: 1000000 !important;
            overflow: visible !important;
        }}
        [data-testid="stMainMenuButton"],
        button[data-testid="stMainMenuButton"] {{
            top: 12px !important;
            right: 10px !important;
            left: auto !important;
            width: 38px !important; height: 38px !important;
            z-index: 1000050 !important;
            box-shadow: 0 4px 14px -2px rgba(15,23,42,0.32) !important;
        }}
        /* The app-header-wrap (title) sticks BELOW the pills bar so it
           never gets covered by the nav. Includes a 10px breathing gap. */
        .app-header-wrap {{
            top: 76px !important;
            margin-top: 10px !important;
        }}
        /* When sidebar is OPEN (rare on mobile) — collapse arrow position */
        [data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"] {{
            top: 12px !important;
        }}
    }}
    .app-header-wrap {{
        text-align: center !important;
        direction: inherit !important;
        margin: 0 0 0.5rem 0;
        position: sticky !important;
        top: 0 !important;
        z-index: 9999 !important;
        background: {metric_bg} !important;
        padding: 0.35rem 0.5rem 0.3rem 0.5rem !important;
        border-bottom: 1px solid {metric_border} !important;
        backdrop-filter: blur(8px) !important;
        -webkit-backdrop-filter: blur(8px) !important;
    }}
    /* ── Pinned navigation inside pages (tabs / sub-nav) ──
       Desktop: header is position:fixed with height:0, so stTabs sticks at 0.
       Mobile: an offset below the app-header-wrap is applied inside the mobile
       media-query further below. Lower z-index than header/top-nav so those
       always win if stacks collide. */
    [data-baseweb="tab-list"] {{
        position: sticky !important;
        top: 0 !important;
        z-index: 900 !important;
        background: {metric_bg} !important;
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        border-bottom: 1px solid {metric_border} !important;
    }}

    /* ─────────────────────────────────────────────────────────────────
       Help-badges (?) — perfect SVG circle, taps open a popup on mobile,
       hover/click on desktop. The native <details> element handles all
       toggling without JS.                                             */
    .pp-help-wrap {{
        text-align: right;
        margin: -2px 0 -4px 0;
        position: relative;
        direction: ltr;
    }}
    details.pp-help {{
        display: inline-block;
        position: relative;
    }}
    details.pp-help > summary.pp-help-summary {{
        list-style: none;
        cursor: pointer;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 22px; height: 22px;
        border-radius: 50%;
        color: #94a3b8;
        background: transparent;
        transition: background 120ms ease, color 120ms ease, transform 120ms ease;
        -webkit-tap-highlight-color: transparent;
        user-select: none;
    }}
    details.pp-help > summary.pp-help-summary::-webkit-details-marker {{ display: none; }}
    details.pp-help > summary.pp-help-summary::marker {{ display: none; content: ""; }}
    details.pp-help > summary.pp-help-summary:hover,
    details.pp-help > summary.pp-help-summary:focus-visible {{
        color: #6366f1;
        background: rgba(99, 102, 241, 0.10);
        outline: none;
    }}
    details.pp-help[open] > summary.pp-help-summary {{
        color: #6366f1;
        background: rgba(99, 102, 241, 0.14);
    }}
    details.pp-help > summary.pp-help-summary .pp-help-icon {{
        display: block; pointer-events: none;
    }}
    details.pp-help > .pp-help-body {{
        position: absolute;
        top: calc(100% + 6px);
        right: 0; left: auto;
        z-index: 10000;
        min-width: 220px;
        max-width: min(340px, calc(100vw - 24px));
        padding: 12px 14px;
        font-size: 0.86rem;
        line-height: 1.55;
        background: {metric_bg};
        color: {metric_text};
        border: 1px solid {metric_border};
        border-radius: 12px;
        box-shadow: 0 14px 32px -8px rgba(0,0,0,0.32);
        unicode-bidi: plaintext;
        white-space: normal;
        word-wrap: break-word;
        text-align: start;
        animation: pp-help-fade 120ms ease-out;
    }}
    @keyframes pp-help-fade {{
        from {{ opacity: 0; transform: translateY(-4px); }}
        to   {{ opacity: 1; transform: translateY(0); }}
    }}
    /* Mobile: keep popup INLINE near the badge (matching Streamlit's
       native `help=` tooltip behaviour), NOT a bottom drawer. The popup
       opens just below the ? badge, capped at viewport-minus-padding for
       width so long Hebrew text wraps cleanly. */
    @media (max-width: 600px) {{
        details.pp-help > .pp-help-body {{
            position: absolute;
            top: calc(100% + 6px);
            right: 0; left: auto; bottom: auto;
            min-width: 220px;
            max-width: min(320px, calc(100vw - 32px));
            padding: 11px 13px;
            font-size: 0.86rem;
            box-shadow: 0 12px 28px -6px rgba(0,0,0,0.35);
            animation: pp-help-fade 120ms ease-out;
        }}
    }}
    /* ── Overlap shields: make sure charts, dataframes and tooltip badges
           never visually clip under the sticky header / nav.             ── */
    [data-testid="stPlotlyChart"],
    [data-testid="stDataFrame"],
    [data-testid="stTable"] {{
        position: relative !important;
        z-index: 1 !important;
    }}
    /* Ensure metric tiles keep gap below sticky top nav on every page. */
    [data-testid="stMetric"] {{
        position: relative !important;
        z-index: 1 !important;
    }}
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
    [data-testid="stDecoration"] {{display: block !important; visibility: visible !important;}}
    #MainMenu {{display: block !important; visibility: visible !important; opacity: 1 !important;}}
    header, [data-testid="stHeader"] {{overflow: visible !important;}}  /* mobile: keep header functional */
    [data-testid="stToolbar"] {{display: flex !important; visibility: visible !important;}}
    [data-testid="stToolbarActions"] {{
        display: flex !important;
        visibility: visible !important;
        overflow: visible !important;
        z-index: 100004 !important;
        position: relative !important;
    }}
    [data-testid="stDataFrame"] [role="grid"] {{direction: {direction}; text-align: {align};}}
    [data-testid="stDataFrame"] table {{direction: {direction}; text-align: {align}; border-collapse: collapse !important;}}
    [data-testid="stDataFrame"] th,
    [data-testid="stDataFrame"] td {{unicode-bidi: plaintext;}}
    [data-testid="stDataFrame"] {{overflow-x: auto;}}
    /* SaaS-like dataframe grid: remove vertical dividers, keep subtle row separators. */
    [data-testid="stDataFrame"] [role="columnheader"],
    [data-testid="stDataFrame"] [role="gridcell"],
    [data-testid="stDataFrame"] [role="rowheader"] {{
        border-left: none !important;
        border-right: none !important;
        border-top: none !important;
        border-bottom: 1px solid {df_line} !important;
    }}
    [data-testid="stDataFrame"] [role="columnheader"] {{
        background-color: {df_header_bg} !important;
        font-weight: 600 !important;
    }}
    [data-testid="stDataFrame"] table th,
    [data-testid="stDataFrame"] table td {{
        border-left: none !important;
        border-right: none !important;
        border-top: none !important;
        border-bottom: 1px solid {df_line} !important;
    }}
    [data-testid="stDataFrame"] table thead th {{
        background-color: {df_header_bg} !important;
    }}
    [data-testid="stMetric"] {{direction: {direction};}}
    [data-testid="stMetric"] {{
        background: {metric_bg} !important;
        border: 1px solid {metric_border} !important;
        border-radius: 12px !important;
        box-shadow: 0 4px 8px rgba(0,0,0,0.10) !important;
        padding: 0.7rem 0.9rem !important;
    }}
    [data-testid="stMetric"] [data-testid="stMetricLabel"] {{
        color: {metric_label} !important;
        font-size: 0.82rem !important;
        line-height: 1.15 !important;
        white-space: nowrap !important;
    }}
    [data-testid="stMetric"] [data-testid="stMetricValue"] {{
        color: {metric_text} !important;
        font-size: 1.25rem !important;
        font-weight: 700 !important;
        line-height: 1.15 !important;
        white-space: nowrap !important;
    }}
    .app-logo-row {{
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        flex-direction: row !important;
        gap: 0.75rem !important;
        margin-bottom: 0.2rem !important;
    }}
    .app-logo-icon {{
        flex-shrink: 0 !important;
        filter: drop-shadow(0 4px 14px rgba(99,102,241,0.4)) !important;
    }}
    .app-logo-text {{
        display: flex !important;
        flex-direction: column !important;
        align-items: flex-end !important;
        gap: 0.08rem !important;
    }}
    .app-logo-badge {{
        font-size: 0.72rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.07em !important;
        text-transform: uppercase !important;
        color: #818cf8 !important;
        background: rgba(99,102,241,0.1) !important;
        border: 1px solid rgba(129,140,248,0.3) !important;
        border-radius: 20px !important;
        padding: 2px 10px !important;
        line-height: 1.6 !important;
    }}
    .app-main-title {{
        text-align: right !important;
        direction: rtl !important;
        background: linear-gradient(135deg, #6366f1 0%, #818cf8 50%, #38bdf8 100%) !important;
        -webkit-background-clip: text !important;
        -webkit-text-fill-color: transparent !important;
        background-clip: text !important;
        font-size: 3.1rem;
        font-weight: 900;
        margin: 0;
        letter-spacing: -0.03em;
        line-height: 1.0;
    }}
    .app-sub-title {{
        text-align: {align} !important;
        direction: {direction} !important;
        color: {subtitle_color} !important;
        font-size: 1.02rem;
        font-weight: 600;
        margin: 0 0 0.35rem 0;
    }}
    .dashboard-stats-line {{
        direction: {direction} !important;
        text-align: {align} !important;
        color: {subtitle_color} !important;
        margin: 0.15rem 0 0.35rem 0;
        font-size: 0.92rem;
    }}
    [data-testid="stMarkdownContainer"],
    [data-testid="stMarkdownContainer"] p,
    h1, h2, h3, h4, h5, h6,
    [data-testid="stDataFrame"] {{
        direction: {direction} !important;
        text-align: {align} !important;
    }}
    /* ── BiDi correctness for neutral glyphs (?, !, «», quotes, digits) ──
       `unicode-bidi: plaintext` makes each text run infer its direction
       from its first strong character. In Hebrew paragraphs, `?`/`!` now
       sit correctly at the visual end (left side). English strings keep
       their neutrals on the right. Fixes the "question mark on wrong
       side" bug on both mobile and desktop. */
    [data-testid="stMarkdownContainer"] p,
    [data-testid="stMarkdownContainer"] li,
    [data-testid="stMarkdownContainer"] span,
    [data-testid="stAlert"] p,
    [data-testid="stAlert"] div,
    [data-testid="stCaptionContainer"],
    [data-testid="stCaptionContainer"] p,
    [data-testid="stMetric"] [data-testid="stMetricLabel"],
    [data-testid="stMetric"] [data-testid="stMetricDelta"],
    label, .stTooltipIcon,
    [data-testid="stWidgetLabel"], [data-testid="stWidgetLabel"] p,
    h1, h2, h3, h4, h5, h6 {{
        unicode-bidi: plaintext !important;
    }}
    [data-baseweb="tab-list"] {{
        display: flex !important;
        overflow-x: hidden !important;
        -webkit-overflow-scrolling: touch;
        scrollbar-width: none;
        white-space: nowrap;
    }}
    [data-baseweb="tab-list"] [data-baseweb="tab"] {{
        flex: 1 1 0% !important;
        min-width: 0 !important;
        text-align: center !important;
        padding-left: 4px !important;
        padding-right: 4px !important;
        font-size: 0.78rem !important;
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
    }}
    [data-testid="stMarkdownContainer"] p {{line-height: 1.35;}}
    /* ── Sidebar: always LEFT — spring animation ── */
    section[data-testid="stSidebar"],
    [data-testid="stSidebar"] {{
        left: 0 !important;
        right: auto !important;
        direction: ltr !important;
        will-change: transform, opacity;
        transition: transform 0.42s cubic-bezier(0.16, 1, 0.3, 1),
                    opacity  0.30s ease-out !important;
    }}
    /* Collapsed: spring off-screen LEFT */
    section[data-testid="stSidebar"][aria-expanded="false"] {{
        transform: translateX(-100%) !important;
        opacity: 0 !important;
        pointer-events: none !important;
    }}
    /* Expanded: spring into view */
    section[data-testid="stSidebar"][aria-expanded="true"] {{
        transform: translateX(0) !important;
        opacity: 1 !important;
        pointer-events: auto !important;
    }}
    [data-testid="stSidebar"] .nav,
    [data-testid="stSidebar"] .nav-item,
    [data-testid="stSidebar"] .nav-link {{
        direction: ltr !important;
        text-align: left !important;
    }}
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
        border: 1px solid {metric_border};
        border-radius: 16px;
        padding: 0.9rem 1rem;
        background: {metric_bg};
        color: {metric_text};
    }}
    .pm-metric-grid {{
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 0.75rem;
        margin: 0.35rem 0 1rem 0;
    }}
    .pm-card {{
        border: 1px solid {metric_border};
        border-radius: 16px;
        padding: 0.9rem 1rem;
        background: {metric_bg};
        box-shadow: 0 6px 18px rgba(10, 20, 30, 0.06);
        color: {metric_text} !important;
    }}
    .pm-title {{font-size: 0.82rem; opacity: 0.8; margin-bottom: 0.35rem; color: {metric_label} !important;}}
    .pm-value {{font-size: 1.35rem; font-weight: 700; line-height: 1.15; color: {metric_text} !important;}}
    .pm-delta {{font-size: 0.84rem; opacity: 0.75; margin-top: 0.2rem; color: {metric_label} !important;}}
    @media (min-width: 769px) {{
        .block-container,
        [data-testid="stAppViewContainer"],
        [data-testid="stMarkdownContainer"] {{
            direction: {direction} !important;
            text-align: {align} !important;
        }}
        /* ── Remove top whitespace on desktop: push title to top ── */
        section.main > div.block-container,
        [data-testid="stMainBlockContainer"],
        .main .block-container,
        .block-container {{
            padding-top: 0rem !important;
            margin-top: 0rem !important;
        }}
        /* Desktop header: transparent and zero-height, but KEEP the toolbar
           actions (3-dots Main Menu, Rerun, Clear Cache) floating at top-right.
           The prior rule `display:none` accidentally removed the Main Menu. */
        header[data-testid="stHeader"] {{
            background: transparent !important;
            box-shadow: none !important;
            border: none !important;
            height: 0 !important;
            min-height: 0 !important;
            overflow: visible !important;
            position: fixed !important;
            top: 0 !important; right: 0 !important; left: 0 !important;
            z-index: 999999 !important;
            pointer-events: none !important;
        }}
        header[data-testid="stHeader"] button,
        header[data-testid="stHeader"] [data-testid="stToolbar"],
        header[data-testid="stHeader"] [data-testid="stToolbarActions"],
        header[data-testid="stHeader"] [data-testid="stMainMenuButton"] {{
            pointer-events: auto !important;
            visibility: visible !important;
            opacity: 1 !important;
        }}
        h1, h2, h3, h4, h5, h6 {{
            direction: {direction} !important;
            text-align: {align} !important;
        }}
        [data-baseweb="tab-list"] {{
            direction: {direction} !important;
            justify-content: flex-start !important;
        }}
        [data-baseweb="tab-list"] [data-baseweb="tab"] {{
            text-align: {align} !important;
        }}
        .pm-card, [data-testid="stMetric"] {{
            background: {metric_bg} !important;
            border: 1px solid {metric_border} !important;
            border-radius: 8px !important;
            box-shadow: 0 2px 4px rgba(0,0,0,0.08) !important;
            transition: transform 0.18s ease, box-shadow 0.18s ease !important;
        }}
        .pm-card:hover, [data-testid="stMetric"]:hover {{
            transform: translateY(-2px);
            box-shadow: 0 10px 18px rgba(0,0,0,0.08) !important;
        }}
        /* Desktop: Fixed sidebar always visible on left — aggressive dark-mode override */
        [data-testid="stSidebar"],
        section[data-testid="stSidebar"] {{
            background: {sidebar_bg} !important;
            background-color: {sidebar_bg} !important;
            border-right: 1px solid {metric_border} !important;
            min-height: 100vh !important;
        }}
        [data-testid="stSidebar"] > div,
        [data-testid="stSidebar"] > div > div,
        [data-testid="stSidebar"] > div:first-child,
        [data-testid="stSidebar"] [data-testid="stSidebarContent"],
        [data-testid="stSidebar"] [data-testid="stSidebarContent"] > div,
        [data-testid="stSidebar"] [data-testid="stSidebarUserContent"] {{
            background: {sidebar_bg} !important;
            background-color: {sidebar_bg} !important;
        }}
        /* Force ALL non-interactive children to inherit the sidebar dark bg */
        [data-testid="stSidebar"] div:not([class*="stButton"]):not([class*="stSelectbox"]):not([data-baseweb]) {{
            background-color: {sidebar_bg} !important;
        }}
    }}
    @media (max-width: 980px) {{
        .pm-metric-grid {{grid-template-columns: repeat(2, minmax(0, 1fr));}}
    }}
    @media (max-width: 768px) {{
        /* Mobile: sticky tabs need to sit BELOW the header + top nav to
           avoid overlapping. Compute offset so chart/tab content clears
           both the app-header-wrap and the 4-pill nav radio. */
        [data-baseweb="tab-list"] {{
            top: calc(6.2rem + env(safe-area-inset-top, 0px)) !important;
        }}
        body, [data-testid="stAppViewContainer"] {{
            direction: {direction} !important;
            text-align: {align} !important;
            font-family: system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif !important;
        }}
        footer {{display: none !important;}}
        #MainMenu {{display: block !important; visibility: visible !important;}}
        header, [data-testid="stHeader"] {{
            display: block !important;
            background: transparent !important;
            box-shadow: none !important;
            z-index: 100001 !important;
            overflow: visible !important;
        }}
        /* Force toolbar + 3-dot menu visible on mobile */
        [data-testid="stToolbar"] {{
            display: flex !important;
            visibility: visible !important;
            opacity: 1 !important;
            overflow: visible !important;
            z-index: 100005 !important;
        }}
        [data-testid="stToolbarActions"] {{
            display: flex !important;
            visibility: visible !important;
            opacity: 1 !important;
            overflow: visible !important;
            z-index: 100005 !important;
            position: relative !important;
        }}
        [data-testid="stMainMenuButton"],
        [data-testid="stToolbar"] button,
        [data-testid="stToolbarActions"] button {{
            display: flex !important;
            visibility: visible !important;
            opacity: 1 !important;
            pointer-events: auto !important;
        }}
        /* Hide Deploy button and branding on mobile, but keep sidebar expand and 3-dot menu */
        [data-testid="stAppDeployButton"],
        [data-testid="stStatusWidget"] {{
            display: none !important;
        }}
        /* Sidebar expand button — force visible as fixed hamburger */
        [data-testid="stExpandSidebarButton"] {{
            display: flex !important;
            visibility: visible !important;
            opacity: 1 !important;
            position: fixed !important;
            top: calc(0.55rem + env(safe-area-inset-top, 0px)) !important;
            left: 0.65rem !important;
            z-index: 100002 !important;
            width: 40px !important;
            height: 40px !important;
            border-radius: 10px !important;
            border: 1px solid rgba(203, 213, 225, 0.9) !important;
            background: rgba(255, 255, 255, 0.95) !important;
            box-shadow: 0 4px 12px rgba(15, 23, 42, 0.12) !important;
            align-items: center !important;
            justify-content: center !important;
            cursor: pointer !important;
            -webkit-tap-highlight-color: transparent !important;
        }}
        .block-container {{
            padding-top: 0.45rem !important;
            padding-bottom: 0.8rem !important;
            padding-left: 0.65rem !important;
            padding-right: 0.65rem !important;
        }}
        /* ── App-level: prevent overscroll bounce ── */
        body {{
            overscroll-behavior-y: none !important;
        }}
        /* ── Title: sticky BELOW the pills nav. Pills are effectively
              fixed-positioned (don't take flow space), so we add a 70px
              top-margin to physically push the title below them. The
              sticky behaviour pins it at the same line when scrolling. ── */
        .app-header-wrap {{
            position: sticky !important;
            top: 76px !important;
            margin-top: 70px !important;
            z-index: 900 !important;        /* below nav (z:998) */
            padding: 0.3rem 0.5rem 0.2rem 0.5rem !important;
        }}
        /* ── Nav radio bar: compact text ── */
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"] p {{
            font-size: 10px !important;
        }}
        /* ── st.tabs: compact, no scroll, all fit in one line ── */
        [data-baseweb="tab-list"] {{
            overflow-x: hidden !important;
            scrollbar-width: none !important;
        }}
        [data-baseweb="tab-list"] [data-baseweb="tab"] {{
            flex: 1 1 0% !important;
            min-width: 0 !important;
            padding-left: 4px !important;
            padding-right: 4px !important;
            font-size: 0.68rem !important;
            white-space: nowrap !important;
        }}
        h1 {{font-size: 1.6rem !important; margin: 0.2rem 0 0.35rem !important; line-height: 1.2 !important;}}
        h2 {{font-size: 1.3rem !important; margin: 0.18rem 0 0.32rem !important; line-height: 1.2 !important;}}
        h3 {{font-size: 1.1rem !important; margin: 0.16rem 0 0.28rem !important; line-height: 1.2 !important;}}

        /* ══ ANTI-SQUISH TABLES ══════════════════════════════════════════════
           Scroll is ONLY on the dataframe element itself.
           Parent containers get overflow-x:visible so they don't clip the
           table without creating their own competing scroll region.          */
        [data-testid="stDataFrame"],
        [data-testid="stTable"] {{
            overflow-x: auto !important;
            -webkit-overflow-scrolling: touch !important;
            display: block !important;
        }}
        /* Prevent parent containers from clipping the table — NO overflow:auto here */
        .element-container,
        [data-testid="stElementContainer"],
        [data-testid="stVerticalBlock"],
        [data-testid="stVerticalBlockBorderWrapper"] {{
            overflow-x: visible !important;
        }}
        /* The glide-data-grid canvas wrapper — allow scroll inward */
        [data-testid="stDataFrame"] > div,
        [data-testid="stDataFrame"] > div > div,
        [data-testid="stDataFrame"] > div > div > div {{
            overflow-x: auto !important;
            -webkit-overflow-scrolling: touch !important;
        }}
        [data-testid="stDataFrame"] table,
        [data-testid="stTable"] table {{
            min-width: 550px !important;
            width: max-content !important;
            border-collapse: collapse !important;
        }}
        [data-testid="stDataFrame"] th,
        [data-testid="stDataFrame"] td,
        [data-testid="stTable"] th,
        [data-testid="stTable"] td {{
            white-space: nowrap !important;
            min-width: 90px !important;
            padding: 4px 8px !important;
            font-size: 11px !important;
        }}
        /* ══ CHART MIN-SIZE: prevent microscopic charts on mobile ═════════ */
        [data-testid="stPlotlyChart"] {{
            min-width: 300px !important;
            overflow-x: auto !important;
        }}
        [data-testid="stPlotlyChart"] > div {{
            min-width: 300px !important;
        }}
        /* ══ KPI CARDS: bigger value, muted label, no clipping on mobile ══ */
        [data-testid="stMetric"] {{
            overflow: visible !important;
            min-height: unset !important;
            height: auto !important;
            padding-bottom: 6px !important;
        }}
        [data-testid="stMetricValue"],
        [data-testid="stMetric"] [data-testid="stMetricValue"] {{
            font-size: 1.35rem !important;
            font-weight: bold !important;
            line-height: 1.2 !important;
            overflow: visible !important;
            white-space: normal !important;
            word-break: break-word !important;
        }}
        [data-testid="stMetricLabel"],
        [data-testid="stMetric"] [data-testid="stMetricLabel"] {{
            font-size: 0.78rem !important;
            color: #6b7280 !important;
            line-height: 1.3 !important;
            overflow: visible !important;
            white-space: normal !important;
            word-break: break-word !important;
        }}
        [data-testid="stMetricDelta"] {{
            font-size: 0.62rem !important;
            white-space: normal !important;
            line-height: 1.3 !important;
        }}
        /* ══ FORM RADIO → HORIZONTAL PILLS ══════════════════════════════ */
        [data-testid="stRadio"] > div[role="radiogroup"] {{
            display: flex !important;
            flex-direction: row !important;
            flex-wrap: wrap !important;
            gap: 4px !important;
        }}
        [data-testid="stRadio"] > div[role="radiogroup"] > label {{
            flex: 1 1 auto !important;
            text-align: center !important;
            padding: 6px 8px !important;
            border-radius: 8px !important;
            border: 1px solid #e2e8f0 !important;
            font-size: 0.75rem !important;
            cursor: pointer !important;
        }}
        /* ══ REDUCE FORM VERTICAL SPACING ═══════════════════════════════ */
        [data-testid="stTextInput"],
        [data-testid="stNumberInput"],
        [data-testid="stSelectbox"],
        [data-testid="stDateInput"] {{
            margin-bottom: 0.3rem !important;
        }}
        [data-testid="stTextInput"] > div,
        [data-testid="stNumberInput"] > div,
        [data-testid="stSelectbox"] > div {{
            min-height: 38px !important;
        }}
        /* ══ PREMIUM PnL COLORS ══════════════════════════════════════════ */
        [data-testid="stMetricDelta"][data-testid*="positive"],
        [data-testid="stMetricDelta"] svg[style*="color: rgb(9, 171, 59)"],
        [data-testid="stMetricDelta"] [style*="color: rgb(9, 171, 59)"] {{
            color: #10b981 !important;
        }}
        [data-testid="stMetricDelta"] svg[style*="color: rgb(255"],
        [data-testid="stMetricDelta"] [style*="color: rgb(255"] {{
            color: #ef4444 !important;
        }}

        [data-testid="stSidebar"] {{
            background: {sidebar_bg} !important;
            background-color: {sidebar_bg} !important;
            z-index: 999999 !important;
            direction: ltr !important;
            transition: opacity 160ms ease-out !important;
            animation: none !important;
            opacity: 1 !important;
            left: 0 !important;
            right: auto !important;
        }}
        [data-testid="stSidebar"][aria-expanded="false"] {{
            width: 0 !important;
            min-width: 0 !important;
            max-width: 0 !important;
            background: transparent !important;
            background-color: transparent !important;
            border: 0 !important;
            box-shadow: none !important;
            overflow: hidden !important;
            opacity: 0 !important;
        }}
        [data-testid="stSidebar"][aria-expanded="true"] {{
            width: 280px !important;
            min-width: 280px !important;
            max-width: 82vw !important;
            opacity: 1 !important;
        }}
        [data-testid="stSidebar"] > div:first-child,
        [data-testid="stSidebar"] [data-testid="stSidebarContent"] {{
            background: {sidebar_bg} !important;
            background-color: {sidebar_bg} !important;
            opacity: 1 !important;
            border-left: 0 !important;
            z-index: 999999 !important;
            direction: ltr !important;
            text-align: left !important;
            overflow-x: hidden !important;
            overflow-y: auto !important;
            backdrop-filter: none !important;
            -webkit-backdrop-filter: none !important;
            transition: none !important;
            animation: none !important;
        }}
        [data-testid="stSidebar"][aria-expanded="true"] > div:first-child,
        [data-testid="stSidebar"][aria-expanded="true"] [data-testid="stSidebarContent"] {{
            border-left: 1px solid #eef2f7 !important;
        }}
        [data-testid="stSidebar"][aria-expanded="false"] > div:first-child,
        [data-testid="stSidebar"][aria-expanded="false"] [data-testid="stSidebarContent"] {{
            border: 0 !important;
            background: transparent !important;
            background-color: transparent !important;
            box-shadow: none !important;
            display: none !important;
            width: 0 !important;
            min-width: 0 !important;
            max-width: 0 !important;
            padding: 0 !important;
            margin: 0 !important;
            overflow: hidden !important;
        }}
        [data-testid="stSidebar"] *,
        [data-testid="stSidebar"] [data-testid="stSidebarContent"] * {{
            writing-mode: horizontal-tb !important;
            word-break: normal !important;
            overflow-wrap: normal !important;
            white-space: normal !important;
            line-break: auto !important;
            transition: none !important;
            animation: none !important;
            -webkit-font-smoothing: antialiased;
            text-rendering: geometricPrecision;
            backface-visibility: hidden;
            text-shadow: none !important;
        }}
        [data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"],
        [data-testid="stSidebar"] [data-testid="stRadio"] [data-baseweb="radio"] {{
            width: 100% !important;
            text-align: left !important;
            border: none !important;
            border-bottom: 1px solid #f0f0f0 !important;
            border-radius: 0 !important;
            padding: 15px 8px !important;
            background: transparent !important;
            box-shadow: none !important;
            color: #333 !important;
            transition: none !important;
            animation: none !important;
        }}
        [data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"] p,
        [data-testid="stSidebar"] [data-testid="stRadio"] [data-baseweb="radio"] p {{
            text-align: left !important;
            font-weight: 500 !important;
            color: #333 !important;
            font-size: 0.96rem !important;
            margin: 0 !important;
            word-break: normal !important;
            overflow-wrap: break-word !important;
            white-space: normal !important;
            transition: none !important;
            animation: none !important;
            -webkit-font-smoothing: antialiased;
            text-rendering: geometricPrecision;
            backface-visibility: hidden;
        }}
        [data-testid="collapsedControl"],
        [data-testid="stSidebarCollapsedControl"],
        button[aria-label*="sidebar"],
        button[aria-label*="Sidebar"] {{
            display: flex !important;
            visibility: visible !important;
            opacity: 1 !important;
            position: fixed !important;
            top: calc(0.55rem + env(safe-area-inset-top));
            left: 0.65rem !important;
            right: auto !important;
            z-index: 100002 !important;
            direction: ltr !important;
        }}
        [data-testid="collapsedControl"] button,
        [data-testid="stSidebarCollapsedControl"] button,
        button[aria-label*="sidebar"],
        button[aria-label*="Sidebar"] {{
            width: 44px !important;
            height: 44px !important;
            border-radius: 12px !important;
            border: 1px solid rgba(203, 213, 225, 0.9) !important;
            background: rgba(255, 255, 255, 0.92) !important;
            box-shadow: 0 6px 14px rgba(15, 23, 42, 0.16) !important;
            direction: ltr !important;
            unicode-bidi: plaintext !important;
        }}
        /* ── KPI cards: compact 2x2 grid on mobile ── */
        .pm-metric-grid {{
            grid-template-columns: repeat(2, minmax(0, 1fr)) !important;
            gap: 0.3rem !important;
            margin: 0.2rem 0 0.5rem 0 !important;
        }}
        .pm-card, [data-testid="stMetric"] {{
            border-radius: 8px !important;
            padding: 0.32rem 0.42rem !important;
            box-shadow: 0 2px 6px rgba(15, 23, 42, 0.07) !important;
        }}
        [data-testid="stMetric"] {{
            min-height: 92px !important;
            height: 100% !important;
        }}
        [data-testid="stMetric"] > div {{
            min-height: 100% !important;
            display: flex !important;
            flex-direction: column !important;
            justify-content: space-between !important;
        }}
        .pm-card .pm-title {{font-size: 0.62rem !important; margin-bottom: 0.15rem !important;}}
        .pm-card .pm-value {{font-size: 0.82rem !important; line-height: 1.1 !important;}}
        .pm-card .pm-delta {{font-size: 0.58rem !important; margin-top: 0.1rem !important;}}
        [data-testid="stMetricValue"] {{font-size: 0.82rem !important; white-space: nowrap !important;}}
        [data-testid="stMetricLabel"] {{font-size: 0.62rem !important; line-height: 1.1 !important; min-height: 1.25rem !important;}}
        [data-testid="stMetricDelta"] {{font-size: 0.58rem !important; min-height: 0.9rem !important;}}
        [data-testid="stDataFrame"] {{overflow-x: auto !important; -ms-overflow-style: none; scrollbar-width: none;}}
        [data-testid="stDataFrame"]::-webkit-scrollbar {{display: none !important;}}
        [data-testid="stDataFrame"] table {{font-size: 12px !important;}}
        [data-testid="stDataFrame"] th, [data-testid="stDataFrame"] td {{padding: 0.28rem 0.36rem !important;}}
        /* ── Hide Plotly toolbar on mobile (saves space, no useful action for touch) ── */
        .modebar-container, .modebar {{ display: none !important; }}
        /* ── Fix scroll locking when touching charts/tables on mobile ── */
        [data-testid="stPlotlyChart"] {{
            touch-action: pan-y pinch-zoom !important;
            -webkit-overflow-scrolling: touch !important;
        }}
        [data-testid="stPlotlyChart"] .plotly .drag {{
            touch-action: pan-y pinch-zoom !important;
        }}
        .js-plotly-plot,
        .js-plotly-plot .plotly,
        .js-plotly-plot .plotly .draglayer,
        .js-plotly-plot .plotly .nsewdrag {{
            touch-action: pan-y pinch-zoom !important;
        }}
        [data-testid="stDataFrame"] {{
            touch-action: pan-x pan-y !important;
            -webkit-overflow-scrolling: touch !important;
            max-height: 60vh !important;
        }}
        /* ── Prevent iframe charts from capturing scroll ── */
        iframe {{
            pointer-events: auto !important;
            touch-action: pan-y !important;
        }}
        /* ── Dark-mode-aware hamburger button ── */
        [data-testid="collapsedControl"] button,
        [data-testid="stSidebarCollapsedControl"] button,
        button[aria-label*="sidebar"],
        button[aria-label*="Sidebar"] {{
            background: {hamburger_bg} !important;
            border: 1px solid {hamburger_border} !important;
        }}
        /* ── Compact page title on mobile ── */
        .app-main-title {{ font-size: 1.9rem !important; margin-bottom: 0 !important; }}
        .app-sub-title {{ font-size: 0.82rem !important; margin-bottom: 0.12rem !important; }}
        .app-logo-icon svg {{ width: 34px !important; height: 34px !important; }}
        .app-logo-badge {{ font-size: 0.6rem !important; padding: 1px 7px !important; }}
        /* ── Touch-optimized button height (Apple HIG: 44px min tap target) ── */
        button[data-testid="baseButton-secondary"],
        button[data-testid="baseButton-primary"],
        [data-testid="stFormSubmitButton"] button {{
            min-height: 44px !important;
        }}
        /* ══════════════════════════════════════════════════════════════════
           PAGE SELECTOR RADIO → INLINE TOP SEGMENTED CONTROL
           Renders above the title. :has() Chrome 105+, Safari 15.4+.
        ══════════════════════════════════════════════════════════════════ */
        /* Force the element container holding the page nav to full width */
        [data-testid="stElementContainer"]:has([data-testid="stRadio"] [data-baseweb="radio"]:nth-child(4):last-child) {{
            position: fixed !important;
            top: calc(0.55rem + env(safe-area-inset-top)) !important;
            left: 50% !important;
            right: auto !important;
            transform: translateX(-50%) !important;
            width: clamp(220px, 64vw, 340px) !important;
            max-width: calc(100vw - 8.8rem) !important;
            z-index: 100001 !important;
            margin: 0 !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child) {{
            background: {nav_bg} !important;
            border: 1px solid {nav_border} !important;
            border-radius: 12px !important;
            padding: 3px !important;
            margin: 0 !important;
            width: 100% !important;
            box-sizing: border-box !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-testid="stWidgetLabel"] {{
            display: none !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [role="radiogroup"] {{
            display: flex !important;
            flex-direction: row !important;
            width: 100% !important;
            margin: 0 !important;
            padding: 0 !important;
            gap: 2px !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"] {{
            flex: 1 !important;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
            padding: 7px 2px !important;
            margin: 0 !important;
            border: none !important;
            border-radius: 9px !important;
            background: transparent !important;
            cursor: pointer !important;
            -webkit-tap-highlight-color: transparent !important;
            transition: background 0.15s ease !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"] svg,
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"] input[type="radio"],
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"] > div:first-child {{
            display: none !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"] p {{
            white-space: nowrap !important;
            font-size: 12px !important;
            font-weight: 500 !important;
            color: {nav_inactive} !important;
            line-height: 1.2 !important;
            text-align: center !important;
            margin: 0 !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"]:has(input:checked) {{
            background: #4f46e5 !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"]:has(input:checked) p {{
            color: #ffffff !important;
            font-weight: 700 !important;
        }}
        /* ── KPI metrics: force 2 columns side-by-side, prevent stacking ── */
        [data-testid="stHorizontalBlock"]:has([data-testid="stMetric"]) {{
            flex-wrap: nowrap !important;
            gap: 0.35rem !important;
        }}
        [data-testid="stHorizontalBlock"]:has([data-testid="stMetric"]) > [data-testid="stColumn"] {{
            flex: 1 1 0% !important;
            min-width: 0 !important;
            max-width: 50% !important;
            width: 50% !important;
        }}
        /* padding-top handled above; no extra override needed */
        .block-container {{
            padding-top: calc(4.4rem + env(safe-area-inset-top)) !important;
        }}
        /* ══ RTL FIX: sidebar expander arrow aligns on the right side (RTL layout) ══ */
        [data-testid="stSidebar"] details[data-testid="stExpander"] > summary,
        [data-testid="stSidebar"] div[data-testid="stExpander"] > details > summary {{
            display: flex !important;
            flex-direction: row-reverse !important;
            justify-content: space-between !important;
            align-items: center !important;
            direction: ltr !important;
        }}
        [data-testid="stSidebar"] [data-testid="stExpander"] svg {{
            transition: transform 180ms ease;
        }}
        /* ══ HEATMAP: horizontal scroll container on mobile ══ */
        [data-testid="stPlotlyChart"] {{
            overflow-x: auto !important;
            -webkit-overflow-scrolling: touch !important;
        }}
        /* ══════════════════════════════════════════════════════════════════ */
    }}

    # Sidebar customizations
    <style>
    /* Force the Streamlit sidebar to stay on the LEFT even in Hebrew (RTL) mode. */
    section[data-testid="stSidebar"],
    [data-testid="stSidebar"] {{
        left: 0 !important;
        right: auto !important;
        direction: ltr !important;
    }}
    </style>
    """

    # ── Comprehensive dark mode: override ALL Streamlit native elements ──────
    if is_dark:
        dark_bg = "#0f172a"
        dark_bg2 = "#1e293b"
        dark_text = "#f1f5f9"
        dark_muted = "#94a3b8"
        dark_border = "#334155"
        dark_accent = "#6366f1"
        css += f"""
    <style>
    /* ══════════════ COMPREHENSIVE DARK MODE ══════════════ */
    /* Override Streamlit CSS custom properties so native components go dark */
    :root, html {{
        --background-color: {dark_bg} !important;
        --secondary-background-color: {dark_bg2} !important;
        --text-color: {dark_text} !important;
        --font: system-ui !important;
    }}
    html, body {{
        background-color: {dark_bg} !important;
        color: {dark_text} !important;
    }}
    /* Dark bottom nav bar */
    [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child) {{
        background: rgba(15,23,42,0.97) !important;
        border-top-color: rgba(51,65,85,0.8) !important;
    }}
    [data-testid="stApp"] {{
        background-color: {dark_bg} !important;
    }}
    [data-testid="stAppViewContainer"],
    [data-testid="stMainBlockContainer"],
    .block-container,
    .main .block-container {{
        background-color: {dark_bg} !important;
    }}
    [data-testid="stHeader"],
    header[data-testid="stHeader"] {{
        background-color: {dark_bg} !important;
        border-bottom: 1px solid {dark_border} !important;
    }}
    /* General text */
    p, span, label, li,
    [data-testid="stMarkdownContainer"],
    [data-testid="stMarkdownContainer"] * {{
        color: {dark_text} !important;
    }}
    h1, h2, h3, h4, h5, h6 {{
        color: {dark_text} !important;
    }}
    /* Caption / muted text */
    [data-testid="stCaptionContainer"],
    [data-testid="stCaptionContainer"] *,
    small, .stCaption {{
        color: {dark_muted} !important;
    }}
    /* Dividers */
    hr {{
        border-color: {dark_border} !important;
    }}
    /* Input fields */
    input, textarea {{
        background-color: {dark_bg2} !important;
        color: {dark_text} !important;
        border-color: {dark_border} !important;
    }}
    [data-baseweb="input"],
    [data-baseweb="textarea"] {{
        background-color: {dark_bg2} !important;
        border-color: {dark_border} !important;
    }}
    [data-baseweb="input"] input,
    [data-baseweb="textarea"] textarea {{
        background-color: {dark_bg2} !important;
        color: {dark_text} !important;
    }}
    /* Selectbox / Dropdown — the input box itself */
    [data-baseweb="select"] > div,
    [data-baseweb="select"] > div > div {{
        background-color: {dark_bg2} !important;
        border-color: {dark_border} !important;
        color: {dark_text} !important;
    }}
    [data-baseweb="select"] svg {{
        fill: {dark_muted} !important;
    }}
    /* Dropdown popover / portal (rendered at body level) */
    [data-baseweb="popover"],
    [data-baseweb="popover"] > div,
    [data-baseweb="popover"] [data-baseweb="menu"],
    [data-baseweb="menu"],
    [data-baseweb="menu"] ul,
    [data-baseweb="menu"] li,
    [role="listbox"],
    [role="listbox"] li,
    [role="listbox"] [role="option"],
    [data-baseweb="layer"] [data-baseweb="popover"],
    [data-baseweb="layer"] [data-baseweb="menu"],
    [data-baseweb="layer"] ul {{
        background-color: {dark_bg2} !important;
        color: {dark_text} !important;
    }}
    [data-baseweb="menu"] li:hover,
    [data-baseweb="menu"] [aria-selected="true"],
    [role="option"]:hover,
    [role="option"][aria-selected="true"],
    [role="listbox"] li:hover {{
        background-color: {dark_border} !important;
        color: {dark_text} !important;
    }}
    /* Highlighted / focused option */
    [data-baseweb="menu"] li[aria-selected="true"],
    [role="option"][aria-selected="true"] {{
        background-color: rgba(99,102,241,0.25) !important;
        color: {dark_text} !important;
    }}
    /* Tabs */
    [data-baseweb="tab-list"] {{
        background-color: {dark_bg} !important;
        border-bottom: 1px solid {dark_border} !important;
    }}
    [data-baseweb="tab"] {{
        background-color: transparent !important;
        color: {dark_muted} !important;
    }}
    [data-baseweb="tab"][aria-selected="true"] {{
        color: {dark_text} !important;
        border-bottom: 2px solid {dark_accent} !important;
    }}
    [data-baseweb="tab-panel"] {{
        background-color: {dark_bg} !important;
    }}
    /* Expanders */
    [data-testid="stExpander"] {{
        background-color: {dark_bg2} !important;
        border: 1px solid {dark_border} !important;
    }}
    [data-testid="stExpander"] summary,
    [data-testid="stExpander"] summary * {{
        color: {dark_text} !important;
    }}
    /* Expander content */
    [data-testid="stExpander"] > div[data-testid="stExpanderDetails"] {{
        background-color: {dark_bg2} !important;
    }}
    /* Radio buttons */
    [data-testid="stRadio"] label,
    [data-testid="stRadio"] label * {{
        color: {dark_text} !important;
    }}
    /* Checkbox */
    [data-testid="stCheckbox"] label,
    [data-testid="stCheckbox"] label * {{
        color: {dark_text} !important;
    }}
    /* Buttons */
    button[data-testid="baseButton-secondary"],
    button[kind="secondary"] {{
        background-color: {dark_bg2} !important;
        color: {dark_text} !important;
        border-color: {dark_border} !important;
    }}
    button[data-testid="baseButton-primary"],
    button[kind="primary"] {{
        background-color: {dark_accent} !important;
        color: #ffffff !important;
    }}
    /* Number/Date input containers */
    [data-testid="stNumberInput"] > div,
    [data-testid="stDateInput"] > div {{
        background-color: {dark_bg2} !important;
        border-color: {dark_border} !important;
    }}
    [data-testid="stNumberInput"] input,
    [data-testid="stDateInput"] input {{
        background-color: {dark_bg2} !important;
        color: {dark_text} !important;
    }}
    /* Multiselect tags */
    [data-testid="stMultiSelect"] [data-baseweb="tag"] {{
        background-color: {dark_border} !important;
        color: {dark_text} !important;
    }}
    /* Alert boxes */
    [data-testid="stAlert"] {{
        background-color: {dark_bg2} !important;
        border-color: {dark_border} !important;
        color: {dark_text} !important;
    }}
    [data-testid="stAlert"] * {{
        color: {dark_text} !important;
    }}
    /* Code blocks */
    [data-testid="stCodeBlock"] pre,
    code {{
        background-color: {dark_bg2} !important;
        color: {dark_text} !important;
    }}
    /* Spinner */
    [data-testid="stSpinner"] p {{
        color: {dark_muted} !important;
    }}
    /* Hamburger/toolbar buttons in dark mode */
    [data-testid="collapsedControl"] button,
    [data-testid="stSidebarCollapsedControl"] button,
    button[aria-label*="sidebar"],
    button[aria-label*="Sidebar"],
    [data-testid="stToolbar"] button {{
        background-color: rgba(30,41,59,0.92) !important;
        border-color: rgba(100,116,139,0.4) !important;
        color: {dark_text} !important;
    }}
    /* Main menu */
    #MainMenu svg, [data-testid="stToolbar"] svg {{
        fill: {dark_text} !important;
    }}
    /* ── Plotly charts: transparent paper so plotly_dark template shows ── */
    [data-testid="stPlotlyChart"] {{
        background-color: transparent !important;
        border-radius: 12px !important;
        overflow: hidden !important;
    }}
    [data-testid="stPlotlyChart"] > div,
    [data-testid="stPlotlyChart"] > div > div {{
        background-color: transparent !important;
    }}
    [data-testid="stPlotlyChart"] .svg-container {{
        background-color: transparent !important;
    }}
    [data-testid="stPlotlyChart"] .main-svg {{
        background: transparent !important;
    }}
    [data-testid="stPlotlyChart"] .main-svg .bg {{
        fill: transparent !important;
    }}
    /* ── DataFrames: force dark via canvas inversion + DOM overrides ── */
    /* The Glide-Data-Grid renders on <canvas> using Streamlit's compiled
       theme (from config.toml). CSS cannot style canvas pixels, so we
       invert the entire dataframe container and then hue-rotate(180deg)
       to restore original hue (green stays green, red stays red). */
    [data-testid="stDataFrame"] {{
        background-color: transparent !important;
        border-radius: 8px !important;
        overflow: hidden !important;
        filter: invert(0.9) hue-rotate(180deg) !important;
    }}
    /* Undo the inversion for images/icons inside the dataframe */
    [data-testid="stDataFrame"] img {{
        filter: invert(1) hue-rotate(180deg) !important;
    }}
    /* DOM-rendered fallback (HTML tables used by data_editor) */
    [data-testid="stDataFrame"] table {{
        background-color: {dark_bg2} !important;
    }}
    [data-testid="stDataFrame"] table th {{
        background-color: {dark_bg} !important;
        color: {dark_text} !important;
        border-color: {dark_border} !important;
    }}
    [data-testid="stDataFrame"] table td {{
        background-color: {dark_bg2} !important;
        color: {dark_text} !important;
        border-color: {dark_border} !important;
    }}
    [data-testid="stDataFrame"] [role="gridcell"],
    [data-testid="stDataFrame"] [role="rowheader"] {{
        background-color: {dark_bg2} !important;
        color: {dark_text} !important;
        border-color: {dark_border} !important;
    }}
    [data-testid="stDataFrame"] [role="columnheader"] {{
        background-color: {dark_bg} !important;
        color: {dark_text} !important;
        border-color: {dark_border} !important;
    }}
    /* ── Sidebar dark mode ── */
    [data-testid="stSidebar"],
    [data-testid="stSidebar"] > div:first-child,
    [data-testid="stSidebar"] [data-testid="stSidebarContent"] {{
        background-color: {dark_bg2} !important;
        color: {dark_text} !important;
    }}
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] label {{
        color: {dark_text} !important;
    }}
    [data-testid="stSidebar"] [data-baseweb="select"] > div,
    [data-testid="stSidebar"] [data-baseweb="select"] > div > div {{
        background-color: {dark_bg} !important;
        border-color: {dark_border} !important;
        color: {dark_text} !important;
    }}
    /* option_menu in dark sidebar */
    [data-testid="stSidebar"] .nav {{
        background-color: {dark_bg} !important;
        border-radius: 10px !important;
    }}
    [data-testid="stSidebar"] .nav-link {{
        color: {dark_muted} !important;
    }}
    [data-testid="stSidebar"] .nav-link:hover {{
        background-color: {dark_border} !important;
        color: {dark_text} !important;
    }}
    [data-testid="stSidebar"] .nav-link.active,
    [data-testid="stSidebar"] .nav-link-selected {{
        background-color: {dark_border} !important;
        color: {dark_text} !important;
    }}
    /* ── Metrics/KPI cards dark mode ── */
    [data-testid="stMetric"] {{
        background-color: {dark_bg2} !important;
        border-color: {dark_border} !important;
    }}
    [data-testid="stMetricValue"] {{
        color: {dark_text} !important;
    }}
    [data-testid="stMetricLabel"] {{
        color: {dark_muted} !important;
    }}
    [data-testid="stMetricDelta"] svg {{
        fill: currentColor !important;
    }}
    /* ── Form containers ── */
    [data-testid="stForm"] {{
        background-color: {dark_bg2} !important;
        border-color: {dark_border} !important;
    }}
    /* ── Selectbox label text ── */
    [data-testid="stSelectbox"] label,
    [data-testid="stMultiSelect"] label,
    [data-testid="stTextInput"] label,
    [data-testid="stNumberInput"] label,
    [data-testid="stDateInput"] label {{
        color: {dark_text} !important;
    }}
    /* ── Fullscreen button ── */
    button[title="View fullscreen"] {{
        color: {dark_muted} !important;
    }}
    /* ── Scrollbar dark ── */
    ::-webkit-scrollbar {{
        width: 8px;
        height: 8px;
    }}
    ::-webkit-scrollbar-track {{
        background: {dark_bg} !important;
    }}
    ::-webkit-scrollbar-thumb {{
        background: {dark_border} !important;
        border-radius: 4px;
    }}
    ::-webkit-scrollbar-thumb:hover {{
        background: {dark_muted} !important;
    }}
    /* ══════════════ END DARK MODE ══════════════ */
    </style>
    """

    st.markdown(css, unsafe_allow_html=True)


def inject_dark_dropdown_fix(is_dark: bool) -> None:
    """Inject dark-mode styles for dropdown popovers AND sidebar into the parent document.

    Streamlit's BaseWeb selectbox dropdown is rendered as a portal at the
    document root level.  CSS injected via st.markdown lives inside the
    Streamlit container and cannot override Styletron's inline styles on
    the portal.  This function uses a tiny JS snippet to plant a <style>
    tag directly in the parent document <head> with high-specificity rules.
    """
    if not is_dark:
        # In light mode, remove any leftover dark style tags, timers, and forced inline styles.
        components.html(
            """<script>(function(){
              try {
                var d = window.parent ? window.parent.document : document;
                // Remove injected dark style tags
                ['pm-dark-dropdown','pm-dark-sidebar'].forEach(function(id){
                  var s = d.getElementById(id);
                  if (s) s.remove();
                });
                // Clear the dark-mode sidebar timer so it stops running
                if (d.__pmSidebarDarkTimer) {
                  clearInterval(d.__pmSidebarDarkTimer);
                  d.__pmSidebarDarkTimer = null;
                }
                // Remove forced inline styles from all sidebar elements
                var sidebar = d.querySelector('[data-testid="stSidebar"]');
                if (sidebar) {
                  sidebar.style.removeProperty('background');
                  sidebar.style.removeProperty('background-color');
                  var all = sidebar.querySelectorAll('*');
                  for (var i = 0; i < all.length; i++) {
                    try {
                      all[i].style.removeProperty('background');
                      all[i].style.removeProperty('background-color');
                      all[i].style.removeProperty('color');
                    } catch(e2){}
                  }
                  // Also clean inside iframes
                  var iframes = sidebar.querySelectorAll('iframe');
                  for (var j = 0; j < iframes.length; j++) {
                    try {
                      var iDoc = iframes[j].contentDocument || (iframes[j].contentWindow && iframes[j].contentWindow.document);
                      if (!iDoc) continue;
                      var st2 = iDoc.getElementById('pm-inner-dark');
                      if (st2) st2.remove();
                      if (iDoc.body) {
                        iDoc.body.style.removeProperty('background');
                        iDoc.body.style.removeProperty('background-color');
                        iDoc.body.style.removeProperty('color');
                      }
                      var iAll = iDoc.querySelectorAll('*');
                      for (var k = 0; k < iAll.length; k++) {
                        try {
                          iAll[k].style.removeProperty('background');
                          iAll[k].style.removeProperty('background-color');
                          iAll[k].style.removeProperty('color');
                        } catch(e4){}
                      }
                    } catch(e3){}
                  }
                }
              } catch(e){}
            })();</script>""",
            height=0, width=0,
        )
        return

    dark_bg2 = "#1e293b"
    dark_text = "#f1f5f9"
    dark_border = "#334155"
    dark_accent = "rgba(99,102,241,0.25)"
    dark_muted = "#94a3b8"
    sidebar_dark = "#1E1E1E"
    sidebar_dark2 = "#161616"

    dropdown_css = f"""
[data-baseweb="popover"] {{
  background-color: {dark_bg2} !important;
}}
[data-baseweb="popover"] > div {{
  background-color: {dark_bg2} !important;
  border-color: {dark_border} !important;
}}
[data-baseweb="menu"],
[data-baseweb="menu"] ul {{
  background-color: {dark_bg2} !important;
}}
[data-baseweb="menu"] li,
[role="option"],
[role="listbox"] li {{
  background-color: {dark_bg2} !important;
  color: {dark_text} !important;
}}
[data-baseweb="menu"] li:hover,
[role="option"]:hover,
[role="listbox"] li:hover {{
  background-color: {dark_border} !important;
  color: {dark_text} !important;
}}
[data-baseweb="menu"] li[aria-selected="true"],
[role="option"][aria-selected="true"] {{
  background-color: {dark_accent} !important;
  color: {dark_text} !important;
}}
[data-baseweb="select"] svg {{
  fill: {dark_muted} !important;
}}
/* Force all portal-level popover layers */
body > div[data-baseweb="layer"] {{
  z-index: 999999 !important;
}}
body > div[data-baseweb="layer"] [data-baseweb="popover"],
body > div[data-baseweb="layer"] [data-baseweb="popover"] > div,
body > div[data-baseweb="layer"] ul,
body > div[data-baseweb="layer"] li {{
  background-color: {dark_bg2} !important;
  color: {dark_text} !important;
}}
body > div[data-baseweb="layer"] li:hover {{
  background-color: {dark_border} !important;
}}
body > div[data-baseweb="layer"] li[aria-selected="true"] {{
  background-color: {dark_accent} !important;
}}
"""
    sidebar_css = f"""
/* ══ DARK MODE SIDEBAR — desktop fix ══════════════════════════════ */
section[data-testid="stSidebar"],
[data-testid="stSidebar"] {{
  background: {sidebar_dark} !important;
  background-color: {sidebar_dark} !important;
}}
[data-testid="stSidebar"] > div,
[data-testid="stSidebar"] > div > div,
[data-testid="stSidebar"] > div > div > div,
[data-testid="stSidebar"] [data-testid="stSidebarContent"],
[data-testid="stSidebar"] [data-testid="stSidebarContent"] > div,
[data-testid="stSidebar"] [data-testid="stSidebarUserContent"],
[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] > div {{
  background: {sidebar_dark} !important;
  background-color: {sidebar_dark} !important;
  color: {dark_text} !important;
}}
/* Catch any remaining plain white div inside sidebar */
[data-testid="stSidebar"] div[style*="background-color: white"],
[data-testid="stSidebar"] div[style*="background: white"],
[data-testid="stSidebar"] div[style*="background-color: rgb(255"],
[data-testid="stSidebar"] div[style*="background: rgb(255"],
[data-testid="stSidebar"] div[style*="background-color:#fff"],
[data-testid="stSidebar"] div[style*="background:#fff"] {{
  background: {sidebar_dark} !important;
  background-color: {sidebar_dark} !important;
}}
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span:not([data-testid="stMetricDelta"]) {{
  color: {dark_text} !important;
}}
"""
    # Escape for JS string
    escaped_dropdown = dropdown_css.replace("\\", "\\\\").replace("`", "\\`").replace("$", "\\$")
    escaped_sidebar = sidebar_css.replace("\\", "\\\\").replace("`", "\\`").replace("$", "\\$")

    components.html(
        f"""<script>(function(){{
          try {{
            var d = window.parent ? window.parent.document : document;
            // Dropdown dark style
            var s = d.getElementById('pm-dark-dropdown');
            if (!s) {{
              s = d.createElement('style');
              s.id = 'pm-dark-dropdown';
              d.head.appendChild(s);
            }}
            s.textContent = `{escaped_dropdown}`;
            // Sidebar dark style
            var sb = d.getElementById('pm-dark-sidebar');
            if (!sb) {{
              sb = d.createElement('style');
              sb.id = 'pm-dark-sidebar';
              d.head.appendChild(sb);
            }}
            sb.textContent = `{escaped_sidebar}`;
            // ── JS inline-style override: walk all sidebar children and force dark bg ──
            function forceSidebarDark() {{
              try {{
                var sidebar = d.querySelector('[data-testid="stSidebar"]');
                if (!sidebar) return;
                var DARK_BG = '{sidebar_dark}';
                var TEXT = '{dark_text}';
                var BORDER = '{dark_border}';
                // Force the sidebar root itself
                sidebar.style.setProperty('background', DARK_BG, 'important');
                sidebar.style.setProperty('background-color', DARK_BG, 'important');
                // Walk ALL descendants and clear white backgrounds
                var all = sidebar.querySelectorAll('*');
                for (var i = 0; i < all.length; i++) {{
                  var el = all[i];
                  var tag = el.tagName ? el.tagName.toLowerCase() : '';
                  // Skip interactive / styled elements that should keep their own bg
                  if (tag === 'button' || tag === 'input' || tag === 'select' || tag === 'textarea') continue;
                  if (el.getAttribute && el.getAttribute('data-baseweb')) continue;
                  // Only force elements that appear visually white/light
                  var cs = window.getComputedStyle(el);
                  var bg = cs.backgroundColor;
                  if (!bg || bg === 'transparent' || bg === 'rgba(0, 0, 0, 0)') continue;
                  // Check if it's light (r+g+b > 500 approximation)
                  var rgb = bg.match(/[0-9]+/g);
                  if (rgb && rgb.length >= 3) {{
                    var brightness = parseInt(rgb[0]) + parseInt(rgb[1]) + parseInt(rgb[2]);
                    if (brightness > 500) {{
                      el.style.setProperty('background', DARK_BG, 'important');
                      el.style.setProperty('background-color', DARK_BG, 'important');
                    }}
                  }}
                }}
              }} catch(e) {{}}
            }}
            forceSidebarDark();

            // ── Pierce into component iframes inside the sidebar ──
            function forceSidebarIframes() {{
              try {{
                var sidebar = d.querySelector('[data-testid="stSidebar"]');
                if (!sidebar) return;
                var DARK_BG = '{sidebar_dark}';
                var TEXT = '{dark_text}';
                var iframes = sidebar.querySelectorAll('iframe');
                for (var i = 0; i < iframes.length; i++) {{
                  try {{
                    var iDoc = iframes[i].contentDocument || (iframes[i].contentWindow && iframes[i].contentWindow.document);
                    if (!iDoc || !iDoc.body) continue;
                    // Force iframe body background
                    iDoc.body.style.setProperty('background', DARK_BG, 'important');
                    iDoc.body.style.setProperty('background-color', DARK_BG, 'important');
                    iDoc.body.style.setProperty('color', TEXT, 'important');
                    // Inject a persistent style tag inside the iframe
                    var styleId = 'pm-inner-dark';
                    if (!iDoc.getElementById(styleId)) {{
                      var st2 = iDoc.createElement('style');
                      st2.id = styleId;
                      st2.textContent = 'html,body{{background:{sidebar_dark}!important;background-color:{sidebar_dark}!important;color:{dark_text}!important;}} .nav,.nav-pills,.stComponent{{background:{sidebar_dark}!important;}} .nav-link{{color:{dark_text}!important;}}';
                      (iDoc.head || iDoc.body).appendChild(st2);
                    }}
                    // Walk all elements in the iframe and force dark on light backgrounds
                    var all = iDoc.querySelectorAll('*');
                    var iWin = iframes[i].contentWindow;
                    for (var j = 0; j < all.length; j++) {{
                      try {{
                        var el = all[j];
                        var tag = (el.tagName || '').toLowerCase();
                        if (tag === 'button' || tag === 'input' || tag === 'select' || tag === 'textarea') continue;
                        var cs = iWin.getComputedStyle(el);
                        var bg = cs.backgroundColor;
                        if (!bg || bg === 'transparent' || bg === 'rgba(0, 0, 0, 0)') continue;
                        var rgb = bg.match(/[0-9]+/g);
                        if (rgb && rgb.length >= 3 && (parseInt(rgb[0]) + parseInt(rgb[1]) + parseInt(rgb[2])) > 500) {{
                          el.style.setProperty('background', DARK_BG, 'important');
                          el.style.setProperty('background-color', DARK_BG, 'important');
                        }}
                      }} catch(e3) {{}}
                    }}
                  }} catch(e2) {{}}
                }}
              }} catch(e) {{}}
            }}
            forceSidebarIframes();

            // Keep re-running so Streamlit rerenders don't undo the fix
            if (!d.__pmSidebarDarkTimer) {{
              d.__pmSidebarDarkTimer = setInterval(function(){{ forceSidebarDark(); forceSidebarIframes(); }}, 800);
            }}
          }} catch(e){{}}
        }})();</script>""",
        height=0, width=0,
    )


def inject_client_fixes() -> None:
    # Small runtime fixes for mobile UX and branding noise.
    components.html(
        """
        <script>
        (function () {
          const hideCss = `
            footer, footer *, [data-testid="stFooter"], [data-testid="stFooter"] *,
            [data-testid="stAppCreator"], [data-testid="stAppCreator"] *,
            [data-testid="stDeployButton"], [data-testid="stStatusWidget"] {
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
              '[data-testid="stDeployButton"]',
              '[data-testid="stStatusWidget"]'
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
            // Try both BaseWeb and modern Streamlit selectors
            const lists = Array.from(rootDoc.querySelectorAll(
              '[data-baseweb="tab-list"], [role="tablist"], [data-testid="stTabs"] > div:first-child'
            ));
            const he = ['סקירה', 'חלוקה', 'דוחות', 'סך הפקדות', 'עסקאות'];
            const en = ['overview', 'allocation', 'reports', 'total deposits', 'transactions'];

            for (const list of lists) {
              const tabs = Array.from(list.querySelectorAll(
                '[data-baseweb="tab"], [role="tab"], button[data-testid="stTab"]'
              ));
              if (tabs.length < 3) continue;
              const labels = tabs.map((tab) => (tab.innerText || '').trim().toLowerCase());
              const heCount = he.filter((name) => labels.some((lbl) => lbl.indexOf(name) >= 0)).length;
              const enCount = en.filter((name) => labels.some((lbl) => lbl.indexOf(name) >= 0)).length;
              if (heCount >= 3 || enCount >= 3) return list;
            }
            return null;
          }

          function setupTabSwipe() {
            // Bind once per session — listeners survive Streamlit rerenders
            if (rootWin.__pmSwipeBound) return;

            // Find the main content tab-list (st.tabs with 3+ items)
            function getContentTabList() {
              const lists = Array.from(rootDoc.querySelectorAll('[data-baseweb="tab-list"]'));
              for (const list of lists) {
                const tabs = Array.from(list.querySelectorAll('[data-baseweb="tab"]'));
                if (tabs.length >= 3) return { list, tabs };
              }
              return null;
            }

            const blockedSelector = [
              'input', 'textarea', 'select',
              '.js-plotly-plot',
              '[data-testid="stDataFrame"]',
              '[data-testid="stTable"]',
              '[data-testid="stDataEditor"]',
              'iframe',
              '[data-no-swipe]'
            ].join(',');

            let startX = 0, startY = 0, locked = false;

            rootDoc.addEventListener('touchstart', (e) => {
              locked = false;
              if (!e.touches || e.touches.length !== 1) return;
              const tgt = e.target;
              if (tgt && tgt.closest && tgt.closest(blockedSelector)) return;
              if (!getContentTabList()) return;
              startX = e.touches[0].clientX;
              startY = e.touches[0].clientY;
              locked = true;
            }, { passive: true });

            rootDoc.addEventListener('touchend', (e) => {
              if (!locked) return;
              locked = false;
              if (!e.changedTouches || !e.changedTouches.length) return;
              const dx = e.changedTouches[0].clientX - startX;
              const dy = e.changedTouches[0].clientY - startY;
              // Must be >55px horizontal and more horizontal than vertical
              if (Math.abs(dx) < 55 || Math.abs(dy) > Math.abs(dx) * 0.65) return;

              const found = getContentTabList();
              if (!found) return;
              const { tabs } = found;

              const activeIdx = tabs.findIndex(t => t.getAttribute('aria-selected') === 'true');
              if (activeIdx < 0) return;

              // dx < 0 = swipe left = next tab; dx > 0 = swipe right = prev tab
              const nextIdx = dx < 0 ? activeIdx + 1 : activeIdx - 1;
              if (nextIdx >= 0 && nextIdx < tabs.length) {
                tabs[nextIdx].click();
                tabs[nextIdx].scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' });
              }
            }, { passive: true });

            rootWin.__pmSwipeBound = true;
          }

          // ═══════════════════════════════════════════════
          // SIDEBAR LEFT-ANCHOR  +  SMOOTH ANIMATION
          // Directly patches the parent document so the sidebar
          // always stays on the LEFT even in Hebrew (RTL) mode.
          // ═══════════════════════════════════════════════
          function fixSidebarLeft() {
            try {
              const d = rootDoc;

              // 1. Force <html> to LTR — this is the "English trick".
              //    Streamlit's flex layout reads direction from the HTML root.
              //    Setting dir="ltr" (even in Hebrew mode) keeps sidebar on LEFT.
              //    Hebrew text still renders RTL correctly via Unicode BiDi.
              const htmlEl = d.documentElement;
              if (htmlEl) {
                if (htmlEl.getAttribute('dir') !== 'ltr') htmlEl.setAttribute('dir', 'ltr');
                if (htmlEl.getAttribute('lang') !== 'en')  htmlEl.setAttribute('lang', 'en');
              }

              // 2. Force stApp (Streamlit layout root) to LTR flex direction.
              const appEl = d.querySelector('[data-testid="stApp"]');
              if (appEl) {
                appEl.style.setProperty('direction', 'ltr', 'important');
                appEl.style.setProperty('flex-direction', 'row', 'important');
              }

              // 3. Force sidebar inline position so it's always anchored left.
              const sidebar = d.querySelector('section[data-testid="stSidebar"], [data-testid="stSidebar"]');
              if (sidebar) {
                sidebar.style.setProperty('left', '0', 'important');
                sidebar.style.setProperty('right', 'auto', 'important');
                sidebar.style.setProperty('direction', 'ltr', 'important');
              }

              // 4. Inject a persistent <style> into the parent document <head>
              //    with !important overrides – survives Streamlit rerenders.
              let pmStyle = d.getElementById('pm-sidebar-ltr-fix');
              if (!pmStyle) {
                pmStyle = d.createElement('style');
                pmStyle.id = 'pm-sidebar-ltr-fix';
                d.head.appendChild(pmStyle);
              }
              const css = [
                'html, body {',
                '  direction: ltr !important;',
                '}',
                '[data-testid="stApp"] {',
                '  direction: ltr !important;',
                '  flex-direction: row !important;',
                '}',
                'section[data-testid="stSidebar"],',
                '[data-testid="stSidebar"] {',
                '  left: 0 !important;',
                '  right: auto !important;',
                '  direction: ltr !important;',
                '  will-change: transform, opacity;',
                '  transition: transform 0.42s cubic-bezier(0.16,1,0.3,1),',
                '              opacity 0.30s ease-out !important;',
                '}',
                'section[data-testid="stSidebar"][aria-expanded="false"],',
                '[data-testid="stSidebar"][aria-expanded="false"] {',
                '  transform: translateX(-100%) !important;',
                '  opacity: 0 !important;',
                '}',
                'section[data-testid="stSidebar"][aria-expanded="true"],',
                '[data-testid="stSidebar"][aria-expanded="true"] {',
                '  transform: translateX(0) !important;',
                '  opacity: 1 !important;',
                '}',
              ].join('\\n');
              if (pmStyle.textContent !== css) pmStyle.textContent = css;
            } catch (e) { /* cross-origin iframe – skip */ }
          }

          function forceToolbarVisible() {
            try {
              // Inject a persistent style into parent doc to force toolbar/3-dot menu visible
              let pmToolbar = rootDoc.getElementById('pm-toolbar-fix');
              if (!pmToolbar) {
                pmToolbar = rootDoc.createElement('style');
                pmToolbar.id = 'pm-toolbar-fix';
                rootDoc.head.appendChild(pmToolbar);
              }
              pmToolbar.textContent = `
                /* Mobile: keep header visible (toolbar/hamburger needed) */
                @media (max-width: 767px) {
                  [data-testid="stHeader"],
                  header[data-testid="stHeader"] {
                    overflow: visible !important;
                    display: block !important;
                    height: auto !important;
                  }
                }
                /* Desktop: header goes transparent & zero-height, but toolbar
                   (3-dots Main Menu) stays tappable. Title hugs the top. */
                @media (min-width: 768px) {
                  [data-testid="stHeader"],
                  header[data-testid="stHeader"] {
                    background: transparent !important;
                    box-shadow: none !important;
                    border: none !important;
                    height: 0 !important;
                    min-height: 0 !important;
                    overflow: visible !important;
                    position: fixed !important;
                    top: 0 !important; right: 0 !important; left: 0 !important;
                    z-index: 999999 !important;
                    pointer-events: none !important;
                  }
                  header[data-testid="stHeader"] button,
                  header[data-testid="stHeader"] [data-testid="stToolbar"],
                  header[data-testid="stHeader"] [data-testid="stToolbarActions"],
                  header[data-testid="stHeader"] [data-testid="stMainMenuButton"] {
                    pointer-events: auto !important;
                  }
                  section.main > div.block-container,
                  [data-testid="stMainBlockContainer"],
                  .main .block-container,
                  .block-container {
                    padding-top: 0 !important;
                    margin-top: 0 !important;
                  }
                }
                [data-testid="stToolbar"] {
                  display: flex !important;
                  visibility: visible !important;
                  opacity: 1 !important;
                  overflow: visible !important;
                  z-index: 100010 !important;
                  pointer-events: auto !important;
                }
                [data-testid="stToolbarActions"] {
                  display: flex !important;
                  visibility: visible !important;
                  opacity: 1 !important;
                  overflow: visible !important;
                  z-index: 100010 !important;
                  pointer-events: auto !important;
                }
                [data-testid="stMainMenuButton"],
                [data-testid="stToolbarActions"] button,
                [data-testid="stToolbar"] button {
                  display: flex !important;
                  visibility: visible !important;
                  opacity: 1 !important;
                  pointer-events: auto !important;
                }
              `;
            } catch(e) {}
          }

          function makeWatchlistExpandUp() {
            try {
              const markers = ['tradingview watchlist', 'רשימת מעקב tradingview'];
              const expanders = Array.from(rootDoc.querySelectorAll('[data-testid="stExpander"]'));
              expanders.forEach((expander) => {
                const summary = expander.querySelector('summary');
                if (!summary) return;
                const label = (summary.innerText || '').trim().toLowerCase();
                if (!markers.some((m) => label.indexOf(m) >= 0)) return;

                const details = summary.closest('details') || expander.querySelector('details');
                if (!details) return;
                details.style.display = 'flex';
                details.style.flexDirection = 'column-reverse';
                details.style.alignItems = 'stretch';
              });
            } catch (e) {
              // Ignore non-critical DOM patch issues.
            }
          }

          // ═══════════════════════════════════════════════════════════
          // UNDEFINED TEXT CLEANER
          // Removes any bare "undefined" text nodes that may appear as
          // artefacts from 0-height component iframes or JS timing.
          // ═══════════════════════════════════════════════════════════
          function removeUndefinedText() {
            try {
              var walker = rootDoc.createTreeWalker(
                rootDoc.body,
                NodeFilter.SHOW_TEXT,
                null,
                false
              );
              var toBlank = [];
              var node;
              while ((node = walker.nextNode())) {
                if (node.nodeValue && node.nodeValue.trim() === 'undefined') {
                  toBlank.push(node);
                }
              }
              toBlank.forEach(function(n) { n.nodeValue = ''; });
            } catch(e) {}
          }

          function run() {
            removeBranding();
            setupTabSwipe();
            fixSidebarLeft();
            makeWatchlistExpandUp();
            forceToolbarVisible();
            removeUndefinedText();
          }

          run();
          if (!rootWin._pmObserverAttached) {
            const obs = new MutationObserver(run);
            obs.observe(rootDoc.body, { childList: true, subtree: true });
            rootWin._pmTimerBranding = rootWin.setInterval(removeBranding, 1200);
            rootWin._pmTimerSidebar = rootWin.setInterval(fixSidebarLeft, 800);
            rootWin._pmTimerToolbar = rootWin.setInterval(forceToolbarVisible, 1500);
            rootWin._pmTimerUndef = rootWin.setInterval(removeUndefinedText, 600);
            rootWin._pmTimerHide = window.setInterval(function () {
              injectHideStyle(document);
              if (rootDoc !== document) injectHideStyle(rootDoc);
            }, 2000);
            rootWin._pmObserverAttached = true;
          }
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
    display_cost_per_unit: float
    display_currency: str


def _normalize_currency_code(value: object) -> str:
    raw = _clean(value).upper()
    if raw in {"", "NAN"}:
        return ""
    if raw in {"ILS", "NIS", "₪", "שח", 'ש"ח'}:
        return "ILS"
    if raw in {"USD", "$"}:
        return "USD"
    return raw


def _infer_display_currency(ticker: str, origin_currency: object) -> str:
    base = _normalize_currency_code(origin_currency)
    if ticker in {"BTC", "ETH", "SOL"}:
        return "USD"
    return base or "ILS"


def _format_currency_value(value: float, currency: str) -> str:
    cur = _normalize_currency_code(currency)
    if cur == "USD":
        return f"${value:,.2f}"
    if cur == "ILS":
        return f"{value:,.2f} ₪"
    if cur:
        return f"{value:,.2f} {cur}"
    return f"{value:,.2f}"


def _mix_he_with_ltr(token: str) -> str:
    # Isolate Latin tokens so bidi layout stays stable inside Hebrew text.
    return f"\u2066{token}\u2069"


def _tradingview_symbol(ticker: object) -> str:
    t = _clean(ticker).upper()
    if not t:
        return ""
    if ":" in t:
        return t
    if t in {"BTC", "ETH", "SOL"}:
        return f"BINANCE:{t}USDT"
    if re.match(r"^[A-Z0-9._-]+$", t):
        return f"NASDAQ:{t}"
    return t


def _parse_followed_symbols(raw: object) -> List[str]:
    text = _clean(raw)
    if not text:
        return []
    parts = re.split(r"[\s,;\n\r]+", text)
    out: List[str] = []
    seen = set()
    for p in parts:
        sym = _clean(p).upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
    return out


def _tradingview_chart_url(ticker: object) -> str:
    t = _clean(ticker).upper()
    symbol = _tradingview_symbol(t)
    if not symbol:
        return ""
    return f"https://www.tradingview.com/chart/?symbol={symbol.replace(':', '%3A')}&pm_ticker={t}"


def _internal_chart_link(ticker: object) -> str:
    t = _clean(ticker).upper()
    if not t:
        return ""
    safe = urlparse.quote(t)
    return f"?tv_ticker={safe}&pm_ticker={safe}"


def _get_query_param(name: str) -> str:
    try:
        if hasattr(st, "query_params"):
            val = st.query_params.get(name, "")
            if isinstance(val, list):
                return _clean(val[0]) if val else ""
            return _clean(val)
    except Exception:
        pass
    try:
        values = st.experimental_get_query_params().get(name, [])
        return _clean(values[0]) if values else ""
    except Exception:
        return ""


def _clear_query_param(name: str) -> None:
    try:
        if hasattr(st, "query_params"):
            if name in st.query_params:
                del st.query_params[name]
            return
    except Exception:
        pass
    try:
        q = st.experimental_get_query_params()
        if name in q:
            q.pop(name, None)
            st.experimental_set_query_params(**q)
    except Exception:
        return


def _render_tradingview_widget(ticker: object, height: int = 560, theme: str = "dark") -> None:
    """Render the TradingView Advanced Chart widget.

    Two implementation details that matter:
      1. **Theme toggle**: TradingView's embed script keeps state per iframe.
         If we keep the same DOM container ID, the script may NOT re-init
         on a theme change. We force a fresh iframe by giving the container
         a unique id that includes the theme + a cache-buster timestamp.
      2. **MA 150 & MA 50**: the embed widget supports MA via `studies`
         array of strings (not objects) — the format
         `"MASimple@tv-basicstudies"` loads a simple moving average.
         Multiple SMAs are supported by repeating the entry. Lengths are
         tuned via `studies_overrides`. The two MAs end up named
         `moving average` and `moving average #1` in the override scope.
    """
    symbol = _tradingview_symbol(ticker)
    if not symbol:
        st.info("No chart symbol")
        return
    tv_theme = "dark" if theme == "dark" else "light"

    # Cache-bust id forces TradingView's embed script to remount on every render.
    # This is what makes the Light/Dark toggle actually take effect.
    cache_key = f"{tv_theme}-{int(time.time() * 1000)}"
    container_id = f"tv-widget-{abs(hash(cache_key)) % (10**9)}"

    bg_outer = "#0b1220" if tv_theme == "dark" else "#ffffff"

    widget_html = f"""
    <!-- tv-cache-bust:{cache_key} -->
    <div id="{container_id}" class="tradingview-widget-container"
         style="height:{height}px;width:100%;background:{bg_outer};border-radius:10px;overflow:hidden">
      <div class="tradingview-widget-container__widget"
           style="height:{height}px;width:100%"></div>
      <script type="text/javascript"
        src="https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js?v={cache_key}" async>
      {{
        "autosize": true,
        "width": "100%",
        "height": "{height}",
        "symbol": "{symbol}",
        "interval": "D",
        "timezone": "Etc/UTC",
        "theme": "{tv_theme}",
        "style": "1",
        "locale": "en",
        "toolbar_bg": "{bg_outer}",
        "allow_symbol_change": true,
        "save_image": true,
        "hide_side_toolbar": false,
        "withdateranges": true,
        "details": false,
        "calendar": false,
        "show_popup_button": true,
        "popup_width": "1000",
        "popup_height": "650",
        "support_host": "https://www.tradingview.com",
        "studies": [
          "MASimple@tv-basicstudies",
          "MASimple@tv-basicstudies"
        ],
        "studies_overrides": {{
          "moving average.length": 150,
          "moving average.plot.color": "rgb(255,152,0)",
          "moving average.plot.linewidth": 2,
          "moving average.plot.linestyle": 0,
          "moving average #1.length": 50,
          "moving average #1.plot.color": "rgb(33,150,243)",
          "moving average #1.plot.linewidth": 2,
          "moving average #1.plot.linestyle": 0
        }},
        "drawings_access": {{
          "type": "all",
          "tools": [
            {{ "name": "Trend Line" }},
            {{ "name": "Horizontal Line" }},
            {{ "name": "Regression Trend" }},
            {{ "name": "Parallel Channel" }},
            {{ "name": "Fib Retracement" }}
          ]
        }},
        "enabled_features": ["drawing_templates"]
      }}
      </script>
    </div>
    """
    components.html(widget_html, height=height + 10, scrolling=False)


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


def _num_or_nan(value: object) -> float:
    s = _clean(value)
    if not s or s.lower() in {"nan", "nat", "none"}:
        return np.nan
    return _num(value)


def _signed_value_color(value: object) -> str:
    num = _num(value)
    if num > 0:
        return "color: #16a34a; font-weight: 600;"
    if num < 0:
        return "color: #dc2626; font-weight: 600;"
    return ""


def _apply_signed_color(styler: object, columns: list[str]) -> object:
    # pandas >= 2.1 favors Styler.map; keep fallback for older environments.
    if not columns:
        return styler
    if hasattr(styler, "map"):
        return styler.map(_signed_value_color, subset=columns)
    if hasattr(styler, "applymap"):
        return styler.applymap(_signed_value_color, subset=columns)
    return styler


def _render_dataframe_adaptive(
    data: object,
    is_mobile: bool,
    mobile_fallback: Optional[object] = None,
    force_same_render_path: bool = False,
    **kwargs,
) -> None:
    """Render Styler with formatting preserved. All columns default to minimal width."""
    # Build a column_config that sets every column to 'small' width,
    # unless the caller already provided one.
    if "column_config" not in kwargs:
        try:
            df = data.data if hasattr(data, "data") else data
            if isinstance(df, pd.DataFrame):
                kwargs["column_config"] = {
                    col: st.column_config.Column(width="small")
                    for col in df.columns
                }
        except Exception:
            pass
    st.dataframe(data, **kwargs)


def _parse_dates_flexible(series: pd.Series) -> pd.Series:
    raw = series.copy()
    clean_vals = raw.map(_clean)
    parsed = pd.Series(pd.NaT, index=raw.index, dtype="datetime64[ns]")

    # Year-first ISO stays year-first.
    iso_mask = clean_vals.str.match(r"^\d{4}-\d{2}-\d{2}$")
    if iso_mask.any():
        parsed.loc[iso_mask] = pd.to_datetime(clean_vals[iso_mask], format="%Y-%m-%d", errors="coerce")

    # Handles values like "2026-02-04 00:00:00" without DD/MM inversion.
    yearfirst_mask = clean_vals.str.match(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[ T].*)?$") & (~iso_mask)
    if yearfirst_mask.any():
        parsed.loc[yearfirst_mask] = pd.to_datetime(clean_vals[yearfirst_mask], errors="coerce", yearfirst=True, dayfirst=False)

    # Sheet-style slashed dates are interpreted strictly as DD/MM.
    slash_mask = clean_vals.str.contains("/", regex=False)
    if slash_mask.any():
        slash_vals = clean_vals[slash_mask]
        slash_parsed = pd.to_datetime(slash_vals, format="%d/%m/%Y", errors="coerce")
        missing_slash = slash_parsed.isna()
        if missing_slash.any():
            slash_parsed_alt = pd.to_datetime(slash_vals[missing_slash], format="%d/%m/%y", errors="coerce")
            slash_parsed.loc[missing_slash] = slash_parsed_alt
        parsed.loc[slash_mask] = slash_parsed

    # Dashed non-ISO dates (e.g. 4-2-2026) are DD-MM-YYYY.
    dash_dmy_mask = clean_vals.str.match(r"^\d{1,2}-\d{1,2}-\d{4}$")
    if dash_dmy_mask.any():
        parsed.loc[dash_dmy_mask] = pd.to_datetime(clean_vals[dash_dmy_mask], format="%d-%m-%Y", errors="coerce")

    # Google Sheets can surface dates as serial numbers (e.g. 46130).
    raw_num = pd.to_numeric(raw, errors="coerce")
    serial_mask = parsed.isna() & raw_num.between(20000, 80000, inclusive="both")
    if serial_mask.any():
        parsed.loc[serial_mask] = pd.to_datetime(raw_num[serial_mask], unit="D", origin="1899-12-30", errors="coerce")

    # Final fallback: day-first only.
    remaining = parsed.isna() & clean_vals.ne("")
    if remaining.any():
        parsed.loc[remaining] = pd.to_datetime(clean_vals[remaining], errors="coerce", dayfirst=True)
    return parsed


def _to_trade_id(row: pd.Series) -> str:
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


def _is_closed_status(value: object) -> bool:
    return _clean(value).lower() in {"סגור", "closed", "close", "sold", "נמכר"}


def _is_excellence_platform(value: object) -> bool:
    p = _clean(value).lower()
    return ("אקסלנס" in p) or ("excellence" in p)


def _strip_market_suffix(value: object) -> str:
    s = _clean(value)
    low = s.lower()
    if ("bit2c" in low or "horizon" in low or "הורייזון" in s) and "זירת מסחר" in s:
        s = s.replace(" (זירת מסחר)", "").replace("(זירת מסחר)", "").strip()
    return s


def _fill_current_location_defaults(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "Current_Location" not in out.columns:
        out["Current_Location"] = ""
    if "Status" not in out.columns:
        out["Status"] = ""
    if "Platform" not in out.columns:
        out["Platform"] = ""

    out["Current_Location"] = out["Current_Location"].map(_strip_market_suffix)
    out["Platform"] = out["Platform"].map(_strip_market_suffix)
    closed_mask = out["Status"].map(_is_closed_status)
    out.loc[closed_mask, "Current_Location"] = "נמכר"

    open_excellence_blank_mask = (~closed_mask) & out["Platform"].map(_is_excellence_platform) & out["Current_Location"].eq("")
    out.loc[open_excellence_blank_mask, "Current_Location"] = "אקסלנס"
    return out


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
        "תאריך מכירה": "Sell_Date",
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

    if "Sell_Date" in df.columns:
        df["Sell_Date"] = _parse_dates_flexible(df["Sell_Date"])

    if "Trade_ID" not in df.columns:
        df["Trade_ID"] = df.apply(_to_trade_id, axis=1)

    df = _fill_current_location_defaults(df)

    df["Event_Type"] = df["Event_Type"].replace("", "TRADE")
    df["Action"] = df.apply(
        lambda r: "SELL"
        if _clean(r.get("Action")) == "SELL" or _clean(r.get("Status")) == "סגור"
        else ("BUY" if _clean(r.get("Action")) in {"", "BUY"} else _clean(r.get("Action"))),
        axis=1,
    )
    return df


def _market_symbol(ticker: str) -> str:
    t = _clean(ticker).upper()
    if t in {"BTC", "ETH", "SOL"}:
        return f"{t}-USD"
    return t


@st.cache_data(ttl=900, show_spinner=False)
def _download_close_matrix(symbols: Tuple[str, ...], days: int = 365) -> pd.DataFrame:
    clean_symbols = tuple(s for s in symbols if _clean(s))
    if not clean_symbols:
        return pd.DataFrame()
    try:
        raw = yf.download(
            list(clean_symbols),
            period=f"{int(days)}d",
            interval="1d",
            progress=False,
            group_by="ticker",
            auto_adjust=False,
            threads=True,
        )
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
    elif isinstance(raw, pd.DataFrame):
        if "Close" in raw.columns and len(clean_symbols) == 1:
            sym = clean_symbols[0]
            s = pd.to_numeric(raw["Close"], errors="coerce")
            if s.notna().any():
                out[sym] = s.rename(sym)

    if not out:
        return pd.DataFrame()
    return pd.concat(out.values(), axis=1)


@st.cache_data(ttl=600, show_spinner=False)
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
            series = pd.to_numeric(close_df[sym], errors="coerce").dropna()
            if not series.empty:
                val = float(series.iloc[-1])
        if val <= 0:
            val = float(_safe_quote(sym))
        out[t] = val if val > 0 else 0.0
    return out


@st.cache_data(ttl=900, show_spinner=False)
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

    frames = []
    for ticker, qty in qty_by_ticker.items():
        sym = symbol_by_ticker[ticker]
        if sym not in close_df.columns:
            continue
        s = pd.to_numeric(close_df[sym], errors="coerce").rename(ticker)
        if s.notna().any():
            frames.append(s * qty)

    if not frames:
        return pd.Series(dtype=float)

    combined = pd.concat(frames, axis=1).ffill().fillna(0)
    return combined.sum(axis=1)


def fifo_metrics(trades: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, float]] = []
    usd_ils_rate = _safe_quote("USDILS=X")
    if usd_ils_rate <= 0:
        usd_ils_rate = 3.6

    for ticker, tdf in trades.sort_values("Purchase_Date").groupby("Ticker"):
        lots: List[FifoLot] = []
        realized = 0.0
        for _, row in tdf.iterrows():
            qty = float(row["Quantity"])
            cost_ils = float(row["Cost_ILS"]) if _num(row["Cost_ILS"]) != 0 else float(_num(row["Cost_Origin"]))
            unit_cost = abs(cost_ils / qty) if qty else 0.0
            origin_currency = _normalize_currency_code(row.get("Origin_Currency", ""))
            display_currency = _infer_display_currency(ticker, origin_currency)
            cost_origin = float(row.get("Cost_Origin", 0.0) or 0.0)
            if display_currency == "USD" and origin_currency == "USD" and cost_origin:
                display_unit_cost = abs(cost_origin / qty) if qty else 0.0
            elif display_currency == "USD" and cost_ils:
                display_unit_cost = abs((cost_ils / usd_ils_rate) / qty) if qty else 0.0
            elif display_currency == "USD" and cost_origin:
                display_unit_cost = abs((cost_origin / usd_ils_rate) / qty) if qty else 0.0
            elif display_currency == "ILS" and cost_ils:
                display_unit_cost = abs(cost_ils / qty) if qty else 0.0
            elif display_currency == "ILS" and origin_currency == "USD" and cost_origin:
                display_unit_cost = abs((cost_origin * usd_ils_rate) / qty) if qty else 0.0
            elif cost_origin:
                display_unit_cost = abs(cost_origin / qty) if qty else 0.0
            else:
                display_unit_cost = unit_cost
            if row["Action"] == "BUY" and qty > 0:
                lots.append(
                    FifoLot(
                        qty=qty,
                        cost_per_unit=unit_cost,
                        display_cost_per_unit=display_unit_cost,
                        display_currency=display_currency,
                    )
                )
            elif row["Action"] == "SELL" and qty != 0:
                sell_qty = abs(qty)
                sell_price = abs(float(_num(row["Current_Value_ILS"]))) / sell_qty if sell_qty and _num(row["Current_Value_ILS"]) != 0 else unit_cost
                while sell_qty > 1e-9 and lots:
                    lot = lots[0]
                    used = min(lot.qty, sell_qty)
                    realized += used * (sell_price - lot.cost_per_unit)
                    lot.qty -= used
                    sell_qty -= used
                    if lot.qty <= 1e-9:
                        lots.pop(0)

        open_qty = sum(lot.qty for lot in lots)
        if open_qty <= 1e-9:
            continue
        open_cost = sum(lot.qty * lot.cost_per_unit for lot in lots)
        open_display_cost = sum(lot.qty * lot.display_cost_per_unit for lot in lots)
        open_display_currency = lots[0].display_currency if lots else _infer_display_currency(ticker, "")
        rows.append(
            {
                "Ticker": ticker,
                "כמות פתוחה (FIFO)": open_qty,
                "עלות פתוחה (₪)": open_cost,
                "רווח ממומש (₪)": realized,
                "מחיר קנייה ממוצע": open_display_cost / open_qty if open_qty else 0.0,
                "מטבע מחיר קנייה ממוצע": open_display_currency,
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


@st.cache_data(ttl=300, show_spinner=False)
def _safe_quote(symbol: str) -> float:
    ticker_symbol = _clean(symbol).upper()
    if not ticker_symbol:
        return 0.0
    try:
        ticker = yf.Ticker(ticker_symbol)
        fast_info = getattr(ticker, "fast_info", None)
        if fast_info:
            for field in ["lastPrice", "regularMarketPrice", "previousClose"]:
                try:
                    val = float(getattr(fast_info, field, 0) or 0)
                    if val > 0:
                        return val
                except Exception:
                    pass

        hist = ticker.history(period="5d", interval="1d", auto_adjust=False)
        if not hist.empty and "Close" in hist.columns:
            close_series = pd.to_numeric(hist["Close"], errors="coerce").dropna()
            if not close_series.empty:
                return float(close_series.iloc[-1])
    except Exception:
        pass
    return 0.0


def _select_or_type(label: str, options: List[str], default: str = "", key_prefix: str = "", tr_fn=None, help_text: Optional[str] = None) -> str:
    cleaned = sorted({_clean(v) for v in options if _clean(v)})
    tr_local = tr_fn or (lambda en, he: he)
    default_clean = _clean(default)
    if default_clean and default_clean not in cleaned:
        cleaned = [default_clean] + cleaned

    other_label = tr_local("Other (type manually)", "אחר (הקלדה ידנית)")
    if not cleaned:
        return st.text_input(label, value=default_clean, key=f"{key_prefix}_{label}_type", help=help_text)

    selectable = cleaned + [other_label]
    default_select = default_clean if default_clean in cleaned else cleaned[0]
    index = selectable.index(default_select) if default_select in selectable else 0
    picked = st.selectbox(label, selectable, index=index, key=f"{key_prefix}_{label}_pick", help=help_text)

    if picked == other_label:
        return st.text_input(
            tr_local("Custom value", "ערך מותאם"),
            value=(default_clean if default_select == other_label else ""),
            key=f"{key_prefix}_{label}_type",
            help=help_text,
        )
    return _clean(picked)


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

    crypto_mask = (work["Type"] == "קריפטו") | (work["Ticker"].isin(CRYPTO_SHARE_TICKERS))
    crypto_value = float(work.loc[crypto_mask, "Current_Value_ILS"].sum())
    btc_value = float(work.loc[work["Ticker"].isin(BTC_SHARE_TICKERS), "Current_Value_ILS"].sum())

    usd_ils = _safe_quote("USDILS=X")
    btc_usd = _safe_quote("BTC-USD")
    eth_usd = _safe_quote("ETH-USD")
    sol_usd = _safe_quote("SOL-USD")

    fx = usd_ils
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
    spot_by_asset = {"BTC": btc_usd, "ETH": eth_usd, "SOL": sol_usd}
    concentration_rows: List[Dict[str, float]] = []
    for asset, etf in asset_map.items():
        direct = work[(work["Ticker"] == asset) & (work["Type"] == "קריפטו")]
        etf_df = work[work["Ticker"] == etf]
        direct_qty = float(direct["Quantity"].sum())
        direct_val = float(direct["Current_Value_ILS"].sum())
        etf_qty = float(etf_df["Quantity"].sum())
        etf_val = float(etf_df["Current_Value_ILS"].sum())
        indirect_ils = etf_val
        if asset == "BTC":
            # BTC exposure includes MSTR as an additional indirect BTC vehicle.
            indirect_ils += float(work.loc[work["Ticker"] == "MSTR", "Current_Value_ILS"].sum())

        estimated_asset_qty = direct_qty
        asset_ils_basis = (direct_val / direct_qty) if direct_qty > 1e-9 else 0.0
        spot_usd = float(spot_by_asset.get(asset, 0.0) or 0.0)
        if asset_ils_basis > 0:
            estimated_asset_qty = direct_qty + (indirect_ils / asset_ils_basis)
        elif fx > 0 and spot_usd > 0:
            estimated_asset_qty = direct_qty + (indirect_ils / fx / spot_usd)

        concentration_rows.append(
            {
                "Asset": asset,
                "Direct_Qty": direct_qty,
                "Direct_ILS": direct_val,
                "ETF_Qty": etf_qty,
                "ETF_ILS": etf_val,
                "Total_Exposure_ILS": direct_val + etf_val,
                "Estimated_BTC_Qty": estimated_asset_qty if asset == "BTC" else np.nan,
                "Estimated_ETH_Qty": estimated_asset_qty if asset == "ETH" else np.nan,
                "Estimated_SOL_Qty": estimated_asset_qty if asset == "SOL" else np.nan,
            }
        )

    live_rates = {
        "USD/ILS": usd_ils,
        "BTC/USD": btc_usd,
        "ETH/USD": eth_usd,
        "SOL/USD": sol_usd,
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
    body = _read_url_with_retries(req)
    return json.loads(body)


def _is_timeout_error(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return True
    if isinstance(exc, urlerror.URLError) and isinstance(getattr(exc, "reason", None), (TimeoutError, socket.timeout)):
        return True
    text = str(exc).lower()
    return "timed out" in text or "timeout" in text


def _is_transient_network_error(exc: Exception) -> bool:
    if _is_timeout_error(exc):
        return True
    if isinstance(exc, urlerror.URLError):
        return True
    text = str(exc).lower()
    return any(token in text for token in ["tempor", "connection reset", "remote end closed", "502", "503", "504"])


def _read_url_with_retries(req: urlrequest.Request) -> str:
    last_exc: Exception | None = None
    for attempt in range(1, NETWORK_MAX_RETRIES + 1):
        try:
            with urlrequest.urlopen(req, timeout=NETWORK_TIMEOUT_SECONDS) as response:
                return response.read().decode("utf-8")
        except Exception as exc:
            last_exc = exc
            should_retry = attempt < NETWORK_MAX_RETRIES and _is_transient_network_error(exc)
            if not should_retry:
                raise
            time.sleep(NETWORK_RETRY_BACKOFF_SECONDS * attempt)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Network read failed")


def _apps_script_on_cooldown() -> bool:
    now = time.time()
    if _APPS_SCRIPT_RETRY_AFTER_TS > now:
        return True
    try:
        if APPS_SCRIPT_COOLDOWN_FILE.exists():
            payload = json.loads(APPS_SCRIPT_COOLDOWN_FILE.read_text(encoding="utf-8"))
            retry_after = float(payload.get("retry_after", 0.0)) if isinstance(payload, dict) else 0.0
            return retry_after > now
    except Exception:
        pass
    return False


def _mark_apps_script_timeout() -> None:
    global _APPS_SCRIPT_RETRY_AFTER_TS
    retry_after = time.time() + APPS_SCRIPT_COOLDOWN_SECONDS
    _APPS_SCRIPT_RETRY_AFTER_TS = retry_after
    try:
        APPS_SCRIPT_COOLDOWN_FILE.write_text(json.dumps({"retry_after": retry_after}), encoding="utf-8")
    except Exception:
        pass


def _clear_apps_script_cooldown() -> None:
    global _APPS_SCRIPT_RETRY_AFTER_TS
    _APPS_SCRIPT_RETRY_AFTER_TS = 0.0
    try:
        if APPS_SCRIPT_COOLDOWN_FILE.exists():
            APPS_SCRIPT_COOLDOWN_FILE.unlink()
    except Exception:
        pass


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


def _normalize_manual_deposit_rows(rows: List[Dict[str, object]], default_platforms: List[str]) -> List[Dict[str, object]]:
    by_platform: Dict[str, float] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        platform = _clean(row.get("Platform", ""))
        if not platform:
            continue
        by_platform[platform] = _num(row.get("Manual_Deposit_ILS", 0.0))
    for platform in default_platforms:
        p = _clean(platform)
        if p and p not in by_platform:
            by_platform[p] = 0.0
    clean_rows = [{"Platform": k, "Manual_Deposit_ILS": float(v)} for k, v in by_platform.items() if _clean(k)]
    clean_rows.sort(key=lambda r: _clean(r.get("Platform", "")).lower())
    return clean_rows


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
            "theme_mode": _normalize_theme_mode(raw.get("theme_mode", THEME_SYSTEM)),
            "demo_mode": str(raw.get("demo_mode", "false")).lower() == "true",
            "followed_symbols": _clean(raw.get("followed_symbols", "")),
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
    theme_mode: str,
    demo_mode: bool,
    followed_symbols: str,
) -> bool:
    try:
        payload = {
            "web_app_url": _clean(web_app_url),
            "api_token": _clean(api_token),
            "spreadsheet_ref": _clean(spreadsheet_ref),
            "worksheet_name": _clean(worksheet_name) or DEFAULT_WORKSHEET_NAME,
            "service_account_file": _clean(service_account_file),
            "language": _clean(language) or DEFAULT_LANGUAGE,
            "theme_mode": _normalize_theme_mode(theme_mode),
            "demo_mode": bool(demo_mode),
            "followed_symbols": _clean(followed_symbols),
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


def _can_use_gspread_fallback(spreadsheet_ref: str, service_account_file: str) -> bool:
    if gspread is None:
        return False
    if not _clean(spreadsheet_ref):
        return False
    if _service_account_from_env() is not None:
        return True
    if _service_account_from_runtime_secrets() is not None:
        return True
    key_path = Path(_clean(service_account_file)) if _clean(service_account_file) else DEFAULT_SERVICE_ACCOUNT_FILE
    if not key_path.is_absolute():
        key_path = Path(__file__).resolve().parent / key_path
    return key_path.exists()


@st.cache_data(ttl=300)
def load_google_snapshot_data_via_gspread(spreadsheet_ref: str, worksheet_name: str, service_account_file: str) -> pd.DataFrame:
    client = _build_gspread_client(service_account_file)
    sheet_id = _extract_sheet_id(spreadsheet_ref)
    if not sheet_id:
        raise RuntimeError("חסר Spreadsheet ID/URL עבור חיבור gspread")

    last_exc: Exception | None = None
    values: list[list[object]] = []
    for attempt in range(1, NETWORK_MAX_RETRIES + 1):
        try:
            book = client.open_by_key(sheet_id)
            ws = book.worksheet(_clean(worksheet_name) or DEFAULT_WORKSHEET_NAME)
            values = ws.get_all_values()
            break
        except Exception as exc:
            last_exc = exc
            should_retry = attempt < NETWORK_MAX_RETRIES and _is_transient_network_error(exc)
            if not should_retry:
                raise
            time.sleep(NETWORK_RETRY_BACKOFF_SECONDS * attempt)
    if last_exc is not None and not values:
        raise last_exc
    if not values:
        return pd.DataFrame()

    headers = [str(h) for h in values[0]]
    rows = [r for r in values[1:] if any(_clean(v) for v in r)]
    return pd.DataFrame(rows, columns=headers)


@st.cache_data(ttl=300)
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


def _save_local_snapshot_cache(df: pd.DataFrame) -> None:
    try:
        if df.empty:
            return
        out = df.copy()
        if "Purchase_Date" in out.columns:
            out["Purchase_Date"] = _parse_dates_flexible(out["Purchase_Date"]).dt.strftime("%Y-%m-%d").fillna("")
        out.to_csv(LOCAL_SNAPSHOT_CACHE_FILE, index=False, encoding="utf-8-sig")
    except Exception:
        return


def _load_emergency_snapshot_backup() -> Tuple[pd.DataFrame, str]:
    try:
        if LOCAL_SNAPSHOT_CACHE_FILE.exists():
            cached = pd.read_csv(LOCAL_SNAPSHOT_CACHE_FILE)
            if not cached.empty:
                return _normalize_snapshot_df(cached), "local_cache"
    except Exception:
        pass

    try:
        if VERIFIED_DATA_FALLBACK_FILE.exists():
            verified = load_verified_data(str(VERIFIED_DATA_FALLBACK_FILE))
            if not verified.empty:
                return verified, "verified_fallback"
    except Exception:
        pass

    return pd.DataFrame(), ""


def _normalize_snapshot_df(df: pd.DataFrame) -> pd.DataFrame:
    rename = {
        "מיקום נוכחי": "Current_Location",
        "פלטפורמה": "Platform",
        "סוג נכס": "Type",
        "טיקר": "Ticker",
        "תאריך רכישה": "Purchase_Date",
        "תאריך מכירה": "Sell_Date",
        "כמות": "Quantity",
        "שער קנייה": "Origin_Buy_Price",
        "עלות כוללת": "Cost_Origin",
        "מטבע": "Origin_Currency",
        "עמלה": "Commission",
        "סטטוס": "Status",
        "עלות ILS": "Cost_ILS",
        "שווי ILS": "Current_Value_ILS",
        "שער מכירה": "Sell_Price_Origin",
        "תשואה במכירה": "Yield_At_Sale",
        "תשואה מקור": "Yield_Origin",
        "תשואה שקלית": "Yield_ILS",
        "Trade_ID": "Trade_ID",
    }
    df = df.rename(columns=rename)

    for col in ["Current_Location", "Platform", "Type", "Ticker", "Status", "Origin_Currency"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].map(_clean)

    for col in ["Quantity", "Origin_Buy_Price", "Cost_Origin", "Commission", "Cost_ILS", "Current_Value_ILS"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = df[col].map(_num)

    if "Sell_Price_Origin" in df.columns:
        df["Sell_Price_Origin"] = df["Sell_Price_Origin"].map(_num)

    for col in ["Yield_At_Sale", "Yield_Origin", "Yield_ILS"]:
        if col in df.columns:
            # Keep truly missing yields as NaN so downstream logic can backfill.
            df[col] = df[col].map(_num_or_nan)

    # ── Validate Cost_Origin against Quantity * Origin_Buy_Price ──
    # If they diverge significantly, recalculate (fixes stale cost data from spreadsheet).
    qty = df["Quantity"]
    price = df["Origin_Buy_Price"]
    expected_cost = qty * price
    has_both = (qty.abs() > 1e-9) & (price.abs() > 1e-9)
    cost_orig = df["Cost_Origin"]
    diverged = has_both & ((cost_orig < 1e-9) | ((cost_orig - expected_cost).abs() / expected_cost.clip(lower=1e-9) > 0.05))
    if diverged.any():
        df.loc[diverged, "Cost_Origin"] = expected_cost[diverged]
        # Proportionally fix Cost_ILS where it was based on the bad Cost_Origin
        for idx in df.index[diverged]:
            old_cost = cost_orig.at[idx]
            new_cost = df.at[idx, "Cost_Origin"]
            if old_cost > 1e-9 and df.at[idx, "Cost_ILS"] > 1e-9:
                ratio = new_cost / old_cost
                df.at[idx, "Cost_ILS"] = df.at[idx, "Cost_ILS"] * ratio

    if "Purchase_Date" not in df.columns:
        df["Purchase_Date"] = pd.NaT
    df["Purchase_Date"] = _parse_dates_flexible(df["Purchase_Date"])

    if "Sell_Date" in df.columns:
        df["Sell_Date"] = _parse_dates_flexible(df["Sell_Date"])

    if "Trade_ID" not in df.columns:
        df["Trade_ID"] = df.apply(_to_trade_id, axis=1)
    else:
        df["Trade_ID"] = df["Trade_ID"].map(_clean)
        missing = df["Trade_ID"] == ""
        if missing.any():
            df.loc[missing, "Trade_ID"] = df.loc[missing].apply(_to_trade_id, axis=1)

    df = _fill_current_location_defaults(df)

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
            "Ticker": "VOO",
            "Purchase_Date": "2025-01-15",
            "Quantity": 24,
            "Origin_Buy_Price": 468,
            "Cost_Origin": 11232,
            "Origin_Currency": "USD",
            "Commission": 6,
            "Status": "פתוח",
            "Cost_ILS": 41450,
            "Current_Value_ILS": 49840,
        },
        {
            "Current_Location": "",
            "Platform": "Global Prime",
            "Type": "ETF",
            "Ticker": "QQQ",
            "Purchase_Date": "2025-03-03",
            "Quantity": 32,
            "Origin_Buy_Price": 521,
            "Cost_Origin": 16672,
            "Origin_Currency": "USD",
            "Commission": 5,
            "Status": "פתוח",
            "Cost_ILS": 61520,
            "Current_Value_ILS": 58740,
        },
        {
            "Current_Location": "Digital Vault",
            "Platform": "Crypto Desk",
            "Type": "ETF",
            "Ticker": "IBIT",
            "Purchase_Date": "2025-05-08",
            "Quantity": 210,
            "Origin_Buy_Price": 39.8,
            "Cost_Origin": 8358,
            "Origin_Currency": "USD",
            "Commission": 7,
            "Status": "פתוח",
            "Cost_ILS": 30910,
            "Current_Value_ILS": 41820,
        },
        {
            "Current_Location": "Digital Vault",
            "Platform": "Crypto Desk",
            "Type": "ETF",
            "Ticker": "ETHA",
            "Purchase_Date": "2025-07-18",
            "Quantity": 320,
            "Origin_Buy_Price": 24.6,
            "Cost_Origin": 7872,
            "Origin_Currency": "USD",
            "Commission": 5,
            "Status": "פתוח",
            "Cost_ILS": 29120,
            "Current_Value_ILS": 26140,
        },
        {
            "Current_Location": "Digital Vault",
            "Platform": "Crypto Desk",
            "Type": "ETF",
            "Ticker": "BSOL",
            "Purchase_Date": "2025-10-02",
            "Quantity": 430,
            "Origin_Buy_Price": 17.4,
            "Cost_Origin": 7482,
            "Origin_Currency": "USD",
            "Commission": 4,
            "Status": "פתוח",
            "Cost_ILS": 27630,
            "Current_Value_ILS": 34480,
        },
        {
            "Current_Location": "Custody Vault",
            "Platform": "Quant Desk",
            "Type": "שוק ההון",
            "Ticker": "MSTR",
            "Purchase_Date": "2025-09-12",
            "Quantity": 18,
            "Origin_Buy_Price": 295,
            "Cost_Origin": 5310,
            "Origin_Currency": "USD",
            "Commission": 3,
            "Status": "פתוח",
            "Cost_ILS": 19620,
            "Current_Value_ILS": 17380,
        },
        {
            "Current_Location": "Core Wallet",
            "Platform": "Digital Alpha",
            "Type": "קריפטו",
            "Ticker": "BTC",
            "Purchase_Date": "2025-08-23",
            "Quantity": 0.36,
            "Origin_Buy_Price": 60500,
            "Cost_Origin": 21780,
            "Origin_Currency": "USD",
            "Commission": 18,
            "Status": "פתוח",
            "Cost_ILS": 80420,
            "Current_Value_ILS": 98950,
        },
        {
            "Current_Location": "Core Wallet",
            "Platform": "Digital Alpha",
            "Type": "קריפטו",
            "Ticker": "ETH",
            "Purchase_Date": "2025-09-30",
            "Quantity": 9.2,
            "Origin_Buy_Price": 2780,
            "Cost_Origin": 25576,
            "Origin_Currency": "USD",
            "Commission": 15,
            "Status": "פתוח",
            "Cost_ILS": 94430,
            "Current_Value_ILS": 87200,
        },
        {
            "Current_Location": "Trading Wallet",
            "Platform": "Digital Alpha",
            "Type": "קריפטו",
            "Ticker": "SOL",
            "Purchase_Date": "2025-11-06",
            "Quantity": 180,
            "Origin_Buy_Price": 145,
            "Cost_Origin": 26100,
            "Origin_Currency": "USD",
            "Commission": 20,
            "Status": "פתוח",
            "Cost_ILS": 96340,
            "Current_Value_ILS": 117600,
        },
        {
            "Current_Location": "Local Broker",
            "Platform": "IsraTrade",
            "Type": "שוק ההון",
            "Ticker": "TA35",
            "Purchase_Date": "2025-06-01",
            "Quantity": 6,
            "Origin_Buy_Price": 1895,
            "Cost_Origin": 11370,
            "Origin_Currency": "ILS",
            "Commission": 42,
            "Status": "פתוח",
            "Cost_ILS": 11412,
            "Current_Value_ILS": 10840,
        },
        {
            "Current_Location": "Custody Vault",
            "Platform": "Global Prime",
            "Type": "שוק ההון",
            "Ticker": "NVDA",
            "Purchase_Date": "2025-07-11",
            "Quantity": 44,
            "Origin_Buy_Price": 121,
            "Cost_Origin": 5324,
            "Origin_Currency": "USD",
            "Commission": 4,
            "Status": "פתוח",
            "Cost_ILS": 19680,
            "Current_Value_ILS": 31740,
        },
        {
            "Current_Location": "",
            "Platform": "Quant Desk",
            "Type": "שוק ההון",
            "Ticker": "ASML",
            "Purchase_Date": "2025-12-09",
            "Quantity": 10,
            "Origin_Buy_Price": 720,
            "Cost_Origin": 7200,
            "Origin_Currency": "USD",
            "Commission": 3,
            "Status": "פתוח",
            "Cost_ILS": 26540,
            "Current_Value_ILS": 24260,
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
            "Current_Value_ILS": 31590,
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
            "Current_Value_ILS": 37140,
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
            "Current_Value_ILS": 29820,
        },
        {
            "Current_Location": "",
            "Platform": "Global Prime",
            "Type": "ETF",
            "Ticker": "ARKK",
            "Purchase_Date": "2025-05-22",
            "Quantity": 90,
            "Origin_Buy_Price": 51,
            "Cost_Origin": 4590,
            "Origin_Currency": "USD",
            "Commission": 3,
            "Status": "סגור",
            "Cost_ILS": 16920,
            "Current_Value_ILS": 15280,
        },
        {
            "Current_Location": "",
            "Platform": "Digital Alpha",
            "Type": "קריפטו",
            "Ticker": "ETH",
            "Purchase_Date": "2025-04-06",
            "Quantity": 3.2,
            "Origin_Buy_Price": 2320,
            "Cost_Origin": 7424,
            "Origin_Currency": "USD",
            "Commission": 9,
            "Status": "סגור",
            "Cost_ILS": 27490,
            "Current_Value_ILS": 30360,
        },
    ]
    df = pd.DataFrame(demo_rows)

    # Keep demo portfolio intentionally compact for presentations (< 100k ILS total open value).
    open_mask = df.get("Status", "").map(_clean) != "סגור"
    open_value_ils = float(df.loc[open_mask, "Current_Value_ILS"].map(_num).sum()) if "Current_Value_ILS" in df.columns else 0.0
    target_open_value_ils = 98000.0
    if open_value_ils > (target_open_value_ils + 1e-9):
        scale = target_open_value_ils / open_value_ils
        for col in ["Origin_Buy_Price", "Cost_Origin", "Cost_ILS", "Current_Value_ILS", "Commission"]:
            if col in df.columns:
                df[col] = df[col].map(_num).astype(float)
                df.loc[open_mask, col] = df.loc[open_mask, col].map(_num) * scale

    return _normalize_snapshot_df(df)


def load_snapshot_data(
    web_app_url: str,
    token: str,
    spreadsheet_ref: str,
    worksheet_name: str,
    service_account_file: str,
) -> Tuple[pd.DataFrame, str]:
    clean_url = _clean(web_app_url)
    fallback_ready = _can_use_gspread_fallback(spreadsheet_ref, service_account_file)

    # If Apps Script recently timed out, skip remote read and load backup immediately.
    if clean_url and _apps_script_on_cooldown():
        backup_df, backup_mode = _load_emergency_snapshot_backup()
        if not backup_df.empty:
            return backup_df, backup_mode

    if clean_url:
        if not is_apps_script_web_app_url(clean_url):
            raise RuntimeError("קישור Web App לא תקין")
        try:
            df_remote = load_google_snapshot_data(clean_url, token)
            _clear_apps_script_cooldown()
            _save_local_snapshot_cache(df_remote)
            return df_remote, "apps_script"
        except Exception as exc:
            err_text = str(exc).lower()
            timeout_like = _is_timeout_error(exc)
            if timeout_like:
                _mark_apps_script_timeout()
            can_fallback = (("unsupported action" in err_text) or ("unauthorized" in err_text) or timeout_like) and fallback_ready
            if not can_fallback:
                backup_df, backup_mode = _load_emergency_snapshot_backup()
                if not backup_df.empty:
                    return backup_df, backup_mode
                if timeout_like:
                    raise RuntimeError("Apps Script timed out after retries, and no local backup data is available.")
                raise
            try:
                raw_df = load_google_snapshot_data_via_gspread(spreadsheet_ref, worksheet_name, service_account_file)
                normalized = _normalize_snapshot_df(raw_df)
                _save_local_snapshot_cache(normalized)
                return normalized, "gspread"
            except Exception:
                backup_df, backup_mode = _load_emergency_snapshot_backup()
                if not backup_df.empty:
                    return backup_df, backup_mode
                raise

    try:
        raw_df = load_google_snapshot_data_via_gspread(spreadsheet_ref, worksheet_name, service_account_file)
        normalized = _normalize_snapshot_df(raw_df)
        _save_local_snapshot_cache(normalized)
        return normalized, "gspread"
    except Exception:
        backup_df, backup_mode = _load_emergency_snapshot_backup()
        if not backup_df.empty:
            return backup_df, backup_mode
        raise


def sync_trade_to_sheet(web_app_url: str, token: str, action: str, trade_row: Dict[str, object]) -> Tuple[bool, str]:
    if not web_app_url:
        return False, "חסר קישור Web App של Apps Script"

    def _norm_date_text(v: object) -> str:
        parsed = _parse_dates_flexible(pd.Series([v])).iloc[0]
        return parsed.strftime("%Y-%m-%d") if pd.notna(parsed) else _clean(v)

    def _prepare_trade_payload(src: Dict[str, object]) -> Dict[str, object]:
        out = {
            "Current_Location": _clean(src.get("Current_Location", "")),
            "Platform": _clean(src.get("Platform", "")),
            "Type": _clean(src.get("Type", "")),
            "Ticker": _clean(src.get("Ticker", "")).upper(),
            "Purchase_Date": _norm_date_text(src.get("Purchase_Date", "")),
            "Quantity": float(_num(src.get("Quantity", 0.0))),
            "Origin_Buy_Price": float(_num(src.get("Origin_Buy_Price", 0.0))),
            "Cost_Origin": float(_num(src.get("Cost_Origin", 0.0))),
            "Origin_Currency": _normalize_currency_code(src.get("Origin_Currency", "")),
            "Commission": float(_num(src.get("Commission", 0.0))),
            "Status": _clean(src.get("Status", "")),
            "Cost_ILS": float(_num(src.get("Cost_ILS", 0.0))),
            "Current_Value_ILS": float(_num(src.get("Current_Value_ILS", 0.0))),
            "Action": _clean(src.get("Action", "")).upper(),
            "Event_Type": _clean(src.get("Event_Type", "TRADE")) or "TRADE",
            "Trade_ID": _clean(src.get("Trade_ID", "")),
            "Sell_Date": _norm_date_text(src.get("Sell_Date", "")),
            "Sell_Price_Origin": float(_num(src.get("Sell_Price_Origin", 0.0))),
        }

        if out["Cost_Origin"] <= 0 and out["Quantity"] > 0 and out["Origin_Buy_Price"] > 0:
            out["Cost_Origin"] = float(out["Quantity"] * out["Origin_Buy_Price"])

        is_closed = _is_closed_status(out.get("Status", "")) or out.get("Action", "") == "SELL"
        out["Status"] = "סגור" if is_closed else "פתוח"
        out["Action"] = "SELL" if is_closed else "BUY"

        if is_closed and not _clean(out.get("Sell_Date", "")):
            out["Sell_Date"] = datetime.now().strftime("%Y-%m-%d")
        if not is_closed:
            out["Sell_Date"] = ""

        if not out["Trade_ID"]:
            out["Trade_ID"] = _to_trade_id(pd.Series(out))
        return out

    normalized_trade = _prepare_trade_payload(trade_row)
    payload = {"token": token or "", "action": action, "trade": normalized_trade}

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


def prepare_core_views(df: pd.DataFrame) -> Dict[str, object]:
    trades = df[(df["Record_Source"] == "STATE_SNAPSHOT") & (df["Event_Type"] == "TRADE")].copy() if "Record_Source" in df.columns else df.copy()
    if "Status" not in trades.columns:
        trades["Status"] = ""
    trades["Status"] = trades["Status"].replace("", "פתוח")
    status_norm = trades["Status"].map(_clean).str.lower()
    closed_values = {"סגור", "closed", "close", "sold", "נמכר"}
    closed_mask = status_norm.isin(closed_values)
    open_trades = trades[~closed_mask].copy()
    closed_trades = trades[closed_mask].copy()

    # Safety fallback: if status labels are malformed and everything appears closed,
    # infer open trades by positive position/value so top-level totals are not zeroed.
    if open_trades.empty and not trades.empty and "Current_Value_ILS" in trades.columns:
        value_mask = trades["Current_Value_ILS"].map(_num) > 0
        qty_mask = trades["Quantity"].map(_num) > 0 if "Quantity" in trades.columns else False
        inferred_open_mask = value_mask | qty_mask
        if inferred_open_mask.any():
            open_trades = trades[inferred_open_mask].copy()
            closed_trades = trades[~inferred_open_mask].copy()

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



# ════════════════════════════════════════════════════════════════════════
# PREMIUM ENHANCEMENTS — inlined (was premium_enhancements.py)
# Glassmorphism CSS, PWA meta tags, keyboard shortcuts, command palette,
# toast notifications, advanced analytics helpers and one-click exports.
# All additive and failure-swallowing: the base app keeps working even
# if any single sub-layer cannot load.
# ════════════════════════════════════════════════════════════════════════
import base64 as _pp_b64
import io as _pp_io
import math as _pp_math
from typing import Optional as _PP_Optional, Sequence as _PP_Sequence


def apply_premium_polish(language: str = "עברית",
                         is_dark: bool = False,
                         is_mobile: bool = False) -> None:
    """Inject every premium UX layer.  Safe to call once per rerun."""
    try:
        _pp_inject_premium_css(is_dark=is_dark, is_mobile=is_mobile)
    except Exception:
        pass
    try:
        _pp_inject_mobile_polish_v2(is_dark=is_dark, is_mobile=is_mobile)
    except Exception:
        pass
    try:
        _pp_inject_pwa_assets(language=language, is_dark=is_dark)
    except Exception:
        pass
    try:
        _pp_inject_productivity_layer(language=language)
    except Exception:
        pass
    try:
        _pp_inject_help_shim()
    except Exception:
        pass


def _pp_inject_help_shim() -> None:
    """Two persistent runtime shims, idempotent across reruns:
      1. Help-badge popover handler (close on outside tap, single-active, Esc).
      2. **Phantom container collapser** — finds every Streamlit
         `stElementContainer` whose only payload is a 0-height iframe or
         a `<style>`/`<script>`-only markdown injection and `display:none`s
         it. Streamlit's parent `stVerticalBlock` has `gap: 1rem`, so even
         0-height children consume 16px. This is what was creating
         ~200px of "dead space" above the title.
    """
    components.html(
        """
        <script>(function(){
          try {
            var d = (window.parent && window.parent.document) ? window.parent.document : document;
            if (d.__pp_help_shim_installed) return;
            d.__pp_help_shim_installed = true;

            // ── 1) Help popover behaviour ──────────────────────────────
            function closeAllExcept(except) {
              d.querySelectorAll('details.pp-help[open]').forEach(function(el){
                if (el !== except) el.removeAttribute('open');
              });
            }
            d.addEventListener('click', function(ev){
              var t = ev.target;
              if (!t || !t.closest) return;
              var inHelp = t.closest('details.pp-help');
              closeAllExcept(inHelp || null);
            }, true);
            d.addEventListener('keydown', function(ev){
              if (ev.key === 'Escape') closeAllExcept(null);
            }, true);
            d.addEventListener('touchstart', function(ev){
              var t = ev.target;
              if (!t || !t.closest) return;
              var inHelp = t.closest('details.pp-help');
              if (!inHelp) closeAllExcept(null);
            }, {passive: true, capture: true});
            // Scroll closes any open help. Streamlit's actual scroll
            // container is NOT the window — `stAppViewContainer` has
            // overflow:hidden and the `section.main` (or similar) below
            // it is what scrolls. So we listen on EVERY element with
            // overflow auto/scroll, plus window/document, plus a polling
            // fallback that walks open details' scroll-parents.
            function onScrollOrTouch() { closeAllExcept(null); }
            var w = d.defaultView || window;
            d.addEventListener('scroll', onScrollOrTouch, {passive: true, capture: true});
            w.addEventListener('scroll', onScrollOrTouch, {passive: true, capture: true});
            w.addEventListener('wheel', onScrollOrTouch, {passive: true, capture: true});
            d.addEventListener('wheel', onScrollOrTouch, {passive: true, capture: true});
            d.addEventListener('touchmove', onScrollOrTouch, {passive: true, capture: true});
            w.addEventListener('resize', onScrollOrTouch);
            w.addEventListener('orientationchange', onScrollOrTouch);

            function findScrollableParent(el) {
              var n = el && el.parentElement;
              while (n) {
                var cs = getComputedStyle(n);
                var oy = cs.overflowY, ox = cs.overflowX;
                if (oy === 'auto' || oy === 'scroll' || ox === 'auto' || ox === 'scroll') {
                  return n;
                }
                n = n.parentElement;
              }
              return null;
            }
            // Whenever a help opens, attach a one-shot scroll listener on
            // its nearest scrollable ancestor (which is what actually
            // scrolls inside Streamlit).
            d.addEventListener('toggle', function(ev){
              var t = ev.target;
              if (!t || !t.matches || !t.matches('details.pp-help')) return;
              if (!t.open) return;
              var sp = findScrollableParent(t);
              if (sp && !sp.__pp_help_scroll_bound) {
                sp.__pp_help_scroll_bound = true;
                sp.addEventListener('scroll', onScrollOrTouch, {passive: true, capture: true});
              }
              // Also bind on each scrollable section.main / stMain
              d.querySelectorAll('section.main, [data-testid="stMain"], [data-testid="stMainBlockContainer"], [data-testid="stAppViewContainer"]').forEach(function(s){
                if (s.__pp_help_scroll_bound) return;
                s.__pp_help_scroll_bound = true;
                s.addEventListener('scroll', onScrollOrTouch, {passive: true, capture: true});
              });
            }, true);

            // Final polling fallback (covers any container we missed)
            var lastSig = '';
            setInterval(function(){
              try {
                var sig = (w.scrollY || 0) + ':' + Array.from(d.querySelectorAll('[data-testid="stMain"], section.main, [data-testid="stMainBlockContainer"]')).map(function(s){return s.scrollTop;}).join(',');
                if (sig !== lastSig) {
                  if (lastSig) closeAllExcept(null);
                  lastSig = sig;
                }
              } catch(e) {}
            }, 200);

            // ── 2) Phantom container collapser (NARROW, CONSERVATIVE) ──
            // Only hide containers that match one of the explicitly-known
            // "phantom" patterns:
            //   (a) Markdown container whose immediate children are ONLY
            //       <style>, <script>, or <link> (no visible content).
            //   (b) Empty markdown container (no children at all).
            //   (c) Iframe with explicit height="0" attribute (the
            //       `components.html(height=0)` case).
            // We DO NOT use computed-height heuristics — those wrongly
            // hide widgets that load asynchronously.
            function isStyleOnlyMarkdown(el) {
              var md = el.querySelector(':scope > .stMarkdown > div > [data-testid="stMarkdownContainer"]');
              if (!md) {
                md = el.querySelector('[data-testid="stMarkdownContainer"]');
                if (!md) return false;
                // Make sure this markdown belongs DIRECTLY to this container
                if (md.closest('[data-testid="stElementContainer"]') !== el) return false;
              }
              var kids = md.children;
              if (!kids || kids.length === 0) return true;  // empty markdown
              for (var i = 0; i < kids.length; i++) {
                var t = kids[i].tagName;
                if (t !== 'STYLE' && t !== 'SCRIPT' && t !== 'LINK') return false;
              }
              return true;
            }
            function isZeroIframe(el) {
              // Only target Streamlit's components.html iframes (data-testid="stIFrame").
              // Custom components (streamlit-option-menu, lottie, etc.) start at
              // height=0 and grow via postMessage — hiding them kills navigation.
              var ifrs = el.querySelectorAll('iframe[data-testid="stIFrame"]');
              for (var i = 0; i < ifrs.length; i++) {
                var ifr = ifrs[i];
                if (ifr.closest('[data-testid="stElementContainer"]') !== el) continue;
                var h = ifr.getAttribute('height');
                if (h === '0' || h === 0 || h === '0px') return true;
                var inlineH = (ifr.style && ifr.style.height) || '';
                if (inlineH === '0' || inlineH === '0px') return true;
                var rect = ifr.getBoundingClientRect();
                if (rect && rect.height < 1) return true;
              }
              return false;
            }
            function isPhantom(el) {
              return isStyleOnlyMarkdown(el) || isZeroIframe(el);
            }
            function collapsePhantoms() {
              var containers = d.querySelectorAll('[data-testid="stElementContainer"]');
              for (var i = 0; i < containers.length; i++) {
                var c = containers[i];
                if (isPhantom(c)) {
                  if (c.style.display !== 'none') {
                    c.style.setProperty('display', 'none', 'important');
                  }
                }
              }
            }
            collapsePhantoms();
            try {
              var mo = new MutationObserver(function(){
                requestAnimationFrame(collapsePhantoms);
              });
              mo.observe(d.body, {childList: true, subtree: true});
            } catch(e) {}
          } catch(e) {}
        })();</script>
        """,
        height=0, width=0,
    )


def _pp_inject_premium_css(is_dark: bool, is_mobile: bool) -> None:
    accent = "#6366f1"
    accent_2 = "#06b6d4"
    bg_soft = "rgba(148,163,184,0.10)" if is_dark else "rgba(99,102,241,0.06)"
    border_soft = "rgba(148,163,184,0.22)" if is_dark else "rgba(15,23,42,0.08)"
    card_bg = (
        "linear-gradient(180deg, rgba(30,41,59,0.72) 0%, rgba(15,23,42,0.72) 100%)"
        if is_dark
        else "linear-gradient(180deg, rgba(255,255,255,0.92) 0%, rgba(248,250,252,0.85) 100%)"
    )
    shadow = (
        "0 10px 28px -12px rgba(0,0,0,0.55), 0 2px 6px -2px rgba(0,0,0,0.35)"
        if is_dark
        else "0 10px 28px -14px rgba(15,23,42,0.22), 0 2px 6px -3px rgba(15,23,42,0.12)"
    )
    css = f"""
    <style id="premium-polish">
    :root {{
        --pp-accent: {accent};
        --pp-accent-2: {accent_2};
        --pp-bg-soft: {bg_soft};
        --pp-border-soft: {border_soft};
        --pp-card-bg: {card_bg};
        --pp-shadow: {shadow};
        --pp-radius: 16px;
        --pp-radius-sm: 10px;
    }}
    html, body, [class*="css"] {{
        font-feature-settings: "cv11","ss01","ss03";
        -webkit-font-smoothing: antialiased;
        text-rendering: optimizeLegibility;
    }}
    div[data-testid="stMetric"] {{
        background: var(--pp-card-bg);
        border: 1px solid var(--pp-border-soft);
        border-radius: var(--pp-radius);
        padding: 14px 18px;
        box-shadow: var(--pp-shadow);
        backdrop-filter: blur(10px) saturate(1.05);
        -webkit-backdrop-filter: blur(10px) saturate(1.05);
        transition: transform .18s ease, box-shadow .18s ease, border-color .18s ease;
        position: relative; overflow: hidden;
    }}
    div[data-testid="stMetric"]::before {{
        content: ""; position: absolute; inset: 0 auto auto 0;
        width: 4px; height: 100%;
        background: linear-gradient(180deg, var(--pp-accent), var(--pp-accent-2));
        opacity: .95;
    }}
    div[data-testid="stMetric"]:hover {{
        transform: translateY(-2px); border-color: var(--pp-accent);
    }}
    div[data-testid="stMetricValue"] {{
        font-weight: 700 !important; letter-spacing: -0.01em;
    }}
    .stButton > button, .stDownloadButton > button {{
        border-radius: 12px !important;
        border: 1px solid var(--pp-border-soft) !important;
        transition: transform .12s ease, box-shadow .12s ease, background .12s ease !important;
        font-weight: 600 !important;
    }}
    .stButton > button:hover, .stDownloadButton > button:hover {{
        transform: translateY(-1px); box-shadow: var(--pp-shadow);
        border-color: var(--pp-accent) !important;
    }}
    .stButton > button:focus-visible, .stDownloadButton > button:focus-visible {{
        outline: 2px solid var(--pp-accent) !important; outline-offset: 2px !important;
    }}
    .stTextInput input, .stNumberInput input, .stDateInput input,
    .stSelectbox div[data-baseweb="select"], .stMultiSelect div[data-baseweb="select"] {{
        border-radius: 12px !important;
    }}
    button[data-baseweb="tab"] {{
        border-radius: 10px 10px 0 0 !important; font-weight: 600 !important;
    }}
    button[data-baseweb="tab"][aria-selected="true"] {{ color: var(--pp-accent) !important; }}
    details[data-testid="stExpander"], div[data-testid="stExpander"] {{
        border-radius: var(--pp-radius) !important;
        border: 1px solid var(--pp-border-soft) !important;
        box-shadow: var(--pp-shadow);
        background: var(--pp-card-bg) !important;
        backdrop-filter: blur(8px);
    }}
    div[data-testid="stDataFrame"] {{
        border-radius: var(--pp-radius-sm) !important;
        border: 1px solid var(--pp-border-soft) !important;
        overflow: hidden;
    }}
    div[data-testid="stAlert"] {{ border-radius: 14px !important; border-left-width: 4px !important; }}
    *::-webkit-scrollbar {{ width: 10px; height: 10px; }}
    *::-webkit-scrollbar-thumb {{
        background: linear-gradient(180deg, var(--pp-accent), var(--pp-accent-2));
        border-radius: 999px; border: 2px solid transparent; background-clip: padding-box;
    }}
    *::-webkit-scrollbar-track {{ background: transparent; }}
    section.main > div.block-container {{ animation: pp-fade .35s ease both; }}
    @keyframes pp-fade {{
        from {{ opacity: 0; transform: translateY(4px); }}
        to   {{ opacity: 1; transform: translateY(0); }}
    }}
    #pp-cmdk-overlay {{
        position: fixed; inset: 0; background: rgba(2,6,23,.55);
        backdrop-filter: blur(6px); z-index: 999999; display: none;
        align-items: flex-start; justify-content: center; padding-top: 14vh;
    }}
    #pp-cmdk {{
        width: min(620px, 92vw);
        background: {'#0f172a' if is_dark else '#ffffff'};
        color: {'#e2e8f0' if is_dark else '#0f172a'};
        border-radius: 16px; border: 1px solid var(--pp-border-soft);
        box-shadow: 0 40px 80px -20px rgba(0,0,0,.55);
        overflow: hidden; font-family: -apple-system,Segoe UI,Roboto,Helvetica,Arial;
    }}
    #pp-cmdk input {{
        width: 100%; padding: 14px 18px; font-size: 16px;
        background: transparent; border: 0; outline: 0;
        color: inherit; border-bottom: 1px solid var(--pp-border-soft);
    }}
    #pp-cmdk ul {{ list-style: none; margin: 0; padding: 6px; max-height: 50vh; overflow: auto; }}
    #pp-cmdk li {{
        padding: 10px 14px; border-radius: 10px; cursor: pointer;
        display:flex; justify-content:space-between; gap:12px; align-items:center;
    }}
    #pp-cmdk li[aria-selected="true"], #pp-cmdk li:hover {{
        background: var(--pp-bg-soft); color: var(--pp-accent);
    }}
    #pp-cmdk kbd {{
        font-size: 11px; padding: 2px 6px; border-radius: 6px;
        border: 1px solid var(--pp-border-soft); opacity: .75;
    }}
    #pp-toast {{
        position: fixed; bottom: 24px; left: 50%; transform: translateX(-50%) translateY(40px);
        background: #0f172a; color: #fff;
        padding: 10px 18px; border-radius: 999px; font-size: 13px; font-weight: 600;
        box-shadow: 0 20px 40px -10px rgba(0,0,0,.5);
        opacity: 0; transition: opacity .2s, transform .2s; z-index: 999999;
        pointer-events: none;
    }}
    #pp-toast.show {{ opacity: 1; transform: translateX(-50%) translateY(0); }}
    @media (max-width: 820px) {{
        section.main > div.block-container {{
            padding: 0.6rem 0.75rem 5rem 0.75rem !important;
            max-width: 100% !important;
        }}
        h1 {{ font-size: 1.4rem !important; line-height: 1.2 !important; }}
        h2 {{ font-size: 1.2rem !important; line-height: 1.2 !important; }}
        h3 {{ font-size: 1.05rem !important; }}
        div[data-testid="stMetric"] {{ padding: 10px 12px !important; }}
        div[data-testid="stMetricValue"] {{ font-size: 1.25rem !important; }}
        div[data-testid="stMetricLabel"] {{ font-size: 0.72rem !important; opacity: .85; }}
        .stButton > button, .stDownloadButton > button {{ min-height: 44px; font-size: 0.95rem; }}
        .stTextInput input, .stNumberInput input, .stDateInput input {{
            font-size: 16px !important; min-height: 44px;
        }}
        div[data-baseweb="select"] > div {{ min-height: 44px !important; }}
        div[data-testid="stDataFrame"], div[data-testid="stTable"] {{
            overflow-x: auto; -webkit-overflow-scrolling: touch;
        }}
        [data-testid="stSidebar"] {{ width: 80vw !important; min-width: 260px !important; }}
        div[role="radiogroup"] {{ gap: 6px !important; flex-wrap: wrap; }}
        div[role="radiogroup"] > label {{
            border: 1px solid var(--pp-border-soft);
            padding: 8px 12px; border-radius: 999px;
            background: var(--pp-card-bg);
        }}
        .js-plotly-plot .plotly .modebar {{ transform: scale(.8); }}
        section.main {{ padding-bottom: env(safe-area-inset-bottom); }}
    }}
    @media (prefers-reduced-motion: reduce) {{
        *, *::before, *::after {{ animation: none !important; transition: none !important; }}
    }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════
# MOBILE POLISH v2  — modern fluid design, 60fps, iOS/Android-grade feel
# Additive layer on top of _pp_inject_premium_css; safe & non-breaking.
# ════════════════════════════════════════════════════════════════════════
def _pp_inject_mobile_polish_v2(is_dark: bool, is_mobile: bool) -> None:
    """Fluid typography, glass UI, faster animations, premium mobile feel.

    All selectors are additive — they only refine the look of existing
    components and never hide or change layout semantics. Safe on desktop
    (no-ops inside media queries where appropriate).
    """
    bg_base = "#0b1220" if is_dark else "#f7f8fb"
    surface = "rgba(30,41,59,0.62)" if is_dark else "rgba(255,255,255,0.72)"
    surface_strong = "rgba(15,23,42,0.88)" if is_dark else "rgba(255,255,255,0.94)"
    border = "rgba(148,163,184,0.22)" if is_dark else "rgba(15,23,42,0.08)"
    text = "#e2e8f0" if is_dark else "#0f172a"
    muted = "#94a3b8" if is_dark else "#64748b"
    shimmer_a = "rgba(255,255,255,0.05)" if is_dark else "rgba(15,23,42,0.04)"
    shimmer_b = "rgba(255,255,255,0.12)" if is_dark else "rgba(15,23,42,0.10)"

    css = f"""
    <style id="pp-mobile-polish-v2">
    /* ── Fluid design tokens (desktop + mobile scale smoothly) ── */
    :root {{
        --pp2-fs-h1: clamp(1.35rem, 1.05rem + 1.4vw, 2.05rem);
        --pp2-fs-h2: clamp(1.15rem, 0.95rem + 1.0vw, 1.55rem);
        --pp2-fs-h3: clamp(1.00rem, 0.88rem + 0.6vw, 1.25rem);
        --pp2-fs-body: clamp(0.92rem, 0.88rem + 0.2vw, 1.02rem);
        --pp2-fs-metric-v: clamp(1.05rem, 0.80rem + 1.2vw, 1.65rem);
        --pp2-fs-metric-l: clamp(0.68rem, 0.60rem + 0.3vw, 0.88rem);
        --pp2-radius-xl: 20px;
        --pp2-radius-l: 14px;
        --pp2-radius-m: 10px;
        --pp2-ease: cubic-bezier(0.22, 1, 0.36, 1);
        --pp2-dur-fast: 120ms;
        --pp2-dur: 220ms;
        --pp2-surface: {surface};
        --pp2-surface-strong: {surface_strong};
        --pp2-border: {border};
        --pp2-text: {text};
        --pp2-muted: {muted};
        --pp2-grad: linear-gradient(135deg,#6366f1 0%,#06b6d4 55%,#10b981 100%);
    }}

    /* ── Smooth native scrolling + no tap-highlight flash ── */
    html {{ scroll-behavior: smooth; }}
    body, [data-testid="stAppViewContainer"] {{
        -webkit-tap-highlight-color: transparent;
        text-size-adjust: 100%; -webkit-text-size-adjust: 100%;
    }}
    * {{ -webkit-touch-callout: none; }}
    input, textarea, [contenteditable] {{ -webkit-touch-callout: default; }}

    /* ── Fluid typography (desktop + mobile) ── */
    .block-container h1, .block-container [data-testid="stHeading"] h1 {{ font-size: var(--pp2-fs-h1) !important; letter-spacing: -0.01em; }}
    .block-container h2 {{ font-size: var(--pp2-fs-h2) !important; letter-spacing: -0.005em; }}
    .block-container h3 {{ font-size: var(--pp2-fs-h3) !important; }}
    .block-container p, .block-container li, .block-container label {{ font-size: var(--pp2-fs-body); }}

    /* ── Metric refinement: gradient hairline + tabular numerics ── */
    [data-testid="stMetric"] {{
        border-radius: var(--pp2-radius-l) !important;
        position: relative;
    }}
    [data-testid="stMetricValue"] {{
        font-variant-numeric: tabular-nums !important;
        font-size: var(--pp2-fs-metric-v) !important;
    }}
    [data-testid="stMetricLabel"] {{
        font-size: var(--pp2-fs-metric-l) !important;
        text-transform: uppercase; letter-spacing: 0.04em; font-weight: 600 !important;
    }}
    [data-testid="stMetricDelta"] {{ font-variant-numeric: tabular-nums; font-weight: 600; }}

    /* Tabular numerics in data frames too → column alignment looks crisp */
    [data-testid="stDataFrame"] {{ font-variant-numeric: tabular-nums; }}

    /* ── Buttons: modern press feedback (no layout shift) ── */
    .stButton > button, .stDownloadButton > button, [data-testid="stFormSubmitButton"] button {{
        transition: transform var(--pp2-dur-fast) var(--pp2-ease),
                    box-shadow var(--pp2-dur-fast) var(--pp2-ease),
                    background var(--pp2-dur-fast) var(--pp2-ease) !important;
        will-change: transform;
    }}
    .stButton > button:active, .stDownloadButton > button:active,
    [data-testid="stFormSubmitButton"] button:active {{
        transform: scale(0.97) !important;
        transition-duration: 60ms !important;
    }}

    /* Primary (indigo) → gradient, crisp on retina */
    button[kind="primary"], button[data-testid="baseButton-primary"] {{
        background: var(--pp2-grad) !important;
        border: 0 !important; color: #fff !important;
        box-shadow: 0 6px 16px -4px rgba(99,102,241,0.45) !important;
    }}
    button[kind="primary"]:hover, button[data-testid="baseButton-primary"]:hover {{
        filter: brightness(1.05) saturate(1.05);
    }}

    /* ── Plotly: perf + clipping ── */
    [data-testid="stPlotlyChart"] {{
        border-radius: var(--pp2-radius-l) !important;
        content-visibility: auto;
        contain-intrinsic-size: 360px;
    }}
    [data-testid="stDataFrame"] {{
        content-visibility: auto;
        contain-intrinsic-size: 480px;
    }}

    /* ── Chart lock: works on both desktop and mobile ── */
    [data-testid="stPlotlyChart"].pp-chart-locked .js-plotly-plot,
    [data-testid="stPlotlyChart"].pp-chart-locked .plot-container,
    [data-testid="stPlotlyChart"].pp-chart-locked .svg-container,
    [data-testid="stPlotlyChart"].pp-chart-locked .nsewdrag,
    [data-testid="stPlotlyChart"].pp-chart-locked .drag {{
        touch-action: pan-y !important;
        pointer-events: none !important;
    }}

    /* ── Skeleton shimmer utility for any <div class="pp-skeleton"> ── */
    .pp-skeleton {{
        background: linear-gradient(90deg, {shimmer_a} 0%, {shimmer_b} 50%, {shimmer_a} 100%);
        background-size: 200% 100%;
        animation: pp-shimmer 1.2s linear infinite;
        border-radius: var(--pp2-radius-m);
    }}
    @keyframes pp-shimmer {{ 0%{{background-position:200% 0}} 100%{{background-position:-200% 0}} }}

    /* ── Focus rings: accessible & modern ── */
    :focus-visible {{
        outline: 2px solid #6366f1 !important;
        outline-offset: 2px !important;
        border-radius: 8px;
    }}

    /* ── Sidebar surface: subtle glass ── */
    [data-testid="stSidebar"] > div:first-child {{
        backdrop-filter: blur(14px) saturate(1.1);
        -webkit-backdrop-filter: blur(14px) saturate(1.1);
    }}

    /* ── Expanders: soft hover lift ── */
    details[data-testid="stExpander"]:hover,
    div[data-testid="stExpander"]:hover {{
        border-color: rgba(99,102,241,0.45) !important;
    }}

    /* ── Tab list: smooth horizontal momentum scroll on mobile ── */
    [data-baseweb="tab-list"] {{
        scroll-behavior: smooth;
        mask-image: linear-gradient(90deg, transparent 0, #000 12px, #000 calc(100% - 12px), transparent 100%);
    }}

    /* ──────────────────────────────────────────────────────────
       MOBILE (≤ 820px) — premium phone experience
    ────────────────────────────────────────────────────────── */
    @media (max-width: 820px) {{
        html, body {{ background: {bg_base}; }}

        /* Pin Streamlit header/toolbar to top, no empty scroll space above.
           overflow MUST be visible — otherwise the floating Main Menu
           (3-dots), which lives inside the header, gets clipped to a 1-px
           sliver at the right edge. */
        [data-testid="stHeader"], header[data-testid="stHeader"] {{
            position: sticky !important; top: 0 !important; z-index: 1000000 !important;
            height: 0 !important; min-height: 0 !important; padding: 0 !important;
            overflow: visible !important;
        }}
        [data-testid="stAppViewContainer"] {{
            padding-top: 0 !important;
        }}

        /* Tighter vertical rhythm, better thumb reach */
        .block-container {{
            padding-top: 0.2rem !important;
            padding-bottom: calc(1.2rem + env(safe-area-inset-bottom)) !important;
        }}

        /* Adaptive KPI grid: wraps to 2-per-row so long labels (CAGR / Calmar /
           Concentration / Beta …) never get truncated on phones. */
        [data-testid="stHorizontalBlock"]:has([data-testid="stMetric"]) {{
            flex-wrap: wrap !important;
            gap: 0.5rem !important;
            row-gap: 0.5rem !important;
        }}
        [data-testid="stHorizontalBlock"]:has([data-testid="stMetric"]) > [data-testid="stColumn"] {{
            flex: 1 1 calc(50% - 0.25rem) !important;
            min-width: calc(50% - 0.25rem) !important;
            max-width: 100% !important;
            box-sizing: border-box !important;
        }}
        [data-testid="stMetric"] {{
            padding: 0.6rem 0.75rem !important;
            min-height: 84px !important;
            border-radius: var(--pp2-radius-l) !important;
            background: var(--pp2-surface) !important;
            backdrop-filter: blur(10px) saturate(1.1);
            -webkit-backdrop-filter: blur(10px) saturate(1.1);
            border: 1px solid var(--pp2-border) !important;
            box-shadow: 0 4px 14px -6px rgba(15,23,42,0.18) !important;
            overflow: hidden !important;
        }}
        /* Metric label — allow wrapping instead of ellipsis so full term shows */
        [data-testid="stMetric"] [data-testid="stMetricLabel"] {{
            white-space: normal !important;
            overflow: visible !important;
            text-overflow: clip !important;
            line-height: 1.2 !important;
            word-break: break-word !important;
            hyphens: auto !important;
        }}
        /* Metric value — keep on single line but shrink font if too long */
        [data-testid="stMetric"] [data-testid="stMetricValue"] {{
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
            max-width: 100% !important;
        }}
        /* Metric delta — wrap description text so nothing gets cut */
        [data-testid="stMetric"] [data-testid="stMetricDelta"] {{
            white-space: normal !important;
            line-height: 1.25 !important;
            word-break: break-word !important;
        }}
        [data-testid="stMetric"]::before {{
            width: 3px !important;
        }}

        /* Bigger, thumb-friendly controls (Apple HIG 44pt). Buttons are
           refined: subtle elevation, refined typography, no kindergarten
           gradient unless explicitly type="primary". */
        .stButton > button, .stDownloadButton > button,
        [data-testid="stFormSubmitButton"] button,
        div[data-baseweb="select"] > div,
        .stTextInput input, .stNumberInput input, .stDateInput input {{
            min-height: 44px !important;
            font-size: 16px !important; /* prevent iOS zoom */
            border-radius: 12px !important;
        }}
        /* Default (secondary) buttons — clean, subtle, professional */
        .stButton > button:not([kind="primary"]):not([data-testid="baseButton-primary"]),
        .stDownloadButton > button:not([kind="primary"]) {{
            background: var(--pp2-surface) !important;
            color: var(--pp2-text) !important;
            border: 1px solid var(--pp2-border) !important;
            font-weight: 600 !important;
            letter-spacing: 0.01em !important;
            box-shadow: 0 1px 2px 0 rgba(15,23,42,0.05),
                        0 1px 3px 0 rgba(15,23,42,0.08) !important;
            transition: background 140ms ease, border-color 140ms ease,
                        box-shadow 140ms ease, transform 80ms ease !important;
        }}
        .stButton > button:not([kind="primary"]):hover,
        .stDownloadButton > button:not([kind="primary"]):hover {{
            background: var(--pp2-surface-strong) !important;
            border-color: rgba(99,102,241,0.45) !important;
            color: #4f46e5 !important;
            box-shadow: 0 2px 6px -1px rgba(79,70,229,0.18),
                        0 4px 10px -3px rgba(15,23,42,0.10) !important;
            transform: translateY(-1px);
        }}
        .stButton > button:not([kind="primary"]):active,
        .stDownloadButton > button:not([kind="primary"]):active {{
            transform: translateY(0);
            background: rgba(99,102,241,0.06) !important;
        }}
        /* Primary buttons (kind="primary") — refined indigo gradient */
        .stButton > button[kind="primary"],
        .stButton > button[data-testid="baseButton-primary"],
        button[kind="primary"] {{
            background: linear-gradient(135deg, #4f46e5 0%, #6366f1 55%, #818cf8 100%) !important;
            color: #fff !important;
            border: 0 !important;
            font-weight: 700 !important;
            letter-spacing: 0.01em !important;
            box-shadow: 0 4px 14px -3px rgba(79,70,229,0.42),
                        0 1px 0 0 rgba(255,255,255,0.16) inset !important;
            transition: filter 120ms ease, box-shadow 140ms ease, transform 80ms ease !important;
        }}
        .stButton > button[kind="primary"]:hover,
        button[kind="primary"]:hover {{
            filter: brightness(1.06);
            transform: translateY(-1px);
            box-shadow: 0 6px 18px -4px rgba(79,70,229,0.55) !important;
        }}
        .stButton > button[kind="primary"]:active,
        button[kind="primary"]:active {{
            transform: translateY(0);
            filter: brightness(0.96);
        }}

        /* Sticky toolbar row above content when it contains buttons */
        [data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"] .stButton) {{
            position: sticky; top: calc(3.6rem + env(safe-area-inset-top));
            z-index: 50;
            backdrop-filter: blur(12px) saturate(1.1);
            -webkit-backdrop-filter: blur(12px) saturate(1.1);
            background: var(--pp2-surface);
            border-radius: 12px; padding: 6px; margin-bottom: 6px;
        }}

        /* Tabs: floating pill appearance */
        [data-baseweb="tab-list"] {{
            gap: 4px !important;
            padding: 4px !important;
            border-radius: 14px !important;
            background: var(--pp2-surface) !important;
            backdrop-filter: blur(10px);
            -webkit-backdrop-filter: blur(10px);
            border: 1px solid var(--pp2-border) !important;
        }}
        /* Tabs — refined "soft pill" appearance, more professional than
           the gradient default. Inactive: muted ghost; active: solid
           accent fill with subtle drop shadow + crisp white text. */
        [data-baseweb="tab-list"] [data-baseweb="tab"] {{
            border-radius: 10px !important;
            padding: 8px 14px !important;
            min-height: 38px !important;
            border: 0 !important;
            font-weight: 600 !important;
            font-size: 13px !important;
            letter-spacing: 0.01em !important;
            color: var(--pp2-muted) !important;
            background: transparent !important;
            transition: background 140ms ease, color 140ms ease, transform 120ms var(--pp2-ease) !important;
        }}
        [data-baseweb="tab-list"] [data-baseweb="tab"]:hover {{
            background: rgba(99,102,241,0.08) !important;
            color: var(--pp2-text) !important;
        }}
        [data-baseweb="tab-list"] [data-baseweb="tab"][aria-selected="true"] {{
            background: linear-gradient(135deg, #4f46e5 0%, #6366f1 60%, #818cf8 100%) !important;
            color: #fff !important;
            box-shadow: 0 4px 12px -3px rgba(79,70,229,0.45) !important;
        }}
        [data-baseweb="tab-list"] [data-baseweb="tab"][aria-selected="true"] * {{ color:#fff !important; }}
        [data-baseweb="tab-list"] [data-baseweb="tab"][aria-selected="true"]:hover {{
            filter: brightness(1.05);
        }}

        /* Plotly charts: taller intrinsic box, no modebar on mobile */
        [data-testid="stPlotlyChart"] {{ contain-intrinsic-size: 300px; position: relative; }}
        .modebar-container, .modebar {{ display: none !important; }}

        /* Radio buttons: always horizontal, no wrap */
        [data-testid="stRadio"] > div {{
            flex-wrap: nowrap !important;
            gap: 0.35rem !important;
        }}
        [data-testid="stRadio"] > div > label {{
            white-space: nowrap !important;
            font-size: 13px !important;
            padding: 6px 10px !important;
            min-width: 0 !important;
            flex-shrink: 1 !important;
        }}

        /* DataFrame: card-like */
        [data-testid="stDataFrame"] {{
            border-radius: var(--pp2-radius-l) !important;
            box-shadow: 0 2px 10px -4px rgba(15,23,42,0.12);
        }}

        /* Expanders: rounder, tighter */
        details[data-testid="stExpander"], div[data-testid="stExpander"] {{
            border-radius: var(--pp2-radius-l) !important;
        }}
        [data-testid="stExpander"] summary {{
            min-height: 44px !important; padding: 10px 12px !important;
        }}

        /* Alert boxes: flatter & rounder */
        [data-testid="stAlert"] {{
            border-radius: 14px !important;
            border-left-width: 4px !important;
        }}

        /* Inputs: soft glass surfaces */
        .stTextInput > div, .stNumberInput > div, .stDateInput > div,
        div[data-baseweb="select"] > div {{
            border-radius: 12px !important;
            background: var(--pp2-surface) !important;
            backdrop-filter: blur(10px);
            -webkit-backdrop-filter: blur(10px);
        }}

        /* Hero title — compact but bold */
        .app-main-title {{
            font-size: clamp(1.35rem, 1.1rem + 2vw, 1.75rem) !important;
            background: var(--pp2-grad);
            -webkit-background-clip: text; background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        .app-sub-title {{ font-size: 0.84rem !important; color: var(--pp2-muted) !important; }}

        /* Top page-nav radio (4-pill segmented control) — refined glass
           pill bar with subtle border, professional shadow, no gradient
           background. Active pill gets a clean indigo fill (matches the
           tabs above and the brand accent), inactive pills stay neutral.
           Pinned at the very top with right-side gutter for 3-dots. */
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child),
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(5):last-child) {{
            background: var(--pp2-surface-strong) !important;
            backdrop-filter: blur(18px) saturate(1.15) !important;
            -webkit-backdrop-filter: blur(18px) saturate(1.15) !important;
            box-shadow: 0 6px 20px -8px rgba(15,23,42,0.20),
                        0 1px 0 0 rgba(255,255,255,0.50) inset !important;
            border: 1px solid var(--pp2-border) !important;
            position: sticky !important;
            top: 6px !important;
            z-index: 998 !important;
            padding: 5px 6px !important;
            border-radius: 14px !important;
            margin-right: 56px !important;  /* gutter for 3-dots */
        }}
        /* Each pill — neutral by default, accent on active */
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"],
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(5):last-child)
        [data-baseweb="radio"] {{
            background: transparent !important;
            border-radius: 10px !important;
            padding: 6px 10px !important;
            transition: background 140ms ease, color 140ms ease, box-shadow 140ms ease !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"]:has(input:checked),
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(5):last-child)
        [data-baseweb="radio"]:has(input:checked) {{
            background: linear-gradient(135deg, #4f46e5 0%, #6366f1 60%, #818cf8 100%) !important;
            box-shadow: 0 4px 14px -3px rgba(79,70,229,0.50) !important;
            color: #fff !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"]:has(input:checked) p,
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(5):last-child)
        [data-baseweb="radio"]:has(input:checked) p,
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"]:has(input:checked) * {{
            color: #fff !important;
            font-weight: 700 !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"]:has(input:checked),
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(5):last-child)
        [data-baseweb="radio"]:has(input:checked) {{
            background: var(--pp2-grad) !important;
            box-shadow: 0 4px 12px -2px rgba(99,102,241,0.55);
        }}

        /* Safer hamburger — bigger, subtle press */
        [data-testid="collapsedControl"] button,
        [data-testid="stSidebarCollapsedControl"] button,
        button[aria-label*="sidebar"] {{
            transition: transform 120ms var(--pp2-ease), box-shadow 120ms var(--pp2-ease) !important;
        }}
        [data-testid="collapsedControl"] button:active,
        [data-testid="stSidebarCollapsedControl"] button:active,
        button[aria-label*="sidebar"]:active {{
            transform: scale(0.92) !important;
        }}

        /* Native-like swipe for sidebar */
        [data-testid="stSidebar"] {{
            box-shadow: 8px 0 32px -12px rgba(0,0,0,0.35) !important;
        }}

        /* Spinner centering */
        [data-testid="stSpinner"] {{ padding: 14px 0; }}
    }}

    /* Small phones: even tighter */
    @media (max-width: 420px) {{
        .block-container {{ padding-left: 0.55rem !important; padding-right: 0.55rem !important; }}
        [data-testid="stMetric"] {{ min-height: 74px !important; padding: 0.5rem 0.6rem !important; }}
        [data-testid="stMetricValue"] {{ font-size: 1.05rem !important; }}
        /* Keep label legible (not micro) — primary readability over max density */
        [data-testid="stMetricLabel"] {{ font-size: 0.68rem !important; line-height: 1.2 !important; }}
    }}

    /* Respect reduced-motion users */
    @media (prefers-reduced-motion: reduce) {{
        html {{ scroll-behavior: auto; }}
        .pp-skeleton {{ animation: none; }}
    }}

    /* Dark tint pass for v2 surfaces */
    {("html, body { background: " + bg_base + " !important; }") if is_dark else ""}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

    # Tiny JS: haptic feedback on primary button taps + passive touch listeners
    # (improves perceived speed + frees the main thread on scroll).
    components.html(
        """
        <script>(function(){
          try {
            var d = window.parent ? window.parent.document : document;
            if (d.getElementById('pp2-haptics')) return;
            var s = d.createElement('script'); s.id = 'pp2-haptics';
            s.textContent = `
              (function(){
                function vib(ms){ try{ if (navigator.vibrate) navigator.vibrate(ms); }catch(e){} }
                document.addEventListener('pointerdown', function(ev){
                  var t = ev.target && ev.target.closest && ev.target.closest(
                    'button, [role="tab"], [data-baseweb="radio"], .stButton>button, .stDownloadButton>button'
                  );
                  if (t) vib(8);
                }, {passive: true, capture: true});
                // Force passive listeners on wheel/touchmove for perf (exclude touchstart to not break dropdowns)
                var origAdd = EventTarget.prototype.addEventListener;
                EventTarget.prototype.addEventListener = function(type, fn, opts){
                  if (type === 'touchmove' || type === 'wheel') {
                    if (opts === undefined || opts === false) opts = {passive:true};
                    else if (typeof opts === 'object' && opts.passive === undefined) opts.passive = true;
                  }
                  return origAdd.call(this, type, fn, opts);
                };
              })();
            `;
            d.head.appendChild(s);
          } catch(e){}
        })();</script>
        """,
        height=0, width=0,
    )


def _pp_inline_icon(color: str) -> str:
    svg = (
        f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 192 192'>"
        f"<rect width='192' height='192' rx='36' fill='{color}'/>"
        f"<path d='M40 140 L76 96 L104 120 L152 60' stroke='white' stroke-width='10' "
        f"fill='none' stroke-linecap='round' stroke-linejoin='round'/>"
        f"<circle cx='152' cy='60' r='8' fill='white'/></svg>"
    )
    return "data:image/svg+xml;base64," + _pp_b64.b64encode(svg.encode()).decode()


def _pp_inject_pwa_assets(language: str, is_dark: bool) -> None:
    theme_color = "#0f172a" if is_dark else "#6366f1"
    manifest = {
        "name": "Portfolio Manager",
        "short_name": "Portfolio",
        "start_url": ".",
        "display": "standalone",
        "background_color": "#0f172a" if is_dark else "#ffffff",
        "theme_color": theme_color,
        "orientation": "any",
        "lang": "he" if language.startswith("ע") else "en",
        "icons": [
            {"src": _pp_inline_icon(theme_color), "sizes": "192x192", "type": "image/svg+xml"},
        ],
    }
    manifest_data_uri = "data:application/manifest+json;base64," + _pp_b64.b64encode(
        json.dumps(manifest, ensure_ascii=False).encode("utf-8")
    ).decode()
    html = f"""
    <script>
    (function() {{
      try {{
        var doc = window.parent ? window.parent.document : document;
        var head = doc.head;
        function ensure(sel, attrs) {{
          var el = doc.querySelector(sel);
          if (!el) {{ el = doc.createElement(attrs._tag); head.appendChild(el); }}
          for (var k in attrs) if (k !== '_tag') el.setAttribute(k, attrs[k]);
        }}
        ensure('meta[name="viewport"]', {{_tag:'meta', name:'viewport',
          content:'width=device-width, initial-scale=1.0, maximum-scale=5.0, viewport-fit=cover'}});
        ensure('meta[name="theme-color"]', {{_tag:'meta', name:'theme-color', content:'{theme_color}'}});
        ensure('meta[name="apple-mobile-web-app-capable"]', {{_tag:'meta', name:'apple-mobile-web-app-capable', content:'yes'}});
        ensure('meta[name="apple-mobile-web-app-status-bar-style"]', {{_tag:'meta', name:'apple-mobile-web-app-status-bar-style', content:'black-translucent'}});
        ensure('meta[name="mobile-web-app-capable"]', {{_tag:'meta', name:'mobile-web-app-capable', content:'yes'}});
        ensure('link[rel="manifest"]', {{_tag:'link', rel:'manifest', href:'{manifest_data_uri}'}});
      }} catch(e) {{}}
    }})();
    </script>
    """
    components.html(html, height=0, width=0)


def _pp_inject_productivity_layer(language: str) -> None:
    is_he = language.startswith("ע")
    cmds = [
        {"id": "nav-dashboard", "label": "דשבורד" if is_he else "Dashboard", "hint": "g d"},
        {"id": "nav-manage",    "label": "ניהול עסקאות" if is_he else "Trade Management", "hint": "g t"},
        {"id": "nav-risk",      "label": "סיכונים ופיפו" if is_he else "Risk & FIFO", "hint": "g r"},
        {"id": "nav-simulator", "label": "סימולטור" if is_he else "Simulator", "hint": "g s"},
        {"id": "nav-quality",   "label": "בקרת נתונים" if is_he else "Data Quality", "hint": "g q"},
        {"id": "toggle-theme",  "label": "החלפת ערכת נושא" if is_he else "Toggle theme", "hint": "t"},
        {"id": "focus-search",  "label": "חיפוש" if is_he else "Focus search", "hint": "/"},
        {"id": "rerun",         "label": "רענון" if is_he else "Rerun", "hint": "r"},
        {"id": "help",          "label": "קיצורי מקלדת" if is_he else "Keyboard shortcuts", "hint": "?"},
    ]
    cmds_json = json.dumps(cmds, ensure_ascii=False)
    placeholder = "הקלד לחיפוש פעולה…" if is_he else "Type to search actions…"
    html = f"""
    <script>
    (function() {{
      try {{
        var doc = window.parent ? window.parent.document : document;
        if (doc.getElementById('pp-cmdk-overlay')) return;
        var overlay = doc.createElement('div');
        overlay.id = 'pp-cmdk-overlay';
        overlay.style.display = 'none';   // ALWAYS start hidden — prevents "undefined" flash
        overlay.innerHTML = `
          <div id="pp-cmdk" role="dialog" aria-modal="true">
            <input id="pp-cmdk-input" type="text" placeholder="{placeholder}" autocomplete="off"/>
            <ul id="pp-cmdk-list"></ul>
          </div>`;
        doc.body.appendChild(overlay);
        var toast = doc.createElement('div');
        toast.id = 'pp-toast';
        toast.style.display = 'none';     // start hidden — avoid "undefined" artefact
        doc.body.appendChild(toast);
        var CMDS = {cmds_json};
        var listEl = doc.getElementById('pp-cmdk-list');
        var inputEl = doc.getElementById('pp-cmdk-input');
        var selected = 0;
        function showToast(msg) {{
          msg = (msg !== undefined && msg !== null) ? String(msg) : '';
          if (!msg) return;  // never show empty / undefined toast
          toast.textContent = msg;
          toast.style.display = 'block';
          toast.classList.add('show');
          clearTimeout(toast._t);
          toast._t = setTimeout(function() {{ toast.classList.remove('show'); toast.style.display = 'none'; }}, 1600);
        }}
        window.ppToast = showToast;
        function render(filter) {{
          var q = (filter || '').trim().toLowerCase();
          var items = CMDS.filter(function(c) {{
            if (!q) return true;
            return (c.label + ' ' + c.hint).toLowerCase().indexOf(q) !== -1;
          }});
          if (selected >= items.length) selected = 0;
          listEl.innerHTML = items.map(function(c, i) {{
            return '<li data-id="'+c.id+'" aria-selected="'+(i===selected)+'">'+
                   '<span>'+c.label+'</span><kbd>'+c.hint+'</kbd></li>';
          }}).join('') || '<li style="opacity:.6">—</li>';
          Array.prototype.forEach.call(listEl.querySelectorAll('li[data-id]'), function(li) {{
            li.addEventListener('click', function() {{ runCommand(li.getAttribute('data-id')); }});
          }});
        }}
        function openPalette() {{
          overlay.style.display = 'flex';
          inputEl.value = ''; selected = 0; render('');
          setTimeout(function() {{ inputEl.focus(); }}, 10);
        }}
        function closePalette() {{ overlay.style.display = 'none'; }}
        window.ppOpenPalette = openPalette;
        function clickByText(needles) {{
          var nodes = doc.querySelectorAll('label, button, [role="tab"]');
          for (var i=0;i<nodes.length;i++) {{
            var t = (nodes[i].innerText || '').trim();
            for (var j=0;j<needles.length;j++) {{
              if (t && t.indexOf(needles[j]) !== -1) {{ nodes[i].click(); return true; }}
            }}
          }}
          return false;
        }}
        function runCommand(id) {{
          closePalette();
          try {{
            if (id === 'nav-dashboard') clickByText(['דשבורד','Dashboard','סקירה','Overview']);
            else if (id === 'nav-manage') clickByText(['ניהול עסקאות','Trade','עסקאות','Trades']);
            else if (id === 'nav-risk') clickByText(['סיכונים','Risk','סיכון','Reports','דוחות']);
            else if (id === 'nav-simulator') clickByText(['סימולטור','Simulator','Sim']);
            else if (id === 'nav-quality') clickByText(['בקרת נתונים','Data Quality','נתונים','Data']);
            else if (id === 'toggle-theme') {{
              var b = doc.querySelector('header button[kind="header"]'); if (b) b.click();
              showToast('Theme');
            }} else if (id === 'focus-search') {{
              var inp = doc.querySelector('input[type="text"], input[type="search"]');
              if (inp) inp.focus();
            }} else if (id === 'rerun') {{
              location.reload();
            }} else if (id === 'help') {{
              showToast('⌘K · / · t · r · g d/t/r/s/q · ?');
            }}
          }} catch(e) {{}}
        }}
        inputEl && inputEl.addEventListener('input', function(e) {{ selected = 0; render(e.target.value); }});
        overlay.addEventListener('click', function(e) {{ if (e.target === overlay) closePalette(); }});
        var gBuffer = 0;
        doc.addEventListener('keydown', function(e) {{
          var tag = (e.target && e.target.tagName || '').toLowerCase();
          var typing = tag === 'input' || tag === 'textarea' || e.target.isContentEditable;
          if ((e.metaKey || e.ctrlKey) && (e.key === 'k' || e.key === 'K')) {{
            e.preventDefault(); openPalette(); return;
          }}
          if (e.key === 'Escape' && overlay.style.display === 'flex') {{
            e.preventDefault(); closePalette(); return;
          }}
          if (overlay.style.display === 'flex') {{
            var items = listEl.querySelectorAll('li[data-id]');
            if (e.key === 'ArrowDown') {{ selected = Math.min(selected+1, items.length-1); render(inputEl.value); e.preventDefault(); }}
            else if (e.key === 'ArrowUp') {{ selected = Math.max(selected-1, 0); render(inputEl.value); e.preventDefault(); }}
            else if (e.key === 'Enter' && items[selected]) {{ runCommand(items[selected].getAttribute('data-id')); e.preventDefault(); }}
            return;
          }}
          if (typing) return;
          if (e.key === '/') {{ e.preventDefault(); runCommand('focus-search'); return; }}
          if (e.key === '?') {{ e.preventDefault(); runCommand('help'); return; }}
          if (e.key === 't') {{ runCommand('toggle-theme'); return; }}
          if (e.key === 'r') {{ runCommand('rerun'); return; }}
          if (e.key === 'f') {{
            if (doc.fullscreenElement) doc.exitFullscreen(); else doc.documentElement.requestFullscreen();
            return;
          }}
          if (e.key === 'g') {{ gBuffer = Date.now(); return; }}
          if (gBuffer && (Date.now() - gBuffer) < 900) {{
            gBuffer = 0;
            if (e.key === 'd') runCommand('nav-dashboard');
            else if (e.key === 't') runCommand('nav-manage');
            else if (e.key === 'r') runCommand('nav-risk');
            else if (e.key === 's') runCommand('nav-simulator');
            else if (e.key === 'q') runCommand('nav-quality');
          }}
        }}, true);
      }} catch(e) {{}}
    }})();
    </script>
    """
    components.html(html, height=0, width=0)


# ── Advanced analytics helpers ───────────────────────────────────────────
def pp_herfindahl_index(values) -> float:
    """HHI concentration on [0,1].  0 = perfectly diversified."""
    arr = np.asarray([v for v in values if v and v > 0], dtype=float)
    if arr.size == 0:
        return 0.0
    w = arr / arr.sum()
    return float((w ** 2).sum())


def pp_historical_var(returns, alpha: float = 0.95) -> float:
    arr = np.asarray(list(returns), dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size == 0:
        return 0.0
    return float(-np.quantile(arr, 1.0 - alpha))


def pp_historical_cvar(returns, alpha: float = 0.95) -> float:
    arr = np.asarray(list(returns), dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size == 0:
        return 0.0
    q = np.quantile(arr, 1.0 - alpha)
    tail = arr[arr <= q]
    if tail.size == 0:
        return float(-q)
    return float(-tail.mean())


def pp_max_drawdown(series) -> float:
    arr = np.asarray(list(series), dtype=float)
    if arr.size == 0:
        return 0.0
    peak = np.maximum.accumulate(arr)
    dd = (arr - peak) / np.where(peak == 0, 1, peak)
    return float(dd.min())


def pp_sharpe_ratio(returns, rf_annual: float = 0.02, periods: int = 252) -> float:
    arr = np.asarray(list(returns), dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size < 2 or arr.std(ddof=1) == 0:
        return 0.0
    rf = rf_annual / periods
    return float((arr.mean() - rf) / arr.std(ddof=1) * _pp_math.sqrt(periods))


def pp_sortino_ratio(returns, rf_annual: float = 0.02, periods: int = 252) -> float:
    arr = np.asarray(list(returns), dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size < 2:
        return 0.0
    rf = rf_annual / periods
    downside = arr[arr < rf]
    dd = downside.std(ddof=1) if downside.size > 1 else 0.0
    if dd == 0:
        return 0.0
    return float((arr.mean() - rf) / dd * _pp_math.sqrt(periods))


def pp_calmar_ratio(returns, periods: int = 252) -> float:
    arr = np.asarray(list(returns), dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size < 2:
        return 0.0
    cum = (1 + arr).cumprod()
    mdd = abs(pp_max_drawdown(cum))
    if mdd == 0:
        return 0.0
    cagr = cum[-1] ** (periods / arr.size) - 1
    return float(cagr / mdd)


def pp_monte_carlo_projection(start_value: float,
                              daily_returns,
                              horizon_days: int = 252,
                              n_paths: int = 1000,
                              seed: _PP_Optional[int] = 42) -> pd.DataFrame:
    """Return a DataFrame with p10 / p50 / p90 / mean simulated paths."""
    arr = np.asarray(list(daily_returns), dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size < 5 or start_value <= 0 or horizon_days <= 0:
        return pd.DataFrame()
    rng = np.random.default_rng(seed)
    mu, sigma = arr.mean(), arr.std(ddof=1)
    sigma = max(sigma, 1e-6)
    shocks = rng.normal(mu, sigma, size=(n_paths, horizon_days))
    paths = start_value * np.cumprod(1 + shocks, axis=1)
    p10 = np.percentile(paths, 10, axis=0)
    p50 = np.percentile(paths, 50, axis=0)
    p90 = np.percentile(paths, 90, axis=0)
    mean = paths.mean(axis=0)
    return pd.DataFrame({"day": np.arange(1, horizon_days + 1),
                         "p10": p10, "p50": p50, "p90": p90, "mean": mean})


def pp_dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Portfolio") -> bytes:
    buf = _pp_io.BytesIO()
    try:
        with pd.ExcelWriter(buf, engine="xlsxwriter") as xw:
            df.to_excel(xw, sheet_name=sheet_name[:31], index=False)
    except Exception:
        try:
            with pd.ExcelWriter(buf, engine="openpyxl") as xw:
                df.to_excel(xw, sheet_name=sheet_name[:31], index=False)
        except Exception:
            return b""
    return buf.getvalue()


def pp_render_export_bar(df: pd.DataFrame, label_prefix: str = "portfolio",
                         is_he: bool = True) -> None:
    """Render a row of one-click export buttons for the supplied DataFrame."""
    if df is None or df.empty:
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button(
            ("⬇️ הורדה CSV" if is_he else "⬇️ CSV"),
            data=df.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"{label_prefix}_{ts}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with c2:
        st.download_button(
            ("⬇️ הורדה JSON" if is_he else "⬇️ JSON"),
            data=df.to_json(orient="records", force_ascii=False, indent=2).encode("utf-8"),
            file_name=f"{label_prefix}_{ts}.json",
            mime="application/json",
            use_container_width=True,
        )
    with c3:
        xls = pp_dataframe_to_excel_bytes(df, sheet_name=label_prefix)
        if xls:
            st.download_button(
                ("⬇️ הורדה Excel" if is_he else "⬇️ Excel"),
                data=xls,
                file_name=f"{label_prefix}_{ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
# ── Advanced Analytics Suite (professional-grade risk & performance) ────
def pp_portfolio_health_score(metrics: Dict[str, float],
                              hhi: float,
                              var_95: float) -> Tuple[int, str, str]:
    """Compute a 0-100 portfolio health score from risk metrics.

    Inputs: sharpe, annual vol, max drawdown (negative ok), CAGR, HHI, VaR 95.
    Returns: (score, label, color).
    """
    sharpe = float(metrics.get("sharpe", 0.0) or 0.0)
    vol = float(metrics.get("vol", 0.0) or 0.0)
    mdd = abs(float(metrics.get("mdd", 0.0) or 0.0))
    cagr = float(metrics.get("cagr", 0.0) or 0.0)
    # Each pillar in [0,100]
    s_sharpe = max(0.0, min(100.0, (sharpe + 0.5) / 2.5 * 100.0))        # -0.5..2.0 → 0..100
    s_drawdown = max(0.0, min(100.0, (1.0 - mdd / 0.40) * 100.0))          # 0%..40% → 100..0
    s_vol = max(0.0, min(100.0, (1.0 - vol / 0.45) * 100.0))               # 0..45% → 100..0
    s_cagr = max(0.0, min(100.0, (cagr + 0.05) / 0.30 * 100.0))            # -5%..25% → 0..100
    s_conc = max(0.0, min(100.0, (1.0 - max(0.0, hhi - 0.10) / 0.50) * 100.0))  # HHI .10..60 → 100..0
    s_var = max(0.0, min(100.0, (1.0 - max(0.0, var_95) / 0.08) * 100.0))  # 0..8% daily → 100..0
    score = int(round(0.25 * s_sharpe + 0.20 * s_drawdown + 0.15 * s_vol +
                      0.15 * s_cagr + 0.15 * s_conc + 0.10 * s_var))
    if score >= 75:
        return score, "Excellent", "#10b981"
    if score >= 55:
        return score, "Solid", "#22c55e"
    if score >= 40:
        return score, "Balanced", "#f59e0b"
    if score >= 25:
        return score, "Cautious", "#f97316"
    return score, "Fragile", "#ef4444"


def _chart_help(en: str, he: str, language: str) -> None:
    """Inline help badge that works on BOTH desktop hover-or-click AND mobile tap.

    Implementation:
    - SVG-based circle (guaranteed perfect 20×20, no font-metric drift).
    - Wrapped in <details><summary> so a tap toggles the popup natively.
    - Popup is position:absolute so it doesn't push surrounding charts.
    - Closing on outside tap is wired up by a tiny global JS shim injected
      once via _pp_inject_help_shim() (see main()).
    """
    raw = he if language in ("he", "עברית") else en
    text = (raw.replace("&", "&amp;")
               .replace("<", "&lt;")
               .replace(">", "&gt;")
               .replace('"', "&quot;")
               .replace("\n", "<br>"))
    st.markdown(
        f"""
<div class="pp-help-wrap">
  <details class="pp-help">
    <summary class="pp-help-summary" aria-label="Help" role="button">
      <svg viewBox="0 0 20 20" width="20" height="20" aria-hidden="true" focusable="false" class="pp-help-icon">
        <circle cx="10" cy="10" r="9" fill="none" stroke="currentColor" stroke-width="1.4"/>
        <text x="10" y="14.5" text-anchor="middle" font-size="12" font-weight="700" fill="currentColor"
              font-family="system-ui,-apple-system,Segoe UI,Roboto,sans-serif">?</text>
      </svg>
    </summary>
    <div class="pp-help-body" role="tooltip">{text}</div>
  </details>
</div>
""",
        unsafe_allow_html=True,
    )


def render_advanced_analytics(
    open_trades: pd.DataFrame,
    value_series: pd.Series,
    risk_metrics_dict: Dict[str, float],
    total_value: float,
    tr,
    template: str,
    is_dark: bool,
    is_mobile: bool,
    language: str,
) -> None:
    """Professional risk + performance analytics block.

    Renders: health-score gauge, risk KPI row (VaR/CVaR/Sortino/Calmar/HHI/Beta),
    drawdown chart, daily-returns distribution with VaR markers, 30-day rolling
    volatility, correlation heatmap, benchmark (SPY) comparison, and a Monte
    Carlo 1-year projection fan chart.  Also exposes a one-click export bar.
    """
    st.markdown(f"### {tr('Advanced Analytics Suite', 'מעבדת אנליטיקה מתקדמת')}")
    if value_series is None or value_series.empty or len(value_series) < 20:
        st.info(tr(
            "Not enough market history to run advanced analytics (need at least 20 trading days).",
            "אין מספיק היסטוריית שוק עבור האנליטיקה המתקדמת (נדרשים לפחות 20 ימי מסחר).",
        ))
        return

    vs = pd.to_numeric(value_series, errors="coerce").dropna()
    if vs.empty:
        return
    daily_ret = vs.pct_change().dropna()
    peak = vs.cummax()
    drawdown = (vs - peak) / peak.replace(0, np.nan)

    # Concentration (HHI) on open positions
    hhi = 0.0
    if not open_trades.empty and "Current_Value_ILS" in open_trades.columns:
        weights = open_trades.groupby("Ticker")["Current_Value_ILS"].sum()
        hhi = pp_herfindahl_index(weights.values)

    var_95 = pp_historical_var(daily_ret, alpha=0.95)
    cvar_95 = pp_historical_cvar(daily_ret, alpha=0.95)
    sortino = pp_sortino_ratio(daily_ret)
    calmar = pp_calmar_ratio(daily_ret)

    # Benchmark SPY (beta + comparison line)
    beta_val = float("nan")
    bench_close = pd.Series(dtype=float)
    try:
        bench_df = _download_close_matrix(("SPY",), days=max(len(vs) + 5, 120))
        if not bench_df.empty and "SPY" in bench_df.columns:
            bench_close = pd.to_numeric(bench_df["SPY"], errors="coerce").dropna()
            if not bench_close.empty:
                joined = pd.concat([vs.rename("pf"), bench_close.rename("spy")], axis=1).dropna()
                if len(joined) >= 20:
                    pf_ret = joined["pf"].pct_change().dropna()
                    bx_ret = joined["spy"].pct_change().dropna()
                    common = pf_ret.index.intersection(bx_ret.index)
                    if len(common) >= 20:
                        cov = np.cov(pf_ret.loc[common], bx_ret.loc[common], ddof=1)
                        if cov.shape == (2, 2) and cov[1, 1] > 0:
                            beta_val = float(cov[0, 1] / cov[1, 1])
    except Exception:
        pass

    score, score_label, score_color = pp_portfolio_health_score(risk_metrics_dict, hhi, var_95)

    gauge_fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        number={"font": {"size": 44, "color": score_color}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1,
                     "tickcolor": "#94a3b8" if is_dark else "#475569"},
            "bar": {"color": score_color, "thickness": 0.28},
            "bgcolor": "rgba(0,0,0,0)",
            "steps": [
                {"range": [0, 25], "color": "rgba(239,68,68,0.25)"},
                {"range": [25, 40], "color": "rgba(249,115,22,0.25)"},
                {"range": [40, 55], "color": "rgba(245,158,11,0.25)"},
                {"range": [55, 75], "color": "rgba(34,197,94,0.25)"},
                {"range": [75, 100], "color": "rgba(16,185,129,0.30)"},
            ],
            "threshold": {"line": {"color": score_color, "width": 4},
                          "thickness": 0.9, "value": score},
        },
        title={"text": f"<b>{tr('Portfolio Health Score', 'ציון בריאות התיק')}</b><br>"
                       f"<span style='font-size:0.9rem;color:{score_color}'>{score_label}</span>",
               "font": {"size": 15}},
    ))
    gauge_fig.update_layout(margin=dict(l=10, r=10, t=60, b=10), height=260 if not is_mobile else 230)

    gc1, gc2 = st.columns([1, 2]) if not is_mobile else (st.container(), st.container())
    with gc1:
        _chart_help(
            "Portfolio Health Score: a composite 0–100 score that synthesises SIX pillars into one headline number: (1) Sharpe — risk-adjusted return · (2) Max Drawdown — worst historical pain · (3) Annual Volatility — scale of daily swings · (4) CAGR — compounded annual growth · (5) HHI — how diversified you are · (6) VaR 95% — potential daily loss. Each pillar is normalised 0–100 against healthy benchmarks, then averaged with risk-weighted emphasis. Bands: 75+ = excellent · 55–75 = healthy · 40–55 = balanced, room to optimise · <40 = review allocation, diversification, or drawdown control. This is an educational overview — always cross-check individual pillars before acting.",
            "ציון בריאות התיק: ציון מרוכב 0-100 המסכם שישה עמודי-תווך למספר אחד: (1) שארפ — תשואה מתואמת סיכון · (2) משיכה מקסימלית — הכאב ההיסטורי החריף · (3) תנודתיות שנתית — עוצמת התנודות היומיות · (4) CAGR — צמיחה שנתית מורכבת · (5) HHI — ריכוזיות ופיזור · (6) VaR 95% — ההפסד היומי הפוטנציאלי. כל עמוד מנורמל 0-100 אל מול סף בריא, וממוצע משוקלל לפי דגש סיכון. טווחים: 75+ = מצוין · 55-75 = בריא · 40-55 = מאוזן, יש מקום לשיפור · <40 = יש לבחון הקצאה, פיזור או בקרת משיכה. זהו סיכום חינוכי — תמיד להצליב מול המדדים הבודדים לפני פעולה.",
            language,
        )
        st.plotly_chart(_apply_plotly_theme(gauge_fig, is_dark, is_mobile), theme="streamlit", use_container_width=True)
    with gc2:
        kpi_row = st.columns(3)
        kpi_row[0].metric(
            tr("Daily VaR 95%", "VaR יומי 95%"), f"{var_95:.2%}",
            tr("Potential loss / day", "הפסד פוטנציאלי ליום"),
            help=tr(
                "Value-at-Risk (VaR 95%). On a 'normal' trading day there is a 95% chance your loss will NOT exceed this percentage — and a 5% chance it will be worse. Example: a 2% VaR on a ₪100,000 portfolio means that on 1 day in 20 you could lose more than ₪2,000. Computed as the 5th percentile of the last 12 months of daily returns. Use it to sanity-check the size of daily swings; pair it with CVaR (below) to understand how bad those worst-5% days actually get.",
                "VaR (ערך בסיכון) ברמת ביטחון 95%: ביום מסחר 'רגיל' יש 95% סיכוי שההפסד לא יעלה על האחוז הזה — וב-5% מהימים ההפסד יהיה גרוע יותר. דוגמה: VaR של 2% על תיק בגודל ₪100,000 משמעותו שפעם ב-20 ימים אפשר להפסיד יותר מ-₪2,000. מחושב כאחוזון ה-5 של תשואות יומיות בשנה האחרונה. משמש לבדיקת סדר-גודל התנודות היומיות; יש לצרף אליו CVaR להבנת חומרת 5% הימים הקשים.",
            ),
        )
        kpi_row[1].metric(
            tr("Daily CVaR 95%", "CVaR יומי 95%"), f"{cvar_95:.2%}",
            tr("Expected tail loss", "הפסד ממוצע בזנב"),
            help=tr(
                "Conditional VaR / Expected Shortfall (CVaR 95%). Answers: 'when things do go badly, how bad on average?' It is the AVERAGE loss on the worst 5% of days — the 'tail' beyond VaR. CVaR is always ≥ VaR in magnitude and is the metric required by Basel III banking regulations because it better captures extreme (black-swan) risk. Rule of thumb: if CVaR is dramatically larger than VaR, your returns have a 'fat tail' — rare events tend to be catastrophic rather than just bad.",
                "CVaR / Expected Shortfall (תוחלת הפסד בזנב) ברמת 95%: עונה על 'כאשר רע, כמה רע בממוצע?'. זהו ההפסד הממוצע ב-5% הימים הגרועים ביותר — ה'זנב' שמעבר ל-VaR. CVaR תמיד גדול או שווה ל-VaR במוחלט, והוא המדד שנדרש בתקנות באזל III לבנקים, כי הוא תופס טוב יותר אירועי קיצון ('ברבור שחור'). כלל אצבע: אם CVaR גדול בהרבה מ-VaR — לתשואות יש 'זנב שמן', כלומר אירועים נדירים נוטים להיות הרסניים ולא רק רעים.",
            ),
        )
        kpi_row[2].metric(
            tr("Sortino", "סורטינו"), f"{sortino:.2f}",
            help=tr(
                "Sortino Ratio = (Return − Risk-Free Rate) / Downside Deviation. Unlike Sharpe, it punishes DOWN moves only — upside volatility is considered 'good volatility' and ignored. Interpretation: <1 weak · 1–2 OK · 2–3 good · >3 excellent. A Sortino much higher than Sharpe means your portfolio's volatility is asymmetric: most 'noise' comes from up-moves (which is the healthy kind). For long-only equity portfolios, 1.5–2.5 is a realistic target.",
                "יחס סורטינו = (תשואה − ריבית חסרת סיכון) / סטיית ירידה. בניגוד לשארפ — מעניש רק תנועות ירידה; תנודתיות בעליות נחשבת 'תנודתיות טובה' ומוסרת מהמשוואה. פרשנות: <1 חלש · 1-2 סביר · 2-3 טוב · >3 מצוין. סורטינו גבוה משמעותית משארפ מעיד על תיק שבו רוב ה'רעש' מגיע מעליות — וזה סימן בריא. ליעד עבור תיקי מניות long-only: 1.5-2.5.",
            ),
        )
        kpi_row2 = st.columns(3)
        kpi_row2[0].metric(
            tr("Calmar", "קלמאר"), f"{calmar:.2f}",
            help=tr(
                "Calmar Ratio = CAGR / |Max Drawdown|. Measures how much return you earn per unit of the worst pain you had to sit through. A Calmar of 0.5 means a 10% annual return cost you a 20% drawdown at some point. Interpretation: <0.5 weak · 0.5–1.0 acceptable · 1.0–3.0 strong · >3.0 exceptional (hedge-fund tier). Unlike Sharpe/Sortino which use volatility, Calmar uses ACTUAL HISTORICAL PAIN — many investors find it more intuitive because you can 'feel' drawdowns.",
                "יחס קלמאר = תשואה שנתית (CAGR) חלקי הירידה המרבית המוחלטת. מודד כמה תשואה מקבלים עבור כל יחידת 'כאב' שחווינו. קלמאר של 0.5 אומר שעל 10% רווח שנתי שילמת ירידה של 20% בדרך. פרשנות: <0.5 חלש · 0.5-1.0 סביר · 1.0-3.0 חזק · >3.0 יוצא-דופן (רמת קרן גידור). בניגוד לשארפ/סורטינו שמשתמשים בתנודתיות — קלמאר משתמש ב'כאב אמיתי' היסטורי, ולכן רבים מרגישים אותו אינטואיטיבי יותר.",
            ),
        )
        kpi_row2[1].metric(
            tr("Concentration (HHI)", "ריכוזיות (HHI)"), f"{hhi:.2f}",
            tr("0=diversified · 1=single-name", "0=מפוזר · 1=נכס יחיד"),
            help=tr(
                "Herfindahl-Hirschman Index (HHI) = Σ (weight_i)². Pure math: if you hold 4 assets at 25% each → HHI = 4×0.0625 = 0.25. If you hold 1 asset at 100% → HHI = 1.0. Regulatory/industry anchors: <0.15 = competitive/diversified · 0.15–0.25 = moderately concentrated · >0.25 = concentrated (the US DOJ's antitrust threshold). For a retail portfolio, aim for <0.20. HHI ignores correlation — you can have low HHI but if all assets are tech stocks, you're effectively concentrated. Always pair with the correlation heatmap.",
                "מדד הירפינדאל-הירשמן (HHI) = סכום (משקל_i)². מתמטיקה: אם מחזיקים 4 נכסים ב-25% כל אחד → HHI = 4×0.0625 = 0.25. אם נכס יחיד ב-100% → HHI = 1.0. עוגני פרשנות: <0.15 = מפוזר · 0.15-0.25 = ריכוזיות בינונית · >0.25 = ריכוזיות גבוהה (הסף לאכיפה אנטי-מונופוליסטית בארה\"ב). לתיק אישי מומלץ לחתור ל-<0.20. המדד מתעלם ממתאמים — אפשר HHI נמוך אבל אם כל הנכסים מניות טכנולוגיה אתה למעשה מרוכז. תמיד יש לצרף את מפת המתאמים.",
            ),
        )
        kpi_row2[2].metric(
            tr("Beta vs S&P 500", "ביטא מול S&P 500"),
            ("—" if not np.isfinite(beta_val) else f"{beta_val:.2f}"),
            help=tr(
                "Beta (β) measures how sensitive your portfolio is to moves in the S&P 500. Formula: β = Cov(portfolio, SPY) / Var(SPY). Interpretation: β=1.0 moves 1-for-1 with the market · β=1.5 amplifies market moves by +50% (aggressive) · β=0.5 absorbs half the market's move (defensive) · β<0 moves AGAINST the market (hedge). A β of 2 in a −10% market month typically means ≈ −20%. Note: β only captures LINEAR correlation with SPY — idiosyncratic (stock-specific) risk and tail-risk are NOT captured here.",
                "ביטא (β) מודדת את הרגישות של התיק לתנועות ה-S&P 500. נוסחה: β = קו-וריאנס(תיק, SPY) / שונות(SPY). פרשנות: β=1.0 תנועה זהה לשוק · β=1.5 מעצימה את תנועות השוק ב-50% (אגרסיבי) · β=0.5 סופגת חצי מתנועת השוק (דפנסיבי) · β<0 נעה הפוך לשוק (גידור). ביטא 2 בחודש של ירידה שוקית של 10% משמעותה ≈ 20% ירידה. שים לב: ביטא תופסת רק מתאם ליניארי עם SPY — סיכון ייחודי (specific risk) וסיכוני זנב אינם כלולים.",
            ),
        )
    try:
        style_metric_cards(border_left_color=score_color, border_radius_px=12, box_shadow=True)
    except Exception:
        pass

    st.markdown("&nbsp;", unsafe_allow_html=True)

    # Drawdown (underwater) chart
    row_a = st.columns(2) if not is_mobile else [st.container(), st.container()]
    with row_a[0]:
        _chart_help("Drawdown: how far the portfolio fell from its peak at any point in time.",
                    "עומק משיכה: עד כמה התיק ירד מהשיא שלו בכל נקודת זמן.", language)
        dd_fig = go.Figure()
        dd_fig.add_trace(go.Scatter(
            x=drawdown.index, y=drawdown.values * 100.0,
            mode="lines", name=tr("Drawdown %", "משיכה %"),
            line=dict(color="#ef4444", width=1.6),
            fill="tozeroy", fillcolor="rgba(239,68,68,0.18)",
            hovertemplate=tr("Date", "תאריך") + ": %{x|%Y-%m-%d}<br>" +
                          tr("Drawdown", "משיכה") + ": %{y:.2f}%<extra></extra>",
        ))
        dd_fig.update_layout(
            template=template,
            title=tr("Underwater Drawdown (%)", "עומק משיכה (%)"),
            yaxis_title="%", xaxis_title=tr("Date", "תאריך"),
            margin=dict(l=10, r=10, t=50, b=30),
            hovermode="x unified",
        )
        st.plotly_chart(_apply_plotly_theme(dd_fig, is_dark, is_mobile), theme="streamlit", use_container_width=True)

    # Returns distribution with VaR / CVaR markers
    with row_a[1]:
        _chart_help("Distribution of daily returns. Dashed lines mark VaR and CVaR loss thresholds.",
                    "פיזור התשואות היומיות. הקווים המקווקווים מסמנים את סף ההפסד של VaR ו-CVaR.", language)
        hist_fig = go.Figure()
        hist_fig.add_trace(go.Histogram(
            x=daily_ret.values * 100.0,
            nbinsx=40,
            marker=dict(color="#4f46e5", line=dict(color="#312e81", width=0.5)),
            opacity=0.85,
            name=tr("Daily returns", "תשואות יומיות"),
        ))
        hist_fig.add_vline(x=-var_95 * 100.0, line=dict(color="#f59e0b", dash="dash", width=2),
                           annotation_text="VaR 95%", annotation_position="top")
        hist_fig.add_vline(x=-cvar_95 * 100.0, line=dict(color="#ef4444", dash="dot", width=2),
                           annotation_text="CVaR 95%", annotation_position="bottom")
        hist_fig.update_layout(
            template=template,
            title=tr("Daily Returns Distribution", "פיזור תשואות יומיות"),
            xaxis_title="%", yaxis_title=tr("Frequency", "שכיחות"),
            margin=dict(l=10, r=10, t=50, b=30),
            showlegend=False, bargap=0.04,
        )
        st.plotly_chart(_apply_plotly_theme(hist_fig, is_dark, is_mobile), theme="streamlit", use_container_width=True)

    # Rolling 30-day annualized volatility
    row_b = st.columns(2) if not is_mobile else [st.container(), st.container()]
    with row_b[0]:
        _chart_help("Rolling 30-day volatility annualized: how 'noisy' the portfolio has been over recent months.",
                    "תנודתיות מתגלגלת 30 יום (שנתית): כמה 'רועשת' הייתה התשואה בחודשים האחרונים.", language)
        roll_vol = daily_ret.rolling(30).std() * np.sqrt(252) * 100.0
        roll_vol = roll_vol.dropna()
        if not roll_vol.empty:
            vol_fig = go.Figure()
            vol_fig.add_trace(go.Scatter(
                x=roll_vol.index, y=roll_vol.values,
                mode="lines", name=tr("30D Annualized Vol", "תנודתיות שנתית 30 יום"),
                line=dict(color="#06b6d4", width=2),
                fill="tozeroy", fillcolor="rgba(6,182,212,0.12)",
                hovertemplate="%{x|%Y-%m-%d}<br>%{y:.1f}%<extra></extra>",
            ))
            vol_fig.update_layout(
                template=template,
                title=tr("Rolling 30-Day Volatility (Annualized)", "תנודתיות מתגלגלת 30 יום (שנתית)"),
                yaxis_title="%", xaxis_title=tr("Date", "תאריך"),
                margin=dict(l=10, r=10, t=50, b=30), hovermode="x unified",
            )
            st.plotly_chart(_apply_plotly_theme(vol_fig, is_dark, is_mobile), theme="streamlit", use_container_width=True)
        else:
            st.info(tr("Not enough data for rolling volatility.", "אין מספיק נתונים לתנודתיות מתגלגלת."))

    # Benchmark comparison (normalized)
    with row_b[1]:
        _chart_help("Portfolio vs S&P 500 normalized to 100: compares growth rates from the same starting point.",
                    "תיק מול S&P 500 מנורמל ל-100: השוואת קצבי צמיחה מנקודת פתיחה זהה.", language)
        if not bench_close.empty:
            joined = pd.concat([vs.rename("pf"), bench_close.rename("spy")], axis=1).dropna()
            if len(joined) >= 5:
                norm = joined.div(joined.iloc[0]).mul(100.0)
                bm_fig = go.Figure()
                bm_fig.add_trace(go.Scatter(x=norm.index, y=norm["pf"], mode="lines",
                                            name=tr("Portfolio", "תיק"),
                                            line=dict(color="#4f46e5", width=2.2)))
                bm_fig.add_trace(go.Scatter(x=norm.index, y=norm["spy"], mode="lines",
                                            name="S&P 500 (SPY)",
                                            line=dict(color="#f59e0b", width=2, dash="dot")))
                bm_fig.update_layout(
                    template=template,
                    title=tr("Portfolio vs S&P 500 (Normalized to 100)",
                             "תיק מול S&P 500 (מנורמל ל-100)"),
                    yaxis_title=tr("Index", "מדד"), xaxis_title=tr("Date", "תאריך"),
                    margin=dict(l=10, r=10, t=50, b=30), hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                )
                st.plotly_chart(_apply_plotly_theme(bm_fig, is_dark, is_mobile), theme="streamlit", use_container_width=True)
            else:
                st.info(tr("Benchmark history unavailable.", "אין מספיק היסטוריית בנצ'מרק."))
        else:
            st.info(tr("Benchmark (SPY) unavailable — check internet / Yahoo access.",
                       "בנצ'מרק (SPY) לא זמין — בדוק גישה לרשת / Yahoo."))

    # Correlation heatmap between holdings
    if not open_trades.empty and "Ticker" in open_trades.columns:
        held = sorted({_clean(t).upper() for t in open_trades["Ticker"].tolist() if _clean(t)})
        if 2 <= len(held) <= 25:
            _chart_help("Correlation heatmap: values near +1 = assets move together, near -1 = opposite, near 0 = independent.",
                        "מפת מתאמים: ערכים קרובים ל-+1 = נכסים נעים יחד, ל-1- = הפוכים, ל-0 = בלתי תלויים.", language)
            try:
                symbols = tuple(_market_symbol(t) for t in held)
                close_mx = _download_close_matrix(symbols, days=252)
                if not close_mx.empty and close_mx.shape[1] >= 2:
                    ret_mx = close_mx.pct_change().dropna(how="all")
                    corr = ret_mx.corr().round(2)
                    sym_to_tkr = {_market_symbol(t): t for t in held}
                    display_labels = [sym_to_tkr.get(c, c) for c in corr.columns]
                    corr.columns = display_labels
                    corr.index = display_labels
                    heat_fig = px.imshow(
                        corr, color_continuous_scale="RdBu_r", zmin=-1, zmax=1,
                        text_auto=".2f", aspect="auto",
                        title=tr("Return Correlation Heatmap (1Y daily)",
                                 "מפת מתאמים של תשואות (יומי, שנה)"),
                    )
                    # On mobile: fixed large size so cells are readable; container scrolls.
                    n_assets = len(corr.columns)
                    cell_px = 70 if is_mobile else 60
                    heat_w = max(320, n_assets * cell_px + 40) if is_mobile else None
                    # Extra height on mobile for colorbar below chart
                    heat_h = max(320, n_assets * cell_px + (120 if is_mobile else 80)) if is_mobile else None

                    # Base layout
                    heat_layout = dict(
                        template=template,
                        margin=dict(l=50, r=10, t=60, b=20 if not is_mobile else 80),
                        width=heat_w,
                        height=heat_h,
                    )
                    # On mobile: move colorbar BELOW the chart (horizontal orientation)
                    # so the full matrix width is available for cells
                    if is_mobile:
                        heat_layout["coloraxis_colorbar"] = dict(
                            orientation="h",
                            x=0.5, y=-0.18,
                            xanchor="center", yanchor="top",
                            title=dict(text="", side="bottom"),
                            thickness=12,
                            len=0.75,
                        )
                    else:
                        heat_layout["coloraxis_colorbar"] = dict(title="")

                    heat_fig.update_layout(**heat_layout)

                    # Desktop: constrain to 80% viewport using column spacers
                    if not is_mobile:
                        _, hm_col, _ = st.columns([1, 4, 1])
                        with hm_col:
                            st.plotly_chart(
                                _apply_plotly_theme(heat_fig, is_dark, is_mobile),
                                use_container_width=True,
                                theme="streamlit",
                            )
                    else:
                        # Returns are in each asset's origin currency (USD/ILS native prices)
                        st.caption(tr(
                            "Correlations computed on daily returns in each asset's native currency.",
                            "המתאמים מחושבים על תשואות יומיות במטבע המקורי של כל נכס (USD/ILS).",
                        ))
                        # Wrap in a scrollable container so the full matrix is accessible
                        st.markdown(
                            "<div style='overflow-x:auto;overflow-y:hidden;"
                            "-webkit-overflow-scrolling:touch;width:100%;'>",
                            unsafe_allow_html=True,
                        )
                        st.plotly_chart(
                            _apply_plotly_theme(heat_fig, is_dark, is_mobile),
                            use_container_width=False,
                            theme="streamlit",
                        )
                        st.markdown("</div>", unsafe_allow_html=True)
            except Exception as _hm_exc:
                st.caption(f"⚠️ Heatmap error: {_hm_exc}")

    # Monte Carlo projection (1 year)
    _chart_help("Monte Carlo simulation: 1,000 random paths based on historical daily returns. Shows the range of possible portfolio values in 1 year.",
                "סימולציית מונטה-קרלו: 1,000 מסלולים אקראיים על בסיס תשואות יומיות היסטוריות. טווח שווי אפשרי לתיק בעוד שנה.", language)
    try:
        mc = pp_monte_carlo_projection(
            start_value=float(vs.iloc[-1]) if len(vs) else float(total_value),
            daily_returns=daily_ret.values,
            horizon_days=252, n_paths=1000,
        )
    except Exception:
        mc = pd.DataFrame()
    if not mc.empty:
        mc_fig = go.Figure()
        mc_fig.add_trace(go.Scatter(x=mc["day"], y=mc["p90"], mode="lines", name="P90",
                                    line=dict(color="rgba(16,185,129,0.0)", width=0),
                                    showlegend=False, hoverinfo="skip"))
        mc_fig.add_trace(go.Scatter(x=mc["day"], y=mc["p10"], mode="lines", name="P10-P90",
                                    fill="tonexty", fillcolor="rgba(79,70,229,0.18)",
                                    line=dict(color="rgba(79,70,229,0.0)", width=0)))
        mc_fig.add_trace(go.Scatter(x=mc["day"], y=mc["p50"], mode="lines",
                                    name=tr("Median", "חציון"),
                                    line=dict(color="#4f46e5", width=2.2)))
        mc_fig.add_trace(go.Scatter(x=mc["day"], y=mc["mean"], mode="lines",
                                    name=tr("Mean", "ממוצע"),
                                    line=dict(color="#f59e0b", width=1.6, dash="dash")))
        mc_fig.update_layout(
            template=template,
            title=tr("Monte-Carlo 1-Year Projection (1,000 paths)",
                     "הדמיית מונטה-קרלו ל-12 חודשים (1,000 מסלולים)"),
            xaxis_title=tr("Trading days", "ימי מסחר"),
            yaxis_title=tr("Projected Value", "שווי משוער"),
            margin=dict(l=10, r=10, t=50, b=30),
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        )
        st.plotly_chart(_apply_plotly_theme(mc_fig, is_dark, is_mobile), theme="streamlit", use_container_width=True)

        final_row = mc.iloc[-1]
        mc_cols = st.columns(3)
        mc_cols[0].metric(
            tr("1Y P10 (pessimistic)", "P10 לשנה (פסימי)"),
            f"₪{float(final_row['p10']):,.0f}",
            help=tr("10th percentile outcome: 90% of simulated paths ended above this value.",
                    "אחוזון 10: ב-90% מהסימולציות, שווי התיק היה גבוה מזה. תרחיש פסימי."),
        )
        mc_cols[1].metric(
            tr("1Y Median", "חציון לשנה"),
            f"₪{float(final_row['p50']):,.0f}",
            help=tr("Median outcome: half of simulated paths ended above and half below this value.",
                    "חציון: מחצית מהסימולציות הסתיימו מעל ומחצית מתחת לערך זה."),
        )
        mc_cols[2].metric(
            tr("1Y P90 (optimistic)", "P90 לשנה (אופטימי)"),
            f"₪{float(final_row['p90']):,.0f}",
            help=tr("90th percentile outcome: only 10% of simulated paths ended above this value.",
                    "אחוזון 90: רק 10% מהסימולציות הסתיימו מעל לערך זה. תרחיש אופטימי."),
        )

    # Export bar for analytics summary
    analytics_summary = pd.DataFrame([
        {"Metric": "Health Score",      "Value": score},
        {"Metric": "Sharpe",            "Value": round(float(risk_metrics_dict.get("sharpe", 0.0) or 0.0), 3)},
        {"Metric": "Sortino",           "Value": round(sortino, 3)},
        {"Metric": "Calmar",            "Value": round(calmar, 3)},
        {"Metric": "Annual Vol",        "Value": round(float(risk_metrics_dict.get("vol", 0.0) or 0.0), 4)},
        {"Metric": "Max Drawdown",      "Value": round(float(risk_metrics_dict.get("mdd", 0.0) or 0.0), 4)},
        {"Metric": "CAGR",              "Value": round(float(risk_metrics_dict.get("cagr", 0.0) or 0.0), 4)},
        {"Metric": "VaR 95 (daily)",    "Value": round(var_95, 4)},
        {"Metric": "CVaR 95 (daily)",   "Value": round(cvar_95, 4)},
        {"Metric": "HHI",               "Value": round(hhi, 4)},
        {"Metric": "Beta vs SPY",       "Value": (None if not np.isfinite(beta_val) else round(beta_val, 3))},
    ])
    st.caption(tr("Export analytics summary:", "ייצוא מסמך האנליטיקה:"))
    pp_render_export_bar(analytics_summary, label_prefix="analytics_summary",
                         is_he=(language == LANG_HE))
# ════════════════════════════════════════════════════════════════════════
# END OF PREMIUM ENHANCEMENTS
# ════════════════════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════════════════════
# SIMULATOR FEATURE — long-term projection + Safe Withdrawal (pension) model
# ════════════════════════════════════════════════════════════════════════
SIM_PREFS_FILE = Path(__file__).resolve().parent / "sim_user_prefs.json"
_SIM_PERSISTED_KEYS = (
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


def save_sim_prefs(values: Mapping[str, object]) -> bool:
    """Persist the simulator input snapshot atomically. Silent on failure so
    a broken disk never crashes the UI."""
    try:
        safe = {k: v for k, v in values.items() if k in _SIM_PERSISTED_KEYS}
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
    # Clamp: cannot lose more than ~100% in a month.
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

    Returns `float("inf")` if the target is never reached within `max_years`.
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


def render_simulator_page(
    tr,
    total_value: float,
    language: str,
    is_dark: bool,
    is_mobile: bool,
) -> None:
    """Render the Simulator page.

    Two modes:
      • Clean Simulator — user fills every input.
      • My Portfolio Simulator — Initial Capital is locked to live total_value.

    Outputs 4 KPIs, a chart (contributions / no-lump / with-lump),
    milestones, delay-cost insight, and a yearly breakdown + CSV export.
    """
    st.markdown(f"### 🧮 {tr('Simulator', 'סימולטור')}")
    st.caption(tr(
        "Long-term growth projection with monthly compounding, optional lump-sum, inflation adjustment and a sustainable monthly pension. Your inputs are saved automatically.",
        "הדמיית צמיחה ארוכת-טווח עם ריבית דריבית חודשית, תרומה חד-פעמית אפשרית, התאמה לאינפלציה ופנסיה חודשית ברת-קיימא. הקלטים נשמרים אוטומטית.",
    ))

    # ── Hydrate persisted inputs from disk (per-user saved state) on the
    # first visit during this browser session. We gate with a sentinel so a
    # stale disk-value doesn't keep overwriting the user's live edits.
    if not st.session_state.get("_sim_prefs_hydrated", False):
        try:
            _prefs = load_sim_prefs()
            for _k, _v in _prefs.items():
                if _k in _SIM_PERSISTED_KEYS and _k not in st.session_state:
                    st.session_state[_k] = _v
        except Exception:
            pass
        st.session_state["_sim_prefs_hydrated"] = True

    # Defensive type-coercion: Streamlit's number_input widget raises a
    # `TypeError: ... must be ... not str/int` when the session-state value
    # type doesn't match the widget signature (step=float ⟹ value must be
    # float, etc). JSON-deserialised prefs may produce ints where floats
    # are expected, OR earlier code may have stashed a string, so we
    # normalise every persisted key to its expected Python type here.
    _SIM_TYPES: Dict[str, Tuple[type, object]] = {
        "sim_age_now": (int, 30),
        "sim_age_target": (int, 67),
        "sim_initial_clean": (float, 0.0),
        "sim_annual_return": (float, 7.0),
        "sim_monthly_contrib": (float, 2000.0),
        "sim_lump_sum": (float, 0.0),
        "sim_lump_month": (int, 0),
        "sim_swr": (float, 4.0),
        "sim_annual_inflation": (float, 3.0),
    }
    for _k, (_typ, _default) in _SIM_TYPES.items():
        if _k in st.session_state:
            _val = st.session_state[_k]
            try:
                if _typ is float:
                    st.session_state[_k] = float(_val if _val is not None else _default)
                elif _typ is int:
                    st.session_state[_k] = int(float(_val) if _val is not None else _default)
            except (TypeError, ValueError):
                st.session_state[_k] = _default

    try:
        tv = float(total_value or 0.0)
    except (TypeError, ValueError):
        tv = 0.0
    has_portfolio = tv > 0.0

    mode_clean = tr("🧼 Clean Simulator", "🧼 סימולטור נקי")
    mode_mine = tr("💼 My Portfolio Simulator", "💼 סימולטור התיק שלי")
    mode_options = [mode_mine, mode_clean] if has_portfolio else [mode_clean]

    default_mode = mode_mine if has_portfolio else mode_clean
    if st.session_state.get("sim_mode_choice") not in mode_options:
        st.session_state["sim_mode_choice"] = default_mode

    mode = st.radio(
        tr("Mode", "מצב"),
        mode_options,
        horizontal=True,
        key="sim_mode_choice",
    )

    use_portfolio = (mode == mode_mine) and has_portfolio

    # ── Inputs (grouped in glassy bordered container) ─────────────────
    with st.container(border=True):
        st.markdown(f"**{tr('Parameters', 'פרמטרים')}**")
        col_l, col_r = (st.columns(2) if not is_mobile else (st.container(), st.container()))

        with col_l:
            # Initialize defaults in session-state first so widgets with keys
            # never violate updated min_value constraints across reruns.
            if "sim_age_now" not in st.session_state:
                st.session_state["sim_age_now"] = 30
            age_now = st.number_input(
                tr("Your current age", "הגיל הנוכחי שלך"),
                min_value=10, max_value=90,
                step=1,
                key="sim_age_now",
                help=tr("Starting age — used only to compute the horizon.",
                        "גיל התחלה — משמש רק לחישוב משך ההשקעה."),
            )
            # Keep target age strictly above current age at all times.
            min_target = int(age_now) + 1
            if "sim_age_target" not in st.session_state:
                st.session_state["sim_age_target"] = max(min_target, 67)
            elif int(st.session_state["sim_age_target"]) < min_target:
                st.session_state["sim_age_target"] = min_target
            age_target = st.number_input(
                tr("Target retirement age", "גיל פרישה מתוכנן"),
                min_value=min_target, max_value=100,
                step=1,
                key="sim_age_target",
                help=tr("Projection ends at this age.", "ההדמיה מסתיימת בגיל זה."),
            )

            if use_portfolio:
                initial_capital = tv
                # Force the locked widget's session value to the live portfolio value.
                st.session_state["sim_initial_mine"] = float(round(tv, 2))
                st.number_input(
                    tr("Initial capital (₪) — locked to your portfolio",
                       "הון התחלתי (₪) — נעול לשווי התיק שלך"),
                    step=100.0,
                    disabled=True,
                    key="sim_initial_mine",
                    help=tr("Auto-seeded from the live portfolio total value.",
                            "מוזן אוטומטית משווי התיק הנוכחי."),
                )
                st.info(tr(
                    f"Seeded from your portfolio: ₪{tv:,.0f}",
                    f"הוזן משווי התיק שלך: ₪{tv:,.0f}",
                ))
            else:
                if "sim_initial_clean" not in st.session_state:
                    st.session_state["sim_initial_clean"] = 0.0
                initial_capital = float(st.number_input(
                    tr("Initial capital (₪)", "הון התחלתי (₪)"),
                    min_value=0.0,
                    step=1000.0,
                    key="sim_initial_clean",
                    help=tr("Starting amount at month 0.", "הסכום בנקודת הזמן 0."),
                ))

        with col_r:
            if "sim_annual_return" not in st.session_state:
                st.session_state["sim_annual_return"] = 7.0
            annual_return = float(st.number_input(
                tr("Expected annual return (%)", "תשואה שנתית צפויה (%)"),
                min_value=-20.0, max_value=40.0,
                step=0.25,
                key="sim_annual_return",
                help=tr("S&P 500 long-run average ≈ 7% real / 10% nominal.",
                        "ממוצע רב-שנתי של S&P 500: ≈7% ריאלי / 10% נומינלי."),
            ))
            if "sim_monthly_contrib" not in st.session_state:
                st.session_state["sim_monthly_contrib"] = 2000.0
            monthly_contrib = float(st.number_input(
                tr("Monthly contribution (₪)", "הפקדה חודשית (₪)"),
                min_value=0.0,
                step=100.0,
                key="sim_monthly_contrib",
                help=tr("Amount added at the end of every month.",
                        "סכום שמתווסף בסוף כל חודש."),
            ))
            if "sim_lump_sum" not in st.session_state:
                st.session_state["sim_lump_sum"] = 0.0
            lump_sum = float(st.number_input(
                tr("One-time lump sum (₪)", "הפקדה חד-פעמית (₪)"),
                min_value=0.0,
                step=1000.0,
                key="sim_lump_sum",
                help=tr("A single extra deposit (e.g. bonus, inheritance).",
                        "הפקדה נוספת חד-פעמית (למשל בונוס, ירושה)."),
            ))

        years_total = max(1.0, float(age_target) - float(age_now))
        months_total = int(round(years_total * 12))

        # Clamp lump-month into the current slider range (horizon may have
        # changed between reruns when the user adjusts current/target age).
        _slider_max = max(1, months_total)
        if "sim_lump_month" not in st.session_state:
            st.session_state["sim_lump_month"] = 0
        elif int(st.session_state["sim_lump_month"]) > _slider_max:
            st.session_state["sim_lump_month"] = _slider_max
        elif int(st.session_state["sim_lump_month"]) < 0:
            st.session_state["sim_lump_month"] = 0
        lump_month = st.slider(
            tr("Lump-sum timing (months from today)",
               "תזמון ההפקדה החד-פעמית (חודשים מהיום)"),
            min_value=0,
            max_value=_slider_max,
            step=1,
            key="sim_lump_month",
            disabled=(lump_sum <= 0.0),
            help=tr("0 = today. Later = less compounding time.",
                    "0 = היום. מאוחר יותר = פחות זמן לריבית דריבית."),
        )

        if "sim_swr" not in st.session_state:
            st.session_state["sim_swr"] = 4.0
        swr_pct = float(st.slider(
            tr("Safe Withdrawal Rate (SWR %)", "שיעור משיכה בטוח (SWR %)"),
            min_value=3.0, max_value=5.0,
            step=0.25,
            key="sim_swr",
            help=tr(
                "Trinity-study rule of thumb: the % of your final balance you can safely withdraw EVERY YEAR without depleting the portfolio over a 30-year retirement. 4% is the classic default; 3.5% is conservative (factors in lower future returns); 5% is aggressive (high sequence-of-returns risk). The withdrawal amount is adjusted for inflation each year. Derived from historical 30-year rolling windows of a 50/50 stock/bond portfolio that survived 95%+ of cases at 4%.",
                "כלל אצבע ממחקר Trinity: האחוז מהיתרה הסופית שניתן למשוך כל שנה מבלי לרוקן את התיק לאורך 30 שנות פרישה. 4% הוא ברירת-המחדל הקלאסית; 3.5% שמרני (בהנחת תשואות עתידיות נמוכות); 5% אגרסיבי (סיכון 'סדר התשואות' גבוה). סכום המשיכה מותאם לאינפלציה מדי שנה. נגזר מחלונות גלגול של 30 שנה היסטוריים של תיק 50/50 מניות/אג\"ח ששרדו מעל 95% מהתרחישים ב-4%.",
            ),
        ))

        # ── NEW: Annual Inflation (converts nominal → REAL final value) ──
        if "sim_annual_inflation" not in st.session_state:
            st.session_state["sim_annual_inflation"] = 3.0
        inflation_pct = float(st.number_input(
            tr("Annual inflation (%)", "אינפלציה שנתית (%)"),
            min_value=0.0, max_value=20.0,
            step=0.25,
            key="sim_annual_inflation",
            help=tr(
                "Discounts future nominal values back into TODAY's purchasing power. "
                "If inflation runs at 3% for 30 years, ₪1,000,000 nominal will buy roughly what ₪412,000 buys today. "
                "Israel CPI long-run ≈ 2–3% · USA CPI long-run ≈ 3% · hyper-inflation scenarios 10%+. "
                "Set to 0 for a pure NOMINAL projection (no adjustment).",
                "היוון (discount) של שוויים עתידיים נומינליים לכוח הקנייה של היום. "
                "אם האינפלציה 3% לאורך 30 שנה, ₪1,000,000 נומינליים יקנו בערך מה ש-₪412,000 קונים היום. "
                "אינפלציה ישראלית ארוכת-טווח ≈ 2-3% · אינפלציה אמריקאית ארוכת-טווח ≈ 3% · היפר-אינפלציה 10%+. "
                "הזן 0 לתחזית נומינלית טהורה (ללא התאמה).",
            ),
        ))

    # ── Projection ────────────────────────────────────────────────────
    df_proj = sim_project_portfolio(
        initial_capital=initial_capital,
        monthly_contribution=monthly_contrib,
        annual_return_pct=annual_return,
        years=years_total,
        lump_sum=lump_sum,
        lump_sum_month=int(lump_month),
    )

    final = df_proj.iloc[-1]
    final_with = float(final["balance_with_lump"])
    final_without = float(final["balance_no_lump"])
    total_contributed = float(final["contributions_cum"])
    compound_gains = max(0.0, final_with - total_contributed - (lump_sum if lump_sum else 0.0))
    monthly_pension = sim_safe_withdrawal_monthly(final_with, swr_pct)
    annual_pension = monthly_pension * 12.0
    lump_uplift = final_with - final_without

    # Inflation-adjusted ("real") equivalents in today's purchasing power.
    _infl_factor = (1.0 + inflation_pct / 100.0) ** float(years_total) if inflation_pct > 0 else 1.0
    real_final = final_with / _infl_factor
    real_monthly_pension = monthly_pension / _infl_factor
    real_compound_gains = compound_gains / _infl_factor

    # ── Persist on every rerun (save-on-change, no sidebar button) ──
    try:
        save_sim_prefs({k: st.session_state.get(k) for k in _SIM_PERSISTED_KEYS})
    except Exception:
        pass

    # ── KPI row — NOMINAL ─────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    _real_delta_final = (
        f"≈ ₪{real_final:,.0f} " + tr("in today's ₪", "בכוח קנייה של היום")
    ) if inflation_pct > 0 else None
    k1.metric(
        tr("Final value", "שווי סופי"),
        f"₪{final_with:,.0f}",
        delta=_real_delta_final,
        help=tr(
            "Nominal projected balance at target age (with lump sum). The delta shows the REAL value in today's purchasing power after deflating by the inflation rate.",
            "יתרה נומינלית חזויה בגיל היעד (כולל הפקדה חד-פעמית). הדלתא מציגה את השווי הריאלי בכוח הקנייה של היום לאחר היוון באינפלציה.",
        ),
    )
    k2.metric(
        tr("Total contributed", "סך תרומות"),
        f"₪{(total_contributed + lump_sum):,.0f}",
        help=tr("Initial capital + monthly contributions + lump sum.",
                "הון התחלתי + הפקדות חודשיות + הפקדה חד-פעמית."),
    )
    _real_delta_gains = (
        f"≈ ₪{real_compound_gains:,.0f} " + tr("real", "ריאלי")
    ) if inflation_pct > 0 else None
    k3.metric(
        tr("Compound gains", "רווחי ריבית דריבית"),
        f"₪{compound_gains:,.0f}",
        delta=_real_delta_gains,
        help=tr(
            "Growth beyond the money you put in — 'interest on interest'. Real delta discounts inflation over the horizon.",
            "הצמיחה מעבר לסכום שהפקדת — 'ריבית על ריבית'. הדלתא הריאלית מבטלת את השפעת האינפלציה לאורך התקופה.",
        ),
    )
    _real_delta_pension = (
        f"≈ ₪{real_monthly_pension:,.0f} " + tr("in today's ₪", "בכוח קנייה של היום")
    ) if inflation_pct > 0 else f"₪{annual_pension:,.0f} / {tr('year', 'שנה')}"
    k4.metric(
        tr("Monthly pension (SWR)", "פנסיה חודשית (SWR)"),
        f"₪{monthly_pension:,.0f}",
        delta=_real_delta_pension,
        help=tr("Sustainable monthly withdrawal at the chosen SWR. Real equivalent shown in today's purchasing power when inflation > 0.",
                "משיכה חודשית ברת-קיימא לפי שיעור המשיכה שנבחר. השווי הריאלי מוצג בכוח הקנייה של היום כאשר האינפלציה גדולה מ-0."),
    )

    # Real-value context banner
    if inflation_pct > 0:
        _real_pct_loss = (1.0 - 1.0 / _infl_factor) * 100.0
        st.caption(tr(
            f"💡 With {inflation_pct:.2f}% annual inflation over {years_total:.1f} years, nominal value loses ≈ {_real_pct_loss:.1f}% of its purchasing power. The 'real' (in today's ₪) equivalent is what your future self will actually feel.",
            f"💡 באינפלציה של {inflation_pct:.2f}% לשנה לאורך {years_total:.1f} שנים, השווי הנומינלי מאבד כ-{_real_pct_loss:.1f}% מכוח הקנייה. השווי הריאלי (בכוח הקנייה של היום) הוא מה שתרגיש באמת בעתיד.",
        ))

    # ── Chart ─────────────────────────────────────────────────────────
    chart_height = 340 if is_mobile else 420
    fig = go.Figure()
    # Layer 1: cumulative contributions (baseline)
    fig.add_trace(go.Scatter(
        x=df_proj["year"], y=df_proj["contributions_cum"],
        mode="lines",
        name=tr("Contributions (baseline)", "תרומות (קו בסיס)"),
        line=dict(color="#94a3b8", width=1.4, dash="dot"),
        hovertemplate=tr("Year", "שנה") + ": %{x:.1f}<br>"
                      + tr("Contributed", "הופקד") + ": ₪%{y:,.0f}<extra></extra>",
    ))
    # Layer 2: balance without lump
    fig.add_trace(go.Scatter(
        x=df_proj["year"], y=df_proj["balance_no_lump"],
        mode="lines",
        name=tr("Balance (no lump)", "יתרה ללא חד-פעמית"),
        line=dict(color="#06b6d4", width=1.8, dash="dash"),
        hovertemplate=tr("Year", "שנה") + ": %{x:.1f}<br>"
                      + tr("Balance", "יתרה") + ": ₪%{y:,.0f}<extra></extra>",
    ))
    # Layer 3: balance with lump (main line)
    fig.add_trace(go.Scatter(
        x=df_proj["year"], y=df_proj["balance_with_lump"],
        mode="lines",
        name=tr("Balance (with lump)", "יתרה עם חד-פעמית"),
        line=dict(color="#6366f1", width=2.4),
        fill="tozeroy",
        fillcolor="rgba(99,102,241,0.10)",
        hovertemplate=tr("Year", "שנה") + ": %{x:.1f}<br>"
                      + tr("Balance", "יתרה") + ": ₪%{y:,.0f}<extra></extra>",
    ))
    # Injection marker
    if lump_sum > 0:
        inj_year = float(lump_month) / 12.0
        fig.add_vline(
            x=inj_year,
            line=dict(color="#f59e0b", width=1.4, dash="dashdot"),
            annotation_text=tr("Lump-sum", "הפקדה"),
            annotation_position="top",
        )

    template = "plotly_dark" if is_dark else "plotly_white"
    fig.update_layout(
        template=template,
        title=tr("Long-term Projection", "הדמיה ארוכת-טווח"),
        xaxis_title=tr("Years", "שנים"),
        yaxis_title=tr("Value (₪)", "שווי (₪)"),
        margin=dict(l=10, r=10, t=56, b=30),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        height=chart_height,
    )
    st.plotly_chart(
        _apply_plotly_theme(fig, is_dark, is_mobile),
        theme="streamlit",
        use_container_width=True,
    )

    # ── Delay-cost insight ─────────────────────────────────────────────
    if lump_sum > 0 and lump_month > 0:
        r_m = (1.0 + annual_return / 100.0) ** (1.0 / 12.0) - 1.0
        # pure_uplift: compound growth of the lump over the full horizon
        pure_uplift = lump_sum * ((1.0 + r_m) ** months_total - 1.0)
        # fraction lost due to delay
        d_months = int(lump_month)
        delay_cost = pure_uplift * (1.0 - (1.0 + r_m) ** (-d_months))
        if delay_cost > 0:
            st.caption(tr(
                f"⏳ Delaying the lump-sum by {d_months} months costs roughly ₪{delay_cost:,.0f} in forgone compounding.",
                f"⏳ דחיית ההפקדה החד-פעמית ב-{d_months} חודשים מאבדת כ-₪{delay_cost:,.0f} בריבית דריבית.",
            ))

    # ── Milestones ─────────────────────────────────────────────────────
    if initial_capital > 0 or lump_sum > 0:
        base = max(initial_capital, 1.0)
        ms = st.columns(3)
        for idx, mult in enumerate([2, 5, 10]):
            target = base * mult
            yrs = sim_years_to_target(
                initial_capital=initial_capital,
                monthly_contribution=monthly_contrib,
                annual_return_pct=annual_return,
                target_balance=target,
                lump_sum=lump_sum,
                max_years=80,
            )
            if np.isfinite(yrs):
                ms[idx].metric(
                    f"×{mult} {tr('capital', 'הון')}",
                    f"{yrs:.1f} " + tr("years", "שנים"),
                    help=tr(f"Years until balance reaches ₪{target:,.0f}.",
                            f"שנים עד שהיתרה תגיע ל-₪{target:,.0f}."),
                )
            else:
                ms[idx].metric(
                    f"×{mult} {tr('capital', 'הון')}",
                    tr(">80y", ">80ש'"),
                    help=tr(f"Target ₪{target:,.0f} not reached within 80 years.",
                            f"היעד ₪{target:,.0f} לא מושג תוך 80 שנה."),
                )

    # ── Yearly breakdown + CSV export ─────────────────────────────────
    with st.expander(tr("📋 Yearly breakdown", "📋 פירוט שנתי")):
        yearly = df_proj.iloc[::12].copy()
        yearly = yearly.assign(
            Year=yearly["year"].round(1),
            Contributed=yearly["contributions_cum"].round(0),
            BalanceNoLump=yearly["balance_no_lump"].round(0),
            BalanceWithLump=yearly["balance_with_lump"].round(0),
        )[["Year", "Contributed", "BalanceNoLump", "BalanceWithLump"]].rename(columns={
            "Year": tr("Year", "שנה"),
            "Contributed": tr("Contributed (₪)", "הופקד (₪)"),
            "BalanceNoLump": tr("Balance no lump (₪)", "יתרה ללא חד-פעמית (₪)"),
            "BalanceWithLump": tr("Balance with lump (₪)", "יתרה עם חד-פעמית (₪)"),
        })
        st.dataframe(yearly, use_container_width=True, hide_index=True)
        csv = yearly.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label=tr("⬇ Download yearly breakdown (CSV)", "⬇ הורדת הפירוט השנתי (CSV)"),
            data=csv,
            file_name="simulator_yearly.csv",
            mime="text/csv",
            use_container_width=is_mobile,
        )

    st.caption(tr(
        "⚠ Projections are educational estimates only — not investment advice. Actual returns vary.",
        "⚠ ההדמיה היא לצורכי הדגמה בלבד — אינה מהווה ייעוץ השקעות. תשואות בפועל עשויות להשתנות.",
    ))

    # Reset stored inputs (button lives at the bottom since it's rarely needed).
    _reset_cols = st.columns([3, 1]) if not is_mobile else (st.container(), st.container())
    with _reset_cols[1]:
        if st.button(
            tr("🔄 Reset inputs", "🔄 איפוס קלטים"),
            key="sim_reset_prefs",
            use_container_width=True,
            help=tr(
                "Clear the saved simulator values and restore factory defaults.",
                "איפוס הערכים השמורים של הסימולטור לברירות-המחדל.",
            ),
        ):
            for _k in _SIM_PERSISTED_KEYS:
                st.session_state.pop(_k, None)
            st.session_state.pop("_sim_prefs_hydrated", None)
            try:
                if SIM_PREFS_FILE.exists():
                    SIM_PREFS_FILE.unlink()
            except Exception:
                pass
            st.rerun()


def main() -> None:
    st.set_page_config(page_title="מערכת ניהול תיק", page_icon="📈", layout="wide", initial_sidebar_state="auto")

    # ══════════════════════════════════════════════════════════════════════
    # Premium CSS — Bug fixes, mobile-only enhancements, sidebar polish
    # (Applied globally; mobile-specific rules wrapped in @media max-width 768px)
    # ══════════════════════════════════════════════════════════════════════
    st.markdown(
        """
        <style>
        /* ── Keep Streamlit's top header VISIBLE — it hosts the 3-dots menu
             (Clear Cache / Rerun / Settings) and sidebar-toggle. On desktop
             we make the header *transparent + floating* so it doesn't push
             content down but the toolbar remains tappable. On mobile it
             stays as a regular bar.                                        ── */
        footer, [data-testid="stDecoration"] {
            display: none !important; visibility: hidden !important; height: 0 !important;
        }
        /* Desktop: transparent, zero-height header — but *keep* its toolbar
           actions visible and floating at the top-right corner. */
        @media (min-width: 768px) {
            header[data-testid="stHeader"] {
                background: transparent !important;
                box-shadow: none !important;
                border: none !important;
                height: 0 !important;
                min-height: 0 !important;
                /* overflow: visible so the 3-dots button and sidebar-toggle
                   remain clickable even though the bar itself is zero-height */
                overflow: visible !important;
                position: fixed !important;
                top: 0 !important;
                right: 0 !important;
                left: 0 !important;
                z-index: 999999 !important;
                pointer-events: none !important;   /* bar itself doesn't block clicks */
            }
            header[data-testid="stHeader"] [data-testid="stToolbar"],
            header[data-testid="stHeader"] [data-testid="stToolbarActions"],
            header[data-testid="stHeader"] [data-testid="stMainMenuButton"],
            header[data-testid="stHeader"] button {
                pointer-events: auto !important;   /* but its buttons do */
            }
        }
        /* Explicitly ensure the collapse/expand sidebar control AND the
           3-dots Main-Menu stay visible in all viewports. */
        [data-testid="collapsedControl"],
        [data-testid="stSidebarCollapseButton"],
        [data-testid="stSidebarCollapsedControl"],
        [data-testid="stMainMenuButton"],
        [data-testid="stToolbar"],
        [data-testid="stToolbarActions"],
        button[aria-label*="sidebar"],
        button[aria-label*="menu" i],
        button[kind="header"],
        #MainMenu {
            display: flex !important;
            visibility: visible !important;
            opacity: 1 !important;
            pointer-events: auto !important;
            z-index: 999999 !important;
        }

        /* ── Strict upper bound: title hugs top ── */
        section.main > div.block-container,
        [data-testid="stMainBlockContainer"],
        .main .block-container,
        .block-container {
            padding-top: 0 !important;
            margin-top: 0 !important;
        }

        /* ── Native LTR rendering for English tickers & numbers inside RTL ── */
        .ltr, [data-ltr="1"], .ticker, .mono-num,
        [data-testid="stMetricValue"], [data-testid="stMetricDelta"],
        code, kbd, samp {
            direction: ltr !important;
            unicode-bidi: isolate !important;
            font-variant-numeric: tabular-nums;
        }
        /* Table cells containing tickers/numbers: isolate LTR inside RTL doc */
        [data-testid="stDataFrame"] td, [data-testid="stDataFrame"] th,
        [data-testid="stTable"] td, [data-testid="stTable"] th {
            unicode-bidi: plaintext;
        }

        /* ── Sidebar polish: hide hamburger/footer, RTL-friendly expander arrows ── */
        [data-testid="stSidebarNav"], [data-testid="stSidebarUserContent"] + div footer {
            display: none !important;
        }
        /* Expander arrows: force LTR so they align on the correct side in RTL layout */
        [data-testid="stSidebar"] details[data-testid="stExpander"] > summary,
        [data-testid="stSidebar"] div[data-testid="stExpander"] > details > summary {
            direction: ltr !important;
            display: flex !important;
            flex-direction: row-reverse !important;
            justify-content: space-between !important;
            align-items: center !important;
        }
        [data-testid="stSidebar"] [data-testid="stExpander"] svg {
            transition: transform 180ms ease;
        }

        /* ── Professional CTA-style buttons (all viewports — lifted on mobile) ── */
        .stButton > button, .stDownloadButton > button,
        [data-testid="stFormSubmitButton"] button {
            border-radius: 12px !important;
            font-weight: 600 !important;
            letter-spacing: 0.01em;
            transition: transform 120ms ease, box-shadow 180ms ease, filter 180ms ease !important;
            box-shadow: 0 2px 8px -2px rgba(15,23,42,0.18);
        }
        .stButton > button:hover, .stDownloadButton > button:hover,
        [data-testid="stFormSubmitButton"] button:hover {
            transform: translateY(-1px);
            box-shadow: 0 6px 18px -4px rgba(79,70,229,0.35);
            filter: brightness(1.04);
        }
        .stButton > button:active, .stDownloadButton > button:active {
            transform: translateY(0);
            filter: brightness(0.97);
        }

        /* ── Tab click reliability: guarantee first-click handling (no double-click) ── */
        [data-baseweb="tab-list"] [data-baseweb="tab"],
        [data-testid="stSidebar"] .nav-link,
        [data-testid="stSidebar"] a.nav-link,
        div[role="tablist"] [role="tab"],
        [data-testid="stRadio"] [data-baseweb="radio"] {
            cursor: pointer !important;
            -webkit-tap-highlight-color: transparent;
            touch-action: manipulation;
            pointer-events: auto !important;
            user-select: none;
        }
        /* Disable any fade/transform that can eat the first click event */
        [data-baseweb="tab-list"] [data-baseweb="tab"] *,
        [data-testid="stSidebar"] .nav-link * {
            pointer-events: none !important;
        }

        /* ── MOBILE-ONLY ENHANCEMENTS (≤767px) ─────────────────────────────── */
        @media (max-width: 767px) {
            /* ══ CRITICAL: Force horizontal scroll on ALL tables — no cell wrapping ══ */
            [data-testid="stDataFrame"],
            [data-testid="stTable"] {
                overflow-x: auto !important;
                -webkit-overflow-scrolling: touch !important;
                display: block !important;
                max-width: 100vw !important;
                width: 100% !important;
            }
            [data-testid="stDataFrame"] table,
            [data-testid="stTable"] table {
                min-width: 640px !important;
                width: max-content !important;
            }
            [data-testid="stDataFrame"] th,
            [data-testid="stDataFrame"] td,
            [data-testid="stTable"] th,
            [data-testid="stTable"] td {
                white-space: nowrap !important;
                min-width: 120px !important;
                padding: 6px 10px !important;
                overflow: visible !important;
            }
            [data-testid="stDataFrame"] > div,
            [data-testid="stDataFrame"] > div > div {
                overflow-x: auto !important;
                -webkit-overflow-scrolling: touch !important;
            }
            /* Scrollable wrapper around tables */
            .mobile-table-scroll {
                overflow-x: auto !important;
                -webkit-overflow-scrolling: touch;
                border-radius: 12px;
            }
            /* ══ KPI: Large values, muted labels ══ */
            [data-testid="stMetricValue"],
            [data-testid="stMetric"] [data-testid="stMetricValue"] {
                font-size: 1.5rem !important;
                font-weight: bold !important;
            }
            [data-testid="stMetricLabel"],
            [data-testid="stMetric"] [data-testid="stMetricLabel"] {
                font-size: 0.875rem !important;
                color: #6b7280 !important;
            }
            /* ══ Scrollable chart wrappers ══ */
            [data-testid="stPlotlyChart"] {
                overflow-x: auto !important;
                -webkit-overflow-scrolling: touch !important;
            }
            /* CTA buttons: thumb-friendly on mobile */
            .stButton > button, .stDownloadButton > button,
            [data-testid="stFormSubmitButton"] button {
                min-height: 46px !important;
                font-size: 15px !important;
                padding: 10px 16px !important;
                border-radius: 14px !important;
                box-shadow: 0 4px 14px -4px rgba(79,70,229,0.35) !important;
            }
        }
        /* ── DESKTOP-ONLY ENHANCEMENTS (≥768px) ─────────────────────────────── */
        @media (min-width: 768px) {
            /* Remove Streamlit's default top whitespace — push title to top */
            section.main > div.block-container,
            [data-testid="stMainBlockContainer"],
            .main .block-container,
            .block-container {
                padding-top: 0rem !important;
                margin-top: 0rem !important;
            }
            /* Header: transparent + zero-height but the 3-dots Main Menu
               (Clear Cache / Rerun / Settings) stays floating at top-right */
            header[data-testid="stHeader"] {
                background: transparent !important;
                box-shadow: none !important;
                border: none !important;
                height: 0 !important;
                min-height: 0 !important;
                overflow: visible !important;
                position: fixed !important;
                top: 0 !important; right: 0 !important; left: 0 !important;
                z-index: 999999 !important;
                pointer-events: none !important;
            }
            header[data-testid="stHeader"] button,
            header[data-testid="stHeader"] [data-testid="stToolbar"],
            header[data-testid="stHeader"] [data-testid="stToolbarActions"],
            header[data-testid="stHeader"] [data-testid="stMainMenuButton"] {
                pointer-events: auto !important;
            }
        }
        </style>

        <script>
        // ══════════════════════════════════════════════════════════════════
        // FIRST-CLICK NAVIGATION RELIABILITY FIX
        // Eliminates the "need to click twice" bug on:
        //   • st.tabs headers (Overview / Allocation / Risk / Transactions)
        //   • streamlit-option-menu sidebar nav-links (Dashboard / Trades / …)
        //   • mobile st.radio segmented control (top 4-way page switch)
        // Strategy: optimistic ARIA hint on pointerdown + keyboard bindings +
        //           MutationObserver so dynamically-added tabs are also covered.
        // ══════════════════════════════════════════════════════════════════
        (function () {
            var NAV_SELECTOR = [
                '[data-baseweb="tab"]',
                '[role="tab"]',
                '[data-testid="stSidebar"] .nav-link',
                '[data-testid="stSidebar"] a.nav-link',
                '[data-testid="stRadio"] [data-baseweb="radio"]'
            ].join(',');

            function harden(el) {
                if (el.__ppHardened) return;
                el.__ppHardened = true;
                try { el.setAttribute('tabindex', '0'); } catch (e) {}

                // Optimistic ARIA hint on pointerdown — removes the "dead first click"
                // perception even before Streamlit finishes its rerender cycle.
                el.addEventListener('pointerdown', function () {
                    try {
                        var list = el.closest('[data-baseweb="tab-list"], [role="tablist"], [data-testid="stRadio"], .nav');
                        if (list) {
                            list.querySelectorAll('[aria-selected="true"]').forEach(function (n) {
                                if (n !== el) n.setAttribute('aria-selected', 'false');
                            });
                        }
                        el.setAttribute('aria-selected', 'true');
                    } catch (e) {}
                }, { passive: true, capture: true });

                // Keyboard: Space / Enter should trigger on first press.
                el.addEventListener('keydown', function (ev) {
                    if (ev.key === 'Enter' || ev.key === ' ') {
                        ev.preventDefault();
                        try { el.click(); } catch (e) {}
                    }
                });
            }

            function scan(root) {
                (root || document).querySelectorAll(NAV_SELECTOR).forEach(harden);
            }

            scan();
            try {
                var mo = new MutationObserver(function (muts) {
                    for (var i = 0; i < muts.length; i++) {
                        var m = muts[i];
                        if (!m.addedNodes) continue;
                        for (var j = 0; j < m.addedNodes.length; j++) {
                            var n = m.addedNodes[j];
                            if (n.nodeType !== 1) continue;
                            if (n.matches && n.matches(NAV_SELECTOR)) harden(n);
                            if (n.querySelectorAll) scan(n);
                        }
                    }
                });
                mo.observe(document.documentElement, { childList: true, subtree: true });
            } catch (e) {}
        })();
        </script>
        """,
        unsafe_allow_html=True,
    )

    settings = load_local_settings()
    language_default = _clean(settings.get("language", DEFAULT_LANGUAGE)) or DEFAULT_LANGUAGE
    if language_default not in {LANG_EN, LANG_HE}:
        language_default = DEFAULT_LANGUAGE
    theme_default = _normalize_theme_mode(settings.get("theme_mode", THEME_SYSTEM))

    if "demo_mode_persist" not in st.session_state:
        st.session_state["demo_mode_persist"] = bool(settings.get("demo_mode", False))

    language = st.sidebar.selectbox("Language" if language_default == LANG_EN else "שפה", [LANG_EN, LANG_HE], index=0 if language_default == LANG_EN else 1)
    tr = (lambda en, he: he if language == LANG_HE else en)
    theme_label_to_value = {
        tr("System", "מערכת"): THEME_SYSTEM,
        tr("Light", "בהיר"): THEME_LIGHT,
        tr("Dark", "כהה"): THEME_DARK,
    }
    theme_value_to_label = {v: k for k, v in theme_label_to_value.items()}
    default_theme_label = theme_value_to_label.get(theme_default, tr("System", "מערכת"))
    appearance_label = st.sidebar.selectbox(
        tr("Appearance", "תצוגה"),
        list(theme_label_to_value.keys()),
        index=list(theme_label_to_value.keys()).index(default_theme_label),
    )
    theme_mode = theme_label_to_value.get(appearance_label, THEME_SYSTEM)
    _default_chart_lock = _is_mobile_client()
    chart_lock = st.sidebar.checkbox(
        tr("🔒 Lock charts (smooth scroll)", "🔒 נעילת תרשימים (גלילה חלקה)"),
        value=bool(st.session_state.get("chart_lock_persist", _default_chart_lock)),
        key="chart_lock_persist",
    )
    demo_mode = st.sidebar.checkbox(
        tr("Demo view", "מצב הדגמה"),
        value=bool(st.session_state.get("demo_mode_persist", False)),
        key="demo_mode_persist",
    )
    live_updates = st.sidebar.checkbox(tr("Live updates", "עדכון חי"), value=False)
    refresh_seconds = st.sidebar.selectbox(
        tr("Refresh every", "רענון כל"),
        [5, 10, 15, 30, 60],
        index=2,
        disabled=not live_updates,
    )

    inject_global_styles(language, theme_mode)
    inject_client_fixes()
    inject_dark_dropdown_fix(_resolve_theme_base(theme_mode) == "dark")

    # Chart lock: apply/remove lock class on all Plotly charts in parent document
    _lock_action = "add" if chart_lock else "remove"
    components.html(f"""<script>(function(){{
      try {{
        var d = window.parent ? window.parent.document : document;
        function applyLock() {{
          d.querySelectorAll('[data-testid="stPlotlyChart"]').forEach(function(c){{
            c.classList.{_lock_action}('pp-chart-locked');
          }});
        }}
        applyLock();
        // Keep observing indefinitely so new charts on other tabs get locked too
        var obs = new MutationObserver(function(){{ requestAnimationFrame(applyLock); }});
        obs.observe(d.body, {{childList:true, subtree:true}});
      }} catch(e){{}}
    }})();</script>""", height=0, width=0)

    # ── Premium polish, PWA meta tags, keyboard shortcuts, command palette ──
    try:
        apply_premium_polish(
            language=language,
            is_dark=_resolve_theme_base(theme_mode) == "dark",
            is_mobile=_is_mobile_client(),
        )
    except Exception:
        pass

    _render_premium_sidebar_lottie(language)
    if not _is_mobile_client():
        _space(16)

    page_dashboard = tr("Dashboard", "דשבורד")
    page_manage = tr("Trade Management", "ניהול עסקאות")
    page_risk = tr("Risk & FIFO", "סיכונים ופיפו") if not _is_mobile_client() else tr("Risk", "סיכון")
    page_simulator = tr("Simulator", "סימולטור")
    page_quality = tr("Data Quality", "בקרת נתונים")
    page_id_to_label = {
        "dashboard": page_dashboard,
        "manage": page_manage,
        "risk": page_risk,
        "simulator": page_simulator,
        "quality": page_quality,
    }
    page_order = ["dashboard", "manage", "risk", "simulator", "quality"]
    active_page_id = _clean(st.session_state.get("active_page_id", "dashboard")) or "dashboard"
    if active_page_id not in page_id_to_label:
        active_page_id = "dashboard"
    is_mobile = _is_mobile_client()
    theme_base = _resolve_theme_base(theme_mode)
    is_dark = theme_base == "dark"
    template = "plotly_dark" if is_dark else "plotly_white"

    if is_mobile:
        # ── Mobile: top segmented control for page nav ──
        # Per UX review, 5 pills felt crowded — "Data" was moved to a dedicated
        # sidebar button above the Data Connection Settings expander.
        mobile_label_to_id = {
            tr("📊 Overview", "📊 סקירה"): "dashboard",
            tr("💼 Trades", "💼 עסקאות"): "manage",
            tr("🛡 Risk", "🛡 סיכון"): "risk",
            tr("🧮 Sim", "🧮 סימולטור"): "simulator",
        }
        mobile_options = list(mobile_label_to_id.keys())

        # If we're on the "Data" page (entered via the sidebar button), show
        # a compact breadcrumb + back button rather than the 4-pill radio so
        # the page switcher doesn't force-override the selection.
        if active_page_id == "quality":
            brc1, brc2 = st.columns([4, 1])
            with brc1:
                st.markdown(
                    f"<div style='display:flex;align-items:center;gap:8px;"
                    f"padding:8px 12px;border-radius:10px;"
                    f"background:rgba(99,102,241,0.08);"
                    f"border:1px solid rgba(99,102,241,0.25);"
                    f"font-weight:600;unicode-bidi:plaintext'>📋 "
                    f"{tr('Data', 'נתונים')}</div>",
                    unsafe_allow_html=True,
                )
            with brc2:
                if st.button(
                    tr("⬅ Back", "⬅ חזור"),
                    use_container_width=True,
                    key="mobile_data_back",
                ):
                    st.session_state["active_page_id"] = "dashboard"
                    st.rerun()
        else:
            active_mobile_index = 0
            for i, lbl in enumerate(mobile_options):
                if mobile_label_to_id.get(lbl) == active_page_id:
                    active_mobile_index = i
                    break
            # Sync widget state BEFORE creating the widget so single-click works.
            _mobile_nav_key = "mobile_page_nav_radio"
            _expected_label = mobile_options[active_mobile_index]
            if st.session_state.get(_mobile_nav_key) not in mobile_options:
                st.session_state[_mobile_nav_key] = _expected_label
            page_choice = st.radio(
                tr("Page", "עמוד"),
                mobile_options,
                horizontal=True,
                label_visibility="collapsed",
                key=_mobile_nav_key,
            )
            active_page_id = mobile_label_to_id.get(page_choice, "dashboard")
    else:
        # ── Desktop: sidebar option-menu navigation ──
        st.sidebar.title(tr("Navigation", "ניווט"))
        page_options = [page_id_to_label[p] for p in page_order]
        active_page_index = page_order.index(active_page_id) if active_page_id in page_order else 0
        if option_menu is not None:
            nav_container_bg = "#1E1E1E" if is_dark else "#f8f9fa"
            nav_container_border = "0px solid transparent"
            nav_icon_color = "#93c5fd" if is_dark else "#2563eb"
            nav_text_color = "#e2e8f0" if is_dark else "#0f172a"
            nav_hover_color = "#334155" if is_dark else "#e5e7eb"
            nav_selected_bg = "#374151" if is_dark else "#e2e8f0"
            nav_selected_text = "#ffffff" if is_dark else "#0f172a"
            with st.sidebar:
                # NOTE: streamlit-option-menu requires a stable `key` so the
                # component's selected state persists across reruns. Without a
                # key each rerun rebuilds the component from default_index and
                # "swallows" the first click (classic double-click bug).
                page = option_menu(
                    menu_title=None,
                    options=page_options,
                    icons=["house", "wallet", "shield-check", "calculator", "database-check"],
                    default_index=active_page_index,
                    key="main_nav_option_menu",
                    orientation="vertical",
                    styles={
                        "container": {
                            "padding": "0.32rem 0.2rem",
                            "background-color": nav_container_bg,
                            "border-radius": "10px",
                            "border": nav_container_border,
                            "direction": "ltr",
                        },
                        "icon": {"color": nav_icon_color, "font-size": "16px"},
                        "nav-link": {
                            "font-size": "14px",
                            "text-align": "left",
                            "direction": "ltr",
                            "margin": "2px 0",
                            "padding": "10px 12px 10px 12px",
                            "border-radius": "8px",
                            "--hover-color": nav_hover_color,
                            "color": nav_text_color,
                        },
                        "nav-link-selected": {
                            "background-color": nav_selected_bg,
                            "color": nav_selected_text,
                            "font-weight": "600",
                            "text-align": "left",
                            "direction": "ltr",
                        },
                    },
                )
                label_to_id = {v: k for k, v in page_id_to_label.items()}
                active_page_id = label_to_id.get(page, "dashboard")
        else:
            st.sidebar.caption(tr("Install streamlit-option-menu for enhanced navigation.", "להשלמת תפריט הניווט יש להתקין streamlit-option-menu."))
            page = st.sidebar.selectbox(tr("Page", "עמוד"), page_options, index=active_page_index)
            label_to_id = {v: k for k, v in page_id_to_label.items()}
            active_page_id = label_to_id.get(page, "dashboard")

    st.session_state["active_page_id"] = active_page_id
    page = page_id_to_label.get(active_page_id, page_dashboard)

    # ── Subtle per-page accent color (color-coded navigation) ─────────
    # A thin gradient strip under the header + a matching accent on the
    # active sidebar nav-link makes orientation instantly clear, without
    # changing the overall palette.
    _page_accents = {
        "dashboard": "#6366f1",   # indigo — overview
        "manage":    "#10b981",   # emerald — active portfolio actions
        "risk":      "#ef4444",   # rose — risk / warning family
        "simulator": "#f59e0b",   # amber — exploration / projection
        "quality":   "#06b6d4",   # cyan — data hygiene
    }
    _accent = _page_accents.get(active_page_id, "#6366f1")
    st.markdown(
        f"""
        <style id="pp-page-accent">
            .app-header-wrap {{
                border-top: 3px solid {_accent} !important;
                box-shadow: 0 2px 8px -4px {_accent}55 !important;
            }}
            .app-sub-title {{ color: {_accent} !important; }}
            /* Sidebar active nav-link gets an accent border on the leading edge */
            [data-testid="stSidebar"] .nav-link-selected,
            [data-testid="stSidebar"] .nav-link.active {{
                border-left: 3px solid {_accent} !important;
                box-shadow: inset 3px 0 0 0 {_accent} !important;
            }}
            /* Mobile pill accent on the current page */
            [data-testid="stRadio"] [data-baseweb="radio"]:has(input:checked) {{
                box-shadow: 0 4px 14px -2px {_accent}88 !important;
            }}
            /* stMetric subtle left accent bar on active page */
            [data-testid="stMetric"]::before {{
                content: "";
                position: absolute;
                top: 12%; bottom: 12%; left: 0;
                width: 2px;
                background: {_accent};
                border-radius: 2px;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""<div class='app-header-wrap'>
  <div class='app-logo-row'>
    <div class='app-logo-text'>
      <h1 class='app-main-title'>{tr('Portfolio OS', 'מערכת ניהול תיק')}</h1>
      <div class='app-logo-badge'>{tr('Smart Portfolio Management', 'ניהול השקעות חכם')}</div>
    </div>
    <div class='app-logo-icon'>
      <svg width='48' height='48' viewBox='0 0 48 48' fill='none' xmlns='http://www.w3.org/2000/svg'>
        <rect width='48' height='48' rx='13' fill='url(#logoGrad)'/>
        <polyline points='9,33 17,22 24,27 32,14 39,19' stroke='white' stroke-width='3' stroke-linecap='round' stroke-linejoin='round' fill='none'/>
        <circle cx='39' cy='19' r='3' fill='white'/>
        <rect x='9' y='35' width='6' height='6' rx='2' fill='rgba(255,255,255,0.65)'/>
        <rect x='18' y='30' width='6' height='11' rx='2' fill='rgba(255,255,255,0.8)'/>
        <rect x='27' y='25' width='6' height='16' rx='2' fill='white'/>
        <defs>
          <linearGradient id='logoGrad' x1='0' y1='0' x2='48' y2='48' gradientUnits='userSpaceOnUse'>
            <stop offset='0%' stop-color='#6366f1'/>
            <stop offset='60%' stop-color='#818cf8'/>
            <stop offset='100%' stop-color='#38bdf8'/>
          </linearGradient>
        </defs>
      </svg>
    </div>
  </div>
  <div class='app-sub-title'>{page}</div>
</div>""",
        unsafe_allow_html=True,
    )

    # ── Mobile-only: dedicated Data (Data Quality) button above the
    # Connection & Data Settings expander. Desktop keeps Data Quality in
    # the sidebar option_menu, so this button would be redundant there.
    if is_mobile:
        _data_btn_primary = (active_page_id == "quality")
        if st.sidebar.button(
            ("📋 " + tr("Data", "נתונים")) + ("  ✓" if _data_btn_primary else ""),
            use_container_width=True,
            type=("primary" if _data_btn_primary else "secondary"),
            help=tr("Open the Data / Data Quality page.",
                    "פתיחת דף הנתונים (בקרת איכות נתונים)."),
            key="sidebar_data_nav_btn",
        ):
            st.session_state["active_page_id"] = "quality"
            st.rerun()

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
        followed_symbols_text = settings.get("followed_symbols", "")
        st.caption(
            tr(
                "TradingView watchlist is fixed to the dashboard default list.",
                "רשימת המעקב של TradingView קבועה לפי רשימת ברירת המחדל בדשבורד.",
            )
        )

        if st.button(tr("Save settings on this machine", "שמור חיבור למחשב הזה")):
            ok = save_local_settings(
                web_app_url,
                api_token,
                spreadsheet_ref,
                worksheet_name,
                service_account_file,
                language,
                theme_mode,
                demo_mode,
                followed_symbols_text,
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

    loading_placeholder = st.empty()
    if st_lottie is not None:
        loading_anim = _load_lottie_json("https://assets2.lottiefiles.com/packages/lf20_jcikwtux.json")
        if loading_anim:
            with loading_placeholder.container():
                st_lottie(loading_anim, height=90, speed=1, loop=True, key="pm_data_loading")

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
            exc_text = str(exc)
            if _is_timeout_error(exc):
                st.error(tr("Google data read failed due to timeout.", "קריאת נתונים מגוגל נכשלה בגלל Timeout."))
                st.info(
                    tr(
                        "Apps Script timed out. The app will auto-use local backup data when available; otherwise retry in a minute.",
                        "Apps Script חרג מזמן התגובה. האפליקציה תעבור אוטומטית לגיבוי מקומי אם קיים; אחרת נסה שוב בעוד דקה.",
                    )
                )
                st.caption(exc_text)
            else:
                st.error(f"{tr('Google data read failed', 'קריאת נתונים מגוגל נכשלה')}: {exc_text}")
            st.stop()

    loading_placeholder.empty()

    web_url_clean = _clean(settings.get("web_app_url", DEFAULT_WEB_APP_URL) if demo_mode else web_app_url)

    if connection_state_box is not None:
        if source_mode == "gspread":
            connection_state_box.warning(tr("gspread read mode active (write actions disabled).", "מצב קריאה דרך gspread פעיל (ללא Web App פעיל, פעולות עריכה/מחיקה מושבתות)."))
        elif source_mode == "local_cache":
            connection_state_box.warning(tr("Local backup cache is active (latest remote read was unavailable).", "גיבוי מקומי פעיל (הקריאה האחרונה מהשרת לא היתה זמינה)."))
        elif source_mode == "verified_fallback":
            connection_state_box.warning(tr("Verified fallback data is active (read-only emergency mode).", "נתוני גיבוי מאומתים פעילים (מצב חירום לקריאה בלבד)."))
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

        stats_text = (
            f"{len(trades):,} {tr('rows loaded', 'רשומות נטענו')} | "
            f"{len(trades[trades['Record_Source'] == 'STATE_SNAPSHOT']):,} {tr('snapshot rows', 'שורות תמונת מצב')} | "
            f"{len(trades[trades['Status'] == 'סגור']):,} {tr('closed', 'סגורות')}"
        )
        st.markdown(f"<div class='dashboard-stats-line'>{stats_text}</div>", unsafe_allow_html=True)

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
            Current_Price=("מחיר שוק", "max"),
            Open_Qty=("Quantity", "sum"),
            Cost_ILS=("Cost_ILS", "sum"),
            Value_ILS=("Current_Value_ILS", "sum"),
            Cost_Origin=("Cost_Origin_With_Fee", "sum"),
            Value_Origin=("Value_Origin_Est", "sum"),
        )
        summary["Net_PnL_ILS"] = summary["Value_ILS"] - summary["Cost_ILS"]
        summary["Yield_Origin"] = np.where(summary["Cost_Origin"] > 0, (summary["Value_Origin"] - summary["Cost_Origin"]) / summary["Cost_Origin"], 0.0)
        summary["Yield_ILS"] = np.where(summary["Cost_ILS"] > 0, summary["Net_PnL_ILS"] / summary["Cost_ILS"], 0.0)

        # Net P/L by asset uses only open trades — closed positions have no sell price stored,
        # so their Current_Value_ILS=0 would incorrectly show the full cost as a loss.
        pnl_source = open_trades.copy() if not open_trades.empty else pd.DataFrame(columns=["Ticker", "Cost_ILS", "Current_Value_ILS"])
        for col in ["Ticker", "Cost_ILS", "Current_Value_ILS"]:
            if col not in pnl_source.columns:
                pnl_source[col] = 0.0 if col != "Ticker" else ""
        pnl_source["Ticker"] = pnl_source["Ticker"].map(_clean)
        pnl_source = pnl_source[pnl_source["Ticker"] != ""]
        pnl_source["Cost_ILS"] = pnl_source["Cost_ILS"].map(_num)
        pnl_source["Current_Value_ILS"] = pnl_source["Current_Value_ILS"].map(_num)
        pnl_by_asset = pnl_source.groupby("Ticker", as_index=False).agg(
            Cost_ILS=("Cost_ILS", "sum"),
            Current_Value_ILS=("Current_Value_ILS", "sum"),
        ) if not pnl_source.empty else pd.DataFrame(columns=["Ticker", "Cost_ILS", "Current_Value_ILS"])
        if not pnl_by_asset.empty:
            pnl_by_asset["Net_PnL_ILS"] = pnl_by_asset["Current_Value_ILS"] - pnl_by_asset["Cost_ILS"]

        total_value_txt = f"{total_value:,.0f}"
        total_cost_txt = f"{total_cost:,.0f}"
        total_return_txt = f"{total_return:.2%}"
        top_holding_value_txt = "--"
        top_holding_delta_txt = tr("No open assets", "אין נכסים פתוחים")
        if not summary.empty:
            total_summary_value = float(summary["Value_ILS"].sum())
            if total_summary_value > 0:
                top_row = summary.loc[summary["Value_ILS"].idxmax()]
                top_weight = float(top_row["Value_ILS"]) / total_summary_value
                top_ticker = _clean(top_row.get("Ticker", "")) or "-"
                top_holding_value_txt = f"{top_weight:.1%}"
                top_holding_delta_txt = f"{top_ticker} | ₪{float(top_row['Value_ILS']):,.0f}"
        total_profit_txt = f"{total_profit:+,.0f} ₪"
        if is_mobile:
            # ── Mobile: 2x2 compact grid ──
            kpi_r1 = st.columns(2)
            kpi_r1[0].metric(tr("Total Value", "שווי כולל"), total_value_txt, f"{total_profit:,.0f} ₪")
            kpi_r1[1].metric(tr("Open P&L", "רווח/הפסד פתוח (₪)"), total_profit_txt)
            kpi_r2 = st.columns(2)
            kpi_r2[0].metric(tr("Return", "תשואה כוללת"), total_return_txt, f"{total_profit:,.0f} ₪")
            kpi_r2[1].metric(
                tr("Top Holding", "אחזקה מובילה"),
                top_holding_value_txt,
                top_holding_delta_txt,
            )
        else:
            # ── Desktop: 4 columns in a row ──
            kpi_cols = st.columns(4)
            kpi_cols[0].metric(tr("Total Value (ILS)", "שווי כולל (₪)"), total_value_txt, f"{total_profit:,.0f} ₪")
            kpi_cols[1].metric(tr("Open P&L (ILS)", "רווח/הפסד פתוח (₪)"), total_profit_txt)
            kpi_cols[2].metric(tr("Total Return", "תשואה כוללת"), total_return_txt)
            kpi_cols[3].metric(
                tr("Closed Positions", "פוזיציות סגורות"),
                str(len(closed_trades)),
                f"{len(open_trades)} {tr('open', 'פתוחות')}",
            )
        style_metric_cards(border_left_color="#4f46e5", border_radius_px=12, box_shadow=True)

        class_mix = pd.DataFrame(columns=["Asset_Class", "Current_Value_ILS", "Assets"])
        if not open_trades.empty and {"Ticker", "Type", "Current_Value_ILS"}.issubset(open_trades.columns):
            class_work = open_trades[["Ticker", "Type", "Current_Value_ILS"]].copy()
            class_work["Ticker"] = class_work["Ticker"].map(lambda v: _clean(v).upper())
            class_work["Type"] = class_work["Type"].map(_clean)

            def _class_bucket(row: pd.Series) -> str:
                ticker = _clean(row.get("Ticker", "")).upper()
                type_text = _clean(row.get("Type", ""))
                type_upper = type_text.upper()
                if ticker in CHART_STOCK_TICKERS:
                    return tr("Stocks", "מניות")
                if (type_text == "קריפטו") or (type_upper == "CRYPTO") or (ticker in CHART_CRYPTO_TICKERS):
                    return tr("Crypto", "קריפטו")
                if "ETF" in type_upper or ticker in CHART_FORCED_ETF_TICKERS:
                    return tr("ETF", "ETF")
                return tr("Stocks", "מניות")

            class_work["Asset_Class"] = class_work.apply(_class_bucket, axis=1)
            class_mix = class_work.groupby("Asset_Class", as_index=False)["Current_Value_ILS"].sum().sort_values("Current_Value_ILS", ascending=False)
            class_assets = (
                class_work.groupby("Asset_Class")["Ticker"]
                .apply(lambda s: ", ".join(sorted({_clean(v).upper() for v in s.tolist() if _clean(v)})))
                .reset_index(name="Assets")
            )
            class_mix = class_mix.merge(class_assets, on="Asset_Class", how="left")

        perf_cols = {"Purchase_Date", "Cost_ILS", "Current_Value_ILS"}
        can_show_build_up = (not open_trades.empty) and perf_cols.issubset(open_trades.columns)

        tab_overview_real, tab_allocation_real, tab_risk_real, tab_tx_real = st.tabs(
            [
                tr("Overview", "סקירה") if is_mobile else tr("Overview", "סקירה כללית"),
                tr("Allocation", "הרכב") if is_mobile else tr("Portfolio Allocation", "הרכב התיק"),
                tr("Reports", "דוחות") if is_mobile else tr("Reports & Analytics", "דוחות ואנליזה"),
                tr("Transactions", "עסקאות") if is_mobile else tr("Transactions & Cash Flow", "תנועות ועסקאות"),
            ]
        )

        # ── Slot containers inside each tab guarantee the required visual order ──
        with tab_overview_real:
            _ov_equity_slot = st.container()   # Equity curve directly under KPIs
            _ov_body_slot = st.container()     # Reserved for future Overview extras
        with tab_allocation_real:
            _alloc_charts_slot = st.container()    # Pie + Bar + Treemap (from old Overview)
            _alloc_classes_slot = st.container()   # Class-allocation (from old Allocation)
            _alloc_exposure_slot = st.container()  # Exposure table at VERY BOTTOM
        with tab_tx_real:
            _tx_deposits_slot = st.container()     # Manual deposits
            _tx_transactions_slot = st.container() # Transactions table

        # Re-map legacy variable names so existing `with tab_X:` blocks below route
        # into the correct new slot without rewriting their bodies.
        tab_overview = _ov_body_slot
        tab_allocation = _alloc_classes_slot
        tab_reports = tab_risk_real
        tab_deposits = _tx_deposits_slot
        tab_transactions = _tx_transactions_slot

        with _alloc_charts_slot:
            col_a, col_b = st.columns(2)
            with col_a:
                fig_pie = px.pie(
                    summary,
                    names="Ticker",
                    values="Value_ILS",
                    title=tr("Portfolio Allocation by Asset", "חלוקת תיק לפי נכס"),
                    hole=0.52,
                    template=template,
                    color_discrete_sequence=_BRAND_PALETTE,
                )
                fig_pie.update_layout(
                    legend=dict(orientation="h", yanchor="top", y=-0.12, xanchor="center", x=0.5),
                    margin=dict(l=10, r=10, t=40, b=40),
                )
                fig_pie.update_traces(
                    hovertemplate="<b>%{label}</b><br>₪%{value:,.0f}<br>%{percent}<extra></extra>",
                    textinfo="percent+label",
                )
                st.plotly_chart(_apply_plotly_theme(fig_pie, is_dark, is_mobile), theme="streamlit", use_container_width=True)
            with col_b:
                bar_df = pnl_by_asset if not pnl_by_asset.empty else summary[["Ticker", "Net_PnL_ILS"]].copy()
                _bar_src = bar_df.sort_values("Net_PnL_ILS", ascending=False).copy()
                _bar_src["_color"] = _bar_src["Net_PnL_ILS"].apply(lambda v: "Profit" if v >= 0 else "Loss")
                fig_bar = px.bar(
                    _bar_src,
                    x="Ticker",
                    y="Net_PnL_ILS",
                    color="_color",
                    color_discrete_map={"Profit": "#16a34a", "Loss": "#dc2626"},
                    title=tr("Net P/L by Asset", "רווח/הפסד לפי נכס"),
                    template=template,
                    labels={"Net_PnL_ILS": tr("Net P/L (ILS)", "רווח/הפסד (₪)"), "_color": ""},
                )
                fig_bar.update_layout(
                    showlegend=False,
                    yaxis_tickformat=",.0f",
                    hovermode="x unified",
                )
                fig_bar.update_traces(
                    hovertemplate="<b>%{x}</b><br>P/L: ₪%{y:,.0f}<extra></extra>"
                )
                st.plotly_chart(_apply_plotly_theme(fig_bar, is_dark, is_mobile, is_bar=True), theme="streamlit", use_container_width=True)

            # ── Treemap: at-a-glance allocation + P/L color overlay ──
            if not summary.empty and float(summary["Value_ILS"].sum()) > 0:
                tm_src = summary.copy()
                # Use origin-currency yield (not ILS-converted) for the color axis
                tm_src["Yield_Origin_Pct"] = tm_src["Yield_Origin"] * 100.0
                # Pre-compute label so NaN yield (parent node) shows "" not "NaN%"
                tm_src["_pct_str"] = tm_src["Yield_Origin_Pct"].apply(
                    lambda x: f"{x:.1f}%" if pd.notna(x) else ""
                )
                try:
                    fig_tree = px.treemap(
                        tm_src,
                        path=[px.Constant(tr("Portfolio", "תיק")), "Ticker"],
                        values="Value_ILS",
                        color="Yield_Origin_Pct",
                        color_continuous_scale=["#dc2626", "#f59e0b", "#16a34a"],
                        color_continuous_midpoint=0,
                        title=tr("Portfolio Treemap (size = value · color = origin yield)",
                                 "מפת חום של התיק (גודל = שווי · צבע = תשואה במטבע מקור)"),
                        template=template,
                        custom_data=["_pct_str"],
                        hover_data={"Value_ILS": ":,.0f", "Yield_Origin_Pct": ":.2f"},
                    )
                    fig_tree.update_traces(
                        texttemplate="<b>%{label}</b><br>₪%{value:,.0f}<br>%{customdata[0]}",
                        textposition="middle center",
                    )
                    # On mobile: move colorbar below the chart to free horizontal space
                    if is_mobile:
                        tree_colorbar = dict(
                            orientation="h", x=0.5, y=-0.08,
                            xanchor="center", yanchor="top",
                            title=dict(text="%", side="bottom"),
                            thickness=12, len=0.75,
                        )
                        tree_margin = dict(l=10, r=10, t=50, b=70)
                    else:
                        tree_colorbar = dict(title="%")
                        tree_margin = dict(l=10, r=10, t=50, b=10)
                    fig_tree.update_layout(
                        margin=tree_margin,
                        coloraxis_colorbar=tree_colorbar,
                    )
                    st.plotly_chart(_apply_plotly_theme(fig_tree, is_dark, is_mobile), theme="streamlit", use_container_width=True)
                except Exception:
                    pass

            def _build_summary_for_exposure(local_open_trades: pd.DataFrame) -> pd.DataFrame:
                if local_open_trades.empty:
                    return pd.DataFrame(columns=["Ticker", "Current_Price", "Open_Qty", "Cost_ILS", "Value_ILS", "Net_PnL_ILS", "Yield_Origin", "Yield_ILS"])
                fx_local = _safe_quote("USDILS=X")
                if fx_local <= 0:
                    fx_local = 3.6
                local_df = enrich_open_trades_with_prices(local_open_trades.copy())
                local_df["Cost_Origin_With_Fee"] = local_df["Cost_Origin"] + local_df["Commission"]
                local_df["Value_Origin_Est"] = np.where(
                    local_df["Origin_Currency"].str.upper() == "USD",
                    local_df["Current_Value_ILS"] / fx_local,
                    local_df["Current_Value_ILS"],
                )
                out = local_df.groupby("Ticker", as_index=False).agg(
                    Current_Price=("מחיר שוק", "max"),
                    Open_Qty=("Quantity", "sum"),
                    Cost_ILS=("Cost_ILS", "sum"),
                    Value_ILS=("Current_Value_ILS", "sum"),
                    Cost_Origin=("Cost_Origin_With_Fee", "sum"),
                    Value_Origin=("Value_Origin_Est", "sum"),
                )
                out["Net_PnL_ILS"] = out["Value_ILS"] - out["Cost_ILS"]
                out["Yield_Origin"] = np.where(out["Cost_Origin"] > 0, (out["Value_Origin"] - out["Cost_Origin"]) / out["Cost_Origin"], 0.0)
                out["Yield_ILS"] = np.where(out["Cost_ILS"] > 0, out["Net_PnL_ILS"] / out["Cost_ILS"], 0.0)
                return out

            def render_exposure_section(summary_df: pd.DataFrame, widget_prefix: str = "overview") -> None:

                st.markdown(f"#### {tr('Exposure Table', 'טבלת חשיפה')}")
                exposure_cols = ["Ticker", "Current_Price", "Open_Qty", "Cost_ILS", "Value_ILS", "Net_PnL_ILS", "Yield_Origin", "Yield_ILS"]
                if summary_df.empty:
                    exposure_base = pd.DataFrame(columns=exposure_cols)
                else:
                    exposure_base = summary_df.reindex(columns=exposure_cols).copy()
                    # Keep current-price column resilient when upstream shape changes.
                    if "Current_Price" not in summary_df.columns and {"Value_ILS", "Open_Qty"}.issubset(summary_df.columns):
                        qty = pd.to_numeric(summary_df.get("Open_Qty"), errors="coerce")
                        val = pd.to_numeric(summary_df.get("Value_ILS"), errors="coerce")
                        with np.errstate(divide="ignore", invalid="ignore"):
                            derived_price = np.where(qty > 0, val / qty, np.nan)
                        exposure_base["Current_Price"] = derived_price
                exposure_work = exposure_base.copy()
                exposure_work["Ticker"] = exposure_work["Ticker"].map(_clean)
                for numeric_col in ["Current_Price", "Open_Qty", "Cost_ILS", "Value_ILS", "Net_PnL_ILS", "Yield_Origin", "Yield_ILS"]:
                    if numeric_col in exposure_work.columns:
                        exposure_work[numeric_col] = exposure_work[numeric_col].map(_num)
                exposure_view = localize_dataframe_columns(exposure_work, language)
                current_price_col = localize_column_name("Current_Price", language)
                pnl_col = localize_column_name("Net_PnL_ILS", language)
                yield_origin_col = localize_column_name("Yield_Origin", language)
                yield_ils_col = localize_column_name("Yield_ILS", language)

                if exposure_view.empty:
                    st.info(tr("No open positions to show in Exposure Table.", "אין פוזיציות פתוחות להצגה בטבלת החשיפה."))
                else:
                    exposure_styled = exposure_view.style.format(
                        {
                            current_price_col: "{:,.4f}",
                            localize_column_name("Open_Qty", language): "{:.8f}",
                            localize_column_name("Cost_ILS", language): "{:,.0f}",
                            localize_column_name("Value_ILS", language): "{:,.0f}",
                            pnl_col: "{:,.0f}",
                            yield_origin_col: "{:.2%}",
                            yield_ils_col: "{:.2%}",
                        },
                        na_rep="",
                    )
                    exposure_styled = _apply_signed_color(exposure_styled, [pnl_col, yield_origin_col, yield_ils_col])
                    # Mobile browsers sometimes drop Styler colors in virtualized grid;
                    # st.table preserves static styled colors/percent formatting reliably.
                    if is_mobile:
                        st.table(exposure_styled)
                    else:
                        _render_dataframe_adaptive(
                            exposure_styled,
                            is_mobile,
                            force_same_render_path=True,
                            use_container_width=True,
                            hide_index=True,
                        )

                watchlist_label = tr("TradingView Watchlist", "רשימת מעקב TradingView")
                category_labels = {
                    "crypto": tr("Crypto (Crypto)", "Crypto (קריפטו)"),
                    "stocks": tr("Actions / Stocks (Stocks & ETFs)", "Actions / Stocks (מניות וקרנות סל)"),
                    "macro": tr("Futures / Commodities / Forex", "Futures / Commodities / Forex (חוזים, סחורות ומט\"ח)"),
                }
                watch_rows = []
                for item in DEFAULT_TRADINGVIEW_WATCHLIST:
                    watch_rows.append(
                        {
                            "Ticker": item["ticker"],
                            "Title": f"{item['ticker']} - {item['label']}",
                            "Symbol": item["tv_symbol"],
                            "Category": category_labels[item["category"]],
                        }
                    )

                if watch_rows:
                    watch_df = pd.DataFrame(watch_rows).drop_duplicates(subset=["Symbol"])
                    watchlist_reset_key = f"{widget_prefix}_watchlist_reset_token"
                    if watchlist_reset_key not in st.session_state:
                        st.session_state[watchlist_reset_key] = False
                    reset_char = "\u200b" if bool(st.session_state.get(watchlist_reset_key, False)) else "\u200c"
                    open_container = st.expander(f"{watchlist_label}{reset_char}", expanded=False)
                    with open_container:
                        for cat in [category_labels["crypto"], category_labels["stocks"], category_labels["macro"]]:
                            part = watch_df[watch_df["Category"] == cat]
                            if part.empty:
                                continue
                            st.markdown(f"**{cat}**")
                            for idx, row in enumerate(part.itertuples(index=False), 1):
                                if st.button(row.Title, key=f"{widget_prefix}_watch_{cat}_{idx}_{row.Symbol}"):
                                    st.session_state["tv_chart_ticker"] = _clean(row.Symbol).upper()
                                    st.session_state["tv_chart_open"] = True
                                    st.session_state[f"{widget_prefix}_chart_scroll_pending"] = True
                                    st.session_state[watchlist_reset_key] = not bool(st.session_state.get(watchlist_reset_key, False))
                                    st.rerun()
                else:
                    st.caption(tr("Watchlist is empty.", "רשימת המעקב ריקה."))

                if st.session_state.get("tv_chart_open") and _clean(st.session_state.get("tv_chart_ticker", "")):
                    active_ticker = _clean(st.session_state.get("tv_chart_ticker", "")).upper()
                    chart_anchor_id = f"tv-chart-anchor-{widget_prefix}"
                    st.markdown(f"<div id='{chart_anchor_id}'></div>", unsafe_allow_html=True)
                    st.markdown(f"#### {tr('TradingView Chart', 'גרף TradingView')} - `{active_ticker}`")
                    # Compact toolbar: [title · close]. Per-chart theme toggle
                    # was removed — TradingView's embed widget caches theme
                    # state in localStorage and ignores re-init theme params,
                    # which made the toggle unreliable.
                    _ttitle_col, close_col = st.columns([8, 1])
                    if close_col.button(
                        tr("✕ Close", "✕ סגור"),
                        key=f"{widget_prefix}_tv_inline_close",
                        type="primary",
                        use_container_width=True,
                    ):
                        st.session_state["tv_chart_open"] = False
                        st.session_state.pop("tv_chart_ticker", None)
                        st.session_state[f"{widget_prefix}_chart_scroll_pending"] = False
                        st.rerun()
                    _render_tradingview_widget(
                        active_ticker,
                        height=580 if is_mobile else 750,
                        theme="dark" if is_dark else "light",
                    )
                    if st.session_state.get(f"{widget_prefix}_chart_scroll_pending", False):
                        components.html(
                            f"""
                            <script>
                            (function() {{
                              const doc = (window.parent && window.parent.document) ? window.parent.document : document;
                              const el = doc.getElementById('{chart_anchor_id}');
                              if (el && el.scrollIntoView) {{
                                setTimeout(function() {{ el.scrollIntoView({{ behavior: 'smooth', block: 'start' }}); }}, 80);
                              }}
                            }})();
                            </script>
                            """,
                            height=0,
                            width=0,
                        )
                        st.session_state[f"{widget_prefix}_chart_scroll_pending"] = False

            if live_updates and hasattr(st, "fragment"):
                @st.fragment(run_every=f"{int(refresh_seconds)}s")
                def _exposure_fragment() -> None:
                    live_summary = summary
                    live_open_for_watchlist = open_trades
                    if not is_demo:
                        try:
                            live_df, _ = load_snapshot_data(web_url_clean, api_token, spreadsheet_ref, worksheet_name, service_account_file)
                            live_core = prepare_core_views(live_df)
                            live_open_for_watchlist = live_core["open_trades"].copy()
                            live_summary = _build_summary_for_exposure(live_open_for_watchlist)
                        except Exception:
                            pass
                    with _alloc_exposure_slot:
                        render_exposure_section(live_summary, widget_prefix="overview")

                _exposure_fragment()
            else:
                with _alloc_exposure_slot:
                    render_exposure_section(summary, widget_prefix="overview")

        # ── Mirror headline widgets into Overview tab (duplicates — do NOT remove from Allocation) ──
        with _ov_body_slot:
            if ("fig_pie" in locals()) and ("fig_bar" in locals()):
                ov_col_a, ov_col_b = st.columns(2)
                with ov_col_a:
                    st.plotly_chart(
                        _apply_plotly_theme(fig_pie, is_dark, is_mobile),
                        use_container_width=True,
                        theme="streamlit",
                        key="ov_mirror_pie",
                    )
                with ov_col_b:
                    st.plotly_chart(
                        _apply_plotly_theme(fig_bar, is_dark, is_mobile, is_bar=True),
                        use_container_width=True,
                        theme="streamlit",
                        key="ov_mirror_bar",
                    )
            # Exposure table mirrored into Overview too (uses distinct widget prefix
            # so all internal Streamlit keys stay unique).
            try:
                render_exposure_section(summary, widget_prefix="overview_body")
            except Exception:
                pass

        # Compute once and share across both Allocation and Reports tabs.
        _shared_reports_payload = build_home_inspired_reports(open_trades)

        with tab_allocation:
            allocation_payload = _shared_reports_payload
            if is_mobile:
                crypto_share_label = tr("Crypto Share", "קריפטו בתיק")
                btc_portfolio_label = tr("BTC in Portfolio", "ביטקוין בתיק")
                btc_crypto_label = tr("BTC in Crypto", "ביטקוין בקריפטו")
            else:
                crypto_share_label = tr("Crypto Share", "משקל קריפטו")
                btc_portfolio_label = tr("BTC Share of Portfolio", "משקל ביטקוין בתיק")
                btc_crypto_label = tr("BTC Share of Crypto", "משקל ביטקוין מתוך הקריפטו")
            alloc_kpi_cols = st.columns(3)
            alloc_kpi_cols[0].metric(
                crypto_share_label,
                f"{float(allocation_payload.get('crypto_share', 0.0)):.2%}",
            )
            alloc_kpi_cols[1].metric(
                btc_portfolio_label,
                f"{float(allocation_payload.get('btc_share_of_portfolio', 0.0)):.2%}",
            )
            alloc_kpi_cols[2].metric(
                btc_crypto_label,
                f"{float(allocation_payload.get('btc_share_of_crypto', 0.0)):.2%}",
            )
            style_metric_cards(border_left_color="#4f46e5", border_radius_px=12, box_shadow=True)
            _space(8)

            alloc_col1, alloc_col2 = st.columns(2)
            with alloc_col1:
                # ── Allocation pie by Asset CLASS (Crypto / ETF / Stocks) ──
                # (The old by-ticker pie here was a duplicate of the one in the
                #  charts slot above, so it was replaced with this class-level view.)
                if not class_mix.empty:
                    st.caption(
                        f"{tr('Crypto share of portfolio', 'משקל קריפטו בתיק')}: "
                        f"{float(allocation_payload.get('crypto_share', 0.0)):.2%}"
                    )
                    class_pie_fig = px.pie(
                        class_mix,
                        names="Asset_Class",
                        values="Current_Value_ILS",
                        title=tr("Allocation by Asset Class", "חלוקה לפי סוג נכס"),
                        hole=0.45,
                        template=template,
                        color_discrete_sequence=_BRAND_PALETTE,
                    )
                    class_pie_fig.update_traces(
                        hovertemplate="<b>%{label}</b><br>₪%{value:,.0f}<br>%{percent}<extra></extra>",
                        textinfo="percent+label",
                    )
                    st.plotly_chart(
                        _apply_plotly_theme(class_pie_fig, is_dark, is_mobile),
                        use_container_width=True,
                        theme="streamlit",
                        key="alloc_class_pie",
                    )
                else:
                    st.info(tr("No asset-class data", "אין נתוני סוגי נכסים"))
            with alloc_col2:
                if not class_mix.empty:
                    class_order = class_mix["Asset_Class"].tolist()
                    class_color_map = {
                        cls: _BRAND_PALETTE[idx % len(_BRAND_PALETTE)]
                        for idx, cls in enumerate(class_order)
                    }
                    type_fig = px.treemap(
                        class_mix,
                        path=["Asset_Class"],
                        values="Current_Value_ILS",
                        title=tr("Allocation by Asset Class", "חלוקה לפי סוג נכס"),
                        template=template,
                        color="Asset_Class",
                        color_discrete_map=class_color_map,
                        color_discrete_sequence=_BRAND_PALETTE,
                    )
                    type_fig.update_traces(
                        hovertemplate="<b>%{label}</b><br>₪%{value:,.0f}<extra></extra>",
                    )
                    st.plotly_chart(
                        _apply_plotly_theme(type_fig, is_dark, is_mobile),
                        use_container_width=True,
                        theme="streamlit",
                        key="alloc_class_treemap",
                    )

                    st.markdown(f"**{tr('Included assets by class', 'נכסים כלולים לפי סוג')}**")
                    for row in class_mix.itertuples(index=False):
                        cls = _clean(getattr(row, "Asset_Class", ""))
                        assets = _clean(getattr(row, "Assets", "")) or "-"
                        color = class_color_map.get(cls, "#64748b")
                        st.markdown(
                            f"<span style='color:{color};font-weight:700'>■</span> <strong>{cls}</strong>: {assets}",
                            unsafe_allow_html=True,
                        )
                else:
                    st.info(tr("No asset-class data", "אין נתוני סוגי נכסים"))

        with tab_reports:
            reports_payload = _shared_reports_payload
            report_options = {
                tr("Crypto Concentration", "ריכוזיות קריפטו"): "concentration_table",
                tr("Winner / Loser", "המנצח / המפסיד"): "winner_loser_table",
                tr("Net Investment by Platform", "השקעה נטו לפי פלטפורמה"): "net_investment_table",
                tr("Live Market Rates", "שערים חיים מהשוק"): "live_rates",
            }

            def _render_report_section(title: str, key: str) -> None:
                st.markdown(f"### {title}")
                if key == "live_rates":
                    rates = reports_payload.get("live_rates", {}) if isinstance(reports_payload, dict) else {}
                    if not rates:
                        st.info(tr("No market rates available.", "אין שערי שוק זמינים כרגע."))
                    else:
                        rates_df = pd.DataFrame(
                            [{tr("Symbol", "סימול"): k, tr("Rate", "שער"): v} for k, v in rates.items()]
                        )
                        rates_styled = rates_df.style.format({tr("Rate", "שער"): "{:,.4f}"})
                        _render_dataframe_adaptive(rates_styled, is_mobile, use_container_width=True, hide_index=True)
                else:
                    report_df = reports_payload.get(key, pd.DataFrame()) if isinstance(reports_payload, dict) else pd.DataFrame()
                    if not isinstance(report_df, pd.DataFrame) or report_df.empty:
                        st.info(tr("No data available for this report.", "אין נתונים זמינים לדוח זה."))
                    else:
                        localized_df = localize_dataframe_columns(report_df, language)
                        fmt_map: Dict[str, str] = {}
                        for col in localized_df.columns:
                            if "Yield" in str(col) or "תשואה" in str(col):
                                fmt_map[col] = "{:.2%}"
                            elif "Qty" in str(col) or "כמות" in str(col):
                                fmt_map[col] = "{:.8f}"
                            elif any(token in str(col) for token in ["ILS", "Rate", "שער", "שווי", "עלות", "Investment", "PnL", "רווח"]):
                                fmt_map[col] = "{:,.0f}"
                        report_styled = localized_df.style
                        if fmt_map:
                            report_styled = report_styled.format(fmt_map)
                        signed_cols = [c for c in localized_df.columns if any(t in str(c).lower() for t in ["yield", "pnl", "תשואה", "רווח"])]
                        if signed_cols:
                            report_styled = _apply_signed_color(report_styled, signed_cols)
                        _render_dataframe_adaptive(report_styled, is_mobile, use_container_width=True, hide_index=True)
                st.divider()

            for _rep_title, _rep_key in report_options.items():
                _render_report_section(_rep_title, _rep_key)

        with _ov_equity_slot:
            if can_show_build_up:
                st.markdown(f"#### {tr('Portfolio Build-Up', 'התפתחות בניית התיק')}")
                perf_track = open_trades.groupby("Purchase_Date", as_index=False)[["Cost_ILS", "Current_Value_ILS"]].sum().sort_values("Purchase_Date")
                perf_track["Cum_Cost_ILS"] = perf_track["Cost_ILS"].cumsum()
                perf_track["Cum_Value_ILS"] = perf_track["Current_Value_ILS"].cumsum()
                fig_track = go.Figure()
                fig_track.add_trace(go.Scatter(
                    x=perf_track["Purchase_Date"], y=perf_track["Cum_Cost_ILS"],
                    mode="lines+markers", name=tr("Cumulative Cost", "עלות מצטברת"),
                    line=dict(color="#94a3b8", width=2),
                    hovertemplate=tr("Date", "תאריך") + ": %{x|%Y-%m-%d}<br>" + tr("Cost", "עלות") + ": ₪%{y:,.0f}<extra></extra>",
                ))
                fig_track.add_trace(go.Scatter(
                    x=perf_track["Purchase_Date"], y=perf_track["Cum_Value_ILS"],
                    mode="lines+markers", name=tr("Cumulative Value", "שווי מצטבר"),
                    line=dict(color="#4f46e5", width=2),
                    fill="tonexty", fillcolor="rgba(79,70,229,0.08)",
                    hovertemplate=tr("Date", "תאריך") + ": %{x|%Y-%m-%d}<br>" + tr("Value", "שווי") + ": ₪%{y:,.0f}<extra></extra>",
                ))
                fig_track.update_layout(
                    template=template,
                    xaxis_title=tr("Date", "תאריך"),
                    yaxis_title=tr("Value (ILS)", "שווי (₪)"),
                    yaxis_tickformat=",.0f",
                    hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                )
                st.plotly_chart(_apply_plotly_theme(fig_track, is_dark, is_mobile), theme="streamlit", use_container_width=True)

        with tab_deposits:
            # (Equity curve moved to Overview tab — top section.)
            deposit_mode = "demo" if is_demo else "live"
            default_platforms = sorted({_clean(v) for v in trades.get("Platform", pd.Series(dtype=str)).tolist() if _clean(v)}) if "Platform" in trades.columns else []
            rows_state_key = f"manual_deposits_rows_{deposit_mode}"
            loaded_state_key = f"manual_deposits_loaded_{deposit_mode}"
            source_state_key = f"manual_deposits_source_{deposit_mode}"
            web_url_for_sync = _clean(web_url_clean)
            can_sync_remote = is_apps_script_web_app_url(web_url_for_sync) and bool(_clean(api_token))

            if not st.session_state.get(loaded_state_key, False):
                rows: List[Dict[str, object]] = []
                source_label = "local"
                if can_sync_remote:
                    ok_remote, remote_rows, _ = load_manual_deposits_remote(web_url_for_sync, api_token, deposit_mode)
                    if ok_remote:
                        rows = remote_rows
                        source_label = "cloud"
                if not rows:
                    local_store = load_manual_deposits_store()
                    rows = local_store.get(deposit_mode, []) if isinstance(local_store, dict) else []
                if (deposit_mode == "demo") and (not rows):
                    rows = [
                        {"Platform": "Global Prime", "Manual_Deposit_ILS": 120000.0},
                        {"Platform": "Quant Desk", "Manual_Deposit_ILS": 85000.0},
                        {"Platform": "Digital Alpha", "Manual_Deposit_ILS": 95000.0},
                        {"Platform": "IsraTrade", "Manual_Deposit_ILS": 35000.0},
                    ]
                    source_label = "demo_seed"
                st.session_state[rows_state_key] = _normalize_manual_deposit_rows(rows, default_platforms)
                st.session_state[source_state_key] = source_label
                st.session_state[loaded_state_key] = True

            current_rows = _normalize_manual_deposit_rows(st.session_state.get(rows_state_key, []), default_platforms)
            st.session_state[rows_state_key] = current_rows

            st.caption(
                tr(
                    f"Manual deposits are isolated and synced separately (source: {st.session_state.get(source_state_key, 'local')}).",
                    f"הפקדות ידניות מבודדות ומסונכרנות בנפרד (מקור: {st.session_state.get(source_state_key, 'local')}).",
                )
            )

            edit_df = pd.DataFrame(current_rows)
            if edit_df.empty:
                edit_df = pd.DataFrame([{"Platform": "", "Manual_Deposit_ILS": 0.0}])

            edited_df = st.data_editor(
                edit_df,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                key=f"manual_deposits_editor_{deposit_mode}",
                column_config={
                    "Platform": st.column_config.TextColumn(tr("Platform", "פלטפורמה"), required=True),
                    "Manual_Deposit_ILS": st.column_config.NumberColumn(tr("Manual Deposit (ILS)", "הפקדה ידנית (₪)"), min_value=0.0, step=100.0, format="%.2f"),
                },
            )

            edited_rows = edited_df.to_dict(orient="records") if isinstance(edited_df, pd.DataFrame) else []
            normalized_rows = _normalize_manual_deposit_rows(edited_rows, default_platforms=[])
            st.session_state[rows_state_key] = normalized_rows

            total_manual = float(sum(_num(r.get("Manual_Deposit_ILS", 0.0)) for r in normalized_rows))
            st.metric(tr("Total Manual Deposits", "סה\"כ הפקדות ידניות"), f"{total_manual:,.2f} ₪")

            action_cols = st.columns([1, 1, 3])
            if action_cols[0].button(tr("Save Deposits", "שמור הפקדות"), key=f"manual_deposits_save_{deposit_mode}"):
                store_payload = load_manual_deposits_store()
                store_payload[deposit_mode] = normalized_rows
                save_ok = save_manual_deposits_store(store_payload)
                if can_sync_remote:
                    remote_ok, remote_msg = save_manual_deposits_remote(web_url_for_sync, api_token, deposit_mode, normalized_rows)
                    if remote_ok:
                        st.session_state[source_state_key] = "cloud"
                        st.success(tr("Saved and synced to cloud", "נשמר וסונכרן לענן"))
                    else:
                        msg = remote_msg or tr("Unknown sync error", "שגיאת סנכרון לא ידועה")
                        st.warning(f"{tr('Saved locally only', 'נשמר מקומית בלבד')}: {msg}")
                elif save_ok:
                    st.session_state[source_state_key] = "local"
                    st.success(tr("Saved locally on this device", "נשמר מקומית במכשיר זה"))
                else:
                    st.error(tr("Failed to save deposits", "שמירת ההפקדות נכשלה"))

            if action_cols[1].button(tr("Reload Cloud", "טען מהענן"), key=f"manual_deposits_reload_{deposit_mode}"):
                if not can_sync_remote:
                    st.info(tr("Cloud sync is unavailable (missing valid Web App URL or token).", "סנכרון ענן לא זמין (חסר Web App URL תקין או טוקן)."))
                else:
                    ok_remote, remote_rows, remote_err = load_manual_deposits_remote(web_url_for_sync, api_token, deposit_mode)
                    if ok_remote:
                        st.session_state[rows_state_key] = _normalize_manual_deposit_rows(remote_rows, default_platforms)
                        st.session_state[source_state_key] = "cloud"
                        st.success(tr("Loaded latest deposits from cloud", "נטענו ההפקדות האחרונות מהענן"))
                        st.rerun()
                    else:
                        st.error(f"{tr('Cloud load failed', 'טעינה מהענן נכשלה')}: {remote_err}")

        with tab_transactions:
            tx_view = trades.copy() if isinstance(trades, pd.DataFrame) else pd.DataFrame()
            if not tx_view.empty:
                tx_view = tx_view.drop(columns=["Event_Type", "Trade_ID", "Record_Source"], errors="ignore")
                fx_cols = []
                for col in tx_view.columns:
                    col_text = _clean(col)
                    col_lower = col_text.lower()
                    if ("שקל" in col_text and "דולר" in col_text) or ("usd" in col_lower and "ils" in col_lower):
                        fx_cols.append(col)
                if fx_cols:
                    tx_view = tx_view.drop(columns=fx_cols, errors="ignore")
                prioritized_cols = [c for c in ["Trade_ID", "Purchase_Date"] if c in tx_view.columns]
                remaining_cols = [c for c in tx_view.columns if c not in prioritized_cols]
                tx_view = tx_view[prioritized_cols + remaining_cols]

                # Keep one canonical current-price column (computed later as Market_Price_Origin).
                def _is_legacy_current_price_col(col_name: object) -> bool:
                    col_text = _clean(col_name)
                    if not col_text or col_text == "Market_Price_Origin":
                        return False
                    low = col_text.lower().replace("_", " ").replace("-", " ")
                    low = re.sub(r"\s+", " ", low).strip()
                    if any(token in low for token in ["yield", "return", "תשואה"]):
                        return False
                    if low in {"current price", "market price", "שער נוכחי"}:
                        return True
                    return ("current" in low and "price" in low) or ("שער" in col_text and "נוכחי" in col_text)

                legacy_current_price_cols = [c for c in tx_view.columns if _is_legacy_current_price_col(c)]
                if legacy_current_price_cols:
                    tx_view = tx_view.drop(columns=legacy_current_price_cols, errors="ignore")

                tx_view = tx_view.replace({"#VALUE!": "נמכר", "VALUE": "נמכר", "#VALUE": "נמכר"})

                is_open_only = False
                if "Status" in tx_view.columns:
                    both_label = tr("Both", "הכל")
                    open_label = tr("Open only", "פתוחות בלבד")
                    closed_label = tr("Closed only", "סגורות בלבד")
                    scope_options = [both_label, open_label, closed_label]
                    scope_key = "dashboard_transactions_status_scope"
                    if st.session_state.get(scope_key) not in scope_options:
                        st.session_state[scope_key] = open_label
                    status_scope = st.radio(
                        tr("Position scope", "הצגת פוזיציות"),
                        scope_options,
                        horizontal=True,
                        key=scope_key,
                    )
                    is_open_only = status_scope == open_label
                    if is_open_only:
                        tx_view = tx_view[~tx_view["Status"].map(_is_closed_status)]
                    elif status_scope == closed_label:
                        tx_view = tx_view[tx_view["Status"].map(_is_closed_status)]

                if is_open_only:
                    sale_related_cols = [
                        c for c in [
                            "Status", "Action", "Sell_Date", "Sell_Price_Origin", "Yield_At_Sale",
                            "סטטוס", "פעולה", "תאריך מכירה", "שער מכירה", "מחיר מכירה", "תשואה במכירה",
                        ]
                        if c in tx_view.columns
                    ]
                    if sale_related_cols:
                        tx_view = tx_view.drop(columns=sale_related_cols, errors="ignore")

                if "Ticker" in tx_view.columns:
                    ticker_options = sorted({_clean(v).upper() for v in tx_view["Ticker"].tolist() if _clean(v)})
                    chosen_tickers = st.multiselect(
                        tr("Ticker filter", "סינון לפי טיקר"),
                        ticker_options,
                        default=[],
                        key="dashboard_transactions_ticker_filter",
                    )
                    if chosen_tickers:
                        selected_set = {t.upper() for t in chosen_tickers}
                        tx_view = tx_view[tx_view["Ticker"].map(lambda v: _clean(v).upper()).isin(selected_set)]
                if "Purchase_Date" in tx_view.columns:
                    # Default ordering: newest purchase date first.
                    tx_view["__sort_date__"] = _parse_dates_flexible(tx_view["Purchase_Date"])
                    tx_view = tx_view.sort_values("__sort_date__", ascending=False, na_position="last").drop(columns=["__sort_date__"])

                # Show current market price/returns for both open and closed rows.
                if "Ticker" in tx_view.columns and "Origin_Buy_Price" in tx_view.columns:
                    fx_now = _safe_quote("USDILS=X")
                    if fx_now <= 0:
                        fx_now = 3.6
                    tickers_for_price = tuple(sorted({_clean(v).upper() for v in tx_view["Ticker"].tolist() if _clean(v)}))
                    live_price_map = fetch_prices(tickers_for_price) if tickers_for_price else {}

                    def _market_price_origin(row: pd.Series) -> float:
                        t = _clean(row.get("Ticker", "")).upper()
                        if not t:
                            return np.nan
                        p_usd = float(_num(live_price_map.get(t, 0.0)))
                        if p_usd <= 0:
                            return np.nan
                        cur = _normalize_currency_code(row.get("Origin_Currency", ""))
                        return p_usd if cur == "USD" else (p_usd * fx_now if cur == "ILS" else p_usd)

                    def _sell_price_origin(row: pd.Series) -> float:
                        if not _is_closed_status(row.get("Status", "")):
                            return np.nan
                        qty = float(_num(row.get("Quantity", 0.0)))
                        val_ils = float(_num(row.get("Current_Value_ILS", 0.0)))
                        if qty > 1e-9 and val_ils > 0:
                            cur = _normalize_currency_code(row.get("Origin_Currency", ""))
                            unit_ils = val_ils / qty
                            return unit_ils / fx_now if cur == "USD" else unit_ils
                        return np.nan

                    tx_view["Market_Price_Origin"] = tx_view.apply(_market_price_origin, axis=1)

                    sell_from_market = tx_view.apply(_sell_price_origin, axis=1)
                    if "Sell_Price_Origin" in tx_view.columns:
                        existing_sell = tx_view["Sell_Price_Origin"].map(_num)
                        tx_view["Sell_Price_Origin"] = np.where(existing_sell > 0, existing_sell, sell_from_market)
                    else:
                        tx_view["Sell_Price_Origin"] = sell_from_market

                    buy_price = tx_view["Origin_Buy_Price"].map(_num)
                    tx_view["Yield_Current"] = np.where(buy_price > 0, (tx_view["Market_Price_Origin"] - buy_price) / buy_price, np.nan)

                    sale_yield_calc = np.where(buy_price > 0, (tx_view["Sell_Price_Origin"] - buy_price) / buy_price, np.nan)
                    if "Yield_At_Sale" in tx_view.columns:
                        existing_yield_sale = tx_view["Yield_At_Sale"]
                        has_existing_yield = ~existing_yield_sale.map(lambda v: pd.isna(v) or _clean(v) == "")
                        tx_view["Yield_At_Sale"] = np.where(has_existing_yield, existing_yield_sale, sale_yield_calc)
                    else:
                        tx_view["Yield_At_Sale"] = sale_yield_calc

                    # Backfill current-yield columns if sheet formulas are missing/blank.
                    cost_ils_series = tx_view["Cost_ILS"].map(_num) if "Cost_ILS" in tx_view.columns else pd.Series(0.0, index=tx_view.index)
                    value_ils_series = tx_view["Current_Value_ILS"].map(_num) if "Current_Value_ILS" in tx_view.columns else pd.Series(0.0, index=tx_view.index)
                    yield_ils_calc = np.where(cost_ils_series > 0, (value_ils_series - cost_ils_series) / cost_ils_series, np.nan)

                    cost_origin_series = tx_view["Cost_Origin"].map(_num) if "Cost_Origin" in tx_view.columns else pd.Series(0.0, index=tx_view.index)
                    value_origin_series = np.where(
                        tx_view["Origin_Currency"].map(_normalize_currency_code) == "USD",
                        np.where(fx_now > 0, value_ils_series / fx_now, np.nan),
                        value_ils_series,
                    )
                    yield_origin_calc = np.where(cost_origin_series > 0, (value_origin_series - cost_origin_series) / cost_origin_series, np.nan)

                    if "Yield_ILS" in tx_view.columns:
                        existing_yield_ils = tx_view["Yield_ILS"]
                        has_existing_ils = ~existing_yield_ils.map(lambda v: pd.isna(v) or _clean(v) == "")
                        tx_view["Yield_ILS"] = np.where(has_existing_ils, existing_yield_ils, yield_ils_calc)
                    else:
                        tx_view["Yield_ILS"] = yield_ils_calc

                    if "Yield_Origin" in tx_view.columns:
                        existing_yield_origin = tx_view["Yield_Origin"]
                        has_existing_origin = ~existing_yield_origin.map(lambda v: pd.isna(v) or _clean(v) == "")
                        tx_view["Yield_Origin"] = np.where(has_existing_origin, existing_yield_origin, yield_origin_calc)
                    else:
                        tx_view["Yield_Origin"] = yield_origin_calc


                    # Hide dedicated helper column (user requested).
                    tx_view = tx_view.drop(columns=["Yield_Current"], errors="ignore")
                yield_ils_cols = [
                    c for c in tx_view.columns
                    if ("תשואה" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower())
                    and ("שקל" in str(c) or "ils" in str(c).lower())
                ]
                yield_origin_cols = [
                    c for c in tx_view.columns
                    if ("תשואה" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower())
                    and ("מקור" in str(c) or "origin" in str(c).lower())
                ]
                yield_cols = []
                for col in yield_ils_cols + yield_origin_cols:
                    if col not in yield_cols:
                        yield_cols.append(col)
                if not yield_cols:
                    yield_cols = [c for c in tx_view.columns if ("תשואה" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower())]
                if yield_cols:
                    non_yield_cols = [c for c in tx_view.columns if c not in yield_cols]
                    tx_view = tx_view[non_yield_cols + yield_cols]

            tx_view = localize_snapshot_view(tx_view, language)

            def _normalize_header_text(col: str) -> str:
                c = _clean(col)
                fixed = {
                    "עלות שקלית": "עלותILS",
                }
                if c in fixed:
                    return fixed[c]
                return c

            tx_view = tx_view.rename(columns=lambda c: _normalize_header_text(str(c)))

            def _is_hidden_tx_column(col_name: object) -> bool:
                c = _clean(col_name)
                c_base = re.sub(r"\s*\(\d+\)$", "", c)
                low = c_base.lower().replace("_", " ").replace("-", " ")
                low = re.sub(r"\s+", " ", low).strip()
                buy_price_dup_aliases = {
                    "origin buy price", "buy price", "שער קנייה", "שער קניה",
                }
                sell_status_aliases = {
                    "sell status", "sale status", "סטטוס מכירה",
                }
                return (low in buy_price_dup_aliases) or (low in sell_status_aliases)

            tx_view = tx_view.drop(columns=[c for c in tx_view.columns if _is_hidden_tx_column(c)], errors="ignore")

            if is_open_only and not tx_view.empty:
                # Final guard: remove sale-only fields that may be reintroduced by computed/localized columns.
                def _open_only_hide_col(col_name: object) -> bool:
                    c = _clean(col_name)
                    low = c.lower()
                    if c in {"Status", "Action", "Sell_Date", "Sell_Price_Origin", "Yield_At_Sale", "סטטוס", "פעולה", "תאריך מכירה", "שער מכירה", "מחיר מכירה", "תשואה במכירה"}:
                        return True
                    return low in {"sell price", "sale price", "sell date", "return at sale"}

                tx_view = tx_view.drop(columns=[c for c in tx_view.columns if _open_only_hide_col(c)], errors="ignore")

            tx_view, _ = _with_calendar_purchase_date(tx_view, language)
            if tx_view.empty:
                st.info(tr("No transactions to display", "אין עסקאות להצגה"))
            else:
                display_view = tx_view.copy()
                display_view = display_view.reset_index(drop=True)
                if not display_view.columns.is_unique:
                    seen_cols: Dict[str, int] = {}
                    unique_cols: List[str] = []
                    for col in display_view.columns:
                        base = str(col)
                        seen_cols[base] = seen_cols.get(base, 0) + 1
                        unique_cols.append(base if seen_cols[base] == 1 else f"{base} ({seen_cols[base]})")
                    display_view.columns = unique_cols

                def _is_blank_like(v: object) -> bool:
                    if pd.isna(v):
                        return True
                    s = _clean(v).lower()
                    return s in {"", "nan", "nat", "none"}

                def _base_col_name(col_name: object) -> str:
                    c = _clean(col_name)
                    c = re.sub(r"\s*\(\d+\)$", "", c)
                    return re.sub(r"\s+", " ", c).strip()

                def _normalized_col_key(col_name: object) -> str:
                    return _base_col_name(col_name).lower().replace("_", " ").replace("-", " ")

                def _coalesce_alias_columns(df_in: pd.DataFrame, aliases: List[str]) -> pd.DataFrame:
                    alias_keys = {_normalized_col_key(a) for a in aliases}
                    candidates = [c for c in df_in.columns if _normalized_col_key(c) in alias_keys]
                    if len(candidates) <= 1:
                        return df_in

                    target_col: Optional[str] = None
                    for alias in aliases:
                        alias_key = _normalized_col_key(alias)
                        for c in candidates:
                            if _normalized_col_key(c) == alias_key:
                                target_col = c
                                break
                        if target_col is not None:
                            break
                    if target_col is None:
                        target_col = candidates[0]

                    merged = df_in[candidates].copy()
                    for c in candidates:
                        merged[c] = merged[c].map(lambda v: np.nan if _is_blank_like(v) else v)
                    df_in[target_col] = merged.bfill(axis=1).iloc[:, 0]

                    extras = [c for c in candidates if c != target_col]
                    if extras:
                        df_in = df_in.drop(columns=extras, errors="ignore")
                    return df_in

                alias_groups = [
                    ["Sell_Price_Origin", "Sell Price", "Sale Price", "מחיר מכירה", "שער מכירה"],
                    ["Yield_ILS", "Return ILS", "תשואה ILS", "תשואה שקלית", "תשואה נטו (₪)", "Net Return (ILS)"],
                    ["Yield_Origin", "Return (Origin)", "תשואה בשער מקור", "תשואה מקור", "תשואה נטו (מקור)", "Net Return (Origin)"],
                    ["Current_Value_ILS", "Value ILS", "Current Value (ILS)", "שווי ILS", "שווי שקלי", "שווי עדכני (₪)"],
                    ["Value USD", "Current Value (USD)", "שווי USD", "שווי בדולר", "שווי עדכני (USD)"],
                ]
                for aliases in alias_groups:
                    display_view = _coalesce_alias_columns(display_view, aliases)

                drop_cols: List[str] = []
                keep_even_if_blank: set[str] = set()
                for col in display_view.columns:
                    col_text = _clean(col)
                    col_lower = col_text.lower()
                    if col_lower.startswith("unnamed:"):
                        drop_cols.append(col)
                        continue
                    if (("usd" in col_lower and "ils" in col_lower) or ("שער" in col_text and "דולר" in col_text and "שקל" in col_text)):
                        drop_cols.append(col)
                        continue
                    if col_text in keep_even_if_blank:
                        continue
                    if display_view[col].map(_is_blank_like).all():
                        drop_cols.append(col)
                if drop_cols:
                    display_view = display_view.drop(columns=drop_cols, errors="ignore")

                def _to_ratio_for_display(v: object) -> float:
                    s = _clean(v)
                    if not s:
                        return np.nan
                    n = _num(v)
                    return n / 100.0 if "%" in s else n

                yield_cols = [c for c in display_view.columns if ("תשואה" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower())]
                for col in yield_cols:
                    display_view[col] = display_view[col].map(_to_ratio_for_display)

                def _safe_date_display(v: object) -> str:
                    if pd.isna(v):
                        return ""
                    if hasattr(v, "strftime"):
                        try:
                            return v.strftime("%d/%m/%Y")
                        except Exception:
                            pass
                    s = _clean(v)
                    if not s:
                        return ""
                    parsed_one = _parse_dates_flexible(pd.Series([s])).iloc[0]
                    if pd.notna(parsed_one):
                        return parsed_one.strftime("%d/%m/%Y")
                    return s

                style_format: Dict[str, object] = {}
                for date_col in ["Purchase_Date", "תאריך רכישה", "Sell_Date", "תאריך מכירה"]:
                    if date_col in display_view.columns:
                        style_format[date_col] = _safe_date_display
                for ycol in yield_cols:
                    if ycol in display_view.columns:
                        style_format[ycol] = "{:.2%}"

                tx_styled = display_view.style
                if style_format:
                    tx_styled = tx_styled.format(style_format, na_rep="")
                if yield_cols:
                    tx_styled = _apply_signed_color(tx_styled, yield_cols)

                _render_dataframe_adaptive(
                    tx_styled,
                    is_mobile,
                    force_same_render_path=True,
                    use_container_width=True,
                    hide_index=True,
                )

    elif page == page_risk:
        # Desktop sessions can stay open for long periods; refresh risk inputs so FIFO stays current.
        if (not is_mobile) and (not is_demo):
            last_risk_refresh = float(st.session_state.get("risk_page_last_refresh_ts", 0.0) or 0.0)
            now_ts = time.time()
            if (now_ts - last_risk_refresh) > 300:
                try:
                    load_google_snapshot_data.clear()
                    load_google_snapshot_data_via_gspread.clear()
                    fresh_df, _ = load_snapshot_data(web_url_clean, api_token, spreadsheet_ref, worksheet_name, service_account_file)
                    if not fresh_df.empty:
                        core = prepare_core_views(fresh_df)
                        trades = core["trades"]
                        open_trades = core["open_trades"].copy()
                        closed_trades = core["closed_trades"].copy()
                        total_cost = float(core["total_cost"])
                        total_value = float(core["total_value"])
                except Exception:
                    pass
                st.session_state["risk_page_last_refresh_ts"] = now_ts
        st.markdown(f"### {tr('Reports: Risk, Performance and FIFO', 'דוחות: סיכונים, ביצועים ועלות פיפו')}")
        fifo_df = fifo_metrics(trades)
        st.subheader(tr("FIFO Engine", "מנוע פיפו"))
        # Educational caption explaining the whole FIFO mechanic in plain language.
        with st.expander(tr("ℹ What does FIFO mean here?", "ℹ מה זה FIFO כאן?"), expanded=False):
            st.markdown(tr(
                "**FIFO = First-In-First-Out.** When you sell part of a position, the system assumes you are selling the OLDEST lots first. "
                "This is the default accounting method for most tax jurisdictions (including Israel's מס רווחי הון) because it tends to produce the largest realised gain — and therefore the highest tax bill — when prices have risen. "
                "\n\n**Why you care:**\n"
                "1. **Realised P/L (רווח ממומש)** — the profit locked in on the lots you've already sold. This is what your tax liability is based on.\n"
                "2. **Open Cost (עלות פתוחה)** — the remaining cost basis of lots you still hold. Compare it against current value to see unrealised P/L.\n"
                "3. **Average Buy Price (מחיר קנייה ממוצע)** — weighted average of the still-open lots. Useful for quick break-even checks.\n"
                "\n**Worked example:** you bought 10 BTC at ₪50K, later 10 more at ₪100K, then sold 10 BTC at ₪150K.\n"
                "• FIFO sells the FIRST 10 lots → realised gain = 10 × (150K − 50K) = ₪1,000K.\n"
                "• Remaining open position: 10 BTC at cost ₪100K each (total open cost ₪1,000K).\n"
                "• LIFO (last-in-first-out) would have given a realised gain of only ₪500K — but FIFO is the default.\n"
                "\n**Caveats:** the engine processes trades in chronological order of `Purchase_Date`; cash dividends, stock splits, and fees are NOT factored into the cost basis here — add them manually if relevant.",
                "**FIFO = First-In-First-Out (ראשון-שנכנס-ראשון-שיוצא).** כאשר מוכרים חלק מפוזיציה, המערכת מניחה שהלוטים הוותיקים ביותר נמכרים קודם. "
                "זוהי שיטת החשבונאות ברירת-המחדל ברוב מדינות העולם (כולל מס רווחי הון בישראל) כי היא נוטה לייצר את הרווח המוכר הגבוה ביותר — ובהתאם גם את חבות המס הגבוהה ביותר — כאשר המחירים עלו. "
                "\n\n**למה חשוב לך:**\n"
                "1. **רווח ממומש (Realized P/L)** — הרווח שכבר 'נעלת' על הלוטים שכבר מכרת. זהו הבסיס לחישוב המס.\n"
                "2. **עלות פתוחה (Open Cost)** — עלות הלוטים שעדיין מוחזקים. השוואה מול השווי הנוכחי נותנת רווח לא-ממומש.\n"
                "3. **מחיר קנייה ממוצע** — ממוצע משוקלל של הלוטים הפתוחים. שימושי לבדיקה מהירה של נקודת איזון (break-even).\n"
                "\n**דוגמה מעשית:** קנית 10 BTC ב-₪50K, ואז עוד 10 BTC ב-₪100K, ואז מכרת 10 BTC ב-₪150K.\n"
                "• FIFO מוכר את 10 הלוטים הראשונים → רווח ממומש = 10 × (150K − 50K) = ₪1,000K.\n"
                "• פוזיציה פתוחה שנותרה: 10 BTC בעלות ₪100K כל אחד (סה\"כ עלות פתוחה ₪1,000K).\n"
                "• LIFO היה נותן רווח ממומש של ₪500K בלבד — אך FIFO היא ברירת-המחדל.\n"
                "\n**אזהרות:** המנוע מעבד עסקאות לפי סדר כרונולוגי של `Purchase_Date`; דיבידנדים במזומן, ספליטים ועמלות לא כלולים בבסיס העלות — יש להזין אותם ידנית במידה ורלוונטי.",
            ))
        if fifo_df.empty:
            st.info(tr("Not enough data for FIFO", "אין מספיק נתונים לחישוב FIFO"))
        else:
            avg_price_col = tr("Average Buy Price", "מחיר קנייה ממוצע")
            avg_price_currency_col = "מטבע מחיר קנייה ממוצע"
            fifo_view = fifo_df.rename(
                columns={
                    "Ticker": tr("Ticker", "טיקר"),
                    "כמות פתוחה (FIFO)": tr("Open Qty (FIFO)", "כמות פתוחה (FIFO)"),
                    "עלות פתוחה (₪)": tr("Open Cost (ILS)", "עלות פתוחה (₪)"),
                    "רווח ממומש (₪)": tr("Realized P/L (ILS)", "רווח ממומש (₪)"),
                    "מחיר קנייה ממוצע": avg_price_col,
                }
            )
            fifo_view[avg_price_col] = fifo_view.apply(
                lambda r: _format_currency_value(float(r[avg_price_col]), r.get(avg_price_currency_col, "")),
                axis=1,
            )
            fifo_view = fifo_view.drop(columns=[avg_price_currency_col], errors="ignore")
            realized_col = tr("Realized P/L (ILS)", "רווח ממומש (₪)")
            fifo_styled = fifo_view.style.format(
                {
                    tr("Open Qty (FIFO)", "כמות פתוחה (FIFO)"): "{:.8f}",
                    tr("Open Cost (ILS)", "עלות פתוחה (₪)"): "₪{:,.0f}",
                    realized_col: "₪{:,.0f}",
                }
            )
            fifo_styled = _apply_signed_color(fifo_styled, [realized_col])
            _render_dataframe_adaptive(
                fifo_styled,
                is_mobile,
                force_same_render_path=True,
                use_container_width=True,
                hide_index=True,
            )

        holdings = open_trades.groupby("Ticker", as_index=False)["Quantity"].sum()
        value_series = portfolio_price_history(tuple(holdings["Ticker"]), tuple(holdings["Quantity"]), days=365)
        metrics = risk_metrics(value_series)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Sharpe", f"{metrics['sharpe']:.2f}",
                  help=tr(
                      "Sharpe Ratio = (Portfolio Return − Risk-Free Rate) / Annualized Volatility. Introduced by Nobel laureate William Sharpe (1966). It asks: 'for every unit of risk you take, how much excess return do you earn?' Benchmarks on annualized daily-returns basis: <0 = losing to cash · 0–1 = sub-par · 1–2 = good · 2–3 = very strong · >3 = outlier (double-check data quality). Weakness: Sharpe treats upside volatility as 'risk' — it penalises good surprises the same as bad ones. For asymmetric returns prefer Sortino (below).",
                      "יחס שארפ = (תשואת התיק − ריבית חסרת סיכון) / תנודתיות שנתית. הוצג ע\"י חתן פרס נובל ויליאם שארפ (1966). שואל: 'על כל יחידת סיכון שלקחת — כמה תשואה עודפת קיבלת?'. עוגני פרשנות (על בסיס תשואות יומיות שנתיות): <0 = מפסיד למזומן · 0-1 = מתחת לממוצע · 1-2 = טוב · 2-3 = חזק מאוד · >3 = חריג (כדאי לבדוק איכות נתונים). חולשה: שארפ מתייחס גם לתנודתיות כלפי מעלה כ'סיכון' — ולכן מעניש גם הפתעות טובות. עבור תשואות א-סימטריות עדיף סורטינו.",
                  ))
        c2.metric(tr("Annual Volatility", "תנודתיות שנתית"), f"{metrics['vol']:.2%}",
                  help=tr(
                      "Annualized Volatility = standard-deviation of DAILY returns × √252 (trading days/year). A portfolio with 20% annual vol typically swings within ±20% around its trend line over a year. Typical ranges: bonds 3–8% · diversified equity 12–18% · single large-cap 20–30% · crypto 60–100%+. Volatility is symmetric — it counts big up-days and big down-days equally. Use it as a 'turbulence gauge'; it does NOT predict direction or returns, only the scale of wiggles.",
                      "תנודתיות שנתית = סטיית-התקן של תשואות יומיות × √252 (ימי מסחר בשנה). תיק בתנודתיות שנתית 20% נוטה לנוע בטווח ±20% סביב קו המגמה שלו לאורך שנה. טווחים אופייניים: אג\"ח 3-8% · תיק מניות מפוזר 12-18% · מניה בודדת גדולה 20-30% · קריפטו 60-100%+. התנודתיות סימטרית — סופרת ימי עליות וירידות חזקים באותה מידה. מדד 'סערה', לא צפי כיוון או תשואה אלא רק עוצמת תנודה.",
                  ))
        c3.metric(tr("Max Drawdown", "משיכה מקסימלית"), f"{metrics['mdd']:.2%}",
                  help=tr(
                      "Maximum Drawdown (MDD) = the largest peak-to-trough decline during the observed period. If your portfolio hit ₪120K then later fell to ₪90K, that's a −25% drawdown. To FULLY recover from an X% drawdown, you need (1/(1−X) − 1) gain: a 20% drop needs +25% to break even; a 50% drop needs +100%. Historical anchors: SPY post-dot-com ≈ −49% (2000–02) · 2008 ≈ −55% · COVID crash Mar-2020 ≈ −34%. Plan your position sizing around your TRUE drawdown tolerance — most people overestimate it.",
                      "משיכה מקסימלית (MDD): הירידה הגדולה ביותר משיא לשפל בתקופה הנצפית. אם התיק הגיע ל-₪120K ואז ירד ל-₪90K זו משיכה של 25%-. להתאוששות מלאה מירידה של X% צריך רווח של (1/(1−X) − 1): ירידה של 20% דורשת +25%, ירידה של 50% דורשת +100%. עוגנים היסטוריים: SPY אחרי הדוט-קום ≈ -49% (2000-2002) · 2008 ≈ -55% · משבר הקורונה מרץ-2020 ≈ -34%. קבעו גודל פוזיציות לפי סובלנות המשיכה האמיתית — רוב האנשים מעריכים אותה ביתר.",
                  ))
        c4.metric("CAGR", f"{metrics['cagr']:.2%}",
                  help=tr(
                      "Compound Annual Growth Rate (CAGR) = (Final Value / Initial Value)^(1/years) − 1. The 'smoothed' annual return that would transform the starting balance into the ending balance over the observed period. CAGR irons out path-dependency — two portfolios that ended the same will have the same CAGR even if one was a rollercoaster. Reference points (long-run, nominal USD): S&P 500 ≈ 10% · 60/40 bond-stock ≈ 8% · US Treasuries ≈ 4% · inflation ≈ 3%. For REAL (after-inflation) returns, subtract CPI.",
                      "שיעור צמיחה שנתי מורכב (CAGR) = (שווי סופי / שווי התחלתי)^(1/שנים) − 1. התשואה השנתית ה'ממוצעת' שהייתה הופכת את הסכום ההתחלתי לסכום הסופי לאורך התקופה. ה-CAGR מחליק את מסלול הצמיחה — שני תיקים שהגיעו לאותו שווי סופי יקבלו אותו CAGR גם אם האחד חווה רכבת הרים. עוגנים ארוכי-טווח נומינליים בדולר: S&P 500 ≈ 10% · תיק 60/40 ≈ 8% · אג\"ח ממשלתי ≈ 4% · אינפלציה ≈ 3%. לתשואה ריאלית יש להוריד CPI.",
                  ))

        if not value_series.empty:
            _hist_color = "#4f46e5"
            _chart_help("Historical portfolio value estimated from market prices. Uses current holdings * historical closing prices.",
                        "שווי התיק ההיסטורי מוערך לפי מחירי שוק. מחשב את האחזקות הנוכחיות לפי מחירי הסגירה ההיסטוריים.", language)
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=value_series.index,
                y=value_series.values,
                mode="lines",
                name=tr("Estimated Historical Value", "שווי היסטורי משוער"),
                line=dict(color=_hist_color, width=2),
                fill="tozeroy",
                fillcolor="rgba(79,70,229,0.10)",
                hovertemplate=tr("Date", "תאריך") + ": %{x|%Y-%m-%d}<br>" + tr("Value", "שווי") + ": ₪%{y:,.0f}<extra></extra>",
            ))
            fig.update_layout(
                template=template,
                title=tr("Historical Value Series (Market-Based Estimate)", "סדרת שווי היסטורית (הערכה לפי מחירי שוק)"),
                xaxis_title=tr("Date", "תאריך"),
                yaxis_title=tr("Value (ILS)", "שווי (₪)"),
                yaxis_tickformat=",.0f",
                hovermode="x unified",
            )
            st.plotly_chart(_apply_plotly_theme(fig, is_dark, is_mobile), theme="streamlit", use_container_width=True)

        if total_value > 0:
            st.markdown(f"#### {tr('Scenario Lab', 'מעבדת תרחישים')}")
            _chart_help("Scenario Lab: simulates portfolio value and P/L under various market shock levels applied uniformly to all holdings.",
                        "מעבדת תרחישים: מדמה את שווי התיק ורווח/הפסד תחת רמות הלם שוק שונות המופעלות באחידות על כל האחזקות.", language)
            scenario_df = pd.DataFrame(
                [
                    {"Scenario": tr("Calm market", "שוק רגוע"), "Shock": -0.03},
                    {"Scenario": tr("Risk-off week", "שבוע ירידות"), "Shock": -0.08},
                    {"Scenario": tr("Macro stress", "סטרס מאקרו"), "Shock": -0.15},
                    {"Scenario": tr("Tail event", "אירוע קיצון"), "Shock": -0.25},
                ]
            )
            scenario_df["Estimated Value (ILS)"] = total_value * (1 + scenario_df["Shock"])
            scenario_df["Estimated P/L (ILS)"] = scenario_df["Estimated Value (ILS)"] - total_cost
            scenario_styled = scenario_df.style.format(
                {
                    "Shock": "{:.1%}",
                    "Estimated Value (ILS)": "{:,.0f}",
                    "Estimated P/L (ILS)": "{:,.0f}",
                }
            )
            scenario_styled = _apply_signed_color(scenario_styled, ["Shock", "Estimated P/L (ILS)"])
            _render_dataframe_adaptive(
                scenario_styled,
                is_mobile,
                force_same_render_path=True,
                use_container_width=True,
                hide_index=True,
            )

        # ── Advanced Analytics Suite (professional risk analytics) ──
        try:
            render_advanced_analytics(
                open_trades=open_trades,
                value_series=value_series,
                risk_metrics_dict=metrics,
                total_value=total_value,
                tr=tr,
                template=template,
                is_dark=is_dark,
                is_mobile=is_mobile,
                language=language,
            )
        except Exception as _adv_exc:
            st.caption(tr(
                f"Advanced analytics unavailable: {_adv_exc}",
                f"האנליטיקה המתקדמת אינה זמינה: {_adv_exc}",
            ))

    elif page == page_manage:
        st.markdown(f"### {tr('Trade Management (Add / Edit / Delete)', 'ניהול עסקאות (הוספה / עריכה / מחיקה)')}")
        st.caption(tr("Changes are saved directly to Google Sheets via Apps Script (no CSV write).", "שמירה מתבצעת ישירות ל-Google Sheets דרך Apps Script (ללא כתיבה ל-CSV)."))
        if is_demo:
            st.info(tr("Demo trade-desk mode: you can safely explore flows and forms without touching your personal portfolio.", "מצב דמו לחדר מסחר: אפשר לבדוק זרימות וטפסים בבטחה בלי לגעת בתיק האישי שלך."))
        write_enabled = (not is_demo) and is_apps_script_web_app_url(_clean(web_url_clean))
        if not write_enabled:
            st.warning(tr("This page is read-only in current mode. Connect Apps Script Web App to enable write actions.", "הדף במצב קריאה בלבד כי אין Web App URL תקין. כדי לאפשר הוספה/עריכה/מחיקה, חבר Apps Script Web App."))
        elif source_mode in {"local_cache", "verified_fallback", "gspread"}:
            st.info(tr("Data view loaded from fallback, but write actions remain enabled via Apps Script.", "התצוגה נטענה מגיבוי, אך פעולות הוספה/עריכה/מחיקה עדיין זמינות דרך Apps Script."))

        st.caption(tr(f"Showing {len(trades):,} snapshot transactions, including closed trades.", f"מציג {len(trades):,} עסקאות תמונת מצב, כולל עסקאות סגורות."))
        trade_view = trades.copy()
        manage_fx = _safe_quote("USDILS=X")
        if manage_fx <= 0:
            manage_fx = 3.6
        trade_qty_abs = trade_view.get("Quantity", 0).map(_num).abs()
        trade_origin_currency = trade_view.get("Origin_Currency", "").map(_normalize_currency_code)
        trade_current_ils = trade_view.get("Current_Value_ILS", 0).map(_num)
        trade_current_origin = np.where(
            trade_origin_currency == "USD",
            trade_current_ils / manage_fx,
            trade_current_ils,
        )
        trade_view["Current_Asset_Value_Display"] = pd.Series(
            np.where(trade_qty_abs > 1e-9, trade_current_origin / trade_qty_abs, 0.0),
            index=trade_view.index,
        )
        trade_view["Current_Asset_Value_Display"] = trade_view.apply(
            lambda r: _format_currency_value(float(r["Current_Asset_Value_Display"]), r.get("Origin_Currency", "")),
            axis=1,
        )
        status_filter = st.multiselect(tr("Status filter", "סינון סטטוס"), sorted([s for s in trade_view["Status"].dropna().astype(str).unique() if s]), default=[])
        if status_filter:
            trade_view = trade_view[trade_view["Status"].isin(status_filter)]
        preview_cols = [c for c in ["Trade_ID", "Purchase_Date", "Current_Location", "Platform", "Type", "Ticker", "Quantity", "Current_Asset_Value_Display", "Cost_Origin", "Cost_ILS", "Status", "validation_status"] if c in trade_view.columns]
        if preview_cols:
            sort_cols = [c for c in ["Purchase_Date", "Ticker"] if c in trade_view.columns]
            if sort_cols:
                # Use a stable sort and a Trade_ID tie-breaker so row order does not drift between reruns.
                if "Trade_ID" in trade_view.columns and "Trade_ID" not in sort_cols:
                    sort_cols = sort_cols + ["Trade_ID"]
                asc = [False if c == "Purchase_Date" else True for c in sort_cols]
                trade_view = trade_view.sort_values(sort_cols, ascending=asc, kind="mergesort")

            manage_select_df = trade_view[preview_cols].copy().reset_index(drop=True)
            row_trade_ids = manage_select_df["Trade_ID"].astype(str).map(_clean)
            selected_trade_id = _clean(st.session_state.get("selected_trade_id", ""))
            visible_trade_ids = {tid for tid in row_trade_ids.tolist() if tid}
            if selected_trade_id and selected_trade_id not in visible_trade_ids:
                selected_trade_id = ""
                st.session_state["selected_trade_id"] = ""

            manage_select_df.insert(0, "__select__", row_trade_ids == selected_trade_id)
            row_trade_id_by_pos = {i: tid for i, tid in enumerate(row_trade_ids.tolist()) if tid}
            manage_select_df = manage_select_df.rename(columns={"__select__": tr("Select", "בחר")})

            manage_select_view, manage_date_cfg = _with_calendar_purchase_date(
                localize_snapshot_view(manage_select_df, language),
                language,
                force_uniform=True,
            )
            select_col = tr("Select", "בחר")
            table_signature = hashlib.sha1(
                "|".join(manage_select_df["Trade_ID"].astype(str).tolist()).encode("utf-8")
            ).hexdigest()[:12]
            edited_manage_df = st.data_editor(
                manage_select_view,
                use_container_width=True,
                hide_index=True,
                column_config=manage_date_cfg,
                disabled=[c for c in manage_select_view.columns if c != select_col],
                key=f"manage_table_selector_{table_signature}",
            )

            selected_ids: List[str] = []
            if select_col in edited_manage_df.columns:
                selected_mask = edited_manage_df[select_col].fillna(False).astype(bool)
                selected_indices = selected_mask[selected_mask].index.tolist()
                selected_ids = [
                    row_trade_id_by_pos.get(int(i), "")
                    for i in selected_indices
                ]
                selected_ids = [tid for tid in selected_ids if tid]

            resolved_selected_id = ""
            if selected_ids:
                # Strict single-select model: newest checked row replaces previous selection.
                newly_checked = [tid for tid in selected_ids if tid and tid != selected_trade_id]
                resolved_selected_id = newly_checked[-1] if newly_checked else selected_ids[-1]

            prev_selected_id = _clean(st.session_state.get("selected_trade_id", ""))
            if resolved_selected_id != prev_selected_id:
                st.session_state["selected_trade_id"] = resolved_selected_id

            # If multiple rows were checked in the editor, force rerun so only one remains checked.
            if len(selected_ids) > 1:
                st.rerun()

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
        editable_cols = [
            "Current_Location", "Platform", "Type", "Ticker", "Purchase_Date", "Quantity",
            "Origin_Buy_Price", "Cost_Origin", "Origin_Currency", "Commission", "Status", "Sell_Date",
            "Cost_ILS", "Current_Value_ILS", "Action", "Event_Type", "Trade_ID",
        ]
        platforms = trades["Platform"].dropna().astype(str).tolist() if "Platform" in trades.columns else []
        types = trades["Type"].dropna().astype(str).tolist() if "Type" in trades.columns else []
        tickers = trades["Ticker"].dropna().astype(str).tolist() if "Ticker" in trades.columns else []
        currencies = trades["Origin_Currency"].dropna().astype(str).tolist() if "Origin_Currency" in trades.columns else []
        locations = trades["Current_Location"].dropna().astype(str).tolist() if "Current_Location" in trades.columns else []

        if mode == "add":
            partial_sell_meta: Optional[Dict[str, object]] = None
            field_help = {
                "side": tr("Choose if this new row is a purchase or a sale event.", "בחר אם הרשומה החדשה היא קנייה או מכירה."),
                "Current_Location": tr("Where the asset is currently held (wallet/broker/exchange).", "איפה הנכס מוחזק כרגע (ארנק/ברוקר/זירה)."),
                "Platform": tr("The trading platform/broker where the trade was executed.", "הפלטפורמה/ברוקר שבה בוצעה העסקה."),
                "Type": tr("Asset category such as Crypto or Capital Market.", "סוג הנכס, למשל קריפטו או שוק ההון."),
                "Ticker": tr("Asset symbol, for example BTC or VOO.", "סימול הנכס, למשל BTC או VOO."),
                "Purchase_Date": tr("Original buy date of this lot.", "תאריך הקנייה המקורי של הלוט הזה."),
                "Sell_Date": tr("Execution date of the sale.", "תאריך ביצוע המכירה."),
                "Sell_Price_Origin": tr("Actual sale price per unit in the original currency.", "מחיר המכירה בפועל ליחידה במטבע המקור."),
                "Quantity": tr("Number of units/coins in this trade.", "כמות היחידות/מטבעות בעסקה."),
                "Origin_Buy_Price": tr("Buy price per unit in the original currency.", "שער קנייה ליחידה במטבע המקור."),
                "Cost_Origin": tr("Total trade cost in original currency.", "עלות כוללת במטבע המקור."),
                "Origin_Currency": tr("Currency of buy price and origin cost (USD/ILS).", "המטבע של שער הקנייה והעלות (USD/ILS)."),
                "Commission": tr("Broker/exchange fee paid for this trade.", "העמלה ששולמה על העסקה."),
            }

            side_label_map = {
                tr("Buy", "קנייה"): "BUY",
                tr("Sell", "מכירה"): "SELL",
            }
            side_options = list(side_label_map.keys())
            side_key = "add_trade_side"
            current_side_label = _clean(st.session_state.get(side_key, side_options[0]))
            if current_side_label not in side_options:
                current_side_label = side_options[0]
            side_index = side_options.index(current_side_label)
            side_label = st.radio(
                tr("Trade side", "צד העסקה"),
                side_options,
                horizontal=True,
                key=side_key,
                index=side_index,
                help=field_help["side"],
            )
            selected_side = side_label_map[side_label]
            is_sell_side = selected_side == "SELL"

            with st.form("add_form"):
                # ── Row 1: Location / Platform / Type ──
                _fc1, _fc2, _fc3 = st.columns(3) if not is_mobile else (st.container(), st.container(), st.container())
                with _fc1:
                    _field_loc = _select_or_type(tr("Current location", "מיקום נוכחי"), locations, "", "add_location", tr, help_text=field_help["Current_Location"])
                with _fc2:
                    _field_plat = _select_or_type(tr("Platform", "פלטפורמה"), platforms, "Bit2C", "add_platform", tr, help_text=field_help["Platform"])
                with _fc3:
                    _field_type = _select_or_type(tr("Asset type", "סוג נכס"), types, "קריפטו", "add_type", tr, help_text=field_help["Type"])
                # ── Row 2: Ticker / Currency ──
                _fc4, _fc5, _fc6 = st.columns([2, 1, 1]) if not is_mobile else (st.container(), st.container(), st.container())
                with _fc4:
                    _field_ticker = _select_or_type(tr("Ticker", "טיקר"), tickers, "BTC", "add_ticker", tr, help_text=field_help["Ticker"]).upper()
                with _fc5:
                    _field_currency = _select_or_type(tr("Origin currency", "מטבע מקור"), currencies, "USD", "add_currency", tr, help_text=field_help["Origin_Currency"]).upper()
                with _fc6:
                    pass  # spacer
                new_row = {
                    "Current_Location": _field_loc,
                    "Platform": _field_plat,
                    "Type": _field_type,
                    "Ticker": _field_ticker,
                    "Origin_Currency": _field_currency,
                    "Action": selected_side,
                    "Event_Type": "TRADE",
                    "Sell_Price_Origin": 0.0,
                }

                if is_sell_side:
                    sell_from_open_lot = False
                    open_lot_ids: List[str] = []
                    if not open_trades.empty:
                        lots_src = open_trades.copy()
                        for _, lot in lots_src.iterrows():
                            lot_id = _clean(lot.get("Trade_ID", ""))
                            if not lot_id:
                                continue
                            lot_qty = float(_num(lot.get("Quantity", 0.0)))
                            if lot_qty <= 0:
                                continue
                            open_lot_ids.append(lot_id)
                        open_lot_ids = sorted(set(open_lot_ids))

                    sell_from_open_lot = st.checkbox(
                        tr("Sell from existing open lot", "מכירה מתוך לוט פתוח קיים"),
                        value=bool(open_lot_ids),
                        disabled=not bool(open_lot_ids),
                    )

                    chosen_source_trade_id = ""
                    source_row = None
                    if sell_from_open_lot and open_lot_ids:
                        chosen_source_trade_id = st.selectbox(
                            tr("Source open lot (Trade_ID)", "לוט מקור פתוח (Trade_ID)"),
                            open_lot_ids,
                        )
                        candidate_rows = open_trades[open_trades["Trade_ID"].astype(str).map(_clean) == _clean(chosen_source_trade_id)]
                        if not candidate_rows.empty:
                            source_row = candidate_rows.iloc[0]

                    if source_row is not None:
                        src_qty = float(_num(source_row.get("Quantity", 0.0)))
                        src_buy_price = float(_num(source_row.get("Origin_Buy_Price", 0.0)))
                        src_cost = float(_num(source_row.get("Cost_Origin", 0.0)))
                        src_commission = float(_num(source_row.get("Commission", 0.0)))
                        src_purchase_dt = _parse_dates_flexible(pd.Series([source_row.get("Purchase_Date", "")])).iloc[0]
                        src_purchase_txt = src_purchase_dt.strftime("%Y-%m-%d") if pd.notna(src_purchase_dt) else _clean(source_row.get("Purchase_Date", ""))

                        sell_qty = st.number_input(
                            tr("Sell quantity", "כמות למכירה"),
                            min_value=0.0,
                            max_value=float(src_qty),
                            value=float(src_qty),
                            format="%.8f",
                            help=field_help["Quantity"],
                        )
                        sell_date_txt = st.date_input(
                            tr("Sell date", "תאריך מכירה"),
                            value=datetime.now(),
                            key="add_sell_date",
                            help=field_help["Sell_Date"],
                        ).strftime("%Y-%m-%d")
                        sell_price_origin = st.number_input(
                            tr("Sell price", "מחיר מכירה"),
                            value=0.0,
                            help=field_help["Sell_Price_Origin"],
                        )
                        new_row["Sell_Price_Origin"] = float(sell_price_origin)

                        ratio = (sell_qty / src_qty) if src_qty > 1e-12 else 0.0
                        allocated_cost = float(src_cost * ratio)
                        allocated_commission = float(src_commission * ratio)
                        fx_for_origin = manage_fx if _normalize_currency_code(source_row.get("Origin_Currency", "")) == "USD" else 1.0

                        new_row.update(
                            {
                                "Current_Location": _clean(source_row.get("Current_Location", "")),
                                "Platform": _clean(source_row.get("Platform", "")),
                                "Type": _clean(source_row.get("Type", "")),
                                "Ticker": _clean(source_row.get("Ticker", "")).upper(),
                                "Origin_Currency": _normalize_currency_code(source_row.get("Origin_Currency", "")),
                                "Purchase_Date": src_purchase_txt,
                                "Quantity": float(sell_qty),
                                "Origin_Buy_Price": src_buy_price,
                                "Cost_Origin": allocated_cost,
                                "Commission": allocated_commission,
                                "Status": "סגור",
                                "Sell_Date": sell_date_txt,
                                "Current_Value_ILS": float(sell_qty * sell_price_origin * fx_for_origin),
                                "Cost_ILS": 0.0,
                            }
                        )

                        remaining_qty = max(0.0, src_qty - float(sell_qty))
                        remaining_cost = max(0.0, src_cost - allocated_cost)
                        remaining_commission = max(0.0, src_commission - allocated_commission)
                        partial_sell_meta = {
                            "source_trade_id": _clean(chosen_source_trade_id),
                            "remaining_qty": remaining_qty,
                            "remaining_cost": remaining_cost,
                            "remaining_commission": remaining_commission,
                            "source_row": source_row.to_dict(),
                            "is_partial": remaining_qty > 1e-9,
                        }
                        st.caption(
                            tr(
                                f"Remaining open quantity after sale: {remaining_qty:.8f}",
                                f"כמות פתוחה שתישאר לאחר המכירה: {remaining_qty:.8f}",
                            )
                        )
                    else:
                        # ── SELL manual form: multi-column grid on desktop ──
                        _sc1, _sc2, _sc3 = st.columns(3) if not is_mobile else (st.container(), st.container(), st.container())
                        with _sc1:
                            _sell_buy_date = st.date_input(tr("Original purchase date", "תאריך קנייה מקורי"), value=datetime.now(), help=field_help["Purchase_Date"]).strftime("%Y-%m-%d")
                            _sell_qty = st.number_input(tr("Sell quantity", "כמות למכירה"), value=0.0, format="%.8f", help=field_help["Quantity"])
                        with _sc2:
                            _sell_buy_price = st.number_input(tr("Buy price", "שער קנייה"), value=0.0, help=field_help["Origin_Buy_Price"])
                            _sell_cost = st.number_input(tr("Origin cost", "עלות מקור"), value=0.0, help=field_help["Cost_Origin"])
                        with _sc3:
                            _sell_comm = st.number_input(tr("Commission", "עמלה"), value=0.0, help=field_help["Commission"])
                            _sell_date_manual = st.date_input(tr("Sell date", "תאריך מכירה"), value=datetime.now(), key="add_sell_date_manual", help=field_help["Sell_Date"]).strftime("%Y-%m-%d")
                        new_row.update(
                            {
                                "Purchase_Date": _sell_buy_date,
                                "Quantity": _sell_qty,
                                "Origin_Buy_Price": _sell_buy_price,
                                "Cost_Origin": _sell_cost,
                                "Commission": _sell_comm,
                                "Status": "סגור",
                                "Sell_Date": _sell_date_manual,
                            }
                        )
                        sell_price_origin = st.number_input(tr("Sell price", "מחיר מכירה"), value=0.0, help=field_help["Sell_Price_Origin"])
                        new_row["Sell_Price_Origin"] = float(sell_price_origin)
                        fx_for_origin = manage_fx if _normalize_currency_code(new_row.get("Origin_Currency", "")) == "USD" else 1.0
                        new_row["Current_Value_ILS"] = float(new_row["Quantity"] * sell_price_origin * fx_for_origin)
                        new_row["Cost_ILS"] = 0.0

                    if float(_num(new_row.get("Cost_Origin", 0.0))) <= 0 and float(_num(new_row.get("Quantity", 0.0))) > 0 and float(_num(new_row.get("Origin_Buy_Price", 0.0))) > 0:
                        new_row["Cost_Origin"] = float(_num(new_row.get("Quantity", 0.0)) * _num(new_row.get("Origin_Buy_Price", 0.0)))
                else:
                    # ── BUY form: multi-column grid layout on desktop ──
                    _bc1, _bc2, _bc3 = st.columns(3) if not is_mobile else (st.container(), st.container(), st.container())
                    with _bc1:
                        _buy_date = st.date_input(tr("Purchase date", "תאריך רכישה"), value=datetime.now(), help=field_help["Purchase_Date"]).strftime("%Y-%m-%d")
                        _buy_qty = st.number_input(tr("Quantity", "כמות"), value=0.0, format="%.8f", help=field_help["Quantity"])
                    with _bc2:
                        _buy_price = st.number_input(tr("Buy price", "שער קנייה"), value=0.0, help=field_help["Origin_Buy_Price"])
                        _buy_cost = st.number_input(tr("Origin cost", "עלות מקור"), value=0.0, help=field_help["Cost_Origin"])
                    with _bc3:
                        _buy_comm = st.number_input(tr("Commission", "עמלה"), value=0.0, help=field_help["Commission"])
                    new_row.update(
                        {
                            "Purchase_Date": _buy_date,
                            "Quantity": _buy_qty,
                            "Origin_Buy_Price": _buy_price,
                            "Cost_Origin": _buy_cost,
                            "Commission": _buy_comm,
                            "Status": "פתוח",
                            "Sell_Date": "",
                            "Current_Value_ILS": 0.0,
                            "Cost_ILS": 0.0,
                        }
                    )
                    if float(_num(new_row.get("Cost_Origin", 0.0))) <= 0 and float(_num(new_row.get("Quantity", 0.0))) > 0 and float(_num(new_row.get("Origin_Buy_Price", 0.0))) > 0:
                        new_row["Cost_Origin"] = float(_num(new_row.get("Quantity", 0.0)) * _num(new_row.get("Origin_Buy_Price", 0.0)))

                new_row["Trade_ID"] = _to_trade_id(pd.Series(new_row))
                submitted = st.form_submit_button(tr("Save", "שמור"))

            if submitted:
                if not write_enabled:
                    st.error(tr("Cannot save in gspread mode. Configure a valid Web App URL on the left.", "לא ניתן לשמור במצב gspread. הגדר Web App URL תקין בצד שמאל."))
                else:
                    add_errors: List[str] = []
                    if float(_num(new_row.get("Quantity", 0.0))) <= 0:
                        add_errors.append(tr("Quantity must be greater than zero.", "כמות חייבת להיות גדולה מאפס."))
                    if float(_num(new_row.get("Origin_Buy_Price", 0.0))) <= 0:
                        add_errors.append(tr("Buy price must be greater than zero.", "שער קנייה חייב להיות גדול מאפס."))
                    if _clean(new_row.get("Action", "")).upper() == "SELL":
                        p_dt = _parse_dates_flexible(pd.Series([new_row.get("Purchase_Date", "")])).iloc[0]
                        s_dt = _parse_dates_flexible(pd.Series([new_row.get("Sell_Date", "")])).iloc[0]
                        if pd.notna(p_dt) and pd.notna(s_dt) and s_dt < p_dt:
                            add_errors.append(tr("Sell date cannot be earlier than purchase date.", "תאריך מכירה לא יכול להיות מוקדם מתאריך הקנייה."))
                        if float(_num(new_row.get("Current_Value_ILS", 0.0))) <= 0:
                            add_errors.append(tr("For SELL, sale price must produce a positive sale value.", "בעסקת מכירה, מחיר המכירה חייב להפיק שווי מכירה חיובי."))

                    if add_errors:
                        for e in add_errors:
                            st.error(e)
                        st.stop()

                    ok, msg = sync_trade_to_sheet(web_url_clean, api_token, "add", new_row)
                    if not ok:
                        st.error(f"{tr('Add failed', 'הוספה נכשלה')}: {msg}")
                    else:
                        # Partial sell: update the source open lot to remaining quantity/cost.
                        if partial_sell_meta and bool(partial_sell_meta.get("is_partial", False)):
                            src = dict(partial_sell_meta.get("source_row", {}))
                            src_trade_id = _clean(partial_sell_meta.get("source_trade_id", ""))
                            source_update = {
                                "Trade_ID": src_trade_id,
                                "Current_Location": _clean(src.get("Current_Location", "")),
                                "Platform": _clean(src.get("Platform", "")),
                                "Type": _clean(src.get("Type", "")),
                                "Ticker": _clean(src.get("Ticker", "")).upper(),
                                "Purchase_Date": _clean(src.get("Purchase_Date", "")),
                                "Quantity": float(_num(partial_sell_meta.get("remaining_qty", 0.0))),
                                "Origin_Buy_Price": float(_num(src.get("Origin_Buy_Price", 0.0))),
                                "Cost_Origin": float(_num(partial_sell_meta.get("remaining_cost", 0.0))),
                                "Origin_Currency": _normalize_currency_code(src.get("Origin_Currency", "")),
                                "Commission": float(_num(partial_sell_meta.get("remaining_commission", 0.0))),
                                "Status": "פתוח",
                                "Sell_Date": "",
                                "Cost_ILS": 0.0,
                                "Current_Value_ILS": 0.0,
                                "Action": "BUY",
                                "Event_Type": "TRADE",
                            }
                            ok_edit, msg_edit = sync_trade_to_sheet(web_url_clean, api_token, "edit", source_update)
                            if not ok_edit:
                                st.warning(
                                    tr(
                                        "Sale row was added, but source lot update failed. Please edit the source lot manually.",
                                        "שורת המכירה נוספה, אך עדכון לוט המקור נכשל. יש לעדכן את לוט המקור ידנית.",
                                    )
                                )
                                st.error(msg_edit)

                        st.success(tr("Trade added directly to Google Sheets", "הרשומה נוספה ישירות ל-Google Sheets"))
                        st.info(msg)
                        load_google_snapshot_data.clear()
                        load_google_snapshot_data_via_gspread.clear()
                        st.rerun()

        elif mode == "edit":
            selected = _clean(st.session_state.get("selected_trade_id", ""))
            if not selected:
                st.info(tr("Select a row in the table above to edit", "סמן שורה בטבלה למעלה כדי לערוך"))
                st.stop()
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
                            d = _parse_dates_flexible(pd.Series([val])).iloc[0]
                            edited[col] = st.date_input(col, value=(d.date() if pd.notna(d) else datetime.now().date()), key=f"e_{col}").strftime("%Y-%m-%d")
                        elif col == "Sell_Date":
                            current_status = _clean(edited.get("Status", trades.at[idx, "Status"]))
                            if _is_closed_status(current_status):
                                d_sell = _parse_dates_flexible(pd.Series([val])).iloc[0]
                                edited[col] = st.date_input(
                                    col,
                                    value=(d_sell.date() if pd.notna(d_sell) else datetime.now().date()),
                                    key=f"e_{col}",
                                ).strftime("%Y-%m-%d")
                            else:
                                edited[col] = ""
                        elif col == "Platform":
                            edited[col] = _select_or_type(tr("Platform", "פלטפורמה"), platforms, _clean(val), f"edit_{selected}_platform", tr)
                        elif col == "Current_Location":
                            edited[col] = _select_or_type(tr("Current location", "מיקום נוכחי"), locations, _clean(val), f"edit_{selected}_location", tr)
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
                            load_google_snapshot_data_via_gspread.clear()
                            st.rerun()
                        else:
                            st.error(f"{tr('Update failed', 'עדכון נכשל')}: {msg}")

        else:
            selected = _clean(st.session_state.get("selected_trade_id", ""))
            if not selected:
                st.info(tr("Select a row in the table above to delete", "סמן שורה בטבלה למעלה כדי למחוק"))
                st.stop()
            if st.button(tr("Delete trade", "מחק רשומה")):
                if not write_enabled:
                    st.error(tr("Cannot delete in gspread mode. Configure a valid Web App URL on the left.", "לא ניתן למחוק במצב gspread. הגדר Web App URL תקין בצד שמאל."))
                else:
                    delete_payload = {"Trade_ID": _clean(selected)}
                    selected_rows = trades[trades["Trade_ID"].astype(str) == selected] if "Trade_ID" in trades.columns else pd.DataFrame()
                    if not selected_rows.empty:
                        src = selected_rows.iloc[0]
                        src_date = _parse_dates_flexible(pd.Series([src.get("Purchase_Date", "")])).iloc[0]
                        delete_payload.update(
                            {
                                "Platform": _clean(src.get("Platform", "")),
                                "Ticker": _clean(src.get("Ticker", "")).upper(),
                                "Purchase_Date": src_date.strftime("%Y-%m-%d") if pd.notna(src_date) else _clean(src.get("Purchase_Date", "")),
                                "Quantity": float(_num(src.get("Quantity", 0))),
                                "Cost_Origin": float(_num(src.get("Cost_Origin", 0))),
                            }
                        )
                    ok, msg = sync_trade_to_sheet(web_url_clean, api_token, "delete", delete_payload)
                    if ok:
                        st.success(tr("Trade deleted directly from Google Sheets", "הרשומה נמחקה ישירות מ-Google Sheets"))
                        st.info(msg)
                        load_google_snapshot_data.clear()
                        load_google_snapshot_data_via_gspread.clear()
                        st.rerun()
                    else:
                        st.error(f"{tr('Delete failed', 'מחיקה נכשלה')}: {msg}")

    elif page == page_simulator:
        try:
            render_simulator_page(
                tr=tr,
                total_value=float(total_value),
                language=language,
                is_dark=is_dark,
                is_mobile=is_mobile,
            )
        except Exception as _sim_exc:
            st.error(tr(
                f"Simulator unavailable: {_sim_exc}",
                f"הסימולטור אינו זמין: {_sim_exc}",
            ))

    else:
        st.markdown(f"### {tr('Data Quality', 'בקרת נתונים ואיכות')}")

        # ── Always-on Data Quality KPIs ──
        non_empty_cells, total_cells, completeness = dataframe_completeness(df)
        dupes = int(df.duplicated(subset=["Trade_ID"]).sum()) if "Trade_ID" in df.columns else 0
        missing_ticker = int(df["Ticker"].map(_clean).eq("").sum()) if "Ticker" in df.columns else 0
        missing_qty = 0
        if "Quantity" in df.columns:
            missing_qty = int(pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).eq(0).sum())
        missing_date = 0
        if "Purchase_Date" in df.columns:
            missing_date = int(_parse_dates_flexible(df["Purchase_Date"]).isna().sum())

        cqa1, cqa2, cqa3, cqa4 = st.columns(4)
        cqa1.metric(tr("Data Completeness", "שלמות נתונים"), f"{completeness:.1%}")
        cqa2.metric(tr("Duplicate Trade IDs", "כפילויות Trade ID"), f"{dupes}")
        cqa3.metric(tr("Missing Tickers", "טיקרים חסרים"), f"{missing_ticker}")
        cqa4.metric(tr("Rows in Snapshot", "שורות בתמונת מצב"), f"{len(df):,}")
        try:
            style_metric_cards(border_left_color="#06b6d4", border_radius_px=12, box_shadow=True)
        except Exception:
            pass

        # ── Column-level completeness ──
        if not df.empty:
            col_total = len(df)
            col_rows = []
            for c in df.columns:
                s = df[c]
                if s.dtype.kind in "biufc":
                    non_empty = int(pd.to_numeric(s, errors="coerce").notna().sum())
                else:
                    non_empty = int(s.astype(str).map(_clean).ne("").sum())
                col_rows.append({"Column": str(c), "Completeness": non_empty / col_total if col_total else 0.0})
            col_df = pd.DataFrame(col_rows).sort_values("Completeness", ascending=True)
            try:
                fig_cc = px.bar(
                    col_df,
                    x="Completeness", y="Column", orientation="h",
                    title=tr("Column-Level Completeness", "שלמות נתונים ברמת עמודה"),
                    template=template,
                    color="Completeness",
                    color_continuous_scale=["#ef4444", "#f59e0b", "#10b981"],
                    range_color=(0.0, 1.0),
                )
                fig_cc.update_traces(
                    hovertemplate="<b>%{y}</b><br>" + tr("Completeness", "שלמות") + ": %{x:.1%}<extra></extra>",
                    texttemplate="%{x:.0%}", textposition="outside",
                )
                fig_cc.update_layout(
                    xaxis_tickformat=".0%",
                    yaxis_title="",
                    yaxis=dict(automargin=True),
                    margin=dict(l=150, r=10, t=50, b=20),
                    coloraxis_showscale=False,
                    height=max(320, 22 * len(col_df) + 80),
                )
                st.plotly_chart(_apply_plotly_theme(fig_cc, is_dark, is_mobile, is_bar=True), theme="streamlit", use_container_width=True)
            except Exception:
                pass

        # ── Suggested actions based on issues found ──
        issues: List[str] = []
        if dupes:
            issues.append(tr(f"• {dupes} duplicate Trade_ID rows — deduplicate in the source sheet.",
                             f"• {dupes} רשומות עם Trade_ID כפול — מומלץ למזג בגיליון המקור."))
        if missing_ticker:
            issues.append(tr(f"• {missing_ticker} rows with empty Ticker — fill or remove.",
                             f"• {missing_ticker} שורות ללא טיקר — יש להשלים או להסיר."))
        if missing_qty:
            issues.append(tr(f"• {missing_qty} rows with zero quantity — verify or archive.",
                             f"• {missing_qty} שורות עם כמות 0 — יש לוודא או לארכב."))
        if missing_date:
            issues.append(tr(f"• {missing_date} rows with invalid purchase date — fix date formats.",
                             f"• {missing_date} שורות עם תאריך רכישה לא תקין — יש לתקן פורמט."))
        if completeness < 0.95:
            issues.append(tr(f"• Overall completeness is {completeness:.1%} — target ≥ 95%.",
                             f"• שלמות כוללת: {completeness:.1%} — יעד מומלץ: 95% ומעלה."))
        if issues:
            with st.expander(tr("Data quality issues & suggested fixes", "ממצאי איכות נתונים והמלצות תיקון"),
                              expanded=True):
                st.markdown("\n".join(issues))
        else:
            st.success(tr("No data-quality issues detected.", "לא נמצאו בעיות איכות נתונים."))

        status_counts = core["status_counts"].copy()

        def _render_recent_data_table(df_src: pd.DataFrame) -> None:
            recent_view, _ = _with_calendar_purchase_date(localize_snapshot_view(df_src, language), language)
            signed_cols = [
                c for c in recent_view.columns
                if any(token in str(c).lower() for token in ["yield", "return", "תשואה", "pnl", "רווח"])
            ]
            recent_styled = recent_view.style.format(na_rep="")
            if signed_cols:
                recent_styled = _apply_signed_color(recent_styled, signed_cols)
            _render_dataframe_adaptive(
                recent_styled,
                is_mobile,
                force_same_render_path=True,
                use_container_width=True,
                hide_index=True,
            )

        if status_counts.empty:
            st.info(tr("No status distribution available.", "אין נתוני סטטוס להצגה."))
            st.subheader(tr("Recent Data", "נתונים אחרונים"))
            _render_recent_data_table(df)
            return

        status_counts_view = status_counts.copy()
        status_counts_view["Status"] = status_counts_view["Status"].map(lambda v: VALUE_LABELS["Status"].get(_clean(v), {}).get(language, _clean(v)))
        status_counts_view = status_counts_view.rename(columns={"Status": SNAPSHOT_HEADERS["Status"][language], "count": tr("Count", "כמות")})
        st.dataframe(status_counts_view)
        fig = px.pie(status_counts, names="Status", values="count", title=tr("Trade Status Distribution", "פיזור סטטוסי עסקאות"), template=template)
        st.plotly_chart(_apply_plotly_theme(fig, is_dark, is_mobile), theme="streamlit", use_container_width=True)

        st.subheader(tr("Recent Data", "נתונים אחרונים"))
        _render_recent_data_table(df)

    if live_updates:
        if page == page_manage:
            st.sidebar.caption(tr("Live updates are paused on Trade Management page.", "עדכון חי מושהה בדף ניהול עסקאות."))
        else:
            st.sidebar.caption(tr("Live updates run on table fragments only.", "עדכון חי פועל רק על מקטעי טבלאות."))


if __name__ == "__main__":
    main()
