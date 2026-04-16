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
CRYPTO_ETFS = {"IBIT", "ETHA", "BSOL", "MSTR"}
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
    "Current_Asset_Value_Display": {LANG_EN: "Current Asset Value", LANG_HE: "שווי נוכחי לנכס"},
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
    if is_mobile:
        fig.update_layout(
            legend=dict(orientation="h", yanchor="bottom", y=-0.30, xanchor="center", x=0.5, font=dict(size=11)),
            margin=dict(l=6, r=6, t=32, b=8),
            hoverlabel=dict(font=dict(size=13), bgcolor="rgba(30,30,30,0.85)", font_color="#fff"),
            title_font=dict(size=14),
            font=dict(size=11),
            autosize=True,
        )
        if is_bar:
            fig.update_layout(showlegend=False, coloraxis_showscale=False, bargap=0.25)
            fig.update_xaxes(tickangle=45, tickfont=dict(size=9))
            fig.update_yaxes(tickfont=dict(size=9))
        else:
            fig.update_xaxes(tickfont=dict(size=9))
            fig.update_yaxes(tickfont=dict(size=9))
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
    .block-container {{padding-top: 1.2rem;}}
    .app-header-wrap {{
        text-align: center !important;
        direction: inherit !important;
        margin: 0.05rem 0 0.5rem 0;
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
    header, [data-testid="stHeader"] {{overflow: visible !important;}}
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
    .app-main-title {{
        text-align: center !important;
        direction: inherit !important;
        color: {title_color} !important;
        font-size: 2.05rem;
        font-weight: 800;
        margin: 0.02rem 0 0.08rem 0;
        letter-spacing: 0.01em;
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
        /* Desktop: Fixed sidebar always visible on left */
        [data-testid="stSidebar"] {{
            background: {sidebar_bg} !important;
            border-right: 1px solid {metric_border} !important;
            min-height: 100vh !important;
        }}
        [data-testid="stSidebar"] > div:first-child,
        [data-testid="stSidebar"] [data-testid="stSidebarContent"] {{
            background: {sidebar_bg} !important;
        }}
    }}
    @media (max-width: 980px) {{
        .pm-metric-grid {{grid-template-columns: repeat(2, minmax(0, 1fr));}}
    }}
    @media (max-width: 768px) {{
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
        }}
        /* Hide Deploy button and branding on mobile, but keep sidebar expand */
        [data-testid="stAppDeployButton"],
        [data-testid="stBaseButton-header"],
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
        h1 {{font-size: 1.6rem !important; margin: 0.2rem 0 0.35rem !important; line-height: 1.2 !important;}}
        h2 {{font-size: 1.3rem !important; margin: 0.18rem 0 0.32rem !important; line-height: 1.2 !important;}}
        h3 {{font-size: 1.1rem !important; margin: 0.16rem 0 0.28rem !important; line-height: 1.2 !important;}}

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
            touch-action: pan-y !important;
            -webkit-overflow-scrolling: touch !important;
        }}
        [data-testid="stPlotlyChart"] .plotly .drag {{
            touch-action: pan-y !important;
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
        .app-main-title {{ font-size: 1.45rem !important; margin-bottom: 0.08rem !important; }}
        .app-sub-title {{ font-size: 0.86rem !important; margin-bottom: 0.18rem !important; }}
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
            width: 100% !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child) {{
            background: {nav_bg} !important;
            border: 1px solid {nav_border} !important;
            border-radius: 12px !important;
            padding: 3px !important;
            margin: 0 0 6px 0 !important;
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
        /* ══════════════════════════════════════════════════════════════════ */
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
    """Inject dark-mode styles for dropdown popovers into the parent document.

    Streamlit's BaseWeb selectbox dropdown is rendered as a portal at the
    document root level.  CSS injected via st.markdown lives inside the
    Streamlit container and cannot override Styletron's inline styles on
    the portal.  This function uses a tiny JS snippet to plant a <style>
    tag directly in the parent document <head> with high-specificity rules.
    """
    if not is_dark:
        # In light mode, remove any leftover dark-dropdown style tag.
        components.html(
            """<script>(function(){
              try {
                var d = window.parent ? window.parent.document : document;
                var s = d.getElementById('pm-dark-dropdown');
                if (s) s.remove();
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
    # Escape for JS string
    escaped = dropdown_css.replace("\\", "\\\\").replace("`", "\\`").replace("$", "\\$")

    components.html(
        f"""<script>(function(){{
          try {{
            var d = window.parent ? window.parent.document : document;
            var s = d.getElementById('pm-dark-dropdown');
            if (!s) {{
              s = d.createElement('style');
              s.id = 'pm-dark-dropdown';
              d.head.appendChild(s);
            }}
            s.textContent = `{escaped}`;
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
            // Re-bind on every run() call (Streamlit rerenders detach listeners)
            if (rootWin.__pmSwipeBound) return;

            const blockedSelector = [
              'input',
              'textarea',
              'select',
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
              if (Math.abs(dx) < 38 || Math.abs(dx) < Math.abs(dy) * 1.1) return;

              const tabList = findDashboardTabList();
              if (!tabList) return;

              const currentTabs = Array.from(tabList.querySelectorAll(
                '[data-baseweb="tab"], [role="tab"], button[data-testid="stTab"]'
              ));
              const active = currentTabs.findIndex((t) => t.getAttribute('aria-selected') === 'true');
              if (active < 0) return;

              // Swipe right should move to the tab on the left, swipe left to the right.
              const next = dx > 0 ? active - 1 : active + 1;
              if (next >= 0 && next < currentTabs.length) {
                currentTabs[next].click();
                currentTabs[next].scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' });
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

          function run() {
            removeBranding();
            setupTabSwipe();
            fixSidebarLeft();
          }

          run();
          if (!rootWin._pmObserverAttached) {
            const obs = new MutationObserver(run);
            obs.observe(rootDoc.body, { childList: true, subtree: true });
            rootWin._pmTimerBranding = rootWin.setInterval(removeBranding, 1200);
            rootWin._pmTimerSidebar = rootWin.setInterval(fixSidebarLeft, 800);
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
    symbol = _tradingview_symbol(ticker)
    if not symbol:
        st.info("No chart symbol")
        return
    tv_theme = "dark" if theme == "dark" else "light"
    widget_html = f"""
    <div class="tradingview-widget-container" style="height:{height - 24}px;width:100%">
      <div id="tradingview_chart" style="height:100%;width:100%"></div>
      <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
      <script type="text/javascript">
        new TradingView.widget({{
          "autosize": true,
          "symbol": "{symbol}",
          "interval": "240",
          "timezone": "Etc/UTC",
          "theme": "{tv_theme}",
          "style": "1",
          "locale": "en",
          "allow_symbol_change": true,
          "container_id": "tradingview_chart"
        }});
      </script>
    </div>
    """
    components.html(widget_html, height=height, scrolling=False)


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
    btc_value = float(work.loc[work["Ticker"].isin(["BTC", "IBIT", "MSTR"]), "Current_Value_ILS"].sum())

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
    concentration_rows: List[Dict[str, float]] = []
    for asset, etf in asset_map.items():
        direct = work[(work["Ticker"] == asset) & (work["Type"] == "קריפטו")]
        etf_df = work[work["Ticker"] == etf]
        direct_qty = float(direct["Quantity"].sum())
        direct_val = float(direct["Current_Value_ILS"].sum())
        etf_qty = float(etf_df["Quantity"].sum())
        etf_val = float(etf_df["Current_Value_ILS"].sum())
        estimated_btc_qty = 0.0
        if asset == "BTC":
            mstr_val = float(work.loc[work["Ticker"] == "MSTR", "Current_Value_ILS"].sum())
            indirect_btc_ils = etf_val + mstr_val
            btc_ils_basis = (direct_val / direct_qty) if direct_qty > 1e-9 else 0.0
            if btc_ils_basis > 0:
                estimated_btc_qty = direct_qty + (indirect_btc_ils / btc_ils_basis)
            elif fx > 0 and btc_usd > 0:
                # Fallback when there is no direct BTC position to infer a portfolio-based BTC ILS price.
                estimated_btc_qty = direct_qty + (indirect_btc_ils / fx / btc_usd)
            else:
                estimated_btc_qty = direct_qty
        concentration_rows.append(
            {
                "Asset": asset,
                "Direct_Qty": direct_qty,
                "Direct_ILS": direct_val,
                "ETF_Qty": etf_qty,
                "ETF_ILS": etf_val,
                "Total_Exposure_ILS": direct_val + etf_val,
                "Estimated_BTC_Qty": estimated_btc_qty,
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
            out["Purchase_Date"] = pd.to_datetime(out["Purchase_Date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
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


def main() -> None:
    st.set_page_config(page_title="מערכת ניהול תיק", page_icon="📈", layout="wide", initial_sidebar_state="auto")

    settings = load_local_settings()
    language_default = _clean(settings.get("language", DEFAULT_LANGUAGE)) or DEFAULT_LANGUAGE
    if language_default not in {LANG_EN, LANG_HE}:
        language_default = DEFAULT_LANGUAGE
    theme_default = _normalize_theme_mode(settings.get("theme_mode", THEME_SYSTEM))

    st.sidebar.markdown("### App")
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
    demo_mode = st.sidebar.checkbox(tr("Demo view", "מצב הדגמה"), value=bool(settings.get("demo_mode", False)))
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

    _render_premium_sidebar_lottie(language)
    _space(16)

    page_dashboard = tr("Dashboard", "דשבורד")
    page_manage = tr("Trade Management", "ניהול עסקאות")
    page_risk = tr("Risk & FIFO", "סיכונים ופיפו")
    page_quality = tr("Data Quality", "בקרת נתונים")
    page_options = [page_dashboard, page_manage, page_risk, page_quality]
    is_mobile = _is_mobile_client()
    theme_base = _resolve_theme_base(theme_mode)
    is_dark = theme_base == "dark"
    template = "plotly_dark" if is_dark else "plotly_white"

    if is_mobile:
        # ── Mobile: top segmented control for page nav ──
        page_labels = {
            tr("📊 Overview", "📊 סקירה"): page_dashboard,
            tr("💼 Trades", "💼 עסקאות"): page_manage,
            tr("🛡 Risk", "🛡 סיכון"): page_risk,
            tr("📋 Data", "📋 נתונים"): page_quality,
        }
        page_choice = st.radio(
            tr("Page", "עמוד"),
            list(page_labels.keys()),
            horizontal=True,
            key="page_selector",
            label_visibility="collapsed",
        )
        page = page_labels.get(page_choice, page_dashboard)
    else:
        # ── Desktop: sidebar option-menu navigation ──
        st.sidebar.title(tr("Navigation", "ניווט"))
        if option_menu is not None:
            nav_container_bg = "#1E1E1E" if is_dark else "#f8f9fa"
            nav_container_border = "0px solid transparent"
            nav_icon_color = "#93c5fd" if is_dark else "#2563eb"
            nav_text_color = "#e2e8f0" if is_dark else "#0f172a"
            nav_hover_color = "#334155" if is_dark else "#e5e7eb"
            nav_selected_bg = "#374151" if is_dark else "#e2e8f0"
            nav_selected_text = "#ffffff" if is_dark else "#0f172a"
            with st.sidebar:
                page = option_menu(
                    menu_title=None,
                    options=page_options,
                    icons=["house", "wallet", "shield-check", "database-check"],
                    default_index=0,
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
        else:
            st.sidebar.caption(tr("Install streamlit-option-menu for enhanced navigation.", "להשלמת תפריט הניווט יש להתקין streamlit-option-menu."))
            page = st.sidebar.selectbox(tr("Page", "עמוד"), page_options)

    st.markdown(
        f"<div class='app-header-wrap'><h1 class='app-main-title'>{tr('Portfolio Manager OS', 'מערכת ניהול תיק')}</h1>"
        f"<div class='app-sub-title'>{page}</div></div>",
        unsafe_allow_html=True,
    )

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
        if is_mobile:
            # ── Mobile: 2x2 compact grid ──
            kpi_r1 = st.columns(2)
            kpi_r1[0].metric(tr("Total Value", "שווי כולל"), total_value_txt, f"{total_profit:,.0f} ₪")
            kpi_r1[1].metric(tr("Total Cost", "עלות כוללת"), total_cost_txt)
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
            kpi_cols[1].metric(tr("Total Cost (ILS)", "עלות כוללת (₪)"), total_cost_txt)
            kpi_cols[2].metric(tr("Total Return", "תשואה כוללת"), total_return_txt)
            kpi_cols[3].metric(
                tr("Closed Positions", "פוזיציות סגורות"),
                str(len(closed_trades)),
                f"{len(open_trades)} {tr('open', 'פתוחות')}",
            )
        style_metric_cards(border_left_color="#4f46e5", border_radius_px=12, box_shadow=True)

        class_mix = pd.DataFrame(columns=["Asset_Class", "Current_Value_ILS"])
        if not open_trades.empty and {"Ticker", "Type", "Current_Value_ILS"}.issubset(open_trades.columns):
            class_work = open_trades[["Ticker", "Type", "Current_Value_ILS"]].copy()
            class_work["Ticker"] = class_work["Ticker"].map(lambda v: _clean(v).upper())
            class_work["Type"] = class_work["Type"].map(_clean)

            def _class_bucket(row: pd.Series) -> str:
                ticker = _clean(row.get("Ticker", "")).upper()
                type_text = _clean(row.get("Type", ""))
                type_upper = type_text.upper()
                if "ETF" in type_upper or ticker in CRYPTO_ETFS:
                    return tr("ETF", "ETF")
                if (type_text == "קריפטו") or (type_upper == "CRYPTO") or (ticker in {"BTC", "ETH", "SOL", "XRP"}):
                    return tr("Crypto", "קריפטו")
                return tr("Stocks", "מניות")

            class_work["Asset_Class"] = class_work.apply(_class_bucket, axis=1)
            class_mix = class_work.groupby("Asset_Class", as_index=False)["Current_Value_ILS"].sum().sort_values("Current_Value_ILS", ascending=False)

        perf_cols = {"Purchase_Date", "Cost_ILS", "Current_Value_ILS"}
        can_show_build_up = (not open_trades.empty) and perf_cols.issubset(open_trades.columns)

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
                st.plotly_chart(_apply_plotly_theme(fig_pie, is_dark, is_mobile), use_container_width=True)
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
                st.plotly_chart(_apply_plotly_theme(fig_bar, is_dark, is_mobile, is_bar=True), use_container_width=True)

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
                        }
                    )
                    exposure_styled = _apply_signed_color(exposure_styled, [pnl_col, yield_origin_col, yield_ils_col])
                    st.dataframe(exposure_styled, use_container_width=True, hide_index=True)

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
                    _, close_col = st.columns([8, 1])
                    if close_col.button(tr("Close", "סגור"), key=f"{widget_prefix}_tv_inline_close"):
                        st.session_state["tv_chart_open"] = False
                        st.session_state.pop("tv_chart_ticker", None)
                        st.session_state[f"{widget_prefix}_chart_scroll_pending"] = False
                    _render_tradingview_widget(active_ticker, height=380 if is_mobile else 560)
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
                    render_exposure_section(live_summary, widget_prefix="overview")

                _exposure_fragment()
            else:
                render_exposure_section(summary, widget_prefix="overview")

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
                if not summary.empty:
                    alloc_fig = px.pie(
                        summary,
                        names="Ticker",
                        values="Value_ILS",
                        title=tr("Allocation by Ticker", "חלוקה לפי טיקר"),
                        hole=0.45,
                        template=template,
                        color_discrete_sequence=_BRAND_PALETTE,
                    )
                    alloc_fig.update_traces(
                        hovertemplate="<b>%{label}</b><br>₪%{value:,.0f}<br>%{percent}<extra></extra>",
                        textinfo="percent+label",
                    )
                    st.plotly_chart(_apply_plotly_theme(alloc_fig, is_dark, is_mobile), use_container_width=True)
                else:
                    st.info(tr("No allocation data", "אין נתוני חלוקה"))
            with alloc_col2:
                if not class_mix.empty:
                    type_fig = px.treemap(
                        class_mix,
                        path=["Asset_Class"],
                        values="Current_Value_ILS",
                        title=tr("Allocation by Asset Class", "חלוקה לפי סוג נכס"),
                        template=template,
                        color_discrete_sequence=_BRAND_PALETTE,
                    )
                    type_fig.update_traces(
                        hovertemplate="<b>%{label}</b><br>₪%{value:,.0f}<extra></extra>",
                    )
                    st.plotly_chart(_apply_plotly_theme(type_fig, is_dark, is_mobile), use_container_width=True)
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
            selected_report = st.selectbox(tr("Report Type", "סוג דוח"), list(report_options.keys()), key="reports_type_select")
            selected_key = report_options[selected_report]

            if selected_key == "live_rates":
                rates = reports_payload.get("live_rates", {}) if isinstance(reports_payload, dict) else {}
                if not rates:
                    st.info(tr("No market rates available.", "אין שערי שוק זמינים כרגע."))
                else:
                    rates_df = pd.DataFrame(
                        [{tr("Symbol", "סימול"): k, tr("Rate", "שער"): v} for k, v in rates.items()]
                    )
                    st.dataframe(
                        rates_df.style.format({tr("Rate", "שער"): "{:,.4f}"}),
                        use_container_width=True,
                        hide_index=True,
                    )
            else:
                report_df = reports_payload.get(selected_key, pd.DataFrame()) if isinstance(reports_payload, dict) else pd.DataFrame()
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
                    st.dataframe(report_styled, use_container_width=True, hide_index=True)

        with tab_deposits:
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
                st.plotly_chart(_apply_plotly_theme(fig_track, is_dark, is_mobile), use_container_width=True)

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
            tx_cols = [c for c in ["Purchase_Date", "Platform", "Type", "Ticker", "Quantity", "Cost_ILS", "Current_Value_ILS", "Status"] if c in trades.columns]
            tx_raw = trades[tx_cols].copy() if tx_cols else pd.DataFrame()
            if not tx_raw.empty and "Ticker" in tx_raw.columns:
                ticker_options = sorted({_clean(v).upper() for v in tx_raw["Ticker"].tolist() if _clean(v)})
                chosen_tickers = st.multiselect(
                    tr("Ticker filter", "סינון לפי טיקר"),
                    ticker_options,
                    default=[],
                    key="dashboard_transactions_ticker_filter",
                )
                if chosen_tickers:
                    selected_set = {t.upper() for t in chosen_tickers}
                    tx_raw = tx_raw[tx_raw["Ticker"].map(lambda v: _clean(v).upper()).isin(selected_set)]
            tx_view = localize_snapshot_view(tx_raw, language) if not tx_raw.empty else pd.DataFrame()
            if tx_view.empty:
                st.info(tr("No transactions to display", "אין עסקאות להצגה"))
            else:
                st.dataframe(tx_view, use_container_width=True, hide_index=True)

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
##
        st.markdown(f"### {tr('Risk, Performance and FIFO', 'סיכונים, ביצועים ועלות פיפו')}")
        fifo_df = fifo_metrics(trades)
        st.subheader(tr("FIFO Engine", "מנוע פיפו"))
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
            st.dataframe(fifo_styled, use_container_width=True, hide_index=True)

        holdings = open_trades.groupby("Ticker", as_index=False)["Quantity"].sum()
        value_series = portfolio_price_history(tuple(holdings["Ticker"]), tuple(holdings["Quantity"]), days=365)
        metrics = risk_metrics(value_series)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Sharpe", f"{metrics['sharpe']:.2f}")
        c2.metric(tr("Annual Volatility", "תנודתיות שנתית"), f"{metrics['vol']:.2%}")
        c3.metric(tr("Max Drawdown", "משיכה מקסימלית"), f"{metrics['mdd']:.2%}")
        c4.metric("CAGR", f"{metrics['cagr']:.2%}")

        if not value_series.empty:
            _hist_color = "#4f46e5"
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
            st.plotly_chart(_apply_plotly_theme(fig, is_dark, is_mobile), use_container_width=True)

        if total_value > 0:
            st.markdown(f"#### {tr('Scenario Lab', 'מעבדת תרחישים')}")
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
            st.dataframe(scenario_styled, use_container_width=True, hide_index=True)

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
        preview_cols = [c for c in ["Trade_ID", "Purchase_Date", "Platform", "Type", "Ticker", "Quantity", "Current_Asset_Value_Display", "Cost_Origin", "Cost_ILS", "Status", "validation_status"] if c in trade_view.columns]
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
                st.stop()
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
        st.markdown(f"### {tr('Data Quality', 'בקרת נתונים ואיכות')}")
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
        st.plotly_chart(_apply_plotly_theme(fig, is_dark, is_mobile), use_container_width=True)

        st.subheader(tr("Recent Data", "נתונים אחרונים"))
        st.dataframe(localize_snapshot_view(df.tail(30), language))

    if live_updates:
        if page == page_manage:
            st.sidebar.caption(tr("Live updates are paused on Trade Management page.", "עדכון חי מושהה בדף ניהול עסקאות."))
        else:
            st.sidebar.caption(tr("Live updates run on table fragments only.", "עדכון חי פועל רק על מקטעי טבלאות."))


if __name__ == "__main__":
    main()
