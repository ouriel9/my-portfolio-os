import hashlib
import json
import secrets as _secrets
import os
import re
import socket
import subprocess
import sys
import threading
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

# ── Optional corporate proxy (Intel WPAD) so yfinance / requests work on a ──
# ── locked-down corp network. OPT-IN ONLY: set PP_USE_INTEL_PROXY=1.        ──
# Security: this must NOT auto-activate. Silently routing ALL outbound traffic
# (which carries the API token and GitHub PAT) through a corp proxy the moment a
# `wpad.intel.com` hostname happens to resolve — including on an untrusted or
# DNS-spoofed network — is an exfiltration risk for a public-repo finance app.
# When explicitly enabled, an unreachable proxy host falls back to direct.
def _auto_detect_proxy() -> None:
    if os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy"):
        return  # already configured by the environment
    if os.environ.get("PP_USE_INTEL_PROXY", "").strip().lower() not in ("1", "true", "yes"):
        return  # not opted in → never touch proxy settings
    try:
        import urllib.request
        urllib.request.urlopen("http://wpad.intel.com/wpad.dat", timeout=2)
        os.environ["HTTPS_PROXY"] = "http://proxy-iil.intel.com:912"
        os.environ["HTTP_PROXY"] = "http://proxy-iil.intel.com:912"
    except Exception:
        pass

_auto_detect_proxy()

CRYPTO_SHARE_TICKERS = {"IBIT", "ETHA", "BSOL"}  # MSTR is a stock — keep crypto-share KPI consistent with the allocation pie (audit)
BTC_SHARE_TICKERS = {"BTC", "IBIT"}
# The crypto-proxy ETFs (IBIT/ETHA/BSOL) are ETFs for the ALLOCATION pie — match
# engine.js assetClass() which returns "etf" for them — while still counting in the
# separate crypto-SHARE KPI via CRYPTO_SHARE_TICKERS. (audit sync-defs assetClass drift)
CHART_CRYPTO_TICKERS = {"BTC", "ETH", "SOL", "XRP"}
CHART_STOCK_TICKERS = {"MSTR"}
CHART_FORCED_ETF_TICKERS = {"QQQ", "VOO", "IBIT", "ETHA", "BSOL"}
# Broad offline classifier sets — mirror engine.js KNOWN_COINS / KNOWN_ETFS so a
# blank-Type / English-CRYPTO / new-coin row classes identically in app.py and the
# native app (allocation pie + crypto-share KPI). (audit sync-defs assetClass drift)
KNOWN_COINS = {
    "BTC", "ETH", "SOL", "XRP", "ADA", "DOGE", "AVAX", "DOT", "LINK", "MATIC", "BNB",
    "LTC", "BCH", "TRX", "SHIB", "UNI", "ATOM", "XLM", "NEAR", "APT", "ARB", "OP",
    "FIL", "ICP", "HBAR", "VET", "ALGO", "SAND", "MANA", "AAVE", "GRT", "SUI", "PEPE",
    "TON", "TIA", "BONK", "JUP", "WIF", "RENDER", "FET", "INJ", "SEI", "STX", "RUNE",
}
KNOWN_ETFS = {
    "SPY", "VOO", "VTI", "QQQ", "QQQM", "IVV", "VEA", "VWO", "BND", "AGG", "VUG", "VTV",
    "IWM", "IJR", "IJH", "VIG", "SCHD", "VYM", "VGT", "XLK", "XLF", "XLE", "XLV", "XLY",
    "XLP", "XLI", "XLU", "XLB", "XLRE", "XLC", "ARKK", "ARKG", "DIA", "EFA", "EEM", "GLD",
    "SLV", "TLT", "LQD", "HYG", "VXUS", "IXUS", "ITOT", "SPLG", "RSP", "SMH", "SOXX",
    "IBIT", "ETHA", "BSOL", "FBTC", "GBTC", "IEFA", "IEMG", "VT", "MGK", "SPYG", "SCHG",
    "JEPI", "JEPQ", "DGRO", "SCHB", "SCHX", "VOOG", "VONG", "SPLV", "USMV", "MOAT", "QUAL",
}


def _is_crypto_ticker(ticker: object) -> bool:
    """Mirror engine.js isCryptoTicker: `-USD`/`-USDT`/`USDT` spot suffixes or a base
    in KNOWN_COINS. (audit sync-defs)"""
    t = _clean(ticker).upper()
    if re.search(r"-USD$|-USDT$|USDT$", t):
        return True
    base = re.sub(r"-?USD[T]?$", "", t)
    return base in KNOWN_COINS


def _asset_class_label(ticker: object, type_text: object) -> str:
    """Label-free asset-class classifier returning 'crypto'/'etf'/'stocks'. Shared by the
    allocation pie and the crypto-share KPI so app.py and the native app agree on blank-Type
    / English-CRYPTO / new-coin rows. Precedence (canonical, audit sync-defs): MSTR→stocks,
    then the forced/crypto-proxy ETFs (IBIT/ETHA/BSOL/QQQ/VOO)→etf BEFORE the crypto test so a
    crypto-proxy ETF tagged 'קריפטו' still buckets as ETF for the pie, then crypto (broad
    KNOWN_COINS/-USD heuristic mirroring engine.js isCryptoTicker), then remaining ETFs."""
    t = _clean(ticker).upper()
    type_clean = _clean(type_text)
    type_upper = type_clean.upper()
    if t in CHART_STOCK_TICKERS:  # MSTR — leveraged equity proxy overrides crypto sets
        return "stocks"
    # Forced/crypto-proxy ETFs win over a crypto Type label (canonical ordering).
    if t in CHART_FORCED_ETF_TICKERS:
        return "etf"
    if (type_clean == "קריפטו") or (type_upper == "CRYPTO") or (t in CHART_CRYPTO_TICKERS) or _is_crypto_ticker(t):
        return "crypto"
    if ("ETF" in type_upper) or (type_clean == "קרן סל") or (t in KNOWN_ETFS):
        return "etf"
    return "stocks"
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
def _resolve_data_dir() -> Path:
    """Directory holding per-user data files (config, deposits, portfolio).

    Dev mode: the repo dir (next to app.py).
    Frozen (PyInstaller): config is intentionally NOT bundled (it holds the
    API token) — prefer the EXE's own directory, falling back to the original
    project dir on this machine so the desktop bundle keeps working.
    """
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        if (exe_dir / "app_local_config.json").exists():
            return exe_dir
        # Candidate source-tree locations (the project was consolidated onto the
        # Desktop; the old PycharmProjects path is kept as a fallback).
        for cand in (
            Path(r"C:\Users\ourie\Desktop\Portfolio OS\my-portfolio-os"),
            Path(r"C:\Users\ourie\PycharmProjects\my-portfolio-os"),
        ):
            if (cand / "app_local_config.json").exists():
                return cand
        return exe_dir
    return Path(__file__).resolve().parent


_DATA_DIR = _resolve_data_dir()
LOCAL_SETTINGS_FILE = _DATA_DIR / "app_local_config.json"
APP_LOCK_FILE = _DATA_DIR / "app_lock.json"  # passcode lock (hash+salt), per device


def _lock_load() -> dict:
    try:
        return json.loads(APP_LOCK_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _lock_save(d: dict) -> None:
    try:
        APP_LOCK_FILE.write_text(json.dumps(d), encoding="utf-8")
    except Exception:
        pass


# ── One-shot GLOBAL lock reset (Streamlit Cloud) ─────────────────────────────
# The lock file lives in _DATA_DIR, which on Streamlit Cloud is shared/server-side
# and SURVIVES redeploys. A stale lock (here: fingerprint-only, no passcode
# fallback) therefore locks the owner out with no way back in if the registered
# credential is lost. Bump _LOCK_RESET_TOKEN to force-clear the lock exactly ONCE
# on the next deploy. A separate marker file records the applied token, so this
# never fights a lock the user deliberately sets afterwards.
_LOCK_RESET_TOKEN = "2026-06-20-r18"
_LOCK_RESET_MARKER = _DATA_DIR / "lock_reset_done.json"


def _lock_maybe_reset() -> None:
    try:
        done = ""
        try:
            done = json.loads(_LOCK_RESET_MARKER.read_text(encoding="utf-8")).get("token", "")
        except Exception:
            done = ""
        if done != _LOCK_RESET_TOKEN:
            try:
                APP_LOCK_FILE.unlink()
            except Exception:
                _lock_save({})
            try:
                _LOCK_RESET_MARKER.write_text(json.dumps({"token": _LOCK_RESET_TOKEN}), encoding="utf-8")
            except Exception:
                pass
    except Exception:
        pass


_lock_maybe_reset()


def _lock_hash(pin: str, salt: str) -> str:
    return hashlib.sha256((salt + str(pin)).encode("utf-8")).hexdigest()


def _bio_register_html(label: str) -> str:
    """Button + WebAuthn registration: enrols the device's platform authenticator
    (fingerprint/face), stores the credential id in localStorage, reloads with ?_bioreg=ok."""
    return """
<div style="text-align:center">
  <button id="bioR" style="width:100%;padding:9px 12px;border-radius:9px;border:1px solid #6366f1;background:transparent;color:inherit;font-size:.92rem;cursor:pointer">\U0001F446 __LABEL__</button>
  <div id="bioRM" style="font-size:.75rem;opacity:.8;margin-top:5px"></div>
</div>
<script>(function(){
  var W=(window.parent&&window.parent!==window)?window.parent:window;
  var b=document.getElementById('bioR'), m=document.getElementById('bioRM');
  b.onclick=async function(){ try{
    var nav=(W.navigator&&W.navigator.credentials)?W.navigator:navigator;
    var cred=await nav.credentials.create({publicKey:{challenge:new Uint8Array(32),rp:{name:'Portfolio OS'},
      user:{id:new Uint8Array(16),name:'owner',displayName:'Owner'},
      pubKeyCredParams:[{type:'public-key',alg:-7},{type:'public-key',alg:-257}],
      authenticatorSelection:{authenticatorAttachment:'platform',userVerification:'required'},timeout:60000}});
    var raw=new Uint8Array(cred.rawId); W.localStorage.setItem('pos_bio_cred', btoa(String.fromCharCode.apply(null,raw)));
    m.style.color='#16a34a'; m.textContent='✓ נרשם / registered';
  }catch(e){ m.textContent='✗ '+((e&&e.message)||e); } };
})();</script>
""".replace("__LABEL__", label)


def _bio_unlock_html(label: str, fail_msg: str, none_msg: str) -> str:
    """Button + WebAuthn assertion: prompts the fingerprint/face, on success reloads with ?_bio=ok."""
    return """
<div style="text-align:center;margin-top:8px">
  <button id="bioU" style="width:100%;padding:10px 14px;border-radius:10px;border:1px solid #6366f1;background:#6366f1;color:#fff;font-size:1rem;cursor:pointer">\U0001F446 __LABEL__</button>
  <div id="bioUM" style="font-size:.78rem;opacity:.8;margin-top:6px"></div>
</div>
<script>(function(){
  var W=(window.parent&&window.parent!==window)?window.parent:window;
  var b=document.getElementById('bioU'), m=document.getElementById('bioUM');
  b.onclick=async function(){ try{
    var cid=W.localStorage.getItem('pos_bio_cred'); if(!cid){ m.textContent='__NONE__'; return; }
    var id=Uint8Array.from(atob(cid),function(c){return c.charCodeAt(0);});
    var nav=(W.navigator&&W.navigator.credentials)?W.navigator:navigator;
    await nav.credentials.get({publicKey:{challenge:new Uint8Array(32),
      allowCredentials:[{type:'public-key',id:id}],userVerification:'required',timeout:60000}});
    // The iframe cannot navigate the parent, but it CAN click a hidden Streamlit button there.
    var bs=W.document.querySelectorAll('button'); var ok=null;
    for(var i=0;i<bs.length;i++){ if((bs[i].textContent||'').trim()==='biounlock_bridge'){ ok=bs[i]; break; } }
    if(ok){ ok.click(); } else { m.textContent='bridge missing — reload once'; }
  }catch(e){ m.textContent='__FAIL__: '+((e&&e.message)||e); } };
})();</script>
""".replace("__LABEL__", label).replace("__NONE__", none_msg).replace("__FAIL__", fail_msg)
DEFAULT_SERVICE_ACCOUNT_FILE = _DATA_DIR / "clean-linker-492313-s3-770814e64205.json"
DEFAULT_WORKSHEET_NAME = "תמונת מצב"
DEFAULT_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbyDKgJszq8NWNgG7OQVPLflfN2rufBhAT5-fzmjy8iEVFMmNLZlK_CeI4MFvx1dijZF/exec"
MANUAL_DEPOSITS_FILE = _DATA_DIR / "manual_deposits_store.json"
LOCAL_PORTFOLIO_FILE = _DATA_DIR / "portfolio_data.json"
LOCAL_SNAPSHOT_CACHE_FILE = Path(__file__).resolve().parent / "snapshot_cache.csv"
VERIFIED_DATA_FALLBACK_FILE = Path(__file__).resolve().parent / "DATA" / "verified_data.csv"
APPS_SCRIPT_COOLDOWN_FILE = Path(__file__).resolve().parent / "apps_script_cooldown.json"

# ── Thread-safe local portfolio file access ────────────────────────────
# All reads/writes to LOCAL_PORTFOLIO_FILE go through this lock so that
# concurrent Streamlit sessions (on the same server) cannot race-corrupt the file.
_LOCAL_FILE_LOCK = threading.Lock()

# In-memory cache of the parsed local store, keyed by the file's mtime. Lets the
# app serve portfolio data straight from RAM on every rerun (instant page
# switches) instead of re-reading + re-parsing portfolio_data.json each time.
# Invalidated automatically whenever the file changes (local edit or background
# sheet sync both bump the mtime). Callers always get a COPY so in-place edits
# (e.g. live price updates on the dashboard) never corrupt the cached frame.
_LOCAL_PORTFOLIO_MEM: Dict[str, object] = {"mtime": -1.0, "df": None, "meta": {}}


def _write_portfolio_atomic(data: dict) -> None:
    """Write portfolio data atomically via temp-file rename + keep 3 rolling backups.

    Backup rotation: portfolio_data.json → .bak1 → .bak2 → .bak3
    Worst-case: only the most recent write is lost (temp file + 3 backups available).
    Must be called with _LOCAL_FILE_LOCK held.
    """
    parent = LOCAL_PORTFOLIO_FILE.parent
    # Rotate backups: .bak2 → .bak3, .bak1 → .bak2
    for i in range(2, 0, -1):
        src = parent / f"portfolio_data.bak{i}.json"
        dst = parent / f"portfolio_data.bak{i + 1}.json"
        if src.exists():
            try:
                src.replace(dst)
            except Exception:
                pass
    # Move current file → .bak1
    if LOCAL_PORTFOLIO_FILE.exists():
        try:
            LOCAL_PORTFOLIO_FILE.replace(parent / "portfolio_data.bak1.json")
        except Exception:
            pass
    # Write to .tmp then rename (atomic on Windows + POSIX)
    tmp = LOCAL_PORTFOLIO_FILE.with_suffix(".ptmp")
    try:
        tmp.write_text(
            json.dumps(data, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        tmp.replace(LOCAL_PORTFOLIO_FILE)
    except Exception:
        # If rename fails, at least try a direct write
        try:
            LOCAL_PORTFOLIO_FILE.write_text(
                json.dumps(data, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
        finally:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass


def _read_portfolio_file_raw() -> dict:
    """Read LOCAL_PORTFOLIO_FILE; on parse error, try backups in order.
    Returns the raw dict or an empty skeleton dict.
    """
    candidates = [LOCAL_PORTFOLIO_FILE] + [
        LOCAL_PORTFOLIO_FILE.parent / f"portfolio_data.bak{i}.json"
        for i in range(1, 4)
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return raw
        except Exception:
            continue
    return {"_meta": {}, "rows": []}


def get_portfolio_file_mtime() -> float:
    """Return the last-modified timestamp of LOCAL_PORTFOLIO_FILE (0.0 if missing)."""
    try:
        return LOCAL_PORTFOLIO_FILE.stat().st_mtime if LOCAL_PORTFOLIO_FILE.exists() else 0.0
    except Exception:
        return 0.0


NETWORK_TIMEOUT_SECONDS = 20
NETWORK_MAX_RETRIES = 3
NETWORK_RETRY_BACKOFF_SECONDS = 1.4
APPS_SCRIPT_COOLDOWN_SECONDS = 300
_APPS_SCRIPT_RETRY_AFTER_TS = 0.0

LANG_EN = "English"
LANG_HE = "עברית"
DEFAULT_LANGUAGE = LANG_HE

# Visible build stamp so we can confirm exactly which version a device (esp. the
# installed PWA) is actually running. Bump on every push that should reach the phone.
APP_BUILD = "2026-06-22 · r27"
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
    # Compact English labels — fit Streamlit's default column widths so
    # the Crypto Concentration table headers don't get truncated mid-word.
    "Direct_Qty": {LANG_EN: "Direct Qty", LANG_HE: "אחזקה ישירה (כמות)"},
    "Direct_ILS": {LANG_EN: "Direct (ILS)", LANG_HE: "אחזקה ישירה (₪)"},
    "ETF_Qty": {LANG_EN: "ETF Qty", LANG_HE: "דרך קרן סל (יחידות)"},
    "ETF_ILS": {LANG_EN: "ETF (ILS)", LANG_HE: "דרך קרן סל (₪)"},
    "Total_Exposure_ILS": {LANG_EN: "Total (ILS)", LANG_HE: "סה\"כ חשיפה (₪)"},
    "Estimated_Coin_Qty": {LANG_EN: "Est. coin qty (incl. ETF)", LANG_HE: "כמות מטבע מוערכת (כולל קרן סל)"},
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
    "Cost_ILS": {LANG_EN: "Cost ILS", LANG_HE: "עלות ILS"},
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
    "Category": {
        "Winner": {LANG_EN: "Winner", LANG_HE: "מנצח"},
        "Loser": {LANG_EN: "Loser", LANG_HE: "מפסיד"},
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
            title_font_color="#f1f5f9",   # keep the chart title readable in dark mode
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
            title_font=dict(size=14, color="#f1f5f9" if is_dark else "#0f172a"),
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
    # An explicit Light/Dark choice always wins. "System" follows the DEVICE: config.toml
    # has no forced base, so Streamlit paints the native shell from the browser's
    # prefers-color-scheme, and st.context.theme.type reports the resolved device theme —
    # so the app's CSS overlay matches the device (and the canvas st.dataframe grid).
    # Both fixed: the old all-white sidebar was the data-pp-collapsed race (r17) + a
    # forced #ffffff, not light mode itself, and every colour below is now is_dark-aware.
    import os as _os
    if _os.environ.get("PP_DESIGN_V2"):
        return THEME_DARK
    mode = _normalize_theme_mode(theme_mode)
    if mode == THEME_LIGHT:
        return THEME_LIGHT
    if mode == THEME_DARK:
        return THEME_DARK
    # system → follow the device (browser prefers-color-scheme). st.context.theme.type
    # is "light"/"dark"; fall back to the (possibly unset) config base, then to light.
    detected = ""
    try:
        detected = (_clean(getattr(getattr(st.context, "theme", None), "type", "")) or "").lower()
    except Exception:
        detected = ""
    if detected not in (THEME_LIGHT, THEME_DARK):
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
    sidebar_bg = "#1E1E1E" if is_dark else "#f3f5fa"  # theme-aware: dark drawer in dark mode, soft light (not pure white) in light mode
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
    badge_color = "#c8d6e5" if is_dark else "#94a3b8"   # help-badge (?) tint – lighter in dark mode
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
        color: {badge_color};
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
        background: {metric_bg} !important;
        color: {metric_text} !important;
        border: 1px solid {metric_border} !important;
        border-radius: 12px;
        box-shadow: 0 14px 32px -8px rgba(0,0,0,0.32);
        unicode-bidi: plaintext;
        white-space: normal;
        word-wrap: break-word;
        text-align: start;
        animation: pp-help-fade 120ms ease-out;
    }}
    /* Ensure all inline elements inside the popup inherit the explicit color,
       not some cascaded light/dark colour from a parent Streamlit container. */
    details.pp-help > .pp-help-body *,
    details.pp-help > .pp-help-body p,
    details.pp-help > .pp-help-body span,
    details.pp-help > .pp-help-body li {{
        color: {metric_text} !important;
    }}
    @keyframes pp-help-fade {{
        from {{ opacity: 0; transform: translateY(-4px); }}
        to   {{ opacity: 1; transform: translateY(0); }}
    }}
    /* Mobile: keep popup INLINE near the badge */
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
    /* ── Client-side dark-mode safety net: if the server-side is_dark
       detection is wrong (e.g. user uses Streamlit native dark theme
       via hamburger menu), CSS prefers-color-scheme still makes the
       popup readable.                                                  ── */
    @media (prefers-color-scheme: dark) {{
        details.pp-help > .pp-help-body {{
            background: #1e293b !important;
            color: #f1f5f9 !important;
            border-color: #334155 !important;
        }}
        details.pp-help > .pp-help-body *,
        details.pp-help > .pp-help-body p,
        details.pp-help > .pp-help-body span,
        details.pp-help > .pp-help-body li {{
            color: #f1f5f9 !important;
        }}
        [data-baseweb="tooltip"],
        [data-baseweb="tooltip"] > div {{
            background-color: #1e293b !important;
            color: #f1f5f9 !important;
            border: 1px solid #334155 !important;
            border-radius: 10px !important;
        }}
        [data-baseweb="tooltip"] p,
        [data-baseweb="tooltip"] span,
        [data-baseweb="tooltip"] div,
        [data-baseweb="tooltip"] [data-testid="stMarkdownContainer"],
        [data-baseweb="tooltip"] [data-testid="stMarkdownContainer"] p,
        [data-testid="stTooltipContent"],
        [data-testid="stTooltipContent"] p {{
            color: #f1f5f9 !important;
            background-color: transparent !important;
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
        align-items: center !important;   /* badge optically centered under the title */
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
    /* NATIVE SIDEBAR: all custom collapsed/expanded overrides removed. The data-pp-collapsed
       machinery + width/transform/visibility/pointer-events hacks fought Streamlit's native
       mobile open/close and scroll-lock and were the root of the menu-won't-reopen and
       broken-vertical-scroll bugs on Android. Streamlit handles the sidebar open/close, width,
       slide animation, backdrop and scroll-lock natively. (android sidebar: stop fighting Streamlit) */
    /* Cosmetic ONLY: when collapsed, make the sidebar fully transparent so no white sliver/strip
       shows at the edge. Background/border/shadow only — NO width/transform/visibility/pointer-events
       (those broke reopen + scroll). Safe because it never changes layout or interactivity. */
    section[data-testid="stSidebar"][aria-expanded="false"],
    section[data-testid="stSidebar"][aria-expanded="false"] > div,
    section[data-testid="stSidebar"][aria-expanded="false"] [data-testid="stSidebarContent"] {{
        background: transparent !important;
        background-color: transparent !important;
        border: none !important;
        box-shadow: none !important;
    }}
    [data-testid="stSidebar"] .nav,
    [data-testid="stSidebar"] .nav-item,
    [data-testid="stSidebar"] .nav-link {{
        direction: ltr !important;
        text-align: left !important;
    }}
    [data-testid="stExpandSidebarButton"] button svg,
    [data-testid="stExpandSidebarButton"] button svg,
    button[aria-label*="sidebar"] svg {{display: none !important;}}
    [data-testid="stExpandSidebarButton"] button::before,
    [data-testid="stExpandSidebarButton"] button::before,
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
        /* Hide Deploy button + ALL Streamlit/GitHub branding on mobile.
           Covers: status widget, "Hosted with Streamlit" badge, "view source"
           on GitHub link, viewer badge, Made-by-Streamlit footer, any
           anchor pointing to streamlit.io / share.streamlit.io / github.com.
           Belt-and-braces: covers older + newer Streamlit testid names. */
        [data-testid="stAppDeployButton"],
        [data-testid="stStatusWidget"],
        [data-testid="stFooter"],
        [data-testid="stAppCreator"],
        [data-testid="viewerBadge_container__r5tak"],
        [class*="viewerBadge"],
        [class*="hostedWithLove"],
        [class*="hostedwithlove"],
        [class*="HostedWithStreamlit"],
        [data-testid="manage-app-button"],
        a[href*="streamlit.io"],
        a[href*="share.streamlit.io"],
        a[href*="streamlit.app"],
        a[href*="github.com"][title*="View" i],
        a[href*="github.com"][aria-label*="GitHub" i],
        a[href*="github.com"][aria-label*="source" i],
        a[aria-label*="streamlit" i],
        a[aria-label*="Streamlit" i],
        footer:not(.app-footer-keep) {{
            display: none !important;
            visibility: hidden !important;
            width: 0 !important;
            height: 0 !important;
            opacity: 0 !important;
            overflow: hidden !important;
            pointer-events: none !important;
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
        /* NATIVE: no custom collapsed/expanded width overrides on mobile — Streamlit's
           native sidebar open/close + scroll-lock handle it. (android sidebar fix) */
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
        /* Collapsed cosmetics ONLY — rely on Streamlit's NATIVE off-screen collapse.
           CRITICAL FIX (r17): the old rule keyed the content's `display:none` on a
           JS-stamped `data-pp-collapsed` hook. On cold-start that hook raced AHEAD of
           Streamlit's real `aria-expanded` and got stuck at "true" while the drawer was
           visually OPEN (aria-expanded="true") → the content was display:none'd → a
           dark, EMPTY sidebar until a refresh. PROVEN with _diag_emptysidebar.py
           (aria=true + pp-collapsed=true → content display:none, width:0).
           Now: visibility depends ONLY on Streamlit's reliable aria-expanded, and we
           NEVER display:none / width:0 the content (no way to hide it while open). */
        [data-testid="stSidebar"][aria-expanded="false"] > div:first-child,
        [data-testid="stSidebar"][aria-expanded="false"] [data-testid="stSidebarContent"] {{
            border: 0 !important;
            background: transparent !important;
            background-color: transparent !important;
            box-shadow: none !important;
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
            border-bottom: 1px solid {df_line} !important;
            border-radius: 0 !important;
            padding: 15px 8px !important;
            background: transparent !important;
            box-shadow: none !important;
            color: {metric_text} !important;
            transition: none !important;
            animation: none !important;
        }}
        [data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"] p,
        [data-testid="stSidebar"] [data-testid="stRadio"] [data-baseweb="radio"] p {{
            text-align: left !important;
            font-weight: 500 !important;
            color: {metric_text} !important;
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
        [data-testid="stExpandSidebarButton"],
        [data-testid="stExpandSidebarButton"],
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
        [data-testid="stExpandSidebarButton"] button,
        [data-testid="stExpandSidebarButton"] button,
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
        [data-testid="stExpandSidebarButton"] button,
        [data-testid="stExpandSidebarButton"] button,
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
            left: 3.2rem !important;
            right: 3.2rem !important;
            transform: none !important;
            width: auto !important;
            max-width: none !important;
            z-index: 100001 !important;
            margin: 0 !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child) {{
            background: {nav_bg} !important;
            border: 1px solid {nav_border} !important;
            border-radius: 12px !important;
            padding: 2px !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-testid="stWidgetLabel"] {{
            display: none !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [role="radiogroup"] {{
            display: flex !important;
            flex-direction: row !important;
            flex-wrap: nowrap !important;
            width: 100% !important;
            margin: 0 !important;
            padding: 0 !important;
            gap: 0px !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"] {{
            flex: 1 1 0% !important;
            min-width: 0 !important;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
            padding: 6px 2px !important;
            margin: 0 !important;
            border: none !important;
            border-radius: 9px !important;
            background: transparent !important;
            cursor: pointer !important;
            -webkit-tap-highlight-color: transparent !important;
            transition: background 0.15s ease !important;
            /* Kill the ~300ms mobile tap delay so the FIRST tap switches page. */
            touch-action: manipulation !important;
        }}
        /* Make the whole segment (incl. its label) one reliable tap target. */
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"] * {{
            touch-action: manipulation !important;
            pointer-events: auto !important;
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
            font-size: 11.5px !important;
            font-weight: 600 !important;
            color: {nav_inactive} !important;
            line-height: 1.2 !important;
            text-align: center !important;
            margin: 0 !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
            max-width: 100% !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"]:has(input:checked) {{
            background: #4f46e5 !important;
        }}
        /* Active pill keeps the SAME weight as inactive (#1) so it doesn't grow
           and clip into the neighbour pill — only colour changes. */
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"]:has(input:checked) p {{
            color: #ffffff !important;
            font-weight: 600 !important;
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

    /* Sidebar customizations: force the Streamlit sidebar to stay on the LEFT
       even in Hebrew (RTL) mode. (Previously this was wrapped in a stray raw
       `# comment` + nested <style> that closed the outer style block early and
       silently dropped every rule after it.) */
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
    [data-testid="stExpandSidebarButton"] button,
    [data-testid="stExpandSidebarButton"] button,
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
        filter: invert(1) hue-rotate(180deg) !important;  /* true invert preserves hue exactly (green stays green / red stays red); 0.9 distorted the signed Styler colors (audit) */
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
    /* ── Checkbox indicators — ensure checkmark is always visible in dark mode ── */
    [data-baseweb="checkbox"] [role="checkbox"][aria-checked="true"],
    [data-baseweb="checkbox"] div[data-checked="true"] {{
        background-color: #6366f1 !important;
        border-color: #6366f1 !important;
    }}
    [data-baseweb="checkbox"] [role="checkbox"][aria-checked="true"] svg,
    [data-baseweb="checkbox"] div[data-checked="true"] svg {{
        fill: #ffffff !important;
        color: #ffffff !important;
    }}
    [data-baseweb="checkbox"] [role="checkbox"] {{
        border-color: {dark_muted} !important;
    }}
    [data-testid="stSidebar"] [data-baseweb="checkbox"] span,
    [data-testid="stSidebar"] [data-baseweb="checkbox"] p {{
        color: {dark_text} !important;
    }}
    /* ── Help-badge: slightly brighter in dark mode for better contrast ── */
    details.pp-help > summary.pp-help-summary {{
        color: {badge_color} !important;
    }}
    /* ── Help popup: explicit dark colours so the panel is always readable ── */
    details.pp-help > .pp-help-body,
    details.pp-help[open] > .pp-help-body {{
        background: #1e293b !important;
        color: #f1f5f9 !important;
        border-color: #334155 !important;
    }}
    details.pp-help > .pp-help-body *,
    details.pp-help > .pp-help-body p,
    details.pp-help > .pp-help-body span,
    details.pp-help > .pp-help-body li {{
        color: #f1f5f9 !important;
    }}
    /* ── Native Streamlit help=… tooltip (st.metric, st.selectbox, etc.) ── */
    [data-baseweb="tooltip"],
    [data-baseweb="tooltip"] > div {{
        background-color: #1e293b !important;
        color: {dark_text} !important;
        border: 1px solid {dark_border} !important;
        border-radius: 10px !important;
    }}
    [data-baseweb="tooltip"] p,
    [data-baseweb="tooltip"] span,
    [data-baseweb="tooltip"] div,
    [data-baseweb="tooltip"] li {{
        color: {dark_text} !important;
        background-color: transparent !important;
    }}
    [data-baseweb="tooltip"] [data-testid="stMarkdownContainer"],
    [data-baseweb="tooltip"] [data-testid="stMarkdownContainer"] p,
    [data-baseweb="tooltip"] [data-testid="stMarkdownContainer"] span,
    [data-testid="stTooltipContent"],
    [data-testid="stTooltipContent"] p,
    [data-testid="stTooltipContent"] span {{
        color: {dark_text} !important;
        background-color: transparent !important;
    }}
    /* ══════════════ END DARK MODE ══════════════ */
    </style>
    """

    st.markdown(css, unsafe_allow_html=True)
    # Design selector: set env PP_DESIGN_V2=1 to use the from-scratch "Aurora"
    # theme (shipped as a parallel desktop build). Default = the 2026 overlay.
    import os as _os
    if _os.environ.get("PP_DESIGN_V2"):
        _inject_design_v2(is_dark)
    else:
        _inject_design_overlay(is_dark)


def _inject_design_v2(is_dark: bool) -> None:
    """'AURORA' — a from-scratch premium design, independent of the v1 overlay.
    A forced dark, glassmorphic 'aurora terminal' aesthetic: near-black aurora
    background, frosted glass cards with luminous hairline borders, neon mint/
    cyan accents, tabular monospace numerics, and tasteful motion. Uses high-
    specificity selectors + !important so it wins over the base CSS AND the
    per-page accent header regardless of injection order. Marker: PP-DESIGN-V2."""
    A = "#5eead4"   # primary accent — mint/teal
    A2 = "#7c93ff"  # secondary accent — periwinkle
    A3 = "#f0abfc"  # tertiary — orchid (for gradients)
    ink = "#e8edf7"      # primary text
    sub = "#9aa7bd"      # muted text
    bg0 = "#070b14"      # base background
    glass = "rgba(20,28,46,.55)"
    glass2 = "rgba(28,38,62,.42)"
    hair = "rgba(140,160,200,.16)"   # hairline border
    hair2 = "rgba(140,160,200,.10)"
    glow = "rgba(94,234,212,.28)"
    st.markdown(f"""
    <style id="pp-design-v2">
    /* ===================== PP-DESIGN-V2 ("AURORA") ===================== */
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@500;600;700&display=swap');
    :root {{ --v2a:{A}; --v2a2:{A2}; --v2a3:{A3}; --v2-ease:cubic-bezier(.16,.84,.34,1); }}

    @keyframes v2Up   {{ from {{ opacity:0; transform: translateY(18px); }} to {{ opacity:1; transform:none; }} }}
    @keyframes v2In   {{ from {{ opacity:0; }} to {{ opacity:1; }} }}
    @keyframes v2Pop  {{ from {{ opacity:0; transform: translateY(14px) scale(.97); }} to {{ opacity:1; transform:none; }} }}
    @keyframes v2Aurora {{ 0% {{ background-position: 0% 50%; }} 50% {{ background-position: 100% 50%; }} 100% {{ background-position: 0% 50%; }} }}
    @keyframes v2Sheen {{ 0% {{ transform: translateX(-120%) rotate(10deg); opacity:0; }} 12% {{ opacity:.5; }} 70% {{ opacity:.15; }} 100% {{ transform: translateX(240%) rotate(10deg); opacity:0; }} }}
    @keyframes v2Glow {{ 0%,100% {{ box-shadow: 0 0 0 1px {hair}, 0 18px 50px -26px rgba(0,0,0,.85); }} 50% {{ box-shadow: 0 0 0 1px {glow}, 0 18px 54px -22px rgba(0,0,0,.85); }} }}

    /* ---- Global typography + AURORA background ---- */
    html, body,
    [data-testid="stAppViewContainer"] *:not([data-testid="stIconMaterial"]):not(.material-icons):not([class*="material-symbols"]) {{
        font-family: 'Space Grotesk', system-ui, -apple-system, 'Segoe UI', sans-serif !important;
        -webkit-font-smoothing: antialiased;
    }}
    /* Keep Material icon ligatures rendering as ICONS (not raw text) */
    [data-testid="stIconMaterial"], .material-icons, [class*="material-symbols"] {{
        font-family: 'Material Symbols Rounded','Material Symbols Outlined','Material Symbols Sharp','Material Icons' !important;
    }}
    [data-testid="stAppViewContainer"] > .main, section.main, [data-testid="stAppViewContainer"] {{
        background:
            radial-gradient(1200px 700px at 82% -8%, rgba(124,147,255,.20), rgba(7,11,20,0) 55%),
            radial-gradient(1000px 680px at 8% 6%, rgba(94,234,212,.16), rgba(7,11,20,0) 52%),
            radial-gradient(900px 700px at 50% 120%, rgba(240,171,252,.12), rgba(7,11,20,0) 55%),
            {bg0} !important;
        background-attachment: fixed !important;
        color: {ink} !important;
    }}
    .main p, .main span, .main label, .main li, [data-testid="stMarkdownContainer"] {{ color: {ink}; }}
    .main h1, .main h2, .main h3, .main h4 {{ color: {ink} !important; letter-spacing: -.02em !important; font-weight: 700 !important; }}

    /* PAGE TRANSITION — opacity entrance removed: it replayed on every Streamlit
       rerun (navigation + 20s fragment updates), flashing the content pale/invisible. */
    section.main .block-container {{ animation: none !important; }}
    [data-baseweb="tab-panel"] {{ animation: none !important; }}

    /* Mobile: remove the ~300ms tap delay so the FIRST tap on any control registers. */
    .stButton > button, .stDownloadButton > button, [data-testid="stFormSubmitButton"] button,
    [role="tab"], [data-baseweb="radio"], [role="radio"], a, [role="button"], button {{
        touch-action: manipulation !important;
    }}

    /* ---- HERO header — animated aurora glass (beats pp-page-accent) ---- */
    html body .app-header-wrap {{
        background:
            linear-gradient(120deg, rgba(94,234,212,.16), rgba(124,147,255,.16) 45%, rgba(240,171,252,.16)) ,
            {glass} !important;
        background-size: 220% 220%, auto !important;
        animation: v2Aurora 14s ease infinite !important;
        border: 1px solid {hair} !important;
        border-top: 1px solid rgba(94,234,212,.4) !important;
        border-radius: 0 0 26px 26px !important;
        box-shadow: 0 24px 60px -28px rgba(0,0,0,.9), 0 1px 0 0 rgba(255,255,255,.06) inset !important;
        backdrop-filter: blur(18px) saturate(1.3) !important;
        -webkit-backdrop-filter: blur(18px) saturate(1.3) !important;
        padding: 1rem 1.4rem .95rem 1.4rem !important;
        overflow: hidden !important;
    }}
    html body .app-header-wrap::before {{
        content:""; position:absolute; top:0; left:0; height:100%; width:46%;
        background: linear-gradient(90deg, rgba(255,255,255,0), rgba(255,255,255,.16) 50%, rgba(255,255,255,0));
        transform: translateX(-120%) rotate(10deg); pointer-events:none;
        animation: v2Sheen 8s ease-in-out 1.5s infinite;
    }}
    html body .app-main-title {{
        background: linear-gradient(100deg, {A} 0%, {A2} 52%, {A3} 100%) !important;
        -webkit-background-clip: text !important; background-clip: text !important;
        -webkit-text-fill-color: transparent !important; color: transparent !important;
        font-weight: 700 !important; letter-spacing: -.025em !important;
        filter: drop-shadow(0 2px 16px rgba(94,234,212,.25)) !important;
    }}
    html body .app-sub-title {{ color: {A} !important; font-weight: 600 !important;
        letter-spacing: .14em !important; text-transform: uppercase !important; opacity: .9 !important; }}
    html body .app-logo-badge {{
        background: rgba(94,234,212,.12) !important; border: 1px solid rgba(94,234,212,.32) !important;
        color: {A} !important; backdrop-filter: blur(6px) !important; font-weight: 600 !important;
        letter-spacing: .02em !important;
    }}
    html body .app-logo-icon {{
        background: rgba(255,255,255,.06) !important; border: 1px solid {hair} !important;
        border-radius: 16px !important; box-shadow: 0 0 24px -6px {glow} !important;
    }}

    /* ---- Metric tiles — frosted glass, neon numerics, glow on hover ---- */
    html body [data-testid="stMetric"] {{
        background: {glass} !important;
        border: 1px solid {hair} !important; border-radius: 20px !important;
        padding: 1.2rem 1.3rem !important;
        box-shadow: 0 18px 50px -26px rgba(0,0,0,.85) !important;
        backdrop-filter: blur(16px) saturate(1.25) !important;
        -webkit-backdrop-filter: blur(16px) saturate(1.25) !important;
        position: relative; overflow: hidden;
        transition: transform .25s var(--v2-ease), box-shadow .25s var(--v2-ease), border-color .25s ease !important;
        animation: v2Pop .55s var(--v2-ease) both;
    }}
    html body [data-testid="stMetric"]::before {{
        content:""; position:absolute; top:0; left:0; right:0; height:2px;
        background: linear-gradient(90deg, {A}, {A2} 55%, {A3}); opacity:.8;
    }}
    html body [data-testid="stMetric"]::after {{
        content:""; position:absolute; top:-45%; inset-inline-end:-30%;
        width:180px; height:180px; border-radius:50%;
        background: radial-gradient(circle, {glow} 0%, rgba(0,0,0,0) 70%); opacity:.6; pointer-events:none;
    }}
    html body [data-testid="stMetric"]:hover {{
        transform: translateY(-5px) !important;
        border-color: rgba(94,234,212,.5) !important;
        box-shadow: 0 26px 60px -24px rgba(0,0,0,.9), 0 0 30px -8px {glow} !important;
    }}
    [data-testid="stHorizontalBlock"] > div:nth-child(1) [data-testid="stMetric"] {{ animation-delay:.03s; }}
    [data-testid="stHorizontalBlock"] > div:nth-child(2) [data-testid="stMetric"] {{ animation-delay:.11s; }}
    [data-testid="stHorizontalBlock"] > div:nth-child(3) [data-testid="stMetric"] {{ animation-delay:.19s; }}
    [data-testid="stHorizontalBlock"] > div:nth-child(4) [data-testid="stMetric"] {{ animation-delay:.27s; }}
    html body [data-testid="stMetricValue"] {{
        font-family: 'JetBrains Mono', ui-monospace, monospace !important;
        font-weight: 700 !important; color: {ink} !important; letter-spacing: -.02em !important;
        font-size: 1.95rem !important; line-height: 1.08 !important;
        text-shadow: 0 0 24px rgba(94,234,212,.14) !important;
    }}
    html body [data-testid="stMetricLabel"] {{
        color: {sub} !important; font-weight: 600 !important;
        text-transform: uppercase !important; letter-spacing: .1em !important; font-size: .72rem !important;
    }}
    html body [data-testid="stMetricDelta"] {{
        font-family: 'JetBrains Mono', monospace !important;
        border-radius: 999px !important; padding: 2px 10px !important;
        background: rgba(94,234,212,.12) !important; width: fit-content; font-weight: 700 !important;
    }}

    /* ---- Sidebar — deep glass rail ---- */
    html body [data-testid="stSidebar"] {{
        background: linear-gradient(180deg, rgba(12,18,32,.92), rgba(7,11,20,.96)) !important;
        border-inline-end: 1px solid {hair} !important;
        backdrop-filter: blur(14px) !important;
    }}
    html body [data-testid="stSidebar"] * {{ color: {ink}; }}
    html body [data-testid="stSidebar"] .nav-link {{
        border-radius: 12px !important; margin: 3px 0 !important; color: {sub} !important;
        transition: background .18s ease, transform .14s var(--v2-ease), color .18s ease !important;
    }}
    html body [data-testid="stSidebar"] .nav-link:hover {{
        background: rgba(94,234,212,.10) !important; color: {ink} !important; transform: translateY(-1px) !important;
    }}
    html body [data-testid="stSidebar"] .nav-link-selected, html body [data-testid="stSidebar"] .nav-link.active {{
        background: linear-gradient(135deg, rgba(94,234,212,.22), rgba(124,147,255,.22)) !important;
        color: {ink} !important; box-shadow: inset 3px 0 0 0 {A} !important;
    }}

    /* ---- Buttons — neon gradient + shine ---- */
    html body .stButton > button, html body .stDownloadButton > button, html body [data-testid="stFormSubmitButton"] button {{
        border-radius: 13px !important; font-weight: 650 !important; color: {ink} !important;
        background: {glass2} !important; border: 1px solid {hair} !important;
        position: relative; overflow: hidden;
        transition: transform .16s var(--v2-ease), box-shadow .18s ease, border-color .18s ease !important;
    }}
    html body .stButton > button:hover, html body .stDownloadButton > button:hover {{
        transform: translateY(-2px) !important; border-color: rgba(94,234,212,.5) !important;
        box-shadow: 0 14px 30px -14px {glow} !important;
    }}
    html body .stButton > button[kind="primary"], html body [data-testid="stFormSubmitButton"] button {{
        background: linear-gradient(135deg, {A}, {A2}) !important; border: none !important;
        color: #04121a !important; font-weight: 700 !important;
        box-shadow: 0 12px 30px -12px {glow} !important;
    }}

    /* ---- Tabs — glass pills ---- */
    html body [data-baseweb="tab-list"] {{ gap: .3rem !important; background: transparent !important; border: none !important; }}
    html body [data-baseweb="tab"] {{
        border-radius: 12px !important; font-weight: 600 !important; color: {sub} !important;
        background: rgba(140,160,200,.06) !important; border: 1px solid {hair2} !important;
        transition: all .16s ease !important;
    }}
    html body [data-baseweb="tab"][aria-selected="true"] {{
        background: linear-gradient(135deg, rgba(94,234,212,.22), rgba(124,147,255,.22)) !important;
        color: {ink} !important; border-color: rgba(94,234,212,.4) !important;
    }}

    /* ---- Cards: expanders / dataframes / charts ---- */
    html body [data-testid="stExpander"] {{
        background: {glass} !important; border: 1px solid {hair} !important; border-radius: 18px !important;
        box-shadow: 0 18px 50px -28px rgba(0,0,0,.85) !important; overflow: hidden;
        backdrop-filter: blur(14px) !important; animation: v2Up .5s var(--v2-ease) both;
    }}
    html body [data-testid="stExpander"]:hover {{ border-color: rgba(94,234,212,.34) !important; }}
    html body [data-testid="stDataFrame"], html body [data-testid="stTable"] {{
        border-radius: 18px !important; overflow: hidden !important; border: 1px solid {hair} !important;
        box-shadow: 0 18px 50px -28px rgba(0,0,0,.85) !important; animation: v2Up .5s var(--v2-ease) both;
    }}
    html body [data-testid="stPlotlyChart"] {{
        border-radius: 20px !important; overflow: hidden; border: 1px solid {hair} !important;
        background: {glass} !important; box-shadow: 0 18px 54px -28px rgba(0,0,0,.85) !important;
        backdrop-filter: blur(8px) !important; animation: v2Up .55s var(--v2-ease) both;
    }}

    /* ---- Inputs ---- */
    html body .stNumberInput input, html body .stTextInput input, html body .stTextArea textarea {{
        background: rgba(10,16,28,.6) !important; color: {ink} !important;
        border: 1px solid {hair} !important; border-radius: 12px !important;
        transition: box-shadow .16s ease, border-color .16s ease !important;
    }}
    html body .stNumberInput input:focus, html body .stTextInput input:focus, html body .stTextArea textarea:focus {{
        border-color: {A} !important; box-shadow: 0 0 0 3px rgba(94,234,212,.18) !important;
    }}

    /* scrollbar */
    ::-webkit-scrollbar-thumb {{ background: rgba(94,234,212,.4) !important; }}
    ::-webkit-scrollbar-thumb:hover {{ background: rgba(94,234,212,.7) !important; }}

    @media (prefers-reduced-motion: reduce) {{ *, *::before, *::after {{ animation: none !important; }} }}

    /* ---- Mobile ---- */
    @media (max-width: 600px) {{
        html body [data-testid="stMetric"] {{ padding: .95rem 1rem !important; border-radius: 17px !important; height:100% !important; }}
        html body [data-testid="stMetricValue"] {{ font-size: 1.55rem !important; }}
        html body [data-testid="stMetricLabel"] {{ font-size: .64rem !important; }}
        html body .app-header-wrap {{ padding: .65rem .85rem !important; border-radius: 0 0 20px 20px !important; }}
        html body .app-main-title {{ font-size: 1.25rem !important; }}
        html body .stButton > button {{ min-height: 46px !important; }}
        html body [data-baseweb="tab"] {{ font-size: .8rem !important; white-space: nowrap !important; }}
        .main .block-container {{ padding-left:.65rem !important; padding-right:.65rem !important; }}
    }}
    /* ============================================================================= */
    </style>
    """, unsafe_allow_html=True)


def _inject_design_overlay(is_dark: bool) -> None:
    """2026 visual refresh — a SELF-CONTAINED design layer added on top of the
    base CSS. Purely cosmetic (color/shadow/radius/typography/spacing); changes
    no layout structure and respects CLAUDE.md (no direction:rtl on layout,
    help badges untouched, no metric-label clipping). Remove this one function
    call to fully revert. Marker: PP-DESIGN-2026."""
    if is_dark:
        page_grad = ("radial-gradient(1100px 560px at 88% -12%, #1b2a4d 0%, rgba(11,17,32,0) 52%),"
                     "radial-gradient(900px 520px at 6% 8%, #16223c 0%, rgba(11,17,32,0) 46%),"
                     "linear-gradient(180deg, #0b1120 0%, #090e1a 100%)")
        card_bg = "linear-gradient(165deg, rgba(33,45,68,.86), rgba(20,28,44,.86))"
        card_brd = "rgba(148,163,184,.16)"
        card_sh = "0 1px 2px rgba(0,0,0,.45), 0 22px 48px -26px rgba(0,0,0,.78)"
        val_col = "#f8fafc"
        lab_col = "#9fb0c7"
        accent_glow = "rgba(99,102,241,.5)"
        sidebar_bg = "linear-gradient(180deg, #0f1729 0%, #0a0f1c 100%)"
    else:
        page_grad = ("radial-gradient(1100px 560px at 88% -12%, #e7eefc 0%, rgba(245,248,253,0) 52%),"
                     "radial-gradient(900px 520px at 4% 6%, #eef1fb 0%, rgba(245,248,253,0) 48%),"
                     "linear-gradient(180deg, #f6f8fd 0%, #eef2fa 100%)")
        card_bg = "linear-gradient(165deg, #ffffff 0%, #f8fbff 100%)"
        card_brd = "rgba(15,23,42,.08)"
        card_sh = "0 1px 2px rgba(15,23,42,.05), 0 18px 40px -22px rgba(15,23,42,.22)"
        val_col = "#0b1220"
        lab_col = "#5b6b85"
        accent_glow = "rgba(99,102,241,.34)"
        sidebar_bg = "linear-gradient(180deg, #f7f9fc 0%, #eef2fb 100%)"  # soft light sidebar in light mode (NOT pure white — avoids the harsh blank look)
    st.markdown(f"""
    <style id="pp-design-2026">
    /* ===================== PP-DESIGN-2026 (premium overlay) ===================== */
    :root {{ --pp-accent:#6366f1; --pp-accent2:#8b5cf6; --pp-radius:18px;
             --pp-ease:cubic-bezier(.22,.61,.36,1); }}

    /* ---- Animation keyframes (GPU-friendly: transform + opacity only) ---- */
    @keyframes ppFadeUp {{ from {{ opacity:0; transform: translateY(16px); }} to {{ opacity:1; transform:none; }} }}
    @keyframes ppFadeIn {{ from {{ opacity:0; }} to {{ opacity:1; }} }}
    @keyframes ppPop    {{ from {{ opacity:0; transform: translateY(12px) scale(.98); }} to {{ opacity:1; transform:none; }} }}
    @keyframes ppSheen  {{ 0% {{ transform: translateX(-150%) rotate(8deg); }} 100% {{ transform: translateX(260%) rotate(8deg); }} }}
    @keyframes ppRise   {{ from {{ opacity:0; transform: translateY(8px); }} to {{ opacity:1; transform:none; }} }}

    /* App background — layered depth instead of flat fill */
    [data-testid="stAppViewContainer"] > .main,
    section.main {{ background: {page_grad} !important; background-attachment: fixed !important; }}

    /* Typography polish */
    html, body, [data-testid="stAppViewContainer"] * {{
        -webkit-font-smoothing: antialiased; text-rendering: optimizeLegibility;
    }}

    /* PAGE TRANSITION — opacity entrance removed (flashed content pale on every rerun) */
    section.main .block-container {{ animation: none !important; }}
    /* Sub-tab panel cross-fade when switching tabs */
    [data-baseweb="tab-panel"] {{ animation: none !important; }}

    /* Metric tiles — elevated glass cards, corner glow, hover lift, staggered entrance */
    [data-testid="stMetric"] {{
        background: {card_bg} !important;
        border: 1px solid {card_brd} !important;
        border-radius: var(--pp-radius) !important;
        padding: 1.15rem 1.25rem !important;
        box-shadow: {card_sh} !important;
        backdrop-filter: blur(10px); -webkit-backdrop-filter: blur(10px);
        transition: transform .22s var(--pp-ease), box-shadow .22s var(--pp-ease), border-color .22s ease !important;
        position: relative; overflow: hidden;
        animation: ppPop .5s var(--pp-ease) both;
    }}
    [data-testid="stMetric"]::after {{
        content:""; position:absolute; top:-40%; inset-inline-end:-30%;
        width:160px; height:160px; border-radius:50%;
        background: radial-gradient(circle, {accent_glow} 0%, rgba(0,0,0,0) 70%);
        opacity:.55; pointer-events:none;
    }}
    [data-testid="stMetric"]:hover {{
        transform: translateY(-4px) !important;
        border-color: var(--pp-accent) !important;
        box-shadow: 0 14px 30px -12px {accent_glow}, {card_sh} !important;
    }}
    [data-testid="stHorizontalBlock"] > div:nth-child(1) [data-testid="stMetric"] {{ animation-delay:.02s; }}
    [data-testid="stHorizontalBlock"] > div:nth-child(2) [data-testid="stMetric"] {{ animation-delay:.09s; }}
    [data-testid="stHorizontalBlock"] > div:nth-child(3) [data-testid="stMetric"] {{ animation-delay:.16s; }}
    [data-testid="stHorizontalBlock"] > div:nth-child(4) [data-testid="stMetric"] {{ animation-delay:.23s; }}
    [data-testid="stHorizontalBlock"] > div:nth-child(5) [data-testid="stMetric"] {{ animation-delay:.30s; }}
    [data-testid="stMetricValue"] {{
        font-weight: 820 !important; letter-spacing: -.025em !important;
        color: {val_col} !important; font-variant-numeric: tabular-nums;
        font-size: 1.9rem !important; line-height: 1.1 !important;
    }}
    [data-testid="stMetricLabel"] {{
        font-weight: 600 !important; letter-spacing: .02em !important;
        color: {lab_col} !important; opacity: .96 !important;
        text-transform: uppercase; font-size: .74rem !important;
    }}

    /* Content surfaces — charts sit in the SAME elevated card as the metric tiles,
       so the dashboard reads as one cohesive system instead of charts floating on
       the page background. Plotly uses a transparent paper bg, so the card shows
       through cleanly in both themes. */
    [data-testid="stPlotlyChart"],
    [data-testid="stVegaLiteChart"] {{
        background: {card_bg} !important;
        border: 1px solid {card_brd} !important;
        border-radius: var(--pp-radius) !important;
        padding: .55rem .5rem .3rem !important;
        box-shadow: {card_sh} !important;
        animation: ppPop .5s var(--pp-ease) both;
    }}
    /* Data tables get the same elevated card so they read as deliberate panels,
       not raw grids floating on the page. overflow:hidden clips the table to the
       card's rounded corners; the inner grid keeps its own cell borders. */
    [data-testid="stDataFrame"] {{
        border: 1px solid {card_brd} !important;
        border-radius: var(--pp-radius) !important;
        box-shadow: {card_sh} !important;
        overflow: hidden !important;
        background: {card_bg} !important;
    }}

    /* Tabs span the FULL width, evenly — each tab gets an equal share of the row so
       the bar is symmetric on phone and desktop (no right-clustered pills). High
       specificity so it wins over the earlier tab rules. */
    html body [data-testid="stTabs"] > div [data-baseweb="tab-list"] {{
        width: 100% !important; display: flex !important; gap: .3rem !important;
        overflow-x: visible !important; mask-image: none !important;
    }}
    html body [data-testid="stTabs"] > div [data-baseweb="tab-list"] [data-baseweb="tab"] {{
        flex: 1 1 0 !important; min-width: 0 !important; justify-content: center !important;
    }}

    /* Buttons — rounded, lift + animated shine */
    .stButton > button, .stDownloadButton > button, [data-testid="stFormSubmitButton"] button {{
        border-radius: 13px !important; font-weight: 680 !important;
        border: 1px solid {card_brd} !important; position: relative; overflow: hidden;
        transition: transform .16s var(--pp-ease), box-shadow .16s ease, filter .16s ease !important;
    }}
    .stButton > button:hover, .stDownloadButton > button:hover {{
        transform: translateY(-2px) !important; filter: brightness(1.05) !important;
        box-shadow: 0 12px 26px -12px {accent_glow} !important;
    }}
    .stButton > button:active, .stDownloadButton > button:active {{ transform: translateY(0) scale(.98) !important; }}
    .stButton > button[kind="primary"], [data-testid="stFormSubmitButton"] button {{
        background: linear-gradient(135deg, var(--pp-accent), var(--pp-accent2)) !important;
        border: none !important; color: #fff !important;
        box-shadow: 0 10px 26px -12px {accent_glow} !important;
    }}
    .stButton > button[kind="primary"]::after, [data-testid="stFormSubmitButton"] button::after {{
        content:""; position:absolute; top:0; left:0; width:40%; height:100%;
        background: linear-gradient(90deg, rgba(255,255,255,0) 0%, rgba(255,255,255,.35) 50%, rgba(255,255,255,0) 100%);
        transform: translateX(-150%) rotate(8deg); pointer-events:none;
    }}
    .stButton > button[kind="primary"]:hover::after, [data-testid="stFormSubmitButton"] button:hover::after {{
        animation: ppSheen .9s ease;
    }}

    /* ---- SIDEBAR — tinted glass panel + polished nav ---- */
    [data-testid="stSidebar"] {{
        background: {sidebar_bg} !important;
        border-inline-end: 1px solid {card_brd} !important;
        box-shadow: 8px 0 30px -22px rgba(15,23,42,.45) !important;
    }}
    [data-testid="stSidebar"] .nav-link {{
        border-radius: 12px !important; margin: 3px 0 !important;
        transition: background .16s ease, transform .14s var(--pp-ease), box-shadow .16s ease !important;
    }}
    [data-testid="stSidebar"] .nav-link:hover {{
        background: rgba(99,102,241,.10) !important;
        transform: translateY(-1px) !important;
    }}
    [data-testid="stSidebar"] [data-testid="stExpander"] {{
        border-radius: 14px !important; border: 1px solid {card_brd} !important;
    }}

    /* Sub-tabs — smoother pills */
    [data-baseweb="tab-list"] {{ gap: .15rem !important; }}
    [data-baseweb="tab"] {{ border-radius: 11px 11px 0 0 !important; font-weight: 640 !important;
        transition: color .15s ease, background .15s ease !important; }}
    /* Active-tab text: #4f46e5 (6.3:1 on white) instead of the brand #6366f1 (4.46,
       just under AA) — fixes the axe color-contrast violation on st.tabs labels. */
    [data-baseweb="tab"][aria-selected="true"] {{ color: #4f46e5 !important; }}

    /* Expanders + dataframes + charts — unified premium card surface (rise-in) */
    [data-testid="stExpander"] {{
        border: 1px solid {card_brd} !important; border-radius: 16px !important;
        box-shadow: {card_sh} !important; overflow: hidden; background: {card_bg} !important;
        animation: ppRise .5s var(--pp-ease) both;
        transition: border-color .18s ease, box-shadow .18s ease !important;
    }}
    [data-testid="stExpander"]:hover {{ box-shadow: 0 16px 34px -18px {accent_glow}, {card_sh} !important; }}
    [data-testid="stDataFrame"], [data-testid="stTable"] {{
        border-radius: 16px !important; overflow: hidden !important;
        border: 1px solid {card_brd} !important; box-shadow: {card_sh} !important;
        animation: ppRise .5s var(--pp-ease) both;
    }}
    [data-testid="stPlotlyChart"] {{
        border-radius: var(--pp-radius); overflow: hidden;
        border: 1px solid {card_brd}; box-shadow: {card_sh}; background: {card_bg};
        animation: ppRise .55s var(--pp-ease) both;
    }}

    /* The header glow + rounded base (full hero look set in pp-page-accent) */
    .app-header-wrap {{
        box-shadow: 0 16px 38px -18px {accent_glow} !important;
        border-radius: 0 0 22px 22px !important;
    }}

    /* Section headings (st.markdown ### …) — leading accent bar gives the page
       a clear vertical rhythm and a premium "sectioned" structure (RTL-aware via
       inset-inline-start, so the bar sits on the right in Hebrew). */
    .main h3 {{
        font-weight: 780 !important; letter-spacing: -.015em !important;
        margin-top: .75rem !important;
        position: relative !important;
        padding-inline-start: .72rem !important;
    }}
    .main h3::before {{
        content: ""; position: absolute; inset-inline-start: 0; top: .2em; bottom: .2em;
        width: 4px; border-radius: 3px;
        background: linear-gradient(180deg, #6366f1, #818cf8) !important;
    }}
    /* KPI delta rendered as a soft pill */
    [data-testid="stMetricDelta"] {{
        display: inline-flex !important; align-items: center; gap: 2px;
        padding: 2px 9px !important; border-radius: 999px !important;
        background: {accent_glow} !important; width: fit-content;
        font-weight: 680 !important;
    }}

    /* Inputs — softer, focus glow */
    .stNumberInput input, .stTextInput input, .stTextArea textarea {{
        border-radius: 11px !important;
        transition: box-shadow .16s ease, border-color .16s ease !important;
    }}
    .stNumberInput input:focus, .stTextInput input:focus, .stTextArea textarea:focus {{
        box-shadow: 0 0 0 3px {accent_glow} !important;
    }}

    /* Respect reduced-motion preference */
    @media (prefers-reduced-motion: reduce) {{
        *, *::before, *::after {{ animation: none !important; }}
    }}

    /* MOBILE refinement — managed as its own tuned layout */
    @media (max-width: 600px) {{
        /* 2-up KPI grid with consistent gaps and equal heights */
        [data-testid="stHorizontalBlock"]:has([data-testid="stMetric"]) {{ gap: .5rem !important; }}
        [data-testid="stMetric"] {{ height: 100% !important; }}
        [data-testid="stMetricValue"] {{ font-size: 1.35rem !important; line-height: 1.15 !important; }}
        [data-testid="stMetricLabel"] {{ font-size: .72rem !important; }}
        /* tighter page gutters so cards get more width */
        .main .block-container {{ padding-left: .6rem !important; padding-right: .6rem !important; }}
        /* section headings a touch smaller on phone */
        .main h3 {{ font-size: 1.05rem !important; }}
        /* charts never overflow the viewport */
        [data-testid="stPlotlyChart"], .js-plotly-plot {{ max-width: 100% !important; }}

        /* Compact the big page header on phones — it ate too much vertical space */
        .app-header-wrap {{ padding: .5rem .7rem !important; border-radius: 0 0 14px 14px !important; }}
        .app-header-wrap h1, .app-header-wrap [data-testid="stHeading"] h1 {{ font-size: 1.15rem !important; }}
        /* Tighter vertical rhythm so more fits above the fold */
        .main .block-container {{ padding-top: .4rem !important; }}
        [data-testid="stVerticalBlock"] {{ gap: .55rem !important; }}
        /* Metric tiles: crisper card with a clearer accent edge on mobile */
        [data-testid="stMetric"] {{
            border-radius: 16px !important;
            box-shadow: 0 1px 2px rgba(0,0,0,.06), 0 10px 22px -16px {accent_glow} !important;
        }}
        /* Sub-tabs scroll as comfortable pills */
        [data-baseweb="tab-list"] {{ gap: .3rem !important; padding-bottom: .15rem !important; }}
        [data-baseweb="tab"] {{
            border-radius: 999px !important; background: rgba(127,127,127,.08) !important;
            font-size: .82rem !important; white-space: nowrap !important;
        }}
        [data-baseweb="tab"][aria-selected="true"] {{
            background: linear-gradient(135deg, var(--pp-accent), var(--pp-accent2)) !important;
            color: #fff !important;
        }}
        /* Inputs/selects bigger touch targets */
        .stNumberInput input, .stTextInput input, [data-baseweb="select"] {{ min-height: 42px !important; }}
        /* Dataframe: comfortable row height, smaller font so columns fit */
        [data-testid="stDataFrame"] {{ font-size: .78rem !important; }}
    }}
    /* ============================================================================= */
    </style>
    """, unsafe_allow_html=True)


def inject_dark_dropdown_fix(is_dark: bool) -> None:
    """Inject dark-mode styles for dropdown popovers AND sidebar into the parent document.

    Streamlit's BaseWeb selectbox dropdown is rendered as a portal at the
    document root level.  CSS injected via st.markdown lives inside the
    Streamlit container and cannot override Styletron's inline styles on
    the portal.  This function uses a tiny JS snippet to plant a <style>
    tag directly in the parent document <head> with high-specificity rules.
    """
    # NO session_state guard: previously this returned early unless is_dark CHANGED, so
    # the dropdown/sidebar theme fix only re-applied on a toggle — which read as "the theme
    # doesn't switch immediately / pieces stay the old colour". The inner JS is idempotent
    # (style-tag id checks + the __pmSidebarDarkTimer guard), so re-emitting every rerun is
    # safe and keeps the resolved theme applied. (theme-immediacy fix)
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
/* ── Native Streamlit help=… tooltip (st.metric, st.selectbox, etc.) ── */
/* BaseWeb renders the tooltip via a portal inside [data-baseweb="layer"]. */
[data-baseweb="tooltip"],
[data-baseweb="tooltip"] > div,
body > div[data-baseweb="layer"] [data-baseweb="tooltip"],
body > div[data-baseweb="layer"] [data-baseweb="tooltip"] > div {{
  background-color: {dark_bg2} !important;
  color: {dark_text} !important;
  border: 1px solid {dark_border} !important;
  border-radius: 10px !important;
}}
[data-baseweb="tooltip"] p,
[data-baseweb="tooltip"] span,
[data-baseweb="tooltip"] li,
[data-baseweb="tooltip"] div,
body > div[data-baseweb="layer"] [data-baseweb="tooltip"] p,
body > div[data-baseweb="layer"] [data-baseweb="tooltip"] span,
body > div[data-baseweb="layer"] [data-baseweb="tooltip"] div {{
  color: {dark_text} !important;
  background-color: transparent !important;
}}
/* Streamlit wraps the tooltip text in stMarkdownContainer */
[data-baseweb="tooltip"] [data-testid="stMarkdownContainer"],
[data-baseweb="tooltip"] [data-testid="stMarkdownContainer"] p,
[data-baseweb="tooltip"] [data-testid="stMarkdownContainer"] span,
body > * [data-testid="stTooltipContent"],
body > * [data-testid="stTooltipContent"] p,
body > * [data-testid="stTooltipContent"] span {{
  color: {dark_text} !important;
  background-color: transparent !important;
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
    # NO session_state guard. This components.html owns the tab-swipe binding, the
    # MutationObservers and the setInterval re-fixers. A `_client_fixes_done` early-return
    # emitted it only on run #1; Streamlit then REMOVED the iframe on the next rerun,
    # killing the observers/timers and freezing the swipe re-bind (same class as the
    # documented st.markdown(<style>) rerun rule). The inner JS is idempotent
    # (__pmSwipeBound / _pmObserverAttached / __pmSbWatch / getElementById guards), so
    # re-emitting every rerun is safe and keeps the swipe + fixes alive. (regression fix)
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
              '[data-testid="stAppDeployButton"]',
              '[data-testid="stStatusWidget"]',
              '[data-testid="manage-app-button"]',
              '[data-testid*="viewerBadge"]',
              '[class*="viewerBadge"]',
              '[class*="hostedWithLove"]',
              '[class*="hostedwithlove"]',
              '[class*="HostedWithStreamlit"]'
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

            // Hide ANY anchor/icon/svg that points to streamlit or github.com
            // and lives at the bottom of the viewport (a typical "viewer badge").
            const brandLinkSel = [
              'a[href*="streamlit.io"]',
              'a[href*="share.streamlit.io"]',
              'a[href*="streamlit.app"]',
              'a[href*="github.com"]',
              'a[aria-label*="streamlit" i]',
              'a[aria-label*="GitHub" i]',
              'a[aria-label*="source" i]',
              'a[title*="streamlit" i]',
              'a[title*="GitHub" i]'
            ].join(',');

            rootDoc.querySelectorAll(brandLinkSel).forEach((link) => {
              // Walk up to a sensible "container" and nuke it
              const holder = link.closest(
                'footer, [data-testid="stFooter"], [data-testid="stAppCreator"], '
                + '[data-testid*="viewerBadge"], [class*="viewerBadge"], '
                + '[class*="hostedWithLove"], [role="contentinfo"]'
              ) || link;
              try {
                holder.style.display = 'none';
                holder.style.visibility = 'hidden';
              } catch (e) {}
            });


            // Final fallback: any FIXED-position element near the viewport
            // bottom edge that contains a streamlit/github link or icon.
            rootDoc.querySelectorAll('div, section, aside, span').forEach((el) => {
              const style = rootWin.getComputedStyle ? rootWin.getComputedStyle(el) : null;
              if (!style) return;
              const isFixed = style.position === 'fixed' || style.position === 'sticky';
              const bottomVal = parseFloat(style.bottom || '9999');
              const nearBottom = bottomVal >= 0 && bottomVal <= 60;
              if (!isFixed || !nearBottom) return;
              if (el.querySelector(brandLinkSel)) {
                el.style.display = 'none';
              }
            });
          }


          function findDashboardTabList() {
            // Try both BaseWeb and modern Streamlit selectors
            const lists = Array.from(rootDoc.querySelectorAll(
              '[data-baseweb="tab-list"], [role="tablist"], [data-testid="stTabs"] > div:first-child'
            ));
            const he = ['סקירה', 'חלוקה', 'דוחות', 'סך הפקדות', 'תנועות'];
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

            // Find the MAIN content tab-list — the page sub-tabs
            // (סקירה/הרכב/דוחות/תנועות), NOT the 3-tab watchlist tabs inside Overview.
            // Score every tab-list: strongly prefer one whose labels match the known
            // page tabs, then prefer more tabs. (Old code grabbed the FIRST list with
            // >=3 tabs, which could be the watchlist => swipe drove the wrong tabs.)
            const PAGE_TAB_WORDS = ['סקירה','הרכב','דוחות','תנועות',
                                    'overview','allocation','holdings','reports','transactions'];
            function getContentTabList() {
              const lists = Array.from(rootDoc.querySelectorAll('[data-baseweb="tab-list"]'));
              let best = null, bestScore = -1;
              for (const list of lists) {
                const tabs = Array.from(list.querySelectorAll('[data-baseweb="tab"]'));
                if (tabs.length < 3) continue;
                const labels = tabs.map(t => (t.innerText || '').trim().toLowerCase());
                const labelHits = PAGE_TAB_WORDS.filter(w =>
                  labels.some(l => l.indexOf(w.toLowerCase()) >= 0)).length;
                const score = labelHits * 100 + tabs.length;   // label match dominates
                if (score > bestScore) { bestScore = score; best = { list, tabs }; }
              }
              return best;
            }

            // Only block elements that OWN a horizontal gesture: form controls and
            // horizontally-scrollable data tables. Charts (plotly donut/pie) and the
            // (height:0) component iframes are NOT blocked — the user naturally swipes
            // over the donut, and blocking it made the swipe feel "dead". (r17 swipe fix)
            const blockedSelector = [
              'input', 'textarea', 'select',
              '[data-testid="stDataFrame"]',
              '[data-testid="stDataEditor"]',
              '[data-no-swipe]'
            ].join(',');

            let startX = 0, startY = 0, startT = 0, locked = false;

            rootDoc.addEventListener('touchstart', (e) => {
              locked = false;
              if (!e.touches || e.touches.length !== 1) return;
              const tgt = e.target;
              if (tgt && tgt.closest && tgt.closest(blockedSelector)) return;
              if (!getContentTabList()) return;
              startX = e.touches[0].clientX;
              startY = e.touches[0].clientY;
              startT = (e.timeStamp || 0);
              locked = true;
            }, { passive: true, capture: true });

            function doSwipe(dx, dy, dt) {
              // Require a clear, reasonably-fast horizontal flick: >40px horizontal,
              // horizontal dominates vertical, and not a slow drag (avoids fighting scroll).
              if (Math.abs(dx) < 40) return;
              if (Math.abs(dx) < Math.abs(dy) * 1.2) return;
              if (dt && dt > 800) return;
              const found = getContentTabList();
              if (!found) return;
              const { tabs } = found;
              const activeIdx = tabs.findIndex(t => t.getAttribute('aria-selected') === 'true');
              if (activeIdx < 0) return;
              // In RTL (Hebrew) the visual tab order is mirrored, but the DOM tab order
              // is NOT — keep index math in DOM order so left/right stay intuitive:
              // swipe left (dx<0) = next tab; swipe right (dx>0) = previous tab.
              const nextIdx = dx < 0 ? activeIdx + 1 : activeIdx - 1;
              if (nextIdx >= 0 && nextIdx < tabs.length) {
                rootWin.__pmActiveTabIdx = nextIdx;           // remember intent (survives reruns)
                tabs[nextIdx].click();
                try { tabs[nextIdx].scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' }); } catch (e) {}
              }
            }

            rootDoc.addEventListener('touchend', (e) => {
              if (!locked) return;
              locked = false;
              if (!e.changedTouches || !e.changedTouches.length) return;
              const dx = e.changedTouches[0].clientX - startX;
              const dy = e.changedTouches[0].clientY - startY;
              const dt = (e.timeStamp || 0) - startT;
              doSwipe(dx, dy, dt);
            }, { passive: true, capture: true });

            // Track MANUAL tab taps too, so the remembered index matches reality.
            rootDoc.addEventListener('click', (e) => {
              try {
                const tab = e.target && e.target.closest && e.target.closest('[data-baseweb="tab"]');
                if (!tab) return;
                const found = getContentTabList();
                if (!found) return;
                const idx = found.tabs.indexOf(tab);
                if (idx >= 0) rootWin.__pmActiveTabIdx = idx;
              } catch (e2) {}
            }, { capture: true });

            // Restore the active sub-tab after a rerun. A 5s sync-watcher (and the
            // websocket reconnect) issue full st.rerun()s, and native st.tabs has NO
            // selected-tab persistence — it remounts at index 0 (Overview), so a swipe
            // or tap visibly "snapped back". This re-clicks the remembered tab once the
            // tab-list re-renders. Guarded: only acts when the current != intended.
            rootWin.__pmRestoreActiveTab = function () {
              try {
                const want = rootWin.__pmActiveTabIdx;
                if (want == null || want <= 0) return;        // 0 / unset → nothing to restore
                const found = getContentTabList();
                if (!found || !found.tabs[want]) return;
                const cur = found.tabs.findIndex(t => t.getAttribute('aria-selected') === 'true');
                if (cur !== want) found.tabs[want].click();
              } catch (e) {}
            };

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
                '/* native sidebar: collapsed/expanded overrides removed — Streamlit handles open/close + scroll-lock */',
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

          // ═══════════════════════════════════════════════════════════
          // ACCESSIBILITY FIXES (axe-core findings)
          //  • Streamlit puts aria-expanded on the sidebar <section>, whose
          //    role doesn't allow it → remove it (aria-allowed-attr, critical).
          //  • Horizontally-scrollable tables aren't keyboard-reachable → make
          //    them focusable + labelled (scrollable-region-focusable, serious).
          //  • Main content lacks a landmark → tag role="main" (region).
          // ═══════════════════════════════════════════════════════════
          // Stamp a STABLE data-pp-collapsed attribute from Streamlit's
          // aria-expanded, then strip aria-expanded (it's invalid on the
          // <section> role → aria-allowed-attr). This keeps axe at 0 AND
          // gives the CSS a reliable hook to fully hide the collapsed
          // sidebar (no leftover sliver/shadow). A dedicated observer below
          // re-syncs on every toggle.
          // REMOVED (r17): the data-pp-collapsed mirror + its MutationObserver. It
          // raced ahead of Streamlit's aria-expanded on cold-start and got stuck
          // "true" while the drawer was open, which the CSS turned into an EMPTY
          // sidebar (content display:none). The CSS now keys purely off Streamlit's
          // native aria-expanded, so this JS hook is both unnecessary and harmful.
          function syncSidebarCollapsed() { /* intentionally a no-op — see note above */ }
          function watchSidebarToggle() { /* intentionally a no-op — see note above */ }

          function fixA11y() {
            try {
              syncSidebarCollapsed();
              watchSidebarToggle();
              rootDoc.querySelectorAll('[data-testid="stTable"] > div,[data-testid="stDataFrame"] div,[data-testid="stDataFrameResizable"]').forEach(function(e){
                if (e.scrollWidth > e.clientWidth + 3 && !e.getAttribute('tabindex')) {
                  e.setAttribute('tabindex','0'); e.setAttribute('role','region');
                  e.setAttribute('aria-label', rootDoc.documentElement.lang === 'he' ? 'טבלה הניתנת לגלילה' : 'Scrollable table');
                }
              });
              var mn = rootDoc.querySelector('[data-testid="stMain"]') || rootDoc.querySelector('section.main') || rootDoc.querySelector('.main');
              if (mn && !mn.getAttribute('role')) mn.setAttribute('role','main');
              var isHe = rootDoc.documentElement.lang === 'he';
              var navEl = rootDoc.querySelector('[data-testid="stRadio"]');
              if (navEl) { var navWrap = navEl.closest('[data-testid="stElementContainer"]') || navEl;
                if (!navWrap.getAttribute('role')) { navWrap.setAttribute('role','navigation'); navWrap.setAttribute('aria-label', isHe ? 'ניווט עמודים' : 'Page navigation'); } }
              var sb = rootDoc.querySelector('section[data-testid="stSidebar"]');
              if (sb && !sb.getAttribute('aria-label')) sb.setAttribute('aria-label', isHe ? 'הגדרות וניווט' : 'Settings and navigation');
              // heading-order: assign aria-level so no level is skipped (the hero
              // is h1 and section titles are h4). aria-level fixes the semantic
              // order for screen readers without changing the visual sizes.
              var prev = 0;
              rootDoc.querySelectorAll('h1,h2,h3,h4,h5,h6').forEach(function(h){
                var lvl = parseInt(h.tagName.charAt(1), 10);
                var cur = prev === 0 ? lvl : Math.min(lvl, prev + 1);
                h.setAttribute('aria-level', String(cur));
                prev = cur;
              });
            } catch(e){}
          }

          // (The top loading bar lives in _pp_inject_loading_bar() — a single, fixed
          // implementation. No duplicate here.)

          function run() {
            removeBranding();
            setupTabSwipe();
            if (rootWin.__pmRestoreActiveTab) rootWin.__pmRestoreActiveTab();  // re-select the active sub-tab after a rerun reset it to Overview
            fixSidebarLeft();
            makeWatchlistExpandUp();
            forceToolbarVisible();
            removeUndefinedText();
            fixA11y();
          }

          run();
          if (!rootWin._pmObserverAttached) {
            // THROTTLED observer: run() does heavy passes (a11y querySelectorAll, a
            // full-body TreeWalker, swipe re-bind). Firing it on EVERY mutation thrashed
            // layout and, over time, starved touch handling so the SWIPE stopped working.
            // Coalesce to at most once per ~120ms. (perf / swipe-stops fix)
            var _runT = null;
            const obs = new MutationObserver(function () {
              if (_runT) return;
              _runT = rootWin.setTimeout(function () { _runT = null; try { run(); } catch (e) {} }, 120);
            });
            obs.observe(rootDoc.body, { childList: true, subtree: true });
            rootWin._pmTimerBranding = rootWin.setInterval(removeBranding, 1200);
            rootWin._pmTimerSidebar = rootWin.setInterval(fixSidebarLeft, 800);
            rootWin._pmTimerToolbar = rootWin.setInterval(forceToolbarVisible, 1500);
            // 200ms full-body TreeWalker was pure waste (the observer already calls it);
            // a 3s safety-net is plenty. (perf fix)
            rootWin._pmTimerUndef = rootWin.setInterval(removeUndefinedText, 3000);
            rootWin.addEventListener('scroll', removeUndefinedText, {passive:true, capture:true});
            rootWin._pmTimerHide = window.setInterval(function () {
              injectHideStyle(document);
              if (rootDoc !== document) injectHideStyle(rootDoc);
            }, 2000);
            rootWin._pmObserverAttached = true;
          }

          // ═══════════════════════════════════════════════════════════
          // MOBILE WEBSOCKET AUTO-RECONNECT
          // Streamlit's WebSocket can drop when:
          //   • the phone screen dims / app goes to background
          //   • Wi-Fi switches between access points
          //   • the browser tab is suspended by the OS
          // When the page re-gains focus (visibilitychange → visible),
          // we check whether Streamlit's connection widget is showing the
          // "disconnected / reconnecting" banner and, if so, force a
          // hard reload so the user sees a fresh app instead of a blank
          // or spinner page.
          // ═══════════════════════════════════════════════════════════
          (function setupMobileReconnect() {
            if (rootWin._pmReconnectBound) return;
            rootWin._pmReconnectBound = true;

            var RECONNECT_TIMEOUT_MS = 8000; // wait this long before reloading
            var _reconnectTimer = null;

            function isStreamlitDisconnected() {
              try {
                var d = rootDoc;
                // ONLY trust Streamlit's real connection indicator. The previous code
                // also scanned ALL stNotification/.stAlert/.stException for the substring
                // "connection"/"disconnect" — a user-facing alert like "Google Sheets
                // connection settings" tripped it and HARD-RELOADED the app (blanking it
                // and resetting the active tab). That broad match is removed.
                var statusEl = d.querySelector('[data-testid="stConnectionStatus"]');
                if (statusEl) {
                  var txt = (statusEl.innerText || '').toLowerCase();
                  if (txt.indexOf('disconnect') >= 0 || txt.indexOf('reconnect') >= 0
                      || txt.indexOf('connecting') >= 0) return true;
                }
              } catch(e) {}
              return false;
            }

            function scheduleReload() {
              if (_reconnectTimer) return; // already scheduled
              _reconnectTimer = rootWin.setTimeout(function() {
                _reconnectTimer = null;
                // Only reload if still disconnected (don't interrupt a successful reconnect)
                if (isStreamlitDisconnected()) {
                  rootWin.location.reload();
                }
              }, RECONNECT_TIMEOUT_MS);
            }

            function cancelReload() {
              if (_reconnectTimer) {
                rootWin.clearTimeout(_reconnectTimer);
                _reconnectTimer = null;
              }
            }

            // When tab becomes visible again (phone unlock / switching back)
            // give Streamlit a short grace period, then reload if disconnected.
            rootDoc.addEventListener('visibilitychange', function() {
              if (rootDoc.visibilityState === 'visible') {
                rootWin.setTimeout(function() {
                  if (isStreamlitDisconnected()) {
                    scheduleReload();
                  }
                }, 2000);
              } else {
                // Tab hidden — cancel any pending reload
                cancelReload();
              }
            });

            // Also intercept the browser "online" event: if Wi-Fi reconnects
            // after a drop, trigger a reload after a short delay.
            rootWin.addEventListener('online', function() {
              rootWin.setTimeout(function() {
                if (isStreamlitDisconnected()) {
                  scheduleReload();
                }
              }, 3000);
            });
          })();

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
    # Yahoo returns "GBp" (lowercase p) for LSE pennies — that is pence (GBX), NOT
    # pounds; detect on the un-uppercased value so GBp→GBP doesn't fold pence into
    # full pounds (100× error). (audit fin-fx) — _clean(value) keeps original case.
    if _clean(value).strip() == "GBp":
        return "GBX"
    if raw in {"GBX", "ILS-AGOROT"}:
        return raw if raw == "GBX" else "ILA"
    return raw


def _infer_display_currency(ticker: str, origin_currency: object) -> str:
    base = _normalize_currency_code(origin_currency)
    # Honour the stored origin currency when present — ILS-origin crypto (e.g. BTC
    # bought on Bit2C in shekels) must display its average buy price in ILS, not USD
    # (otherwise the break-even reference is off by the FX factor and in the wrong
    # symbol). Only default crypto to USD when no origin currency is recorded. (audit #11)
    if base:
        return base
    if ticker in {"BTC", "ETH", "SOL"}:
        return "USD"
    return "ILS"


def _format_currency_value(value: float, currency: str) -> str:
    # Currency symbol always PREFIXES the number (₪5,197 / $12.34), matching
    # the "₪{:,.0f}" format used in the tables — one consistent convention, no
    # "0.00 ₪" suffix outliers.
    cur = _normalize_currency_code(currency)
    if cur == "USD":
        return f"${value:,.2f}"
    if cur == "ILS":
        return f"₪{value:,.2f}"
    if cur:
        return f"{cur} {value:,.2f}"
    return f"{value:,.2f}"


def _mix_he_with_ltr(token: str) -> str:
    # Isolate Latin tokens so bidi layout stays stable inside Hebrew text.
    return f"\u2066{token}\u2069"


def _tradingview_symbol(ticker: object) -> str:
    t = _clean(ticker).upper()
    if not t:
        return ""
    if t in {"BTC", "ETH", "SOL"}:
        return f"BINANCE:{t}USDT"
    # Allow only safe exchange:symbol format (no injection vectors)
    if ":" in t:
        if re.match(r"^[A-Z0-9._-]+:[A-Z0-9._-]+$", t):
            return t
        return ""
    if re.match(r"^[A-Z0-9._-]+$", t):
        return f"NASDAQ:{t}"
    return ""


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
        "symbol": {json.dumps(symbol).replace('</', '<\\/')},
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
        v = float(s)
        # Reject non-finite ('inf'/'Infinity'/'nan' that slip past float()) so a corrupt
        # cell can't poison total_value/total_cost/Yield sums with inf/NaN. (audit bug-st)
        return v if (v == v and v not in (float("inf"), float("-inf"))) else 0.0
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
    if raw.empty:
        return parsed

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
    # Include a status/role discriminator + Sell_Date so an OPEN lot and a separately
    # recorded fully-closed lot with identical buy parameters don't collide to the same
    # Trade_ID (which would let edit/delete-by-id target the wrong row). (audit bug-st)
    role = "SELL" if _is_closed_status(row.get("Status", "")) else "BUY"
    sell_date = _clean(row.get("Sell_Date", ""))
    raw = (
        f"{platform}|{location}|{asset_type}|{ticker}|{purchase_date}|"
        f"{qty:.12f}|{buy_price:.12f}|{cost_origin:.12f}|{currency}|{commission:.12f}|"
        f"{role}|{sell_date}"
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


# Canonical set of status labels that mark a trade as closed/sold. Defined once
# here and referenced everywhere instead of being copy-pasted as a raw literal.
CLOSED_STATUS_VALUES = frozenset({"סגור", "closed", "close", "sold", "נמכר"})


def _is_closed_status(value: object) -> bool:
    return _clean(value).lower() in CLOSED_STATUS_VALUES


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
        if _clean(r.get("Action")) == "SELL" or _is_closed_status(r.get("Status"))
        else ("BUY" if _clean(r.get("Action")) in {"", "BUY"} else _clean(r.get("Action"))),
        axis=1,
    )
    return df


# Crypto spot tickers that yfinance quotes as "<COIN>-USD" (mirrors engine.js CRYPTO_SPOT).
_CRYPTO_SPOT = frozenset({"BTC", "ETH", "SOL", "XRP", "ADA", "DOGE", "AVAX", "DOT", "LINK", "MATIC"})


def _market_symbol(ticker: str) -> str:
    t = _clean(ticker).upper()
    if t in _CRYPTO_SPOT:
        return f"{t}-USD"
    return t


def _yf_price_currency(ticker: str) -> str:
    """Return the quote currency that yfinance uses for this ticker's price.

    BTC/ETH/SOL map to BTC-USD / ETH-USD / SOL-USD → price is always in USD.
    Israeli stocks use a .TA suffix from TASE → Yahoo quotes them in ILA agorot
    (1/100 ILS); _ils_per_native_unit("ILA")=÷100 then yields the true ILS value.
    London .L/.LON symbols quote in GBp pence → currency GBX.
    All other tickers (US stocks, ETFs, crypto pairs) are USD-quoted.

    This is intentionally separate from Origin_Currency: a user can buy BTC on
    Bit2C (Origin_Currency=ILS) but yfinance still returns a USD price, so we
    must always apply the USD→ILS FX rate when computing Current_Value_ILS.
    """
    sym = _market_symbol(ticker).upper()
    if sym.endswith(".TA"):
        return "ILA"
    if sym.endswith(".L") or sym.endswith(".LON"):
        return "GBX"
    return "USD"


@st.cache_data(ttl=900, show_spinner=False)
def _fx_pair_rate(pair: str) -> float:
    """Live ILS-per-unit for a Yahoo FX pair (e.g. GBPILS=X, EURILS=X). Cached."""
    return float(_safe_quote(pair))


def _ils_per_native_unit(quote_currency: object, usd_ils: float) -> float:
    """ILS per ONE unit of the yfinance quote currency — the Python equivalent of
    engine.js toIls() per unit. Mirrors it byte-for-byte: ILS/NIS=1, ILA=÷100,
    GBX=÷100×GBPILS, GBP=×GBPILS, EUR=×EURILS, USD=×USDILS. Unknown → 0.0 so the
    caller keeps the stored value (no silent wrong-rate substitution). (audit fin-fx)"""
    c = _normalize_currency_code(quote_currency)
    if c in {"ILS", ""}:
        return 1.0
    if c == "ILA":
        return 0.01
    if c == "USD":
        return float(usd_ils) if usd_ils and usd_ils > 0 else 0.0
    if c in {"GBX", "GBP"}:
        gbp = _fx_pair_rate("GBPILS=X")
        if not (gbp and gbp > 0):
            return 0.0
        return gbp / 100.0 if c == "GBX" else gbp
    if c == "EUR":
        eur = _fx_pair_rate("EURILS=X")
        return float(eur) if eur and eur > 0 else 0.0
    r = _fx_pair_rate(f"{c}ILS=X")
    return float(r) if r and r > 0 else 0.0


def _ils_per_origin_parent_unit(origin_currency: object, usd_ils: float) -> float:
    """ILS per ONE unit of the asset's PARENT origin currency — used to back out a
    native-currency value from Current_Value_ILS for the origin-yield denominator.
    Cost_Origin is stored in the parent unit (GBP not pence, ILS not agorot), so the
    value must be expressed in the same parent unit: ILS/ILA→1 (parent ILS), USD→USDILS,
    GBX/GBP→GBPILS, EUR→EURILS. Mirrors engine.js _mktNat (parent-unit). (audit fin-fx)"""
    c = _normalize_currency_code(origin_currency)
    if c in {"ILS", "ILA", ""}:
        return 1.0
    if c == "USD":
        return float(usd_ils) if usd_ils and usd_ils > 0 else 0.0
    if c in {"GBX", "GBP"}:
        gbp = _fx_pair_rate("GBPILS=X")
        return float(gbp) if gbp and gbp > 0 else 0.0
    if c == "EUR":
        eur = _fx_pair_rate("EURILS=X")
        return float(eur) if eur and eur > 0 else 0.0
    r = _fx_pair_rate(f"{c}ILS=X")
    return float(r) if r and r > 0 else 0.0


def _value_origin_series(df: pd.DataFrame, usd_ils: float) -> pd.Series:
    """Per-row Current_Value_ILS expressed in the asset's PARENT origin currency, so
    Yield_Origin compares like units against Cost_Origin (parent unit). Replaces the
    binary `c=='USD' else pass-through-ILS` that leaked ILS into non-USD origin
    values (100×/FX errors for GBX/ILA/EUR). (audit fin-fx)"""
    vi = df["Current_Value_ILS"].map(_num) if "Current_Value_ILS" in df.columns else pd.Series(0.0, index=df.index)
    cur = df["Origin_Currency"] if "Origin_Currency" in df.columns else pd.Series("ILS", index=df.index)
    rates = cur.map(lambda c: _ils_per_origin_parent_unit(c, usd_ils))
    return pd.Series(
        [v / r if (r and r > 0) else v for v, r in zip(vi.tolist(), rates.tolist())],
        index=df.index,
    )


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


@st.cache_data(ttl=120, show_spinner=False)
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


@st.cache_data(ttl=25, show_spinner=False)
def fetch_live_prices(tickers: Tuple[str, ...]) -> Dict[str, float]:
    """Live intraday prices via yf.download(period='1d', interval='1m').

    Used by the 20-second auto-refresh fragment for near-real-time prices
    without a full page reload.  Falls back to fetch_prices() when intraday
    data is unavailable (market closed, ticker not found, etc.).

    TTL=25s — slightly longer than fragment's 20s interval to avoid redundant
    yfinance calls within a single fragment cycle.
    """
    clean = tuple(dict.fromkeys([_clean(t).upper() for t in tickers if _clean(t)]))
    if not clean:
        return {}

    symbol_map = {t: _market_symbol(t) for t in clean}
    syms = list(dict.fromkeys(symbol_map.values()))
    out: Dict[str, float] = {}

    try:
        raw = yf.download(
            syms,
            period="1d",
            interval="1m",
            progress=False,
            auto_adjust=False,
            threads=True,
        )
        if raw is not None and not getattr(raw, "empty", True):
            if isinstance(raw.columns, pd.MultiIndex):
                level_zero = {str(v) for v in raw.columns.get_level_values(0)}
                if "Close" in level_zero:
                    for t in clean:
                        sym = symbol_map[t]
                        try:
                            s = pd.to_numeric(raw["Close"][sym], errors="coerce").dropna()
                            if not s.empty:
                                out[t] = float(s.iloc[-1])
                        except Exception:
                            continue
                else:
                    for t in clean:
                        sym = symbol_map[t]
                        try:
                            part = raw[sym]
                            if isinstance(part, pd.DataFrame) and "Close" in part.columns:
                                s = pd.to_numeric(part["Close"], errors="coerce").dropna()
                                if not s.empty:
                                    out[t] = float(s.iloc[-1])
                        except Exception:
                            continue
            elif isinstance(raw, pd.DataFrame) and "Close" in raw.columns and len(syms) == 1:
                s = pd.to_numeric(raw["Close"], errors="coerce").dropna()
                if not s.empty:
                    out[clean[0]] = float(s.iloc[-1])
    except Exception:
        pass

    # Fallback: use daily-close cache for any ticker with no intraday data
    missing = tuple(t for t in clean if out.get(t, 0) <= 0)
    if missing:
        try:
            fallback = fetch_prices(missing)
            for t in missing:
                if fallback.get(t, 0) > 0:
                    out[t] = fallback[t]
        except Exception:
            pass

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

    # USD-quoted tickers must be converted to ILS before summing, otherwise the
    # combined series weights each holding by its native-currency notional (a USD
    # position counts ~3.6x light vs an equal-value ILS one), distorting every
    # aggregate risk metric (vol/Sharpe/MDD/CAGR/beta). Use the USD/ILS history
    # aligned to the price index; fall back to spot if history is unavailable.
    needs_fx = any(_yf_price_currency(t) == "USD" for t in qty_by_ticker)
    fx_series = None
    fx_spot = 0.0
    if needs_fx:
        fx_df = _download_close_matrix(("USDILS=X",), days=max(int(days), 30))
        if not fx_df.empty and "USDILS=X" in fx_df.columns:
            fx_series = pd.to_numeric(fx_df["USDILS=X"], errors="coerce")
        # Must NOT call _usd_ils_rate() here: it writes st.session_state, which is
        # forbidden inside this @st.cache_data function and crashed the Risk page.
        # _safe_quote is session-state-free. (audit bug-st)
        _spot = _safe_quote("USDILS=X")
        fx_spot = _spot if _spot and _spot > 0 else 3.3

    frames = []
    for ticker, qty in qty_by_ticker.items():
        sym = symbol_by_ticker[ticker]
        if sym not in close_df.columns:
            continue
        s = pd.to_numeric(close_df[sym], errors="coerce").rename(ticker)
        if not s.notna().any():
            continue
        if _yf_price_currency(ticker) == "USD":
            if fx_series is not None:
                s = s * fx_series.reindex(s.index).ffill().bfill()
            else:
                s = s * fx_spot
        frames.append(s * qty)

    if not frames:
        return pd.Series(dtype=float)

    combined = pd.concat(frames, axis=1).ffill().fillna(0)
    return combined.sum(axis=1)


def fifo_metrics(trades: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, float]] = []
    usd_ils_rate = _usd_ils_rate()

    # FIFO must consume lots in true chronological order. Sort on a CANONICAL date
    # key (handles DD/MM/YYYY, serials, non-zero-padded dates — a lexical sort on the
    # raw string mis-orders these) with a same-date BUY-before-SELL tie-break so a
    # same-day buy is always an available lot before its sell consumes it. Mirrors
    # engine.js fifoByTicker. (audit fin-streamlit #3, edge-extreme same-day)
    work = trades.copy()
    if "Purchase_Date" in work.columns:
        _fifo_dt = _parse_dates_flexible(work["Purchase_Date"])
    else:
        _fifo_dt = pd.Series(pd.NaT, index=work.index, dtype="datetime64[ns]")
    # NaT dates sort last (they have no usable order); BUY (0) before SELL (1) on ties.
    work["__fifo_date_key__"] = _fifo_dt.astype("int64")
    work.loc[_fifo_dt.isna(), "__fifo_date_key__"] = np.iinfo("int64").max
    # Dispatch off Status (closed/open) like engine.js fifoByTicker/_cmpFifo, NOT the
    # Action field — Action is display-only and can drift out of sync with Status. A
    # CLOSED row sorts after (1) an OPEN row (0) on same-date ties so a same-day buy is
    # an available lot before its sell consumes it. (audit fin-fifo dispatch parity)
    work["__fifo_action_key__"] = work["Status"].map(lambda s: 1 if _is_closed_status(s) else 0) if "Status" in work.columns else 0
    work = work.sort_values(["__fifo_date_key__", "__fifo_action_key__"], kind="stable")

    for ticker, tdf in work.groupby("Ticker", sort=False):
        lots: List[FifoLot] = []
        realized = 0.0
        for _, row in tdf.iterrows():
            # _num safely coerces a blank/garbage Quantity cell to 0.0 instead of
            # raising ValueError and crashing the whole Risk page. (audit fifo robustness)
            qty = float(_num(row["Quantity"]))
            origin_currency = _normalize_currency_code(row.get("Origin_Currency", ""))
            stored_cost_ils = _num(row["Cost_ILS"])
            fallback_cost = float(_num(row["Cost_Origin"]))
            if stored_cost_ils != 0:
                cost_ils = float(stored_cost_ils)
            else:
                # No stored Cost_ILS (e.g. a legacy/un-synced row): fall back to
                # Cost_Origin converted at the BUY-DATE FX (NOT today's spot, which made
                # realized P&L drift with the live rate). _buy_date_fx now handles every
                # currency (USD/GBP/GBX/EUR=rate, ILS/ILA=1). (audit fin-streamlit #1/#2, fin-fx)
                _bdfx = _buy_date_fx(origin_currency, row.get("Purchase_Date", ""))
                cost_ils = fallback_cost * (_bdfx if _bdfx and _bdfx > 0 else 1.0)
            # A row is "self-contained" (carries its own round-trip cost basis) when it
            # has a positive own cost — stored Cost_ILS OR a positive Cost_Origin — NOT
            # merely Cost_ILS != 0. App-created sells used to store Cost_ILS=0, defeating
            # this guard and creating phantom open qty. (audit fin-streamlit #3)
            has_own_cost = (stored_cost_ils != 0) or (fallback_cost > 0)
            unit_cost = abs(cost_ils / qty) if qty else 0.0
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
            _row_closed = _is_closed_status(row.get("Status", ""))
            if (not _row_closed) and qty > 0:
                lots.append(
                    FifoLot(
                        qty=qty,
                        cost_per_unit=unit_cost,
                        display_cost_per_unit=display_unit_cost,
                        display_currency=display_currency,
                    )
                )
            elif _row_closed and qty != 0:
                sell_qty = abs(qty)
                # Zero proceeds on a closed row = TOTAL LOSS (sold for nothing), so sell
                # price is 0, not the buy price — otherwise the realized loss vanishes. (audit C3)
                sell_price = abs(float(_num(row["Current_Value_ILS"]))) / sell_qty if sell_qty and _num(row["Current_Value_ILS"]) != 0 else 0.0
                # A closed position is stored as a SINGLE self-contained row carrying
                # BOTH its own buy cost (Cost_ILS / Cost_Origin) and sale proceeds
                # (Current_Value_ILS) — it is a complete round-trip, NOT a market sell
                # against the running lot inventory. Realize it against its OWN cost
                # basis and leave open BUY lots untouched; otherwise a closed row would
                # cannibalise an unrelated still-open lot of the same ticker (understating
                # open qty AND pricing realized P/L off the wrong cost basis) the moment a
                # ticker has both an open lot and a closed row. Key on a positive OWN cost
                # basis (not Cost_ILS != 0) so app-created sells are detected. (audit #3)
                if has_own_cost:
                    realized += sell_qty * (sell_price - unit_cost)
                else:
                    # True-ledger sell with no own cost basis → consume oldest open
                    # lots first (FIFO), then any residual against this row's cost.
                    while sell_qty > 1e-9 and lots:
                        lot = lots[0]
                        used = min(lot.qty, sell_qty)
                        realized += used * (sell_price - lot.cost_per_unit)
                        lot.qty -= used
                        sell_qty -= used
                        if lot.qty <= 1e-9:
                            lots.pop(0)
                    # Residual after consuming all open lots: only realize it when this
                    # row actually carries a cost basis (unit_cost>0). With no own cost
                    # (unit_cost==0) booking the residual = phantom zero-cost profit on a
                    # negative-inventory oversell — skip it rather than inflate realized. (audit fin-fifo #4)
                    if sell_qty > 1e-9 and unit_cost > 0:
                        realized += sell_qty * (sell_price - unit_cost)

        open_qty = sum(lot.qty for lot in lots)
        # Emit a row when there's an open position OR realized P/L to report.
        # Closed-only tickers have open_qty == 0 but still carry realized P/L.
        if open_qty <= 1e-9 and abs(realized) <= 1e-9:
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


def _usd_ils_rate(default: float = 3.3) -> float:
    """Robust USD/ILS rate. Prefers a live quote; on failure uses the last good
    value seen this session; only as a true last resort uses `default`.

    Replaces a previously hard-coded 3.6 fallback that, when the live FX fetch
    failed, silently inflated every USD position by ~20%+ vs the real ~2.9 rate."""
    live = _safe_quote("USDILS=X")
    try:
        if live and live > 0:
            st.session_state["_last_good_usdils"] = float(live)
            return float(live)
        cached = float(st.session_state.get("_last_good_usdils", 0) or 0)
        if cached > 0:
            return cached
    except Exception:
        if live and live > 0:
            return float(live)
    return default


@st.cache_data(ttl=86400, show_spinner=False)
def _fx_pair_on(pair: str, date_str: str) -> float:
    """Historical close for a Yahoo FX pair (e.g. USDILS=X, GBPILS=X, EURILS=X) on a
    specific (past) date — the most recent trading day on/before it within a short
    window. Returns 0.0 on any failure. NOTE: must NOT touch st.session_state (cached)."""
    d = _clean(date_str)
    p = _clean(pair).upper()
    if not d or not p:
        return 0.0
    try:
        start = pd.to_datetime(d, errors="coerce")
        if pd.isna(start):
            return 0.0
        # Pull a small window ending the day after the target so the close on the
        # target date (or the prior trading day if it was a weekend/holiday) is included.
        win_start = (start - pd.Timedelta(days=6)).strftime("%Y-%m-%d")
        win_end = (start + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        data = yf.download(p, start=win_start, end=win_end,
                           progress=False, auto_adjust=False)
        if data is None or data.empty or "Close" not in data.columns:
            return 0.0
        # Robust to a single-trading-day window (.squeeze() would return a bare scalar with
        # no .dropna() -> AttributeError -> silent fallback to today's spot) and to a
        # multi-column Close frame: flatten to a 1-D numeric series and take the last. (audit)
        close = pd.to_numeric(pd.Series(np.ravel(data["Close"].to_numpy())), errors="coerce").dropna()
        if close.empty:
            return 0.0
        return float(close.iloc[-1])
    except Exception:
        return 0.0


def _usdils_on(date_str: str) -> float:
    """USD/ILS close on a specific (past) date, for converting backdated-sale
    proceeds at the actual sell-date rate rather than today's spot. Returns 0.0 on
    any failure so callers can fall back to the live rate."""
    return _fx_pair_on("USDILS=X", date_str)


def _buy_date_fx(currency: object, purchase_date: object) -> float:
    """ILS-per-unit-of-origin (PARENT unit) on the BUY date, for freezing a
    deterministic Cost_ILS on a sell row. USD → historical USD/ILS close; GBX/GBP →
    historical GBP/ILS; EUR → historical EUR/ILS; ILA/ILS/blank → 1.0 (parent is ILS).
    Live pair rate only as a last resort. Mirrors the GAS cost-basis convention
    costIls = (Cost_Origin+Commission) * buy-date FX, so a closed row's realized P&L
    never drifts with TODAY's spot rate. (audit fin-streamlit #1/#2, fin-fx)"""
    cur = _normalize_currency_code(currency)
    if cur in {"ILS", "ILA", ""}:
        return 1.0
    pair = {"USD": "USDILS=X", "GBX": "GBPILS=X", "GBP": "GBPILS=X", "EUR": "EURILS=X"}.get(cur, f"{cur}ILS=X")
    hist = _fx_pair_on(pair, _clean(purchase_date))
    if hist and hist > 0:
        return hist
    live = _fx_pair_rate(pair) if pair != "USDILS=X" else _usd_ils_rate()
    return float(live) if live and live > 0 else 0.0


def _frozen_cost_ils_for_sell(cost_origin: float, commission: float,
                              currency: object, purchase_date: object) -> float:
    """Frozen ILS cost basis to store on a sell row: (allocated Cost_Origin +
    allocated Commission) converted at the BUY-date FX. Commission is part of the
    cost basis, so it belongs in the ILS basis too. Used so fifo_metrics / the
    realized-ILS column never fall back to today's spot. (audit fin-streamlit #1)"""
    return (float(cost_origin) + float(commission)) * _buy_date_fx(currency, purchase_date)


def _commission_ils_series(df: pd.DataFrame, fx: float) -> pd.Series:
    """Per-row buy commission converted to ILS (Commission is stored in origin
    currency). USD commission is converted at the row's BUY-date FX when a purchase
    date is available (consistent with the frozen Cost_ILS basis), else at `fx`.
    WARNING: NOT used to build Cost_ILS_With_Fee. Cost_ILS is ALREADY commission-
    inclusive ((Cost_Origin+Commission)*fx), so adding this series to it would
    DOUBLE-COUNT the buy fee in the Yield_ILS denominator. Do not wire it in. (audit #10)"""
    if "Commission" not in df.columns:
        return pd.Series(0.0, index=df.index)
    comm = df["Commission"].map(_num)
    if "Origin_Currency" not in df.columns:
        return comm
    cur = df["Origin_Currency"].map(_normalize_currency_code)
    has_date = "Purchase_Date" in df.columns

    def _row_comm(idx: object) -> float:
        c = float(_num(df.at[idx, "Commission"]))
        ccy = cur.at[idx]
        # Commission is stored in the PARENT origin currency. ILS/ILA/blank are already
        # in shekels (parent ILS) — no FX. USD/GBP/GBX/EUR convert at the buy-date rate.
        if ccy in {"ILS", "ILA", ""} or c == 0:
            return c
        rate = _buy_date_fx(ccy, df.at[idx, "Purchase_Date"]) if has_date else float(fx)
        return c * rate if (rate and rate > 0) else c

    return pd.Series([_row_comm(i) for i in df.index], index=df.index)


def _smart_qty(v) -> str:
    """Format a quantity sensibly: whole counts show no decimals; fractional
    (crypto) shows up to 6 decimals with trailing zeros trimmed. Replaces the
    "6.00000000" / "0.43825404" padding that read like a bug in finance tables."""
    try:
        f = float(v)
    except Exception:
        return ""
    if f != f or f in (float("inf"), float("-inf")):  # NaN / inf — int(NaN) raises ValueError
        return ""
    if f == 0:
        return "0"
    if f == int(f) or abs(f) >= 100:
        return f"{f:,.0f}"
    return f"{f:,.6f}".rstrip("0").rstrip(".")


def _fmt_cell(v, f) -> str:
    """Apply a Styler-style format spec (callable or '{:...}' string) to one value."""
    if v is None or (isinstance(v, float) and v != v):
        return ""
    try:
        if callable(f):
            return f(v)
        if isinstance(f, str):
            return f.format(v)
    except Exception:
        pass
    return str(v)


def _esc(s: str) -> str:
    import html as _html
    return _html.escape(str(s), quote=True)


def _render_df_mobile_cards(df, fmt_map=None, signed_cols=()) -> None:
    """Render a wide DataFrame as one compact 'key: value' card per row — so on a
    phone NOTHING gets clipped off the edge (the critic's #1 fix). Used instead of
    a horizontally-overflowing st.dataframe on mobile."""
    fmt_map = fmt_map or {}
    cols = [c for c in df.columns]
    if not cols:
        return
    title_col = cols[0]
    signed = set(signed_cols)
    parts = ['<div class="pp-mcards">']
    for _, r in df.iterrows():
        parts.append('<div class="pp-mcard"><div class="pp-mcard-t">%s</div>' % _esc(_fmt_cell(r[title_col], fmt_map.get(title_col)) or r[title_col]))
        for c in cols[1:]:
            v = r[c]
            vs = _fmt_cell(v, fmt_map.get(c))
            cls = ""
            if c in signed:
                try:
                    cls = " pos" if float(v) >= 0 else " neg"
                except Exception:
                    cls = ""
            parts.append('<div class="pp-mrow"><span class="k">%s</span><span class="v%s">%s</span></div>' % (_esc(c), cls, _esc(vs)))
        parts.append('</div>')
    parts.append('</div>')
    st.markdown("".join(parts), unsafe_allow_html=True)


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

    # Crypto exposure uses the SAME broad classifier as the allocation pie and
    # engine.js aggregates() (assetClass=='crypto' OR a crypto-proxy ETF), so a
    # blank-Type / English-CRYPTO / new-coin row counts identically in both apps.
    # (audit sync-defs crypto-share asymmetry)
    crypto_mask = work.apply(lambda r: _asset_class_label(r.get("Ticker", ""), r.get("Type", "")) == "crypto", axis=1) | (work["Ticker"].isin(CRYPTO_SHARE_TICKERS))
    crypto_value = float(work.loc[crypto_mask, "Current_Value_ILS"].sum())
    btc_value = float(work.loc[work["Ticker"].isin(BTC_SHARE_TICKERS), "Current_Value_ILS"].sum())

    usd_ils = _safe_quote("USDILS=X")
    btc_usd = _safe_quote("BTC-USD")
    eth_usd = _safe_quote("ETH-USD")
    sol_usd = _safe_quote("SOL-USD")

    fx = usd_ils if usd_ils and usd_ils > 0 else _usd_ils_rate()

    work["Cost_Origin_With_Fee"] = work["Cost_Origin"] + work["Commission"]
    # Cost_ILS is ALREADY commission-inclusive ((Cost_Origin+Commission)*fx per GAS/
    # live convention), so the ILS-yield denominator does NOT add Commission again.
    work["Cost_ILS_With_Fee"] = work["Cost_ILS"]
    work["Value_Origin_Est"] = _value_origin_series(work, fx)
    # Detect tickers with mixed currencies — summing ILS+USD costs is invalid.
    _rpt_ticker_pure = (
        work.groupby("Ticker")["Origin_Currency"]
        .apply(lambda s: len(set(s.str.upper().dropna().tolist())) <= 1)
        .to_dict()
    )

    summary = work.groupby("Ticker", as_index=False).agg(
        Cost_ILS=("Cost_ILS", "sum"),
        Cost_ILS_With_Fee=("Cost_ILS_With_Fee", "sum"),
        Value_ILS=("Current_Value_ILS", "sum"),
        # Commission is part of the cost basis: include it in the origin-yield
        # denominator (Cost_Origin + Commission) to match GAS yieldOrigin and
        # engine.js. (audit #10)
        Cost_Origin=("Cost_Origin", "sum"),
        Cost_Origin_With_Fee=("Cost_Origin_With_Fee", "sum"),
        Value_Origin=("Value_Origin_Est", "sum"),
    )
    if summary.empty:
        winner_loser = pd.DataFrame(columns=["Category", "Ticker", "Yield"])
    else:
        summary["Yield_ILS"] = np.where(
            summary["Cost_ILS_With_Fee"] > 0,
            (summary["Value_ILS"] - summary["Cost_ILS_With_Fee"]) / summary["Cost_ILS_With_Fee"],
            0.0,
        )
        _rpt_origin_raw = np.where(
            summary["Cost_Origin_With_Fee"] > 0,
            (summary["Value_Origin"] - summary["Cost_Origin_With_Fee"]) / summary["Cost_Origin_With_Fee"],
            0.0,
        )
        summary["Yield"] = np.where(
            summary["Ticker"].map(lambda t: _rpt_ticker_pure.get(t, True)),
            _rpt_origin_raw,
            summary["Yield_ILS"],
        )
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
        # Indirect exposure = the pure spot-crypto ETF only (IBIT/ETHA/BSOL),
        # converted to coin units. MSTR is intentionally NOT folded into BTC:
        # it's a leveraged equity proxy, not a 1:1 BTC vehicle, and keeping it
        # out matches the Next.js/Flet exposure calc exactly (cross-app parity).
        indirect_ils = etf_val

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
                "Estimated_Coin_Qty": estimated_asset_qty,
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
    try:
        return json.loads(body)
    except (json.JSONDecodeError, ValueError):
        # Google sometimes answers with HTTP 200 but a NON-JSON body — a sign-in /
        # quota / Apps Script redirect HTML page (common intermittently on a phone on
        # cellular). json.loads then raised the cryptic "Expecting value: line 1
        # column 1 (char 0)" that surfaced as the user's pull error. Convert it to a
        # clear, actionable, localized message instead. (sync robustness)
        _looks_html = (body or "").lstrip()[:1] == "<"
        raise RuntimeError(
            ("גוגל החזיר תשובה שאינה JSON"
             + (" (דף התחברות/מכסה/הפניה זמני)" if _looks_html else "")
             + " — נסה שוב בעוד רגע. / Google returned a non-JSON response — retry in a moment.")
        )


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
    # DURABLE BASE: st.secrets persists on Streamlit Community Cloud across every
    # restart/sleep/redeploy. The local file (app_local_config.json) is EPHEMERAL
    # on Cloud — it gets wiped on reboot, which is why the URL/token "kept getting
    # lost". So we ALWAYS merge st.secrets as the base, then overlay the local file
    # (for desktop/local runs) where it has values. Set the secrets ONCE in the
    # Cloud dashboard → Settings → Secrets and the sync never drops again.
    secrets_layer: Dict[str, str] = {}
    try:
        import streamlit as _st
        sec = _st.secrets
        for k in ("web_app_url", "api_token", "spreadsheet_ref", "worksheet_name",
                  "language", "theme_mode", "demo_mode", "followed_symbols"):
            if k in sec:
                v = _clean(str(sec.get(k, "")))
                if v:
                    secrets_layer[k] = v
    except Exception:
        secrets_layer = {}

    file_layer: Dict[str, str] = {}
    if LOCAL_SETTINGS_FILE.exists():
        try:
            raw = json.loads(LOCAL_SETTINGS_FILE.read_text(encoding="utf-8"))
            for k in ("web_app_url", "api_token", "spreadsheet_ref", "worksheet_name",
                      "service_account_file", "language", "theme_mode", "demo_mode", "followed_symbols"):
                v = _clean(str(raw.get(k, "")))
                if v:
                    file_layer[k] = v
        except Exception:
            file_layer = {}

    merged = {**secrets_layer, **file_layer}  # local file overrides secrets where set
    if not merged:
        return {}
    return {
        "web_app_url": _clean(merged.get("web_app_url", DEFAULT_WEB_APP_URL)) or DEFAULT_WEB_APP_URL,
        "api_token": _clean(merged.get("api_token", "")),
        "spreadsheet_ref": _clean(merged.get("spreadsheet_ref", "")),
        "worksheet_name": _clean(merged.get("worksheet_name", DEFAULT_WORKSHEET_NAME)) or DEFAULT_WORKSHEET_NAME,
        "service_account_file": _clean(file_layer.get("service_account_file", str(DEFAULT_SERVICE_ACCOUNT_FILE))),
        "language": _clean(merged.get("language", DEFAULT_LANGUAGE)) or DEFAULT_LANGUAGE,
        "theme_mode": _normalize_theme_mode(merged.get("theme_mode", THEME_SYSTEM)),
        "demo_mode": str(merged.get("demo_mode", "false")).lower() == "true",
        "followed_symbols": _clean(merged.get("followed_symbols", "")),
    }


def save_local_settings(
    web_app_url: str = "",
    api_token: str = "",
    spreadsheet_ref: str = "",
    worksheet_name: str = "",
    service_account_file: str = "",
    language: str = "",
    theme_mode: str = "",
    demo_mode: bool = False,
    followed_symbols: str = "",
    **extra: object,
) -> bool:
    """Persist settings. Known fields are cleaned/typed; any extra kwargs
    (e.g. github_repo, github_pat, github_branch, github_data_path) are
    written through verbatim so the GitHub-sync UI can store its config
    here without needing a separate file."""
    try:
        payload: Dict[str, object] = {
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
        # Merge extras (GitHub config + any future fields)
        for k, v in (extra or {}).items():
            payload[k] = v
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


# ══════════════════════════════════════════════════════════════════════
# LOCAL-FIRST PORTFOLIO STORE  (portfolio_data.json)
# ══════════════════════════════════════════════════════════════════════
# All trade data lives here.  Google Sheets is an OPTIONAL sync target.
# Dirty rows have _dirty=True and _dirty_op in {"add","edit","delete"}.
# ══════════════════════════════════════════════════════════════════════

_PORTFOLIO_TRADE_FIELDS = [
    "Current_Location", "Platform", "Type", "Ticker",
    "Purchase_Date", "Sell_Date", "Quantity", "Origin_Buy_Price",
    "Cost_Origin", "Origin_Currency", "Commission", "Status",
    "Cost_ILS", "Current_Value_ILS", "Sell_Price_Origin",
    "Yield_At_Sale", "Yield_At_Sale_ILS", "Yield_Origin", "Yield_ILS", "Trade_ID",
]


def _portfolio_row_to_dict(row: object) -> Dict[str, object]:
    """Serialise one trade row (Series / dict) to a plain JSON-safe dict."""
    out: Dict[str, object] = {}
    for field in _PORTFOLIO_TRADE_FIELDS:
        if hasattr(row, "get"):
            val = row.get(field, "")
        elif hasattr(row, "__getitem__"):
            try:
                val = row[field]
            except (KeyError, IndexError):
                val = ""
        else:
            val = ""
        if isinstance(val, float) and (val != val):   # NaN
            val = ""
        if isinstance(val, (np.integer, np.floating)):
            val = val.item()
        out[field] = val
    return out


def load_local_portfolio() -> Tuple["pd.DataFrame", Dict[str, object]]:
    """Read portfolio from the local JSON store.

    Uses backup-aware _read_portfolio_file_raw() so a corrupted main file
    automatically falls back to the most-recent good backup.

    Returns:
        (df, meta) — df is a normalised DataFrame (may be empty),
        meta is the _meta dict (empty dict if file missing/corrupt).
    """
    # Fast path: serve the parsed frame from RAM when the file is unchanged.
    _mt = get_portfolio_file_mtime()
    _mem = _LOCAL_PORTFOLIO_MEM
    if _mem.get("df") is not None and _mem.get("mtime") == _mt:
        try:
            return _mem["df"].copy(), dict(_mem.get("meta") or {})
        except Exception:
            pass  # fall through to a fresh read on any cache mishap

    if not LOCAL_PORTFOLIO_FILE.exists():
        # Also check if any backup exists (main file may have been deleted)
        has_backup = any(
            (LOCAL_PORTFOLIO_FILE.parent / f"portfolio_data.bak{i}.json").exists()
            for i in range(1, 4)
        )
        if not has_backup:
            return pd.DataFrame(), {}
    try:
        raw = _read_portfolio_file_raw()
        meta = raw.get("_meta", {})
        rows = raw.get("rows", [])
        if not rows:
            return pd.DataFrame(), meta
        clean_rows = [
            {k: v for k, v in r.items() if not k.startswith("_")}
            for r in rows
            if isinstance(r, dict) and not r.get("_deleted", False)
        ]
        if not clean_rows:
            return pd.DataFrame(), meta
        result = _normalize_snapshot_df(pd.DataFrame(clean_rows))
        # Cache the parsed result; hand callers a copy so they can mutate freely.
        _LOCAL_PORTFOLIO_MEM["mtime"] = _mt
        _LOCAL_PORTFOLIO_MEM["df"] = result
        _LOCAL_PORTFOLIO_MEM["meta"] = dict(meta) if isinstance(meta, dict) else {}
        return result.copy(), meta
    except Exception:
        return pd.DataFrame(), {}


def save_local_portfolio(
    df: "pd.DataFrame",
    *,
    meta_patch: Optional[Dict[str, object]] = None,
    preserve_dirty: bool = True,
) -> bool:
    """Write portfolio DataFrame to the local JSON store.

    Thread-safe (acquires _LOCAL_FILE_LOCK) and atomic (temp-file rename).
    preserve_dirty: keep existing _dirty/_dirty_op flags from the file
                    (True after a local edit; False after a Google pull).
    """
    with _LOCAL_FILE_LOCK:
        try:
            existing_flags: Dict[str, Dict[str, object]] = {}
            existing_meta: Dict[str, object] = {}
            if preserve_dirty:
                try:
                    raw = _read_portfolio_file_raw()
                    existing_meta = raw.get("_meta", {})
                    for r in raw.get("rows", []):
                        if isinstance(r, dict):
                            tid = _clean(str(r.get("Trade_ID", "")))
                            if tid:
                                existing_flags[tid] = {
                                    "_dirty": bool(r.get("_dirty", False)),
                                    "_dirty_op": str(r.get("_dirty_op", "")),
                                    "_deleted": bool(r.get("_deleted", False)),
                                }
                except Exception:
                    pass

            new_meta: Dict[str, object] = {**existing_meta}
            new_meta["_last_modified"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
            new_meta["_version"] = int(existing_meta.get("_version", 0)) + 1
            if meta_patch:
                new_meta.update(meta_patch)

            rows_out = []
            for _, row in df.iterrows():
                rd = _portfolio_row_to_dict(row)
                tid = _clean(str(rd.get("Trade_ID", "")))
                flags = existing_flags.get(tid, {}) if preserve_dirty else {}
                rd["_dirty"] = bool(flags.get("_dirty", False))
                rd["_dirty_op"] = str(flags.get("_dirty_op", ""))
                rd["_deleted"] = bool(flags.get("_deleted", False))
                rows_out.append(rd)

            _write_portfolio_atomic({"_meta": new_meta, "rows": rows_out})
            return True
        except Exception:
            return False


def apply_local_trade(op: str, trade_row: Dict[str, object], *, mark_dirty: bool = True) -> bool:
    """Apply a single trade operation to the local store.

    Thread-safe (acquires _LOCAL_FILE_LOCK), atomic write, and version-bumped.
    op: "add" | "edit" | "delete"
    trade_row must contain Trade_ID (for edit/delete).
    mark_dirty: True for offline-first (marks rows as unsynced);
                False for Google-first atomic (trade already pushed, just mirror locally).
    Returns True on success.
    """
    with _LOCAL_FILE_LOCK:
        try:
            existing = _read_portfolio_file_raw()
            meta = dict(existing.get("_meta", {}))
            meta["_last_modified"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
            meta["_version"] = int(meta.get("_version", 0)) + 1
            rows: List[Dict[str, object]] = list(existing.get("rows", []))
            tid = _clean(str(trade_row.get("Trade_ID", "")))

            if op == "add":
                new_r = _portfolio_row_to_dict(trade_row)
                new_r["_dirty"] = mark_dirty
                new_r["_dirty_op"] = "add" if mark_dirty else ""
                new_r["_deleted"] = False
                rows.append(new_r)

            elif op == "edit":
                updated = False
                for i, r in enumerate(rows):
                    if isinstance(r, dict) and _clean(str(r.get("Trade_ID", ""))) == tid:
                        updated_r = _portfolio_row_to_dict(trade_row)
                        if mark_dirty:
                            updated_r["_dirty"] = True
                            updated_r["_dirty_op"] = r.get("_dirty_op", "edit") if r.get("_dirty_op") == "add" else "edit"
                        else:
                            updated_r["_dirty"] = False
                            updated_r["_dirty_op"] = ""
                        updated_r["_deleted"] = False
                        rows[i] = updated_r
                        updated = True
                        break
                if not updated:   # trade not in store yet — treat as add
                    new_r = _portfolio_row_to_dict(trade_row)
                    new_r["_dirty"] = mark_dirty
                    new_r["_dirty_op"] = "add" if mark_dirty else ""
                    new_r["_deleted"] = False
                    rows.append(new_r)

            elif op == "delete":
                for i, r in enumerate(rows):
                    if isinstance(r, dict) and _clean(str(r.get("Trade_ID", ""))) == tid:
                        if not mark_dirty:
                            # Google-first: already deleted on Google → remove locally entirely
                            rows.pop(i)
                        elif r.get("_dirty_op") == "add":
                            # Never synced → just erase it
                            rows.pop(i)
                        else:
                            rows[i]["_dirty"] = True
                            rows[i]["_dirty_op"] = "delete"
                            rows[i]["_deleted"] = True
                        break

            _write_portfolio_atomic({"_meta": meta, "rows": rows})
            return True
        except Exception:
            return False


def count_dirty_local_trades() -> int:
    """Return the number of local trades pending sync to Google Sheets."""
    if not LOCAL_PORTFOLIO_FILE.exists():
        return 0
    try:
        raw = _read_portfolio_file_raw()
        return sum(1 for r in raw.get("rows", []) if isinstance(r, dict) and bool(r.get("_dirty", False)))
    except Exception:
        return 0


def get_local_portfolio_meta() -> Dict[str, object]:
    """Return the _meta block of the local store (empty dict if missing)."""
    if not LOCAL_PORTFOLIO_FILE.exists():
        return {}
    try:
        raw = _read_portfolio_file_raw()
        return raw.get("_meta", {}) if isinstance(raw, dict) else {}
    except Exception:
        return {}


def sync_portfolio_from_google(
    web_app_url: str,
    api_token: str,
    spreadsheet_ref: str,
    worksheet_name: str,
    service_account_file: str,
    tr=None,
    *,
    force_full_replace: bool = False,
    **_extra: object,
) -> Tuple[bool, str, int]:
    """Pull latest data from Google Sheets and save to local store.

    Preserves any existing dirty (locally-modified) rows so they are not
    overwritten by the remote pull — UNLESS `force_full_replace=True`,
    which discards every local-dirty row and replaces them with whatever
    is in Google (use this when you want to "reset to remote").

    `**_extra` swallows any future-compatibility kwargs callers add.

    Returns (ok, message, row_count).
    """
    if tr is None:
        tr = lambda en, he: en
    try:
        # ── Step 1: Clear ALL remote-fetch caches so we get truly fresh data ──
        for _cache_fn in (load_google_snapshot_data, load_google_snapshot_data_via_gspread):
            try:
                _cache_fn.clear()
            except Exception:
                pass

        _clean_web_url = _clean(web_app_url)
        _clean_sa = _clean(service_account_file)

        # ── Step 2: Fetch remote data — prefer Apps Script, fall back to gspread ──
        # load_google_snapshot_data already calls _normalize_snapshot_df internally.
        # load_google_snapshot_data_via_gspread returns raw data that needs normalizing.
        if _clean_web_url and is_apps_script_web_app_url(_clean_web_url):
            df_remote = load_google_snapshot_data(_clean_web_url, api_token)
        elif _can_use_gspread_fallback(spreadsheet_ref, _clean_sa):
            df_raw = load_google_snapshot_data_via_gspread(spreadsheet_ref, worksheet_name, _clean_sa)
            df_remote = _normalize_snapshot_df(df_raw) if not df_raw.empty else df_raw
        else:
            return False, tr(
                "No Google connection configured (no Web App URL or service-account credentials).",
                "אין חיבור גוגל — לא הוגדר Web App URL או קובץ service-account.",
            ), 0

        if df_remote.empty:
            return False, tr("No rows returned from Google Sheets.", "גוגל שיט לא החזיר נתונים."), 0

        # ── Step 3: TRUE 2-way sync — Google is the single source of truth
        # on pull. Local data becomes an exact mirror of Google: rows that
        # were deleted on another device (and pushed) are now missing from
        # Google → they get deleted locally too. Rows present in Google but
        # missing locally → they get added locally. This is what the user
        # asked for ("אם בגוגל שיט יש שורה פחות אני מצפה שתימחק מהאפליקציה").
        #
        # Implication: if you have unsynced LOCAL edits (dirty rows), they
        # WILL be lost on pull — push them first. We surface the count so
        # the user can decide. The previous merge-preserves-dirty behavior
        # was confusing ("I deleted a row, why is it back?") and is gone.
        dirty_ids: set = set()
        try:
            raw = _read_portfolio_file_raw()
            dirty_ids = {
                _clean(str(r.get("Trade_ID", "")))
                for r in raw.get("rows", [])
                if isinstance(r, dict) and bool(r.get("_dirty", False))
            }
        except Exception:
            pass

        # ── Step 4: Compute the diff (for the user-facing message) ──
        local_df, _ = load_local_portfolio()
        local_ids = (
            set(local_df["Trade_ID"].map(lambda t: _clean(str(t)))) if not local_df.empty else set()
        )
        remote_ids = set(df_remote["Trade_ID"].map(lambda t: _clean(str(t))))
        n_added = len(remote_ids - local_ids)        # rows in Google not in local
        n_removed = len(local_ids - remote_ids)      # rows locally that Google deleted
        n_kept = len(remote_ids & local_ids)         # rows in both (could still differ in fields, but treated as same identity)

        # ── Step 5: Save. force_full_replace was accepted but never honoured — every
        # pull silently full-mirrored and DROPPED unsynced local (dirty) edits, while the
        # sidebar promised "Normal pull keeps them" and offered a Force-pull checkbox that
        # did nothing. Now implemented: a NORMAL pull preserves dirty rows (they win over
        # remote for the same Trade_ID, and dirty-only-local rows are kept); FORCE pull
        # is the true full mirror that discards local edits.
        n_dirty = len(dirty_ids)
        if force_full_replace or not dirty_ids or local_df.empty:
            merged_df = df_remote
            _preserve = False
        else:
            _dirty_mask_local = local_df["Trade_ID"].map(lambda t: _clean(str(t))).isin(dirty_ids)
            dirty_df = local_df[_dirty_mask_local]
            _remote_keep = ~df_remote["Trade_ID"].map(lambda t: _clean(str(t))).isin(dirty_ids)
            merged_df = pd.concat([df_remote[_remote_keep], dirty_df], ignore_index=True)
            _preserve = True
        save_local_portfolio(
            merged_df,
            preserve_dirty=_preserve,
            meta_patch={"_last_synced": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")},
        )
        _save_local_snapshot_cache(merged_df)

        n_google = len(df_remote)
        # Build human-readable summary of what changed
        diff_bits = []
        if n_added:    diff_bits.append(tr(f"+{n_added} added", f"+{n_added} נוספו"))
        if n_removed:  diff_bits.append(tr(f"-{n_removed} deleted", f"-{n_removed} נמחקו"))
        if n_dirty:
            if force_full_replace:
                diff_bits.append(tr(f"⚠ {n_dirty} local edits OVERWRITTEN", f"⚠ {n_dirty} עריכות מקומיות נדרסו"))
            else:
                diff_bits.append(tr(f"{n_dirty} local edits kept", f"{n_dirty} עריכות מקומיות נשמרו"))
        detail = (" · " + ", ".join(diff_bits)) if diff_bits else ""
        return True, tr(
            f"Pulled {n_google:,} rows from Google Sheets.{detail}",
            f"נשלפו {n_google:,} שורות מגוגל שיט.{detail}",
        ), n_google
    except Exception as exc:
        return False, str(exc), 0


def sync_portfolio_to_google(
    web_app_url: str,
    api_token: str,
    tr=None,
) -> Tuple[bool, str, int]:
    """Push all locally-dirty trades to Google Sheets.

    Returns (ok, message, pushed_count).
    """
    if tr is None:
        tr = lambda en, he: en
    try:
        raw = _read_portfolio_file_raw()
        dirty = [r for r in raw.get("rows", []) if isinstance(r, dict) and bool(r.get("_dirty", False))]
        if not dirty:
            return True, tr("Nothing to sync.", "אין שינויים לסנכרן."), 0

        pushed = 0
        errors: List[str] = []
        for r in dirty:  # r is a reference into raw["rows"] (no copy) — mutating it persists below
            op_code = str(r.get("_dirty_op", "edit"))
            trade_clean = {k: v for k, v in r.items() if not k.startswith("_")}
            ok, msg = sync_trade_to_sheet(web_app_url, api_token, op_code, trade_clean)
            if ok:
                pushed += 1
                # Clear dirty PER-ROW immediately. The old code only cleared flags AFTER
                # an `if errors: return` early-exit, so on a PARTIAL failure the rows that
                # already pushed kept _dirty=True and got RE-PUSHED next time → duplicate
                # sheet rows. Clearing here makes a re-push send only the genuinely-failed ones.
                r["_dirty"] = False
                r["_dirty_op"] = ""
            else:
                errors.append(f"[{trade_clean.get('Trade_ID', '?')}] {msg}")

        # Persist whatever succeeded (those rows are now clean), EVEN on partial failure.
        if pushed:
            raw.setdefault("_meta", {})["_last_synced"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
            raw["_meta"]["_version"] = int(raw["_meta"].get("_version", 0)) + 1
            with _LOCAL_FILE_LOCK:
                _write_portfolio_atomic(raw)
        if errors:
            return False, "; ".join(errors[:3]), pushed
        return True, tr(f"Pushed {pushed:,} changes to Google Sheets.", f"נדחפו {pushed:,} שינויים לגוגל שיט."), pushed
    except Exception as exc:
        return False, str(exc), 0


# ───────────────────────────────────────────────────────────────────
# GitHub-based portfolio sync (Google-Sheets-free cross-device sync)
# ───────────────────────────────────────────────────────────────────
#
# Why: the user already pushes app.py to GitHub to get it onto their
# phone. We piggyback on that workflow: the data file lives in the
# SAME repo. Reads use the public raw URL (works without a token).
# Writes use the GitHub Contents API + a fine-grained Personal Access
# Token (PAT) the user pastes once into the Settings expander.
#
# A pull from device A overrides the local file; a push from device B
# commits the merged JSON. There's a small "last-pulled-sha" stored in
# the meta so we can detect cross-device conflicts (user can resolve
# by force-push).
GITHUB_REPO_DEFAULT = "ourielda/my-portfolio-os"  # owner/repo (override per user)
GITHUB_DATA_PATH_DEFAULT = "portfolio_data.json"
GITHUB_BRANCH_DEFAULT = "main"


def _github_raw_url(repo: str, branch: str, path: str) -> str:
    """Public raw URL for a file in a GitHub repo."""
    return f"https://raw.githubusercontent.com/{repo}/{branch}/{path}"


def _github_contents_url(repo: str, path: str) -> str:
    """API URL for the Contents endpoint of a single file."""
    return f"https://api.github.com/repos/{repo}/contents/{path}"


def sync_portfolio_from_github(
    repo: str,
    branch: str,
    path: str,
    pat: str = "",
) -> Tuple[bool, str, int]:
    """Pull `portfolio_data.json` from GitHub and replace the local file.

    PAT is OPTIONAL: public repos work without it via the raw URL.
    Private repos require the PAT (sent as Bearer token to the API).

    Returns (ok, message, row_count).
    """
    try:
        repo = _clean(repo) or GITHUB_REPO_DEFAULT
        branch = _clean(branch) or GITHUB_BRANCH_DEFAULT
        path = _clean(path) or GITHUB_DATA_PATH_DEFAULT
        pat = _clean(pat)

        if pat:
            # Authenticated read via API (works for private repos too)
            req = urlrequest.Request(_github_contents_url(repo, path) + f"?ref={branch}")
            req.add_header("Authorization", f"Bearer {pat}")
            req.add_header("Accept", "application/vnd.github+json")
            req.add_header("X-GitHub-Api-Version", "2022-11-28")
            with urlrequest.urlopen(req, timeout=15) as resp:
                meta = json.loads(resp.read().decode("utf-8"))
            content_b64 = meta.get("content", "")
            sha = meta.get("sha", "")
            import base64 as _b64
            raw_text = _b64.b64decode(content_b64).decode("utf-8")
        else:
            # Public read via raw URL
            with urlrequest.urlopen(_github_raw_url(repo, branch, path), timeout=15) as resp:
                raw_text = resp.read().decode("utf-8")
            sha = ""

        payload = json.loads(raw_text)
        if not isinstance(payload, dict) or "rows" not in payload:
            return False, tr("Remote file is not a valid portfolio JSON.", "הקובץ ברימוט אינו JSON תיק תקין."), 0

        # Normalize back into a DataFrame and use the same save pipeline.
        rows = [r for r in payload.get("rows", []) if isinstance(r, dict) and not r.get("_deleted", False)]
        if not rows:
            return False, tr("Remote file has no rows.", "הקובץ ברימוט ריק."), 0
        clean_rows = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]
        df_remote = _normalize_snapshot_df(pd.DataFrame(clean_rows))

        # Same merge strategy as Google: keep locally-dirty rows.
        dirty_ids: set = set()
        try:
            raw_local = _read_portfolio_file_raw()
            dirty_ids = {
                _clean(str(r.get("Trade_ID", "")))
                for r in raw_local.get("rows", [])
                if isinstance(r, dict) and bool(r.get("_dirty", False))
            }
        except Exception:
            pass
        local_df, _ = load_local_portfolio()
        dirty_df = local_df[local_df["Trade_ID"].map(lambda t: _clean(str(t))).isin(dirty_ids)] if not local_df.empty else pd.DataFrame()
        remote_non_dirty = df_remote[~df_remote["Trade_ID"].map(lambda t: _clean(str(t))).isin(dirty_ids)]
        merged_df = pd.concat([remote_non_dirty, dirty_df], ignore_index=True) if not dirty_df.empty else remote_non_dirty

        save_local_portfolio(
            merged_df,
            preserve_dirty=True,
            meta_patch={"_github_last_pulled_sha": sha, "_github_repo": repo, "_github_branch": branch, "_github_path": path},
        )
        _save_local_snapshot_cache(merged_df)
        return True, tr(f"Pulled {len(df_remote):,} rows from GitHub.", f"נשלפו {len(df_remote):,} שורות מ-GitHub."), len(df_remote)
    except urlerror.HTTPError as exc:
        return False, tr(f"GitHub HTTP {exc.code}: {exc.reason}", f"שגיאת HTTP {exc.code} מ-GitHub: {exc.reason}"), 0
    except Exception as exc:
        return False, str(exc), 0


def sync_portfolio_to_github(
    repo: str,
    branch: str,
    path: str,
    pat: str,
    commit_message: str = "Update portfolio data via app",
) -> Tuple[bool, str, int]:
    """Push the local `portfolio_data.json` to GitHub via the Contents API.

    Requires PAT (write scope: `contents:write` for fine-grained, or `repo`
    for classic). Returns (ok, message, row_count).
    """
    try:
        repo = _clean(repo) or GITHUB_REPO_DEFAULT
        branch = _clean(branch) or GITHUB_BRANCH_DEFAULT
        path = _clean(path) or GITHUB_DATA_PATH_DEFAULT
        pat = _clean(pat)
        if not pat:
            return False, tr("GitHub PAT (Personal Access Token) is required for push.", "נדרש Personal Access Token של GitHub כדי לדחוף."), 0

        # Read current local payload
        if not LOCAL_PORTFOLIO_FILE.exists():
            return False, tr("Local portfolio file is missing — nothing to push.", "אין קובץ תיק מקומי לדחיפה."), 0
        local_text = LOCAL_PORTFOLIO_FILE.read_text(encoding="utf-8")

        # Get current SHA from GitHub (needed for update)
        sha = ""
        try:
            req_get = urlrequest.Request(_github_contents_url(repo, path) + f"?ref={branch}")
            req_get.add_header("Authorization", f"Bearer {pat}")
            req_get.add_header("Accept", "application/vnd.github+json")
            req_get.add_header("X-GitHub-Api-Version", "2022-11-28")
            with urlrequest.urlopen(req_get, timeout=15) as resp:
                meta = json.loads(resp.read().decode("utf-8"))
                sha = meta.get("sha", "")
        except urlerror.HTTPError as exc:
            if exc.code != 404:
                raise  # Anything other than "file doesn't exist yet" → propagate

        import base64 as _b64
        body = {
            "message": _clean(commit_message) or "Update portfolio data",
            "content": _b64.b64encode(local_text.encode("utf-8")).decode("ascii"),
            "branch": branch,
        }
        if sha:
            body["sha"] = sha

        req_put = urlrequest.Request(
            _github_contents_url(repo, path),
            data=json.dumps(body).encode("utf-8"),
            method="PUT",
        )
        req_put.add_header("Authorization", f"Bearer {pat}")
        req_put.add_header("Accept", "application/vnd.github+json")
        req_put.add_header("X-GitHub-Api-Version", "2022-11-28")
        req_put.add_header("Content-Type", "application/json")

        with urlrequest.urlopen(req_put, timeout=20) as resp:
            result = json.loads(resp.read().decode("utf-8"))

        new_sha = (result.get("content") or {}).get("sha", "")

        # Clear all _dirty flags + bump version (same flow as Google push)
        try:
            raw = _read_portfolio_file_raw()
            for r in raw.get("rows", []):
                if isinstance(r, dict):
                    r.pop("_dirty", None)
                    r.pop("_dirty_op", None)
            raw.setdefault("_meta", {})
            raw["_meta"]["_last_synced"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
            raw["_meta"]["_github_last_pulled_sha"] = new_sha
            raw["_meta"]["_github_repo"] = repo
            raw["_meta"]["_github_branch"] = branch
            raw["_meta"]["_github_path"] = path
            raw["_meta"]["_version"] = int(raw["_meta"].get("_version", 0)) + 1
            with _LOCAL_FILE_LOCK:
                _write_portfolio_atomic(raw)
        except Exception:
            pass

        rows_count = local_text.count('"Trade_ID"')  # rough row count
        return True, tr(f"Pushed to GitHub ({rows_count:,} rows).", f"נדחף ל-GitHub ({rows_count:,} שורות)."), rows_count
    except urlerror.HTTPError as exc:
        try:
            err_body = exc.read().decode("utf-8")
        except Exception:
            err_body = ""
        return False, tr(f"GitHub HTTP {exc.code}: {exc.reason}. {err_body[:200]}", f"שגיאת HTTP {exc.code} מ-GitHub: {exc.reason}. {err_body[:200]}"), 0
    except Exception as exc:
        return False, str(exc), 0


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


def _fetch_google_snapshot_raw(web_app_url: str, token: str) -> pd.DataFrame:
    """Pure network fetch of the snapshot from Apps Script — NO Streamlit calls,
    NO caching. Safe to run from a background thread (see _background_sheet_sync).
    """
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


@st.cache_data(ttl=300)
def load_google_snapshot_data(web_app_url: str, token: str) -> pd.DataFrame:
    return _fetch_google_snapshot_raw(web_app_url, token)


# ── Background (non-blocking) sheet sync ─────────────────────────────────
# The app renders INSTANTLY from the local store; the live Google Sheet check
# happens in a daemon thread. When it finishes it writes portfolio_data.json,
# and the 5-second sync-watcher fragment (_portfolio_sync_watcher) reruns the
# app so the fresh numbers appear — no spinner, no blocking, WhatsApp-style.
_BG_SYNC_LOCK = threading.Lock()
_BG_SYNC_STATE: Dict[str, object] = {
    "running": False, "last_start": 0.0, "last_finish": 0.0, "last_result": "",
}
# How stale the local store must be before we kick a background refresh, and the
# minimum gap between background syncs (protects Apps Script quota). Non-blocking,
# so we can refresh far more often than the old 30-min blocking threshold.
_BG_REFRESH_STALE_SECONDS = 180
_BG_SYNC_MIN_INTERVAL_SECONDS = 90


def _background_sheet_sync(web_app_url: str, token: str) -> None:
    result = "err:unknown"
    try:
        df_remote = _fetch_google_snapshot_raw(web_app_url, token)
        if df_remote is not None and not df_remote.empty:
            _save_local_snapshot_cache(df_remote)
            save_local_portfolio(
                df_remote, preserve_dirty=False,
                meta_patch={"_last_synced": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")},
            )
            result = "ok:%d" % len(df_remote)
        else:
            result = "empty"
        _clear_apps_script_cooldown()
    except Exception as exc:
        if _is_timeout_error(exc):
            _mark_apps_script_timeout()
        result = "err:" + str(exc)[:120]
    finally:
        with _BG_SYNC_LOCK:
            _BG_SYNC_STATE["running"] = False
            _BG_SYNC_STATE["last_finish"] = time.time()
            _BG_SYNC_STATE["last_result"] = result


def _maybe_start_bg_sync(web_app_url: str, token: str) -> bool:
    """Start a background sheet sync if one isn't already running and enough time
    has passed. Returns True if a new sync was started. Never blocks."""
    now = time.time()
    with _BG_SYNC_LOCK:
        if _BG_SYNC_STATE["running"]:
            return False
        if now - float(_BG_SYNC_STATE["last_start"] or 0) < _BG_SYNC_MIN_INTERVAL_SECONDS:
            return False
        _BG_SYNC_STATE["running"] = True
        _BG_SYNC_STATE["last_start"] = now
    try:
        threading.Thread(
            target=_background_sheet_sync, args=(web_app_url, token), daemon=True
        ).start()
        return True
    except Exception:
        with _BG_SYNC_LOCK:
            _BG_SYNC_STATE["running"] = False
        return False


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
        "תשואה במכירה (₪)": "Yield_At_Sale_ILS",
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

    for col in ["Yield_At_Sale", "Yield_At_Sale_ILS", "Yield_Origin", "Yield_ILS"]:
        if col in df.columns:
            # Keep truly missing yields as NaN so downstream logic can backfill.
            df[col] = df[col].map(_num_or_nan)

    # ── Backfill Cost_Origin ONLY when it is missing/zero ──
    # Previously a >5% divergence from Quantity*Origin_Buy_Price also triggered an
    # overwrite + proportional Cost_ILS scaling — but a stored Cost_Origin legitimately
    # differs (commission baked in, averaged partial fills), so that silently corrupted
    # the cost basis (e.g. SOL). Trust a stored non-zero Cost_Origin; only fill blanks. (audit)
    qty = df["Quantity"]
    price = df["Origin_Buy_Price"]
    expected_cost = qty * price
    has_both = (qty.abs() > 1e-9) & (price.abs() > 1e-9)
    cost_orig = df["Cost_Origin"]
    diverged = has_both & (cost_orig < 1e-9)
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
    # Use the canonical closed-status set (סגור/closed/close/sold/נמכר), not just
    # "סגור". Previously a sale labeled with any other synonym was tagged BUY,
    # which made FIFO treat it as a phantom buy lot (overstated open qty, missing
    # realized PnL). Mirrors _is_closed_status used everywhere else.
    df["Action"] = df["Status"].map(lambda x: "SELL" if _is_closed_status(x) else "BUY")
    return df


def build_demo_snapshot_data() -> pd.DataFrame:
    # Synthetic showcase portfolio used only for demo mode. Intentionally compact
    # (~10 open holdings, low total ≈ 80k ILS) so the allocation pie stays clean and
    # readable, while still feeling varied: every asset class (ETF / stock / crypto),
    # two currencies (USD/ILS), clear winners AND clear losers, plus two closed trades
    # (one gain, one loss) so realized-PnL / FIFO / allocation / build-up all populate.
    # Read-only. (Mirrors the native apps' demo philosophy at presentation scale.)
    FX = {"USD": 3.65, "ILS": 1.0}

    def mk(ticker, typ, platform, ccy, qty, buy, now_mul, date, commission,
           closed=False, sell_date="2026-03-18", loc=""):
        fx = FX.get(ccy, 1.0)
        cost_origin = qty * buy
        # Market value scales off the FEE-EXCLUSIVE invested principal.
        cur_ils = (cost_origin * fx) * now_mul
        # Cost_ILS is commission-INCLUSIVE ((Cost_Origin+Commission)*fx), mirroring the
        # GAS/live convention so demo Yield_ILS and Yield_Origin agree. (audit demo fee)
        cost_ils = (cost_origin + commission) * fx
        row = {
            "Current_Location": loc,
            "Platform": platform,
            "Type": typ,
            "Ticker": ticker,
            "Purchase_Date": date,
            "Quantity": qty,
            "Origin_Buy_Price": buy,
            "Cost_Origin": round(cost_origin, 2),
            "Origin_Currency": ccy,
            "Commission": commission,
            "Status": "סגור" if closed else "פתוח",
            "Cost_ILS": round(cost_ils, 2),
            "Current_Value_ILS": round(cur_ils, 2),
        }
        if closed:
            row["Sell_Date"] = sell_date
            row["Sell_Price_Origin"] = round(buy * now_mul, 4)
        return row

    demo_rows = [
        # ── ETFs (USD) — steady core ─────────────────────────────────────────
        mk("VOO",  "ETF", "Global Prime", "USD", 6, 408, 1.33, "2023-11-10", 6),
        mk("QQQ",  "ETF", "Global Prime", "USD", 4, 372, 1.46, "2024-01-22", 6),
        mk("IBIT", "ETF", "Global Prime", "USD", 30, 33, 1.52, "2024-02-19", 4),
        # ── Stocks (USD) — a big winner + clear losers ───────────────────────
        mk("NVDA", "שוק ההון", "Global Prime", "USD", 7,  95,  2.65, "2023-09-18", 5),
        mk("AAPL", "שוק ההון", "Global Prime", "USD", 5,  165, 1.28, "2024-02-08", 5),
        mk("TSLA", "שוק ההון", "Quant Desk",   "USD", 4,  255, 0.80, "2024-06-12", 5, loc="Underwater"),
        mk("INTC", "שוק ההון", "Quant Desk",   "USD", 12, 44,  0.53, "2024-04-19", 4, loc="Underwater"),
        # ── Crypto (ILS · local desk) — a moon + a crash ─────────────────────
        mk("BTC",  "קריפטו", "Bit2C", "ILS", 0.03, 142000, 1.85, "2023-08-14", 0, loc="Cold Wallet"),
        mk("SOL",  "קריפטו", "Bit2C", "ILS", 11,   390,    2.20, "2023-10-22", 0, loc="Staking Wallet"),
        mk("DOGE", "קריפטו", "Bit2C", "ILS", 4000, 0.5,    0.58, "2024-09-11", 0, loc="Underwater"),
        # ── Closed trades (one gain, one loss) ───────────────────────────────
        mk("META", "שוק ההון", "Global Prime", "USD", 5,  300, 1.70, "2023-07-01", 5, closed=True, sell_date="2025-11-20"),
        mk("ARKK", "ETF",      "Global Prime", "USD", 20, 51,  0.84, "2024-05-22", 4, closed=True, sell_date="2025-09-08"),
    ]
    df = pd.DataFrame(demo_rows)

    # Keep the demo total intentionally low for presentations (≈ 80k ILS open value)
    # so the allocation pie and figures stay clean and uncluttered.
    open_mask = df["Status"].map(_clean) != "סגור"
    open_value_ils = float(df.loc[open_mask, "Current_Value_ILS"].map(_num).sum())
    target_open_value_ils = 80000.0
    if open_value_ils > (target_open_value_ils + 1e-9):
        scale = target_open_value_ils / open_value_ils
        for col in ["Cost_Origin", "Cost_ILS", "Current_Value_ILS", "Commission"]:
            df.loc[open_mask, col] = df.loc[open_mask, col].map(_num) * scale

    return _normalize_snapshot_df(df)


def _local_store_age_seconds(meta: Dict[str, object]) -> float:
    """Seconds since the local store was last synced from the sheet.
    Returns a large number (treat as very stale) if unknown/unparseable."""
    raw = _clean(str((meta or {}).get("_last_synced", "")))
    if not raw:
        return 1e12
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            ts = datetime.strptime(raw[:19], fmt)
            return max(0.0, (datetime.utcnow() - ts).total_seconds())
        except Exception:
            continue
    return 1e12


def load_snapshot_data(
    web_app_url: str,
    token: str,
    spreadsheet_ref: str,
    worksheet_name: str,
    service_account_file: str,
) -> Tuple[pd.DataFrame, str]:
    """Load portfolio snapshot — LOCAL-FIRST.

    Priority:
      1. Local portfolio store (portfolio_data.json)  ← primary source of truth
      2. snapshot_cache.csv / verified_data.csv       ← auto-migrate on first run
      3. Google Sheets (Apps Script or gspread)       ← used on first run / after sync
    """
    # ── 1. Local store (primary, but refreshed when stale) ───────────
    local_df, _meta = load_local_portfolio()
    if not local_df.empty:
        # INSTANT render: always serve the local store immediately. When it is
        # stale (and there are no unsynced local edits), refresh from the live
        # sheet in a BACKGROUND thread — never block the page. The 5-second
        # sync-watcher fragment reruns the app once the fresh data lands, so the
        # numbers update behind the scenes without hurting the experience.
        try:
            clean_url0 = _clean(web_app_url)
            if (clean_url0 and is_apps_script_web_app_url(clean_url0)
                    and not _apps_script_on_cooldown()
                    and _local_store_age_seconds(_meta) > _BG_REFRESH_STALE_SECONDS
                    and count_dirty_local_trades() == 0):
                _maybe_start_bg_sync(clean_url0, token)
        except Exception:
            pass
        return local_df, "local_store"

    # ── 1b. Auto-migrate from snapshot_cache / verified_data ─────────
    # On first run (portfolio_data.json doesn't exist yet but there is an
    # existing cache), silently migrate so the user sees their data immediately.
    _backup_df, _backup_mode = _load_emergency_snapshot_backup()
    if not _backup_df.empty:
        save_local_portfolio(_backup_df, preserve_dirty=False)
        return _backup_df, "local_store"

    # ── 2. Google Sheets (only when local store is truly empty) ──────
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
            save_local_portfolio(df_remote, preserve_dirty=False)
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
                save_local_portfolio(normalized, preserve_dirty=False)
                return normalized, "gspread"
            except Exception:
                backup_df, backup_mode = _load_emergency_snapshot_backup()
                if not backup_df.empty:
                    return backup_df, backup_mode
                raise

    if fallback_ready:
        try:
            raw_df = load_google_snapshot_data_via_gspread(spreadsheet_ref, worksheet_name, service_account_file)
            normalized = _normalize_snapshot_df(raw_df)
            _save_local_snapshot_cache(normalized)
            save_local_portfolio(normalized, preserve_dirty=False)
            return normalized, "gspread"
        except Exception:
            pass

    backup_df, backup_mode = _load_emergency_snapshot_backup()
    if not backup_df.empty:
        return backup_df, backup_mode

    # ── 3. Truly empty — no data anywhere ────────────────────────────
    return pd.DataFrame(), "empty"


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


@st.cache_data(ttl=300, show_spinner=False, hash_funcs={pd.DataFrame: lambda d: (len(d), tuple(d.columns), str(pd.util.hash_pandas_object(d).sum() if len(d) else 0))})
def prepare_core_views(df: pd.DataFrame) -> Dict[str, object]:
    trades = df[(df["Record_Source"] == "STATE_SNAPSHOT") & (df["Event_Type"] == "TRADE")].copy() if "Record_Source" in df.columns else df.copy()
    if "Status" not in trades.columns:
        trades["Status"] = ""
    trades["Status"] = trades["Status"].replace("", "פתוח")
    status_norm = trades["Status"].map(_clean).str.lower()
    closed_mask = status_norm.isin(CLOSED_STATUS_VALUES)
    open_trades = trades[~closed_mask].copy()
    closed_trades = trades[closed_mask].copy()

    # Safety fallback: rescue ONLY rows whose Status is UNRECOGNIZED (neither a known
    # closed label nor a SELL action) — a closed/sold row ALWAYS has positive proceeds
    # in Current_Value_ILS, so the old value/qty heuristic reclassified a genuinely
    # all-sold book as OPEN, fabricating phantom Total Value / Open P&L and zeroing the
    # closed count. A truly all-closed portfolio must stay closed. (audit bug-st #3)
    if open_trades.empty and not trades.empty:
        action_norm = trades["Action"].map(_clean).str.upper() if "Action" in trades.columns else pd.Series("", index=trades.index)
        unknown_mask = (~closed_mask) & action_norm.ne("SELL")
        if unknown_mask.any():
            open_trades = trades[unknown_mask].copy()
            closed_trades = trades[~unknown_mask].copy()

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


@st.cache_data(ttl=120, show_spinner=False, hash_funcs={pd.DataFrame: lambda d: (len(d), tuple(d.columns), str(pd.util.hash_pandas_object(d).sum() if len(d) else 0))})
def _enrich_compute(open_trades: pd.DataFrame, live_prices: Mapping[str, float], usd_ils: float) -> pd.DataFrame:
    """Pure (no session_state) per-row live-price enrichment. Cached on
    (data, live_prices, rate) so repeated dashboard reruns reuse the result
    instead of re-running the pandas maps every time. The session_state-touching
    rate/price resolution stays in the thin wrapper below. (perf-st)"""
    out = open_trades.copy()
    out["מחיר שוק"] = out["Ticker"].map(live_prices).fillna(0.0) if "Ticker" in out.columns else 0.0
    qty_series = out["Quantity"].map(_num) if "Quantity" in out.columns else 0.0
    out["שווי שוק (יחסי מטבע מקור)"] = qty_series * out["מחיר שוק"]
    # Recalculate Current_Value_ILS using live prices + FX.
    # IMPORTANT: use _yf_price_currency (the currency yfinance quotes the price in),
    # NOT Origin_Currency.  Bit2C BTC has Origin_Currency=ILS (prices quoted in ₪),
    # but yfinance returns BTC-USD → we must still multiply by the USD/ILS rate.
    if "Ticker" in out.columns:
        price_cur_series = out["Ticker"].map(lambda t: _yf_price_currency(_clean(t).upper()))
    else:
        price_cur_series = pd.Series(["USD"] * len(out), index=out.index)
    _fxm = price_cur_series.map(lambda c: _ils_per_native_unit(c, usd_ils))
    live_value_ils = qty_series * out["מחיר שוק"] * _fxm
    # Only override where we got a live price (>0) AND a usable ILS multiplier (>0);
    # a 0 multiplier means an unknown/unavailable rate → keep the stored value
    # rather than zero it out (mirrors engine.js toIls NaN → keep stored). (audit fin-fx)
    has_live = (out["מחיר שוק"] > 0) & (_fxm > 0)
    out.loc[has_live, "Current_Value_ILS"] = live_value_ils[has_live]
    return out


def enrich_open_trades_with_prices(open_trades: pd.DataFrame) -> pd.DataFrame:
    out = open_trades.copy()
    tickers = tuple(sorted(t for t in out["Ticker"].dropna().unique() if _clean(t))) if "Ticker" in out.columns else tuple()
    live_prices = fetch_prices(tickers)
    usd_ils = _safe_quote("USDILS=X")
    if usd_ils <= 0:
        usd_ils = _usd_ils_rate()  # session_state-touching fallback stays OUT of the cached compute
    return _enrich_compute(out, dict(live_prices), float(usd_ils))


def refresh_open_trade_values(df: pd.DataFrame) -> pd.DataFrame:
    """RETIRED — intentionally NOT called (kept for reference only).

    The per-load synchronous yfinance refresh this performed was removed in favour
    of instant local-first paint + a background sync + the live-price KPI fragment.
    Do NOT re-wire it into the load path: it does a blocking network loop that would
    reintroduce the slow first-paint it was built to avoid.

    Refresh Current_Value_ILS for all open trades using live yfinance prices.

    Should be called once after loading portfolio data from local store so that
    KPI totals (total_value, total_profit) reflect real-time market prices instead
    of the stale values persisted in portfolio_data.json.
    """
    if df.empty:
        return df

    out = df.copy()

    # Identify open trades
    if "Status" in out.columns:
        status_norm = out["Status"].map(_clean).str.lower()
        open_mask = ~status_norm.isin(CLOSED_STATUS_VALUES)
    else:
        open_mask = pd.Series([True] * len(out), index=out.index)

    open_idx = out.index[open_mask]
    if len(open_idx) == 0:
        return out

    tickers = tuple(sorted(t for t in out.loc[open_idx, "Ticker"].dropna().unique() if _clean(t))) if "Ticker" in out.columns else tuple()
    if not tickers:
        return out

    live_prices = fetch_prices(tickers)
    usd_ils = _safe_quote("USDILS=X")
    if usd_ils <= 0:
        usd_ils = _usd_ils_rate()
    for idx in open_idx:
        row = out.loc[idx]
        ticker = _clean(str(row.get("Ticker", "")))
        if not ticker:
            continue
        price = live_prices.get(ticker, 0.0)
        if price <= 0:
            continue
        qty = float(_num(row.get("Quantity", 0)))
        if abs(qty) < 1e-12:
            continue
        cur = _normalize_currency_code(row.get("Origin_Currency", "")) or "ILS"
        # Use yfinance quote currency — not Origin_Currency — to decide FX multiplier.
        # BTC on Bit2C has Origin_Currency=ILS but yfinance returns a USD price (BTC-USD),
        # so we must always apply the USD→ILS rate for crypto / USD-quoted assets.
        yf_cur = _yf_price_currency(ticker.upper())
        fx = _ils_per_native_unit(yf_cur, usd_ils)
        if fx <= 0:
            continue
        out.at[idx, "Current_Value_ILS"] = qty * price * fx

    return out


def dataframe_completeness(df: pd.DataFrame) -> Tuple[int, int, float]:
    if df.empty:
        return 0, 0, 0.0

    # Sale-only fields are LEGITIMATELY empty for open positions; internal "_" cols
    # are bookkeeping. Counting them (and counting the literal string "NaT"/"עדיין
    # פתוח" as "filled") made completeness permanently report ~92% on perfectly
    # clean data, tripping a false "data-quality issue" warning on every load.
    SALE_ONLY = {"Sell_Date", "Sell_Price_Origin", "Yield_At_Sale", "Buy_Price"}
    PLACEHOLDERS = {"", "nat", "none", "nan", "null", "—", "-", "עדיין פתוח"}

    cols = [c for c in df.columns if not str(c).startswith("_") and c not in SALE_ONLY]
    if not cols:
        return 0, 0, 0.0

    total_cells = int(df.shape[0] * len(cols))
    non_empty = 0
    for col in cols:
        series = df[col]
        if series.dtype == object:
            cleaned = series.map(lambda v: _clean(v).lower())
            non_empty += int((~cleaned.isin(PLACEHOLDERS)).sum())
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


def _pp_inject_loading_bar() -> None:
    """A clear top PAGE-LOADING bar: a thin animated gradient bar slides in while
    Streamlit is rerunning (page switch / heavy compute) and completes + fades the
    moment the page is ready for full interaction. Set up ONCE on the parent window
    (survives the per-rerun component iframe). Debounced ~220ms so fast 20s-fragment
    refreshes don't flash it. Works on desktop AND phone."""
    components.html(
        """
        <script>(function(){
          try{
            var W=window.parent, doc=W.document;
            if(W.__ppLoadbarInit) return;          // set up exactly once
            W.__ppLoadbarInit=true;
            var st=doc.createElement('style');
            st.textContent='#pp-loadbar{position:fixed;top:0;left:0;height:3px;width:0;z-index:2147483647;'
              +'background:linear-gradient(90deg,#6366f1,#22d3ee,#a78bfa);background-size:200% 100%;'
              +'box-shadow:0 0 12px rgba(99,102,241,.75);border-radius:0 3px 3px 0;opacity:0;'
              +'transition:width .2s ease,opacity .45s ease;animation:ppLoadShift 1.1s linear infinite;}'
              +'@keyframes ppLoadShift{to{background-position:200% 0}}';
            doc.head.appendChild(st);
            var bar=doc.createElement('div'); bar.id='pp-loadbar'; doc.body.appendChild(bar);
            var running=false, prog=0, anim=null, delay=null, startTs=0;
            function up(){ prog=Math.min(prog+Math.max(0.4,(92-prog)*0.07),92); bar.style.width=prog+'%'; }
            function start(){ if(running) return; running=true; startTs=Date.now(); prog=10; bar.style.opacity='1'; bar.style.width='10%'; anim=W.setInterval(up,180); }
            function done(){ if(delay){W.clearTimeout(delay); delay=null;} if(!running) return; running=false; W.clearInterval(anim);
              bar.style.width='100%'; W.setTimeout(function(){bar.style.opacity='0'; W.setTimeout(function(){bar.style.width='0';},450);},180); }
            function busy(){ try{
              // Only Streamlit's run-status widget — NOT [data-stale], which can stay
              // stuck on some pages (e.g. the AI Agent) and froze the bar. (fix)
              // Detect by CONTENT, not offsetParent: the app hides stStatusWidget with
              // display:none (offsetParent is then null even WHILE running), which made
              // this bar never appear. innerText/spinner still update under display:none.
              var sw=doc.querySelector('[data-testid="stStatusWidget"]');
              if(!sw) return false;
              var t=(sw.innerText||'').toLowerCase();
              // TEXT ONLY. A generic svg / connection icon is present even when IDLE, so
              // matching svg made busy() true forever → the bar STUCK at ~90%. Streamlit
              // shows the literal word "Running"/"Connecting" only while actually running.
              return t.indexOf('running')>=0 || t.indexOf('connecting')>=0;
            }catch(e){ return false; } }
            W.setInterval(function(){
              if(running && (Date.now()-startTs)>6000){ done(); return; }   // safety net: never stay stuck >6s
              if(busy()){ if(!running && !delay){ delay=W.setTimeout(function(){delay=null; start();},220); } }
              else { done(); }
            },140);
          }catch(e){}
        })();</script>
        """,
        height=0, width=0,
    )


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
    try:
        _pp_inject_loading_bar()
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
    # Idempotency guard: the JS plants persistent parent-document listeners +
    # a polling interval once (`__pp_help_shim_installed`). Re-injecting on every
    # rerun only spawns a throwaway iframe, so inject once per session.
    # (perf-st: idempotent injector)
    try:
        if st.session_state.get("_pp_help_shim_done"):
            return
        st.session_state["_pp_help_shim_done"] = True
    except Exception:
        pass
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
                    c.setAttribute('data-pp-phantom2', '1');
                  }
                } else if (c.getAttribute('data-pp-phantom2')) {
                  // NON-DESTRUCTIVE: this scanner walks the WHOLE document (incl. the sidebar).
                  // A container that was momentarily empty during a slow cold-start render got
                  // display:none'd and was NEVER restored — that is why the sidebar opened
                  // empty/WHITE on the first open until a manual refresh. Restore it the moment
                  // it gains real content. (android cold-start: empty sidebar fix)
                  c.style.removeProperty('display');
                  c.removeAttribute('data-pp-phantom2');
                }
              }
            }
            collapsePhantoms();
            try {
              // SINGLE observer (disconnect any prior) — this block re-emits every rerun
              // and previously leaked a new subtree observer each time. (perf leak fix)
              var Wp = d.defaultView || window.parent || window;
              try { if (Wp.__ppPhantomObs2) Wp.__ppPhantomObs2.disconnect(); } catch(e2) {}
              var mo = new MutationObserver(function(){
                requestAnimationFrame(collapsePhantoms);
              });
              mo.observe(d.body, {childList: true, subtree: true});
              Wp.__ppPhantomObs2 = mo;
            } catch(e) {}
          } catch(e) {}
        })();</script>
        """,
        height=0, width=0,
    )


def _pp_inject_premium_css(is_dark: bool, is_mobile: bool) -> None:
    # NO idempotency guard: this emits its <style> via st.markdown, which lives in
    # Streamlit's element tree. Streamlit rebuilds that tree on EVERY rerun, so a
    # session_state guard that returns early after run #1 deletes the <style> on the
    # next interaction — the header/animations/mobile sizing vanish mid-session.
    # st.markdown re-emit is cheap; it MUST run every rerun. (regression fix)
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
    button[data-baseweb="tab"][aria-selected="true"] {{ color: #4f46e5 !important; }}
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
    section.main > div.block-container {{ animation: none !important; }}
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
    # NO idempotency guard: emitted via st.markdown (Streamlit element tree, rebuilt
    # every rerun). A session_state early-return deletes this <style> on the next
    # interaction, killing the mobile polish + animations. Must re-emit every run. (regression fix)
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

    /* ── Focus rings: accessible & modern (main content only) ── */
    :focus-visible {{
        outline: 2px solid #6366f1 !important;
        outline-offset: 2px !important;
        border-radius: 8px;
    }}
    /* Suppress focus rings inside the sidebar — sidebar is mouse/touch driven
       and the colored outline creates a confusing "small hoop" next to Hebrew
       widget labels (mislabelled as a status indicator by users). */
    [data-testid="stSidebar"] :focus-visible,
    [data-testid="stSidebar"] :focus {{
        outline: none !important;
        box-shadow: none !important;
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
        /* Primary buttons (kind="primary") — same charcoal-to-sapphire
           gradient as the nav pills + tabs. ONE accent identity across
           every prominent button on mobile. Less "kindergarten purple". */
        .stButton > button[kind="primary"],
        .stButton > button[data-testid="baseButton-primary"],
        button[kind="primary"] {{
            background: linear-gradient(135deg, #1e40af 0%, #2563eb 55%, #3b82f6 100%) !important;
            color: #fff !important;
            border: 0 !important;
            font-weight: 700 !important;
            letter-spacing: 0.01em !important;
            box-shadow: 0 4px 14px -3px rgba(15,23,42,0.45),
                        0 1px 0 0 rgba(255,255,255,0.10) inset !important;
            transition: filter 120ms ease, box-shadow 140ms ease, transform 80ms ease !important;
        }}
        .stButton > button[kind="primary"]:hover,
        button[kind="primary"]:hover {{
            filter: brightness(1.18);
            transform: translateY(-1px);
            box-shadow: 0 6px 18px -4px rgba(30,58,138,0.55) !important;
        }}
        .stButton > button[kind="primary"]:active,
        button[kind="primary"]:active {{
            transform: translateY(0);
            filter: brightness(0.94);
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
            background: linear-gradient(135deg, #1e40af 0%, #2563eb 55%, #3b82f6 100%) !important;
            color: #fff !important;
            box-shadow: 0 4px 12px -3px rgba(15,23,42,0.55),
                        0 1px 0 0 rgba(255,255,255,0.08) inset !important;
        }}
        [data-baseweb="tab-list"] [data-baseweb="tab"][aria-selected="true"] * {{ color:#fff !important; }}
        [data-baseweb="tab-list"] [data-baseweb="tab"][aria-selected="true"]:hover {{
            filter: brightness(1.10);
        }}

        /* Plotly charts: taller intrinsic box, no modebar on mobile */
        [data-testid="stPlotlyChart"] {{ contain-intrinsic-size: 300px; position: relative; }}
        .modebar-container, .modebar {{ display: none !important; }}

        /* Radio buttons: always horizontal, no wrap; labels must not shrink
           below their text width so the gradient bg covers all the text. */
        [data-testid="stRadio"] > div {{
            flex-wrap: nowrap !important;
            gap: 0.35rem !important;
        }}
        [data-testid="stRadio"] > div > label {{
            white-space: nowrap !important;
            font-size: 13px !important;
            padding: 6px 10px !important;
            min-width: fit-content !important;
            flex-shrink: 0 !important;
        }}
        /* Page-nav 4-pill bar: allow hidden horizontal scroll so Hebrew labels
           that are longer than the allocated space don't overflow the colored frame */
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child) > div,
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(5):last-child) > div {{
            overflow-x: auto !important;
            overflow-y: hidden !important;
            -webkit-overflow-scrolling: touch !important;
            scrollbar-width: none !important;
        }}
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child) > div::-webkit-scrollbar,
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(5):last-child) > div::-webkit-scrollbar {{
            display: none !important;
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
            margin-right: 0 !important;
            margin-left: 0 !important;
            width: 100% !important;
            box-sizing: border-box !important;
        }}
        /* Each pill — neutral by default; ACTIVE uses a deep charcoal-to-
           sapphire gradient (premium, not "kindergarten purple"). The same
           palette is reused for tabs and primary buttons below so the whole
           mobile UI shares ONE accent identity. */
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
            background: linear-gradient(135deg, #1e40af 0%, #2563eb 55%, #3b82f6 100%) !important;
            box-shadow: 0 4px 14px -3px rgba(15,23,42,0.55),
                        0 1px 0 0 rgba(255,255,255,0.08) inset !important;
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
        /* (legacy gradient rule removed — was overriding the new royal-blue
           premium gradient defined above. Same selector, kept for backwards
           compat but now matches the brand palette.) */
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
        [data-baseweb="radio"]:has(input:checked),
        [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(5):last-child)
        [data-baseweb="radio"]:has(input:checked) {{
            background: linear-gradient(135deg, #1e40af 0%, #2563eb 55%, #3b82f6 100%) !important;
            box-shadow: 0 4px 12px -2px rgba(37,99,235,0.50) !important;
        }}

        /* Safer hamburger — bigger, subtle press */
        [data-testid="stExpandSidebarButton"] button,
        [data-testid="stExpandSidebarButton"] button,
        button[aria-label*="sidebar"] {{
            transition: transform 120ms var(--pp2-ease), box-shadow 120ms var(--pp2-ease) !important;
        }}
        [data-testid="stExpandSidebarButton"] button:active,
        [data-testid="stExpandSidebarButton"] button:active,
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
    # Idempotency guard: the JS uses an `ensure()` upsert into the parent <head>,
    # so re-running it only spawns a throwaway iframe. Inject once per session,
    # re-injecting only when the theme or language changes (those alter the
    # manifest / theme-color). (perf-st: idempotent injector)
    try:
        _pwa_key = (language, bool(is_dark))
        if st.session_state.get("_pp_pwa_assets_key") == _pwa_key:
            return
        st.session_state["_pp_pwa_assets_key"] = _pwa_key
    except Exception:
        pass
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
        // 'default' (not 'black-translucent'): in a standalone iOS PWA, black-translucent
        // makes the web content render UNDER the status bar/notch, hiding the page header
        // and pushing the sidebar menu button (top ~12px) behind the clock so it can't be
        // tapped. 'default' reserves the status-bar space so all top UI is visible+tappable.
        ensure('meta[name="apple-mobile-web-app-status-bar-style"]', {{_tag:'meta', name:'apple-mobile-web-app-status-bar-style', content:'default'}});
        ensure('meta[name="mobile-web-app-capable"]', {{_tag:'meta', name:'mobile-web-app-capable', content:'yes'}});
        ensure('link[rel="manifest"]', {{_tag:'link', rel:'manifest', href:'{manifest_data_uri}'}});
      }} catch(e) {{}}
    }})();
    </script>
    """
    components.html(html, height=0, width=0)


def _pp_inject_productivity_layer(language: str) -> None:
    # Idempotency guard: this injects a command-palette + keyboard-shortcut layer
    # into the PARENT document (which persists across reruns) and the JS already
    # self-guards on `pp-cmdk-overlay`. Re-running it on every rerun only spawns a
    # fresh throwaway iframe. Inject once per session (re-inject if language flips,
    # since the command labels are localized). (perf-st: idempotent injector)
    try:
        _pl_guard = st.session_state.get("_pp_prod_layer_lang")
        if _pl_guard == language:
            return
        st.session_state["_pp_prod_layer_lang"] = language
    except Exception:
        pass
    is_he = language.startswith("ע")
    cmds = [
        {"id": "nav-dashboard", "label": "דשבורד" if is_he else "Dashboard", "hint": "g d"},
        {"id": "nav-manage",    "label": "ניהול עסקאות" if is_he else "Trade Management", "hint": "g t"},
        {"id": "nav-risk",      "label": "סיכונים ו-FIFO" if is_he else "Risk & FIFO", "hint": "g r"},
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
          // Check the g-buffered "go to page" shortcuts FIRST so g→t doesn't
          // get intercepted by the standalone "t = toggle theme" handler.
          if (gBuffer && (Date.now() - gBuffer) < 900) {{
            gBuffer = 0;
            if (e.key === 'd') {{ runCommand('nav-dashboard'); return; }}
            else if (e.key === 't') {{ runCommand('nav-manage'); return; }}
            else if (e.key === 'r') {{ runCommand('nav-risk'); return; }}
            else if (e.key === 's') {{ runCommand('nav-simulator'); return; }}
            else if (e.key === 'q') {{ runCommand('nav-quality'); return; }}
            // unknown second key → just clear the buffer and fall through
          }}
          if (e.key === '/') {{ e.preventDefault(); runCommand('focus-search'); return; }}
          if (e.key === '?') {{ e.preventDefault(); runCommand('help'); return; }}
          if (e.key === 't') {{ runCommand('toggle-theme'); return; }}
          if (e.key === 'r') {{ runCommand('rerun'); return; }}
          if (e.key === 'f') {{
            if (doc.fullscreenElement) doc.exitFullscreen(); else doc.documentElement.requestFullscreen();
            return;
          }}
          if (e.key === 'g') {{ gBuffer = Date.now(); return; }}
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
    # A non-positive cumulative (total wipeout from a daily return < -1) has no
    # meaningful CAGR and raising it to a fractional power yields NaN. (audit bug-st)
    if cum[-1] <= 0:
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


@st.cache_data(ttl=900, show_spinner=False)
def _mc_projection_cached(start_value: float, daily_returns, horizon_days: int, n_paths: int) -> pd.DataFrame:
    """Memoized Monte Carlo. The simulation is deterministic (fixed seed) and its
    inputs (start value + the cached daily-return series) only change every ~15min,
    so re-running 1000×252 paths on every Risk-page rerun (theme toggle, etc.) was
    pure waste. st.cache_data hashes the return array and reuses the result."""
    return pp_monte_carlo_projection(start_value=start_value, daily_returns=daily_returns,
                                     horizon_days=horizon_days, n_paths=n_paths)


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
            width="stretch",
        )
    with c2:
        st.download_button(
            ("⬇️ הורדה JSON" if is_he else "⬇️ JSON"),
            data=df.to_json(orient="records", force_ascii=False, indent=2).encode("utf-8"),
            file_name=f"{label_prefix}_{ts}.json",
            mime="application/json",
            width="stretch",
        )
    with c3:
        xls = pp_dataframe_to_excel_bytes(df, sheet_name=label_prefix)
        if xls:
            st.download_button(
                ("⬇️ הורדה Excel" if is_he else "⬇️ Excel"),
                data=xls,
                file_name=f"{label_prefix}_{ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width="stretch",
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
        st.plotly_chart(_apply_plotly_theme(gauge_fig, is_dark, is_mobile), theme="streamlit", width="stretch")
    with gc2:
        kpi_row = st.columns(3) if not is_mobile else [st.container(), st.container(), st.container()]
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
        kpi_row2 = st.columns(3) if not is_mobile else [st.container(), st.container(), st.container()]
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

    st.divider()
    st.subheader(tr("📉 Drawdown & Returns", "📉 משיכה ותשואות"))

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
        st.plotly_chart(_apply_plotly_theme(dd_fig, is_dark, is_mobile), theme="streamlit", width="stretch")

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
        st.plotly_chart(_apply_plotly_theme(hist_fig, is_dark, is_mobile), theme="streamlit", width="stretch")

    # Rolling 30-day annualized volatility
    st.divider()
    st.subheader(tr("📊 Volatility & Benchmark", "📊 תנודתיות ובנצ'מרק"))
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
            st.plotly_chart(_apply_plotly_theme(vol_fig, is_dark, is_mobile), theme="streamlit", width="stretch")
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
                st.plotly_chart(_apply_plotly_theme(bm_fig, is_dark, is_mobile), theme="streamlit", width="stretch")
            else:
                st.info(tr("Benchmark history unavailable.", "אין מספיק היסטוריית בנצ'מרק."))
        else:
            st.info(tr("Benchmark (SPY) unavailable — check internet / Yahoo access.",
                       "בנצ'מרק (SPY) לא זמין — בדוק גישה לרשת / Yahoo."))

    # Correlation heatmap between holdings
    if not open_trades.empty and "Ticker" in open_trades.columns:
        held = sorted({_clean(t).upper() for t in open_trades["Ticker"].tolist() if _clean(t)})
        if 2 <= len(held) <= 25:
            st.divider()
            st.subheader(tr("🔗 Correlation Heatmap", "🔗 מפת מתאמים"))
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
                    # Adapt the color range to the ACTUAL off-diagonal data so the
                    # heatmap is readable. A fixed [-1, 1] range washes everything to
                    # one wall of red when (as here) all real correlations sit in
                    # ~0.45..1.0. If all correlations are non-negative use a sequential
                    # Reds ramp (high corr = dark red = "less diversified"); only fall
                    # back to a diverging scale when genuine negative correlations exist.
                    _cv = corr.values.astype(float).copy()
                    np.fill_diagonal(_cv, np.nan)
                    _cmin = float(np.nanmin(_cv)) if np.isfinite(np.nanmin(_cv)) else -1.0
                    if _cmin >= 0:
                        _zmin, _cscale = max(0.0, round(_cmin - 0.05, 2)), "Reds"
                    else:
                        _zmin, _cscale = -1.0, "RdBu_r"
                    heat_fig = px.imshow(
                        corr, color_continuous_scale=_cscale, zmin=_zmin, zmax=1,
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
                                width="stretch",
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
                            width="content",
                            theme="streamlit",
                        )
                        st.markdown("</div>", unsafe_allow_html=True)
            except Exception as _hm_exc:
                st.caption(f"⚠️ Heatmap error: {_hm_exc}")

    # Monte Carlo projection (1 year)
    st.divider()
    st.subheader(tr("🎲 Monte Carlo Projection", "🎲 הדמיית מונטה-קרלו"))
    _chart_help("Monte Carlo simulation: 1,000 random paths based on historical daily returns. Shows the range of possible portfolio values in 1 year.",
                "סימולציית מונטה-קרלו: 1,000 מסלולים אקראיים על בסיס תשואות יומיות היסטוריות. טווח שווי אפשרי לתיק בעוד שנה.", language)
    try:
        mc = _mc_projection_cached(
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
        st.plotly_chart(_apply_plotly_theme(mc_fig, is_dark, is_mobile), theme="streamlit", width="stretch")

        final_row = mc.iloc[-1]
        mc_cols = st.columns(3) if not is_mobile else [st.container(), st.container(), st.container()]
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

# Per-mode input keys. Each mode (`mine` for "My Portfolio", `clean` for
# "Clean") gets its OWN copy of every input below; switching modes never
# overwrites the other.
_SIM_MODE_KEYS = (
    # Horizon + global
    "age_now", "age_target", "swr", "annual_inflation",
    # Financial-independence: desired monthly income (today's ₪) + chosen FI age
    "fi_income", "fi_target_age",
    # Regular taxable portfolio
    "regular_initial", "regular_monthly", "regular_lump", "regular_lump_month",
    "regular_return",
    # Pension fund (tax-exempt at retirement)
    "pension_initial", "pension_monthly", "pension_return",
    "pension_fee_accumulation", "pension_fee_deposits",
    # Education fund (קרן השתלמות) — tax-exempt
    "education_initial", "education_monthly", "education_return",
    "education_fee_accumulation", "education_fee_deposits",
)
# Singletons (not per-mode)
_SIM_GLOBAL_KEYS = ("sim_mode_choice",)


def _sim_key(mode: str, key: str) -> str:
    """Namespaced session-state key. Examples:
        ('mine', 'age_now')        → 'sim_mine_age_now'
        ('clean', 'pension_fee_*') → 'sim_clean_pension_fee_*'"""
    return f"sim_{mode}_{key}"


def _sim_default(key: str) -> object:
    """Default value for a given input key (typed appropriately)."""
    defaults = {
        "age_now": 30, "age_target": 67,
        "swr": 4.0, "annual_inflation": 3.0,
        "fi_income": 12000.0, "fi_target_age": 60,
        "regular_initial": 0.0, "regular_monthly": 2000.0,
        "regular_lump": 0.0, "regular_lump_month": 0,
        "regular_return": 7.0,
        "pension_initial": 0.0, "pension_monthly": 1500.0, "pension_return": 6.0,
        "pension_fee_accumulation": 0.5, "pension_fee_deposits": 1.0,
        "education_initial": 0.0, "education_monthly": 800.0, "education_return": 6.0,
        "education_fee_accumulation": 0.5, "education_fee_deposits": 0.5,
    }
    return defaults.get(key, 0.0)


def _sim_allowed_keys() -> set:
    """The ONLY session_state keys that may be restored from saved prefs /
    localStorage. Excludes widget keys like the reset button (`sim_reset_prefs_*`)
    — pre-setting a button's session_state value raises a Streamlit error."""
    s = set(_SIM_GLOBAL_KEYS)
    for _m in ("mine", "clean"):
        for _k in _SIM_MODE_KEYS:
            s.add(_sim_key(_m, _k))
    return s


def _sim_remote_cfg() -> Tuple[str, str]:
    try:
        s = load_local_settings()
        return _clean(s.get("web_app_url", "")), _clean(s.get("api_token", ""))
    except Exception:
        return "", ""


@st.cache_data(ttl=20, show_spinner=False)
def _remote_sim_prefs_read_cached(url: str, token: str) -> Dict[str, object]:
    if not url:
        return {}
    try:
        r = call_apps_script_(url, {"token": token, "action": "read_sim_prefs"})
        p = r.get("prefs", {}) if isinstance(r, dict) else {}
        return p if isinstance(p, dict) else {}
    except Exception:
        return {}


def _remote_sim_prefs_read() -> Dict[str, object]:
    url, token = _sim_remote_cfg()
    return _remote_sim_prefs_read_cached(url, token)


def _remote_sim_prefs_save(values: Mapping[str, object]) -> None:
    url, token = _sim_remote_cfg()
    if not url:
        return
    try:
        payload = {k: v for k, v in dict(values).items() if v is not None}
        call_apps_script_(url, {"token": token, "action": "save_sim_prefs", "prefs": payload})
        try:
            _remote_sim_prefs_read_cached.clear()  # bust cache so the next read reflects this save
        except Exception:
            pass
    except Exception:
        pass


def load_sim_prefs() -> Dict[str, object]:
    """Load simulator inputs. Merges the LOCAL on-disk cache with the SHARED
    remote store (Apps Script), so a change on one device shows on all others
    (desktop, phone, Next.js). Remote is the cross-device source of truth."""
    local: Dict[str, object] = {}
    if SIM_PREFS_FILE.exists():
        try:
            raw = json.loads(SIM_PREFS_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                local = raw
        except Exception:
            local = {}
    remote = _remote_sim_prefs_read()
    return {**local, **remote}


# Debounce state for the cross-device simulator-prefs push. The simulator calls
# save_sim_prefs() on EVERY rerun (every slider/input change); previously each of
# those did a synchronous Apps Script POST, so interactions waited on a remote
# round-trip. We now push in a daemon thread and only when the snapshot changed.
_SIM_PREFS_PUSH_STATE: Dict[str, object] = {"sig": None}


def save_sim_prefs(values: Mapping[str, object]) -> bool:
    """Persist the simulator input snapshot — ADDITIVE merge so that
    switching modes never overwrites the other mode's saved data.

    If Streamlit's widget GC has cleared the inactive-mode widget keys
    from session_state, the last known values from disk are preserved."""
    try:
        whitelist = set(_SIM_GLOBAL_KEYS)
        for mode in ("mine", "clean"):
            for k in _SIM_MODE_KEYS:
                whitelist.add(_sim_key(mode, k))

        # Load existing file to avoid destroying the OTHER-mode values
        existing: Dict[str, object] = {}
        if SIM_PREFS_FILE.exists():
            try:
                _raw = json.loads(SIM_PREFS_FILE.read_text(encoding="utf-8"))
                if isinstance(_raw, dict):
                    existing = _raw
            except Exception:
                pass

        # Merge: start from what's on disk, overlay only the keys the
        # caller actually provided AND that are non-None
        merged = {k: v for k, v in existing.items() if k in whitelist}
        for k, v in values.items():
            if k in whitelist and v is not None:
                merged[k] = v

        SIM_PREFS_FILE.write_text(
            json.dumps(merged, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        # Push to the shared store so other devices (phone, Next.js) pick it up —
        # in the BACKGROUND and only when the snapshot actually changed, so a
        # slider drag never blocks the rerun on a slow Apps Script round-trip.
        try:
            import threading
            _sig = json.dumps(merged, sort_keys=True, ensure_ascii=False)
            if _sig != _SIM_PREFS_PUSH_STATE.get("sig"):
                _SIM_PREFS_PUSH_STATE["sig"] = _sig
                threading.Thread(
                    target=_remote_sim_prefs_save, args=(merged,), daemon=True
                ).start()
        except Exception:
            pass
        return True
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────
# Financial math: tax + management fees
# ──────────────────────────────────────────────────────────────────
ISRAELI_CAPITAL_GAINS_TAX = 0.25  # 25% on REAL (inflation-adjusted) profits


def _real_capital_gains_tax(
    final_nominal: float,
    initial: float,
    monthly: float,
    months: int,
    lump: float,
    lump_month: int,
    inflation_pct: float,
    years: float,
) -> float:
    """Israeli rule of thumb: 25% on the inflation-adjusted (real) gain.

    Each contribution is indexed only from its OWN deposit date — the initial
    capital (t=0) gets the full horizon, monthly deposit m gets (months-m)/12
    years, and the lump at lump_month gets (months-lump_month)/12 years. The
    previous version indexed the entire basis by the full horizon, overstating
    the indexed basis and under-charging tax for contribution-heavy plans.
    Tax = 25% × max(0, nominal − indexed_basis). Always >= 0."""
    # Sanitize every numeric arg to a finite float so NaN/garbage degrades to 0
    # identically to engine.js (which guards the same way) instead of NaN/crash. (audit fin-tax)
    def _fin(x: object) -> float:
        try:
            v = float(x)
        except (TypeError, ValueError):
            return 0.0
        return v if (v == v and v not in (float("inf"), float("-inf"))) else 0.0
    final_nominal = _fin(final_nominal)
    initial = _fin(initial)
    monthly = _fin(monthly)
    lump = _fin(lump)
    inflation_pct = _fin(inflation_pct)
    years = _fin(years)
    if final_nominal <= 0 or years <= 0:
        return 0.0
    i = max(0.0, inflation_pct) / 100.0
    months = max(0, int(_fin(months)))
    # months=0 with a monthly stream: derive the horizon from years so a documented
    # months=0 caller matches engine.js (which does months = round(years*12)). (audit fin-tax)
    if monthly and months == 0 and years > 0:
        # Half-up (floor(x+0.5)) to match JS Math.round — Python's round() is banker's
        # rounding (round-half-to-even), which would derive a different month at x.5 and
        # diverge from engine.js's months. (audit fin-tax rounding parity)
        months = int(_pp_math.floor(years * 12 + 0.5))
    # Index the initial term over the SAME month-based horizon the projection uses
    # (months/12), not the float `years`, so every basis term references one horizon. (audit fin-tax)
    indexed_basis = max(0.0, float(initial)) * (1.0 + i) ** (months / 12.0)
    if monthly and months > 0:
        # deposit m (1..months): (months-m)/12 yrs of indexing → exact geometric sum
        exps = np.arange(months - 1, -1, -1, dtype=float) / 12.0
        indexed_basis += float(monthly) * float(np.sum((1.0 + i) ** exps))
    if lump:
        lm = max(0, min(int(_fin(lump_month)), months))
        indexed_basis += max(0.0, float(lump)) * (1.0 + i) ** ((months - lm) / 12.0)
    real_gain = max(0.0, final_nominal - indexed_basis)
    return ISRAELI_CAPITAL_GAINS_TAX * real_gain


def sim_project_fund(
    initial: float,
    monthly_contrib: float,
    annual_return_pct: float,
    years: float,
    fee_accumulation_pct: float = 0.0,
    fee_deposits_pct: float = 0.0,
) -> float:
    """Project a tax-exempt fund (Pension or Education) with TWO fee types:
    - `fee_accumulation_pct`: annual % skimmed off the BALANCE (e.g. 0.5%/yr).
    - `fee_deposits_pct`: % skimmed off EACH deposit (e.g. 1% of every payment).
    Returns the gross balance at maturity (no tax — these funds are exempt
    when used for their designated purpose)."""
    try:
        initial = float(initial or 0.0)
        monthly_contrib = float(monthly_contrib or 0.0)
        annual_return_pct = float(annual_return_pct or 0.0)
        years = float(years or 0.0)
        fee_acc = float(fee_accumulation_pct or 0.0)
        fee_dep = float(fee_deposits_pct or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if years <= 0:
        return initial
    # Net annual return after accumulation fee
    net_annual = annual_return_pct - fee_acc
    if 1.0 + net_annual / 100.0 <= 0:
        r_m = -0.999999
    else:
        r_m = (1.0 + net_annual / 100.0) ** (1.0 / 12.0) - 1.0
    # Each deposit is reduced by deposit fee
    effective_monthly = monthly_contrib * (1.0 - fee_dep / 100.0)
    n = max(1, int(round(years * 12)))
    balance = initial
    for _ in range(1, n + 1):
        balance = balance * (1.0 + r_m) + effective_monthly
    return balance


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


@st.cache_data(ttl=600, show_spinner=False)
def sim_monte_carlo_band(initial_capital: float, monthly_contribution: float,
                         annual_return_pct: float, annual_vol_pct: float, years: float,
                         lump_sum: float = 0.0, lump_sum_month: int = 0,
                         n_paths: int = 800, seed: int = 7) -> pd.DataFrame:
    """Probabilistic envelope around the deterministic projection: same contribution
    schedule, but each month's growth is drawn from a normal with the given annual
    mean/vol. Returns per-YEAR p10/p50/p90 balances (with the lump applied)."""
    months = int(round(max(0.0, years) * 12))
    if months <= 0 or n_paths <= 0:
        return pd.DataFrame()
    r_m = (1.0 + annual_return_pct / 100.0) ** (1.0 / 12.0) - 1.0
    vol_m = (annual_vol_pct / 100.0) / (12.0 ** 0.5)
    rng = np.random.default_rng(seed)
    shocks = rng.normal(r_m, max(vol_m, 1e-9), size=(n_paths, months))
    bal = np.full(n_paths, float(initial_capital) + (float(lump_sum) if lump_sum_month == 0 else 0.0))
    yr_idx, p10, p50, p90, yrs = 0, [], [], [], []
    for m in range(1, months + 1):
        bal = bal * (1.0 + shocks[:, m - 1]) + float(monthly_contribution)
        if m == int(lump_sum_month) and lump_sum_month > 0:
            bal = bal + float(lump_sum)
        if m % 12 == 0 or m == months:
            p10.append(float(np.percentile(bal, 10)))
            p50.append(float(np.percentile(bal, 50)))
            p90.append(float(np.percentile(bal, 90)))
            yrs.append(m / 12.0)
    return pd.DataFrame({"year": yrs, "p10": p10, "p50": p50, "p90": p90})


@st.cache_data(ttl=600, show_spinner=False)
def sim_goal_success_prob(initial_capital: float, monthly_contribution: float,
                          annual_return_pct: float, annual_vol_pct: float, years: float,
                          target: float, fixed_addition: float = 0.0,
                          lump_sum: float = 0.0, lump_sum_month: int = 0,
                          n_paths: int = 2000, seed: int = 11):
    """Probability that (Monte-Carlo regular bucket final + a deterministic addition,
    e.g. pension+education funds) reaches the target nest egg. Returns None if N/A."""
    months = int(round(max(0.0, years) * 12))
    if months <= 0 or target <= 0:
        return None
    r_m = (1.0 + annual_return_pct / 100.0) ** (1.0 / 12.0) - 1.0
    vol_m = (annual_vol_pct / 100.0) / (12.0 ** 0.5)
    rng = np.random.default_rng(seed)
    shocks = rng.normal(r_m, max(vol_m, 1e-9), size=(n_paths, months))
    bal = np.full(n_paths, float(initial_capital) + (float(lump_sum) if lump_sum_month == 0 else 0.0))
    for m in range(1, months + 1):
        bal = bal * (1.0 + shocks[:, m - 1]) + float(monthly_contribution)
        if m == int(lump_sum_month) and lump_sum_month > 0:
            bal = bal + float(lump_sum)
    return float(np.mean((bal + float(fixed_addition)) >= float(target)))


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


def sim_required_monthly_for_fi(
    years: float,
    annual_return_pct: float,
    portfolio_initial: float,
    desired_monthly_income_today: float,
    swr_pct: float,
    inflation_pct: float,
    pension_final: float = 0.0,
    education_final: float = 0.0,
    lump_sum: float = 0.0,
    lump_sum_month: int = 0,
) -> Dict[str, float]:
    """How much must you save MONTHLY (into the taxable portfolio) to reach
    financial independence by `years` from now?

    FI = a nest egg whose Safe-Withdrawal-Rate withdrawal covers the desired
    monthly income. The work pension + education funds' projected balances count
    toward that nest egg, so only the REMAINING gap must come from monthly saving.

    Returns a dict: required_monthly, required_egg_nominal, required_egg_today,
    funds_cover (pension+education at target), gap, feasible (bool as 1.0/0.0).
    """
    try:
        years = max(0.0, float(years)); swr = float(swr_pct)
        income_today = max(0.0, float(desired_monthly_income_today))
        infl = max(0.0, float(inflation_pct)) / 100.0
        p0 = max(0.0, float(portfolio_initial))
        funds = max(0.0, float(pension_final)) + max(0.0, float(education_final))
    except (TypeError, ValueError):
        return {"required_monthly": float("inf"), "required_egg_nominal": 0.0,
                "required_egg_today": 0.0, "funds_cover": 0.0, "gap": 0.0, "feasible": 0.0}
    if swr <= 0:
        return {"required_monthly": float("inf"), "required_egg_nominal": 0.0,
                "required_egg_today": 0.0, "funds_cover": funds, "gap": 0.0, "feasible": 0.0}

    egg_today = (income_today * 12.0) / (swr / 100.0)                 # nest egg in today's ₪
    egg_nominal = egg_today * ((1.0 + infl) ** years)                # inflated to the target date
    gap = egg_nominal - funds                                        # what the taxable portfolio must reach

    def _egg(mc: float) -> float:
        df = sim_project_portfolio(p0, mc, annual_return_pct, years, lump_sum, lump_sum_month)
        return float(df["balance_with_lump"].iloc[-1])

    if gap <= 0 or _egg(0.0) >= gap:
        return {"required_monthly": 0.0, "required_egg_nominal": egg_nominal,
                "required_egg_today": egg_today, "funds_cover": funds, "gap": max(0.0, gap), "feasible": 1.0}

    lo, hi = 0.0, 100.0
    for _ in range(60):
        if _egg(hi) >= gap:
            break
        hi *= 2.0
        if hi > 5e8:
            return {"required_monthly": float("inf"), "required_egg_nominal": egg_nominal,
                    "required_egg_today": egg_today, "funds_cover": funds, "gap": gap, "feasible": 0.0}
    for _ in range(80):
        mid = (lo + hi) / 2.0
        if _egg(mid) >= gap:
            hi = mid
        else:
            lo = mid
    return {"required_monthly": hi, "required_egg_nominal": egg_nominal,
            "required_egg_today": egg_today, "funds_cover": funds, "gap": gap, "feasible": 1.0}


def render_simulator_page(
    tr,
    total_value: float,
    language: str,
    is_dark: bool,
    is_mobile: bool,
) -> None:
    """Render the Simulator page.

    Two modes — each with FULLY INDEPENDENT state (switching modes never
    overwrites the other's data):
      • 💼 My Portfolio Simulator — Regular-portfolio Initial Capital is
        locked to the live total_value of your real portfolio.
      • 🧼 Clean Simulator — every input is user-editable from scratch.

    Each mode has THREE sub-portfolios that are projected independently:
      1. Regular taxable portfolio   → 25% capital-gains tax on REAL
                                       (inflation-adjusted) profits
      2. Pension fund (קרן פנסיה)    → tax-EXEMPT (designated retirement use)
      3. Education fund (קרן השתלמות)→ tax-EXEMPT after vesting period

    Funds (Pension / Education) factor in TWO management-fee types:
      - Fee on accumulation: annual % skimmed off the running balance
      - Fee on deposits: % skimmed off every monthly contribution

    All KPI cards (Total Value · Real Value · Monthly SWR Allowance) show
    NET values after every tax + fee deduction.

    Inputs persist to disk (sim_user_prefs.json) keyed by mode, so closing
    the browser, refreshing, or even rebooting the laptop preserves
    everything. Switching mode never touches the OTHER mode's saved data.
    """
    st.markdown(f"### 🧮 {tr('Simulator', 'סימולטור')}")
    st.caption(tr(
        "Long-term projection: regular taxable portfolio (25% tax on real "
        "gains) + tax-exempt Pension and Education funds. Each mode keeps "
        "its own inputs — switching never overwrites the other side. All "
        "results show NET values after taxes and management fees.",
        "תחזית ארוכת-טווח: תיק רגיל חייב במס (25% על רווח ריאלי) + קרן "
        "פנסיה וקרן השתלמות פטורות ממס. כל מצב שומר את הקלטים שלו בנפרד — "
        "מעבר בין מצבים לעולם לא מוחק את השני. כל התוצאות הן ערכי NET לאחר "
        "מסים ודמי-ניהול.",
    ))

    # ── Browser LocalStorage (device-local, mobile-compatible) ──────────
    # Uses streamlit-local-storage which sends data via Streamlit's own
    # bidirectional component messaging — no window.parent access needed,
    # works on iOS Safari and Android Chrome.
    _sim_ls: object = None
    try:
        from streamlit_local_storage import LocalStorage as _LSClass
        _sim_ls = _LSClass(key="_sim_prefs_ls")
    except Exception:
        _sim_ls = None

    # ── Hydrate persisted inputs ────────────────────────────────────────
    # Strategy:
    #   1. On EVERY rerun we attempt to re-hydrate from localStorage. The
    #      component may return None on the very first render (it loads
    #      asynchronously), so retrying every rerun catches the late-arrival
    #      case which previously produced "values reset" complaints.
    #   2. Hydration is ADDITIVE — keys already present in session_state win.
    #      So after the user edits an input, that value is what gets saved
    #      back via save_sim_prefs / setItem on the same rerun, and
    #      re-hydration on the next rerun reads the same value.
    #   3. Disk fallback runs ONCE per session (server-side; shared between
    #      sessions on the same machine).
    def _hydrate_from_ls() -> None:
        if _sim_ls is None:
            return
        try:
            _ls_raw = _sim_ls.getItem("sim_prefs")
            if not _ls_raw:
                return
            _ls_dict = json.loads(_ls_raw) if isinstance(_ls_raw, str) else _ls_raw
            if not isinstance(_ls_dict, dict):
                return
            _allow = _sim_allowed_keys()
            for _k, _v in _ls_dict.items():
                if _k in _allow and _k not in st.session_state and _v is not None:
                    st.session_state[_k] = _v
        except Exception:
            pass

    # Always retry localStorage every rerun (it may have just initialized)
    _hydrate_from_ls()

    # First-visit-only: hydrate from server-side disk fallback
    if not st.session_state.get("_sim_prefs_hydrated", False):
        try:
            _prefs = load_sim_prefs()
            _allow = _sim_allowed_keys()
            for _k, _v in _prefs.items():
                if _k in _allow and _k not in st.session_state and _v is not None:
                    st.session_state[_k] = _v
        except Exception:
            pass
        st.session_state["_sim_prefs_hydrated"] = True

    # ── Mode selector (independent state per mode) ─────────────────────
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
        tr("Mode", "מצב"), mode_options, horizontal=True, key="sim_mode_choice",
    )
    use_portfolio = (mode == mode_mine) and has_portfolio
    mode_id = "mine" if (mode == mode_mine) else "clean"

    # ── Per-mode re-hydration ──────────────────────────────────────────
    # Streamlit ≥1.28 clears widget-key state when widgets are no longer
    # rendered. Switching from Mine→Clean removes sim_mine_* from session_state.
    # When the user switches back we must reload those values from disk
    # before _need() falls back to defaults.
    _sentinel_keys = ("regular_initial", "regular_monthly", "regular_return", "age_now")
    _mode_keys_missing = any(
        _sim_key(mode_id, k) not in st.session_state for k in _sentinel_keys
    )
    if _mode_keys_missing:
        try:
            _disk_prefs = load_sim_prefs()
            _allow = _sim_allowed_keys()
            for _k, _v in _disk_prefs.items():
                if _k in _allow and _k not in st.session_state and _v is not None:
                    st.session_state[_k] = _v
        except Exception:
            pass

    # Defensive type-coercion for THIS mode's keys.
    _SIM_TYPES: Dict[str, type] = {
        "age_now": int, "age_target": int, "regular_lump_month": int, "fi_target_age": int,
    }
    for k in _SIM_MODE_KEYS:
        full = _sim_key(mode_id, k)
        if full in st.session_state:
            target_type = _SIM_TYPES.get(k, float)
            try:
                if target_type is int:
                    st.session_state[full] = int(float(st.session_state[full]) if st.session_state[full] is not None else _sim_default(k))
                else:
                    st.session_state[full] = float(st.session_state[full] if st.session_state[full] is not None else _sim_default(k))
            except (TypeError, ValueError):
                st.session_state[full] = _sim_default(k)

    # Helper: ensure key exists in session_state with a typed default.
    def _need(key: str):
        full = _sim_key(mode_id, key)
        if full not in st.session_state:
            st.session_state[full] = _sim_default(key)
        return full

    # ── Inputs ─────────────────────────────────────────────────────────
    with st.container(border=True):
        st.markdown(f"**{tr('Horizon & global', 'אופק וזמן')}**")
        col_l, col_r = (st.columns(2) if not is_mobile else (st.container(), st.container()))
        with col_l:
            k_age_now = _need("age_now")
            age_now = st.number_input(
                tr("Your current age", "הגיל הנוכחי שלך"),
                min_value=10, max_value=90, step=1, key=k_age_now,
            )
            min_target = int(age_now) + 1
            k_age_t = _need("age_target")
            if int(st.session_state[k_age_t]) < min_target:
                st.session_state[k_age_t] = min_target
            age_target = st.number_input(
                tr("Target retirement age", "גיל פרישה מתוכנן"),
                min_value=min_target, max_value=100, step=1, key=k_age_t,
            )
        with col_r:
            k_swr = _need("swr")
            swr_pct = float(st.slider(
                tr("Safe Withdrawal Rate (SWR %)", "שיעור משיכה בטוח (SWR %)"),
                min_value=3.0, max_value=5.0, step=0.25, key=k_swr,
                help=tr("Trinity-study rule. 4% is the classic default.",
                        "כלל אצבע ממחקר Trinity. 4% הוא ברירת-המחדל."),
            ))
            k_infl = _need("annual_inflation")
            inflation_pct = float(st.number_input(
                tr("Annual inflation (%)", "אינפלציה שנתית (%)"),
                min_value=0.0, max_value=20.0, step=0.25, key=k_infl,
                help=tr("Used for REAL value + 25% real-gains tax basis.",
                        "משמש לחישוב שווי ריאלי ובסיס מס רווח-הון ריאלי."),
            ))

        years_total = max(1.0, float(age_target) - float(age_now))
        # Half-up to match JS Math.round (engine.js), not Python's banker's round(). (audit)
        months_total = int(_pp_math.floor(years_total * 12 + 0.5))

    # ── Regular (TAXABLE) portfolio ─────────────────────────────────────
    with st.container(border=True):
        st.markdown(
            f"**📈 {tr('Regular portfolio (25% tax on real gains)', 'תיק רגיל (מס 25% על רווח ריאלי)')}**"
        )
        col_l, col_r = (st.columns(2) if not is_mobile else (st.container(), st.container()))
        with col_l:
            if use_portfolio:
                # Lock initial capital to live portfolio value
                st.session_state[_sim_key(mode_id, "regular_initial")] = float(round(tv, 2))
                regular_initial = float(tv)
                st.number_input(
                    tr("Initial capital (₪) — locked to your portfolio",
                       "הון התחלתי (₪) — נעול לשווי התיק שלך"),
                    step=100.0, disabled=True,
                    key=_sim_key(mode_id, "regular_initial"),
                )
                st.info(tr(
                    f"Seeded from your portfolio: ₪{tv:,.0f}",
                    f"הוזן משווי התיק שלך: ₪{tv:,.0f}",
                ))
            else:
                k = _need("regular_initial")
                regular_initial = float(st.number_input(
                    tr("Initial capital (₪)", "הון התחלתי (₪)"),
                    min_value=0.0, step=1000.0, key=k,
                ))
            k = _need("regular_monthly")
            regular_monthly = float(st.number_input(
                tr("Monthly contribution (₪)", "הפקדה חודשית (₪)"),
                min_value=0.0, step=100.0, key=k,
            ))
        with col_r:
            k = _need("regular_return")
            regular_return = float(st.number_input(
                tr("Expected annual return (%)", "תשואה שנתית צפויה (%)"),
                min_value=-20.0, max_value=40.0, step=0.25, key=k,
                help=tr("S&P 500 long-run ≈ 7% real / 10% nominal.",
                        "ממוצע רב-שנתי של S&P 500: ≈7% ריאלי / 10% נומינלי."),
            ))
            regular_vol = float(st.number_input(
                tr("Expected volatility (% / yr)", "תנודתיות צפויה (% לשנה)"),
                min_value=0.0, max_value=60.0, value=15.0, step=1.0, key="sim_vol_pct_widget",
                help=tr("Annual standard deviation used for the probability band around the "
                        "projection. Diversified equity ≈ 15%; all-stock ≈ 18%; crypto-heavy 40%+.",
                        "סטיית התקן השנתית לחישוב רצועת ההסתברות סביב התחזית. תיק מניות מפוזר ≈15%; "
                        "מנייתי לגמרי ≈18%; כבד-קריפטו 40%+."),
            ))
            k = _need("regular_lump")
            regular_lump = float(st.number_input(
                tr("One-time lump sum (₪)", "הפקדה חד-פעמית (₪)"),
                min_value=0.0, step=1000.0, key=k,
            ))
            k = _need("regular_lump_month")
            slider_max = max(1, months_total)
            if int(st.session_state[k]) > slider_max:
                st.session_state[k] = slider_max
            elif int(st.session_state[k]) < 0:
                st.session_state[k] = 0
            regular_lump_month = int(st.slider(
                tr("Lump-sum timing (months)", "תזמון ההפקדה (חודשים)"),
                min_value=0, max_value=slider_max, step=1, key=k,
                disabled=(regular_lump <= 0.0),
            ))

    # ── Pension fund (TAX-EXEMPT) ───────────────────────────────────────
    with st.expander(f"🏦 {tr('Pension fund — tax-exempt', 'קרן פנסיה — פטורה ממס')}", expanded=False):
        col_l, col_r = (st.columns(2) if not is_mobile else (st.container(), st.container()))
        with col_l:
            k = _need("pension_initial")
            pension_initial = float(st.number_input(
                tr("Initial amount (₪)", "סכום התחלתי (₪)"),
                min_value=0.0, step=1000.0, key=k,
            ))
            k = _need("pension_monthly")
            pension_monthly = float(st.number_input(
                tr("Monthly contribution (₪)", "הפקדה חודשית (₪)"),
                min_value=0.0, step=100.0, key=k,
                help=tr("Combined employer + employee + severance.",
                        "סך הפקדות עובד + מעסיק + פיצויים."),
            ))
            k = _need("pension_return")
            pension_return = float(st.number_input(
                tr("Expected annual return (%)", "תשואה שנתית צפויה (%)"),
                min_value=-20.0, max_value=40.0, step=0.25, key=k,
            ))
        with col_r:
            k = _need("pension_fee_accumulation")
            pension_fee_acc = float(st.number_input(
                tr("Mgmt fee — accumulation (%/yr)",
                   "דמי-ניהול מצבירה (%/שנה)"),
                min_value=0.0, max_value=5.0, step=0.05, key=k,
                help=tr("Annual % skimmed off the balance.",
                        "אחוז שנתי שגובה הקרן מסך הצבירה."),
            ))
            k = _need("pension_fee_deposits")
            pension_fee_dep = float(st.number_input(
                tr("Mgmt fee — deposits (%)",
                   "דמי-ניהול מהפקדות (%)"),
                min_value=0.0, max_value=10.0, step=0.05, key=k,
                help=tr("% skimmed off each monthly contribution.",
                        "אחוז שנגרע מכל הפקדה חודשית."),
            ))

    # ── Education fund (TAX-EXEMPT after 6 yr vest) ─────────────────────
    with st.expander(f"🎓 {tr('Education fund — tax-exempt', 'קרן השתלמות — פטורה ממס')}", expanded=False):
        col_l, col_r = (st.columns(2) if not is_mobile else (st.container(), st.container()))
        with col_l:
            k = _need("education_initial")
            education_initial = float(st.number_input(
                tr("Initial amount (₪)", "סכום התחלתי (₪)"),
                min_value=0.0, step=1000.0, key=k,
            ))
            k = _need("education_monthly")
            education_monthly = float(st.number_input(
                tr("Monthly contribution (₪)", "הפקדה חודשית (₪)"),
                min_value=0.0, step=100.0, key=k,
            ))
            k = _need("education_return")
            education_return = float(st.number_input(
                tr("Expected annual return (%)", "תשואה שנתית צפויה (%)"),
                min_value=-20.0, max_value=40.0, step=0.25, key=k,
            ))
        with col_r:
            k = _need("education_fee_accumulation")
            education_fee_acc = float(st.number_input(
                tr("Mgmt fee — accumulation (%/yr)",
                   "דמי-ניהול מצבירה (%/שנה)"),
                min_value=0.0, max_value=5.0, step=0.05, key=k,
            ))
            k = _need("education_fee_deposits")
            education_fee_dep = float(st.number_input(
                tr("Mgmt fee — deposits (%)",
                   "דמי-ניהול מהפקדות (%)"),
                min_value=0.0, max_value=10.0, step=0.05, key=k,
            ))

    # ── Projections ────────────────────────────────────────────────────
    df_reg = sim_project_portfolio(
        initial_capital=regular_initial,
        monthly_contribution=regular_monthly,
        annual_return_pct=regular_return,
        years=years_total,
        lump_sum=regular_lump,
        lump_sum_month=regular_lump_month,
    )
    reg_final_gross = float(df_reg.iloc[-1]["balance_with_lump"])
    reg_total_contributed = (
        regular_initial + regular_monthly * months_total + regular_lump
    )
    reg_tax = _real_capital_gains_tax(
        final_nominal=reg_final_gross,
        initial=regular_initial,
        monthly=regular_monthly,
        months=months_total,
        lump=regular_lump,
        lump_month=regular_lump_month,
        inflation_pct=inflation_pct,
        years=years_total,
    )
    reg_final_net = reg_final_gross - reg_tax

    pension_final = sim_project_fund(
        initial=pension_initial,
        monthly_contrib=pension_monthly,
        annual_return_pct=pension_return,
        years=years_total,
        fee_accumulation_pct=pension_fee_acc,
        fee_deposits_pct=pension_fee_dep,
    )
    education_final = sim_project_fund(
        initial=education_initial,
        monthly_contrib=education_monthly,
        annual_return_pct=education_return,
        years=years_total,
        fee_accumulation_pct=education_fee_acc,
        fee_deposits_pct=education_fee_dep,
    )

    total_final_net = reg_final_net + pension_final + education_final
    inflation_factor = (1.0 + inflation_pct / 100.0) ** float(years_total) if inflation_pct > 0 else 1.0
    total_real = total_final_net / inflation_factor
    monthly_pension = sim_safe_withdrawal_monthly(total_final_net, swr_pct)
    annual_pension = monthly_pension * 12.0

    total_contributed_all = (
        reg_total_contributed
        + (pension_initial + pension_monthly * months_total)
        + (education_initial + education_monthly * months_total)
    )

    # Persist on every rerun — only keys currently in session to avoid
    # overwriting cleaned-up inactive-mode values with None.
    # save_sim_prefs merges with existing disk file so the other mode's
    # values are never lost (see additive merge in save_sim_prefs).
    _sim_prefs_snapshot: Dict[str, object] = {}
    try:
        _sim_prefs_snapshot = {
            k: v for k, v in
            {k: st.session_state.get(k) for k in list(st.session_state.keys()) if k in _sim_allowed_keys()}.items()
            if v is not None
        }
        save_sim_prefs(_sim_prefs_snapshot)
    except Exception:
        pass
    # Also persist to browser localStorage — use the fully-merged disk
    # content so mobile/desktop each store the complete state.
    # Also save to browser localStorage via streamlit-local-storage
    # (device-local; uses Streamlit's bidirectional component messaging
    # → works on iOS Safari / Android Chrome without iframe restrictions)
    if _sim_ls is not None:
        try:
            _ls_json_str = json.dumps(_sim_prefs_snapshot, ensure_ascii=False, default=str)
            # Use a content-hash suffix so Streamlit re-renders when value changes
            _ls_save_key = f"_sim_ls_save_{abs(hash(_ls_json_str)) % 1_000_000}"
            _sim_ls.setItem("sim_prefs", _ls_json_str, key=_ls_save_key)
        except Exception:
            pass

    # ── KPI row — NET totals (after tax + fees) ────────────────────────
    st.markdown(f"### 💰 {tr('Net results (after tax + fees)', 'תוצאות נטו (אחרי מס ודמי-ניהול)')}")
    if is_mobile:
        k1, k2 = st.columns(2)
        k3, k4 = st.columns(2)
    else:
        k1, k2, k3, k4 = st.columns(4)
    _real_delta_total = (
        f"≈ ₪{total_real:,.0f} " + tr("real", "ריאלי")
    ) if inflation_pct > 0 else None
    k1.metric(
        tr("Total Value (NET)", "שווי כולל (NET)"),
        f"₪{total_final_net:,.0f}",
        delta=_real_delta_total,
        help=tr(
            "Sum of regular portfolio (after 25% tax on real gains) + pension fund + education fund (both tax-exempt).",
            "סך התיק הרגיל (אחרי מס 25% על רווח ריאלי) + קרן פנסיה + קרן השתלמות (פטורות ממס).",
        ),
    )
    k2.metric(
        tr("Real Value (today's ₪)", "שווי ריאלי (₪ של היום)"),
        f"₪{total_real:,.0f}",
        help=tr(
            "Total Value deflated by your inflation rate — what your future self will actually feel.",
            "השווי הכולל מהוון לפי האינפלציה — כוח הקנייה האמיתי בעתיד.",
        ),
    )
    _monthly_real = sim_safe_withdrawal_monthly(total_real, swr_pct)
    k3.metric(
        tr("Monthly pension (NET)", "קצבה חודשית (NET)"),
        f"₪{monthly_pension:,.0f}",
        # Green delta is ALWAYS the inflation-adjusted MONTHLY value (per
        # user spec — "monthly real, not yearly"). When inflation = 0 the
        # real value equals the nominal value (still meaningful).
        delta=(
            f"₪{_monthly_real:,.0f} "
            + tr("real/month (inflation-adj.)", "ריאלי/חודש (מותאם אינפלציה)")
        ),
        help=tr(
            "Nominal monthly withdrawal at your chosen SWR% (on NET total). "
            "The green delta shows the real purchasing-power equivalent today, monthly.",
            "משיכה חודשית נומינלית לפי שיעור SWR שנבחר (על השווי הנקי). "
            "הדלתא הירוקה מציגה את הערך הריאלי החודשי – כוח הקנייה בערכי היום.",
        ),
    )
    k4.metric(
        tr("Total contributed", "סך תרומות"),
        f"₪{total_contributed_all:,.0f}",
        help=tr(
            "Sum of every initial deposit + monthly contributions + lump sum across all 3 buckets.",
            "סכום כל ההפקדות (התחלתי + חודשיות + חד-פעמית) בכל שלוש הקופות.",
        ),
    )

    # ── Financial-independence calculator ──────────────────────────────
    with st.container(border=True):
        st.markdown(f"### 🎯 {tr('Financial Independence — how much to save', 'עצמאות כלכלית — כמה צריך לחסוך')}")
        _fi_help = tr(
            "How much you must save MONTHLY into the regular portfolio to reach financial "
            "independence by your target age — counting your current portfolio AND the "
            "projected pension + education funds. FI = a nest egg whose safe-withdrawal-rate "
            "covers your desired monthly income.",
            "כמה צריך לחסוך בחודש בתיק הרגיל כדי להגיע לעצמאות כלכלית עד גיל היעד — בהתחשב "
            "בתיק הנוכחי וגם בקרן הפנסיה וקרן ההשתלמות הצפויות. עצמאות כלכלית = קרן שמשיכה "
            "בטוחה ממנה (SWR) מכסה את ההכנסה החודשית הרצויה.")
        _age_now_i = int(st.session_state.get(_sim_key(mode_id, "age_now")) or 30)
        _fi_a, _fi_b = (st.columns(2) if not is_mobile else (st.container(), st.container()))
        with _fi_a:
            k_fa = _need("fi_target_age")
            # keep the saved value within the valid range for this current age
            if int(st.session_state.get(k_fa) or 60) <= _age_now_i:
                st.session_state[k_fa] = min(100, _age_now_i + 30)
            fi_age = int(st.number_input(
                tr("Reach financial independence by age", "להגיע לעצמאות כלכלית עד גיל"),
                min_value=_age_now_i + 1, max_value=100, step=1, key=k_fa,
                help=tr("Choose the age by which you want to be financially independent.",
                        "בחר את הגיל שעד אליו אתה רוצה להגיע לעצמאות כלכלית."),
            ))
        with _fi_b:
            k_fi = _need("fi_income")
            fi_income = float(st.number_input(
                tr("Desired monthly income then (today's ₪)",
                   "הכנסה חודשית רצויה אז (₪ של היום)"),
                min_value=0.0, step=500.0, key=k_fi, help=_fi_help,
            ))

        # Compute the required monthly saving for ANY target age — re-projecting
        # the pension + education funds to that age (they grow with the horizon).
        def _fi_for_age(age):
            yrs = max(0.0, float(age) - float(_age_now_i))
            pf = sim_project_fund(pension_initial, pension_monthly, pension_return, yrs, pension_fee_acc, pension_fee_dep)
            ef = sim_project_fund(education_initial, education_monthly, education_return, yrs, education_fee_acc, education_fee_dep)
            res = sim_required_monthly_for_fi(
                years=yrs, annual_return_pct=regular_return, portfolio_initial=regular_initial,
                desired_monthly_income_today=fi_income, swr_pct=swr_pct, inflation_pct=inflation_pct,
                pension_final=pf, education_final=ef, lump_sum=regular_lump, lump_sum_month=regular_lump_month,
            )
            return yrs, res

        _yrs_fi, fi = _fi_for_age(fi_age)
        req = fi["required_monthly"]

        if not fi["feasible"]:
            st.warning(tr(
                f"Reaching FI by age {fi_age} isn't achievable with realistic saving — "
                "raise the age, lower the desired income, or raise expected return.",
                f"לא ניתן להגיע לעצמאות כלכלית עד גיל {fi_age} בחיסכון סביר — "
                "העלה את הגיל, הורד את ההכנסה הרצויה, או הגדל את התשואה הצפויה."))
        else:
            g1, g2, g3 = st.columns(3) if not is_mobile else [st.container(), st.container(), st.container()]
            g1.metric(tr("Required monthly saving", "להפריש בחודש"), f"₪{req:,.0f}",
                      help=tr(f"Into the regular portfolio, for {int(_yrs_fi)} years.",
                              f"לתיק הרגיל, למשך {int(_yrs_fi)} שנים."))
            g2.metric(tr("Required nest egg (at that age)", "קרן נדרשת (באותו גיל)"),
                      f"₪{fi['required_egg_nominal']:,.0f}",
                      help=tr(f"In today's ₪: ₪{fi['required_egg_today']:,.0f}",
                              f"בערכי היום: ₪{fi['required_egg_today']:,.0f}"))
            _gap_plan = req - float(regular_monthly)
            if _gap_plan <= 0:
                g3.metric(tr("Your current plan", "התוכנית הנוכחית"), f"₪{regular_monthly:,.0f}",
                          delta=tr("On track ✅", "בדרך הנכונה ✅"))
            else:
                g3.metric(tr("Your current plan", "התוכנית הנוכחית"), f"₪{regular_monthly:,.0f}",
                          delta=(f"₪{_gap_plan:,.0f} " + tr("short/mo", "חסר לחודש")),
                          delta_color="inverse")
            st.caption(tr(
                f"To be financially independent by age {fi_age} ({int(_yrs_fi)} yrs), save "
                f"≈ ₪{req:,.0f}/month; pension + education funds contribute ₪{fi['funds_cover']:,.0f} "
                f"toward the required ₪{fi['required_egg_nominal']:,.0f}.",
                f"כדי להיות עצמאי כלכלית עד גיל {fi_age} (בעוד {int(_yrs_fi)} שנים), הפרש "
                f"כ-₪{req:,.0f} לחודש; קרן הפנסיה וההשתלמות תורמות ₪{fi['funds_cover']:,.0f} "
                f"מתוך ה-₪{fi['required_egg_nominal']:,.0f} הנדרשים."))

            # Monte-Carlo odds of hitting the goal on the CURRENT plan (current
            # monthly saving + expected return/vol), counting the deterministic
            # pension+education funds at that age. Answers "will I actually make it?".
            try:
                _pf_fi = sim_project_fund(pension_initial, pension_monthly, pension_return, _yrs_fi, pension_fee_acc, pension_fee_dep)
                _ef_fi = sim_project_fund(education_initial, education_monthly, education_return, _yrs_fi, education_fee_acc, education_fee_dep)
                _prob = sim_goal_success_prob(
                    regular_initial, regular_monthly, regular_return, regular_vol, _yrs_fi,
                    float(fi["required_egg_nominal"]), fixed_addition=_pf_fi + _ef_fi,
                    lump_sum=regular_lump, lump_sum_month=regular_lump_month)
            except Exception:
                _prob = None
            if _prob is not None:
                _pct = _prob * 100.0
                _emoji = "🟢" if _pct >= 75 else ("🟡" if _pct >= 50 else "🔴")
                st.progress(min(1.0, max(0.0, _prob)),
                            text=tr(f"{_emoji} Probability of reaching the goal on your current plan: {_pct:.0f}%",
                                    f"{_emoji} סיכוי להגיע ליעד בתוכנית הנוכחית: {_pct:.0f}%"))
                st.caption(tr(
                    f"Across 2,000 simulated market paths (return {regular_return:.1f}% ± {regular_vol:.0f}% vol), "
                    f"saving ₪{regular_monthly:,.0f}/mo, including pension + education funds.",
                    f"מתוך 2,000 מסלולי שוק מדומים (תשואה {regular_return:.1f}% ± {regular_vol:.0f}% תנודתיות), "
                    f"בהפקדה ₪{regular_monthly:,.0f} לחודש, כולל קרנות פנסיה והשתלמות."))

        # Sensitivity: required monthly saving across several target ages.
        _cand = sorted({a for a in [_age_now_i + 5, 50, 55, 60, 65, 67, 70, fi_age] if a > _age_now_i})
        _rows = []
        for a in _cand:
            _, _f = _fi_for_age(a)
            _rows.append({
                tr("Target age", "גיל יעד"): a,
                tr("Years", "שנים"): int(a - _age_now_i),
                tr("Monthly saving", "חיסכון חודשי"): (tr("not reachable", "לא בר-השגה") if not _f["feasible"] else f"₪{_f['required_monthly']:,.0f}"),
                tr("Required nest egg", "קרן נדרשת"): f"₪{_f['required_egg_nominal']:,.0f}",
            })
        st.caption(tr("How much to save per month, by the age you want FI:",
                      "כמה להפריש בחודש, לפי הגיל שבו תרצה עצמאות כלכלית:"))
        st.dataframe(pd.DataFrame(_rows), hide_index=True, width="stretch")

    # ── Per-bucket breakdown ───────────────────────────────────────────
    with st.expander(tr("📊 Per-bucket breakdown", "📊 פירוט לפי קופה"), expanded=False):
        b1, b2, b3 = st.columns(3) if not is_mobile else [st.container(), st.container(), st.container()]
        b1.metric(
            tr("Regular (gross)", "רגיל (לפני מס)"),
            f"₪{reg_final_gross:,.0f}",
            delta=(f"-₪{reg_tax:,.0f} " + tr("tax", "מס")) if reg_tax > 0 else None,
            delta_color="inverse" if reg_tax > 0 else "off",
            help=tr(
                f"25% of inflation-adjusted gain. Tax = ₪{reg_tax:,.0f}. NET = ₪{reg_final_net:,.0f}.",
                f"25% מהרווח הריאלי. מס = ₪{reg_tax:,.0f}. NET = ₪{reg_final_net:,.0f}.",
            ),
        )
        b2.metric(
            tr("Pension fund", "קרן פנסיה"),
            f"₪{pension_final:,.0f}",
            help=tr(
                "Tax-exempt at retirement. Both fees applied during accumulation.",
                "פטורה ממס בפרישה. שני סוגי דמי-ניהול נלקחים בחשבון לאורך הצבירה.",
            ),
        )
        b3.metric(
            tr("Education fund", "קרן השתלמות"),
            f"₪{education_final:,.0f}",
            help=tr(
                "Tax-exempt after 6-yr vesting. Both fees applied during accumulation.",
                "פטורה ממס לאחר 6 שנות וותק. שני סוגי דמי-ניהול נלקחים בחשבון.",
            ),
        )

    # ── Projection chart (regular bucket only — funds are point-in-time) ─
    chart_height = 340 if is_mobile else 420
    fig = go.Figure()
    # Probabilistic envelope (p10–p90) so the projection reads as a RANGE of likely
    # outcomes, not a single deterministic promise. Drawn first → sits behind lines.
    try:
        _mc_band = sim_monte_carlo_band(regular_initial, regular_monthly, regular_return,
                                        regular_vol, years_total, regular_lump, regular_lump_month)
    except Exception:
        _mc_band = pd.DataFrame()
    if not _mc_band.empty:
        fig.add_trace(go.Scatter(x=_mc_band["year"], y=_mc_band["p90"], mode="lines",
                                 line=dict(width=0), showlegend=False, hoverinfo="skip"))
        fig.add_trace(go.Scatter(
            x=_mc_band["year"], y=_mc_band["p10"], mode="lines",
            name=tr("Likely range (10–90%)", "טווח סביר (10–90%)"),
            line=dict(width=0), fill="tonexty", fillcolor="rgba(99,102,241,0.14)",
            hovertemplate="₪%{y:,.0f}<extra>P10</extra>"))
        fig.add_trace(go.Scatter(
            x=_mc_band["year"], y=_mc_band["p50"], mode="lines",
            name=tr("Median outcome", "תרחיש חציוני"),
            line=dict(color="#8b5cf6", width=1.3, dash="dot")))
    fig.add_trace(go.Scatter(
        x=df_reg["year"], y=df_reg["contributions_cum"],
        mode="lines",
        name=tr("Contributions (baseline)", "תרומות (קו בסיס)"),
        line=dict(color="#94a3b8", width=1.4, dash="dot"),
    ))
    fig.add_trace(go.Scatter(
        x=df_reg["year"], y=df_reg["balance_no_lump"],
        mode="lines",
        name=tr("Regular bucket (no lump)", "תיק רגיל (ללא חד-פעמית)"),
        line=dict(color="#06b6d4", width=1.8, dash="dash"),
    ))
    fig.add_trace(go.Scatter(
        x=df_reg["year"], y=df_reg["balance_with_lump"],
        mode="lines",
        name=tr("Regular bucket (with lump)", "תיק רגיל (עם חד-פעמית)"),
        line=dict(color="#6366f1", width=2.4),
    ))
    if regular_lump > 0:
        fig.add_vline(
            x=float(regular_lump_month) / 12.0,
            line=dict(color="#f59e0b", width=1.4, dash="dashdot"),
            annotation_text=tr("Lump-sum", "הפקדה"),
            annotation_position="top",
        )
    template = "plotly_dark" if is_dark else "plotly_white"
    fig.update_layout(
        template=template,
        title=tr("Regular portfolio projection (BEFORE tax)",
                 "תחזית תיק רגיל (לפני מס)"),
        xaxis_title=tr("Years", "שנים"),
        yaxis_title=tr("Value (₪)", "שווי (₪)"),
        margin=dict(l=10, r=10, t=56, b=30),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        height=chart_height,
    )
    st.plotly_chart(
        _apply_plotly_theme(fig, is_dark, is_mobile),
        theme="streamlit", width="stretch",
    )

    # ── Yearly breakdown (regular bucket) ──────────────────────────────
    with st.expander(tr("📋 Yearly breakdown (regular bucket)", "📋 פירוט שנתי (תיק רגיל)")):
        yearly = df_reg.iloc[::12].copy()
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
        st.dataframe(yearly, width="stretch", hide_index=True)
        csv = yearly.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label=tr("⬇ Download yearly breakdown (CSV)", "⬇ הורדת הפירוט השנתי (CSV)"),
            data=csv,
            file_name="simulator_yearly.csv",
            mime="text/csv",
            width="stretch" if is_mobile else "content",
        )

    st.caption(tr(
        "⚠ Educational projections only — not investment advice. Tax = 25% on real (inflation-adjusted) gains for the regular bucket; pension + education funds are treated as tax-exempt per Israeli rules.",
        "⚠ ההדמיה היא לצורכי הדגמה בלבד — אינה ייעוץ השקעות. מיסוי = 25% על רווח ריאלי בתיק הרגיל; קרנות פנסיה והשתלמות פטורות ממס לפי הכללים בישראל.",
    ))

    # Reset stored inputs (button lives at the bottom since it's rarely needed).
    _reset_cols = st.columns([3, 1]) if not is_mobile else (st.container(), st.container())
    with _reset_cols[1]:
        if st.button(
            tr("🔄 Reset inputs (this mode)", "🔄 איפוס קלטים (מצב זה)"),
            key=f"sim_reset_prefs_{mode_id}",
            width="stretch",
            help=tr(
                "Clear saved values for THIS mode only — the other mode's data is preserved.",
                "איפוס הערכים השמורים של המצב הנוכחי בלבד — נתוני המצב השני נשמרים.",
            ),
        ):
            for _k in _SIM_MODE_KEYS:
                st.session_state.pop(_sim_key(mode_id, _k), None)
            try:
                # Persist immediately so on-disk reflects the deletion
                save_sim_prefs({k: st.session_state.get(k) for k in list(st.session_state.keys()) if k in _sim_allowed_keys()})
            except Exception:
                pass
            st.rerun()


# ════════════════════════════════════════════════════════════════════
#  In-app AI Chat agent — Gemini (cloud, always) / Claude (local CLI,
#  desktop only). Full live-data context, image vision, conversation
#  memory, free-text trade add/edit/delete, and custom-document output.
# ════════════════════════════════════════════════════════════════════
GEMINI_CHAT_MODEL = "gemini-flash-latest"


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_gemini_key_remote(url: str, token: str) -> str:
    """Fetch the Gemini key from the Apps Script backend (so cloud-hosted apps /
    the phone get it automatically, without pasting it into each platform)."""
    try:
        r = call_apps_script_(url, {"token": token, "action": "get_gemini_key"})
        return _clean(r.get("key", "")) if isinstance(r, dict) else ""
    except Exception:
        return ""


def _gemini_api_key() -> str:
    """Gemini key — Streamlit Cloud secrets → local config → Apps Script backend."""
    try:
        if "gemini_api_key" in st.secrets:
            k = _clean(st.secrets.get("gemini_api_key", ""))
            if k:
                return k
    except Exception:
        pass
    try:
        if LOCAL_SETTINGS_FILE.exists():
            raw = json.loads(LOCAL_SETTINGS_FILE.read_text(encoding="utf-8"))
            k = _clean(raw.get("gemini_api_key", ""))
            if k:
                return k
    except Exception:
        pass
    try:
        s = load_local_settings()
        url = _clean(s.get("web_app_url", "")); tok = _clean(s.get("api_token", ""))
        if url and tok:
            return _fetch_gemini_key_remote(url, tok)
    except Exception:
        pass
    return ""


def _gemini_chat(system_text: str, history: List[Dict[str, str]], user_text: str,
                 images: Optional[List] = None, tools: Optional[list] = None,
                 model: Optional[str] = None) -> Dict[str, object]:
    """One Gemini generateContent call. Returns the parsed JSON (caller reads text
    or functionCall). images = list of (mime, bytes)."""
    import urllib.request, urllib.error, base64
    key = _gemini_api_key()
    if not key:
        return {"error": "no_key"}
    model = model or GEMINI_CHAT_MODEL
    url = "https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent?key=%s" % (model, key)
    contents: List[Dict[str, object]] = []
    for h in (history or []):
        contents.append({"role": ("model" if h.get("role") == "model" else "user"),
                         "parts": [{"text": str(h.get("text", ""))}]})
    user_parts: List[Dict[str, object]] = [{"text": user_text or ""}]
    for (mime, data) in (images or []):
        user_parts.append({"inline_data": {"mime_type": mime or "image/png",
                                            "data": base64.b64encode(data).decode("ascii")}})
    contents.append({"role": "user", "parts": user_parts})
    payload: Dict[str, object] = {
        "systemInstruction": {"parts": [{"text": system_text}]},
        "contents": contents,
        "generationConfig": {"temperature": 0.4, "maxOutputTokens": 2048},
    }
    if tools:
        payload["tools"] = tools
    try:
        req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"),
                                     headers={"Content-Type": "application/json"})
        resp = urllib.request.urlopen(req, timeout=90)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try:
            return {"error": "http_%s" % e.code, "detail": e.read().decode()[:300]}
        except Exception:
            return {"error": "http_%s" % e.code}
    except Exception as e:
        return {"error": "exc", "detail": str(e)[:200]}


def _gemini_text(resp: Dict[str, object]) -> str:
    try:
        parts = resp["candidates"][0]["content"]["parts"]
        return "".join(p.get("text", "") for p in parts if isinstance(p, dict)).strip()
    except Exception:
        return ""


def _chat_data_context(df) -> str:
    """Compact live-portfolio context for the agent, incl. individual open lots
    (with Trade_IDs) so it can reference / edit / delete specific trades."""
    try:
        if df is None or df.empty:
            return "No portfolio data available (empty)."
        d = df.copy()
        for c in ["Ticker", "Platform", "Type", "Status", "Origin_Currency", "Purchase_Date", "Trade_ID"]:
            if c not in d.columns:
                d[c] = ""
        for c in ["Quantity", "Cost_ILS", "Current_Value_ILS", "Commission"]:
            if c not in d.columns:
                d[c] = 0.0
            d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0.0)
        closed_mask = d["Status"].map(_is_closed_status)  # canonical (incl. "close", case-insensitive) — was an inline list missing "close" (audit)
        o = d[~closed_mask]
        tv = float(o["Current_Value_ILS"].sum()); tc = float(o["Cost_ILS"].sum())
        L = ["LIVE PORTFOLIO (ILS ₪):",
             "open_value=%0.0f cost=%0.0f unrealized_pnl=%0.0f return=%0.2f%% open_rows=%d closed_rows=%d" % (
                 tv, tc, tv - tc, ((tv - tc) / tc * 100 if tc else 0), len(o), int(closed_mask.sum()))]
        g = o.groupby("Ticker").agg(qty=("Quantity", "sum"), cost=("Cost_ILS", "sum"), value=("Current_Value_ILS", "sum"))
        L.append("holdings [ticker: qty cost value pnl ret%]:")
        for tk, r in g.sort_values("value", ascending=False).iterrows():
            pl = r["value"] - r["cost"]; y = (pl / r["cost"] * 100) if r["cost"] else 0
            L.append("  %s: qty=%s cost=%0.0f value=%0.0f pnl=%0.0f ret=%0.2f%%" % (tk, round(float(r["qty"]), 4), r["cost"], r["value"], pl, y))
        L.append("open lots [Trade_ID | ticker | platform | date | qty | cost_ILS | currency]:")
        for _, r in o.iterrows():
            L.append("  %s | %s | %s | %s | %s | %0.0f | %s" % (
                str(r.get("Trade_ID", ""))[:14], r.get("Ticker", ""), r.get("Platform", ""),
                r.get("Purchase_Date", ""), round(float(r.get("Quantity", 0)), 6),
                float(r.get("Cost_ILS", 0)), r.get("Origin_Currency", "")))
        return "\n".join(L[:160])
    except Exception as e:
        return "data context error: " + str(e)[:120]


def _claude_cli_path() -> Optional[str]:
    """Path to the local Claude Code CLI (desktop only)."""
    cands = [
        Path(os.environ.get("APPDATA", "")) / "npm" / "node_modules" / "@anthropic-ai" / "claude-code" / "bin" / "claude.exe",
        Path(os.environ.get("APPDATA", "")) / "npm" / "claude.cmd",
    ]
    for c in cands:
        try:
            if c and c.exists():
                return str(c)
        except Exception:
            continue
    return None


def _claude_chat(user_text: str, history: List[Dict[str, str]], data_context: str,
                 image_paths: Optional[List[str]] = None, timeout: int = 240) -> str:
    """Route a turn through the local Claude Code CLI (uses the MAX subscription;
    it can run portfolio_cli.py to actually read/write trades). Desktop only."""
    exe = _claude_cli_path()
    if not exe:
        return "Claude לא זמin במחשב (זמין רק כשהאפליקציה רצה על המחשב עם Claude Code מותקן)."
    convo = ""
    for h in (history or [])[-6:]:
        convo += ("\nUser: " if h.get("role") == "user" else "\nAssistant: ") + str(h.get("text", ""))[:600]
    imgs = ""
    for p in (image_paths or []):
        imgs += "\nתמונה מצורפת (קרא אותה עם Read): " + p
    prompt = user_text + imgs
    system = (
        "You are Ouriel's portfolio agent inside his app. Reply in HEBREW, concise. "
        "You manage the portfolio via the CLI (writes to the shared Google Sheet → all apps sync):\n"
        "  python portfolio_cli.py snapshot|add --json \"{...}\"|edit --json \"{...}\"|delete --id ID\n"
        "Use the live data below. Before any add/edit/delete confirm the parsed values in one line, then run the CLI and report what changed. Never invent numbers.\n\n"
        "Recent conversation:" + (convo or " (none)") + "\n\n" + data_context
    )
    cmd = [exe, "-p", "--model", "sonnet", "--output-format", "text",
           "--allowedTools", "Bash(python portfolio_cli.py:*),PowerShell(python portfolio_cli.py:*),Read,Glob,Grep",
           "--append-system-prompt", system]
    env = dict(os.environ); env["PYTHONUTF8"] = "1"; env["PYTHONIOENCODING"] = "utf-8"; env.pop("ANTHROPIC_API_KEY", None)
    try:
        proc = subprocess.run(cmd, input=prompt, cwd=str(_DATA_DIR), env=env, capture_output=True,
                              text=True, encoding="utf-8", errors="replace", timeout=timeout)
    except subprocess.TimeoutExpired:
        return "Claude לקח יותר מדי זמן. נסה שאלה קצרה יותר או השתמש ב-Gemini."
    except Exception as exc:
        return "שגיאת הרצת Claude: " + str(exc)[:200]
    out = (proc.stdout or "").strip()
    return out[:3800] if out else ("לא התקבלה תשובה. " + (proc.stderr or "")[:300])


def _claude_via_relay(prompt: str, web_app_url: str, token: str, max_wait: int = 150):
    """Submit a Claude request to the backend and poll for the desktop's answer.
    Lets the phone use Claude when the PC (running claude_relay.py) is on.
    Returns the answer text, or None on timeout / no relay."""
    if not (web_app_url and token):
        return None
    try:
        r = call_apps_script_(web_app_url, {"token": token, "action": "claude_submit", "prompt": prompt})
        rid = r.get("id") if isinstance(r, dict) else None
        if not rid:
            return None
        waited = 0
        while waited < max_wait:
            time.sleep(3); waited += 3
            rr = call_apps_script_(web_app_url, {"token": token, "action": "claude_result", "id": rid})
            ans = rr.get("answer") if isinstance(rr, dict) else None
            if ans:
                return ans
        return None
    except Exception:
        return None


# ── AI agent trade write-back: Gemini function-calling, human-confirmed ──────
# The model can PROPOSE adding a buy or deleting a trade; nothing is written until
# Ouriel approves the staged action. Sells stay on the Manage page (FIFO lot
# matching is too error-prone to delegate to an LLM).
_AI_TRADE_TOOLS = [{
    "function_declarations": [
        {
            "name": "add_buy_trade",
            "description": ("Record a NEW BUY (purchase) in the portfolio. Extract every field you can "
                            "from the user's words or an attached broker screenshot. The user confirms "
                            "before anything is saved. For SELLS, do NOT call this — tell the user to use "
                            "the Manage page so the sale is matched to the right open lot (FIFO)."),
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "description": "Asset symbol, e.g. BTC, ETH, VOO, QQQ"},
                    "quantity": {"type": "number", "description": "Number of units bought"},
                    "price": {"type": "number", "description": "Buy price per unit in the origin currency"},
                    "currency": {"type": "string", "enum": ["ILS", "USD"], "description": "Origin currency (ILS or USD)"},
                    "platform": {"type": "string", "description": "Broker/exchange, e.g. Bit2C, Horizon, Excellence"},
                    "asset_type": {"type": "string", "description": "קריפטו for crypto, שוק ההון for stocks/ETFs"},
                    "date": {"type": "string", "description": "Purchase date YYYY-MM-DD; today if unknown"},
                    "commission": {"type": "number", "description": "Fee in origin currency; 0 if unknown"},
                },
                "required": ["ticker", "quantity", "price"],
            },
        },
        {
            "name": "sell_trade",
            "description": ("Record a SALE from an existing OPEN lot, identified by its Trade_ID (from the "
                            "open-lots list in the data context). Handles full or partial sells; the user "
                            "confirms before anything is written. Realized P/L is computed automatically."),
            "parameters": {
                "type": "object",
                "properties": {
                    "trade_id": {"type": "string", "description": "Trade_ID of the open lot being sold from"},
                    "sell_quantity": {"type": "number", "description": "Units to sell; if omitted, sells the whole lot"},
                    "sell_price": {"type": "number", "description": "Sale price per unit in the lot's origin currency"},
                    "sell_date": {"type": "string", "description": "Sale date YYYY-MM-DD; today if unknown"},
                },
                "required": ["trade_id", "sell_price"],
            },
        },
        {
            "name": "edit_trade",
            "description": ("Edit fields of an existing trade identified by its Trade_ID (from the open-lots "
                            "list). Provide ONLY the fields to change. The user confirms before saving."),
            "parameters": {
                "type": "object",
                "properties": {
                    "trade_id": {"type": "string", "description": "Trade_ID of the trade to edit"},
                    "quantity": {"type": "number"},
                    "buy_price": {"type": "number", "description": "New buy price per unit"},
                    "platform": {"type": "string"},
                    "date": {"type": "string", "description": "New purchase date YYYY-MM-DD"},
                    "commission": {"type": "number"},
                },
                "required": ["trade_id"],
            },
        },
        {
            "name": "delete_trade",
            "description": ("Delete an existing trade by its Trade_ID (shown in the open-lots list in the "
                            "data context). The user confirms before it is deleted."),
            "parameters": {
                "type": "object",
                "properties": {"trade_id": {"type": "string", "description": "The Trade_ID to delete"}},
                "required": ["trade_id"],
            },
        },
    ]
}]


def _ai_extract_function_call(resp: Dict[str, object]) -> Optional[Dict[str, object]]:
    """Pull the first functionCall (name + args) out of a Gemini response, if any."""
    try:
        for p in resp["candidates"][0]["content"]["parts"]:
            if isinstance(p, dict) and isinstance(p.get("functionCall"), dict):
                fc = p["functionCall"]
                return {"name": _clean(fc.get("name", "")), "args": dict(fc.get("args", {}) or {})}
    except Exception:
        pass
    return None


def _ai_add_args_to_row(args: Mapping[str, object]) -> Dict[str, object]:
    """Convert add_buy_trade function args into a trade_row the existing save path
    (sync_trade_to_sheet / apply_local_trade) understands. BUY/open only."""
    qty = float(_num(args.get("quantity", 0)))
    price = float(_num(args.get("price", 0)))
    cur = _clean(args.get("currency", "")).upper()
    cur = cur if cur in ("ILS", "USD") else "ILS"
    date = _clean(args.get("date", "")) or datetime.now().strftime("%Y-%m-%d")
    commission = float(_num(args.get("commission", 0)))
    cost_origin = qty * price
    return {
        "Current_Location": "",
        "Platform": _clean(args.get("platform", "")),
        "Type": _clean(args.get("asset_type", "")),
        "Ticker": _clean(args.get("ticker", "")).upper(),
        "Origin_Currency": cur,
        "Quantity": qty,
        "Origin_Buy_Price": price,
        "Cost_Origin": cost_origin,
        "Commission": commission,
        "Action": "BUY",
        "Event_Type": "TRADE",
        "Status": "פתוח",
        "Sell_Date": "",
        "Current_Value_ILS": 0.0,
        # Freeze a real ILS cost basis at the buy-date FX immediately (mirrors web/mobile
        # BUY) so local FIFO/aggregates are commission- & FX-correct before any reload
        # repopulates it from Google. (audit fin-commission staged-buy)
        "Cost_ILS": _frozen_cost_ils_for_sell(cost_origin, commission, cur, date),
        "Purchase_Date": date,
    }


def _ai_find_trade_row(df, trade_id: str) -> Optional[Dict[str, object]]:
    """Look up a trade row by Trade_ID (prefix-tolerant — the data context shows
    14-char Trade_IDs, so the model may echo a truncated id)."""
    try:
        if df is None or df.empty or "Trade_ID" not in df.columns:
            return None
        tid = _clean(trade_id)
        if not tid:
            return None
        ids = df["Trade_ID"].astype(str).map(_clean)
        hit = df[ids == tid]
        if hit.empty:
            hit = df[ids.str.startswith(tid)]
        return hit.iloc[0].to_dict() if not hit.empty else None
    except Exception:
        return None


def _ai_build_sell(df, args: Mapping[str, object]) -> Optional[Dict[str, object]]:
    """Stage a SELL against an open lot. Full sell → convert that lot to closed
    (no double-count); partial → add a closed slice + shrink the source lot.
    Realized P/L follows automatically (proceeds − allocated cost basis)."""
    src = _ai_find_trade_row(df, _clean(args.get("trade_id", "")))
    if not src:
        return None
    src_qty = float(_num(src.get("Quantity", 0)))
    sell_price = float(_num(args.get("sell_price", 0)))
    # Allow sell_price == 0 only when the caller explicitly marks a TOTAL LOSS
    # (write-off). FIFO supports zero-proceeds total loss; rejecting it omitted a
    # real realized loss from the closed record. (audit fin-tax total-loss)
    _total_loss = bool(args.get("total_loss")) and str(args.get("total_loss")).strip().lower() not in {"false", "0", "no", ""}
    if src_qty <= 0 or sell_price < 0 or (sell_price == 0 and not _total_loss):
        return None
    sell_qty = float(_num(args.get("sell_quantity", 0))) or src_qty
    sell_qty = min(sell_qty, src_qty)
    if sell_qty <= 0:
        return None
    cur = _normalize_currency_code(src.get("Origin_Currency", "")) or "ILS"
    sell_date = _clean(args.get("sell_date", "")) or datetime.now().strftime("%Y-%m-%d")
    # Backdated sale: convert proceeds at the sell-DATE FX rate (matching GAS's
    # sRate), not today's spot. Fall back to today's live rate if the historical
    # lookup fails. Handles every currency (USD/GBP/GBX/EUR), ILS/ILA→1. (audit #9, fin-fx)
    if cur in {"ILS", "ILA", ""}:
        fx = 1.0
    else:
        _pair = {"USD": "USDILS=X", "GBX": "GBPILS=X", "GBP": "GBPILS=X", "EUR": "EURILS=X"}.get(cur, f"{cur}ILS=X")
        _hist_fx = _fx_pair_on(_pair, sell_date)
        if _hist_fx and _hist_fx > 0:
            fx = _hist_fx
        else:
            _live = _fx_pair_rate(_pair) if _pair != "USDILS=X" else _usd_ils_rate()
            fx = float(_live) if _live and _live > 0 else 1.0
    src_cost = float(_num(src.get("Cost_Origin", 0)))
    src_comm = float(_num(src.get("Commission", 0)))
    ratio = sell_qty / src_qty if src_qty else 0.0
    alloc_cost, alloc_comm = src_cost * ratio, src_comm * ratio
    src_purchase = _clean(src.get("Purchase_Date", ""))
    # Freeze a real ILS cost basis at the BUY-date FX (NOT today's, NOT the sell-date
    # FX) so realized P&L is deterministic and the FIFO/realized-ILS column never falls
    # back to today's spot. (audit fin-streamlit #1/#2)
    alloc_cost_ils = _frozen_cost_ils_for_sell(alloc_cost, alloc_comm, cur, src_purchase)
    ticker = _clean(src.get("Ticker", "")).upper()
    sell_row = {
        "Current_Location": _clean(src.get("Current_Location", "")),
        "Platform": _clean(src.get("Platform", "")), "Type": _clean(src.get("Type", "")),
        "Ticker": ticker, "Origin_Currency": cur,
        "Purchase_Date": src_purchase,
        "Quantity": sell_qty, "Origin_Buy_Price": float(_num(src.get("Origin_Buy_Price", 0))),
        "Cost_Origin": alloc_cost, "Commission": alloc_comm,
        "Status": "סגור", "Action": "SELL", "Event_Type": "TRADE",
        "Sell_Date": sell_date, "Sell_Price_Origin": sell_price,
        "Current_Value_ILS": sell_qty * sell_price * fx, "Cost_ILS": alloc_cost_ils,
    }
    # Realized P&L = proceeds (at the SELL-date FX) − the frozen acquisition cost
    # basis (allocated Cost_Origin + buy commission, converted at the BUY-date FX).
    # Using the buy-date FX on the cost side keeps realized deterministic and in
    # agreement with fifo_metrics + GAS. (audit fin-streamlit #2, fin-commission)
    realized = sell_qty * sell_price * fx - alloc_cost_ils
    label = "%s · %g @ %s%.2f" % (ticker, sell_qty, ("$" if cur == "USD" else "₪"), sell_price)
    if (src_qty - sell_qty) > 1e-9:  # partial
        rem_cost = max(0.0, src_cost - alloc_cost)
        rem_comm = max(0.0, src_comm - alloc_comm)
        source_update = dict(src)
        source_update.update({"Quantity": src_qty - sell_qty,
                              "Cost_Origin": rem_cost,
                              "Commission": rem_comm,
                              # Freeze the surviving open lot's ILS cost basis at the
                              # buy-date FX too, so the residual position is priced and
                              # yielded off a real basis (not Cost_Origin*today). (audit)
                              "Cost_ILS": _frozen_cost_ils_for_sell(rem_cost, rem_comm, cur, src_purchase),
                              "Status": "פתוח", "Sell_Date": "", "Action": "BUY"})
        return {"op": "sell_partial", "sell_row": sell_row, "source_update": source_update,
                "label": label, "realized": realized}
    sell_row["Trade_ID"] = _clean(src.get("Trade_ID", ""))  # full: convert lot in place
    return {"op": "sell_full", "row": sell_row, "label": label, "realized": realized}


def _ai_build_edit(df, args: Mapping[str, object]) -> Optional[Dict[str, object]]:
    """Stage an EDIT — merge only the provided fields into the existing trade row."""
    src = _ai_find_trade_row(df, _clean(args.get("trade_id", "")))
    if not src:
        return None
    row = dict(src)
    changed = []
    if args.get("quantity") is not None:
        row["Quantity"] = float(_num(args.get("quantity"))); changed.append("qty")
    if args.get("buy_price") is not None:
        row["Origin_Buy_Price"] = float(_num(args.get("buy_price"))); changed.append("price")
    if _clean(args.get("platform", "")):
        row["Platform"] = _clean(args.get("platform")); changed.append("platform")
    if _clean(args.get("date", "")):
        row["Purchase_Date"] = _clean(args.get("date")); changed.append("date")
    if args.get("commission") is not None:
        row["Commission"] = float(_num(args.get("commission"))); changed.append("commission")
    if not changed:
        return None
    if "qty" in changed or "price" in changed:
        row["Cost_Origin"] = float(_num(row.get("Quantity", 0))) * float(_num(row.get("Origin_Buy_Price", 0)))
    return {"op": "edit", "row": row, "label": _clean(row.get("Ticker", "")) + " · " + ", ".join(changed)}


def _ai_execute_trade(pending: Mapping[str, object], web_app_url: str, token: str) -> Tuple[bool, str]:
    """Execute a human-approved staged trade. Mirrors the Manage form: push to
    Google first when configured, then persist locally."""
    op = _clean(pending.get("op", ""))
    has_google = is_apps_script_web_app_url(_clean(web_app_url))

    def _do(action: str, row: Dict[str, object]) -> Tuple[bool, str]:
        # Google-first when configured (atomic), then mirror locally.
        if has_google:
            ok, msg = sync_trade_to_sheet(web_app_url, token, action, row)
            if not ok:
                return False, msg
            apply_local_trade(action, row, mark_dirty=False)
        else:
            apply_local_trade(action, row, mark_dirty=True)
        return True, _clean(row.get("Trade_ID", ""))

    if op == "add":
        row = dict(pending.get("row", {}))
        row["Trade_ID"] = _to_trade_id(pd.Series(row))
        return _do("add", row)
    if op == "edit" or op == "sell_full":
        return _do("edit", dict(pending.get("row", {})))
    if op == "sell_partial":
        # Add the sold slice, then shrink the source open lot. Add first so a
        # failure can't leave the source already reduced with no matching sale.
        sell_row = dict(pending.get("sell_row", {}))
        sell_row["Trade_ID"] = _to_trade_id(pd.Series(sell_row))
        ok, msg = _do("add", sell_row)
        if not ok:
            return False, msg
        ok2, msg2 = _do("edit", dict(pending.get("source_update", {})))
        if not ok2:
            return False, "sale recorded; source-lot update failed: " + msg2
        return True, sell_row.get("Trade_ID", "")
    if op == "delete":
        tid = _clean(pending.get("trade_id", ""))
        if not tid:
            return False, "missing trade_id"
        return _do("delete", {"Trade_ID": tid})
    return False, "unknown op"


def _render_ai_pending_trade(tr, web_app_url, token) -> None:
    """If the agent has staged a trade, show a confirmation card. Nothing is
    written until the user clicks Approve."""
    pending = st.session_state.get("_ai_pending_trade")
    if not pending:
        return
    op = _clean(pending.get("op", ""))
    with st.container(border=True):
        if op == "add":
            row = dict(pending.get("row", {}))
            st.markdown("### " + tr("⚠ Confirm new BUY", "⚠ אישור קנייה חדשה"))
            cur = _clean(row.get("Origin_Currency", "ILS"))
            _sym = "$" if cur == "USD" else "₪"
            st.markdown(
                f"**{_clean(row.get('Ticker',''))}** · "
                f"{tr('Qty','כמות')} {row.get('Quantity',0):g} · "
                f"{tr('Price','מחיר')} {_sym}{row.get('Origin_Buy_Price',0):,.2f} · "
                f"{tr('Cost','עלות')} {_sym}{row.get('Cost_Origin',0):,.2f}"
                + (f" · {_clean(row.get('Platform',''))}" if _clean(row.get('Platform','')) else "")
                + f" · {_clean(row.get('Purchase_Date',''))}"
            )
        elif op in ("sell_full", "sell_partial"):
            row = dict(pending.get("sell_row", pending.get("row", {})))
            cur = _clean(row.get("Origin_Currency", "ILS"))
            _sym = "$" if cur == "USD" else "₪"
            _rl = float(_num(pending.get("realized", 0)))
            st.markdown("### " + tr("⚠ Confirm SALE", "⚠ אישור מכירה")
                        + (" — " + tr("partial", "חלקית") if op == "sell_partial" else " — " + tr("full lot", "כל הלוט")))
            st.markdown(
                f"**{_clean(row.get('Ticker',''))}** · "
                f"{tr('Sell qty','כמות למכירה')} {row.get('Quantity',0):g} · "
                f"{tr('Price','מחיר')} {_sym}{row.get('Sell_Price_Origin',0):,.2f} · "
                f"{tr('Proceeds','תמורה')} ₪{row.get('Current_Value_ILS',0):,.0f}"
                + f" · {tr('Realized P/L','רווח/הפסד ממומש')} ₪{_rl:,.0f}"
                + f" · {_clean(row.get('Sell_Date',''))}"
            )
        elif op == "edit":
            row = dict(pending.get("row", {}))
            cur = _clean(row.get("Origin_Currency", "ILS"))
            _sym = "$" if cur == "USD" else "₪"
            st.markdown("### " + tr("⚠ Confirm edit", "⚠ אישור עריכה"))
            st.markdown(
                f"**{_clean(row.get('Ticker',''))}** "
                f"`{_clean(row.get('Trade_ID',''))[:14]}` → "
                f"{tr('Qty','כמות')} {row.get('Quantity',0):g} · "
                f"{tr('Price','מחיר')} {_sym}{row.get('Origin_Buy_Price',0):,.2f}"
                + (f" · {_clean(row.get('Platform',''))}" if _clean(row.get('Platform','')) else "")
                + (f" · {_clean(row.get('Purchase_Date',''))}" if _clean(row.get('Purchase_Date','')) else "")
            )
        elif op == "delete":
            st.markdown("### " + tr("⚠ Confirm delete", "⚠ אישור מחיקה"))
            st.markdown(tr("Delete trade", "מחיקת עסקה") + f" `{_clean(pending.get('trade_id',''))}`"
                        + (f" — {_clean(pending.get('label',''))}" if pending.get("label") else ""))
        ca, cc = st.columns(2)
        if ca.button(tr("✅ Approve & save", "✅ אשר ושמור"), key="_ai_trade_ok", type="primary", width="stretch"):
            ok, msg = _ai_execute_trade(pending, web_app_url, token)
            st.session_state.pop("_ai_pending_trade", None)
            if ok:
                # Bust the snapshot caches so the next render reflects the write
                # (mirrors the Manage form's post-save refresh).
                for _c in (load_google_snapshot_data, load_google_snapshot_data_via_gspread):
                    try:
                        _c.clear()
                    except Exception:
                        pass
                st.session_state.setdefault("ai_chat_history", []).append(
                    {"role": "assistant", "text": tr("✅ Done — saved.", "✅ בוצע — נשמר.") + f" ({msg})"})
                st.toast(tr("Trade saved", "העסקה נשמרה"), icon="✅")
            else:
                st.session_state.setdefault("ai_chat_history", []).append(
                    {"role": "assistant", "text": tr("❌ Save failed", "❌ השמירה נכשלה") + f": {msg}"})
            st.rerun()
        if cc.button(tr("✖ Cancel", "✖ ביטול"), key="_ai_trade_cancel", width="stretch"):
            st.session_state.pop("_ai_pending_trade", None)
            st.rerun()


def render_ai_chat_page(tr, df, web_app_url, token, language, is_dark, is_mobile) -> None:
    # ── Professional, Gemini-like chat styling (theme-aware, app accent) ──
    _abg = "#1f2937" if is_dark else "#ffffff"
    _abd = "rgba(255,255,255,0.08)" if is_dark else "#e6e9f0"
    _atx = "#e5e7eb" if is_dark else "#1f2937"
    _chip_bg = "rgba(255,255,255,0.05)" if is_dark else "#f4f6fb"
    st.markdown(
        "<style>"
        "[data-testid='stChatMessage']{background:transparent!important;border:none!important;padding:3px 0!important;}"
        "[data-testid='stChatMessage'] [data-testid='stChatMessageContent']{border-radius:16px!important;"
        "padding:11px 15px!important;line-height:1.6!important;box-shadow:0 1px 3px rgba(15,23,42,0.10)!important;max-width:90%!important;}"
        "[data-testid='stChatMessage']:has([data-testid='stChatMessageAvatarAssistant']) [data-testid='stChatMessageContent']{"
        f"background:{_abg}!important;border:1px solid {_abd}!important;color:{_atx}!important;}}"
        "[data-testid='stChatMessage']:has([data-testid='stChatMessageAvatarUser']){flex-direction:row-reverse!important;}"
        "[data-testid='stChatMessage']:has([data-testid='stChatMessageAvatarUser']) [data-testid='stChatMessageContent']{"
        "background:linear-gradient(135deg,#f59e0b,#d97706)!important;color:#fff!important;border:none!important;}"
        "[data-testid='stChatMessageAvatarUser'],[data-testid='stChatMessageAvatarAssistant']{box-shadow:0 2px 6px rgba(79,70,229,.25)!important;}"
        "[data-testid='stChatInput']{border-radius:18px!important;}[data-testid='stChatInput'] textarea{font-size:15px!important;}"
        # #3 — theme the bottom chat-input panel in dark mode (Streamlit leaves
        #      stBottom/stChatInput light by default → a white bar on navy).
        + (
            # #3 (full) — Streamlit leaves the chat textarea + its inner wrapper
            # light even in dark mode. Force the whole surface dark, high
            # specificity, incl. the inner baseweb input div.
            "html body [data-testid='stBottom'],html body [data-testid='stBottomBlockContainer'],"
            "html body [data-testid='stBottom'] > div{background:#0f172a!important;}"
            "html body [data-testid='stChatInput'],"
            "html body [data-testid='stChatInput'] > div,"
            "html body [data-testid='stChatInput'] [data-baseweb='textarea'],"
            "html body [data-testid='stChatInput'] [data-baseweb='base-input']{"
            "background:#1e293b!important;border-color:#334155!important;}"
            "html body [data-testid='stChatInput']{border:1px solid #334155!important;border-radius:18px!important;}"
            "html body [data-testid='stChatInput'] textarea{background:#1e293b!important;color:#e2e8f0!important;-webkit-text-fill-color:#e2e8f0!important;}"
            "html body [data-testid='stChatInput'] textarea::placeholder{color:#94a3b8!important;-webkit-text-fill-color:#94a3b8!important;}"
            "html body [data-testid='stChatInput'] button{color:#e2e8f0!important;background:transparent!important;}"
            "html body [data-testid='stChatInput'] button svg{fill:#e2e8f0!important;}"
            if is_dark else ""
        ) +
        ".agent-hero{background:linear-gradient(120deg,#f59e0b,#fbbf24 55%,#fde047);border-radius:16px;padding:14px 18px;"
        "color:#3f2d00;margin-bottom:10px;box-shadow:0 10px 28px -12px rgba(234,179,8,.55);}"
        # Text is ALWAYS on the gold gradient → force dark, !important, so the
        # dark-mode markdown colour rule can't flip it to white (1.6:1 fail).
        ".agent-hero h3,.agent-hero h3 *{margin:0;color:#3f2d00 !important;font-size:1.18rem;font-weight:700;}"
        ".agent-hero p,.agent-hero p *{margin:3px 0 0;opacity:.95;font-size:.85rem;color:#4a3a06 !important;}"
        f".agent-chip{{display:inline-block;padding:8px 13px;margin:4px 6px 4px 0;border-radius:13px;"
        f"background:{_chip_bg};border:1px solid {_abd};color:{_atx};font-size:.85rem;}}"
        "</style>",
        unsafe_allow_html=True,
    )

    has_gemini = bool(_gemini_api_key())
    _local_claude = _claude_cli_path() is not None       # desktop: direct CLI
    _relay_claude = bool(web_app_url and token)           # phone: relay to the PC (when on)
    claude_ok = _local_claude or _relay_claude
    providers, pmap = [], {}
    # #10 — clean brand names, no inconsistent emoji (was Gemini ☁️ / Claude 🧠,
    # which read as swapped/semantically-off against the avatars).
    if has_gemini:
        providers.append("Gemini"); pmap["Gemini"] = "gemini"
    if claude_ok:
        providers.append("Claude"); pmap["Claude"] = "claude"

    _eng = " · ".join(providers) if providers else tr("no engine", "אין מנוע")
    st.markdown(
        f"<div class='agent-hero'><h3><span class='material-symbols-rounded' style=\"font-family:'Material Symbols Rounded','Material Symbols Outlined';font-size:1.18rem;vertical-align:-3px;font-feature-settings:'liga' 1;\">smart_toy</span> {tr('AGENT AI', 'סוכן AI')}</h3>"
        f"<p>{tr('Your portfolio agent — full data access, image understanding, analysis & advice', 'סוכן התיק שלך — גישה לכל הנתונים, הבנת תמונות, ניתוח וייעוץ')}"
        f" · {_eng}</p></div>",
        unsafe_allow_html=True,
    )

    if not providers:
        st.warning(tr("No AI engine configured. Add a free Gemini API key (gemini_api_key) in settings/secrets.",
                      "לא הוגדר מנוע AI. הוסף מפתח Gemini חינמי (gemini_api_key) בהגדרות/secrets."))
        return

    cc1, cc2 = st.columns([3, 1])
    with cc1:
        prov_label = st.radio(tr("Engine", "מנוע"), providers, horizontal=True,
                              key="ai_chat_provider")
    with cc2:
        if st.button(tr("Clear", "נקה"), icon=":material/delete:", width="stretch"):
            st.session_state["ai_chat_history"] = []
            st.rerun()
    provider = pmap.get(prov_label, "gemini")

    # A staged (proposed) trade waits here for explicit human approval before any write.
    _render_ai_pending_trade(tr, web_app_url, token)

    hist = st.session_state.setdefault("ai_chat_history", [])
    if not hist:
        st.markdown("<div style='margin:8px 0 2px;opacity:.75'>" + tr("Try asking:", "נסה לשאול:") + "</div>",
                    unsafe_allow_html=True)
        _chips = [tr("Analyze my risk", "נתח לי את הסיכון"),
                  tr("How am I doing this month?", "איך אני מתקדם החודש?"),
                  tr("Crypto vs stocks", "קריפטו מול מניות"),
                  tr("Worst holding & why?", "האחזקה הכי גרועה ולמה?")]
        st.markdown("".join("<span class='agent-chip'>" + c + "</span>" for c in _chips), unsafe_allow_html=True)
    for m in hist:
        _role = "user" if m["role"] == "user" else "assistant"
        with st.chat_message(_role, avatar=("🧑" if _role == "user" else "🤖")):
            st.markdown(m["text"])

    placeholder = tr("Ask anything, edit trades, or attach a buy/sell screenshot…",
                     "שאל כל דבר, ערוך עסקאות, או צרף צילום מסך של קנייה/מכירה…")
    inp = st.chat_input(placeholder, accept_file=True, file_type=["png", "jpg", "jpeg"])
    if not inp:
        return
    # st.chat_input with accept_file returns an object with .text and .files
    user_text = getattr(inp, "text", None)
    files = getattr(inp, "files", None) or []
    if user_text is None and isinstance(inp, str):
        user_text = inp
    user_text = (user_text or "").strip()

    images = []
    img_paths = []
    for f in files:
        try:
            data = f.getvalue()
            images.append((getattr(f, "type", "image/png") or "image/png", data))
            if provider == "claude":
                tmp = _DATA_DIR / ("_chat_img_" + str(abs(hash(data)) % 100000) + ".png")
                tmp.write_bytes(data); img_paths.append(str(tmp))
        except Exception:
            pass

    with st.chat_message("user", avatar="🧑"):
        st.markdown(user_text or "📎 " + tr("(image)", "(תמונה)"))
        for (mime, data) in images:
            st.image(data, width=220)

    ctx = _chat_data_context(df)
    system = (
        "You are Ouriel's personal portfolio assistant inside his app. Answer in HEBREW, "
        "clear and concise, and analyze the data freely (returns, risk, allocation, what-ifs, advice). "
        "You can read attached images (e.g. a broker buy/sell screenshot) and extract the trade details. "
        "Currency is ₪ (ILS). Use ONLY the data below; never invent numbers. "
        "TRADE ACTIONS (each is shown to the user for explicit confirmation before anything is written — "
        "propose confidently, but never assume it is already saved):\n"
        "• PURCHASE ('קניתי 0.5 ETH ב-12000 ש\"ח ב-Bit2C' or a buy screenshot) → call add_buy_trade with every field.\n"
        "• SALE ('מכרתי 0.3 ETH ב-15000' or a sell screenshot) → call sell_trade with the matching open-lot Trade_ID "
        "(from the list below), the sell_price, and sell_quantity (omit to sell the whole lot). Realized P/L and FIFO "
        "lot accounting are handled automatically.\n"
        "• EDIT ('תקן את הכמות של ... ל-...') → call edit_trade with the Trade_ID and only the changed fields.\n"
        "• REMOVE → call delete_trade with the Trade_ID.\n"
        "Match Trade_IDs from the open-lots list; if you cannot find a matching lot, ask the user to clarify "
        "instead of guessing. CRITICAL: pass every price/quantity NUMBER exactly as the user stated it, in the "
        "asset's own origin currency — NEVER convert between ILS and USD yourself (the app handles FX). If the user "
        "says 4500 dollars, pass price=4500 with currency USD, not 16650.\n\n" + ctx)

    def _gemini_answer(spin_note=None):
        gh = [{"role": ("model" if h["role"] == "assistant" else "user"), "text": h["text"]} for h in hist[-8:]]
        resp = _gemini_chat(system, gh, user_text or "(ניתחתי את התמונה המצורפת)", images, tools=_AI_TRADE_TOOLS)
        # Trade action proposed → stage it for explicit confirmation, then rerun so
        # the confirmation card renders at the top. Nothing is written here.
        fc = _ai_extract_function_call(resp)
        if fc:
            name, args = fc.get("name", ""), fc.get("args", {})
            staged, note = None, ""
            if name == "add_buy_trade" and _num(args.get("quantity", 0)) > 0:
                staged = {"op": "add", "row": _ai_add_args_to_row(args)}
                note = tr("I prepared a buy for your confirmation 👇", "הכנתי קנייה לאישור שלך 👇")
            elif name == "sell_trade":
                staged = _ai_build_sell(df, args)
                note = (tr("I prepared a sale for your confirmation 👇", "הכנתי מכירה לאישור שלך 👇")
                        if staged else tr("I couldn't find a matching open lot — which one?",
                                          "לא מצאתי לוט פתוח תואם — איזה מהם?"))
            elif name == "edit_trade":
                staged = _ai_build_edit(df, args)
                note = (tr("I prepared an edit for your confirmation 👇", "הכנתי עריכה לאישור שלך 👇")
                        if staged else tr("I couldn't find that trade — which Trade_ID?",
                                          "לא מצאתי את העסקה — איזה Trade_ID?"))
            elif name == "delete_trade" and _clean(args.get("trade_id", "")):
                staged = {"op": "delete", "trade_id": _clean(args.get("trade_id", ""))}
                note = tr("I prepared a deletion for your confirmation 👇", "הכנתי מחיקה לאישור שלך 👇")
            if staged:
                st.session_state["_ai_pending_trade"] = staged
                hist.append({"role": "user", "text": user_text or "📎 (תמונה)"})
                hist.append({"role": "assistant", "text": note})
                st.session_state["ai_chat_history"] = hist[-40:]
                st.rerun()
            elif note:
                # A tool was called but couldn't be staged (e.g. lot not found) — a
                # functionCall reply carries no text, so surface the note ourselves.
                return (spin_note + note) if spin_note else note
        txt = _gemini_text(resp)
        if not txt:
            txt = "⚠️ " + tr("No response", "אין תגובה") + ": " + str(resp.get("detail") or resp.get("error") or "?")[:300]
        return (spin_note + txt) if spin_note else txt

    with st.chat_message("assistant", avatar="🤖"):
        if provider == "claude" and _claude_cli_path() is not None:
            with st.spinner(tr("Claude is thinking…", "Claude חושב…")):
                answer = _claude_chat(user_text, hist, ctx, img_paths)
        elif provider == "claude" and not images:
            # Phone: relay the request to the desktop's Claude (when the PC is on).
            with st.spinner(tr("Claude is thinking on your PC…", "Claude חושב על המחשב שלך…")):
                _convo = ""
                for h in hist[-6:]:
                    _convo += ("\nUser: " if h["role"] == "user" else "\nAssistant: ") + str(h["text"])[:600]
                _full = (user_text or "(נשלחה הודעה)") + "\n\nRecent conversation:" + (_convo or " (none)") + "\n\n" + ctx
                answer = _claude_via_relay(_full, web_app_url, token)
            if not answer:
                with st.spinner(tr("PC didn't answer — using Gemini…", "המחשב לא ענה — עובר ל-Gemini…")):
                    answer = _gemini_answer(tr("⏳ Claude didn't answer (is the PC on?) — answered with Gemini:\n\n",
                                               "⏳ Claude לא הגיב (המחשב כבוי?) — עניתי עם Gemini:\n\n"))
        else:
            with st.spinner(tr("Thinking…", "חושב…")):
                answer = _gemini_answer()
        st.markdown(answer)

    hist.append({"role": "user", "text": user_text or "📎 (תמונה)"})
    hist.append({"role": "assistant", "text": answer})
    st.session_state["ai_chat_history"] = hist[-40:]


def _run_data_migrations() -> None:
    """Data migrations embedded in app.py — run on every device that pulls from Git.

    Runs every startup; each migration checks the actual data condition (idempotent).
    Current migrations:
      - horizon_cost_fix (2026-05-09): Fix wrong Cost_Origin / Cost_ILS for 5 Horizon crypto trades.
        The original import miscalculated costs by ~10x for BTC Sep-2025.
        Matched by exact Quantity (unique per trade) — robust across different Trade_ID values.
    """
    # ── Migration: Horizon cost fix ────────────────────────────────────────
    # Key: (Ticker, exact_quantity_string) — unique fingerprint per trade
    # Value: (Cost_Origin_correct, Cost_ILS_correct)
    # Source of truth: DATA/הוריזון וBIT2C .txt
    _HORIZON_FIXES_BY_QTY: Dict[tuple, tuple] = {
        ("BTC", 0.01777571): (2061.45, 6895.14),   # BTC Sep-2025  (was CO≈214, yield≈+570%)
        ("SOL", 9.17858764): (2061.25, 7186.55),   # SOL Sep-2025  (was CO≈1964)
        ("BTC", 0.0167499):  (1511.79, 5208.57),   # BTC Nov-2025  (was CO≈1440)
        ("ETH", 0.716013):   (2066.51, 6708.30),   # ETH Jan-2026  (was CO≈1968)
        ("SOL", 20.864301):  (2518.16, 8174.20),   # SOL Jan-2026  (was CO≈2399)
    }

    pdata_path = Path("portfolio_data.json")
    try:
        pdata = json.loads(pdata_path.read_text(encoding="utf-8"))
        changed = 0
        for row in pdata.get("rows", []):
            ticker = str(row.get("Ticker", "")).strip().upper()
            try:
                qty = round(float(row.get("Quantity") or 0), 8)
            except (TypeError, ValueError):
                qty = 0.0
            key = (ticker, qty)
            if key not in _HORIZON_FIXES_BY_QTY:
                continue
            co_correct, ci_correct = _HORIZON_FIXES_BY_QTY[key]
            co_current = float(row.get("Cost_Origin") or 0)
            # Only fix if the stored value is still wrong (prevent infinite overwrite)
            if abs(co_current - co_correct) < 0.01:
                continue  # Already correct — skip
            row["Cost_Origin"] = co_correct
            row["Cost_ILS"] = ci_correct
            # Recompute Buy_Price = Cost_Origin / Quantity
            if qty > 0:
                row["Buy_Price"] = round(co_correct / qty, 6)
                row["Origin_Buy_Price"] = round(co_correct / qty, 6)
            changed += 1
        if changed:
            pdata_path.write_text(json.dumps(pdata, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass  # File may not exist on a fresh device — that's fine

    # Always delete snapshot_cache.csv — it's a stale fallback that can carry wrong data
    try:
        cache_path = Path("snapshot_cache.csv")
        if cache_path.exists():
            cache_path.unlink()
    except Exception:
        pass


DIVIDENDS_FILE = _DATA_DIR / "dividends.json"


def _load_dividends() -> list:
    try:
        if DIVIDENDS_FILE.exists():
            data = json.loads(DIVIDENDS_FILE.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
    except Exception:
        pass
    return []


def _save_dividends(rows: list) -> None:
    try:
        DIVIDENDS_FILE.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def render_smart_features(open_trades: "pd.DataFrame", language: str) -> None:
    """Self-contained value-add panel: Smart Insights, Rebalancing advisor, and
    Dividend tracking. Each section is wrapped in try/except so a failure in one
    never breaks the dashboard. Added 2026-06-12. Marker: PP-SMART-FEATURES."""
    def _t(en, he):
        return he if language == LANG_HE else en

    # ── 1. Smart Insights ────────────────────────────────────────────────
    with st.expander("💡 " + _t("Smart Insights", "תובנות חכמות"), expanded=False):
        try:
            df = open_trades.copy()
            for c in ("Current_Value_ILS", "Cost_ILS"):
                if c not in df.columns:
                    df[c] = 0.0
                df[c] = df[c].map(_num)
            total_val = float(df["Current_Value_ILS"].sum())
            if total_val <= 0:
                st.info(_t("No open positions to analyse.", "אין פוזיציות פתוחות לניתוח."))
            else:
                by_tkr = df.groupby("Ticker", as_index=False)["Current_Value_ILS"].sum()
                weights = (by_tkr["Current_Value_ILS"] / total_val)
                hhi = float((weights ** 2).sum())
                top = by_tkr.loc[by_tkr["Current_Value_ILS"].idxmax()]
                top_w = float(top["Current_Value_ILS"]) / total_val
                # Per-ticker yield from AGGREGATE value vs cost (NOT the mean of per-lot
                # yields, which mis-ranks tickers held across several lots).
                _agg_y = df.groupby("Ticker", as_index=False)[["Current_Value_ILS", "Cost_ILS"]].sum()
                _agg_y["_y"] = (_agg_y["Current_Value_ILS"] - _agg_y["Cost_ILS"]) / _agg_y["Cost_ILS"].where(_agg_y["Cost_ILS"] > 0, other=pd.NA)
                tkr_y = _agg_y.dropna(subset=["_y"]).set_index("Ticker")["_y"]
                # Crypto exposure must use the SAME definition as the rest of the
                # dashboard (build_home_inspired_reports / Crypto-Share KPI): spot
                # crypto Type PLUS the crypto-proxy ETFs (IBIT/ETHA/BSOL).
                # Counting only Type=="קריפטו" here previously reported ~46% while
                # other panels showed ~72% for the same portfolio.
                _ticker_up = df.get("Ticker", pd.Series(dtype=str)).map(lambda x: _clean(x).upper())
                # Same broad classifier as the allocation pie / Crypto-Share KPI / engine.js
                # so a blank-Type or new-coin row counts identically. (audit sync-defs)
                crypto_mask = df.apply(lambda r: _asset_class_label(r.get("Ticker", ""), r.get("Type", "")) == "crypto", axis=1) | _ticker_up.isin(CRYPTO_SHARE_TICKERS)
                crypto_share = float(df.loc[crypto_mask, "Current_Value_ILS"].sum()) / total_val if total_val else 0.0
                msgs = []
                if top_w >= 0.30:
                    _tk_top = _esc(top['Ticker'])  # escape: ticker comes from the sheet → rendered as HTML below (audit XSS)
                    msgs.append(("⚠️", _t(f"High concentration: {_tk_top} is {top_w:.0%} of the portfolio.",
                                          f"ריכוזיות גבוהה: {_tk_top} מהווה {top_w:.0%} מהתיק.")))
                msgs.append(("📊", _t(f"Concentration index (HHI): {hhi:.2f} ({'diversified' if hhi < 0.25 else 'concentrated'}).",
                                      f"מדד ריכוזיות (HHI): {hhi:.2f} ({'מפוזר' if hhi < 0.25 else 'מרוכז'}).")))
                if not tkr_y.empty:
                    w, l = tkr_y.idxmax(), tkr_y.idxmin()
                    _we, _le = _esc(w), _esc(l)  # escape: sheet-sourced tickers rendered as HTML (audit XSS)
                    # Ranked on ILS yield (Current_Value_ILS vs Cost_ILS) — labelled (₪)
                    # so it reads distinctly from the dashboard Winner/Loser card, which
                    # ranks on the origin-currency yield and can name a different ticker.
                    msgs.append(("🏆", _t(f"Best (₪): {_we} ({tkr_y[w]:+.1%}) · Worst (₪): {_le} ({tkr_y[l]:+.1%}).",
                                          f"מוביל (₪): {_we} ({tkr_y[w]:+.1%}) · מפגר (₪): {_le} ({tkr_y[l]:+.1%}).")))
                msgs.append(("🪙", _t(f"Crypto is {crypto_share:.0%} of the portfolio.",
                                      f"קריפטו מהווה {crypto_share:.0%} מהתיק.")))
                for icon, m in msgs:
                    st.markdown(f"<div style='padding:.45rem .7rem;margin:.3rem 0;border-radius:10px;"
                                f"background:rgba(99,102,241,.08);unicode-bidi:plaintext'>{icon} {m}</div>",
                                unsafe_allow_html=True)
        except Exception as exc:
            st.caption(_t("Insights unavailable.", "תובנות לא זמינות.") + f" ({str(exc)[:60]})")

    # ── 2. Rebalancing advisor ───────────────────────────────────────────
    with st.expander("⚖️ " + _t("Rebalancing Advisor", "יועץ איזון מחדש")):
        try:
            df = open_trades.copy()
            df["Current_Value_ILS"] = df.get("Current_Value_ILS", pd.Series(dtype=float)).map(_num)
            total_val = float(df["Current_Value_ILS"].sum())
            def _cls(row):
                t = _clean(row.get("Type", ""))
                if t == "קריפטו":
                    return _t("Crypto", "קריפטו")
                if "ETF" in _clean(row.get("Ticker", "")).upper() or t == "ETF":
                    return "ETF"
                return _t("Stocks", "מניות")
            if total_val <= 0:
                st.info(_t("No open positions.", "אין פוזיציות פתוחות."))
            else:
                df["_cls"] = df.apply(_cls, axis=1)
                cur = df.groupby("_cls")["Current_Value_ILS"].sum()
                classes = list(cur.index)
                st.caption(_t("Set your target % per class (must sum to ~100):",
                              "קבע אחוז יעד לכל סוג נכס (סכום ~100):"))
                cols = st.columns(len(classes))
                targets = {}
                default = round(100.0 / len(classes), 1)
                for i, c in enumerate(classes):
                    targets[c] = cols[i].number_input(c, min_value=0.0, max_value=100.0,
                                                      value=float(round(cur[c] / total_val * 100, 1)),
                                                      step=1.0, key=f"rebal_{c}")
                rows = []
                for c in classes:
                    cur_v = float(cur[c])
                    tgt_v = total_val * targets[c] / 100.0
                    delta = tgt_v - cur_v
                    # Wrap each ₪ amount in an LTR isolate (U+2066…U+2069) so the
                    # shekel sign + digits render as one left-to-right unit and do
                    # NOT get reordered (e.g. "109,440₪") inside the RTL table.
                    _iso = lambda s: "⁦" + s + "⁩"
                    rows.append({
                        _t("Class", "סוג"): c,
                        _t("Current", "נוכחי"): _iso(f"₪{cur_v:,.0f} ({cur_v/total_val:.0%})"),
                        _t("Target", "יעד"): _iso(f"₪{tgt_v:,.0f} ({targets[c]:.0f}%)"),
                        _t("Action", "פעולה"): (
                            (_t("Buy", "קנה") if delta > 0 else _t("Sell", "מכור")) + " " + _iso(f"₪{abs(delta):,.0f}")
                        ) if abs(delta) > total_val * 0.005 else _t("On target", "מאוזן"),
                    })
                st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
        except Exception as exc:
            st.caption(_t("Rebalancing unavailable.", "איזון לא זמין.") + f" ({str(exc)[:60]})")

    # ── 3. Dividend tracker ──────────────────────────────────────────────
    with st.expander("💰 " + _t("Dividend Tracker", "מעקב דיבידנדים")):
        try:
            divs = _load_dividends()
            with st.form("add_dividend", clear_on_submit=True):
                c1, c2, c3 = st.columns([1, 1, 1])
                d_tkr = c1.text_input(_t("Ticker", "טיקר"), key="div_tkr")
                d_amt = c2.number_input(_t("Amount (₪)", "סכום (₪)"), min_value=0.0, step=10.0, key="div_amt")
                d_date = c3.text_input(_t("Date (YYYY-MM-DD)", "תאריך"), key="div_date")
                if st.form_submit_button("➕ " + _t("Add", "הוסף")) and _clean(d_tkr) and d_amt > 0:
                    divs.append({"ticker": _clean(d_tkr).upper(), "amount": float(d_amt),
                                 "date": _clean(d_date) or "—"})
                    _save_dividends(divs)
                    st.rerun()
            if divs:
                ddf = pd.DataFrame(divs)
                total = float(ddf["amount"].sum())
                this_year = float(ddf[ddf["date"].astype(str).str[:4] == "2026"]["amount"].sum())
                m1, m2 = st.columns(2)
                m1.metric(_t("Total received", "סך התקבל"), f"₪{total:,.0f}")
                m2.metric(_t("This year (2026)", "השנה (2026)"), f"₪{this_year:,.0f}")
                by_t = ddf.groupby("ticker", as_index=False)["amount"].sum().sort_values("amount", ascending=False)
                st.dataframe(by_t.rename(columns={"ticker": _t("Ticker", "טיקר"), "amount": _t("Total (₪)", "סה\"כ (₪)")}),
                             width="stretch", hide_index=True)
            else:
                st.info(_t("No dividends recorded yet. Add one above.", "עדיין לא נרשמו דיבידנדים. הוסף למעלה."))
        except Exception as exc:
            st.caption(_t("Dividend tracker unavailable.", "מעקב דיבידנדים לא זמין.") + f" ({str(exc)[:60]})")


def _snapshot_ls():
    """Browser localStorage handle for the snapshot cache (phone: instant load /
    offline fallback). Instantiated per-run (the proven _sim_ls pattern) — must
    NOT be wrapped in st.cache_resource/cache_data, because LocalStorage is a
    Streamlit component and widgets inside cached functions raise
    CachedWidgetWarning."""
    # Per-session SINGLETON: read + save both call this in the same run, and creating
    # two LocalStorage(key="_pp_snapshot_ls") widgets in one run raised DuplicateWidgetID
    # and crashed first load. Instantiate once and reuse (matches the _sim_ls pattern). (audit bug-st)
    if "_pp_snapshot_ls_obj" in st.session_state:
        return st.session_state["_pp_snapshot_ls_obj"]
    inst = None
    try:
        from streamlit_local_storage import LocalStorage
        inst = LocalStorage(key="_pp_snapshot_ls")
    except Exception:
        inst = None
    st.session_state["_pp_snapshot_ls_obj"] = inst
    return inst


def _read_cached_snapshot_ls():
    ls = _snapshot_ls()
    if ls is None:
        return None
    try:
        raw = ls.getItem("pp_snapshot")
        if raw:
            d = pd.read_json(_pp_io.StringIO(raw))
            return d if (d is not None and not d.empty) else None
    except Exception:
        return None
    return None


def _save_cached_snapshot_ls(df) -> None:
    ls = _snapshot_ls()
    if ls is None or df is None or getattr(df, "empty", True):
        return
    try:
        js = df.to_json(force_ascii=False)
        k = "_pp_snap_save_" + str(abs(hash(js)) % 1_000_000)
        ls.setItem("pp_snapshot", js, key=k)
    except Exception:
        pass


def main() -> None:
    # Run one-time data migrations before anything else
    _run_data_migrations()

    # Determine saved language early so page_title can be bilingual
    _early_lang = LANG_HE
    try:
        _early_cfg = json.loads(LOCAL_SETTINGS_FILE.read_text(encoding="utf-8"))
        _early_lang = _early_cfg.get("language", LANG_HE)
    except Exception:
        pass
    _page_title = "Portfolio OS"
    st.set_page_config(page_title=_page_title, page_icon="📈", layout="wide", initial_sidebar_state="auto")

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
            /* Hide the app-running status widget on desktop — it shows as a
               small coloured dot/spinner on every rerun and confuses users.
               The JS injection in inject_client_fixes tries to hide it but can
               race; this CSS rule is the belt-and-suspenders guarantee. */
            [data-testid="stStatusWidget"] {
                display: none !important;
                visibility: hidden !important;
                opacity: 0 !important;
                pointer-events: none !important;
            }
        }
        /* Explicitly ensure the collapse/expand sidebar control AND the
           3-dots Main-Menu stay visible in all viewports. */
        [data-testid="stExpandSidebarButton"],
        [data-testid="stSidebarCollapseButton"],
        [data-testid="stExpandSidebarButton"],
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

                // Remove mobile's ~300 ms touch-delay before click fires.
                // touch-action:manipulation tells the browser "no double-tap-zoom on this element"
                // → click fires immediately on first tap.
                try {
                    el.style.touchAction = 'manipulation';
                    el.style.webkitTapHighlightColor = 'transparent';
                    el.style.cursor = 'pointer';
                } catch (e) {}

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

                // Mobile: touchend → synthesise a click ONCE if the natural click
                // doesn't follow within 80 ms. Catches cases where Streamlit's
                // pointer handlers swallow the first click on touch devices.
                var touchStartXY = null;
                el.addEventListener('touchstart', function (ev) {
                    if (ev.touches && ev.touches[0]) {
                        touchStartXY = [ev.touches[0].clientX, ev.touches[0].clientY];
                    }
                }, { passive: true });
                el.addEventListener('touchend', function (ev) {
                    if (!touchStartXY || !ev.changedTouches || !ev.changedTouches[0]) return;
                    var dx = Math.abs(ev.changedTouches[0].clientX - touchStartXY[0]);
                    var dy = Math.abs(ev.changedTouches[0].clientY - touchStartXY[1]);
                    touchStartXY = null;
                    // Only synthesise if it was a TAP (not a drag/scroll)
                    if (dx > 10 || dy > 10) return;
                    var clickFired = false;
                    var oneShot = function () { clickFired = true; el.removeEventListener('click', oneShot, true); };
                    el.addEventListener('click', oneShot, true);
                    setTimeout(function () {
                        if (!clickFired) {
                            try { el.click(); } catch (e) {}
                        }
                        try { el.removeEventListener('click', oneShot, true); } catch (e) {}
                    }, 80);
                }, { passive: true });

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
    # Dark-first product: a fresh/unset install defaults to (and renders) Dark, so
    # the Appearance control matches the actual dark render (see _resolve_theme_base).
    theme_default = _normalize_theme_mode(settings.get("theme_mode", THEME_DARK))

    if "demo_mode_persist" not in st.session_state:
        st.session_state["demo_mode_persist"] = bool(settings.get("demo_mode", False))

    language = st.sidebar.selectbox("Language" if language_default == LANG_EN else "שפה", [LANG_EN, LANG_HE], index=0 if language_default == LANG_EN else 1)
    tr = (lambda en, he: he if language == LANG_HE else en)

    # ── App lock (passcode + optional fingerprint gate) ──
    _lk = _lock_load()
    # Fingerprint just registered (from the sidebar) → enable biometric unlock.
    try:
        if st.query_params.get("_bioreg") == "ok":
            _lk["bio_enabled"] = True
            _lock_save(_lk)
            try:
                del st.query_params["_bioreg"]
            except Exception:
                pass
    except Exception:
        pass
    # SECURITY: the old `?_bio=ok` query-param unlock was a TRIVIAL bypass — anyone could
    # append ?_bio=ok to the public URL and unlock. Removed. The real WebAuthn flow unlocks
    # by clicking the hidden Streamlit bridge button (_bio_bridge_btn) after the on-device
    # fingerprint ceremony succeeds. NOTE: an in-app lock on a publicly-reachable single-tenant
    # Cloud URL is a DETERRENT, not a true confidentiality boundary (the server already holds
    # the data); for real protection use a private deployment / Streamlit auth / viewer allowlist.
    if (_lk.get("pin_hash") or _lk.get("bio_enabled")) and not st.session_state.get("_app_unlocked"):
        st.markdown(
            "<div style='max-width:380px;margin:14vh auto 1rem;text-align:center'>"
            "<div style='font-size:3rem;line-height:1'>🔒</div>"
            f"<h2 style='margin:.4rem 0'>{tr('Locked', 'נעול')}</h2>"
            f"<div style='opacity:.7'>{tr('Enter your passcode to continue', 'הזן את קוד הגישה כדי להמשיך')}</div></div>",
            unsafe_allow_html=True,
        )
        _lc1, _lc2, _lc3 = st.columns([1, 2, 1])
        with _lc2:
            if _lk.get("pin_hash"):
                _pin = st.text_input(tr("Passcode", "קוד גישה"), type="password", key="_lock_pin_input",
                                     label_visibility="collapsed", placeholder=tr("Passcode", "קוד גישה"))
                if st.button(tr("Unlock", "פתיחה"), key="_lock_unlock_btn", use_container_width=True, type="primary"):
                    if _pin and _lock_hash(_pin, _lk.get("salt", "")) == _lk.get("pin_hash"):
                        st.session_state["_app_unlocked"] = True
                        st.rerun()
                    else:
                        st.error(tr("Wrong passcode", "קוד שגוי"))
            if _lk.get("bio_enabled"):
                components.html(_bio_unlock_html(
                    tr("Unlock with fingerprint", "פתיחה עם טביעת אצבע"),
                    tr("Fingerprint failed", "טביעת אצבע נכשלה"),
                    tr("No fingerprint enrolled", "לא נרשמה טביעת אצבע")), height=96)
                # Hidden bridge: WebAuthn JS clicks this (it cannot navigate the parent iframe).
                st.markdown("<style>.st-key-_bio_bridge_btn{display:none !important;}</style>", unsafe_allow_html=True)
                if st.button("biounlock_bridge", key="_bio_bridge_btn"):
                    st.session_state["_app_unlocked"] = True
                    st.rerun()
        st.stop()

    theme_label_to_value = {
        tr("System", "מערכת"): THEME_SYSTEM,
        tr("Light", "בהיר"): THEME_LIGHT,
        tr("Dark", "כהה"): THEME_DARK,
    }
    theme_value_to_label = {v: k for k, v in theme_label_to_value.items()}
    default_theme_label = theme_value_to_label.get(theme_default, tr("System", "מערכת"))
    # KEY + session_state (NOT a dynamic index=). A keyless selectbox whose index is
    # derived from a value the widget itself persists lags by one interaction — the
    # FIRST theme switch did nothing, only the second worked. With a stable key the
    # widget owns its state and the change applies on the SAME rerun. The key is seeded
    # from the saved preference once per session; the label list is language-dependent,
    # so re-seed if the stored label isn't a valid option (e.g. after a language switch).
    _appearance_key = "appearance_label_key"
    if (_appearance_key not in st.session_state
            or st.session_state.get(_appearance_key) not in theme_label_to_value):
        st.session_state[_appearance_key] = default_theme_label
    appearance_label = st.sidebar.selectbox(
        tr("Appearance", "תצוגה"),
        list(theme_label_to_value.keys()),
        key=_appearance_key,
    )
    theme_mode = theme_label_to_value.get(appearance_label, THEME_SYSTEM)
    # ── ⚙ Settings: version + chart/demo toggles + App-lock (passcode + fingerprint) ──
    # Single grouped Settings expander (rendered here so the chart_lock/demo values it
    # writes are available to the main content read below, with no one-rerun lag).
    # ⚙ Settings moved to the BOTTOM of the sidebar (below Data + AI) — rendered by the
    # tabbed _render_sidebar_settings() panel near the end of this function.

    # Auto-persist language + theme so ALL connected devices (phone, tablet, desktop)
    # share the same preference on next page load — writes to the shared server-side JSON.
    if language != language_default or theme_mode != theme_default:
        try:
            save_local_settings(
                settings.get("web_app_url", ""),
                settings.get("api_token", ""),
                settings.get("spreadsheet_ref", ""),
                settings.get("worksheet_name", DEFAULT_WORKSHEET_NAME),
                settings.get("service_account_file", str(DEFAULT_SERVICE_ACCOUNT_FILE)),
                language,
                theme_mode,
                bool(settings.get("demo_mode", False)),
                settings.get("followed_symbols", ""),
            )
        except Exception:
            pass
    # chart_lock / demo VALUES are read here (main content below needs them), but the
    # actual checkboxes now render inside the bottom ⚙ Settings expander. Reading from
    # session_state keeps the value available at the top; a toggle applies on the
    # immediately-following rerun (a checkbox click always reruns anyway).
    _default_chart_lock = _is_mobile_client()
    chart_lock = bool(st.session_state.get("chart_lock_persist", _default_chart_lock))
    demo_mode = bool(st.session_state.get("demo_mode_persist", False))
    # Live updates always-on: fragment refreshes every 30s without full-page reload
    live_updates = True
    refresh_seconds = 30

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
        // Collapse PHANTOM containers that render at 0 height but still add flex-gap
        // space above the sticky hero — the dead space that appears after navigating
        // to a page (e.g. the AI agent) and back. Only targets style/script-only
        // markdown injections and attribute-less 0-height JS-effect iframes, so the
        // visible TradingView widget and still-loading charts are never touched.
        function killPhantoms() {{
          var main = d.querySelector('section.main') || d;
          main.querySelectorAll('[data-testid="stElementContainer"]').forEach(function(el){{
            var ifr = el.querySelector(':scope > iframe[data-testid="stIFrame"]');
            var phantomIframe = ifr && ifr.offsetHeight === 0 && !ifr.getAttribute('height');
            var md = el.querySelector(':scope > [data-testid="stMarkdown"] > [data-testid="stMarkdownContainer"]');
            var onlyStyle = md && md.children.length > 0 &&
              Array.prototype.every.call(md.children, function(c){{ return c.tagName==='STYLE'||c.tagName==='SCRIPT'; }}) &&
              !((md.innerText||'').trim());
            // NON-PERMANENT: re-evaluate every tick. Mark with data-pp-phantom (not a
            // permanent ppHidden skip) so a container that was momentarily empty during a
            // SLOW cold-start render and got hidden is RESTORED the moment it gains real
            // content — this is what made the hero/page content vanish on Android cold start
            // until a manual refresh. Only ever un-hide containers WE hid. (android cold-start fix)
            if (phantomIframe || onlyStyle) {{ el.style.display='none'; el.dataset.ppPhantom='1'; }}
            else if (el.dataset.ppPhantom) {{ el.style.removeProperty('display'); el.removeAttribute('data-pp-phantom'); }}
          }});
        }}
        function tick() {{ applyLock(); killPhantoms(); }}
        tick();
        // SINGLE observer. This components.html re-emits every rerun; the old code created
        // a NEW MutationObserver on d.body{{subtree}} each time and never disconnected, so
        // observers piled up across reruns until the page choked and TOUCH (swipe) stopped
        // working. Disconnect any prior observer first → only ever one, with the fresh
        // _lock_action closure. (perf leak / swipe-stops-after-a-while fix)
        var W = d.defaultView || window.parent || window;
        try {{ if (W.__ppChartLockObs) W.__ppChartLockObs.disconnect(); }} catch(e){{}}
        var obs = new MutationObserver(function(){{ requestAnimationFrame(tick); }});
        obs.observe(d.body, {{childList:true, subtree:true}});
        W.__ppChartLockObs = obs;
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
    page_risk = tr("Risk & FIFO", "סיכונים ו-" + _mix_he_with_ltr("FIFO")) if not _is_mobile_client() else tr("Risk", "סיכון")
    page_simulator = tr("Simulator", "סימולטור")
    page_quality = tr("Data Quality", "בקרת נתונים")
    page_chat = tr("AGENT AI", "סוכן AI")
    page_id_to_label = {
        "dashboard": page_dashboard,
        "manage": page_manage,
        "risk": page_risk,
        "simulator": page_simulator,
        "quality": page_quality,
        "chat": page_chat,
    }
    page_order = ["dashboard", "manage", "risk", "simulator", "quality", "chat"]
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
        # #10 — clean text labels; a consistent Material Symbols icon is added
        # before each via CSS ::before (keyed by position) instead of mixed
        # emoji 📊💼🛡🧮. st.radio renders :material/x: as literal text, so we
        # keep the label text icon-free and let the icon font do the glyph.
        # Degrades gracefully to clean text if the icon font ever fails.
        mobile_label_to_id = {
            tr("Overview", "סקירה"): "dashboard",
            tr("Trades", "עסקאות"): "manage",
            tr("Risk", "סיכון"): "risk",
            tr("Sim", "סימולטור"): "simulator",
        }
        mobile_options = list(mobile_label_to_id.keys())

        # The 4-pill nav is ALWAYS shown. On pages that aren't one of the pills
        # (Data / Agent, entered via the sidebar), NO pill is highlighted
        # (index=None) — and clicking any pill navigates straight there. The key
        # embeds the active page so the widget re-initialises (honouring the
        # index, including None) whenever navigation comes from outside the pills.
        active_mobile_index = next(
            (i for i, lbl in enumerate(mobile_options)
             if mobile_label_to_id.get(lbl) == active_page_id),
            None,
        )
        page_choice = st.radio(
            tr("Page", "עמוד"),
            mobile_options,
            horizontal=True,
            label_visibility="collapsed",
            index=active_mobile_index,
            # The key MUST embed active_page_id: a Streamlit keyed widget's stored
            # session_state value overrides index= on rerun, so a static key freezes
            # the selection and (when navigating to a non-pill page, index=None) the
            # stale value forces st.rerun() back to the old pill — making page nav
            # impossible. Re-keying per page remounts the widget so index is honored. (regression fix)
            key=f"mobile_page_nav_radio_{active_page_id}",
        )
        _chosen_id = mobile_label_to_id.get(page_choice) if page_choice else None
        if _chosen_id and _chosen_id != active_page_id:
            st.session_state["active_page_id"] = _chosen_id
            st.rerun()
        elif _chosen_id:
            active_page_id = _chosen_id
    else:
        # ── Desktop: sidebar option-menu navigation ──
        st.sidebar.title(tr("Navigation", "ניווט"))
        page_options = [page_id_to_label[p] for p in page_order]
        active_page_index = page_order.index(active_page_id) if active_page_id in page_order else 0
        if option_menu is not None:
            # Reset stored option_menu value when language changes (labels changed).
            # Without this, option_menu falls back to index 0 instead of the correct page.
            _nav_key = "main_nav_option_menu"
            if st.session_state.get(_nav_key) not in page_options:
                st.session_state[_nav_key] = page_options[active_page_index]
            nav_container_bg = "#1E1E1E" if is_dark else "#f8f9fa"
            nav_container_border = "0px solid transparent"
            nav_icon_color = "#93c5fd" if is_dark else "#2563eb"
            nav_text_color = "#e2e8f0" if is_dark else "#0f172a"
            nav_hover_color = "#334155" if is_dark else "#e5e7eb"
            # Selected item = solid accent-blue pill with white text, in BOTH themes.
            # (Previously a theme-conditional light/grey bg could end up light-on-light
            # — e.g. under "system" theme or a theme-detection mismatch — making the
            # clicked page's label nearly invisible. A saturated pill is always legible.)
            nav_selected_bg = "#4f46e5"
            nav_selected_text = "#ffffff"
            with st.sidebar:
                # NOTE: streamlit-option-menu requires a stable `key` so the
                # component's selected state persists across reruns. Without a
                # key each rerun rebuilds the component from default_index and
                # "swallows" the first click (classic double-click bug).
                # RTL-aware: in Hebrew the nav must be RTL so (a) the Latin
                # tokens ("FIFO") render on the correct side (bidi) and (b) the
                # icon leads on the RIGHT next to the label — not detached left.
                _nav_dir = "rtl" if language == LANG_HE else "ltr"
                _nav_align = "right" if language == LANG_HE else "left"
                page = option_menu(
                    menu_title=None,
                    options=page_options,
                    icons=["house", "wallet", "shield-check", "calculator", "database-check", "robot"],
                    default_index=active_page_index,
                    key="main_nav_option_menu",
                    orientation="vertical",
                    styles={
                        "container": {
                            "padding": "0.32rem 0.2rem",
                            "background-color": nav_container_bg,
                            "border-radius": "10px",
                            "border": nav_container_border,
                            "direction": _nav_dir,
                        },
                        "icon": {"color": nav_icon_color, "font-size": "16px",
                                 "margin": "0 0 0 8px" if language == LANG_HE else "0 8px 0 0"},
                        "nav-link": {
                            "font-size": "14px",
                            "text-align": _nav_align,
                            "direction": _nav_dir,
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
                            "text-align": _nav_align,
                            "direction": _nav_dir,
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

    # ── Per-page accent color (color-coded navigation) ─────────────────
    # Dashboard is BLUE (per spec). Each page has its own accent that
    # propagates to: header border, sidebar nav-link, mobile pill, metric
    # left-bar, primary buttons, focus rings, sub-tab underline, expander
    # header, dataframe header. Designed to make orientation instant and
    # visually reinforce the "where am I" feeling.
    _page_accents = {
        "dashboard": "#6366f1",   # indigo — overview (per spec)
        "manage":    "#10b981",   # emerald — active portfolio actions
        "risk":      "#ef4444",   # rose — risk / warning family
        "simulator": "#f59e0b",   # amber — exploration / projection
        "quality":   "#06b6d4",   # cyan — data hygiene
        "chat":      "#eab308",   # yellow — AI agent
    }
    # Per-accent dark variant (for gradient stops, hover, subtle bg tint)
    _page_accents_dark = {
        "dashboard": "#1e40af",
        "manage":    "#047857",
        "risk":      "#b91c1c",
        "simulator": "#b45309",
        "quality":   "#0e7490",
        "chat":      "#a16207",
    }
    _accent = _page_accents.get(active_page_id, "#6366f1")
    _accent_dark = _page_accents_dark.get(active_page_id, "#1e40af")

    # Per user preference: the HERO uses the VIVID page accents (brighter is
    # the look they want) — no desaturation. A light scrim below keeps the
    # white title legible on the lighter accents without dulling the colour.
    _hero_accent = _accent
    _hero_accent_dark = _accent_dark
    st.markdown(
        f"""
        <style id="pp-page-accent">
            /* ===== Premium gradient HERO header (page-aware) ===== */
            @keyframes ppHeroSheen {{ 0% {{ transform: translateX(-60%) rotate(12deg); opacity:0; }}
                                      15% {{ opacity:.6; }} 60% {{ opacity:.25; }}
                                      100% {{ transform: translateX(220%) rotate(12deg); opacity:0; }} }}
            .app-header-wrap {{
                background:
                    radial-gradient(130% 150% at 90% -25%, rgba(255,255,255,.24) 0%, rgba(255,255,255,0) 42%),
                    linear-gradient(180deg, rgba(15,23,42,.05) 0%, rgba(15,23,42,.14) 100%),
                    linear-gradient(118deg, {_hero_accent_dark} 0%, {_hero_accent} 60%, {_hero_accent}e6 100%) !important;
                border: none !important;
                border-top: none !important;
                border-bottom: none !important;
                border-radius: 0 0 24px 24px !important;
                box-shadow: 0 18px 42px -18px {_hero_accent}99,
                            0 1px 0 0 rgba(255,255,255,.22) inset !important;
                padding: 0.9rem 1.35rem 0.85rem 1.35rem !important;
                position: sticky !important;
                overflow: hidden !important;
            }}
            /* moving light sweep across the hero */
            .app-header-wrap::before {{
                content:""; position:absolute; top:0; left:0; height:100%; width:45%;
                background: linear-gradient(90deg, rgba(255,255,255,0) 0%, rgba(255,255,255,.28) 50%, rgba(255,255,255,0) 100%);
                transform: translateX(-60%) rotate(12deg); pointer-events:none;
                animation: ppHeroSheen 7s ease-in-out 1.2s infinite;
            }}
            .app-main-title {{
                background: none !important;
                -webkit-background-clip: border-box !important;
                background-clip: border-box !important;
                -webkit-text-fill-color: #ffffff !important;
                color: #ffffff !important;
                font-weight: 850 !important;
                letter-spacing: -.022em !important;
                text-shadow: 0 2px 20px rgba(0,0,0,.20) !important;
            }}
            .app-sub-title {{
                color: rgba(255,255,255,.94) !important;
                font-weight: 700 !important;
                letter-spacing: .06em !important;
                opacity: .92 !important;
            }}
            .app-logo-badge {{
                /* Dark glass chip — a translucent-white pill failed WCAG badly over
                   the vivid hero (white-on-pill CR 1.71 on the amber/yellow accents,
                   3.30 on indigo). A slate fill at .46 lifts worst-case contrast to
                   ~5.1:1 across ALL six page accents while keeping the glassy look. */
                background: rgba(15,23,42,.46) !important;
                border: 1px solid rgba(255,255,255,.30) !important;
                color: #ffffff !important;
                text-shadow: 0 1px 2px rgba(0,0,0,.28) !important;
                backdrop-filter: blur(6px) !important;
                -webkit-backdrop-filter: blur(6px) !important;
                font-weight: 650 !important;
            }}
            .app-logo-icon {{
                background: rgba(255,255,255,.15) !important;
                border: 1px solid rgba(255,255,255,.28) !important;
                border-radius: 16px !important;
                box-shadow: 0 8px 22px -10px rgba(0,0,0,.35) !important;
            }}

            /* Sidebar active nav-link */
            [data-testid="stSidebar"] .nav-link-selected,
            [data-testid="stSidebar"] .nav-link.active {{
                border-left: 3px solid {_accent} !important;
                box-shadow: inset 3px 0 0 0 {_accent} !important;
                background-color: {_accent}14 !important;
                color: {_accent} !important;
            }}

            /* Mobile pill accent — tinted glow that matches the page */
            [data-testid="stRadio"] [data-baseweb="radio"]:has(input:checked) {{
                box-shadow: 0 4px 14px -2px {_accent}99 !important;
            }}
            @media (max-width: 820px) {{
                [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(4):last-child)
                [data-baseweb="radio"]:has(input:checked),
                [data-testid="stRadio"]:has([role="radiogroup"] > [data-baseweb="radio"]:nth-child(5):last-child)
                [data-baseweb="radio"]:has(input:checked) {{
                    background: linear-gradient(135deg, {_accent_dark} 0%, {_accent} 60%, {_accent}cc 100%) !important;
                    box-shadow: 0 4px 14px -3px {_accent}88,
                                0 1px 0 0 rgba(255,255,255,0.10) inset !important;
                }}
                /* Tabs active state matches accent */
                [data-baseweb="tab-list"] [data-baseweb="tab"][aria-selected="true"] {{
                    background: linear-gradient(135deg, {_accent_dark} 0%, {_accent} 60%, {_accent}cc 100%) !important;
                    box-shadow: 0 4px 12px -3px {_accent}88 !important;
                }}
                /* Primary buttons — accent gradient (mobile) */
                .stButton > button[kind="primary"],
                .stButton > button[data-testid="baseButton-primary"],
                button[kind="primary"] {{
                    background: linear-gradient(135deg, {_accent_dark} 0%, {_accent} 55%, {_accent}cc 100%) !important;
                    box-shadow: 0 4px 14px -3px {_accent}88,
                                0 1px 0 0 rgba(255,255,255,0.10) inset !important;
                }}
                /* Secondary button hover gets the accent border */
                .stButton > button:not([kind="primary"]):hover,
                .stDownloadButton > button:not([kind="primary"]):hover {{
                    border-color: {_accent}88 !important;
                    color: {_accent} !important;
                    box-shadow: 0 2px 6px -1px {_accent}33,
                                0 4px 10px -3px rgba(15,23,42,0.10) !important;
                }}
            }}

            /* Sub-tab underline (active tab) */
            [data-baseweb="tab-list"] [data-baseweb="tab"][aria-selected="true"] {{
                border-bottom: 2px solid {_accent} !important;
            }}

            /* Metric left accent bar */
            [data-testid="stMetric"]::before {{
                content: "";
                position: absolute;
                top: 12%; bottom: 12%; left: 0;
                width: 3px;
                background: linear-gradient(180deg, {_accent} 0%, {_accent_dark} 100%);
                border-radius: 3px;
            }}
            /* Metric values + delta numbers stay NEUTRAL (per user request).
               Per-page accent is applied only to the bar/border/header. */

            /* Expander header — subtle accent on hover + coloured caret */
            details[data-testid="stExpander"]:hover,
            div[data-testid="stExpander"]:hover {{
                border-color: {_accent}88 !important;
            }}
            /* Expander SVG accent: main content only — NOT sidebar.
               The sidebar chevrons are row-reversed to the left side and
               a coloured arrow near the top looks like a stray dot to users. */
            section.main details[data-testid="stExpander"] svg,
            section.main div[data-testid="stExpander"] svg,
            .main details[data-testid="stExpander"] svg,
            .main div[data-testid="stExpander"] svg {{
                color: {_accent} !important;
            }}

            /* DataFrame header strip */
            [data-testid="stDataFrame"] [role="columnheader"] {{
                border-top: 2px solid {_accent}55 !important;
            }}

            /* Focus rings: apply accent only in main content, NOT sidebar */
            section.main :focus-visible,
            .main :focus-visible {{
                outline-color: {_accent} !important;
            }}

            /* Scrollbar thumb — accent tinted */
            ::-webkit-scrollbar-thumb {{
                background: {_accent}66 !important;
            }}
            ::-webkit-scrollbar-thumb:hover {{
                background: {_accent}aa !important;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ── Design-critique corrective CSS (addresses critique #4/#5/#9/#11) ──
    _crit_css = (
        "<style id='pp-crit-fixes'>"
        # #9 hero: trim vertical waste — smaller padding + title (but DO NOT pull
        # it up under the nav; user wants the colored title banner fully visible
        # below the nav pills, so no margin-top:0 / container padding-top change).
        "html body .app-header-wrap{padding:.62rem 1.1rem .56rem 1.1rem !important;border-radius:0 0 18px 18px !important;}"
        "html body .app-main-title{font-size:1.5rem !important;}"
        "html body .app-header-wrap .app-logo-icon svg{width:40px !important;height:40px !important;}"
        # #11 content not hidden behind the sticky nav
        ".main .block-container{scroll-margin-top:84px !important;}"
        # #5 sidebar opaque drawer — theme-aware bg + matching text appended just below.
    )
    # Theme-aware sidebar drawer: dark bg + light text in dark mode; soft-light bg +
    # dark text in light mode. (The earlier all-WHITE sidebar was the data-pp-collapsed
    # content-hide race — removed r17 — NOT the light colour itself, so light mode is safe.)
    _sb_bg = "#0f172a" if is_dark else "#f3f5fa"
    _sb_txt = "#e2e8f0" if is_dark else "#0f172a"
    _crit_css += (
        f"[data-testid='stSidebar']{{background:{_sb_bg} !important;backdrop-filter:none !important;}}"
        # labels, captions, markdown AND selectbox values follow the drawer text colour
        "[data-testid='stSidebar'] label,[data-testid='stSidebar'] p,[data-testid='stSidebar'] span,"
        "[data-testid='stSidebar'] h1,[data-testid='stSidebar'] h2,[data-testid='stSidebar'] h3,"
        "[data-testid='stSidebar'] [data-testid='stWidgetLabel'],[data-testid='stSidebar'] [data-baseweb='select'] *,"
        f"[data-testid='stSidebar'] [data-testid='stMarkdownContainer']{{color:{_sb_txt} !important;}}"
        # EXCEPTION: primary (dark accent) buttons keep WHITE text in BOTH themes —
        # otherwise the force-colour above makes their label invisible on the dark button.
        "[data-testid='stSidebar'] button[kind='primary'],[data-testid='stSidebar'] button[kind='primary'] *,"
        "[data-testid='stSidebar'] button[data-testid='baseButton-primary'],"
        "[data-testid='stSidebar'] button[data-testid='stBaseButton-primary'],"
        "[data-testid='stSidebar'] button[data-testid='baseButton-primary'] *,"
        "[data-testid='stSidebar'] button[data-testid='stBaseButton-primary'] *"
        "{color:#ffffff !important;}"
    )
    # The two dedicated sidebar nav buttons (נתונים / סוכן AI) rendered dark-navy in
    # LIGHT mode, which — with the dark sidebar text above — made their label invisible.
    # Force them theme-aware by their stable st.key classes (highest-confidence target),
    # bg + text + border together so they read correctly in BOTH themes when NOT active.
    _sbtn_bg = "#1e293b" if is_dark else "#ffffff"
    _sbtn_txt = "#e2e8f0" if is_dark else "#0f172a"
    _sbtn_bd = "#334155" if is_dark else "#dbe2ec"
    _crit_css += (
        "html body [data-testid='stSidebar'] .st-key-sidebar_data_nav_btn button:not([kind='primary']),"
        "html body [data-testid='stSidebar'] .st-key-sidebar_chat_nav_btn button:not([kind='primary'])"
        f"{{background:{_sbtn_bg} !important;border:1px solid {_sbtn_bd} !important;}}"
        "html body [data-testid='stSidebar'] .st-key-sidebar_data_nav_btn button:not([kind='primary']) *,"
        "html body [data-testid='stSidebar'] .st-key-sidebar_chat_nav_btn button:not([kind='primary']) *"
        f"{{color:{_sbtn_txt} !important;}}"
    )
    # Main app surface MUST follow the theme too. config.toml base="dark" paints the
    # native shell dark (great for dark mode / no white flash), but in LIGHT mode that
    # left a dark canvas under dark text => invisible content. Force the outer
    # containers to the theme base so light mode is actually light. (section.main keeps
    # its premium gradient on top; this is the flat base behind it.)
    _app_bg = "#0b1120" if is_dark else "#f6f8fd"
    _crit_css += (
        f"[data-testid='stApp'],[data-testid='stAppViewContainer'],"
        f"[data-testid='stMain'],section.main > div.block-container{{background-color:{_app_bg} !important;}}"
    )
    if is_mobile:
        # dim the page behind the open mobile drawer
        _crit_css += "[data-testid='stSidebar']{box-shadow:0 0 0 100vmax rgba(0,0,0,.5) !important;}"
        # #4 — kill the big empty gap between the back-breadcrumb and the hero on
        # the Data / Agent pages (the breadcrumb container carried excess block
        # gap + bottom margin, pushing the hero ~115px down).
        _crit_css += (
            ".st-key-pp_breadcrumb{margin:0 0 -0.4rem 0 !important;padding:0 !important;gap:0 !important;}"
            ".st-key-pp_breadcrumb [data-testid='stHorizontalBlock']{margin-bottom:0 !important;}"
            ".st-key-pp_breadcrumb + div,.st-key-pp_breadcrumb ~ [data-testid='stElementContainer']:first-of-type{margin-top:0 !important;}"
            # Regression fix: keep the back button clear of the fixed >> collapse
            # control (top-left) and never truncate its label.
            ".st-key-pp_breadcrumb{margin-inline-start:54px !important;}"
            ".st-key-pp_breadcrumb button{width:auto !important;white-space:nowrap !important;overflow:visible !important;text-overflow:clip !important;}"
            ".st-key-pp_breadcrumb button p{overflow:visible !important;text-overflow:clip !important;}"
        )
        # #10 — consistent Material Symbols icon before each nav pill label
        #       (positional ::before; labels themselves are icon-free text).
        # Scope STRICTLY to the 4-pill nav (a radiogroup with exactly 4 options)
        # so the icons do NOT leak into other radios (Edit/Delete, Gemini/Claude).
        _NAV4 = "[data-testid='stRadio']:has([role='radiogroup'] > [data-baseweb='radio']:nth-child(4):last-child)"
        _crit_css += (
            _NAV4 + " [role='radiogroup'] label [data-testid='stMarkdownContainer'] p::before{"
            "font-family:'Material Symbols Rounded','Material Symbols Outlined','Material Symbols Sharp' !important;"
            "font-weight:normal !important;font-style:normal !important;"
            "font-feature-settings:'liga' 1 !important;-webkit-font-feature-settings:'liga' 1 !important;"
            "-webkit-font-smoothing:antialiased;font-size:1rem;line-height:1;"
            "vertical-align:-3px;margin-inline-end:4px;display:inline-block;flex:0 0 auto;}"
            + _NAV4 + " [role='radiogroup'] > label:nth-child(1) [data-testid='stMarkdownContainer'] p::before{content:'insights';}"
            + _NAV4 + " [role='radiogroup'] > label:nth-child(2) [data-testid='stMarkdownContainer'] p::before{content:'account_balance_wallet';}"
            + _NAV4 + " [role='radiogroup'] > label:nth-child(3) [data-testid='stMarkdownContainer'] p::before{content:'shield';}"
            + _NAV4 + " [role='radiogroup'] > label:nth-child(4) [data-testid='stMarkdownContainer'] p::before{content:'calculate';}"
        )
    if is_dark:
        # #4 dark-mode body/caption contrast (push muted text toward WCAG AA)
        _crit_css += (
            "[data-testid='stCaptionContainer'],[data-testid='stCaptionContainer'] *,"
            "[data-testid='stMarkdownContainer'] p,.stMarkdown p{color:#cbd5e1 !important;}"
        )
        # Dark-mode checkboxes: the empty box was nearly invisible on the navy
        # sidebar (read as a bare label). Give it a visible border + fill.
        _crit_css += (
            "html body [data-baseweb='checkbox'] [data-testid='stCheckbox'] > label > span:first-child,"
            "html body [data-testid='stCheckbox'] [data-baseweb='checkbox'] span[data-checked='false'],"
            "html body [data-testid='stCheckbox'] svg{outline:none;}"
            "html body [data-testid='stCheckbox'] [data-baseweb='checkbox'] > span:first-child,"
            "html body [data-testid='stSidebar'] [data-baseweb='checkbox'] > span:first-child{"
            "background:#1e293b !important;border:1.5px solid #64748b !important;border-radius:5px !important;}"
        )
    # ── Metric (KPI) cards: real card↔background contrast (#3), 4px accent (#6)
    #    on the correct side (#7 RTL), darker label (#9). Uses html-body-prefixed
    #    selectors so it OVERRIDES style_metric_cards (which my earlier 1-attr
    #    selector lost to — the 8px left stripe never moved).
    _mc_bg = "#232e47" if is_dark else "#ffffff"
    _mc_bd = "#2e3a55" if is_dark else "#e5e7ef"
    _mc_lbl = "#e2e8f0" if is_dark else "#5b5c63"
    _crit_css += (
        "html body [data-testid='stMetric']{background:" + _mc_bg + " !important;"
        "border:1px solid " + _mc_bd + " !important;border-radius:14px !important;"
        "box-shadow:0 1px 3px rgba(15,23,42,.10) !important;}"
        # DOUBLE-STRIPE fix (#7): the design overlay draws a 3px indigo→cyan
        # gradient bar via ::before on the LEFT. With our own single border
        # accent, that ::before is a second stripe — kill it.
        "html body [data-testid='stMetric']::before{display:none !important;}"
        "html body [data-testid='stMetricLabel'],html body [data-testid='stMetricLabel'] *{color:" + _mc_lbl + " !important;}"
        # #7 — lock the KPI value to LTR so "-87,370" never reorders in RTL.
        "html body [data-testid='stMetricValue']{direction:ltr !important;unicode-bidi:isolate !important;}"
        + ("html body [data-testid='stMetricValue']{text-align:right !important;}" if language == LANG_HE else "")
        # #8 contrast — dashboard stats sub-line was ~4.0:1 (grey on near-white).
        + ("html body .dashboard-stats-line{color:#475569 !important;}" if not is_dark
           else "html body .dashboard-stats-line{color:#cbd5e1 !important;}")
    )
    # #2 — EN dashboard sub-tabs (Overview/Allocation/Reports/Transactions) were
    # clipping ("Transaction" lost its s). Let the tab strip scroll horizontally
    # with no clip, tabs never shrink.
    _crit_css += (
        "html body [data-baseweb='tab-list']{overflow-x:auto !important;overflow-y:hidden !important;"
        "scrollbar-width:none !important;-webkit-overflow-scrolling:touch !important;flex-wrap:nowrap !important;gap:6px !important;}"
        "html body [data-baseweb='tab-list']::-webkit-scrollbar{display:none !important;}"
        # Breathing room between tabs (they were touching on desktop).
        "html body [data-baseweb='tab-list'] [data-baseweb='tab']{flex:0 0 auto !important;white-space:nowrap !important;margin-right:2px !important;padding-left:14px !important;padding-right:14px !important;}"
        "html body [data-baseweb='tab-list'] [data-baseweb='tab'] p{white-space:nowrap !important;overflow:visible !important;text-overflow:clip !important;}"
        # Dead-space fix: style-only st.markdown injections (pp-crit-fixes,
        # pp-page-accent, etc.) and zero-height component iframes render as
        # height-0 element containers that STILL consume the vertical-block gap.
        # After visiting the chat page ~17 of them stack above the hero → a tall
        # dead gap. Collapse them (a <style> keeps applying even under
        # display:none; the component JS keeps running). This zeroes the gap.
        "html body [data-testid='stMain'] [data-testid='stElementContainer']:has(> [data-testid='stMarkdownContainer'] style),"
        "html body [data-testid='stMain'] [data-testid='stElementContainer']:has(> [data-testid='stMarkdownContainer'] > style),"
        "html body [data-testid='stMain'] [data-testid='stElementContainer']:has(style),"
        "html body [data-testid='stMain'] [data-testid='stElementContainer']:has(> [data-testid='stIFrame'] iframe[height='0']),"
        "html body [data-testid='stMain'] [data-testid='stElementContainer']:has(> iframe[height='0']){display:none !important;}"
    )
    # #8 — commit to one rule: indigo is the GLOBAL selection primary; section
    # colours stay decorative (hero only). So every selected radio gets an
    # indigo glow (not the section's green/amber), and the active nav pill is
    # indigo on every page — no more "purple control inside a green section".
    _crit_css += (
        "html body [data-testid='stRadio'] [data-baseweb='radio']:has(input:checked){box-shadow:0 4px 12px -3px rgba(79,70,229,.45) !important;}"
        "html body [data-testid='stRadio']:has([role='radiogroup'] > [data-baseweb='radio']:nth-child(4):last-child) [data-baseweb='radio']:has(input:checked){background:#4f46e5 !important;}"
    )
    # Accent on the START side. The metric container is dir:ltr even in Hebrew,
    # so border-inline-start would wrongly sit left in HE — use explicit physical
    # sides per language (verified). border:1px above keeps the other sides thin.
    if language == LANG_HE:
        # Force the card RTL so the accent sits on the START side = RIGHT in
        # Hebrew (the critic measured it stuck on the left). Kill BOTH physical
        # borders first so style_metric_cards' left bar can't survive.
        _crit_css += ("html body [data-testid='stMetric']{direction:rtl !important;"
                      "border-left:1px solid " + _mc_bd + " !important;"
                      "border-right:4px solid #4f46e5 !important;}")
    else:
        _crit_css += ("html body [data-testid='stMetric']{"
                      "border-right:1px solid " + _mc_bd + " !important;"
                      "border-left:4px solid #4f46e5 !important;}")
    # Hero page-label bidi: render per content direction so "סיכונים ו-FIFO"
    # reads correctly (the FSI/PDI marks need an RTL base to land right).
    _crit_css += "html body .app-sub-title{unicode-bidi:plaintext !important;}"
    if language == LANG_HE:
        # KPI card ORDER must reverse in RTL — Total Value (first) belongs on
        # the RIGHT, not the left. direction:rtl on the metrics row flips the
        # flex column order.
        _crit_css += ("html body [data-testid='stHorizontalBlock']:has([data-testid='stMetric']){direction:rtl !important;}")
    # Delta pill: NEUTRAL background + Streamlit's NATIVE sign-based text color
    # (green = gain, red = loss). The old rule forced green text+background on EVERY
    # delta, so losses showed up green — a critical misread of P&L. (audit C6)
    _bdg_bg = "rgba(148,163,184,.16)" if is_dark else "#f1f5f9"
    _crit_css += ("html body [data-testid='stMetricDelta']{background:" + _bdg_bg + " !important;border-radius:8px !important;padding:1px 7px !important;}")
    # #14 — the Closed-Positions "N open" delta is a NEUTRAL count, not a gain:
    # override the green pill with a muted grey chip and hide the up-arrow.
    _ncol = "#94a3b8" if is_dark else "#64748b"
    _nbg = "rgba(148,163,184,.16)" if is_dark else "#f1f5f9"
    _crit_css += (
        "html body .st-key-kpi_closed [data-testid='stMetricDelta']{background:" + _nbg + " !important;}"
        "html body .st-key-kpi_closed [data-testid='stMetricDelta'] *{color:" + _ncol + " !important;fill:" + _ncol + " !important;}"
        "html body .st-key-kpi_closed [data-testid='stMetricDelta'] svg{display:none !important;}"
    )
    if is_mobile:
        # #1: styling for the per-row mobile cards (_render_df_mobile_cards)
        _cbg = "#1f2937" if is_dark else "#ffffff"
        _cbd = "rgba(255,255,255,.09)" if is_dark else "#e6e9f0"
        _ctt = "#f1f5f9" if is_dark else "#0f172a"
        _crb = "rgba(255,255,255,.07)" if is_dark else "#f1f5f9"
        _pos = "#4ade80" if is_dark else "#16a34a"
        _neg = "#f87171" if is_dark else "#dc2626"
        _crit_css += (
            ".pp-mcards{display:flex;flex-direction:column;gap:10px;margin:6px 0;}"
            ".pp-mcard{border:1px solid " + _cbd + ";border-radius:14px;padding:11px 14px;background:" + _cbg + ";}"
            ".pp-mcard-t{font-weight:800;font-size:1.02rem;margin-bottom:6px;color:" + _ctt + ";unicode-bidi:plaintext;}"
            ".pp-mrow{display:flex;justify-content:space-between;gap:10px;padding:4px 0;font-size:.9rem;border-top:1px solid " + _crb + ";}"
            ".pp-mrow:first-of-type{border-top:none;}.pp-mrow .k{opacity:.72;}"
            ".pp-mrow .v{font-weight:700;unicode-bidi:plaintext;text-align:start;}"
            ".pp-mrow .v.pos{color:" + _pos + ";}.pp-mrow .v.neg{color:" + _neg + ";}"
        )
    _crit_css += "</style>"
    st.markdown(_crit_css, unsafe_allow_html=True)

    st.markdown(
        f"""<div class='app-header-wrap'>
  <div class='app-logo-row'>
    <div class='app-logo-text'>
      <h1 class='app-main-title'>Portfolio OS</h1>
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
            tr("Data", "נתונים") + ("  ✓" if _data_btn_primary else ""),
            icon=":material/database:",
            width="stretch",
            type=("primary" if _data_btn_primary else "secondary"),
            help=tr("Open the Data / Data Quality page.",
                    "פתיחת דף הנתונים (בקרת איכות נתונים)."),
            key="sidebar_data_nav_btn",
        ):
            st.session_state["active_page_id"] = "quality"
            st.rerun()

    connection_state_box = None

    # ── Mobile sidebar nav: AI Agent button right under Data (Settings is LAST, below) ──
    if is_mobile:
        _chat_active = (active_page_id == "chat")
        if st.sidebar.button(
            tr("AGENT AI", "סוכן AI") + ("  ✓" if _chat_active else ""),
            icon=":material/smart_toy:",
            width="stretch",
            type="secondary",
            help=tr("Chat with your AI portfolio agent.", "שיחה עם סוכן ה-AI של התיק."),
            key="sidebar_chat_nav_btn",
        ):
            st.session_state["active_page_id"] = "chat"
            st.rerun()

    # ════════ ⚙ Settings — ONE tabbed panel at the BOTTOM of the sidebar ════════
    # Tabs: ☁ Sync · 🔗 Connection · 🔒 Lock · ⚙ General. The Connection tab assigns the
    # web_app_url/api_token/... locals the main content reads below; widgets inside an
    # expander/tab still run every rerun (collapsed or not), so the locals are always set.
    _dirty_badge = count_dirty_local_trades()
    _settings_label = tr("⚙ Settings", "⚙ הגדרות") + (f"   ·   ☁ {_dirty_badge}" if _dirty_badge > 0 else "")
    # Safe defaults so the connection locals exist no matter what:
    web_app_url = settings.get("web_app_url", DEFAULT_WEB_APP_URL)
    api_token = settings.get("api_token", "")
    spreadsheet_ref = settings.get("spreadsheet_ref", "")
    worksheet_name = settings.get("worksheet_name", DEFAULT_WORKSHEET_NAME)
    service_account_file = settings.get("service_account_file", str(DEFAULT_SERVICE_ACCOUNT_FILE))
    followed_symbols_text = settings.get("followed_symbols", "")
    with st.sidebar.expander(_settings_label, expanded=(_dirty_badge > 0)):
        _tab_sync, _tab_conn, _tab_lock, _tab_gen = st.tabs([
            tr("☁ Sync", "☁ סנכרון"),
            tr("🔗 Connect", "🔗 חיבור"),
            tr("🔒 Lock", "🔒 נעילה"),
            tr("⚙ General", "⚙ כללי"),
        ])

        # ── ☁ Sync ──
        with _tab_sync:
            _sync_web_url = _clean(settings.get("web_app_url", DEFAULT_WEB_APP_URL) or DEFAULT_WEB_APP_URL)
            _sync_token = _clean(settings.get("api_token", ""))
            _sync_sheet_ref = _clean(settings.get("spreadsheet_ref", ""))
            _sync_ws = _clean(settings.get("worksheet_name", DEFAULT_WORKSHEET_NAME)) or DEFAULT_WORKSHEET_NAME
            _sync_sa = _clean(settings.get("service_account_file", str(DEFAULT_SERVICE_ACCOUNT_FILE)))
            _lm = get_local_portfolio_meta()
            _last_synced = _clean(str(_lm.get("_last_synced", "")))[:16]
            st.caption(tr(
                f"Last synced: **{_last_synced or 'never'}** · pending: **{_dirty_badge}**",
                f"סונכרן: **{_last_synced or 'אף פעם'}** · ממתינים: **{_dirty_badge}**",
            ))
            _has_conn = bool(_sync_web_url) or bool(_sync_sheet_ref)
            if not _has_conn:
                st.info(tr("Set a Web App URL / Spreadsheet ID in 🔗 Connect first.",
                           "הגדר Web App URL / Spreadsheet ID בלשונית 🔗 חיבור."))
            _force_pull = st.checkbox(
                tr("Force pull (replace local with Google)", "שליפה כפויה (החלף מקומי בנתוני גוגל)"),
                value=False, key="chk_force_pull", disabled=not _has_conn,
                help=tr("Discards unsynced local edits and mirrors exactly what Google has.",
                        "מוחק עריכות מקומיות שלא סונכרנו ומשקף בדיוק את מה שבגוגל."))
            if st.button(tr("☁ Sync now", "☁ סנכרן עכשיו"), width="stretch",
                         type="primary", disabled=not _has_conn, key="btn_pull_google"):
                with st.spinner(tr("Syncing…", "מסנכרן…")):
                    _pull_ok, _pull_msg, _pull_n = sync_portfolio_from_google(
                        _sync_web_url, _sync_token, _sync_sheet_ref, _sync_ws, _sync_sa,
                        tr=tr, force_full_replace=_force_pull,
                    )
                load_google_snapshot_data.clear()
                load_google_snapshot_data_via_gspread.clear()
                if _pull_ok:
                    _dep_errors: List[str] = []
                    if is_apps_script_web_app_url(_sync_web_url) and bool(_sync_token):
                        for _dep_mode in ["live", "demo"]:
                            _dep_ok, _dep_rows, _dep_err = load_manual_deposits_remote(_sync_web_url, _sync_token, _dep_mode)
                            if _dep_ok:
                                if _dep_rows:  # empty cloud → keep local (no silent wipe)
                                    _dep_store = load_manual_deposits_store()
                                    _dep_store[_dep_mode] = _dep_rows
                                    save_manual_deposits_store(_dep_store)
                                    st.session_state.pop(f"manual_deposits_loaded_{_dep_mode}", None)
                            else:
                                _dep_errors.append(f"{_dep_mode}: {_dep_err}")
                    _dep_note = f"  \n⚠ {'; '.join(_dep_errors)}" if _dep_errors else ""
                    st.success(f"✅ {_pull_msg}{_dep_note}")
                    st.rerun()
                else:
                    st.error(f"❌ {_pull_msg}")

        # ── 🔗 Connection (assigns the locals the main content reads) ──
        with _tab_conn:
            web_app_url = st.text_input("Apps Script Web App URL", value=settings.get("web_app_url", DEFAULT_WEB_APP_URL))
            api_token = st.text_input("API Token", value=settings.get("api_token", ""), type="password")
            spreadsheet_ref = st.text_input(
                tr("Spreadsheet URL or ID", "Spreadsheet URL / ID"),
                value=settings.get("spreadsheet_ref", ""),
                help=tr("Full Google Sheets URL or just its ID.", "URL מלא של Google Sheets או ה-ID בלבד."))
            worksheet_name = st.text_input(tr("Worksheet name", "שם גיליון"), value=settings.get("worksheet_name", DEFAULT_WORKSHEET_NAME))
            service_account_file = st.text_input(
                tr("Service Account JSON path", "נתיב Service Account JSON"),
                value=settings.get("service_account_file", str(DEFAULT_SERVICE_ACCOUNT_FILE)))
            followed_symbols_text = settings.get("followed_symbols", "")
            _cc1, _cc2 = st.columns(2)
            with _cc1:
                if st.button(tr("💾 Save", "💾 שמור"), width="stretch", key="_conn_save_btn"):
                    _ok = save_local_settings(web_app_url, api_token, spreadsheet_ref, worksheet_name,
                                              service_account_file, language, theme_mode, demo_mode, followed_symbols_text)
                    (st.success(tr("Saved", "נשמר")) if _ok else st.error(tr("Save failed", "השמירה נכשלה")))
            with _cc2:
                if st.button(tr("↻ Refresh", "↻ רענן"), width="stretch", key="_conn_refresh_btn"):
                    load_google_snapshot_data.clear()
                    load_google_snapshot_data_via_gspread.clear()
                    st.rerun()
            if st.checkbox(tr("Show cloud-secrets block (permanent connection)", "הצג בלוק secrets (חיבור קבוע בענן)"), key="_show_secrets"):
                st.caption(tr("Paste into the app dashboard → Settings → Secrets, once.",
                              "הדבק בלוח הבקרה ← Settings ← Secrets, פעם אחת."))
                st.code(f'web_app_url = "{web_app_url}"\napi_token = "{api_token}"\n'
                        f'spreadsheet_ref = "{spreadsheet_ref}"\nworksheet_name = "{worksheet_name}"\n', language="toml")
            connection_state_box = st.empty()

        # ── 🔒 Lock (passcode + fingerprint) ──
        with _tab_lock:
            _lk_now = _lock_load()
            _has_pin = bool(_lk_now.get("pin_hash"))
            _has_bio = bool(_lk_now.get("bio_enabled"))
            if _has_pin or _has_bio:
                st.caption("🟢 " + tr("Lock is ON", "הנעילה פעילה") + (" · 👆" if _has_bio else "") + (" · #" if _has_pin else ""))
                if st.button(tr("Remove lock", "הסר נעילה"), key="_lock_remove_btn", use_container_width=True):
                    _lock_save({})
                    st.session_state["_app_unlocked"] = True
                    st.rerun()
                st.divider()
            _newpin = st.text_input(tr("Set / change passcode (4+ chars)", "קבע / שנה קוד (4+ תווים)"),
                                    type="password", key="_lock_newpin")
            if st.button(tr("Save passcode", "שמור קוד"), key="_lock_savepin_btn", use_container_width=True):
                if len(str(_newpin)) >= 4:
                    _salt = _secrets.token_hex(8)
                    _lk_now["pin_hash"] = _lock_hash(_newpin, _salt)
                    _lk_now["salt"] = _salt
                    _lock_save(_lk_now)
                    st.session_state["_app_unlocked"] = True
                    st.success(tr("Passcode set — required next time you open the app.",
                                  "הקוד נשמר — יידרש בפעם הבאה שתפתח את האפליקציה."))
                else:
                    st.error(tr("At least 4 characters", "לפחות 4 תווים"))
            st.divider()
            st.caption("👆 " + tr("Fingerprint (phone) — experimental", "טביעת אצבע (טלפון) — ניסיוני"))
            if _has_bio:
                if st.button(tr("Disable fingerprint", "בטל טביעת אצבע"), key="_bio_disable_btn", use_container_width=True):
                    _lk_now["bio_enabled"] = False
                    _lock_save(_lk_now)
                    st.rerun()
                st.caption(tr("Register your fingerprint on this device:", "רשום את טביעת האצבע במכשיר הזה:"))
                components.html(_bio_register_html(tr("Register fingerprint", "רשום טביעת אצבע")), height=84)
            else:
                if st.button(tr("Enable fingerprint", "הפעל טביעת אצבע"), key="_bio_enable_btn", use_container_width=True):
                    if not _lk_now.get("pin_hash"):
                        st.warning(tr("Set a passcode above first — it's your fallback if the fingerprint is ever lost.",
                                      "קבע קודם קוד גישה למעלה — הוא הגיבוי אם טביעת האצבע תאבד."))
                    else:
                        _lk_now["bio_enabled"] = True
                        _lock_save(_lk_now)
                        st.session_state["_app_unlocked"] = True
                        st.rerun()

        # ── ⚙ General ──
        with _tab_gen:
            st.checkbox(
                tr("🔒 Lock charts (smooth scroll)", "🔒 נעילת תרשימים (גלילה חלקה)"),
                value=bool(st.session_state.get("chart_lock_persist", _is_mobile_client())),
                key="chart_lock_persist",
            )
            st.checkbox(
                tr("Demo view", "מצב הדגמה"),
                value=bool(st.session_state.get("demo_mode_persist", False)),
                key="demo_mode_persist",
            )
            st.divider()
            st.caption(tr(f"build {APP_BUILD} · st{st.__version__}", f"גרסה {APP_BUILD} · st{st.__version__}"))

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
        # Validate Apps Script URL format ONLY if a URL was actually provided
        if web_url_clean and not is_apps_script_web_app_url(web_url_clean):
            if is_google_sheet_url(web_url_clean):
                st.error(tr("Google Sheets URL was entered instead of Apps Script Web App URL", "הוזן קישור של Google Sheets במקום קישור Web App של Apps Script"))
                st.info(tr("Use a URL that starts with https://script.google.com/macros/s/ and ends with /exec", "צריך להדביק קישור שמתחיל ב-https://script.google.com/macros/s/ ומסתיים ב-/exec"))
            else:
                st.error(tr("Invalid Web App URL", "קישור Web App לא תקין"))
                st.info("https://script.google.com/macros/s/.../exec")
            st.stop()

        # Phone/cloud: hydrate the server-side local store from the BROWSER cache,
        # so last data shows instantly and survives a slow/failed sheet read. The
        # background refresh (auto-pull inside load_snapshot_data) then updates it.
        try:
            if load_local_portfolio()[0].empty:
                _cached_df = _read_cached_snapshot_ls()
                if _cached_df is not None and not _cached_df.empty:
                    save_local_portfolio(_normalize_snapshot_df(_cached_df), preserve_dirty=False)
        except Exception:
            pass

        # LOCAL-FIRST: attempt to load (local store is tried first inside load_snapshot_data)
        try:
            df, source_mode = load_snapshot_data(web_url_clean, api_token, spreadsheet_ref, worksheet_name, service_account_file)
        except Exception as exc:
            exc_text = str(exc)
            if _is_timeout_error(exc):
                st.error(tr("Google data read failed due to timeout.", "קריאת נתונים מגוגל נכשלה בגלל Timeout."))
                st.info(tr(
                    "Apps Script timed out. The app will auto-use local backup data when available; otherwise retry in a minute.",
                    "Apps Script חרג מזמן התגובה. האפליקציה תעבור אוטומטית לגיבוי מקומי אם קיים; אחרת נסה שוב בעוד דקה.",
                ))
                st.caption(exc_text)
            else:
                st.error(f"{tr('Data read failed', 'קריאת נתונים נכשלה')}: {exc_text}")
            st.stop()

        # If completely empty with no Google connection configured, show first-run wizard
        if source_mode == "empty":
            _current_page_for_empty = st.session_state.get("active_page_id", "dashboard")
            if _current_page_for_empty != "manage":
                st.info(tr(
                    "📭 No portfolio data found. Go to **Trade Management** in the sidebar to add your first trade "
                    "(no Google Sheets needed).",
                    "📭 לא נמצאו נתוני תיק. עבור לדף **ניהול עסקאות** בסרגל הצד כדי להוסיף את העסקה הראשונה "
                    "(ללא צורך בגוגל שיט).",
                ))
                st.stop()

    loading_placeholder.empty()

    # Keep the BROWSER snapshot cache fresh (real data only) so the phone can
    # paint instantly / offline on the next load. Never block the data path.
    try:
        if source_mode not in ("demo", "empty") and df is not None and not df.empty:
            _save_cached_snapshot_ls(df)
    except Exception:
        pass

    web_url_clean = _clean(settings.get("web_app_url", DEFAULT_WEB_APP_URL) if demo_mode else web_app_url)

    # ── Store file mtime in session_state so the cross-device watcher can detect changes ──
    if not demo_mode:
        st.session_state["_portfolio_file_mtime"] = get_portfolio_file_mtime()

    if connection_state_box is not None:
        _dirty_count = count_dirty_local_trades() if not demo_mode else 0
        _local_meta = get_local_portfolio_meta() if not demo_mode else {}
        _last_sync = _clean(str(_local_meta.get("_last_synced", "")))
        if source_mode == "local_store":
            _sync_label = (
                f"☁️ {_dirty_count} " + tr("unsynced change(s)", "שינוי/ים לא מסונכרנ/ים") + " · "
                if _dirty_count > 0 else ""
            )
            _ts_label = (tr("Last sync:", "סנכרון אחרון:") + " " + _last_sync[:16]) if _last_sync else tr("Never synced with Google", "לא סונכרן עם גוגל")
            connection_state_box.info(f"💾 {tr('Local store active', 'מאגר מקומי פעיל')} · {_sync_label}{_ts_label}")
        elif source_mode == "gspread":
            connection_state_box.warning(tr("gspread read mode active (write actions disabled).", "מצב קריאה דרך gspread פעיל (ללא Web App פעיל, פעולות עריכה/מחיקה מושבתות)."))
        elif source_mode == "local_cache":
            connection_state_box.warning(tr("Local backup cache is active (latest remote read was unavailable).", "גיבוי מקומי פעיל (הקריאה האחרונה מהשרת לא היתה זמינה)."))
        elif source_mode == "verified_fallback":
            connection_state_box.warning(tr("Verified fallback data is active (read-only emergency mode).", "נתוני גיבוי מאומתים פעילים (מצב חירום לקריאה בלבד)."))
        elif source_mode == "demo":
            connection_state_box.info(tr("Demo mode active - sample data only.", "מצב הדגמה פעיל - נתוני דוגמה בלבד."))
        else:
            connection_state_box.success(tr("Imported from Google Sheets (saved locally).", "נייבא מגוגל שיט (נשמר מקומית)."))

    if df.empty:
        _current_page_for_df = st.session_state.get("active_page_id", "dashboard")
        if _current_page_for_df != "manage":
            st.warning(tr("No portfolio rows found.", "לא נמצאו עסקאות בתיק."))
            st.stop()

    # ── Cross-device change watcher ──────────────────────────────────────
    # Checks portfolio_data.json mtime every 5 s.  When another session
    # (different browser tab / device on the same server) modifies the file,
    # this triggers a full-app rerun so the current session sees the update.
    if not demo_mode and hasattr(st, "fragment"):
        @st.fragment(run_every="5s")
        def _portfolio_sync_watcher() -> None:
            stored_mtime = float(st.session_state.get("_portfolio_file_mtime", 0.0))
            current_mtime = get_portfolio_file_mtime()
            if current_mtime > stored_mtime + 0.5:
                # File was changed externally — reload the full page
                st.rerun()
        _portfolio_sync_watcher()

    # ── INSTANT PAINT ───────────────────────────────────────────────────
    # Every page now renders immediately from the STORED values already in df
    # (synced from the sheet, refreshed in the background every few minutes).
    # The old code did a SYNCHRONOUS yfinance fetch_prices() here — wrapped in
    # a "🔄 Fetching live prices…" spinner — on EVERY page load/switch, which
    # blocked the paint on every cache miss. It's removed: live values are kept
    # current by the dashboard's KPI fragment (run_every≈20s, fetch_live_prices)
    # and by the background sheet sync, neither of which blocks rendering.
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
            f"{int(trades['Status'].map(_clean).str.lower().isin(CLOSED_STATUS_VALUES).sum()):,} {tr('closed', 'סגורות')}"
        )
        st.markdown(f"<div class='dashboard-stats-line'>{stats_text}</div>", unsafe_allow_html=True)

        fx = _safe_quote("USDILS=X")
        if fx <= 0:
            fx = _usd_ils_rate()
        # DEMO must stay deterministic: enrich_open_trades_with_prices() fetches LIVE
        # yfinance prices and overrode the curated 80k-scaled demo values, so the pie /
        # top-holding / bar / treemap reflected TODAY's market instead of the showcase.
        # In demo, enrich with EMPTY live prices → adds the structural columns but keeps
        # the stored Current_Value_ILS (override only happens where a live price > 0).
        if is_demo:
            dashboard_df = _enrich_compute(open_trades.copy(), {}, fx)
        else:
            dashboard_df = enrich_open_trades_with_prices(open_trades)
        dashboard_df["Cost_Origin_With_Fee"] = dashboard_df["Cost_Origin"] + dashboard_df["Commission"]
        dashboard_df["Cost_ILS_With_Fee"] = dashboard_df["Cost_ILS"].map(_num)
        dashboard_df["Value_Origin_Est"] = _value_origin_series(dashboard_df, fx)
        # Detect mixed-currency tickers (e.g. BTC bought on both Bit2C/ILS and Excellence/USD)
        _dash_ticker_pure = (
            dashboard_df.groupby("Ticker")["Origin_Currency"]
            .apply(lambda s: len(set(s.str.upper().dropna().tolist())) <= 1)
            .to_dict()
        )

        summary = dashboard_df.groupby("Ticker", as_index=False).agg(
            Current_Price=("מחיר שוק", "max"),
            Open_Qty=("Quantity", "sum"),
            Cost_ILS=("Cost_ILS", "sum"),
            Cost_ILS_With_Fee=("Cost_ILS_With_Fee", "sum"),  # incl. commission (denom)
            Value_ILS=("Current_Value_ILS", "sum"),
            Cost_Origin=("Cost_Origin", "sum"),      # plain cost, no commission
            Cost_Origin_With_Fee=("Cost_Origin_With_Fee", "sum"),  # incl. commission (denom)
            Value_Origin=("Value_Origin_Est", "sum"),
        )
        summary["Net_PnL_ILS"] = summary["Value_ILS"] - summary["Cost_ILS_With_Fee"]
        summary["Yield_ILS"] = np.where(summary["Cost_ILS_With_Fee"] > 0, summary["Net_PnL_ILS"] / summary["Cost_ILS_With_Fee"], 0.0)
        # Detect ILS-only tickers (origin currency == ILS for every trade of that ticker)
        _dash_ticker_ils = (
            dashboard_df.groupby("Ticker")["Origin_Currency"]
            .apply(lambda s: all((_normalize_currency_code(c) or "ILS") in {"ILS", ""} for c in s.dropna().tolist()))
            .to_dict()
        )
        # For ILS tickers: Yield_Origin == Yield_ILS (same currency — Cost_Origin excl. fees ≠ Cost_ILS incl. fees = phantom discrepancy)
        # For USD pure tickers: use origin-currency computation (legit different)
        # For mixed-currency tickers: fall back to Yield_ILS
        _dash_origin_raw = np.where(
            summary["Cost_Origin_With_Fee"] > 0,
            (summary["Value_Origin"] - summary["Cost_Origin_With_Fee"]) / summary["Cost_Origin_With_Fee"],
            0.0,
        )
        summary["Yield_Origin"] = np.where(
            summary["Ticker"].map(lambda t: _dash_ticker_ils.get(t, False)),
            summary["Yield_ILS"],   # ILS: identical currency, force equal
            np.where(
                summary["Ticker"].map(lambda t: _dash_ticker_pure.get(t, True)),
                _dash_origin_raw,   # USD pure: meaningful difference
                summary["Yield_ILS"],  # Mixed: fall back
            ),
        )

        # Net P/L by asset — use dashboard_df (live-enriched) not raw open_trades.
        # Net P/L nets out commission too (Cost_ILS_With_Fee), matching the summary
        # Net_PnL_ILS so both P/L surfaces agree. (audit #10)
        pnl_source = dashboard_df.copy() if not dashboard_df.empty else pd.DataFrame(columns=["Ticker", "Cost_ILS", "Cost_ILS_With_Fee", "Current_Value_ILS"])
        for col in ["Ticker", "Cost_ILS", "Cost_ILS_With_Fee", "Current_Value_ILS"]:
            if col not in pnl_source.columns:
                pnl_source[col] = 0.0 if col != "Ticker" else ""
        pnl_source["Ticker"] = pnl_source["Ticker"].map(_clean)
        pnl_source = pnl_source[pnl_source["Ticker"] != ""]
        pnl_source["Cost_ILS"] = pnl_source["Cost_ILS"].map(_num)
        pnl_source["Cost_ILS_With_Fee"] = pnl_source["Cost_ILS_With_Fee"].map(_num)
        pnl_source["Current_Value_ILS"] = pnl_source["Current_Value_ILS"].map(_num)
        pnl_by_asset = pnl_source.groupby("Ticker", as_index=False).agg(
            Cost_ILS=("Cost_ILS_With_Fee", "sum"),
            Current_Value_ILS=("Current_Value_ILS", "sum"),
        ) if not pnl_source.empty else pd.DataFrame(columns=["Ticker", "Cost_ILS", "Current_Value_ILS"])
        if not pnl_by_asset.empty:
            pnl_by_asset["Net_PnL_ILS"] = pnl_by_asset["Current_Value_ILS"] - pnl_by_asset["Cost_ILS"]

        # ── KPI metrics — live fragment updates every 20s ─────────────────
        _kpi_slot = st.container()
        with _kpi_slot:
            @st.fragment(run_every="20s")
            def _kpi_live_fragment() -> None:
                _tv = total_value
                _tp = total_profit
                _tc_val = total_cost
                _live_sum = summary.copy() if not summary.empty else summary
                if not is_demo and not open_trades.empty:
                    try:
                        _tks = tuple(sorted(
                            t for t in open_trades["Ticker"].dropna().unique() if _clean(t)
                        ))
                        _lpx = fetch_live_prices(_tks)
                        _ufx = _usd_ils_rate()
                        if _lpx:
                            _enriched_kpi = open_trades.copy()
                            _enriched_kpi["מחיר שוק"] = _enriched_kpi["Ticker"].map(_lpx).fillna(0.0)
                            _qty_k = _enriched_kpi["Quantity"].map(_num)
                            # Use yfinance quote currency — NOT Origin_Currency — for FX multiplier.
                            # BTC on Bit2C has Origin_Currency=ILS but yfinance returns USD price.
                            _price_cur_k = _enriched_kpi["Ticker"].map(
                                lambda t: _yf_price_currency(_clean(t).upper())
                            )
                            _fxm_k = _price_cur_k.map(lambda c: _ils_per_native_unit(c, _ufx))
                            _vk = _qty_k * _enriched_kpi["מחיר שוק"] * _fxm_k
                            _has_k = (_enriched_kpi["מחיר שוק"] > 0) & (_fxm_k > 0)
                            _enriched_kpi.loc[_has_k, "Current_Value_ILS"] = _vk[_has_k]
                            _tv = float(_enriched_kpi["Current_Value_ILS"].map(_num).sum())
                            if "Cost_ILS" in _enriched_kpi.columns:
                                _tc_val = float(_enriched_kpi["Cost_ILS"].map(_num).sum())
                            _tp = _tv - _tc_val
                            # Rebuild summary for top-holding display
                            _enriched_kpi["Cost_Origin_With_Fee"] = _enriched_kpi["Cost_Origin"] + _enriched_kpi["Commission"]
                            _enriched_kpi["Value_Origin_Est"] = _value_origin_series(_enriched_kpi, _ufx)
                            _live_sum = _enriched_kpi.groupby("Ticker", as_index=False).agg(
                                Value_ILS=("Current_Value_ILS", "sum"),
                            )
                    except Exception:
                        pass

                # #6/#7 — currency stated ONCE, in the LABEL (where the ₪ glyph
                # renders reliably; the metric-value font lacks it and showed a
                # tofu box). Values are pure numbers, forced LTR via CSS so the
                # minus stays on the left in RTL ("-81,369", not "81,369-").
                _tv_txt = f"{_tv:,.0f}"
                _tp_txt = f"{_tp:,.0f}"
                _tr_val = (_tp / _tc_val) if _tc_val > 0 else 0.0
                _tr_txt = f"{_tr_val:.2%}"
                _top_val_txt, _top_delta_txt = "--", tr("No open assets", "אין נכסים פתוחים")
                if not _live_sum.empty and "Value_ILS" in _live_sum.columns:
                    _tot_s = float(_live_sum["Value_ILS"].sum())
                    if _tot_s > 0:
                        _top_row = _live_sum.loc[_live_sum["Value_ILS"].idxmax()]
                        _top_val_txt = f"{float(_top_row['Value_ILS']) / _tot_s:.1%}"
                        _top_delta_txt = f"{_clean(_top_row.get('Ticker', ''))} | ₪{float(_top_row['Value_ILS']):,.0f}"

                if is_mobile:
                    kpi_r1 = st.columns(2)
                    with kpi_r1[0].container(key="kpi_total"):
                        st.metric(tr("Total Value (₪)", "שווי כולל (₪)"), _tv_txt)
                    with kpi_r1[1].container(key="kpi_pnl"):
                        st.metric(tr("Open P&L (₪)", "רווח/הפסד פתוח (₪)"), _tp_txt)
                    kpi_r2 = st.columns(2)
                    with kpi_r2[0].container(key="kpi_ret"):
                        st.metric(tr("Return", "תשואה כוללת"), _tr_txt)
                    with kpi_r2[1].container(key="kpi_closed"):
                        st.metric(
                            tr("Closed Positions", "פוזיציות סגורות"),
                            str(len(closed_trades)),
                            f"{len(open_trades)} {tr('open', 'פתוחות')}",
                            delta_color="off",  # #14 neutral count, not a gain → no green ↑
                        )
                else:
                    kpi_cols = st.columns(4)
                    with kpi_cols[0].container(key="kpi_total"):
                        st.metric(tr("Total Value (₪)", "שווי כולל (₪)"), _tv_txt)
                    with kpi_cols[1].container(key="kpi_pnl"):
                        st.metric(tr("Open P&L (₪)", "רווח/הפסד פתוח (₪)"), _tp_txt)
                    with kpi_cols[2].container(key="kpi_ret"):
                        st.metric(tr("Total Return", "תשואה כוללת"), _tr_txt)
                    with kpi_cols[3].container(key="kpi_closed"):
                        st.metric(
                            tr("Closed Positions", "פוזיציות סגורות"),
                            str(len(closed_trades)),
                            f"{len(open_trades)} {tr('open', 'פתוחות')}",
                            delta_color="off",  # #14 neutral count, not a gain → no green ↑
                        )
                style_metric_cards(border_left_color="#4f46e5", border_radius_px=12, box_shadow=True)
                # #4 finance convention: P&L / Return value reads RED when negative,
                # GREEN when positive (pre-attentive — not via reading the minus sign).
                # #7 contrast: darker red (#b91c1c ≈ 6:1 on white) for negatives.
                _pnl_c = "#b91c1c" if _tp < 0 else "#15803d"
                _ret_c = "#b91c1c" if _tr_val < 0 else "#15803d"
                # Visual hierarchy: the TOTAL VALUE is the headline number, so its card
                # gets a premium indigo-gradient fill with white numerics — instantly
                # readable as "this is THE figure", with the P&L/Return/Positions cards
                # reading as supporting context. P&L + Return additionally carry a
                # semantic top-accent bar (red when losing, green when gaining) so the
                # state is pre-attentive — grasped before the digits are even read.
                _total_grad = "linear-gradient(135deg,#4338ca 0%,#6366f1 52%,#818cf8 100%)"
                st.markdown(
                    "<style>"
                    ".st-key-kpi_total [data-testid='stMetric']{background:" + _total_grad + " !important;"
                    "border:none !important;box-shadow:0 20px 44px -20px rgba(79,70,229,.62) !important;}"
                    ".st-key-kpi_total [data-testid='stMetric']::before{height:0 !important;}"
                    ".st-key-kpi_total [data-testid='stMetricValue'],.st-key-kpi_total [data-testid='stMetricValue'] *{"
                    "color:#ffffff !important;text-shadow:0 1px 10px rgba(0,0,0,.20) !important;}"
                    ".st-key-kpi_total [data-testid='stMetricLabel'],.st-key-kpi_total [data-testid='stMetricLabel'] *{"
                    "color:rgba(255,255,255,.94) !important;}"
                    ".st-key-kpi_pnl [data-testid='stMetric']::before{background:" + _pnl_c + " !important;opacity:1 !important;}"
                    ".st-key-kpi_ret [data-testid='stMetric']::before{background:" + _ret_c + " !important;opacity:1 !important;}"
                    ".st-key-kpi_pnl [data-testid='stMetricValue'],.st-key-kpi_pnl [data-testid='stMetricValue'] *{color:" + _pnl_c + " !important;}"
                    ".st-key-kpi_ret [data-testid='stMetricValue'],.st-key-kpi_ret [data-testid='stMetricValue'] *{color:" + _ret_c + " !important;}"
                    "</style>",
                    unsafe_allow_html=True,
                )

            _kpi_live_fragment()

        class_mix = pd.DataFrame(columns=["Asset_Class", "Current_Value_ILS", "Assets"])
        if not open_trades.empty and {"Ticker", "Type", "Current_Value_ILS"}.issubset(open_trades.columns):
            class_work = open_trades[["Ticker", "Type", "Current_Value_ILS"]].copy()
            class_work["Ticker"] = class_work["Ticker"].map(lambda v: _clean(v).upper())
            class_work["Type"] = class_work["Type"].map(_clean)

            def _class_bucket(row: pd.Series) -> str:
                # Single source of truth shared with the crypto-share KPI and mirrored
                # to engine.js assetClass() (broad KNOWN_COINS/KNOWN_ETFS + -USD heuristic)
                # so blank-Type / English-CRYPTO / new-coin rows bucket identically across
                # the Streamlit and native apps. (audit sync-defs assetClass drift)
                cls = _asset_class_label(row.get("Ticker", ""), row.get("Type", ""))
                if cls == "crypto":
                    return tr("Crypto", "קריפטו")
                if cls == "etf":
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
                tr("Transactions", "תנועות") if is_mobile else tr("Transactions & Cash Flow", "תנועות ועסקאות"),
            ]
        )

        # ── Slot containers inside each tab guarantee the required visual order ──
        with tab_overview_real:
            _ov_body_slot = st.container()         # Overview: mirrored pie+bar, then Build-Up
        with tab_allocation_real:
            _alloc_charts_slot = st.container()    # Pie + Bar + Treemap
            _alloc_classes_slot = st.container()   # Class-allocation
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
                # Plotly pies cannot represent negative magnitudes — a holding whose ILS
                # value went negative would be silently dropped/distorted while still
                # counted in the KPI total. Filter to positive slices and note any
                # exclusions so the pie reconciles with the totals. (audit bug-st)
                _pie_src = summary[summary["Value_ILS"].map(_num) > 0] if "Value_ILS" in summary.columns else summary
                _pie_excluded = len(summary) - len(_pie_src)
                fig_pie = px.pie(
                    _pie_src,
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
                st.plotly_chart(_apply_plotly_theme(fig_pie, is_dark, is_mobile), theme="streamlit", width="stretch")
                if _pie_excluded > 0:
                    st.caption(tr(f"{_pie_excluded} holding(s) with non-positive value excluded from the pie.",
                                  f"{_pie_excluded} אחזקות עם שווי לא חיובי הוחרגו מהתרשים."))
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
                    labels={"Net_PnL_ILS": tr("Net P/L (ILS)", "רווח/הפסד (₪)"), "Ticker": tr("Ticker", "טיקר"), "_color": ""},
                )
                fig_bar.update_layout(
                    showlegend=False,
                    yaxis_tickformat=",.0f",
                    hovermode="x unified",
                    xaxis_title=tr("Ticker", "טיקר"),
                )
                fig_bar.update_traces(
                    hovertemplate="<b>%{x}</b><br>P/L: ₪%{y:,.0f}<extra></extra>"
                )
                st.plotly_chart(_apply_plotly_theme(fig_bar, is_dark, is_mobile, is_bar=True), theme="streamlit", width="stretch")

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
                    st.plotly_chart(_apply_plotly_theme(fig_tree, is_dark, is_mobile), theme="streamlit", width="stretch")
                except Exception:
                    pass

            def _build_summary_for_exposure(local_open_trades: pd.DataFrame) -> pd.DataFrame:
                if local_open_trades.empty:
                    return pd.DataFrame(columns=["Ticker", "Current_Price", "Open_Qty", "Cost_ILS", "Value_ILS", "Net_PnL_ILS", "Yield_Origin", "Yield_ILS"])
                fx_local = _safe_quote("USDILS=X")
                if fx_local <= 0:
                    fx_local = _usd_ils_rate()
                local_df = enrich_open_trades_with_prices(local_open_trades.copy())
                local_df["Cost_Origin_With_Fee"] = local_df["Cost_Origin"] + local_df["Commission"]
                local_df["Cost_ILS_With_Fee"] = local_df["Cost_ILS"].map(_num)
                local_df["Value_Origin_Est"] = _value_origin_series(local_df, fx_local)
                # Detect tickers with mixed Origin_Currency (e.g. BTC bought on both
                # Bit2C/ILS and Excellence/USD). Summing ILS costs with USD costs is
                # arithmetically undefined — fall back to Yield_ILS for those tickers.
                _ticker_pure = (
                    local_df.groupby("Ticker")["Origin_Currency"]
                    .apply(lambda s: len(set(s.str.upper().dropna().tolist())) <= 1)
                    .to_dict()
                )
                out = local_df.groupby("Ticker", as_index=False).agg(
                    Current_Price=("מחיר שוק", "max"),
                    Open_Qty=("Quantity", "sum"),
                    Cost_ILS=("Cost_ILS", "sum"),
                    Cost_ILS_With_Fee=("Cost_ILS_With_Fee", "sum"),
                    Value_ILS=("Current_Value_ILS", "sum"),
                    Cost_Origin=("Cost_Origin", "sum"),
                    Cost_Origin_With_Fee=("Cost_Origin_With_Fee", "sum"),
                    Value_Origin=("Value_Origin_Est", "sum"),
                )
                out["Net_PnL_ILS"] = out["Value_ILS"] - out["Cost_ILS_With_Fee"]
                out["Yield_ILS"] = np.where(out["Cost_ILS_With_Fee"] > 0, out["Net_PnL_ILS"] / out["Cost_ILS_With_Fee"], 0.0)
                # Yield_Origin: use origin-currency computation only for pure-single-currency
                # tickers. For mixed-currency tickers (BTC/ETH with ILS+USD trades) use
                # Yield_ILS (ILS-denominated) which is always unambiguous.
                # Commission is in the cost-basis denominator to match GAS. (audit #10)
                _origin_raw = np.where(
                    out["Cost_Origin_With_Fee"] > 0,
                    (out["Value_Origin"] - out["Cost_Origin_With_Fee"]) / out["Cost_Origin_With_Fee"],
                    0.0,
                )
                _is_pure = out["Ticker"].map(lambda t: _ticker_pure.get(t, True))
                out["Yield_Origin"] = np.where(_is_pure, _origin_raw, out["Yield_ILS"])
                return out

            def render_exposure_section(summary_df: pd.DataFrame, widget_prefix: str = "overview", include_watchlist: bool = True) -> None:

                st.markdown(f"#### {tr('Exposure Table', 'טבלת חשיפה')}")
                # Return shown in the ORIGIN currency (per-ticker), not ILS (user pref).
                exposure_cols = ["Ticker", "Current_Price", "Open_Qty", "Cost_ILS", "Value_ILS", "Net_PnL_ILS", "Yield_Origin"]
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
                    def _fmt_qty(v):
                        # Sensible quantity precision: whole share counts show no
                        # decimals; fractional (crypto) shows up to 6 decimals with
                        # trailing zeros trimmed — instead of always 8 decimals
                        # (which padded "23.00000000" and bloated the column width).
                        try:
                            f = float(v)
                        except Exception:
                            return ""
                        if f == 0:
                            return "0"
                        if f == int(f) or abs(f) >= 100:
                            return f"{f:,.0f}"
                        return f"{f:,.6f}".rstrip("0").rstrip(".")
                    # On mobile the full 8-column table overflows the viewport and the
                    # last column ("שווי כולל") gets silently clipped. Show only the
                    # essential columns there so everything fits and nothing is cut.
                    if is_mobile:
                        _keep = [localize_column_name(x, language)
                                 for x in ["Ticker", "Open_Qty", "Value_ILS", "Net_PnL_ILS", "Yield_Origin"]]
                        _keep = [c for c in _keep if c in exposure_view.columns]
                        if _keep:
                            exposure_view = exposure_view[_keep]
                    _fmt_map = {
                        current_price_col: "{:,.4f}",
                        localize_column_name("Open_Qty", language): _fmt_qty,
                        localize_column_name("Cost_ILS", language): "{:,.0f}",
                        localize_column_name("Value_ILS", language): "{:,.0f}",
                        pnl_col: "{:,.0f}",
                        yield_origin_col: "{:.2%}",
                        yield_ils_col: "{:.2%}",
                    }
                    _fmt_map = {k: v for k, v in _fmt_map.items() if k in exposure_view.columns}
                    try:
                        exposure_styled = exposure_view.style.format(_fmt_map, na_rep="")
                        _color_cols = [c for c in [pnl_col, yield_origin_col, yield_ils_col] if c in exposure_view.columns]
                        exposure_styled = _apply_signed_color(exposure_styled, _color_cols)
                        # Use st.dataframe on BOTH desktop and mobile. It renders the Styler
                        # natively via the Arrow grid (a real component, not sanitized HTML),
                        # so it ALWAYS shows. The old mobile path (st.table → markdown-HTML)
                        # left only the "טבלת חשיפה" title with no rows.
                        st.dataframe(exposure_styled, width="stretch", hide_index=True)
                    except Exception:
                        # Last-ditch: plain frame so the table can never silently vanish.
                        st.dataframe(exposure_view, width="stretch", hide_index=True)

                if include_watchlist:
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
                        # Inline category TABS (no floating popover): the watchlist sits in
                        # normal document flow, so the chart that opens below it can NEVER be
                        # covered — the popover used to overlay the chart. Organised by category;
                        # 4-col chip grid on desktop, 2-col on mobile. Click a chip -> live chart.
                        st.markdown("##### 📈 " + watchlist_label)
                        st.caption(tr("Pick an instrument to open its live chart below.",
                                      "בחר נכס לפתיחת גרף חי למטה."))
                        _wl_cats = [
                            ("crypto", "🪙 " + tr("Crypto", "קריפטו")),
                            ("stocks", "📈 " + tr("Stocks", "מניות")),
                            ("macro", "🌐 " + tr("Macro", "מאקרו")),
                        ]
                        _wl_tabs = st.tabs([lbl for _, lbl in _wl_cats])
                        _wl_ncol = 2 if is_mobile else 4
                        for _ti, (catkey, _lbl) in enumerate(_wl_cats):
                            with _wl_tabs[_ti]:
                                part = watch_df[watch_df["Category"] == category_labels[catkey]]
                                if part.empty:
                                    st.caption("—")
                                    continue
                                _wrows = list(part.itertuples(index=False))
                                for _base in range(0, len(_wrows), _wl_ncol):
                                    _chunk = _wrows[_base:_base + _wl_ncol]
                                    _cols = st.columns(_wl_ncol)
                                    for _ci, row in enumerate(_chunk):
                                        with _cols[_ci]:
                                            if st.button(row.Ticker, help=row.Title, width="stretch",
                                                         key=f"{widget_prefix}_watch_{catkey}_{_base + _ci}_{row.Symbol}"):
                                                st.session_state["tv_chart_ticker"] = _clean(row.Symbol).upper()
                                                st.session_state["tv_chart_open"] = True
                                                st.session_state[f"{widget_prefix}_chart_scroll_pending"] = True
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
                            width="stretch",
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

        # ── Exposure table with live-price fragment ──────────────────────────
        # Fragment MUST be anchored inside the target container (_alloc_exposure_slot)
        # to satisfy Streamlit's "no widgets outside fragment boundary" rule.
        if live_updates and hasattr(st, "fragment") and page != page_manage:
            with _alloc_exposure_slot:
                @st.fragment(run_every="20s")
                def _exposure_fragment() -> None:
                    live_summary = summary
                    if not is_demo and not open_trades.empty:
                        try:
                            # Use intraday 1-min prices (TTL=25s) for near-real-time display
                            _live_tickers = tuple(sorted(
                                t for t in open_trades["Ticker"].dropna().unique() if _clean(t)
                            ))
                            _live_px = fetch_live_prices(_live_tickers)
                            if _live_px:
                                _fx = _usd_ils_rate()
                                _enriched = open_trades.copy()
                                _enriched["מחיר שוק"] = _enriched["Ticker"].map(_live_px).fillna(0.0)
                                _qty = _enriched["Quantity"].map(_num)
                                # Use yfinance quote currency — NOT Origin_Currency — for FX multiplier.
                                # BTC on Bit2C has Origin_Currency=ILS but yfinance returns USD price.
                                _price_cur = _enriched["Ticker"].map(
                                    lambda t: _yf_price_currency(_clean(t).upper())
                                )
                                _fxm = _price_cur.map(lambda c: _ils_per_native_unit(c, _fx))
                                _live_val = _qty * _enriched["מחיר שוק"] * _fxm
                                _has = (_enriched["מחיר שוק"] > 0) & (_fxm > 0)
                                _enriched.loc[_has, "Current_Value_ILS"] = _live_val[_has]
                                # Build summary directly from enriched data (skip re-enrichment)
                                _enriched["Cost_Origin_With_Fee"] = _enriched["Cost_Origin"] + _enriched["Commission"]
                                _enriched["Cost_ILS_With_Fee"] = _enriched["Cost_ILS"].map(_num)
                                _enriched["Value_Origin_Est"] = _value_origin_series(_enriched, _fx)
                                live_summary = _enriched.groupby("Ticker", as_index=False).agg(
                                    Current_Price=("מחיר שוק", "max"),
                                    Open_Qty=("Quantity", "sum"),
                                    Cost_ILS=("Cost_ILS", "sum"),
                                    Cost_ILS_With_Fee=("Cost_ILS_With_Fee", "sum"),
                                    Value_ILS=("Current_Value_ILS", "sum"),
                                    Cost_Origin=("Cost_Origin", "sum"),
                                    Cost_Origin_With_Fee=("Cost_Origin_With_Fee", "sum"),
                                    Value_Origin=("Value_Origin_Est", "sum"),
                                )
                                live_summary["Net_PnL_ILS"] = live_summary["Value_ILS"] - live_summary["Cost_ILS_With_Fee"]
                                live_summary["Yield_ILS"] = np.where(
                                    live_summary["Cost_ILS_With_Fee"] > 0,
                                    live_summary["Net_PnL_ILS"] / live_summary["Cost_ILS_With_Fee"],
                                    0.0,
                                )
                                # Mixed-currency tickers (e.g. BTC with ILS+USD trades):
                                # summing ILS and USD costs is invalid → fall back to Yield_ILS.
                                _e_pure = (
                                    _enriched.groupby("Ticker")["Origin_Currency"]
                                    .apply(lambda s: len(set(s.str.upper().dropna().tolist())) <= 1)
                                    .to_dict()
                                )
                                _e_ils = (
                                    _enriched.groupby("Ticker")["Origin_Currency"]
                                    .apply(lambda s: all((_normalize_currency_code(c) or "ILS") in {"ILS", ""} for c in s.dropna().tolist()))
                                    .to_dict()
                                )
                                _e_origin_raw = np.where(
                                    live_summary["Cost_Origin_With_Fee"] > 0,
                                    (live_summary["Value_Origin"] - live_summary["Cost_Origin_With_Fee"]) / live_summary["Cost_Origin_With_Fee"],
                                    0.0,
                                )
                                live_summary["Yield_Origin"] = np.where(
                                    live_summary["Ticker"].map(lambda t: _e_ils.get(t, False)),
                                    live_summary["Yield_ILS"],   # ILS: force equal (same currency)
                                    np.where(
                                        live_summary["Ticker"].map(lambda t: _e_pure.get(t, True)),
                                        _e_origin_raw,   # USD pure: meaningful
                                        live_summary["Yield_ILS"],  # Mixed: fall back
                                    ),
                                )
                        except Exception:
                            pass
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
                        width="stretch",
                        theme="streamlit",
                        key="ov_mirror_pie",
                    )
                with ov_col_b:
                    st.plotly_chart(
                        _apply_plotly_theme(fig_bar, is_dark, is_mobile, is_bar=True),
                        width="stretch",
                        theme="streamlit",
                        key="ov_mirror_bar",
                    )

            # Portfolio Build-Up — directly BELOW the "רווח/הפסד לפי נכס" bar, same (Overview) tab.
            # Wrapped so a chart error can NEVER abort the rest of this block (e.g. the
            # exposure table below) — that previously made the exposure vanish on mobile.
            _build_up_slot = st.container()
            if can_show_build_up:
                try:
                    with _build_up_slot:
                        st.markdown(f"#### {tr('Portfolio Build-Up', 'התפתחות בניית התיק')}")
                        perf_track = dashboard_df.groupby("Purchase_Date", as_index=False)[["Cost_ILS", "Current_Value_ILS"]].sum().sort_values("Purchase_Date")
                        perf_track["Cum_Cost_ILS"] = perf_track["Cost_ILS"].cumsum()
                        perf_track["Cum_Value_ILS"] = perf_track["Current_Value_ILS"].cumsum()
                        _x = perf_track["Purchase_Date"]
                        _cost = perf_track["Cum_Cost_ILS"]
                        _value = perf_track["Cum_Value_ILS"]
                        _final_val = float(_value.iloc[-1]) if len(_value) else 0.0
                        _final_cost = float(_cost.iloc[-1]) if len(_cost) else 0.0
                        _ret = (_final_val / _final_cost - 1.0) if _final_cost else 0.0
                        _up = _final_val >= _final_cost
                        # Whole chart is tinted green/red by the overall result for an at-a-glance read.
                        _accent = "#22c55e" if _up else "#ef4444"
                        _line_col = ("#34d399" if is_dark else "#10b981") if _up else ("#f87171" if is_dark else "#ef4444")
                        _fill = "rgba(16,185,129,0.16)" if _up else "rgba(239,68,68,0.14)"
                        fig_track = go.Figure()
                        # Invested-capital baseline (muted dotted spline).
                        fig_track.add_trace(go.Scatter(
                            x=_x, y=_cost, mode="lines", name=tr("Cumulative Cost", "עלות מצטברת"),
                            line=dict(color="#94a3b8", width=1.6, dash="dot", shape="spline", smoothing=0.7),
                            hovertemplate=tr("Cost", "עלות") + ": ₪%{y:,.0f}<extra></extra>",
                        ))
                        # Portfolio value with a soft gain/loss band filled down to the cost line.
                        fig_track.add_trace(go.Scatter(
                            x=_x, y=_value, mode="lines", name=tr("Cumulative Value", "שווי מצטבר"),
                            line=dict(color=_line_col, width=3.4, shape="spline", smoothing=0.7),
                            fill="tonexty", fillcolor=_fill,
                            hovertemplate=tr("Value", "שווי") + ": ₪%{y:,.0f}<extra></extra>",
                        ))
                        # Emphasised final point + value/return badge.
                        if len(_x):
                            fig_track.add_trace(go.Scatter(
                                x=[_x.iloc[-1]], y=[_final_val], mode="markers",
                                marker=dict(size=12, color=_accent, line=dict(color="#ffffff", width=2)),
                                showlegend=False, hoverinfo="skip",
                            ))
                            fig_track.add_annotation(
                                x=_x.iloc[-1], y=_final_val,
                                text=f"<b>₪{_final_val:,.0f}</b>  ({_ret * 100:+.1f}%)",
                                showarrow=False, yshift=22, xshift=-6,
                                font=dict(color=_accent, size=13),
                                bgcolor="rgba(15,23,42,0.65)" if is_dark else "rgba(255,255,255,0.75)",
                                bordercolor=_accent, borderwidth=1, borderpad=4,
                            )
                        fig_track.update_layout(
                            template=template,
                            xaxis_title=None,
                            yaxis_title=tr("Value (ILS)", "שווי (₪)"),
                            yaxis_tickformat="~s",
                            hovermode="x unified",
                            xaxis=dict(showgrid=False, showspikes=True, spikethickness=1,
                                       spikedash="dot", spikecolor="rgba(148,163,184,0.5)", spikemode="across"),
                            yaxis=dict(gridcolor="rgba(148,163,184,0.14)", zeroline=False),
                        )
                        # Apply the shared theme, THEN force a visible legend ABOVE the plot.
                        _fig_track = _apply_plotly_theme(fig_track, is_dark, is_mobile)
                        _fig_track.update_layout(
                            showlegend=True,
                            legend=dict(orientation="h", yanchor="bottom", y=1.0, xanchor="right", x=1,
                                        font=dict(color="#e2e8f0" if is_dark else "#334155", size=12),
                                        bgcolor="rgba(0,0,0,0)"),
                            margin=dict(t=64, l=8, r=8, b=8),
                            # Taller, more prominent on desktop; keep compact on mobile.
                            height=320 if is_mobile else 460,
                        )
                        st.plotly_chart(_fig_track, theme=None, width="stretch")
                except Exception:
                    pass

            # Exposure table mirrored into Overview tab — live fragment updates every 20s
            _ov_exposure_slot = st.container()
            with _ov_exposure_slot:
                @st.fragment(run_every="20s")
                def _ov_exposure_fragment() -> None:
                    _ov_summary = summary
                    if not is_demo and not open_trades.empty:
                        try:
                            _tks_ov = tuple(sorted(
                                t for t in open_trades["Ticker"].dropna().unique() if _clean(t)
                            ))
                            _lpx_ov = fetch_live_prices(_tks_ov)
                            if _lpx_ov:
                                _fx_ov = _usd_ils_rate()
                                _ov_e = open_trades.copy()
                                _ov_e["מחיר שוק"] = _ov_e["Ticker"].map(_lpx_ov).fillna(0.0)
                                _qty_ov = _ov_e["Quantity"].map(_num)
                                # Use yfinance quote currency — NOT Origin_Currency — for FX multiplier.
                                # BTC on Bit2C has Origin_Currency=ILS but yfinance returns USD price.
                                _price_cur_ov = _ov_e["Ticker"].map(
                                    lambda t: _yf_price_currency(_clean(t).upper())
                                )
                                _fxm_ov = _price_cur_ov.map(lambda c: _ils_per_native_unit(c, _fx_ov))
                                _vov = _qty_ov * _ov_e["מחיר שוק"] * _fxm_ov
                                _hov = (_ov_e["מחיר שוק"] > 0) & (_fxm_ov > 0)
                                _ov_e.loc[_hov, "Current_Value_ILS"] = _vov[_hov]
                                _ov_e["Cost_Origin_With_Fee"] = _ov_e["Cost_Origin"] + _ov_e["Commission"]
                                _ov_e["Cost_ILS_With_Fee"] = _ov_e["Cost_ILS"].map(_num)
                                _ov_e["Value_Origin_Est"] = _value_origin_series(_ov_e, _fx_ov)
                                _ov_summary = _ov_e.groupby("Ticker", as_index=False).agg(
                                    Current_Price=("מחיר שוק", "max"),
                                    Open_Qty=("Quantity", "sum"),
                                    Cost_ILS=("Cost_ILS", "sum"),
                                    Cost_ILS_With_Fee=("Cost_ILS_With_Fee", "sum"),
                                    Value_ILS=("Current_Value_ILS", "sum"),
                                    Cost_Origin=("Cost_Origin", "sum"),
                                    Cost_Origin_With_Fee=("Cost_Origin_With_Fee", "sum"),
                                    Value_Origin=("Value_Origin_Est", "sum"),
                                )
                                _ov_summary["Net_PnL_ILS"] = _ov_summary["Value_ILS"] - _ov_summary["Cost_ILS_With_Fee"]
                                _ov_summary["Yield_ILS"] = np.where(
                                    _ov_summary["Cost_ILS_With_Fee"] > 0,
                                    _ov_summary["Net_PnL_ILS"] / _ov_summary["Cost_ILS_With_Fee"],
                                    0.0,
                                )
                                # Mixed-currency tickers (BTC/ETH with ILS+USD trades):
                                # costs in different currencies can't be summed → use Yield_ILS.
                                _ov_pure = (
                                    _ov_e.groupby("Ticker")["Origin_Currency"]
                                    .apply(lambda s: len(set(s.str.upper().dropna().tolist())) <= 1)
                                    .to_dict()
                                )
                                _ov_ils = (
                                    _ov_e.groupby("Ticker")["Origin_Currency"]
                                    .apply(lambda s: all((_normalize_currency_code(c) or "ILS") in {"ILS", ""} for c in s.dropna().tolist()))
                                    .to_dict()
                                )
                                _ov_origin_raw = np.where(
                                    _ov_summary["Cost_Origin_With_Fee"] > 0,
                                    (_ov_summary["Value_Origin"] - _ov_summary["Cost_Origin_With_Fee"]) / _ov_summary["Cost_Origin_With_Fee"],
                                    0.0,
                                )
                                _ov_summary["Yield_Origin"] = np.where(
                                    _ov_summary["Ticker"].map(lambda t: _ov_ils.get(t, False)),
                                    _ov_summary["Yield_ILS"],   # ILS: force equal (same currency)
                                    np.where(
                                        _ov_summary["Ticker"].map(lambda t: _ov_pure.get(t, True)),
                                        _ov_origin_raw,   # USD pure: meaningful
                                        _ov_summary["Yield_ILS"],  # Mixed: fall back
                                    ),
                                )
                        except Exception:
                            pass
                    # Mirror: render only the exposure TABLE — the heavy 26-symbol
                    # watchlist + TradingView chart render once in the primary section
                    # so the dashboard never loads two iframes at once. (audit double-render)
                    render_exposure_section(_ov_summary, widget_prefix="overview_body", include_watchlist=False)

                _ov_exposure_fragment()

        # Compute once and share across both Allocation and Reports tabs.
        # Use dashboard_df (already live-enriched) so all charts reflect current prices.
        _shared_reports_payload = build_home_inspired_reports(dashboard_df)

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
            alloc_kpi_cols = st.columns(3) if not is_mobile else [st.container(), st.container(), st.container()]
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
                        width="stretch",
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
                        width="stretch",
                        theme="streamlit",
                        key="alloc_class_treemap",
                    )

                    st.markdown(f"**{tr('Included assets by class', 'נכסים כלולים לפי סוג')}**")
                    for row in class_mix.itertuples(index=False):
                        cls = _clean(getattr(row, "Asset_Class", ""))
                        assets = _clean(getattr(row, "Assets", "")) or "-"
                        color = class_color_map.get(cls, "#64748b")
                        # Sheet-sourced values are user-influenced; escape before HTML injection.
                        cls_safe = cls.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                        assets_safe = assets.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                        st.markdown(
                            f"<span style='color:{color};font-weight:700'>■</span> <strong>{cls_safe}</strong>: {assets_safe}",
                            unsafe_allow_html=True,
                        )
                else:
                    st.info(tr("No asset-class data", "אין נתוני סוגי נכסים"))

        with tab_reports:
            reports_payload = _shared_reports_payload
            try:
                # Use the LIVE-enriched frame (dashboard_df) so Smart Insights / Rebalancing
                # reflect current market prices, not the stale stored Current_Value_ILS.
                render_smart_features(dashboard_df if isinstance(dashboard_df, pd.DataFrame) and not dashboard_df.empty else open_trades, language)
                st.markdown("---")
            except Exception as _sf_exc:
                st.caption(f"Smart features unavailable ({str(_sf_exc)[:60]})")
            report_options = {
                tr("Crypto Concentration", "ריכוזיות קריפטו"): "concentration_table",
                tr("Winner / Loser", "המנצח / המפסיד"): "winner_loser_table",
                tr("Net Investment by Platform", "השקעה נטו לפי פלטפורמה"): "net_investment_table",
            }

            def _render_report_section(title: str, key: str) -> None:
                st.markdown(f"### {title}")
                report_df = reports_payload.get(key, pd.DataFrame()) if isinstance(reports_payload, dict) else pd.DataFrame()
                if not isinstance(report_df, pd.DataFrame) or report_df.empty:
                    st.info(tr("No data available for this report.", "אין נתונים זמינים לדוח זה."))
                else:
                    localized_df = localize_dataframe_columns(report_df, language)
                    # Translate cell values using VALUE_LABELS (e.g. "Winner"→"מנצח", "קריפטו"→"Crypto")
                    for raw_col, val_map in VALUE_LABELS.items():
                        # Find the localized column name for this raw column key
                        vis_col_en = COLUMN_LABELS.get(raw_col, {}).get(language, raw_col)
                        vis_col_he = COLUMN_LABELS.get(raw_col, {}).get(LANG_HE, raw_col)
                        for col_candidate in [raw_col, vis_col_en, vis_col_he]:
                            if col_candidate in localized_df.columns:
                                localized_df[col_candidate] = localized_df[col_candidate].map(
                                    lambda v, vm=val_map: vm.get(_clean(str(v)) if pd.notna(v) else "", {}).get(language, v) if pd.notna(v) else v
                                )
                                break
                    fmt_map: Dict[str, str] = {}
                    for col in localized_df.columns:
                        col_s = str(col)
                        if any(t in col_s for t in ["Yield", "Return", "תשואה"]):
                            fmt_map[col] = "{:.2%}"
                        elif "Qty" in col_s or "כמות" in col_s:
                            fmt_map[col] = _smart_qty
                        elif any(token in col_s for token in ["ILS", "Rate", "שער", "שווי", "עלות", "Investment", "PnL", "רווח"]):
                            fmt_map[col] = "{:,.0f}"
                        else:
                            # Any other numeric column (e.g. ETF units, estimated
                            # BTC/ETH/SOL qty in Crypto Concentration) → trim trailing
                            # zeros so "322.000000" shows as "322". Fixes #2 reaching
                            # the report tables that slipped the keyword match.
                            try:
                                if pd.api.types.is_numeric_dtype(localized_df[col]):
                                    fmt_map[col] = _smart_qty
                            except Exception:
                                pass
                    report_styled = localized_df.style
                    # na_rep so empty cells render as "—" instead of a literal "None"
                    # (e.g. the ETHA/BSOL columns in the Crypto-Concentration table).
                    report_styled = report_styled.format(fmt_map or None, na_rep="—")
                    signed_cols = [c for c in localized_df.columns if any(t in str(c).lower() for t in ["yield", "return", "pnl", "תשואה", "רווח"])]
                    if is_mobile:
                        # #1: render as cards so wide report tables never clip on a phone
                        _render_df_mobile_cards(localized_df, fmt_map, signed_cols)
                    else:
                        if signed_cols:
                            report_styled = _apply_signed_color(report_styled, signed_cols)
                        _render_dataframe_adaptive(report_styled, is_mobile, width="stretch", hide_index=True)
                st.divider()

            for _rep_title, _rep_key in report_options.items():
                _render_report_section(_rep_title, _rep_key)

            # ── Live Market Rates — live fragment (updates every 20s) ──────────
            st.markdown(f"### {tr('Exchange Rates', 'שערי מטבע')}")
            if live_updates and hasattr(st, "fragment"):
                @st.fragment(run_every="20s")
                def _reports_live_rates_fragment() -> None:
                    _lang_r = st.session_state.get("_tx_frag_lang", LANG_HE)
                    _mob_r = st.session_state.get("_tx_frag_mobile", False)
                    try:
                        _rates: Dict[str, float] = {}
                        # Only crypto/USD pairs — no stocks/ETFs
                        _crypto_tickers = ["BTC-USD", "ETH-USD", "SOL-USD"]
                        _lp = fetch_live_prices(tuple(_crypto_tickers))
                        _fx = _safe_quote("USDILS=X")
                        if _fx > 0:
                            _rates[("USD/ILS" if _lang_r == LANG_EN else "דולר/שקל")] = round(_fx, 4)
                        _eur_ils = _safe_quote("EURILS=X")
                        if _eur_ils > 0:
                            _rates[("EUR/ILS" if _lang_r == LANG_EN else "יורו/שקל")] = round(_eur_ils, 4)
                        for _tk in _crypto_tickers:
                            _label = _tk.replace("-USD", "") + "/USD"
                            _p = _lp.get(_tk, 0.0)
                            if _p and _p > 0:
                                _rates[_label] = round(_p, 2)
                        if _rates:
                            _rdf = pd.DataFrame([{tr("Symbol", "סימול"): k, tr("Rate", "שער"): v} for k, v in _rates.items()])
                            _rs = _rdf.style.format({tr("Rate", "שער"): "{:,.2f}"})
                            _render_dataframe_adaptive(_rs, _mob_r, width="stretch", hide_index=True)
                        else:
                            st.info(tr("No market rates available.", "אין שערי שוק זמינים כרגע."))
                    except Exception:
                        st.info(tr("No market rates available.", "אין שערי שוק זמינים כרגע."))
                _reports_live_rates_fragment()
            else:
                rates = reports_payload.get("live_rates", {}) if isinstance(reports_payload, dict) else {}
                if not rates:
                    st.info(tr("No market rates available.", "אין שערי שוק זמינים כרגע."))
                else:
                    rates_df = pd.DataFrame(
                        [{tr("Symbol", "סימול"): k, tr("Rate", "שער"): v} for k, v in rates.items()]
                    )
                    _render_dataframe_adaptive(rates_df.style.format({tr("Rate", "שער"): "{:,.4f}"}), is_mobile, width="stretch", hide_index=True)
            st.divider()

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
                width="stretch",
                hide_index=True,
                num_rows="dynamic",
                key=f"manual_deposits_editor_{deposit_mode}",
                column_config={
                    "Platform": st.column_config.TextColumn(tr("Platform", "פלטפורמה"), required=True),
                    "Manual_Deposit_ILS": st.column_config.NumberColumn(tr("Manual Deposit (ILS)", "הפקדה ידנית (₪)"), min_value=0.0, step=100.0, format="%,.2f"),
                },
            )

            edited_rows = edited_df.to_dict(orient="records") if isinstance(edited_df, pd.DataFrame) else []
            normalized_rows = _normalize_manual_deposit_rows(edited_rows, default_platforms=[])
            st.session_state[rows_state_key] = normalized_rows

            total_manual = float(sum(_num(r.get("Manual_Deposit_ILS", 0.0)) for r in normalized_rows))
            st.metric(tr("Total Manual Deposits", "סה\"כ הפקדות ידניות"), f"{total_manual:,.2f} ₪")

            action_cols = st.columns([1, 1, 3])
            if action_cols[0].button(tr("Save Deposits", "שמור הפקדות"), key=f"manual_deposits_save_{deposit_mode}"):
                if not can_sync_remote:
                    st.error(tr(
                        "Google Sheets connection required to save deposits. Configure a valid Web App URL.",
                        "נדרש חיבור לגוגל שיט כדי לשמור הפקדות. הגדר Web App URL תקין.",
                    ))
                else:
                    # GOOGLE-FIRST ATOMIC: push to Google first; save locally only on success.
                    remote_ok, remote_msg = save_manual_deposits_remote(web_url_for_sync, api_token, deposit_mode, normalized_rows)
                    if remote_ok:
                        store_payload = load_manual_deposits_store()
                        store_payload[deposit_mode] = normalized_rows
                        save_manual_deposits_store(store_payload)
                        st.session_state[source_state_key] = "cloud"
                        st.success(tr("Saved and synced to cloud", "נשמר וסונכרן לענן"))
                    else:
                        msg = remote_msg or tr("Unknown sync error", "שגיאת סנכרון לא ידועה")
                        st.error(f"{tr('Push to Google failed — no changes saved.', 'הדחיפה לגוגל נכשלה — לא נשמר שום שינוי.')}: {msg}")

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

                # Dedicated closed-trade sale-return columns — origin-rate and ₪-rate —
                # mirroring the native apps (web/js/app.js) + the sheet's "תשואה במכירה (₪)".
                # Computed only for closed rows (blank for open → auto-hidden). Feeds the
                # static render path; the live fragment recomputes the same two columns.
                if "Status" in tx_view.columns and not tx_view.empty:
                    _sale_clm = tx_view["Status"].map(_is_closed_status)
                    _sale_bp = tx_view["Origin_Buy_Price"].map(_num) if "Origin_Buy_Price" in tx_view.columns else pd.Series(0.0, index=tx_view.index)
                    _sale_sp = tx_view["Sell_Price_Origin"].map(_num) if "Sell_Price_Origin" in tx_view.columns else pd.Series(0.0, index=tx_view.index)
                    _sale_ci = tx_view["Cost_ILS"].map(_num) if "Cost_ILS" in tx_view.columns else pd.Series(0.0, index=tx_view.index)
                    _sale_vi = tx_view["Current_Value_ILS"].map(_num) if "Current_Value_ILS" in tx_view.columns else pd.Series(0.0, index=tx_view.index)
                    # Effective ILS cost basis: stored Cost_ILS (already commission-inclusive)
                    # when present, else the frozen basis (Cost_Origin+Commission at buy-date FX)
                    # so the realized-ILS column populates even for legacy app-created sells whose
                    # Cost_ILS is 0. (audit fin-streamlit #1/#9/#10)
                    _sale_ci_eff = tx_view.apply(
                        lambda r: float(_num(r.get("Cost_ILS", 0.0)))
                        if _num(r.get("Cost_ILS", 0.0)) != 0
                        else _frozen_cost_ils_for_sell(_num(r.get("Cost_Origin", 0.0)), _num(r.get("Commission", 0.0)),
                                                       r.get("Origin_Currency", ""), r.get("Purchase_Date", "")),
                        axis=1,
                    ) if not tx_view.empty else _sale_ci
                    # Origin-rate fallback requires a REAL sell price (sp>0); otherwise leave
                    # blank rather than emit a false -100% (e.g. CRK has no recorded sell price).
                    _sale_orig_fb = pd.Series(np.where((_sale_bp > 0) & (_sale_sp > 0), (_sale_sp - _sale_bp) / _sale_bp, np.nan), index=tx_view.index)
                    if "Yield_At_Sale" in tx_view.columns:
                        _sale_orig = tx_view["Yield_At_Sale"].map(_num_or_nan)
                        _sale_orig = _sale_orig.where(~_sale_orig.isna(), _sale_orig_fb)
                    else:
                        _sale_orig = _sale_orig_fb
                    tx_view = tx_view.drop(columns=[c for c in ["Yield_At_Sale", "תשואה במכירה"] if c in tx_view.columns], errors="ignore")
                    tx_view["תשואה במכירה (מקור)"] = np.where(_sale_clm, _sale_orig, np.nan)
                    tx_view["תשואה במכירה (₪)"] = np.where(_sale_clm & (_sale_ci_eff > 0), (_sale_vi - _sale_ci_eff) / _sale_ci_eff, np.nan)

                if is_open_only:
                    sale_related_cols = [
                        c for c in [
                            "Status", "Action", "Sell_Date", "Sell_Price_Origin", "Yield_At_Sale",
                            "סטטוס", "פעולה", "תאריך מכירה", "שער מכירה", "מחיר מכירה", "תשואה במכירה",
                            "תשואה במכירה (מקור)", "תשואה במכירה (₪)",
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
                        placeholder=tr("Choose tickers…", "בחר טיקרים…"),
                    )
                    if chosen_tickers:
                        selected_set = {t.upper() for t in chosen_tickers}
                        tx_view = tx_view[tx_view["Ticker"].map(lambda v: _clean(v).upper()).isin(selected_set)]
                if "Purchase_Date" in tx_view.columns:
                    # Default ordering: newest purchase date first.
                    tx_view["__sort_date__"] = _parse_dates_flexible(tx_view["Purchase_Date"])
                    tx_view = tx_view.sort_values("__sort_date__", ascending=False, na_position="last").drop(columns=["__sort_date__"])

                # Store tx_view snapshot for live-price fragment (auto-refreshes every 20s)
                st.session_state["_tx_frag_base"] = tx_view.copy()
                st.session_state["_tx_frag_is_open"] = is_open_only
            # ── Transactions table: live-price fragment refreshes every 20s ─────────
            # Mutable display state must live in session_state so the fragment
            # auto-reruns are self-contained (independent of the full-page run).
            st.session_state["_tx_frag_lang"] = language
            st.session_state["_tx_frag_mobile"] = is_mobile

            def _render_tx_table(
                _tx: pd.DataFrame,
                _lang: str,
                _mob: bool,
                _open: bool,
            ) -> None:
                """Shared rendering helper used by both the fragment and the static fallback."""
                def _nrm_hdr(col: str) -> str:
                    c = _clean(col)
                    return {"\u05e2\u05dc\u05d5\u05ea \u05e9\u05e7\u05dc\u05d9\u05ea": "\u05e2\u05dc\u05d5\u05ea ILS"}.get(c, c)

                def _is_hid(cn: object) -> bool:
                    low = re.sub(r"\s+", " ", re.sub(r"\s*\(\d+\)$", "", _clean(cn)).lower().replace("_", " ").replace("-", " ")).strip()
                    return low in {"origin buy price", "buy price", "\u05e9\u05e2\u05e8 \u05e7\u05e0\u05d9\u05d9\u05d4", "\u05e9\u05e2\u05e8 \u05e7\u05e0\u05d9\u05d4", "sell status", "sale status", "\u05e1\u05d8\u05d8\u05d5\u05e1 \u05de\u05db\u05d9\u05e8\u05d4"}

                def _oo_hid(cn: object) -> bool:
                    c = _clean(cn)
                    if c in {"Status", "Action", "Sell_Date", "Sell_Price_Origin", "Yield_At_Sale",
                             "\u05e1\u05d8\u05d8\u05d5\u05e1", "\u05e4\u05e2\u05d5\u05dc\u05d4", "\u05ea\u05d0\u05e8\u05d9\u05da \u05de\u05db\u05d9\u05e8\u05d4", "\u05e9\u05e2\u05e8 \u05de\u05db\u05d9\u05e8\u05d4", "\u05de\u05d7\u05d9\u05e8 \u05de\u05db\u05d9\u05e8\u05d4", "\u05ea\u05e9\u05d5\u05d0\u05d4 \u05d1\u05de\u05db\u05d9\u05e8\u05d4"}:
                        return True
                    return c.lower() in {"sell price", "sale price", "sell date", "return at sale"}

                def _is_blank(v: object) -> bool:
                    if pd.isna(v): return True
                    return _clean(v).lower() in {"", "nan", "nat", "none"}

                def _nck(cn: object) -> str:
                    return re.sub(r"\s+", " ", re.sub(r"\s*\(\d+\)$", "", _clean(cn)).lower().replace("_", " ").replace("-", " ")).strip()

                def _coalesce_tx(df: pd.DataFrame, aliases: List[str]) -> pd.DataFrame:
                    akeys = {_nck(a) for a in aliases}
                    cands = [c for c in df.columns if _nck(c) in akeys]
                    if len(cands) <= 1: return df
                    tgt = next((c for a in aliases for c in cands if _nck(c) == _nck(a)), cands[0])
                    merged = df[cands].copy()
                    for c in cands:
                        merged[c] = merged[c].map(lambda v: np.nan if _is_blank(v) else v)
                    df[tgt] = merged.bfill(axis=1).iloc[:, 0]
                    extras = [c for c in cands if c != tgt]
                    if extras: df = df.drop(columns=extras, errors="ignore")
                    return df

                _tx = localize_snapshot_view(_tx, _lang)
                _tx = _tx.rename(columns=lambda c: _nrm_hdr(str(c)))
                _tx = _tx.drop(columns=[c for c in _tx.columns if _is_hid(c)], errors="ignore")
                if _open and not _tx.empty:
                    _tx = _tx.drop(columns=[c for c in _tx.columns if _oo_hid(c)], errors="ignore")
                _tx, _ = _with_calendar_purchase_date(_tx, _lang)

                if _tx.empty:
                    st.info(tr("No transactions to display", "\u05d0\u05d9\u05df \u05e2\u05e1\u05e7\u05d0\u05d5\u05ea \u05dc\u05d4\u05e6\u05d2\u05d4"))
                    return

                _dv = _tx.copy().reset_index(drop=True)
                if not _dv.columns.is_unique:
                    _seen2: Dict[str, int] = {}; _uniq2: List[str] = []
                    for c in _dv.columns:
                        b = str(c); _seen2[b] = _seen2.get(b, 0) + 1
                        _uniq2.append(b if _seen2[b] == 1 else f"{b} ({_seen2[b]})")
                    _dv.columns = _uniq2

                for _ag in [
                    ["Sell_Price_Origin", "Sell Price", "Sale Price", "\u05de\u05d7\u05d9\u05e8 \u05de\u05db\u05d9\u05e8\u05d4", "\u05e9\u05e2\u05e8 \u05de\u05db\u05d9\u05e8\u05d4"],
                    ["Yield_ILS", "Return ILS", "\u05ea\u05e9\u05d5\u05d0\u05d4 ILS", "\u05ea\u05e9\u05d5\u05d0\u05d4 \u05e9\u05e7\u05dc\u05d9\u05ea", "\u05ea\u05e9\u05d5\u05d0\u05d4 \u05e0\u05d8\u05d5 (\u20aa)", "Net Return (ILS)"],
                    ["Yield_Origin", "Return (Origin)", "\u05ea\u05e9\u05d5\u05d0\u05d4 \u05d1\u05e9\u05e2\u05e8 \u05de\u05e7\u05d5\u05e8", "\u05ea\u05e9\u05d5\u05d0\u05d4 \u05de\u05e7\u05d5\u05e8", "\u05ea\u05e9\u05d5\u05d0\u05d4 \u05e0\u05d8\u05d5 (\u05de\u05e7\u05d5\u05e8)", "Net Return (Origin)"],
                    ["Current_Value_ILS", "Value ILS", "Current Value (ILS)", "\u05e9\u05d5\u05d5\u05d9 ILS", "\u05e9\u05d5\u05d5\u05d9 \u05e9\u05e7\u05dc\u05d9", "\u05e9\u05d5\u05d5\u05d9 \u05e2\u05d3\u05db\u05e0\u05d9 (\u20aa)"],
                    ["Value USD", "Current Value (USD)", "\u05e9\u05d5\u05d5\u05d9 USD", "\u05e9\u05d5\u05d5\u05d9 \u05d1\u05d3\u05d5\u05dc\u05e8", "\u05e9\u05d5\u05d5\u05d9 \u05e2\u05d3\u05db\u05e0\u05d9 (USD)"],
                ]:
                    _dv = _coalesce_tx(_dv, _ag)

                _drop_c2: List[str] = []
                for col in _dv.columns:
                    ct2 = _clean(col); cl2 = ct2.lower()
                    if cl2.startswith("unnamed:"): _drop_c2.append(col); continue
                    if ("usd" in cl2 and "ils" in cl2) or ("\u05e9\u05e2\u05e8" in ct2 and "\u05d3\u05d5\u05dc\u05e8" in ct2 and "\u05e9\u05e7\u05dc" in ct2): _drop_c2.append(col); continue
                    if _dv[col].map(_is_blank).all(): _drop_c2.append(col)
                if _drop_c2: _dv = _dv.drop(columns=_drop_c2, errors="ignore")

                def _to_ratio2(v: object) -> float:
                    s = _clean(v)
                    if not s: return np.nan
                    n = _num(v)
                    return n / 100.0 if "%" in s else n

                def _dfmt2(v: object) -> str:
                    if pd.isna(v): return ""
                    if hasattr(v, "strftime"):
                        try: return v.strftime("%d/%m/%Y")
                        except Exception: pass
                    s = _clean(v)
                    if not s: return ""
                    p = _parse_dates_flexible(pd.Series([s])).iloc[0]
                    return p.strftime("%d/%m/%Y") if pd.notna(p) else s

                _yc2 = [c for c in _dv.columns if "\u05ea\u05e9\u05d5\u05d0\u05d4" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower()]
                for c in _yc2:
                    _dv[c] = _dv[c].map(_to_ratio2)

                _sfmt2: Dict[str, object] = {}
                for dc in ["Purchase_Date", "\u05ea\u05d0\u05e8\u05d9\u05da \u05e8\u05db\u05d9\u05e9\u05d4", "Sell_Date", "\u05ea\u05d0\u05e8\u05d9\u05da \u05de\u05db\u05d9\u05e8\u05d4"]:
                    if dc in _dv.columns: _sfmt2[dc] = _dfmt2
                for yc in _yc2:
                    if yc in _dv.columns: _sfmt2[yc] = "{:.2%}"

                _styled2 = _dv.style
                if _sfmt2: _styled2 = _styled2.format(_sfmt2, na_rep="")
                if _yc2: _styled2 = _apply_signed_color(_styled2, _yc2)
                _render_dataframe_adaptive(_styled2, _mob, force_same_render_path=True, width="stretch", hide_index=True)

                # ── Export bar — below the table ───────────────────────────
                _is_he_tx = _lang.startswith("ע")
                _export_prefix = "trades_open" if _open else "trades_all"
                _ts_tx = datetime.now().strftime("%Y%m%d_%H%M")
                _ec1, _ec2, _ec3 = st.columns(3)
                with _ec1:
                    st.download_button(
                        ("⬇️ CSV" if _is_he_tx else "⬇️ CSV"),
                        data=_dv.to_csv(index=False).encode("utf-8-sig"),
                        file_name=f"{_export_prefix}_{_ts_tx}.csv",
                        mime="text/csv",
                        width="stretch",
                        key=f"dl_csv_{_export_prefix}_{_ts_tx}",
                    )
                with _ec2:
                    st.download_button(
                        ("⬇️ JSON" if _is_he_tx else "⬇️ JSON"),
                        data=_dv.to_json(orient="records", force_ascii=False, indent=2).encode("utf-8"),
                        file_name=f"{_export_prefix}_{_ts_tx}.json",
                        mime="application/json",
                        width="stretch",
                        key=f"dl_json_{_export_prefix}_{_ts_tx}",
                    )
                with _ec3:
                    _xls_tx = pp_dataframe_to_excel_bytes(_dv, sheet_name=_export_prefix)
                    if _xls_tx:
                        st.download_button(
                            ("⬇️ Excel" if _is_he_tx else "⬇️ Excel"),
                            data=_xls_tx,
                            file_name=f"{_export_prefix}_{_ts_tx}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            width="stretch",
                            key=f"dl_xlsx_{_export_prefix}_{_ts_tx}",
                        )

            if live_updates and hasattr(st, "fragment"):
                @st.fragment(run_every="20s")
                def _tx_live_fragment() -> None:
                    _base = st.session_state.get("_tx_frag_base", pd.DataFrame())
                    _lang_f = st.session_state.get("_tx_frag_lang", LANG_HE)
                    _mob_f = st.session_state.get("_tx_frag_mobile", False)
                    _open_f = st.session_state.get("_tx_frag_is_open", False)
                    if _base is None or (isinstance(_base, pd.DataFrame) and _base.empty):
                        st.info(tr("No transactions to display", "\u05d0\u05d9\u05df \u05e2\u05e1\u05e7\u05d0\u05d5\u05ea \u05dc\u05d4\u05e6\u05d2\u05d4"))
                        return
                    _tx = _base.copy()
                    # Re-fetch live prices (TTL=25 s — yfinance 1-min intraday data)
                    if "Ticker" in _tx.columns:
                        _fx = _usd_ils_rate()
                        _tks = tuple(sorted({_clean(v).upper() for v in _tx["Ticker"].tolist() if _clean(v)}))
                        _lpm = fetch_live_prices(_tks) if _tks else {}

                        def _mpo(row: pd.Series) -> float:
                            t = _clean(row.get("Ticker", "")).upper()
                            if not t: return np.nan
                            p = float(_num(_lpm.get(t, 0.0)))
                            if p <= 0: return np.nan
                            cur = _normalize_currency_code(row.get("Origin_Currency", ""))
                            # FX decision must use the currency YAHOO quotes in, not Origin_Currency:
                            # a .TA (TASE) stock is ILS-quoted AND ILS-origin → must NOT be ×FX (was 3.6× too high). (audit)
                            yf_cur = _yf_price_currency(t)
                            if yf_cur == "USD" and cur == "ILS": return p * _fx   # USD quote on an ILS asset (e.g. Bit2C crypto)
                            return p   # same currency (incl. .TA ILS stocks) → no conversion

                        def _spo(row: pd.Series) -> float:
                            if not _is_closed_status(row.get("Status", "")): return np.nan
                            qty = float(_num(row.get("Quantity", 0.0)))
                            val = float(_num(row.get("Current_Value_ILS", 0.0)))
                            if qty > 1e-9 and val > 0:
                                cur = _normalize_currency_code(row.get("Origin_Currency", ""))
                                unit = val / qty
                                # Back-derive the origin sell price from ILS proceeds at the
                                # SELL-date FX (proceeds were frozen at sale time), NOT today's
                                # spot — otherwise the derived price drifts with the live rate. (audit #14)
                                if cur == "USD":
                                    _sfx = _usdils_on(_clean(row.get("Sell_Date", "")))
                                    if not (_sfx and _sfx > 0):
                                        _sfx = _fx
                                    return unit / _sfx
                                return unit
                            return np.nan

                        _tx["Market_Price_Origin"] = _tx.apply(_mpo, axis=1)
                        _sfm = _tx.apply(_spo, axis=1)
                        if "Sell_Price_Origin" in _tx.columns:
                            _es = _tx["Sell_Price_Origin"].map(_num)
                            _tx["Sell_Price_Origin"] = np.where(_es > 0, _es, _sfm)
                        else:
                            _tx["Sell_Price_Origin"] = _sfm

                        if "Origin_Buy_Price" in _tx.columns:
                            _bp = _tx["Origin_Buy_Price"].map(_num)
                            _tx["Yield_Current"] = np.where(_bp > 0, (_tx["Market_Price_Origin"] - _bp) / _bp, np.nan)
                            # App-created sells (blank Yield_At_Sale) use a COMMISSION-INCLUSIVE
                            # origin basis (Cost_Origin+Commission), matching GAS yieldAtSale
                            # (F*U-(H+J))/(H+J) and the ILS sale-return — NOT the fee-excluding
                            # (sp-bp)/bp which over-states the return. (audit sync-defs C)
                            _co_sc = _tx["Cost_Origin"].map(_num) if "Cost_Origin" in _tx.columns else pd.Series(0.0, index=_tx.index)
                            _comm_sc = _tx["Commission"].map(_num) if "Commission" in _tx.columns else pd.Series(0.0, index=_tx.index)
                            _qty_sc = _tx["Quantity"].map(_num) if "Quantity" in _tx.columns else pd.Series(0.0, index=_tx.index)
                            _basis_sc = _co_sc + _comm_sc
                            _proceeds_sc = _qty_sc * _tx["Sell_Price_Origin"].map(_num)
                            _syc = np.where(
                                _basis_sc > 0,
                                (_proceeds_sc - _basis_sc) / _basis_sc,
                                np.where(_bp > 0, (_tx["Sell_Price_Origin"] - _bp) / _bp, np.nan),
                            )
                            if "Yield_At_Sale" in _tx.columns:
                                _hys = ~_tx["Yield_At_Sale"].map(lambda v: pd.isna(v) or _clean(v) == "")
                                _tx["Yield_At_Sale"] = np.where(_hys, _tx["Yield_At_Sale"], _syc)
                            else:
                                _tx["Yield_At_Sale"] = _syc

                        _ci = _tx["Cost_ILS"].map(_num) if "Cost_ILS" in _tx.columns else pd.Series(0.0, index=_tx.index)
                        _vi = _tx["Current_Value_ILS"].map(_num) if "Current_Value_ILS" in _tx.columns else pd.Series(0.0, index=_tx.index)
                        # Effective ILS cost basis: stored Cost_ILS (already commission-inclusive)
                        # when present, else the frozen basis (Cost_Origin+Commission at the
                        # buy-date FX) — so the realized-ILS column and Yield_ILS both populate
                        # even for legacy app-created sells whose Cost_ILS is 0. (audit fin-streamlit #1/#9/#10)
                        _ci_eff = _tx.apply(
                            lambda r: float(_num(r.get("Cost_ILS", 0.0)))
                            if _num(r.get("Cost_ILS", 0.0)) != 0
                            else _frozen_cost_ils_for_sell(_num(r.get("Cost_Origin", 0.0)), _num(r.get("Commission", 0.0)),
                                                           r.get("Origin_Currency", ""), r.get("Purchase_Date", "")),
                            axis=1,
                        ) if not _tx.empty else _ci
                        # Dedicated closed-trade sale returns: origin-rate (from Yield_At_Sale)
                        # and ₪-rate (STORED sale proceeds vs cost — NOT live prices; the sale
                        # already happened). Blank for open rows. Mirrors the static path above.
                        _clm_f = _tx["Status"].map(_is_closed_status) if "Status" in _tx.columns else pd.Series(False, index=_tx.index)
                        if "Yield_At_Sale" in _tx.columns:
                            _tx["תשואה במכירה (מקור)"] = np.where(_clm_f, _tx["Yield_At_Sale"].map(_num_or_nan), np.nan)
                            _tx = _tx.drop(columns=["Yield_At_Sale"], errors="ignore")
                        _tx["תשואה במכירה (₪)"] = np.where(_clm_f & (_ci_eff > 0), (_vi - _ci_eff) / _ci_eff, np.nan)
                        # ── Update Current_Value_ILS from live prices BEFORE computing yields ──
                        # Without this, _vi is the stale value from portfolio_data.json and
                        # the per-trade yield won't reflect real-time prices.
                        if _lpm:
                            _qty_tx = _tx["Quantity"].map(_num) if "Quantity" in _tx.columns else pd.Series(0.0, index=_tx.index)
                            _cur_tx = _tx["Origin_Currency"].map(lambda c: _normalize_currency_code(c) or "ILS") if "Origin_Currency" in _tx.columns else pd.Series("ILS", index=_tx.index)
                            _px_tx = _tx["Ticker"].map(lambda t: float(_num(_lpm.get(_clean(t).upper(), 0.0)))) if "Ticker" in _tx.columns else pd.Series(0.0, index=_tx.index)
                            # FX multiplier must be based on yfinance quote currency, NOT Origin_Currency.
                            # BTC on Bit2C: Origin_Currency=ILS but price from yfinance is USD → must × FX.
                            _fxm_tx = _tx["Ticker"].map(
                                lambda t: _ils_per_native_unit(_yf_price_currency(_clean(t).upper()), _fx)
                            ) if "Ticker" in _tx.columns else pd.Series(_fx, index=_tx.index)
                            _live_vi_tx = _qty_tx * _px_tx * _fxm_tx
                            # NEVER apply a live price to a CLOSED position — its value is frozen
                            # at the sale (proceeds × USD/ILS on the sell date). Overwriting it with
                            # today's market price would corrupt the realised ILS return.
                            _has_live_tx = (_px_tx > 0) & (_fxm_tx > 0) & (~_clm_f)
                            if _has_live_tx.any():
                                _vi = _vi.copy()
                                _vi[_has_live_tx] = _live_vi_tx[_has_live_tx]
                        _co = _tx["Cost_Origin"].map(_num) if "Cost_Origin" in _tx.columns else pd.Series(0.0, index=_tx.index)
                        _comm_tx = _tx["Commission"].map(_num) if "Commission" in _tx.columns else pd.Series(0.0, index=_tx.index)
                        # Commission is part of the cost basis: include it in BOTH the origin-yield
                        # denominator (Cost_Origin + Commission) AND the ILS-yield denominator
                        # (_ci_eff is already commission-inclusive) so the two yields agree. (audit #10)
                        _cowf = _co + _comm_tx
                        _yic = np.where(_ci_eff > 0, (_vi - _ci_eff) / _ci_eff, np.nan)
                        # Value in origin currency: divide the ILS value by the PARENT-unit FX
                        # rate for EVERY currency (USD/GBP/GBX/EUR/ILS/ILA), mirroring the
                        # dashboard's _value_origin_series so non-USD rows (GBX/EUR/ILA) no
                        # longer leak the raw ILS value into the origin column. (audit fin-fx)
                        _cur_norm_tx = _tx["Origin_Currency"].map(_normalize_currency_code) if "Origin_Currency" in _tx.columns else pd.Series("ILS", index=_tx.index)
                        _por_tx = _cur_norm_tx.map(lambda c: _ils_per_origin_parent_unit(c, _fx))
                        _is_ils_origin = _cur_norm_tx.isin(["ILS", "ILA", ""])
                        _vo = np.where(
                            _por_tx.to_numpy() > 0,
                            _vi / _por_tx.to_numpy(),
                            _vi,
                        )
                        # For ILS/ILA assets Yield_Origin == Yield_ILS by definition (same currency,
                        # no FX). Using Cost_Origin (excl. fees) vs Cost_ILS (incl. fees) would create
                        # a phantom discrepancy, so we force them equal. Other currencies use the
                        # commission-inclusive parent-unit basis.
                        _yoc = np.where(
                            _is_ils_origin.to_numpy(),
                            _yic,  # ILS/ILA assets: Yield_Origin == Yield_ILS
                            np.where(_cowf > 0, (_vo - _cowf) / _cowf, np.nan),
                        )
                        # Always use fresh live yields — don't preserve stale values from the
                        # stored JSON (which may be from an old Google Sheets sync).
                        _tx["Yield_ILS"] = _yic
                        _tx["Yield_Origin"] = _yoc
                        _tx = _tx.drop(columns=["Yield_Current"], errors="ignore")

                    # Reorder yield columns to the right
                    _yils_f = [c for c in _tx.columns if ("\u05ea\u05e9\u05d5\u05d0\u05d4" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower()) and ("\u05e9\u05e7\u05dc" in str(c) or "ils" in str(c).lower())]
                    _yorig_f = [c for c in _tx.columns if ("\u05ea\u05e9\u05d5\u05d0\u05d4" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower()) and ("\u05de\u05e7\u05d5\u05e8" in str(c) or "origin" in str(c).lower())]
                    _ycols_f: List[str] = []
                    for c in _yils_f + _yorig_f:
                        if c not in _ycols_f: _ycols_f.append(c)
                    if not _ycols_f:
                        _ycols_f = [c for c in _tx.columns if "\u05ea\u05e9\u05d5\u05d0\u05d4" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower()]
                    if _ycols_f:
                        _tx = _tx[[c for c in _tx.columns if c not in _ycols_f] + _ycols_f]

                    _render_tx_table(_tx, _lang_f, _mob_f, _open_f)

                _tx_live_fragment()
            else:
                # Static fallback: use the tx_view with prices already computed
                _base_static = st.session_state.get("_tx_frag_base", tx_view)
                if isinstance(_base_static, pd.DataFrame) and not _base_static.empty:
                    # Reorder yield columns
                    _ys1 = [c for c in _base_static.columns if ("\u05ea\u05e9\u05d5\u05d0\u05d4" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower()) and ("\u05e9\u05e7\u05dc" in str(c) or "ils" in str(c).lower())]
                    _ys2 = [c for c in _base_static.columns if ("\u05ea\u05e9\u05d5\u05d0\u05d4" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower()) and ("\u05de\u05e7\u05d5\u05e8" in str(c) or "origin" in str(c).lower())]
                    _ys: List[str] = []
                    for c in _ys1 + _ys2:
                        if c not in _ys: _ys.append(c)
                    if not _ys:
                        _ys = [c for c in _base_static.columns if "\u05ea\u05e9\u05d5\u05d0\u05d4" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower()]
                    if _ys:
                        _base_static = _base_static[[c for c in _base_static.columns if c not in _ys] + _ys]
                _render_tx_table(_base_static if isinstance(_base_static, pd.DataFrame) else tx_view, st.session_state.get("_tx_frag_lang", language), st.session_state.get("_tx_frag_mobile", is_mobile), st.session_state.get("_tx_frag_is_open", is_open_only))


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
        st.markdown(f"### {tr('Reports: Risk, Performance and FIFO', 'דוחות: סיכונים, ביצועים ו-' + _mix_he_with_ltr('FIFO'))}")
        fifo_df = fifo_metrics(trades)
        st.subheader(tr("FIFO Engine", "מנוע " + _mix_he_with_ltr("FIFO")))
        # Educational caption explaining the whole FIFO mechanic in plain language.
        with st.expander(tr("ℹ What does FIFO mean here?", "ℹ מה זה FIFO כאן?"), expanded=False):
            st.markdown(tr(
                "**FIFO = First-In-First-Out.** When you sell part of a position, the system assumes you are selling the OLDEST lots first. "
                "This is the default accounting method for most tax jurisdictions (including Israel's מס רווחי הון) because it tends to produce the largest realised gain — and therefore the highest tax bill — when prices have risen. "
                "\n\n**Why you care:**\n"
                "1. **Realised P/L (רווח ממומש)** — the profit locked in on the lots you've already sold. This is what your tax liability is based on. Note: this figure is NOMINAL and PRE-TAX — it is not inflation-indexed and the 25% Israeli capital-gains tax is not deducted here.\n"
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
                "1. **רווח ממומש (Realized P/L)** — הרווח שכבר 'נעלת' על הלוטים שכבר מכרת. זהו הבסיס לחישוב המס. שים לב: נתון זה הוא נומינלי ולפני מס — הוא אינו צמוד למדד ומס רווחי ההון (25%) אינו מנוכה כאן.\n"
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
                # Zero/empty avg price → "—" (was a misleading "₪0.00" on e.g. SCHD).
                lambda r: ("—" if not float(r[avg_price_col] or 0)
                           else _format_currency_value(float(r[avg_price_col]), r.get(avg_price_currency_col, ""))),
                axis=1,
            )
            fifo_view = fifo_view.drop(columns=[avg_price_currency_col], errors="ignore")
            realized_col = tr("Realized P/L (ILS)", "רווח ממומש (₪)")
            # #1 mobile: the full 5-col FIFO table overflows 390px and silently
            # clips columns. Keep only the essentials on phones so nothing is cut.
            if is_mobile:
                _fifo_keep = [tr("Ticker", "טיקר"),
                              tr("Open Qty (FIFO)", "כמות פתוחה (FIFO)"),
                              tr("Open Cost (ILS)", "עלות פתוחה (₪)"),
                              realized_col]
                _fifo_keep = [c for c in _fifo_keep if c in fifo_view.columns]
                if _fifo_keep:
                    fifo_view = fifo_view[_fifo_keep]
            # No inline ₪ in the cells: the st.dataframe canvas font lacks the shekel
            # glyph and rendered it as a tofu box (▢5,197). The column HEADERS already
            # state "(₪)", so plain numbers are both correct and cleaner.
            _fifo_fmt = {
                tr("Open Qty (FIFO)", "כמות פתוחה (FIFO)"): _smart_qty,
                tr("Open Cost (ILS)", "עלות פתוחה (₪)"): "{:,.0f}",
                realized_col: "{:,.0f}",
            }
            if is_mobile:
                # #2 — wide table headers clipped on phones ("Open Qty (FIF(").
                # Render as per-row cards (key: value) so nothing is ever cut.
                _render_df_mobile_cards(fifo_view, _fifo_fmt, signed_cols=(realized_col,))
            else:
                fifo_styled = fifo_view.style.format(_fifo_fmt)
                fifo_styled = _apply_signed_color(fifo_styled, [realized_col])
                _render_dataframe_adaptive(
                    fifo_styled,
                    is_mobile,
                    force_same_render_path=True,
                    width="stretch",
                    hide_index=True,
                )

        st.divider()
        st.subheader(tr("📊 Risk Metrics", "📊 מדדי סיכון"))

        holdings = open_trades.groupby("Ticker", as_index=False)["Quantity"].sum()
        value_series = portfolio_price_history(tuple(holdings["Ticker"]), tuple(holdings["Quantity"]), days=365)
        metrics = risk_metrics(value_series)

        if is_mobile:
            c1, c2 = st.columns(2)
            c3, c4 = st.columns(2)
        else:
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
            st.divider()
            st.subheader(tr("📈 Historical Portfolio Value", "📈 שווי תיק היסטורי"))
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
            st.plotly_chart(_apply_plotly_theme(fig, is_dark, is_mobile), theme="streamlit", width="stretch")

        if total_value > 0:
            st.divider()
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
                width="stretch",
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
            # Log the real exception to the server console (dev) instead of leaking the
            # raw text — which can expose internal paths / column names — into the UI.
            import traceback as _adv_tb
            _adv_tb.print_exc()
            st.caption(tr(
                "Advanced analytics are temporarily unavailable.",
                "האנליטיקה המתקדמת אינה זמינה כעת.",
            ))

    elif page == page_manage:
        st.markdown(f"### {tr('Trade Management (Add / Edit / Delete)', 'ניהול עסקאות (הוספה / עריכה / מחיקה)')}")
        st.caption(tr(
            "Changes are saved locally first. Use the ☁ Sync panel to push to Google Sheets.",
            "שינויים נשמרים תחילה מקומית. השתמש בחלונית ☁ סנכרון כדי לדחוף לגוגל שיט.",
        ))
        # Show persisted Google-sync error from previous rerun (set when add fails on Google)
        if "_add_trade_google_error" in st.session_state:
            _prev_err = st.session_state.pop("_add_trade_google_error")
            st.warning(f"⚠️ {_prev_err}")
        if is_demo:
            st.info(tr("Demo trade-desk mode: you can safely explore flows and forms without touching your personal portfolio.", "מצב דמו לחדר מסחר: אפשר לבדוק זרימות וטפסים בבטחה בלי לגעת בתיק האישי שלך."))
        # LOCAL-FIRST: writes are always enabled (we write locally); Google is optional
        write_enabled = not is_demo
        has_google_write = (not is_demo) and is_apps_script_web_app_url(_clean(web_url_clean))
        if not has_google_write and not is_demo:
            st.info(tr(
                "💾 Local-only mode — changes are saved on this device. "
                "Configure a Web App URL to also sync to Google Sheets.",
                "💾 מצב מקומי בלבד — שינויים נשמרים במכשיר הזה. "
                "הגדר Web App URL כדי לסנכרן גם עם גוגל שיט.",
            ))

        # ── Action radio FIRST (Add / Edit / Delete) ───────────────────────
        # Per UX overhaul: form ON TOP, full snapshot table at the BOTTOM.
        # For "Add" we don't need the row-selector at all — show only the
        # form. For Edit/Delete the row-selector + the edit/delete form
        # appear together (selection still required to act on a row).
        _action_label_map = {
            tr("➕ Add", "➕ הוספה"): "add",
            tr("✏ Edit", "✏ עריכה"): "edit",
            tr("🗑 Delete", "🗑 מחיקה"): "delete",
        }
        _action_label = st.radio(
            tr("Action", "פעולה"),
            list(_action_label_map.keys()),
            horizontal=True,
            key="manage_top_action",
        )
        mode = _action_label_map[_action_label]
        st.session_state["_manage_mode"] = mode

        if mode == "add":
            st.info(tr(
                "💡 Tip: you can also add a trade in seconds from the **AI agent** — just type "
                "“I bought 0.5 ETH at 12000 on Bit2C” or attach a buy screenshot, then approve.",
                "💡 טיפ: אפשר גם להוסיף עסקה בשניות דרך **סוכן AI** — פשוט כתוב “קניתי 0.5 ETH ב-12000 ב-Bit2C” "
                "או צרף צילום מסך של הקנייה, ואשר.",
            ))

        st.caption(tr(f"Showing {len(trades):,} snapshot transactions, including closed trades.", f"מציג {len(trades):,} עסקאות תמונת מצב, כולל עסקאות סגורות."))
        trade_view = trades.copy()
        manage_fx = _safe_quote("USDILS=X")
        if manage_fx <= 0:
            manage_fx = _usd_ils_rate()
        trade_qty_abs = trade_view.get("Quantity", 0).map(_num).abs()
        trade_origin_currency = trade_view.get("Origin_Currency", "").map(_normalize_currency_code)
        trade_current_ils = trade_view.get("Current_Value_ILS", 0).map(_num)
        trade_current_origin = pd.Series(np.where(
            trade_origin_currency == "USD",
            trade_current_ils / manage_fx,
            trade_current_ils,
        ), index=trade_view.index)
        # Prefer live market prices over stored snapshot price
        _manage_tickers = tuple(sorted({
            _clean(v).upper() for v in trade_view.get("Ticker", pd.Series([], dtype=str)).tolist() if _clean(v)
        }))
        _manage_live_prices = fetch_live_prices(_manage_tickers) if _manage_tickers else {}

        def _manage_live_unit_price(row: pd.Series) -> float:
            t = _clean(row.get("Ticker", "")).upper()
            cur = _normalize_currency_code(row.get("Origin_Currency", ""))
            if t and t in _manage_live_prices:
                p_usd = float(_num(_manage_live_prices.get(t, 0.0)))
                if p_usd > 0:
                    # Decide FX by Yahoo's quote currency, not Origin_Currency, so ILS-quoted
                    # .TA stocks are not multiplied by the USD/ILS rate (~3.6× too high). (audit)
                    yf_cur = _yf_price_currency(t)
                    if yf_cur == "USD" and cur == "ILS":
                        return p_usd * manage_fx
                    return p_usd
            # Fallback: derive from stored current value / qty
            try:
                cur_orig = float(trade_current_origin.loc[row.name])
            except Exception:
                cur_orig = 0.0
            qty_abs = abs(float(_num(row.get("Quantity", 0))))
            return cur_orig / qty_abs if qty_abs > 1e-9 else 0.0

        trade_view["Current_Asset_Value_Display"] = trade_view.apply(_manage_live_unit_price, axis=1)
        trade_view["Current_Asset_Value_Display"] = trade_view.apply(
            lambda r: _format_currency_value(float(r["Current_Asset_Value_Display"]), r.get("Origin_Currency", "")),
            axis=1,
        )
        status_filter = st.multiselect(tr("Status filter", "סינון סטטוס"), sorted([s for s in trade_view["Status"].dropna().astype(str).unique() if s]), default=[], placeholder=tr("Choose statuses…", "בחר סטטוסים…"))
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
                width="stretch",
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

        # `mode` was already chosen above (Action radio at the top of the
        # page — see "manage_top_action" key). Reuse that value instead of
        # rendering a second radio (which would crash on duplicate widget id).
        mode = st.session_state.get("_manage_mode", "add")
        editable_cols = [
            "Current_Location", "Platform", "Type", "Ticker", "Purchase_Date", "Quantity",
            "Origin_Buy_Price", "Cost_Origin", "Origin_Currency", "Commission", "Status", "Sell_Date",
            "Cost_ILS", "Current_Value_ILS", "Action", "Event_Type", "Trade_ID",
        ]
        platforms = trades["Platform"].dropna().astype(str).tolist() if "Platform" in trades.columns else []
        types_raw = trades["Type"].dropna().astype(str).tolist() if "Type" in trades.columns else []
        # Translate type values for display (Hebrew → EN in EN mode)
        _type_vl = VALUE_LABELS.get("Type", {})
        _type_raw_to_disp = {raw: langs.get(language, raw) for raw, langs in _type_vl.items()}
        _type_disp_to_raw = {v: k for k, v in _type_raw_to_disp.items()}
        types = [_type_raw_to_disp.get(t, t) for t in types_raw]
        # Default type label in current language
        _default_type = _type_raw_to_disp.get("קריפטו", tr("Crypto", "קריפטו"))
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
                    _field_type = _select_or_type(tr("Asset type", "סוג נכס"), types, _default_type, "add_type", tr, help_text=field_help["Type"])
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
                    "Type": _type_disp_to_raw.get(_field_type, _field_type),  # translate back to canonical Hebrew
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
                        _mark_total_loss = st.checkbox(
                            tr("Mark as total loss (sold for nothing)", "סמן כהפסד מוחלט (נמכר ללא תמורה)"),
                            value=False, key="add_sell_total_loss",
                        )
                        new_row["Sell_Price_Origin"] = float(sell_price_origin)

                        ratio = (sell_qty / src_qty) if src_qty > 1e-12 else 0.0
                        allocated_cost = float(src_cost * ratio)
                        allocated_commission = float(src_commission * ratio)
                        # Backdated sale: value proceeds at the sell-DATE FX (matching GAS
                        # sRate), not today's spot. Generalized to every currency; warn the
                        # user when the historical lookup fails so a spot-fallback isn't
                        # silently frozen into the stored row. (audit #9, fin-fx)
                        _sell_cur0 = _normalize_currency_code(source_row.get("Origin_Currency", ""))
                        if _sell_cur0 in {"ILS", "ILA", ""}:
                            fx_for_origin = 1.0
                        else:
                            _pair0 = {"USD": "USDILS=X", "GBX": "GBPILS=X", "GBP": "GBPILS=X", "EUR": "EURILS=X"}.get(_sell_cur0, f"{_sell_cur0}ILS=X")
                            _sell_fx_hist = _fx_pair_on(_pair0, sell_date_txt)
                            if _sell_fx_hist and _sell_fx_hist > 0:
                                fx_for_origin = _sell_fx_hist
                            else:
                                _live0 = _fx_pair_rate(_pair0) if _pair0 != "USDILS=X" else manage_fx
                                fx_for_origin = float(_live0) if _live0 and _live0 > 0 else manage_fx
                                st.warning(tr("Historical FX for the sell date was unavailable; using today's spot rate. Realized P&L may drift.",
                                              "שער חליפין היסטורי לתאריך המכירה אינו זמין; נעשה שימוש בשער הנוכחי. הרווח הממומש עשוי לסטות."))

                        _sell_cur = _normalize_currency_code(source_row.get("Origin_Currency", ""))
                        # Freeze the closed slice's ILS cost basis at the BUY-date FX
                        # (allocated Cost_Origin + buy commission) so realized P&L is
                        # deterministic and the realized-ILS column populates. (audit fin-streamlit #1)
                        _frozen_cost_ils = _frozen_cost_ils_for_sell(
                            allocated_cost, allocated_commission, _sell_cur, src_purchase_txt
                        )
                        new_row.update(
                            {
                                "Current_Location": _clean(source_row.get("Current_Location", "")),
                                "Platform": _clean(source_row.get("Platform", "")),
                                "Type": _clean(source_row.get("Type", "")),
                                "Ticker": _clean(source_row.get("Ticker", "")).upper(),
                                "Origin_Currency": _sell_cur,
                                "Purchase_Date": src_purchase_txt,
                                "Quantity": float(sell_qty),
                                "Origin_Buy_Price": src_buy_price,
                                "Cost_Origin": allocated_cost,
                                "Commission": allocated_commission,
                                "Status": "סגור",
                                "Sell_Date": sell_date_txt,
                                "Current_Value_ILS": float(sell_qty * sell_price_origin * fx_for_origin),
                                "Cost_ILS": _frozen_cost_ils,
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
                        _mark_total_loss = st.checkbox(
                            tr("Mark as total loss (sold for nothing)", "סמן כהפסד מוחלט (נמכר ללא תמורה)"),
                            value=False, key="add_sell_total_loss_manual",
                        )
                        new_row["Sell_Price_Origin"] = float(sell_price_origin)
                        # Backdated sale: value proceeds at the sell-DATE FX (matching GAS
                        # sRate), not today's spot. Generalized to every currency; warn when
                        # the historical lookup fails so a spot-fallback isn't silently
                        # frozen into the stored row. (audit #9, fin-fx)
                        _sell_cur_m = _normalize_currency_code(new_row.get("Origin_Currency", ""))
                        if _sell_cur_m in {"ILS", "ILA", ""}:
                            fx_for_origin = 1.0
                        else:
                            _pair_m = {"USD": "USDILS=X", "GBX": "GBPILS=X", "GBP": "GBPILS=X", "EUR": "EURILS=X"}.get(_sell_cur_m, f"{_sell_cur_m}ILS=X")
                            _sell_fx_hist_m = _fx_pair_on(_pair_m, _sell_date_manual)
                            if _sell_fx_hist_m and _sell_fx_hist_m > 0:
                                fx_for_origin = _sell_fx_hist_m
                            else:
                                _live_m = _fx_pair_rate(_pair_m) if _pair_m != "USDILS=X" else manage_fx
                                fx_for_origin = float(_live_m) if _live_m and _live_m > 0 else manage_fx
                                st.warning(tr("Historical FX for the sell date was unavailable; using today's spot rate. Realized P&L may drift.",
                                              "שער חליפין היסטורי לתאריך המכירה אינו זמין; נעשה שימוש בשער הנוכחי. הרווח הממומש עשוי לסטות."))
                        new_row["Current_Value_ILS"] = float(new_row["Quantity"] * sell_price_origin * fx_for_origin)
                        # Freeze the ILS cost basis at the BUY-date FX (Cost_Origin +
                        # buy commission) so realized P&L is deterministic / the
                        # realized-ILS column populates. (audit fin-streamlit #1)
                        new_row["Cost_ILS"] = _frozen_cost_ils_for_sell(
                            _sell_cost, _sell_comm,
                            _normalize_currency_code(new_row.get("Origin_Currency", "")),
                            _sell_buy_date,
                        )

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
                    # Freeze a real ILS cost basis at the buy-date FX immediately (mirrors
                    # web/mobile BUY) so local FIFO/aggregates are commission- & FX-correct
                    # before any reload repopulates it from Google. (audit staged-buy)
                    new_row["Cost_ILS"] = _frozen_cost_ils_for_sell(
                        float(_num(new_row.get("Cost_Origin", 0.0))), float(_num(_buy_comm)),
                        _normalize_currency_code(new_row.get("Origin_Currency", "")),
                        _buy_date,
                    )

                new_row["Trade_ID"] = _to_trade_id(pd.Series(new_row))
                submitted = st.form_submit_button(tr("💾 Save trade", "💾 שמור עסקה"), type="primary", width="stretch")

            if submitted:
                if not write_enabled:
                    st.error(tr("Cannot save in this mode.", "לא ניתן לשמור במצב זה."))
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
                        # Allow a zero-proceeds SELL only when explicitly marked a TOTAL LOSS
                        # (write-off). FIFO books it as a realized loss; rejecting it omitted a
                        # real loss from the closed record. (audit fin-tax total-loss)
                        _is_total_loss = bool(st.session_state.get("add_sell_total_loss")) or bool(st.session_state.get("add_sell_total_loss_manual"))
                        _sale_val = float(_num(new_row.get("Current_Value_ILS", 0.0)))
                        if _sale_val < 0 or (_sale_val == 0 and not _is_total_loss):
                            add_errors.append(tr("For SELL, sale price must produce a positive sale value (or tick 'Mark as total loss').", "בעסקת מכירה, מחיר המכירה חייב להפיק שווי מכירה חיובי (או סמן 'הפסד מוחלט')."))

                    if add_errors:
                        for e in add_errors:
                            st.error(e)
                        st.stop()

                    # GOOGLE-FIRST ATOMIC: push to Google first; save locally only on success.
                    if not has_google_write:
                        st.error(tr(
                            "Google Sheets connection required. Configure a valid Web App URL to save trades.",
                            "נדרש חיבור לגוגל שיט. הגדר Web App URL תקין כדי לשמור עסקאות.",
                        ))
                        st.stop()

                    ok, msg = sync_trade_to_sheet(web_url_clean, api_token, "add", new_row)
                    if not ok:
                        st.error(f"{tr('Push to Google failed — no changes saved.', 'הדחיפה לגוגל נכשלה — לא נשמר שום שינוי.')}: {msg}")
                        st.stop()

                    # Google succeeded → save locally (mark_dirty=False: already on Google)
                    apply_local_trade("add", new_row, mark_dirty=False)

                    if partial_sell_meta and bool(partial_sell_meta.get("is_partial", False)):
                        src = dict(partial_sell_meta.get("source_row", {}))
                        src_trade_id = _clean(partial_sell_meta.get("source_trade_id", ""))
                        _rem_cost = float(_num(partial_sell_meta.get("remaining_cost", 0.0)))
                        _rem_comm = float(_num(partial_sell_meta.get("remaining_commission", 0.0)))
                        _rem_cur = _normalize_currency_code(src.get("Origin_Currency", ""))
                        source_update = {
                            "Trade_ID": src_trade_id,
                            "Current_Location": _clean(src.get("Current_Location", "")),
                            "Platform": _clean(src.get("Platform", "")),
                            "Type": _clean(src.get("Type", "")),
                            "Ticker": _clean(src.get("Ticker", "")).upper(),
                            "Purchase_Date": _clean(src.get("Purchase_Date", "")),
                            "Quantity": float(_num(partial_sell_meta.get("remaining_qty", 0.0))),
                            "Origin_Buy_Price": float(_num(src.get("Origin_Buy_Price", 0.0))),
                            "Cost_Origin": _rem_cost,
                            "Origin_Currency": _rem_cur,
                            "Commission": _rem_comm,
                            "Status": "פתוח",
                            "Sell_Date": "",
                            # Freeze the surviving open lot's ILS basis at the buy-date FX
                            # too, so the residual position is priced/yielded off a real
                            # basis rather than Cost_Origin*today. (audit fin-streamlit #1)
                            "Cost_ILS": _frozen_cost_ils_for_sell(
                                _rem_cost, _rem_comm, _rem_cur, _clean(src.get("Purchase_Date", ""))
                            ),
                            "Current_Value_ILS": 0.0,
                            "Action": "BUY",
                            "Event_Type": "TRADE",
                        }
                        ok_edit, msg_edit = sync_trade_to_sheet(web_url_clean, api_token, "edit", source_update)
                        if not ok_edit:
                            st.warning(
                                tr(
                                    "Sale row was added, but source lot update failed on Google. Please edit the source lot manually.",
                                    "שורת המכירה נוספה, אך עדכון לוט המקור נכשל בגוגל. יש לעדכן את לוט המקור ידנית.",
                                )
                            )
                            st.error(msg_edit)
                        else:
                            apply_local_trade("edit", source_update, mark_dirty=False)

                    st.success(tr("Trade added (local + Google Sheets)", "הרשומה נוספה (מקומי + גוגל שיט)"))
                    st.info(msg)
                    # Refresh local store from Google to get server-assigned Trade_IDs.
                    # IMPORTANT: clear cache FIRST so we get the post-add snapshot (not stale).
                    load_google_snapshot_data.clear()
                    load_google_snapshot_data_via_gspread.clear()
                    try:
                        _fresh_df = load_google_snapshot_data(web_url_clean, api_token)
                        save_local_portfolio(
                            _fresh_df,
                            preserve_dirty=False,
                            meta_patch={"_last_synced": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")},
                        )
                    except Exception:
                        pass
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
                            # Translate current val to display form, show translated options, store back as canonical
                            _type_val_disp = _type_raw_to_disp.get(_clean(val), _clean(val))
                            _type_result = _select_or_type(tr("Asset type", "סוג נכס"), types, _type_val_disp, f"edit_{selected}_type", tr)
                            edited[col] = _type_disp_to_raw.get(_type_result, _type_result)
                        elif col == "Ticker":
                            edited[col] = _select_or_type(tr("Ticker", "טיקר"), tickers, _clean(val), f"edit_{selected}_ticker", tr).upper()
                        elif col == "Origin_Currency":
                            edited[col] = _select_or_type(tr("Origin currency", "מטבע מקור"), currencies, _clean(val), f"edit_{selected}_currency", tr).upper()
                        elif col == "Status":
                            _status_opts = [tr("Open", "פתוח"), tr("Closed", "סגור")]
                            _status_idx = 1 if _is_closed_status(val) else 0
                            _status_sel = st.selectbox(tr("Status", "סטטוס"), _status_opts, index=_status_idx, key=f"e_{col}")
                            # Always store in canonical Hebrew form (consistent with the data sheet)
                            edited[col] = "סגור" if _status_sel == tr("Closed", "סגור") else "פתוח"
                        elif col == "Action":
                            edited[col] = st.selectbox(col, ["BUY", "SELL"], index=0 if _clean(val) != "SELL" else 1, key=f"e_{col}")
                        else:
                            edited[col] = st.text_input(col, value=_clean(val), key=f"e_{col}")
                    submitted = st.form_submit_button(tr("Update", "עדכן"))

                if submitted:
                    if not write_enabled:
                        st.error(tr("Cannot update in gspread mode. Configure a valid Web App URL on the left.", "לא ניתן לעדכן במצב gspread. הגדר Web App URL תקין בצד שמאל."))
                    elif not has_google_write:
                        st.error(tr(
                            "Google Sheets connection required. Configure a valid Web App URL to update trades.",
                            "נדרש חיבור לגוגל שיט. הגדר Web App URL תקין כדי לעדכן עסקאות.",
                        ))
                    else:
                        edited["Trade_ID"] = selected
                        # GOOGLE-FIRST ATOMIC: push to Google first; save locally only on success.
                        ok, msg = sync_trade_to_sheet(web_url_clean, api_token, "edit", edited)
                        if ok:
                            apply_local_trade("edit", edited, mark_dirty=False)
                            st.success(tr("Trade updated (local + Google Sheets)", "הרשומה עודכנה (מקומי + גוגל שיט)"))
                            st.info(msg)
                            load_google_snapshot_data.clear()
                            load_google_snapshot_data_via_gspread.clear()
                            st.rerun()
                        else:
                            st.error(f"{tr('Update failed in Google Sheets — no changes saved.', 'עדכון נכשל בגוגל שיט — לא נשמר שום שינוי.')}: {msg}")

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
                        # Remove from local store (clean delete — already on Google)
                        apply_local_trade("delete", delete_payload, mark_dirty=False)
                        st.success(tr("Trade deleted (local + Google Sheets)", "הרשומה נמחקה (מקומי + גוגל שיט)"))
                        st.info(msg)
                        load_google_snapshot_data.clear()
                        load_google_snapshot_data_via_gspread.clear()
                        st.rerun()
                    else:
                        # GOOGLE-FIRST ATOMIC: do NOT remove locally if Google failed
                        st.error(f"{tr('Delete failed in Google Sheets — no changes saved.', 'מחיקה נכשלה בגוגל שיט — לא נשמר שום שינוי.')}: {msg}")

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

    elif page == page_chat:
        try:
            render_ai_chat_page(
                tr=tr, df=df, web_app_url=web_url_clean, token=api_token,
                language=language, is_dark=is_dark, is_mobile=is_mobile,
            )
        except Exception as _chat_exc:
            st.error(tr(f"AI Chat unavailable: {_chat_exc}", f"צ'אט AI אינו זמין: {_chat_exc}"))

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

        if is_mobile:
            cqa1, cqa2 = st.columns(2)
            cqa3, cqa4 = st.columns(2)
        else:
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
                # #5/#6 — readable % label OUTSIDE the bar in theme-aware text
                # (was green-on-green inside; removing it lost the value). The
                # x-range is padded to 1.18 so the outside label has room on the
                # full 100% bars.
                _lbl_col = "#e2e8f0" if is_dark else "#334155"
                fig_cc.update_traces(
                    hovertemplate="<b>%{y}</b><br>" + tr("Completeness", "שלמות") + ": %{x:.1%}<extra></extra>",
                    texttemplate="%{x:.0%}", textposition="outside",
                    textfont=dict(color=_lbl_col, size=12),
                    cliponaxis=False,
                )
                fig_cc.update_layout(
                    xaxis_tickformat=".0%",
                    xaxis=dict(range=[0, 1.18]),
                    yaxis_title="",
                    yaxis=dict(automargin=True),
                    margin=dict(l=150, r=20, t=50, b=20),
                    coloraxis_showscale=False,
                    height=max(320, 22 * len(col_df) + 80),
                    # Title must read light on navy in dark mode — theme="streamlit"
                    # otherwise forces Streamlit's native (light) theme onto it.
                    title_font=dict(color="#f1f5f9" if is_dark else "#0f172a"),
                )
                st.plotly_chart(_apply_plotly_theme(fig_cc, is_dark, is_mobile, is_bar=True), theme=None, width="stretch")
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
            # Normalize yield/return values to ratio then format as %
            pct_cols = [
                c for c in recent_view.columns
                if ("תשואה" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower())
            ]
            def _to_ratio_for_display_dq(v: object) -> float:
                s = _clean(v)
                if not s:
                    return np.nan
                n = _num(v)
                return n / 100.0 if "%" in s else n
            for _pc in pct_cols:
                recent_view[_pc] = recent_view[_pc].map(_to_ratio_for_display_dq)
            recent_fmt: Dict[str, object] = {c: "{:.2%}" for c in pct_cols if c in recent_view.columns}
            recent_styled = recent_view.style.format(recent_fmt, na_rep="")
            if signed_cols:
                recent_styled = _apply_signed_color(recent_styled, signed_cols)
            _render_dataframe_adaptive(
                recent_styled,
                is_mobile,
                force_same_render_path=True,
                width="stretch",
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
        st.plotly_chart(_apply_plotly_theme(fig, is_dark, is_mobile), theme="streamlit", width="stretch")

        st.subheader(tr("Recent Data", "נתונים אחרונים"))
        _render_recent_data_table(df)

    if live_updates and page == page_manage:
        st.sidebar.caption(tr("⏸ Live price refresh paused while editing trades.", "⏸ רענון חי מושהה בזמן עריכת עסקאות."))


if __name__ == "__main__":
    main()
##