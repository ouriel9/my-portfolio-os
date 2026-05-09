"""
Patch app.py: replace the static Transactions tab rendering with a live-price @st.fragment.
Removes lines 9600-9871 (0-indexed: 9599-9870) and inserts the fragment code.
"""
import sys

TARGET = "C:/Users/ourie/PycharmProjects/my-portfolio-os/app.py"

NEW_CODE = """\
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
                \"\"\"Shared rendering helper used by both the fragment and the static fallback.\"\"\"
                def _nrm_hdr(col: str) -> str:
                    c = _clean(col)
                    return {"\\u05e2\\u05dc\\u05d5\\u05ea \\u05e9\\u05e7\\u05dc\\u05d9\\u05ea": "\\u05e2\\u05dc\\u05d5\\u05ea ILS"}.get(c, c)

                def _is_hid(cn: object) -> bool:
                    low = re.sub(r"\\s+", " ", re.sub(r"\\s*\\(\\d+\\)$", "", _clean(cn)).lower().replace("_", " ").replace("-", " ")).strip()
                    return low in {"origin buy price", "buy price", "\\u05e9\\u05e2\\u05e8 \\u05e7\\u05e0\\u05d9\\u05d9\\u05d4", "\\u05e9\\u05e2\\u05e8 \\u05e7\\u05e0\\u05d9\\u05d4", "sell status", "sale status", "\\u05e1\\u05d8\\u05d8\\u05d5\\u05e1 \\u05de\\u05db\\u05d9\\u05e8\\u05d4"}

                def _oo_hid(cn: object) -> bool:
                    c = _clean(cn)
                    if c in {"Status", "Action", "Sell_Date", "Sell_Price_Origin", "Yield_At_Sale",
                             "\\u05e1\\u05d8\\u05d8\\u05d5\\u05e1", "\\u05e4\\u05e2\\u05d5\\u05dc\\u05d4", "\\u05ea\\u05d0\\u05e8\\u05d9\\u05da \\u05de\\u05db\\u05d9\\u05e8\\u05d4", "\\u05e9\\u05e2\\u05e8 \\u05de\\u05db\\u05d9\\u05e8\\u05d4", "\\u05de\\u05d7\\u05d9\\u05e8 \\u05de\\u05db\\u05d9\\u05e8\\u05d4", "\\u05ea\\u05e9\\u05d5\\u05d0\\u05d4 \\u05d1\\u05de\\u05db\\u05d9\\u05e8\\u05d4"}:
                        return True
                    return c.lower() in {"sell price", "sale price", "sell date", "return at sale"}

                def _is_blank(v: object) -> bool:
                    if pd.isna(v): return True
                    return _clean(v).lower() in {"", "nan", "nat", "none"}

                def _nck(cn: object) -> str:
                    return re.sub(r"\\s+", " ", re.sub(r"\\s*\\(\\d+\\)$", "", _clean(cn)).lower().replace("_", " ").replace("-", " ")).strip()

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
                    st.info(tr("No transactions to display", "\\u05d0\\u05d9\\u05df \\u05e2\\u05e1\\u05e7\\u05d0\\u05d5\\u05ea \\u05dc\\u05d4\\u05e6\\u05d2\\u05d4"))
                    return

                _dv = _tx.copy().reset_index(drop=True)
                if not _dv.columns.is_unique:
                    _seen2: Dict[str, int] = {}; _uniq2: List[str] = []
                    for c in _dv.columns:
                        b = str(c); _seen2[b] = _seen2.get(b, 0) + 1
                        _uniq2.append(b if _seen2[b] == 1 else f"{b} ({_seen2[b]})")
                    _dv.columns = _uniq2

                for _ag in [
                    ["Sell_Price_Origin", "Sell Price", "Sale Price", "\\u05de\\u05d7\\u05d9\\u05e8 \\u05de\\u05db\\u05d9\\u05e8\\u05d4", "\\u05e9\\u05e2\\u05e8 \\u05de\\u05db\\u05d9\\u05e8\\u05d4"],
                    ["Yield_ILS", "Return ILS", "\\u05ea\\u05e9\\u05d5\\u05d0\\u05d4 ILS", "\\u05ea\\u05e9\\u05d5\\u05d0\\u05d4 \\u05e9\\u05e7\\u05dc\\u05d9\\u05ea", "\\u05ea\\u05e9\\u05d5\\u05d0\\u05d4 \\u05e0\\u05d8\\u05d5 (\\u20aa)", "Net Return (ILS)"],
                    ["Yield_Origin", "Return (Origin)", "\\u05ea\\u05e9\\u05d5\\u05d0\\u05d4 \\u05d1\\u05e9\\u05e2\\u05e8 \\u05de\\u05e7\\u05d5\\u05e8", "\\u05ea\\u05e9\\u05d5\\u05d0\\u05d4 \\u05de\\u05e7\\u05d5\\u05e8", "\\u05ea\\u05e9\\u05d5\\u05d0\\u05d4 \\u05e0\\u05d8\\u05d5 (\\u05de\\u05e7\\u05d5\\u05e8)", "Net Return (Origin)"],
                    ["Current_Value_ILS", "Value ILS", "Current Value (ILS)", "\\u05e9\\u05d5\\u05d5\\u05d9 ILS", "\\u05e9\\u05d5\\u05d5\\u05d9 \\u05e9\\u05e7\\u05dc\\u05d9", "\\u05e9\\u05d5\\u05d5\\u05d9 \\u05e2\\u05d3\\u05db\\u05e0\\u05d9 (\\u20aa)"],
                    ["Value USD", "Current Value (USD)", "\\u05e9\\u05d5\\u05d5\\u05d9 USD", "\\u05e9\\u05d5\\u05d5\\u05d9 \\u05d1\\u05d3\\u05d5\\u05dc\\u05e8", "\\u05e9\\u05d5\\u05d5\\u05d9 \\u05e2\\u05d3\\u05db\\u05e0\\u05d9 (USD)"],
                ]:
                    _dv = _coalesce_tx(_dv, _ag)

                _drop_c2: List[str] = []
                for col in _dv.columns:
                    ct2 = _clean(col); cl2 = ct2.lower()
                    if cl2.startswith("unnamed:"): _drop_c2.append(col); continue
                    if ("usd" in cl2 and "ils" in cl2) or ("\\u05e9\\u05e2\\u05e8" in ct2 and "\\u05d3\\u05d5\\u05dc\\u05e8" in ct2 and "\\u05e9\\u05e7\\u05dc" in ct2): _drop_c2.append(col); continue
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

                _yc2 = [c for c in _dv.columns if "\\u05ea\\u05e9\\u05d5\\u05d0\\u05d4" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower()]
                for c in _yc2:
                    _dv[c] = _dv[c].map(_to_ratio2)

                _sfmt2: Dict[str, object] = {}
                for dc in ["Purchase_Date", "\\u05ea\\u05d0\\u05e8\\u05d9\\u05da \\u05e8\\u05db\\u05d9\\u05e9\\u05d4", "Sell_Date", "\\u05ea\\u05d0\\u05e8\\u05d9\\u05da \\u05de\\u05db\\u05d9\\u05e8\\u05d4"]:
                    if dc in _dv.columns: _sfmt2[dc] = _dfmt2
                for yc in _yc2:
                    if yc in _dv.columns: _sfmt2[yc] = "{:.2%}"

                _styled2 = _dv.style
                if _sfmt2: _styled2 = _styled2.format(_sfmt2, na_rep="")
                if _yc2: _styled2 = _apply_signed_color(_styled2, _yc2)
                _render_dataframe_adaptive(_styled2, _mob, force_same_render_path=True, use_container_width=True, hide_index=True)

            if live_updates and hasattr(st, "fragment"):
                @st.fragment(run_every="20s")
                def _tx_live_fragment() -> None:
                    _base = st.session_state.get("_tx_frag_base", pd.DataFrame())
                    _lang_f = st.session_state.get("_tx_frag_lang", LANG_HE)
                    _mob_f = st.session_state.get("_tx_frag_mobile", False)
                    _open_f = st.session_state.get("_tx_frag_is_open", False)
                    if _base is None or (isinstance(_base, pd.DataFrame) and _base.empty):
                        st.info(tr("No transactions to display", "\\u05d0\\u05d9\\u05df \\u05e2\\u05e1\\u05e7\\u05d0\\u05d5\\u05ea \\u05dc\\u05d4\\u05e6\\u05d2\\u05d4"))
                        return
                    _tx = _base.copy()
                    # Re-fetch live prices (TTL=25 s — yfinance 1-min intraday data)
                    if "Ticker" in _tx.columns:
                        _fx = _safe_quote("USDILS=X") or 3.6
                        _tks = tuple(sorted({_clean(v).upper() for v in _tx["Ticker"].tolist() if _clean(v)}))
                        _lpm = fetch_live_prices(_tks) if _tks else {}

                        def _mpo(row: pd.Series) -> float:
                            t = _clean(row.get("Ticker", "")).upper()
                            if not t: return np.nan
                            p = float(_num(_lpm.get(t, 0.0)))
                            if p <= 0: return np.nan
                            cur = _normalize_currency_code(row.get("Origin_Currency", ""))
                            return p if cur == "USD" else (p * _fx if cur == "ILS" else p)

                        def _spo(row: pd.Series) -> float:
                            if not _is_closed_status(row.get("Status", "")): return np.nan
                            qty = float(_num(row.get("Quantity", 0.0)))
                            val = float(_num(row.get("Current_Value_ILS", 0.0)))
                            if qty > 1e-9 and val > 0:
                                cur = _normalize_currency_code(row.get("Origin_Currency", ""))
                                unit = val / qty
                                return unit / _fx if cur == "USD" else unit
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
                            _syc = np.where(_bp > 0, (_tx["Sell_Price_Origin"] - _bp) / _bp, np.nan)
                            if "Yield_At_Sale" in _tx.columns:
                                _hys = ~_tx["Yield_At_Sale"].map(lambda v: pd.isna(v) or _clean(v) == "")
                                _tx["Yield_At_Sale"] = np.where(_hys, _tx["Yield_At_Sale"], _syc)
                            else:
                                _tx["Yield_At_Sale"] = _syc

                        _ci = _tx["Cost_ILS"].map(_num) if "Cost_ILS" in _tx.columns else pd.Series(0.0, index=_tx.index)
                        _vi = _tx["Current_Value_ILS"].map(_num) if "Current_Value_ILS" in _tx.columns else pd.Series(0.0, index=_tx.index)
                        _yic = np.where(_ci > 0, (_vi - _ci) / _ci, np.nan)
                        _co = _tx["Cost_Origin"].map(_num) if "Cost_Origin" in _tx.columns else pd.Series(0.0, index=_tx.index)
                        _vo = np.where(
                            _tx["Origin_Currency"].map(_normalize_currency_code) == "USD",
                            np.where(_fx > 0, _vi / _fx, np.nan), _vi,
                        )
                        _yoc = np.where(_co > 0, (_vo - _co) / _co, np.nan)
                        if "Yield_ILS" in _tx.columns:
                            _hyi = ~_tx["Yield_ILS"].map(lambda v: pd.isna(v) or _clean(v) == "")
                            _tx["Yield_ILS"] = np.where(_hyi, _tx["Yield_ILS"], _yic)
                        else:
                            _tx["Yield_ILS"] = _yic
                        if "Yield_Origin" in _tx.columns:
                            _hyo = ~_tx["Yield_Origin"].map(lambda v: pd.isna(v) or _clean(v) == "")
                            _tx["Yield_Origin"] = np.where(_hyo, _tx["Yield_Origin"], _yoc)
                        else:
                            _tx["Yield_Origin"] = _yoc
                        _tx = _tx.drop(columns=["Yield_Current"], errors="ignore")

                    # Reorder yield columns to the right
                    _yils_f = [c for c in _tx.columns if ("\\u05ea\\u05e9\\u05d5\\u05d0\\u05d4" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower()) and ("\\u05e9\\u05e7\\u05dc" in str(c) or "ils" in str(c).lower())]
                    _yorig_f = [c for c in _tx.columns if ("\\u05ea\\u05e9\\u05d5\\u05d0\\u05d4" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower()) and ("\\u05de\\u05e7\\u05d5\\u05e8" in str(c) or "origin" in str(c).lower())]
                    _ycols_f: List[str] = []
                    for c in _yils_f + _yorig_f:
                        if c not in _ycols_f: _ycols_f.append(c)
                    if not _ycols_f:
                        _ycols_f = [c for c in _tx.columns if "\\u05ea\\u05e9\\u05d5\\u05d0\\u05d4" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower()]
                    if _ycols_f:
                        _tx = _tx[[c for c in _tx.columns if c not in _ycols_f] + _ycols_f]

                    ts = datetime.now().strftime("%H:%M:%S")
                    st.caption(f"\\U0001f7e2 {tr('Live prices', '\\u05e9\\u05e2\\u05e8\\u05d9\\u05dd \\u05d7\\u05d9\\u05d9\\u05dd')} \\u00b7 {ts}")
                    _render_tx_table(_tx, _lang_f, _mob_f, _open_f)

                _tx_live_fragment()
            else:
                # Static fallback: use the tx_view with prices already computed
                _base_static = st.session_state.get("_tx_frag_base", tx_view)
                if isinstance(_base_static, pd.DataFrame) and not _base_static.empty:
                    # Reorder yield columns
                    _ys1 = [c for c in _base_static.columns if ("\\u05ea\\u05e9\\u05d5\\u05d0\\u05d4" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower()) and ("\\u05e9\\u05e7\\u05dc" in str(c) or "ils" in str(c).lower())]
                    _ys2 = [c for c in _base_static.columns if ("\\u05ea\\u05e9\\u05d5\\u05d0\\u05d4" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower()) and ("\\u05de\\u05e7\\u05d5\\u05e8" in str(c) or "origin" in str(c).lower())]
                    _ys: List[str] = []
                    for c in _ys1 + _ys2:
                        if c not in _ys: _ys.append(c)
                    if not _ys:
                        _ys = [c for c in _base_static.columns if "\\u05ea\\u05e9\\u05d5\\u05d0\\u05d4" in str(c) or "yield" in str(c).lower() or "return" in str(c).lower()]
                    if _ys:
                        _base_static = _base_static[[c for c in _base_static.columns if c not in _ys] + _ys]
                _render_tx_table(_base_static if isinstance(_base_static, pd.DataFrame) else tx_view, st.session_state.get("_tx_frag_lang", language), st.session_state.get("_tx_frag_mobile", is_mobile), st.session_state.get("_tx_frag_is_open", is_open_only))
"""

# Read file
with open(TARGET, encoding='utf-8') as f:
    lines = f.readlines()

# Line indices (0-based): 9599 to 9870 inclusive (line numbers 9600-9871)
start_idx = 9599
end_idx = 9871  # exclusive

print(f"Removing lines {start_idx+1} to {end_idx} ({end_idx - start_idx} lines)")
print(f"Inserting {len(NEW_CODE.splitlines())} new lines")

# Build new file
new_lines = lines[:start_idx] + [NEW_CODE + "\n"] + lines[end_idx:]

with open(TARGET, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print("Done! File written.")

