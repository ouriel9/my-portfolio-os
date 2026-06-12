from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import flet as ft
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from core import (
    AppConfig,
    build_exposure_summary,
    build_reports,
    fetch_prices,
    fifo_metrics,
    load_config,
    load_manual_deposits_remote,
    load_manual_deposits_store,
    load_snapshot_data,
    portfolio_price_history,
    prepare_core_views,
    save_manual_deposits_remote,
    save_manual_deposits_store,
    build_demo_snapshot_data,
    # New imports — simulator + risk maths ported from app.py
    SIM_PERSISTED_KEYS,
    load_sim_prefs,
    save_sim_prefs,
    sim_project_portfolio,
    sim_safe_withdrawal_monthly,
    sim_years_to_target,
    risk_metrics,
)
import numpy as np


@dataclass
class AppState:
    config: AppConfig
    source_mode: str
    df: pd.DataFrame
    core: Dict[str, object]
    reports: Dict[str, object]
    deposits_mode: str
    deposits_rows: List[Dict[str, object]]


def _fmt_num(v: float, d: int = 0) -> str:
    return f"{float(v):,.{d}f}"


def _load_initial_state() -> AppState:
    config = load_config()
    df, source_mode = load_snapshot_data(config)
    core = prepare_core_views(df)
    reports = build_reports(core["open_trades"])
    deposits_mode = "demo" if config.demo_mode else "live"

    rows = []
    if config.web_app_url and config.api_token:
        ok, remote_rows, _ = load_manual_deposits_remote(config.web_app_url, config.api_token, deposits_mode)
        if ok:
            rows = remote_rows
    if not rows:
        rows = load_manual_deposits_store().get(deposits_mode, [])

    return AppState(
        config=config,
        source_mode=source_mode,
        df=df,
        core=core,
        reports=reports,
        deposits_mode=deposits_mode,
        deposits_rows=rows,
    )


def main(page: ft.Page) -> None:
    page.title = "Portfolio Manager OS"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.padding = 0
    page.scroll = ft.ScrollMode.HIDDEN
    page.window_min_width = 900
    page.window_min_height = 620

    # ── Brand theme ──────────────────────────────────────────────────
    BRAND = "#4f46e5"
    page.theme = ft.Theme(color_scheme_seed=BRAND, use_material3=True)
    page.dark_theme = ft.Theme(color_scheme_seed=BRAND, use_material3=True)

    try:
        state = _load_initial_state()
    except Exception as exc:
        cfg = load_config()
        fallback_df = build_demo_snapshot_data()
        fallback_core = prepare_core_views(fallback_df)
        state = AppState(
            config=cfg,
            source_mode=f"startup_fallback ({exc})",
            df=fallback_df,
            core=fallback_core,
            reports=build_reports(fallback_core["open_trades"]),
            deposits_mode="demo",
            deposits_rows=[],
        )

    # Language is mutable so the user can toggle EN/HE from the header.
    lang_is_he = ["עבר" in str(state.config.language)]

    def _is_he() -> bool:
        return lang_is_he[0]

    def tr(en: str, he: str) -> str:
        return he if _is_he() else en

    is_dark = [False]  # mutable flag for theme toggle

    def _chart_template() -> str:
        """Return Plotly template that follows the desktop theme."""
        return "plotly_dark" if is_dark[0] else "plotly_white"

    def _is_dark() -> bool:
        return is_dark[0]

    def _card_bg() -> str:
        return "#1e1e2e" if _is_dark() else "#ffffff"

    def _card_border() -> str:
        return "#3a3a5c" if _is_dark() else "#e2e8f0"

    def _text_primary() -> str:
        return "#f1f5f9" if _is_dark() else "#0f172a"

    def _text_secondary() -> str:
        return "#94a3b8" if _is_dark() else "#64748b"

    def _page_bg() -> str:
        return "#0f0f1a" if _is_dark() else "#f8fafc"

    def _rail_bg() -> str:
        return "#1a1a2e" if _is_dark() else "#ffffff"

    # ── Helpers ───────────────────────────────────────────────────────
    def render_plotly_or_fallback(fig: go.Figure, title: str) -> ft.Control:
        if hasattr(ft, "PlotlyChart"):
            return ft.PlotlyChart(fig, expand=True)
        return ft.Container(
            content=ft.Column([
                ft.Text(title, size=16, weight=ft.FontWeight.BOLD),
                ft.Text("Chart rendering unavailable in this Flet build.",
                        color=ft.Colors.BLUE_GREY_700),
            ], tight=True),
            border=ft.border.all(1, ft.Colors.BLUE_GREY_100),
            border_radius=10,
            padding=12,
        )

    def df_to_table(df: pd.DataFrame, numeric_cols: List[str] | None = None) -> ft.DataTable:
        numeric_cols = numeric_cols or []
        cols = [ft.DataColumn(ft.Text(str(c), weight=ft.FontWeight.BOLD, size=12, color=_text_secondary())) for c in df.columns]
        rows = []
        for _, r in df.iterrows():
            cells = []
            for c in df.columns:
                val = r[c]
                if c in numeric_cols:
                    try:
                        text = _fmt_num(float(val), 2)
                    except Exception:
                        text = str(val)
                else:
                    text = str(val)
                cells.append(ft.DataCell(ft.Text(text, size=12, color=_text_primary())))
            rows.append(ft.DataRow(cells=cells))
        return ft.DataTable(
            columns=cols, rows=rows, column_spacing=14, horizontal_margin=8,
            heading_row_color=ft.Colors.with_opacity(0.04, ft.Colors.ON_SURFACE),
            data_row_max_height=38,
        )

    def kpi_card(label: str, value: str, subtitle: str = "", positive: bool | None = None) -> ft.Container:
        delta_color = ft.Colors.GREEN_400 if positive is True else (ft.Colors.RED_400 if positive is False else _text_secondary())
        return ft.Container(
            content=ft.Column([
                ft.Text(label, size=11, color=_text_secondary(), weight=ft.FontWeight.W_500),
                ft.Text(value, size=22, weight=ft.FontWeight.BOLD, color=_text_primary()),
                ft.Text(subtitle, size=11, color=delta_color),
            ], tight=True, spacing=4),
            bgcolor=_card_bg(),
            border=ft.border.all(1, _card_border()),
            border_radius=14,
            padding=ft.padding.symmetric(horizontal=18, vertical=14),
            expand=True,
            shadow=ft.BoxShadow(blur_radius=12, color=ft.Colors.with_opacity(0.07, ft.Colors.BLACK), offset=ft.Offset(0, 3)),
        )

    # ── Status elements ───────────────────────────────────────────────
    status_bar = ft.Text("", size=11, color=ft.Colors.BLUE_GREY_400)
    source_chip = ft.Text(f"⚡ {state.source_mode}", size=11, color=ft.Colors.BLUE_GREY_400)
    loading_ring = ft.ProgressRing(width=18, height=18, stroke_width=2, visible=False)
    content_area = ft.Container(expand=True, padding=ft.padding.all(16))

    def _set_status(msg: str, color: str = ft.Colors.BLUE_GREY_400):
        status_bar.value = msg
        status_bar.color = color
        page.update()

    # ── Data refresh ──────────────────────────────────────────────────
    def refresh_data(_=None):
        nonlocal state
        loading_ring.visible = True
        _set_status(tr("Refreshing…", "מרענן…"), ft.Colors.BLUE_400)
        try:
            # Manual refresh must bypass the fast-open cache and hit Google.
            df, source_mode = load_snapshot_data(state.config, prefer_cache=False)
            state.df = df
            state.source_mode = source_mode
            state.core = prepare_core_views(df)
            state.reports = build_reports(state.core["open_trades"])
            source_chip.value = f"⚡ {state.source_mode}"
            _set_status(tr("Data refreshed", "הנתונים עודכנו"), ft.Colors.GREEN_600)
        except Exception as exc:
            _set_status(str(exc), ft.Colors.RED_600)
        loading_ring.visible = False
        render_view()

    # ── Theme toggle ──────────────────────────────────────────────────
    def toggle_theme(_=None):
        is_dark[0] = not is_dark[0]
        page.theme_mode = ft.ThemeMode.DARK if is_dark[0] else ft.ThemeMode.LIGHT
        render_view()

    # ── View builders ─────────────────────────────────────────────────
    selected_watch_symbol: ft.Ref[ft.Dropdown] = ft.Ref()

    def dashboard_view() -> ft.Control:
        core = state.core
        open_trades = core["open_trades"]
        closed_trades = state.core.get("closed_trades", pd.DataFrame())
        summary = build_exposure_summary(open_trades)

        total_value = float(core["total_value"])
        total_cost = float(core["total_cost"])
        total_profit = float(core["total_profit"])
        total_return = float(core["total_return"])

        is_profit = total_profit >= 0
        kpis = ft.ResponsiveRow(
            controls=[
                ft.Container(col={"xs": 6, "md": 3}, content=kpi_card(
                    tr("Total Value (ILS)", "שווי כולל (₪)"),
                    f"₪{_fmt_num(total_value)}",
                    (("+" if is_profit else "") + f"₪{_fmt_num(total_profit)}"),
                    positive=is_profit,
                )),
                ft.Container(col={"xs": 6, "md": 3}, content=kpi_card(
                    tr("Total Cost (ILS)", "עלות כוללת (₪)"),
                    f"₪{_fmt_num(total_cost)}",
                )),
                ft.Container(col={"xs": 6, "md": 3}, content=kpi_card(
                    tr("Total Return", "תשואה כוללת"),
                    f"{total_return:.2%}",
                    positive=total_return >= 0,
                )),
                ft.Container(col={"xs": 6, "md": 3}, content=kpi_card(
                    tr("Open / Closed", "פתוחות / סגורות"),
                    str(len(open_trades)),
                    f"{len(closed_trades)} {tr('closed', 'סגורות')}",
                )),
            ],
            spacing=10,
            run_spacing=10,
        )

        fig_pie = go.Figure()
        if not summary.empty:
            fig_pie = px.pie(
                summary, names="Ticker", values="Value_ILS", hole=0.45,
                title=tr("Allocation by Ticker", "חלוקה לפי טיקר"),
                color_discrete_sequence=px.colors.qualitative.Vivid,
            )
            fig_pie.update_traces(textposition="outside", textinfo="percent+label")
            fig_pie.update_layout(
                template=_chart_template(),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=10, t=40, b=10),
                showlegend=False,
            )

        bar_fig = go.Figure()
        if not summary.empty:
            df_bar = summary.sort_values("Net_PnL_ILS", ascending=False).copy()
            df_bar["Color"] = df_bar["Net_PnL_ILS"].apply(lambda x: "#10b981" if x >= 0 else "#ef4444")
            bar_fig = go.Figure(go.Bar(
                x=df_bar["Ticker"], y=df_bar["Net_PnL_ILS"],
                marker_color=df_bar["Color"],
                hovertemplate="%{x}<br>₪%{y:,.0f}<extra></extra>",
            ))
            bar_fig.update_layout(
                template=_chart_template(),
                title=tr("Net P/L by Asset (Open Only)", "רווח/הפסד לפי נכס (פתוחים בלבד)"),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=10, t=40, b=10),
                yaxis_tickformat=",.0f",
                showlegend=False,
            )

        table_ctrl: ft.Control = ft.Text(tr("No open positions", "אין פוזיציות פתוחות"))
        if not summary.empty:
            cols_show = [c for c in ["Ticker", "Current_Price", "Open_Qty", "Cost_ILS", "Value_ILS", "Net_PnL_ILS", "Yield_ILS"] if c in summary.columns]
            table_ctrl = ft.Container(
                content=ft.Column([df_to_table(summary[cols_show], numeric_cols=["Current_Price", "Open_Qty", "Cost_ILS", "Value_ILS", "Net_PnL_ILS", "Yield_ILS"])], scroll=ft.ScrollMode.AUTO),
                border=ft.border.all(1, _card_border()),
                border_radius=12,
                padding=8,
                bgcolor=_card_bg(),
            )

        watch_options = [
            "BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "BINANCE:SOLUSDT",
            "NASDAQ:VOO", "NASDAQ:QQQ", "NASDAQ:NVDA", "FX_IDC:USDILS",
        ]
        chart_col = ft.Column()

        def render_watch_chart(_=None):
            symbol = selected_watch_symbol.current.value if selected_watch_symbol.current else watch_options[0]
            ticker = symbol.split(":")[-1].replace("USDT", "").replace("ILS", "").replace("IDC", "")
            hist = portfolio_price_history((ticker,), (1.0,), days=180)
            if hist.empty:
                chart_col.controls = [ft.Text(tr("No chart data", "אין נתוני גרף"))]
            else:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=hist.index, y=hist.values, mode="lines", name=ticker,
                    line=dict(color=BRAND, width=2),
                    fill="tozeroy", fillcolor="rgba(79,70,229,0.10)",
                ))
                fig.update_layout(
                    template=_chart_template(),
                    title=f"{symbol}",
                    height=340,
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=10, r=10, t=36, b=10),
                    xaxis=dict(showgrid=False),
                    yaxis=dict(showgrid=True, gridcolor="rgba(148,163,184,0.15)"),
                )
                chart_col.controls = [render_plotly_or_fallback(fig, symbol)]
            page.update()

        watch_row = ft.Row([
            ft.Dropdown(
                ref=selected_watch_symbol,
                label=tr("Watchlist", "רשימת מעקב"),
                value=watch_options[0],
                options=[ft.dropdown.Option(v) for v in watch_options],
                width=300,
            ),
            ft.FilledButton(tr("Load Chart", "טען גרף"), on_click=render_watch_chart, icon=ft.Icons.SHOW_CHART),
        ], wrap=True)
        render_watch_chart()

        return ft.Column([
            kpis,
            ft.ResponsiveRow([
                ft.Container(col={"xs": 12, "md": 6}, content=ft.Container(
                    content=render_plotly_or_fallback(fig_pie, tr("Allocation", "חלוקה")),
                    border=ft.border.all(1, _card_border()), border_radius=12, bgcolor=_card_bg(), padding=8,
                )),
                ft.Container(col={"xs": 12, "md": 6}, content=ft.Container(
                    content=render_plotly_or_fallback(bar_fig, tr("Net P/L", "רווח/הפסד")),
                    border=ft.border.all(1, _card_border()), border_radius=12, bgcolor=_card_bg(), padding=8,
                )),
            ], spacing=10, run_spacing=10),
            ft.Text(tr("Open Positions", "פוזיציות פתוחות"), size=18, weight=ft.FontWeight.BOLD, color=_text_primary()),
            table_ctrl,
            ft.Divider(color=_card_border()),
            watch_row,
            chart_col,
        ], spacing=14, scroll=ft.ScrollMode.AUTO, expand=True)

    def transactions_view() -> ft.Control:
        trades = state.core["trades"].copy()
        if trades.empty:
            return ft.Text(tr("No transactions", "אין עסקאות"), color=_text_secondary())
        subset = [c for c in ["Purchase_Date", "Platform", "Type", "Ticker", "Quantity", "Cost_ILS", "Current_Value_ILS", "Status", "Trade_ID"] if c in trades.columns]
        t = trades[subset].copy().sort_values("Purchase_Date", ascending=False)
        return ft.Column([
            ft.Text(tr("Transactions", "עסקאות"), size=20, weight=ft.FontWeight.BOLD, color=_text_primary()),
            ft.Container(
                content=ft.Column([df_to_table(t.head(300), numeric_cols=["Quantity", "Cost_ILS", "Current_Value_ILS"])], scroll=ft.ScrollMode.AUTO),
                border=ft.border.all(1, _card_border()), border_radius=12, bgcolor=_card_bg(), padding=8,
            ),
        ], spacing=12, scroll=ft.ScrollMode.AUTO, expand=True)

    def _kpi_with_tooltip(label: str, value: str, tooltip: str, positive: bool | None = None) -> ft.Container:
        """KPI card identical visual to kpi_card but with a tooltipped (i) icon."""
        delta_color = ft.Colors.GREEN_400 if positive is True else (ft.Colors.RED_400 if positive is False else _text_secondary())
        return ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Text(label, size=11, color=_text_secondary(), weight=ft.FontWeight.W_500),
                    ft.Container(
                        content=ft.Icon(ft.Icons.INFO_OUTLINED, size=14, color=_text_secondary()),
                        tooltip=tooltip,
                    ),
                ], spacing=6, alignment=ft.MainAxisAlignment.START),
                ft.Text(value, size=22, weight=ft.FontWeight.BOLD, color=delta_color),
            ], tight=True, spacing=4),
            bgcolor=_card_bg(),
            border=ft.border.all(1, _card_border()),
            border_radius=14,
            padding=ft.padding.symmetric(horizontal=18, vertical=14),
            expand=True,
            shadow=ft.BoxShadow(blur_radius=12, color=ft.Colors.with_opacity(0.07, ft.Colors.BLACK), offset=ft.Offset(0, 3)),
        )

    def risk_view() -> ft.Control:
        trades = state.core["trades"]
        open_trades = state.core["open_trades"]
        fifo = fifo_metrics(trades)
        tickers = tuple(open_trades.get("Ticker", []) if not open_trades.empty else [])
        qtys = tuple(open_trades.get("Quantity", []) if not open_trades.empty else [])
        hist = portfolio_price_history(tickers, qtys, days=365)

        # ── Sharpe / Vol / MDD / CAGR KPI row ──
        m = risk_metrics(hist) if not hist.empty else {"sharpe": 0.0, "vol": 0.0, "mdd": 0.0, "cagr": 0.0}
        tip_sharpe = tr(
            "Sharpe Ratio = (Portfolio Return − Risk-Free) / Annualized Volatility. <0 losing to cash · 0–1 sub-par · 1–2 good · 2–3 very strong · >3 outlier.",
            "יחס שארפ = (תשואת התיק − ריבית חסרת סיכון) / תנודתיות שנתית. <0 מפסיד למזומן · 0-1 מתחת לממוצע · 1-2 טוב · 2-3 חזק מאוד · >3 חריג.",
        )
        tip_vol = tr(
            "Annualized Volatility = std-dev of daily returns × √252. Bonds 3-8% · diversified equity 12-18% · large-cap 20-30% · crypto 60-100%+.",
            "תנודתיות שנתית = סטיית-תקן יומית × √252. אג\"ח 3-8% · תיק מניות מפוזר 12-18% · מניה גדולה 20-30% · קריפטו 60-100%+.",
        )
        tip_mdd = tr(
            "Max Drawdown — largest peak-to-trough decline. To recover from -20% need +25%; from -50% need +100%.",
            "משיכה מקסימלית — הירידה הגדולה ביותר משיא לשפל. להתאוששות מ-20%- צריך +25%; מ-50%- צריך +100%.",
        )
        tip_cagr = tr(
            "Compound Annual Growth Rate = (Final/Initial)^(1/years) − 1. S&P 500 long-run ≈ 10% nominal · 60/40 ≈ 8% · inflation ≈ 3%.",
            "שיעור צמיחה שנתי מורכב = (סופי/התחלתי)^(1/שנים) − 1. עוגנים: S&P 500 ≈ 10% נומינלי · 60/40 ≈ 8% · אינפלציה ≈ 3%.",
        )
        kpis = ft.ResponsiveRow([
            ft.Container(col={"xs": 6, "md": 3}, content=_kpi_with_tooltip("Sharpe", f"{m['sharpe']:.2f}", tip_sharpe, positive=m['sharpe'] > 0)),
            ft.Container(col={"xs": 6, "md": 3}, content=_kpi_with_tooltip(tr("Annual Volatility", "תנודתיות שנתית"), f"{m['vol']:.2%}", tip_vol)),
            ft.Container(col={"xs": 6, "md": 3}, content=_kpi_with_tooltip(tr("Max Drawdown", "משיכה מקסימלית"), f"{m['mdd']:.2%}", tip_mdd, positive=False if m['mdd'] < 0 else None)),
            ft.Container(col={"xs": 6, "md": 3}, content=_kpi_with_tooltip("CAGR", f"{m['cagr']:.2%}", tip_cagr, positive=m['cagr'] > 0)),
        ], spacing=10, run_spacing=10)

        fig = go.Figure()
        if not hist.empty:
            fig.add_trace(go.Scatter(
                x=hist.index, y=hist.values, mode="lines",
                name=tr("Portfolio Value", "שווי תיק"),
                line=dict(color=BRAND, width=2),
                fill="tozeroy", fillcolor="rgba(79,70,229,0.08)",
            ))
            fig.update_layout(
                template=_chart_template(),
                title=tr("Portfolio Value History (365d)", "היסטוריית שווי תיק (365 יום)"),
                height=360,
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=10, t=40, b=10),
                yaxis_tickformat=",.0f",
            )

        # ── FIFO explainer expander, mirrors app.py:8215 ──
        fifo_explainer_text = tr(
            "FIFO = First-In-First-Out. When you sell part of a position, the system assumes you are selling the OLDEST lots first. "
            "This is the default for most tax jurisdictions (incl. Israel's מס רווחי הון).\n\n"
            "Why you care:\n"
            "• Realised P/L — the profit locked in on lots already sold (this is what your tax bill is based on).\n"
            "• Open Cost — remaining cost basis of lots still held; compare to current value for unrealised P/L.\n"
            "• Average Buy Price — weighted average of still-open lots; quick break-even check.\n\n"
            "Worked example: bought 10 BTC at ₪50K, then 10 more at ₪100K, then sold 10 at ₪150K.\n"
            "  FIFO sells first 10 lots → realised gain = 10 × (150K − 50K) = ₪1,000K.\n"
            "  Remaining open: 10 BTC at cost ₪100K each.\n"
            "  LIFO would have realised only ₪500K — but FIFO is the default.",
            "FIFO = ראשון-שנכנס-ראשון-שיוצא. בעת מכירה חלקית של פוזיציה, המערכת מניחה שהלוטים הוותיקים נמכרים ראשונים. "
            "זוהי שיטת ברירת-המחדל ברוב מדינות העולם (כולל מס רווחי הון בישראל).\n\n"
            "למה חשוב:\n"
            "• רווח ממומש — הרווח שכבר 'נעלת' על לוטים שנמכרו (זהו הבסיס למס).\n"
            "• עלות פתוחה — עלות הלוטים שעדיין מוחזקים; השוואה לשווי הנוכחי = רווח לא-ממומש.\n"
            "• מחיר קנייה ממוצע — ממוצע משוקלל של הלוטים הפתוחים; לבדיקת נקודת איזון.\n\n"
            "דוגמה: 10 BTC ב-₪50K, אז עוד 10 ב-₪100K, אז מכרת 10 ב-₪150K.\n"
            "  FIFO מוכר את 10 הלוטים הראשונים → רווח ממומש = 10 × (150K − 50K) = ₪1,000K.\n"
            "  פוזיציה פתוחה שנותרה: 10 BTC בעלות ₪100K כל אחד.\n"
            "  LIFO היה נותן רווח ממומש של ₪500K בלבד — אך FIFO היא ברירת-המחדל.",
        )
        fifo_explainer = ft.ExpansionTile(
            title=ft.Text(tr("ℹ What does FIFO mean here?", "ℹ מה זה FIFO כאן?"),
                          weight=ft.FontWeight.W_600, color=_text_primary()),
            controls=[
                ft.Container(
                    content=ft.Text(fifo_explainer_text, size=12, color=_text_primary(), selectable=True),
                    padding=ft.padding.all(14),
                ),
            ],
            collapsed_bgcolor=_card_bg(),
            bgcolor=_card_bg(),
            expanded=False,
        )

        fifo_ctrl: ft.Control = ft.Text(tr("No FIFO data", "אין נתוני FIFO"), color=_text_secondary())
        if not fifo.empty:
            fifo_ctrl = ft.Container(
                content=ft.Column([df_to_table(fifo, numeric_cols=["Open_Qty_FIFO", "Open_Cost_ILS", "Realized_PnL_ILS", "Average_Buy_Price"])], scroll=ft.ScrollMode.AUTO),
                border=ft.border.all(1, _card_border()), border_radius=12, bgcolor=_card_bg(), padding=8,
            )

        return ft.Column([
            ft.Text(tr("Risk & FIFO Analysis", "ניתוח סיכונים ו-FIFO"), size=20, weight=ft.FontWeight.BOLD, color=_text_primary()),
            kpis,
            ft.Container(
                content=fifo_explainer,
                border=ft.border.all(1, _card_border()), border_radius=12, bgcolor=_card_bg(),
            ),
            ft.Text(tr("FIFO Engine", "מנוע פיפו"), size=16, weight=ft.FontWeight.BOLD, color=_text_primary()),
            fifo_ctrl,
            ft.Container(
                content=render_plotly_or_fallback(fig, tr("Portfolio Value History", "היסטוריית שווי")),
                border=ft.border.all(1, _card_border()), border_radius=12, bgcolor=_card_bg(), padding=8,
            ),
        ], spacing=14, scroll=ft.ScrollMode.AUTO, expand=True)

    report_type_control: ft.Dropdown | None = None
    report_area = ft.Column()

    def refresh_report_area(_=None):
        r = state.reports
        key = report_type_control.value if report_type_control else "winner"
        if key == "winner":
            df = r.get("winner_loser_table", pd.DataFrame())
            report_area.controls = [df_to_table(df, numeric_cols=["Yield_ILS"]) if not df.empty else ft.Text(tr("No data", "אין נתונים"), color=_text_secondary())]
        elif key == "platform":
            df = r.get("net_investment_table", pd.DataFrame())
            report_area.controls = [df_to_table(df, numeric_cols=["Net_Investment_ILS", "Current_Value_ILS", "PnL_ILS"]) if not df.empty else ft.Text(tr("No data", "אין נתונים"), color=_text_secondary())]
        else:
            rates = r.get("live_rates", {})
            df = pd.DataFrame([{"Symbol": k, "Rate": v} for k, v in rates.items()])
            report_area.controls = [df_to_table(df, numeric_cols=["Rate"]) if not df.empty else ft.Text(tr("No data", "אין נתונים"), color=_text_secondary())]
        page.update()

    def reports_view() -> ft.Control:
        nonlocal report_type_control
        if report_type_control is None:
            report_type_control = ft.Dropdown(
                label=tr("Report Type", "סוג דוח"),
                value="winner",
                width=280,
                options=[
                    ft.dropdown.Option("winner", tr("Winner/Loser", "מנצח/מפסיד")),
                    ft.dropdown.Option("platform", tr("Net Investment by Platform", "השקעה נטו לפי פלטפורמה")),
                    ft.dropdown.Option("rates", tr("Live Rates", "שערים חיים")),
                ],
                on_select=refresh_report_area,
            )
            report_type_control.on_change = refresh_report_area
            refresh_report_area()
        return ft.Column([
            ft.Text(tr("Reports", "דוחות"), size=20, weight=ft.FontWeight.BOLD, color=_text_primary()),
            report_type_control,
            ft.Container(
                content=report_area,
                border=ft.border.all(1, _card_border()), border_radius=12, bgcolor=_card_bg(), padding=8,
            ),
        ], spacing=14, scroll=ft.ScrollMode.AUTO, expand=True)

    def allocation_view() -> ft.Control:
        open_trades = state.core["open_trades"]
        summary = build_exposure_summary(open_trades)
        if summary.empty:
            return ft.Text(tr("No allocation data", "אין נתוני חלוקה"), color=_text_secondary())

        alloc_fig = px.pie(
            summary, names="Ticker", values="Value_ILS", hole=0.45,
            title=tr("Allocation by Ticker", "חלוקה לפי טיקר"),
            color_discrete_sequence=px.colors.qualitative.Vivid,
        )
        alloc_fig.update_layout(template=_chart_template(), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", margin=dict(l=10, r=10, t=40, b=10))

        type_mix = open_trades.groupby("Type", as_index=False)["Current_Value_ILS"].sum() if not open_trades.empty else pd.DataFrame(columns=["Type", "Current_Value_ILS"])
        type_fig = go.Figure()
        if not type_mix.empty:
            type_fig = px.bar(
                type_mix, x="Type", y="Current_Value_ILS",
                title=tr("Allocation by Asset Class", "חלוקה לפי סוג נכס"),
                color_discrete_sequence=[BRAND],
            )
            type_fig.update_layout(template=_chart_template(), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", margin=dict(l=10, r=10, t=40, b=10))

        return ft.ResponsiveRow([
            ft.Container(col={"xs": 12, "md": 6}, content=ft.Container(
                content=render_plotly_or_fallback(alloc_fig, tr("Allocation by Ticker", "חלוקה לפי טיקר")),
                border=ft.border.all(1, _card_border()), border_radius=12, bgcolor=_card_bg(), padding=8,
            )),
            ft.Container(col={"xs": 12, "md": 6}, content=ft.Container(
                content=render_plotly_or_fallback(type_fig, tr("Allocation by Asset Class", "חלוקה לפי סוג נכס")),
                border=ft.border.all(1, _card_border()), border_radius=12, bgcolor=_card_bg(), padding=8,
            )),
        ], spacing=10, run_spacing=10)

    def manage_view() -> ft.Control:
        trades = state.core["trades"].copy()
        if trades.empty:
            return ft.Text(tr("No transactions", "אין עסקאות"), color=_text_secondary())
        subset = [c for c in ["Trade_ID", "Purchase_Date", "Platform", "Type", "Ticker", "Quantity", "Cost_ILS", "Current_Value_ILS", "Status"] if c in trades.columns]
        view_df = trades[subset].copy().sort_values("Purchase_Date", ascending=False)
        return ft.Column([
            ft.Text(tr("Trade Management", "ניהול עסקאות"), size=20, weight=ft.FontWeight.BOLD, color=_text_primary()),
            ft.Container(
                content=ft.Text(
                    tr("Desktop app mirrors and analyzes data. Write actions remain on Streamlit.",
                       "האפליקציה בדסקטופ מציגה ומנתחת נתונים; פעולות כתיבה נשארות ב-Streamlit."),
                    size=12, color=_text_secondary(),
                ),
                bgcolor=ft.Colors.with_opacity(0.06, ft.Colors.BLUE),
                border_radius=8, padding=ft.padding.symmetric(horizontal=12, vertical=8),
            ),
            ft.Container(
                content=ft.Column([df_to_table(view_df.head(400), numeric_cols=["Quantity", "Cost_ILS", "Current_Value_ILS"])], scroll=ft.ScrollMode.AUTO),
                border=ft.border.all(1, _card_border()), border_radius=12, bgcolor=_card_bg(), padding=8,
            ),
        ], spacing=12, scroll=ft.ScrollMode.AUTO, expand=True)

    deposit_platform_control: ft.Dropdown | None = None
    deposit_amount_control: ft.TextField | None = None
    deposit_table = ft.Column()

    def redraw_deposit_table():
        rows = state.deposits_rows
        if not rows:
            deposit_table.controls = [ft.Text(tr("No rows", "אין שורות"), color=_text_secondary())]
        else:
            df = pd.DataFrame(rows)
            deposit_table.controls = [df_to_table(df, numeric_cols=["Manual_Deposit_ILS"])]

    def upsert_deposit(_=None):
        platform = deposit_platform_control.value if deposit_platform_control else ""
        amount = float((deposit_amount_control.value if deposit_amount_control else 0.0) or 0.0)
        if not platform:
            _set_status(tr("Please choose a platform", "בחר פלטפורמה"), ft.Colors.RED_600)
            return
        found = False
        for row in state.deposits_rows:
            if str(row.get("Platform")) == platform:
                row["Manual_Deposit_ILS"] = amount
                found = True
                break
        if not found:
            state.deposits_rows.append({"Platform": platform, "Manual_Deposit_ILS": amount})
        state.deposits_rows = sorted(state.deposits_rows, key=lambda x: str(x.get("Platform", "")).lower())
        redraw_deposit_table()
        _set_status(tr("Row updated locally", "השורה עודכנה מקומית"), ft.Colors.BLUE_600)

    def save_deposits(_=None):
        store = load_manual_deposits_store()
        store[state.deposits_mode] = state.deposits_rows
        save_manual_deposits_store(store)

        if state.config.web_app_url and state.config.api_token:
            ok, msg = save_manual_deposits_remote(state.config.web_app_url, state.config.api_token, state.deposits_mode, state.deposits_rows)
            if ok:
                _set_status(tr("Saved locally + cloud", "נשמר מקומית + ענן"), ft.Colors.GREEN_600)
            else:
                _set_status(f"{tr('Saved locally, cloud failed:', 'נשמר מקומית, ענן נכשל:')} {msg}", ft.Colors.ORANGE_700)
        else:
            _set_status(tr("Saved locally", "נשמר מקומית"), ft.Colors.GREEN_600)

    def reload_deposits(_=None):
        rows = []
        if state.config.web_app_url and state.config.api_token:
            ok, remote_rows, _ = load_manual_deposits_remote(state.config.web_app_url, state.config.api_token, state.deposits_mode)
            if ok:
                rows = remote_rows
        if not rows:
            rows = load_manual_deposits_store().get(state.deposits_mode, [])
        state.deposits_rows = rows
        redraw_deposit_table()
        _set_status(tr("Deposits reloaded", "הפקדות נטענו מחדש"), ft.Colors.BLUE_600)

    def deposits_view() -> ft.Control:
        nonlocal deposit_platform_control, deposit_amount_control
        platforms = sorted({str(p) for p in state.core["trades"].get("Platform", []) if str(p).strip()}) if not state.core["trades"].empty else []
        if deposit_platform_control is None:
            deposit_platform_control = ft.Dropdown(label=tr("Platform", "פלטפורמה"), width=200, options=[ft.dropdown.Option(p) for p in platforms])
        else:
            deposit_platform_control.options = [ft.dropdown.Option(p) for p in platforms]
        if deposit_amount_control is None:
            deposit_amount_control = ft.TextField(label=tr("Manual Deposit ILS", "הפקדה ידנית ₪"), width=200, value="0", prefix=ft.Text("₪"))

        redraw_deposit_table()
        total_manual = sum(float(r.get("Manual_Deposit_ILS", 0.0)) for r in state.deposits_rows)

        return ft.Column([
            ft.Text(tr("Total Deposits", "סך הפקדות"), size=20, weight=ft.FontWeight.BOLD, color=_text_primary()),
            ft.Container(
                content=ft.Text(f"₪{_fmt_num(total_manual, 2)}", size=28, weight=ft.FontWeight.BOLD, color=_text_primary()),
                bgcolor=_card_bg(), border=ft.border.all(1, _card_border()), border_radius=12,
                padding=ft.padding.symmetric(horizontal=20, vertical=14),
                shadow=ft.BoxShadow(blur_radius=10, color=ft.Colors.with_opacity(0.07, ft.Colors.BLACK), offset=ft.Offset(0, 3)),
            ),
            ft.Row([
                deposit_platform_control,
                deposit_amount_control,
                ft.FilledButton(tr("Upsert", "עדכן"), on_click=upsert_deposit, icon=ft.Icons.EDIT),
                ft.FilledButton(tr("Save", "שמור"), on_click=save_deposits, icon=ft.Icons.SAVE),
                ft.OutlinedButton(tr("Reload", "טען מחדש"), on_click=reload_deposits, icon=ft.Icons.REFRESH),
            ], wrap=True),
            ft.Container(
                content=deposit_table,
                border=ft.border.all(1, _card_border()), border_radius=12, bgcolor=_card_bg(), padding=8,
            ),
        ], spacing=14, scroll=ft.ScrollMode.AUTO, expand=True)

    def quality_view() -> ft.Control:
        df = state.df
        total = int(df.shape[0] * df.shape[1]) if not df.empty else 0
        non_empty = int(df.notna().sum().sum()) if not df.empty else 0
        completeness = (non_empty / total) if total else 0.0

        rows_stat = kpi_card(tr("Rows", "שורות"), f"{len(df):,}")
        cols_stat = kpi_card(tr("Columns", "עמודות"), f"{len(df.columns):,}")
        comp_stat = kpi_card(tr("Completeness", "שלמות"), f"{completeness:.2%}", positive=completeness >= 0.9)
        source_stat = kpi_card(tr("Data Source", "מקור נתונים"), state.source_mode)

        return ft.Column([
            ft.Text(tr("Data Quality", "בקרת נתונים"), size=20, weight=ft.FontWeight.BOLD, color=_text_primary()),
            ft.ResponsiveRow([
                ft.Container(col={"xs": 6, "md": 3}, content=rows_stat),
                ft.Container(col={"xs": 6, "md": 3}, content=cols_stat),
                ft.Container(col={"xs": 6, "md": 3}, content=comp_stat),
                ft.Container(col={"xs": 6, "md": 3}, content=source_stat),
            ], spacing=10, run_spacing=10),
        ], spacing=14, scroll=ft.ScrollMode.AUTO, expand=True)

    # ── Simulator view (port of render_simulator_page from app.py:5909) ──
    # Persisted inputs live in core.SIM_PERSISTED_KEYS / sim_user_prefs.json.
    sim_state: Dict[str, object] = dict(load_sim_prefs())  # hydrate from disk

    def _sim_get(key: str, default):
        v = sim_state.get(key)
        return v if v is not None else default

    def simulator_view() -> ft.Control:
        # Refs we update from inside event handlers.
        age_now_field: ft.Ref[ft.TextField] = ft.Ref()
        age_target_field: ft.Ref[ft.TextField] = ft.Ref()
        annual_return_field: ft.Ref[ft.TextField] = ft.Ref()
        monthly_field: ft.Ref[ft.TextField] = ft.Ref()
        lump_field: ft.Ref[ft.TextField] = ft.Ref()
        lump_month_field: ft.Ref[ft.Slider] = ft.Ref()
        swr_field: ft.Ref[ft.Slider] = ft.Ref()
        infl_field: ft.Ref[ft.TextField] = ft.Ref()
        initial_field: ft.Ref[ft.TextField] = ft.Ref()
        mode_radio: ft.Ref[ft.RadioGroup] = ft.Ref()

        # Dynamic content area updated by re-projecting on every input change.
        kpi_row = ft.ResponsiveRow(spacing=10, run_spacing=10)
        chart_holder = ft.Column()
        milestones_row = ft.ResponsiveRow(spacing=10, run_spacing=10)
        delay_caption = ft.Text("", size=12, color=_text_secondary())
        infl_caption = ft.Text("", size=12, color=_text_secondary())
        breakdown_holder = ft.Column()

        tv_total = float(state.core.get("total_value", 0.0) or 0.0)
        has_portfolio = tv_total > 0.0
        mode_clean = tr("🧼 Clean Simulator", "🧼 סימולטור נקי")
        mode_mine = tr("💼 My Portfolio Simulator", "💼 סימולטור התיק שלי")
        default_mode = mode_mine if has_portfolio else mode_clean
        current_mode = str(_sim_get("sim_mode_choice", default_mode))
        if current_mode not in (mode_clean, mode_mine) or (current_mode == mode_mine and not has_portfolio):
            current_mode = default_mode
            sim_state["sim_mode_choice"] = current_mode

        def _safe_num(value: object, default: float = 0.0) -> float:
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        def recompute(_=None):
            """Read inputs, run sim_project_portfolio, update KPIs/chart/etc."""
            try:
                age_now = int(_safe_num(age_now_field.current.value if age_now_field.current else 30, 30))
                age_target = int(_safe_num(age_target_field.current.value if age_target_field.current else 67, 67))
                if age_target <= age_now:
                    age_target = age_now + 1
                annual_return = _safe_num(annual_return_field.current.value if annual_return_field.current else 7.0, 7.0)
                monthly_contrib = _safe_num(monthly_field.current.value if monthly_field.current else 2000.0, 2000.0)
                lump_sum = _safe_num(lump_field.current.value if lump_field.current else 0.0, 0.0)
                swr = _safe_num(swr_field.current.value if swr_field.current else 4.0, 4.0)
                infl = _safe_num(infl_field.current.value if infl_field.current else 3.0, 3.0)
                use_portfolio = (mode_radio.current.value == mode_mine) if mode_radio.current else (current_mode == mode_mine)
                if use_portfolio:
                    initial_capital = tv_total
                else:
                    initial_capital = _safe_num(initial_field.current.value if initial_field.current else 0.0, 0.0)

                years_total = max(1.0, float(age_target - age_now))
                months_total = int(round(years_total * 12))
                lump_month = int(_safe_num(lump_month_field.current.value if lump_month_field.current else 0, 0))
                lump_month = max(0, min(lump_month, months_total))
                if lump_month_field.current:
                    lump_month_field.current.max = months_total
                    lump_month_field.current.value = lump_month
                    lump_month_field.current.disabled = lump_sum <= 0.0

                # Persist on every change
                sim_state.update({
                    "sim_age_now": age_now, "sim_age_target": age_target,
                    "sim_initial_clean": initial_capital if not use_portfolio else _sim_get("sim_initial_clean", 0.0),
                    "sim_annual_return": annual_return, "sim_monthly_contrib": monthly_contrib,
                    "sim_lump_sum": lump_sum, "sim_lump_month": lump_month,
                    "sim_swr": swr, "sim_annual_inflation": infl,
                    "sim_mode_choice": mode_radio.current.value if mode_radio.current else current_mode,
                })
                try:
                    save_sim_prefs(sim_state)
                except Exception:
                    pass

                df_proj = sim_project_portfolio(
                    initial_capital=initial_capital,
                    monthly_contribution=monthly_contrib,
                    annual_return_pct=annual_return,
                    years=years_total,
                    lump_sum=lump_sum,
                    lump_sum_month=lump_month,
                )

                final = df_proj.iloc[-1]
                final_with = float(final["balance_with_lump"])
                final_without = float(final["balance_no_lump"])
                total_contributed = float(final["contributions_cum"])
                compound_gains = max(0.0, final_with - total_contributed - lump_sum)
                monthly_pension = sim_safe_withdrawal_monthly(final_with, swr)
                annual_pension = monthly_pension * 12.0

                infl_factor = (1.0 + infl / 100.0) ** years_total if infl > 0 else 1.0
                real_final = final_with / infl_factor
                real_pension = monthly_pension / infl_factor
                real_gains = compound_gains / infl_factor

                # ── KPI row
                def _kpi_with_subtitle(label: str, value: str, subtitle: str, tooltip: str, positive: bool | None = None) -> ft.Container:
                    delta_color = ft.Colors.GREEN_400 if positive is True else (ft.Colors.RED_400 if positive is False else _text_secondary())
                    return ft.Container(
                        content=ft.Column([
                            ft.Row([
                                ft.Text(label, size=11, color=_text_secondary(), weight=ft.FontWeight.W_500),
                                ft.Container(content=ft.Icon(ft.Icons.INFO_OUTLINED, size=14, color=_text_secondary()), tooltip=tooltip),
                            ], spacing=6),
                            ft.Text(value, size=22, weight=ft.FontWeight.BOLD, color=_text_primary()),
                            ft.Text(subtitle, size=11, color=delta_color),
                        ], tight=True, spacing=4),
                        bgcolor=_card_bg(), border=ft.border.all(1, _card_border()), border_radius=14,
                        padding=ft.padding.symmetric(horizontal=18, vertical=14), expand=True,
                        shadow=ft.BoxShadow(blur_radius=12, color=ft.Colors.with_opacity(0.07, ft.Colors.BLACK), offset=ft.Offset(0, 3)),
                    )

                sub_final = (f"≈ ₪{real_final:,.0f} " + tr("in today's ₪", "בכוח קנייה של היום")) if infl > 0 else ""
                sub_gains = (f"≈ ₪{real_gains:,.0f} " + tr("real", "ריאלי")) if infl > 0 else ""
                sub_pension = (
                    f"≈ ₪{real_pension:,.0f} " + tr("in today's ₪", "בכוח קנייה של היום")
                    if infl > 0 else f"₪{annual_pension:,.0f} / {tr('year', 'שנה')}"
                )
                kpi_row.controls = [
                    ft.Container(col={"xs": 6, "md": 3}, content=_kpi_with_subtitle(
                        tr("Final value", "שווי סופי"),
                        f"₪{final_with:,.0f}",
                        sub_final,
                        tr("Nominal projected balance at target age (with lump). Subtitle shows REAL (today's ₪) after deflating by inflation.",
                           "יתרה נומינלית חזויה בגיל היעד (כולל חד-פעמית). הכותרת מציגה את השווי הריאלי בכוח הקנייה של היום."),
                        positive=final_with > total_contributed + lump_sum,
                    )),
                    ft.Container(col={"xs": 6, "md": 3}, content=_kpi_with_subtitle(
                        tr("Total contributed", "סך תרומות"),
                        f"₪{(total_contributed + lump_sum):,.0f}", "",
                        tr("Initial capital + monthly contributions + lump sum.",
                           "הון התחלתי + הפקדות חודשיות + הפקדה חד-פעמית."),
                    )),
                    ft.Container(col={"xs": 6, "md": 3}, content=_kpi_with_subtitle(
                        tr("Compound gains", "רווחי ריבית דריבית"),
                        f"₪{compound_gains:,.0f}",
                        sub_gains,
                        tr("Growth beyond money you put in — 'interest on interest'. Real delta discounts inflation over horizon.",
                           "צמיחה מעבר לסכום שהפקדת — 'ריבית על ריבית'. הדלתא הריאלית מבטלת את השפעת האינפלציה."),
                        positive=compound_gains > 0,
                    )),
                    ft.Container(col={"xs": 6, "md": 3}, content=_kpi_with_subtitle(
                        tr("Monthly pension (SWR)", "פנסיה חודשית (SWR)"),
                        f"₪{monthly_pension:,.0f}",
                        sub_pension,
                        tr("Trinity-study sustainable monthly withdrawal at chosen SWR. Real equivalent shown in today's ₪ when inflation > 0.",
                           "משיכה חודשית ברת-קיימא לפי שיעור המשיכה שנבחר. שווי ריאלי מוצג בכוח קנייה של היום כשהאינפלציה > 0."),
                        positive=True,
                    )),
                ]

                # Inflation context caption
                if infl > 0:
                    real_pct_loss = (1.0 - 1.0 / infl_factor) * 100.0
                    infl_caption.value = tr(
                        f"💡 With {infl:.2f}% annual inflation over {years_total:.1f} years, nominal value loses ≈ {real_pct_loss:.1f}% of purchasing power.",
                        f"💡 באינפלציה של {infl:.2f}% לשנה לאורך {years_total:.1f} שנים, השווי הנומינלי מאבד כ-{real_pct_loss:.1f}% מכוח הקנייה.",
                    )
                else:
                    infl_caption.value = ""

                # ── Chart: contributions baseline / no-lump / with-lump
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=df_proj["year"], y=df_proj["contributions_cum"], mode="lines",
                    name=tr("Contributions (baseline)", "תרומות (קו בסיס)"),
                    line=dict(color="#94a3b8", width=1.4, dash="dot"),
                ))
                fig.add_trace(go.Scatter(
                    x=df_proj["year"], y=df_proj["balance_no_lump"], mode="lines",
                    name=tr("Balance (no lump)", "יתרה ללא חד-פעמית"),
                    line=dict(color="#06b6d4", width=1.8, dash="dash"),
                ))
                fig.add_trace(go.Scatter(
                    x=df_proj["year"], y=df_proj["balance_with_lump"], mode="lines",
                    name=tr("Balance (with lump)", "יתרה עם חד-פעמית"),
                    line=dict(color="#6366f1", width=2.4),
                    fill="tozeroy", fillcolor="rgba(99,102,241,0.10)",
                ))
                if lump_sum > 0:
                    fig.add_vline(
                        x=float(lump_month) / 12.0,
                        line=dict(color="#f59e0b", width=1.4, dash="dashdot"),
                        annotation_text=tr("Lump-sum", "הפקדה"),
                        annotation_position="top",
                    )
                fig.update_layout(
                    template=_chart_template(),
                    title=tr("Long-term Projection", "הדמיה ארוכת-טווח"),
                    xaxis_title=tr("Years", "שנים"),
                    yaxis_title=tr("Value (₪)", "שווי (₪)"),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=10, r=10, t=56, b=30),
                    hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                    height=420,
                )
                chart_holder.controls = [render_plotly_or_fallback(fig, tr("Long-term Projection", "הדמיה ארוכת-טווח"))]

                # ── Delay-cost insight
                if lump_sum > 0 and lump_month > 0:
                    r_m = (1.0 + annual_return / 100.0) ** (1.0 / 12.0) - 1.0
                    pure_uplift = lump_sum * ((1.0 + r_m) ** months_total - 1.0)
                    delay_cost = pure_uplift * (1.0 - (1.0 + r_m) ** (-int(lump_month)))
                    if delay_cost > 0:
                        delay_caption.value = tr(
                            f"⏳ Delaying the lump-sum by {int(lump_month)} months costs roughly ₪{delay_cost:,.0f} in forgone compounding.",
                            f"⏳ דחיית ההפקדה החד-פעמית ב-{int(lump_month)} חודשים מאבדת כ-₪{delay_cost:,.0f} בריבית דריבית.",
                        )
                    else:
                        delay_caption.value = ""
                else:
                    delay_caption.value = ""

                # ── Milestones (×2, ×5, ×10 capital)
                base = max(initial_capital, 1.0)
                milestone_cards = []
                for mult in (2, 5, 10):
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
                        text = f"{yrs:.1f} " + tr("years", "שנים")
                        tip = tr(f"Years until balance reaches ₪{target:,.0f}.", f"שנים עד שהיתרה תגיע ל-₪{target:,.0f}.")
                    else:
                        text = tr(">80y", ">80ש'")
                        tip = tr(f"Target ₪{target:,.0f} not reached within 80 years.", f"היעד ₪{target:,.0f} לא מושג תוך 80 שנה.")
                    milestone_cards.append(ft.Container(
                        col={"xs": 12, "md": 4},
                        content=ft.Container(
                            content=ft.Column([
                                ft.Row([
                                    ft.Text(f"×{mult} {tr('capital', 'הון')}", size=11, color=_text_secondary()),
                                    ft.Container(content=ft.Icon(ft.Icons.INFO_OUTLINED, size=14, color=_text_secondary()), tooltip=tip),
                                ], spacing=6),
                                ft.Text(text, size=20, weight=ft.FontWeight.BOLD, color=_text_primary()),
                            ], tight=True, spacing=4),
                            bgcolor=_card_bg(), border=ft.border.all(1, _card_border()),
                            border_radius=14, padding=ft.padding.symmetric(horizontal=18, vertical=14),
                        ),
                    ))
                milestones_row.controls = milestone_cards

                # ── Yearly breakdown table
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
                breakdown_holder.controls = [
                    ft.Container(
                        content=ft.Column([df_to_table(yearly, numeric_cols=list(yearly.columns)[1:])], scroll=ft.ScrollMode.AUTO),
                        border=ft.border.all(1, _card_border()), border_radius=12, bgcolor=_card_bg(), padding=8,
                    ),
                ]

                page.update()
            except Exception as exc:  # never crash the UI
                _set_status(f"Simulator error: {exc}", ft.Colors.RED_600)

        # ── Build inputs
        mode_options = [ft.Radio(value=mode_clean, label=mode_clean)]
        if has_portfolio:
            mode_options.insert(0, ft.Radio(value=mode_mine, label=mode_mine))
        mode_group = ft.RadioGroup(
            ref=mode_radio,
            value=current_mode,
            content=ft.Row(mode_options, wrap=True),
            on_change=recompute,
        )

        col_left = ft.Column([
            ft.TextField(
                ref=age_now_field, label=tr("Your current age", "הגיל הנוכחי שלך"),
                value=str(int(_safe_num(_sim_get("sim_age_now", 30), 30))),
                tooltip=tr("Starting age — used only to compute the horizon.", "גיל התחלה — משמש רק לחישוב משך ההשקעה."),
                keyboard_type=ft.KeyboardType.NUMBER, on_change=recompute, width=260,
            ),
            ft.TextField(
                ref=age_target_field, label=tr("Target retirement age", "גיל פרישה מתוכנן"),
                value=str(int(_safe_num(_sim_get("sim_age_target", 67), 67))),
                tooltip=tr("Projection ends at this age.", "ההדמיה מסתיימת בגיל זה."),
                keyboard_type=ft.KeyboardType.NUMBER, on_change=recompute, width=260,
            ),
            ft.TextField(
                ref=initial_field, label=tr("Initial capital (₪)", "הון התחלתי (₪)"),
                value=str(round(tv_total, 2) if (current_mode == mode_mine and has_portfolio) else _safe_num(_sim_get("sim_initial_clean", 0.0), 0.0)),
                tooltip=tr("Starting amount at month 0 (locked when 'My Portfolio' mode).",
                           "הסכום בנקודת הזמן 0 (נעול במצב 'התיק שלי')."),
                keyboard_type=ft.KeyboardType.NUMBER, on_change=recompute, width=260,
                disabled=(current_mode == mode_mine and has_portfolio),
                prefix=ft.Text("₪"),
            ),
        ], spacing=10)

        col_right = ft.Column([
            ft.TextField(
                ref=annual_return_field, label=tr("Expected annual return (%)", "תשואה שנתית צפויה (%)"),
                value=str(_safe_num(_sim_get("sim_annual_return", 7.0), 7.0)),
                tooltip=tr("S&P 500 long-run average ≈ 7% real / 10% nominal.",
                           "ממוצע רב-שנתי של S&P 500: ≈7% ריאלי / 10% נומינלי."),
                keyboard_type=ft.KeyboardType.NUMBER, on_change=recompute, width=260,
                suffix=ft.Text("%"),
            ),
            ft.TextField(
                ref=monthly_field, label=tr("Monthly contribution (₪)", "הפקדה חודשית (₪)"),
                value=str(_safe_num(_sim_get("sim_monthly_contrib", 2000.0), 2000.0)),
                tooltip=tr("Amount added at the end of every month.", "סכום שמתווסף בסוף כל חודש."),
                keyboard_type=ft.KeyboardType.NUMBER, on_change=recompute, width=260,
                prefix=ft.Text("₪"),
            ),
            ft.TextField(
                ref=lump_field, label=tr("One-time lump sum (₪)", "הפקדה חד-פעמית (₪)"),
                value=str(_safe_num(_sim_get("sim_lump_sum", 0.0), 0.0)),
                tooltip=tr("A single extra deposit (e.g. bonus, inheritance).",
                           "הפקדה נוספת חד-פעמית (למשל בונוס, ירושה)."),
                keyboard_type=ft.KeyboardType.NUMBER, on_change=recompute, width=260,
                prefix=ft.Text("₪"),
            ),
        ], spacing=10)

        # SWR + inflation + lump-month sliders
        years_default = max(1, int(_safe_num(_sim_get("sim_age_target", 67), 67)) - int(_safe_num(_sim_get("sim_age_now", 30), 30)))
        months_default = max(1, years_default * 12)
        controls_row2 = ft.Row([
            ft.Column([
                ft.Text(tr("Lump-sum timing (months from today)", "תזמון ההפקדה החד-פעמית (חודשים מהיום)"), size=12, color=_text_secondary()),
                ft.Slider(
                    ref=lump_month_field, min=0, max=months_default,
                    value=min(int(_safe_num(_sim_get("sim_lump_month", 0), 0)), months_default),
                    divisions=max(1, months_default), label="{value}", on_change=recompute,
                ),
            ], expand=True),
        ])
        controls_row3 = ft.Row([
            ft.Column([
                ft.Text(tr("Safe Withdrawal Rate (SWR %)", "שיעור משיכה בטוח (SWR %)"), size=12, color=_text_secondary()),
                ft.Slider(
                    ref=swr_field, min=3.0, max=5.0, value=_safe_num(_sim_get("sim_swr", 4.0), 4.0),
                    divisions=8, label="{value}%", on_change=recompute,
                ),
            ], expand=True),
            ft.TextField(
                ref=infl_field, label=tr("Annual inflation (%)", "אינפלציה שנתית (%)"),
                value=str(_safe_num(_sim_get("sim_annual_inflation", 3.0), 3.0)),
                tooltip=tr(
                    "Discounts future nominal values back into TODAY's purchasing power. Israel CPI ≈ 2-3% · USA CPI ≈ 3%. Set 0 for nominal projection.",
                    "היוון של שוויים עתידיים נומינליים לכוח הקנייה של היום. אינפלציה ישראלית ≈ 2-3% · אמריקאית ≈ 3%. הזן 0 להדמיה נומינלית.",
                ),
                keyboard_type=ft.KeyboardType.NUMBER, on_change=recompute, width=200,
                suffix=ft.Text("%"),
            ),
        ])

        params_box = ft.Container(
            content=ft.Column([
                ft.Text(tr("Parameters", "פרמטרים"), weight=ft.FontWeight.W_700, color=_text_primary()),
                mode_group,
                ft.ResponsiveRow([
                    ft.Container(col={"xs": 12, "md": 6}, content=col_left),
                    ft.Container(col={"xs": 12, "md": 6}, content=col_right),
                ], spacing=12, run_spacing=12),
                controls_row2,
                controls_row3,
            ], spacing=10),
            bgcolor=_card_bg(), border=ft.border.all(1, _card_border()),
            border_radius=12, padding=14,
        )

        # Initial paint
        recompute()

        return ft.Column([
            ft.Text(tr("🧮 Simulator", "🧮 סימולטור"), size=22, weight=ft.FontWeight.BOLD, color=_text_primary()),
            ft.Text(
                tr(
                    "Long-term growth projection with monthly compounding, optional lump-sum, inflation adjustment and a sustainable monthly pension. Inputs are auto-saved.",
                    "הדמיית צמיחה ארוכת-טווח עם ריבית דריבית חודשית, תרומה חד-פעמית אפשרית, התאמה לאינפלציה ופנסיה חודשית ברת-קיימא. הקלטים נשמרים אוטומטית.",
                ),
                size=12, color=_text_secondary(),
            ),
            params_box,
            kpi_row,
            infl_caption,
            ft.Container(
                content=chart_holder,
                border=ft.border.all(1, _card_border()), border_radius=12, bgcolor=_card_bg(), padding=8,
            ),
            delay_caption,
            milestones_row,
            ft.Text(tr("📋 Yearly breakdown", "📋 פירוט שנתי"), size=14, weight=ft.FontWeight.W_600, color=_text_primary()),
            breakdown_holder,
            ft.Text(
                tr("⚠ Projections are educational estimates only — not investment advice. Actual returns vary.",
                   "⚠ ההדמיה היא לצורכי הדגמה בלבד — אינה מהווה ייעוץ השקעות. תשואות בפועל עשויות להשתנות."),
                size=11, color=_text_secondary(),
            ),
        ], spacing=14, scroll=ft.ScrollMode.AUTO, expand=True)

    # ── Navigation ─────────────────────────────────────────────────────
    NAV_ITEMS = [
        ("dashboard",     tr("Dashboard", "דשבורד"),         ft.Icons.HOME_ROUNDED,        dashboard_view),
        ("allocation",    tr("Allocation", "חלוקה"),          ft.Icons.PIE_CHART_ROUNDED,   allocation_view),
        ("simulator",     tr("Simulator", "סימולטור"),        ft.Icons.CALCULATE_ROUNDED,   simulator_view),
        ("reports",       tr("Reports", "דוחות"),             ft.Icons.BAR_CHART_ROUNDED,   reports_view),
        ("deposits",      tr("Deposits", "הפקדות"),           ft.Icons.SAVINGS_ROUNDED,     deposits_view),
        ("transactions",  tr("Transactions", "עסקאות"),       ft.Icons.RECEIPT_LONG_ROUNDED, transactions_view),
        ("manage",        tr("Trade Manage", "ניהול עסקאות"), ft.Icons.SWAP_HORIZ_ROUNDED,  manage_view),
        ("risk",          tr("Risk / FIFO", "סיכונים"),       ft.Icons.SHIELD_ROUNDED,      risk_view),
        ("quality",       tr("Data Quality", "בקרת נתונים"), ft.Icons.VERIFIED_ROUNDED,    quality_view),
    ]

    selected_nav = [0]

    rail = ft.NavigationRail(
        selected_index=0,
        label_type=ft.NavigationRailLabelType.ALL,
        min_width=80,
        min_extended_width=180,
        group_alignment=-0.95,
        destinations=[
            ft.NavigationRailDestination(
                icon=item[2],
                selected_icon=item[2],
                label=item[1],
            )
            for item in NAV_ITEMS
        ],
        bgcolor=_rail_bg(),
    )

    def _calc_rail_height() -> float:
        # NavigationRail must receive bounded height in this Flet build.
        base_h = float(getattr(page, "height", 0) or 0)
        if base_h <= 0:
            base_h = 760.0
        # Reserve approximate header + status bar area.
        return max(base_h - 120.0, 520.0)

    rail_container = ft.Container(
        content=rail,
        width=188,
        height=_calc_rail_height(),
        bgcolor=_rail_bg(),
        alignment=ft.Alignment.TOP_LEFT,
    )

    def on_rail_change(e):
        selected_nav[0] = e.control.selected_index
        render_view()

    rail.on_change = on_rail_change

    def render_view():
        idx = selected_nav[0]
        _, _, _, builder = NAV_ITEMS[idx]
        page.bgcolor = _page_bg()
        rail.bgcolor = _rail_bg()
        content_area.content = builder()
        content_area.bgcolor = _page_bg()
        page.update()

    # ── Header ────────────────────────────────────────────────────────
    def _theme_icon() -> str:
        return ft.Icons.LIGHT_MODE_ROUNDED if _is_dark() else ft.Icons.DARK_MODE_ROUNDED

    theme_btn = ft.IconButton(
        icon=ft.Icons.DARK_MODE_ROUNDED,
        tooltip=tr("Toggle dark/light mode", "מצב כהה/בהיר"),
        on_click=lambda _: [toggle_theme(_), setattr(theme_btn, "icon", _theme_icon()), page.update()],
    )

    header = ft.Container(
        content=ft.Row([
            ft.Row([
                ft.Icon(ft.Icons.SHOW_CHART_ROUNDED, color=BRAND, size=26),
                ft.Text(
                    tr("Portfolio Manager OS", "מנהל תיק השקעות"),
                    size=20, weight=ft.FontWeight.BOLD, color=_text_primary(),
                ),
            ], spacing=8),
            ft.Row([
                loading_ring,
                source_chip,
                ft.FilledTonalButton(
                    tr("Refresh", "רענן"), icon=ft.Icons.REFRESH_ROUNDED,
                    on_click=refresh_data,
                ),
                theme_btn,
            ], spacing=8),
        ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
        padding=ft.padding.symmetric(horizontal=20, vertical=12),
        bgcolor=_rail_bg(),
        border=ft.border.only(bottom=ft.border.BorderSide(1, _card_border())),
    )

    status_bar_container = ft.Container(
        content=ft.Row([status_bar], alignment=ft.MainAxisAlignment.START),
        padding=ft.padding.symmetric(horizontal=20, vertical=6),
        bgcolor=_rail_bg(),
        border=ft.border.only(top=ft.border.BorderSide(1, _card_border())),
    )

    # ── Layout ────────────────────────────────────────────────────────
    main_layout = ft.Row(
        [
            rail_container,
            ft.VerticalDivider(width=1, color=_card_border()),
            content_area,
        ],
        expand=True,
        spacing=0,
    )

    def _on_resize(_):
        rail_container.height = _calc_rail_height()
        page.update()

    page.on_resize = _on_resize

    page.add(
        ft.Column(
            [
                header,
                main_layout,
                status_bar_container,
            ],
            spacing=0,
            expand=True,
        )
    )

    page.bgcolor = _page_bg()
    render_view()


if __name__ == "__main__":
    ft.app(target=main, assets_dir=None)
