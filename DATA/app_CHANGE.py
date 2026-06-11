# הגדרת הטאבים החדשה
        tab_overview_real, tab_allocation_real, tab_risk_real, tab_tx_real = st.tabs(
            [
                tr("Overview", "סקירה כללית"),
                tr("Portfolio Allocation", "הרכב התיק"),
                tr("Risk & Analytics", "אנליזה וסיכונים"),
                tr("Transactions & Cash Flow", "תנועות ועסקאות"),
            ]
        )

        # יצירת סלוטים (Containers) כדי לשלוט בסדר ההזרקה של התוכן
        with tab_overview_real:
            _ov_equity_slot = st.container()   # עקומת ההון בראש הטאב
            _ov_body_slot = st.container()     # גרפי פאי/באר תחתיה

        with tab_allocation_real:
            _alloc_charts_slot = st.container()    # גרפי התפלגות
            _alloc_classes_slot = st.container()   # חלוקה לפי סוגי נכסים
            _alloc_exposure_slot = st.container()  # טבלת חשיפה בתחתית הטאב

        with tab_tx_real:
            _tx_deposits_slot = st.container()     # הפקדות ידניות
            _tx_transactions_slot = st.container() # טבלת עסקאות

        # מיפוי מחדש של המשתנים הישנים לסלוטים החדשים כדי למנוע שכתוב של הלוגיקה
        tab_overview = _ov_body_slot
        tab_allocation = _alloc_classes_slot
        tab_reports = tab_risk_real
        tab_deposits = _tx_deposits_slot
        tab_transactions = _tx_transactions_slot

        # --- הזרקת התוכן לעקומת ההון (Overview Top) ---
        with _ov_equity_slot:
            if can_show_build_up:
                st.markdown(f"#### {tr('Portfolio Build-Up', 'התפתחות בניית התיק')}")
                perf_track = open_trades.groupby("Purchase_Date", as_index=False)[["Cost_ILS", "Current_Value_ILS"]].sum().sort_values("Purchase_Date")
                perf_track["Cum_Cost_ILS"] = perf_track["Cost_ILS"].cumsum()
                perf_track["Cum_Value_ILS"] = perf_track["Current_Value_ILS"].cumsum()
                fig_track = go.Figure()
                fig_track.add_trace(go.Scatter(x=perf_track["Purchase_Date"], y=perf_track["Cum_Cost_ILS"], mode="lines+markers", name=tr("Cumulative Cost", "עלות מצטברת")))
                fig_track.add_trace(go.Scatter(x=perf_track["Purchase_Date"], y=perf_track["Cum_Value_ILS"], mode="lines+markers", name=tr("Cumulative Value", "שווי מצטבר"), fill="tonexty", fillcolor="rgba(79,70,229,0.08)"))
                st.plotly_chart(_apply_plotly_theme(fig_track, is_dark, is_mobile), use_container_width=True)

        # --- הזרקת התוכן לטבלת החשיפה (Allocation Bottom) ---
        # הלוגיקה בתוך render_exposure_section תשתמש כעת ב-_alloc_exposure_slot