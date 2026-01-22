• Uses realized intraday PnL  
• Normalized by absolute exposure  
• Robust even when price / market value data is missing  
"""
    )

    sector_agg = (
        intraday
        .groupby(["snapshot_ts", "time_label", "egm_sector_v2"])
        .agg(
            sector_pnl=("daily_pnl", "sum"),
            sector_gross=("gross_notional", lambda x: x.abs().sum()),
        )
        .reset_index()
        .sort_values("snapshot_ts")
    )

    sector_agg["sector_return_pct"] = 0.0
    mask = sector_agg["sector_gross"] != 0

    sector_agg.loc[mask, "sector_return_pct"] = (
        sector_agg.loc[mask, "sector_pnl"]
        / sector_agg.loc[mask, "sector_gross"]
        * 100
    )

    sector_agg["move_bucket"] = sector_agg["sector_return_pct"].apply(classify_move)

    pivot = (
        sector_agg
        .pivot(
            index="egm_sector_v2",
            columns="time_label",
            values="move_bucket",
        )
        .sort_index()
    )

    st.dataframe(pivot, width="stretch")

# =================================================
# TAB 2 — PRICE CHANGE DRIVEN
# =================================================
with tab_price:
    st.header("📈 Price Change–Driven Analysis")

    bucket_table = (
        intraday
        .groupby(["time_label", "move_bucket"])
        .agg(names=("ticker", "nunique"))
        .reset_index()
        .pivot(index="move_bucket", columns="time_label", values="names")
        .reindex(BUCKET_ORDER)
        .fillna(0)
        .astype(int)
    )

    st.caption(
        "Counts show number of names in each price-move bucket "
        "at each 30-minute snapshot (CST)."
    )

    st.dataframe(bucket_table, width="stretch")

    selected_bucket = st.selectbox(
        "Select Price-Move Bucket",
        BUCKET_ORDER,
    )

    bucket_df = latest[latest["move_bucket"] == selected_bucket].copy()

    st.subheader(f"🏭 Sector Breakdown – {selected_bucket}")

    sector_view = (
        bucket_df
        .groupby("egm_sector_v2")
        .agg(
            names=("ticker", "nunique"),
            net_nmv=("nmv", "sum"),
            avg_move=("price_change_pct", "mean"),
        )
        .reset_index()
        .sort_values("net_nmv", ascending=False)
    )

    st.dataframe(sector_view, width="stretch")

    selected_sector = st.selectbox(
        "Select Sector",
        sector_view["egm_sector_v2"].dropna().unique()
    )

    sector_df = bucket_df[bucket_df["egm_sector_v2"] == selected_sector].copy()

    if selected_sector == "Comm/Tech":
        st.subheader("🧩 Comm/Tech – Cohort Breakdown")

        cohorts = load_commtech_cohorts()
        ct_df = sector_df.merge(cohorts, on="ticker", how="inner")

        if not ct_df.empty:
            cohort_view = (
                ct_df
                .groupby("cohort_name")
                .agg(
                    names=("ticker", "nunique"),
                    net_nmv=("nmv", "sum"),
                    avg_move=("price_change_pct", "mean"),
                )
                .reset_index()
                .sort_values("net_nmv", ascending=False)
            )

            st.dataframe(cohort_view, width="stretch")

            selected_cohort = st.selectbox(
                "Select Cohort",
                cohort_view["cohort_name"].unique()
            )

            sector_df = ct_df[ct_df["cohort_name"] == selected_cohort].copy()

    st.subheader("📋 Instrument Detail (Latest Snapshot)")

    cols = [
        "ticker",
        "description",
        "egm_sector_v2",
        "quantity",
        "price_change_pct",
        "nmv",
    ]

    if "weight_pct" in sector_df.columns:
        cols += ["weight_pct", "is_primary"]

    st.dataframe(
        sector_df[cols].sort_values("price_change_pct"),
        width="stretch",
    )