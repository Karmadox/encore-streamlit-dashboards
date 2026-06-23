import streamlit as st
import pandas as pd
import psycopg2
from datetime import date

# -------------------------------------------------
# SIMPLE PASSWORD AUTH
# -------------------------------------------------

def check_password():
    def password_entered():
        if st.session_state["password"] == st.secrets["auth"]["password"]:
            st.session_state["authenticated"] = True
        else:
            st.session_state["authenticated"] = False

    if "authenticated" not in st.session_state:
        st.text_input("Enter Password", type="password", key="password")
        st.button("Login", on_click=password_entered)
        return False
    elif not st.session_state["authenticated"]:
        st.text_input("Enter Password", type="password", key="password")
        st.button("Login", on_click=password_entered)
        st.error("Incorrect password")
        return False
    return True

if not check_password():
    st.stop()

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------

st.set_page_config(page_title="Nasdaq-100 Market State", layout="wide")
DB_CONFIG = st.secrets["db"]

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

# --------------------------------------------------
# DATA LOADERS
# --------------------------------------------------

@st.cache_data(ttl=300)
def load_latest_snapshot_date():
    sql = "SELECT MAX(snapshot_date) FROM encoredb.ndx_market_snapshot"
    with get_conn() as conn:
        return pd.read_sql(sql, conn).iloc[0, 0]

@st.cache_data(ttl=300)
def load_market_state(snapshot_date):
    sql = """
        SELECT *
        FROM encoredb.v_index_canonical_market_state_enriched
        WHERE index_name = 'NASDAQ100'
        AND snapshot_date = %s
        ORDER BY index_rank
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(snapshot_date,))
        
@st.cache_data(ttl=60)
def load_positions():
    sql = """
        SELECT ticker, SUM(quantity) AS quantity
        FROM encoredb.positions_snapshot_latest
        GROUP BY ticker
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

@st.cache_data(ttl=60)
def load_nq_index_level():
    sql = """
        SELECT close
        FROM encoredb.marketdata_intraday
        WHERE security = 'NQ1 Index'
        ORDER BY timestamp DESC
        LIMIT 1
    """
    with get_conn() as conn:
        df = pd.read_sql(sql, conn)
        return None if df.empty else df["close"].iloc[0]

@st.cache_data(ttl=300)
def load_official_ndx_ytd():

    sql = """

        SELECT

            pct_ytd

        FROM encoredb.index_performance_snapshot

        WHERE security = 'NDX Index'

        ORDER BY snapshot_date DESC

        LIMIT 1

    """

    with get_conn() as conn:

        df = pd.read_sql(sql, conn)

    return (

        df["pct_ytd"].iloc[0]

        if not df.empty

        else None

    )

@st.cache_data(ttl=300)
def load_chain_linked_return():

    sql = """
    WITH price_returns AS (

        SELECT
            p.trade_date,
            i.ticker,

            LN(
                p.close_price
                /
                LAG(p.close_price) OVER (
                    PARTITION BY p.instrument_id
                    ORDER BY p.trade_date
                )
            ) AS log_return

        FROM encoredb.equity_daily_prices p

        JOIN encoredb.instruments i
          ON p.instrument_id = i.instrument_id

        WHERE p.trade_date >= DATE '2026-01-30'

    ),

    daily_index_return AS (

        SELECT
            r.trade_date,

            SUM(
                (w.ndx_weight_pct / 100.0)
                * r.log_return
            ) AS index_log_return

        FROM price_returns r

        JOIN encoredb.ndx_weights_snapshot w
          ON w.ticker = r.ticker
         AND w.snapshot_date = r.trade_date

        WHERE r.log_return IS NOT NULL

        GROUP BY r.trade_date

    )

    SELECT
        SUM(index_log_return) AS total_log_return
    FROM daily_index_return
    """

    with get_conn() as conn:
        df = pd.read_sql(sql, conn)

    if df.empty:
        return None

    total_log_return = df.iloc[0]["total_log_return"]

    import numpy as np

    return (
        np.exp(total_log_return) - 1
    ) * 100

@st.cache_data(ttl=300)
def load_historical_attribution():

    sql = """
    WITH price_returns AS (

        SELECT
            p.trade_date,
            i.ticker,

            LN(
                p.close_price
                /
                LAG(p.close_price) OVER (
                    PARTITION BY p.instrument_id
                    ORDER BY p.trade_date
                )
            ) AS log_return

        FROM encoredb.equity_daily_prices p

        JOIN encoredb.instruments i
          ON p.instrument_id = i.instrument_id

        WHERE p.trade_date >= DATE '2026-01-30'

    ),

    weighted_returns AS (

        SELECT

            CASE
                WHEN m.cohort_name = 'Semiconductors'
                    THEN 'Semiconductors'
                ELSE 'Non-Semiconductors'
            END AS grp,

            (w.ndx_weight_pct / 100.0)
            * r.log_return AS weighted_log_return

        FROM price_returns r

        JOIN encoredb.ndx_weights_snapshot w
          ON w.ticker = r.ticker
         AND w.snapshot_date = r.trade_date

        JOIN (
        
            SELECT DISTINCT
                ticker,
                cohort_name
        
            FROM encoredb.v_index_canonical_market_state_enriched
        
        ) m
        
          ON m.ticker = r.ticker

        WHERE r.log_return IS NOT NULL

    )

    SELECT
        grp,
        SUM(weighted_log_return) AS total_log_return

    FROM weighted_returns

    GROUP BY grp
    """

    with get_conn() as conn:
        return pd.read_sql(sql, conn)

@st.cache_data(ttl=300)
def load_latest_analyst_revisions():

    sql = """
    WITH latest_date AS (
        SELECT MAX(snapshot_date) AS snapshot_date
        FROM encoredb.ndx_analyst_revisions
    )

    SELECT
        r.snapshot_date,
        i.ticker,
        i.name,
        r.analyst_count,
        r.up_1m,
        r.dn_1m,
        r.up_3m,
        r.dn_3m,
        r.target_now,
        r.target_1m_ago,
        r.target_3m_ago,
        r.target_delta_1m_pct,
        r.target_delta_3m_pct,
        r.revision_breadth_1m,
        r.revision_breadth_3m
    FROM encoredb.ndx_analyst_revisions r
    JOIN latest_date d
      ON r.snapshot_date = d.snapshot_date
    JOIN encoredb.instruments i
      ON i.instrument_id = r.instrument_id
    ORDER BY i.ticker;
    """

    with get_conn() as conn:
        return pd.read_sql(sql, conn)

@st.cache_data(ttl=300)
def load_latest_market_snapshot():

    sql = """
    WITH latest_date AS (
        SELECT MAX(snapshot_date) AS snapshot_date
        FROM encoredb.ndx_market_snapshot
    )

    SELECT
        m.snapshot_date,
        i.ticker,
        i.name,
        m.index_rank,
        m.index_weight_pct,
        m.last_price,
        m.pct_change_1d,
        m.pct_change_5d,
        m.pct_change_1m,
        m.pct_change_ytd,
        m.pct_to_best_target,
        m.analyst_count,
        m.best_analyst_rating,
        m.best_eps_3mo_pct_chg,
        m.best_eps_yoy_gth,
        m.eps_up_1m,
        m.eps_dn_1m,
        m.eps_up_3m,
        m.eps_dn_3m,
        m.days_to_earnings
    FROM encoredb.ndx_market_snapshot m
    JOIN latest_date d
      ON m.snapshot_date = d.snapshot_date
    JOIN encoredb.instruments i
      ON i.instrument_id = m.instrument_id
    ORDER BY m.index_rank;
    """

    with get_conn() as conn:
        return pd.read_sql(sql, conn)
        
# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------

snapshot_date = load_latest_snapshot_date()
df = load_market_state(snapshot_date)
positions = load_positions()
nq_index_level = load_nq_index_level()
official_ndx_ytd = load_official_ndx_ytd()
chain_linked_ndx = load_chain_linked_return()
historical_attr = load_historical_attribution()

# --------------------------------------------------
# MERGE REAL POSITIONS
# --------------------------------------------------

df = df.merge(positions, on="ticker", how="left")
df["quantity"] = df["quantity"].fillna(0)
df["real_value"] = df["quantity"] * df["last_price"]

# --------------------------------------------------
# SYNTHETIC FUTURES OVERLAY
# --------------------------------------------------

NQ_MULTIPLIER = 20
synthetic_index_notional = 0

nq_row = positions[positions["ticker"].str.startswith("NQ", na=False)]

net_contracts = 0
synthetic_summary_text = "No NQ futures position"

if not nq_row.empty and nq_index_level is not None:

    net_contracts = nq_row["quantity"].sum()

    # Build contract text summary
    contract_lines = []
    for _, row in nq_row.iterrows():
        contract_lines.append(f"{int(row['quantity'])} {row['ticker']}")

    synthetic_summary_text = " / ".join(contract_lines)

    synthetic_index_notional = net_contracts * nq_index_level * NQ_MULTIPLIER

df["weight_decimal"] = df["index_weight_pct"] / 100
df["synthetic_value"] = df["weight_decimal"] * synthetic_index_notional
df["synthetic_quantity"] = df["synthetic_value"] / df["last_price"]

df["synthetic_value"] = df["synthetic_value"].fillna(0)
df["synthetic_quantity"] = df["synthetic_quantity"].fillna(0)
df["net_position_value"] = df["real_value"] + df["synthetic_value"]

# --------------------------------------------------
# PORTFOLIO TRP TENSION (GROSS-WEIGHTED, DIRECTION-AWARE)
# --------------------------------------------------

# --------------------------------------------------
# EQUITY-ONLY TRP (STOCK BOOK)
# --------------------------------------------------

equity_df = df[df["real_value"] != 0].copy()
equity_df = equity_df[equity_df["pct_to_best_target"].notna()]

equity_gross_exposure = equity_df["real_value"].abs().sum()

if equity_gross_exposure > 0:
    equity_trp = (
        (equity_df["real_value"] * equity_df["pct_to_best_target"]).sum()
        / equity_gross_exposure
    )
else:
    equity_trp = 0

trp_df = df.copy()

# Only include actual exposure
trp_df = trp_df[trp_df["net_position_value"] != 0]

# Must have analyst target data
trp_df = trp_df[trp_df["pct_to_best_target"].notna()]

gross_exposure = trp_df["net_position_value"].abs().sum()

if gross_exposure > 0:
    portfolio_trp = (
        (trp_df["net_position_value"] * trp_df["pct_to_best_target"])
        .sum()
        / gross_exposure
    )
else:
    portfolio_trp = 0

# --------------------------------------------------
# HEDGE IMPACT
# --------------------------------------------------

hedge_impact = equity_trp - portfolio_trp

# --------------------------------------------------

# TABS

# --------------------------------------------------

tabs = st.tabs([

    "📈 Market State",

    "📊 Analyst Revisions & Snapshot"

])

# --------------------------------------------------

# TAB 1

# --------------------------------------------------

with tabs[0]:
    # --------------------------------------------------
    # HEADER
    # --------------------------------------------------
    
    st.title("📈 Nasdaq-100 — Market State")
    st.caption(f"As of end of day: {snapshot_date.strftime('%d %b %Y')}")
    
    # --------------------------------------------------
    # SYNTHETIC OVERLAY SUMMARY (NEW SECTION)
    # --------------------------------------------------
    
    st.markdown("### 🧾 Synthetic Overlay")
    
    colA, colB, colC = st.columns(3)
    
    colA.metric("Contracts", synthetic_summary_text)
    colB.metric("Net NQ Contracts", f"{int(net_contracts)}")
    colC.metric("Synthetic Notional", f"{synthetic_index_notional:,.0f}")
    
    st.divider()
    
    col1, col2, col3 = st.columns(3)
    
    col1.metric("Equity TRP (Stock Book)", f"{equity_trp:.2f}%")
    col2.metric("Total TRP (Incl Overlay)", f"{portfolio_trp:.2f}%")
    col3.metric("Hedge Dampening Effect", f"{hedge_impact:.2f}%")
    
    # --------------------------------------------------
    # GLOBAL METRICS
    # --------------------------------------------------
    
    top5_weight = df.loc[df["index_rank"] <= 5, "index_weight_pct"].sum()
    top10_weight = df.loc[df["index_rank"] <= 10, "index_weight_pct"].sum()
    pct_near_high = (df["pct_from_52w_high"] >= -10).mean() * 100
    earnings_14d = df["days_to_earnings"].between(0,14).sum()
    
    total_real = df["real_value"].sum()
    total_synth = df["synthetic_value"].sum()
    total_net = df["net_position_value"].sum()
    
    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("Top 5 weight", f"{top5_weight:.1f}%")
    c2.metric("Top 10 weight", f"{top10_weight:.1f}%")
    c3.metric("% within 10% of high", f"{pct_near_high:.0f}%")
    c4.metric("Earnings ≤14d", earnings_14d)
    c5.metric("Real Exposure", f"{total_real:,.0f}")
    c6.metric("Net Exposure", f"{total_net:,.0f}")
    
    st.divider()
    
    # --------------------------------------------------
    # FILTERS
    # --------------------------------------------------
    
    col1,col2,col3,col4 = st.columns(4)
    
    with col1:
        role_filter = st.multiselect("Role bucket",
            sorted(df["role_bucket"].dropna().unique()))
    
    with col2:
        cohort_filter = st.multiselect("Cohort",
            sorted(df["cohort_name"].dropna().unique()))
    
    with col3:
        max_rank = st.slider("Show top N constituents",1,101,101)
    
    with col4:
        earnings_filter = st.checkbox("Only earnings ≤14 days")
    
    filtered = df.copy()
    
    if role_filter:
        filtered = filtered[filtered["role_bucket"].isin(role_filter)]
    if cohort_filter:
        filtered = filtered[filtered["cohort_name"].isin(cohort_filter)]
    filtered = filtered[filtered["index_rank"] <= max_rank]
    if earnings_filter:
        filtered = filtered[filtered["days_to_earnings"].between(0,14)]
    
    # --------------------------------------------------
    # SELECTED COHORT % METRIC
    # --------------------------------------------------
    
    if cohort_filter:
        selected_weight = filtered["index_weight_pct"].sum()
        total_weight = df["index_weight_pct"].sum()
        selected_pct = selected_weight / total_weight * 100
    
        st.metric(
            "Selected Cohort % of Nasdaq-100",
            f"{selected_pct:.2f}%"
        )
    
    # --------------------------------------------------
    # MAIN TABLE
    # --------------------------------------------------
    
    st.subheader("📋 Canonical Market State + Synthetic Overlay")
    
    display_cols = [
        "ticker","sector_name","cohort_name","role_bucket",
        "index_rank","index_weight_pct","last_price",
        "pct_change_1d","pct_change_5d","pct_change_1m",
        "pct_change_ytd","pct_from_52w_high",
        "quantity","real_value","synthetic_quantity",
        "synthetic_value","net_position_value",
        "best_target_price","pct_to_best_target",
        "target_delta_1m_pct","revision_breadth_1m",
        "target_delta_3m_pct","revision_breadth_3m",
        "revision_signal",
        "analyst_count","best_analyst_rating",
        "days_to_earnings"
    ]
    
    table_df = filtered[[c for c in display_cols if c in filtered.columns]]
    table_df = table_df.set_index("ticker")
    
    st.dataframe(table_df, use_container_width=True)
    
    # --------------------------------------------------
    # FILTERED TOTALS
    # --------------------------------------------------
    
    st.markdown("### 📊 Selected Totals")
    
    c1,c2,c3 = st.columns(3)
    c1.metric("Real (Selected)", f"{filtered['real_value'].sum():,.0f}")
    c2.metric("Synthetic (Selected)", f"{filtered['synthetic_value'].sum():,.0f}")
    c3.metric("Net (Selected)", f"{filtered['net_position_value'].sum():,.0f}")
    
    # --------------------------------------------------
    # YTD PERFORMANCE DECOMPOSITION
    # --------------------------------------------------
    
    st.divider()
    st.subheader("📈 Nasdaq YTD Performance Decomposition")
    
    perf_summary = (
    
        filtered.groupby("cohort_name", dropna=False)
    
        .agg(
    
            avg_ytd_return=(
    
                "pct_change_ytd",
    
                "mean"
    
            ),
    
            total_weight=(
    
                "index_weight_pct",
    
                "sum"
    
            )
    
        )
    
        .reset_index()
    
    )
    
    # ---------------------------------------------
    # Weighted YTD Return
    # ---------------------------------------------
    
    weighted_returns = []
    
    index_contributions = []
    
    for cohort in perf_summary["cohort_name"]:
    
        cohort_df = filtered[
            filtered["cohort_name"] == cohort
        ]
    
        total_weight = cohort_df[
            "index_weight_pct"
        ].sum()
    
        if total_weight > 0:
    
            weighted_return = (
    
                (
                    cohort_df["pct_change_ytd"]
                    * cohort_df["index_weight_pct"]
                ).sum()
    
                / total_weight
    
            )
    
            contribution = (
    
                (
                    cohort_df["pct_change_ytd"]
                    * cohort_df["index_weight_pct"]
                ).sum()
    
                / 100
    
            )
    
        else:
    
            weighted_return = None
            contribution = None
    
        weighted_returns.append(weighted_return)
    
        index_contributions.append(contribution)
    
    perf_summary["weighted_ytd_return"] = weighted_returns
    
    perf_summary["index_contribution"] = index_contributions
    
    # ---------------------------------------------
    # Sort
    # ---------------------------------------------
    
    perf_summary = perf_summary.sort_values(
    
        "index_contribution",
    
        ascending=False
    
    )
    
    perf_summary = perf_summary.reset_index(drop=True)
    
    # ---------------------------------------------
    # Display
    # ---------------------------------------------
    
    st.dataframe(
    
        perf_summary.style.format({
    
            "avg_ytd_return": "{:.2f}%",
    
            "weighted_ytd_return": "{:.2f}%",
    
            "index_contribution": "{:.2f}%",
    
            "total_weight": "{:.2f}%"
    
        }),
    
        use_container_width=True
    
    )
    
    # ---------------------------------------------
    # Semiconductor vs Rest
    # ---------------------------------------------
    
    semis = perf_summary[
        perf_summary["cohort_name"]
        == "Semiconductors"
    ]
    
    semi_df = filtered[
        filtered["cohort_name"] == "Semiconductors"
    ].copy()
    
    semi_df["weighted_contribution"] = (
        semi_df["pct_change_ytd"]
        * semi_df["index_weight_pct"]
    )
    
    st.subheader("🔬 Semiconductor Cohort Decomposition")
    
    st.dataframe(
        semi_df[
            [
                "ticker",
                "index_weight_pct",
                "pct_change_ytd",
                "weighted_contribution"
            ]
        ]
        .sort_values(
            "weighted_contribution",
            ascending=False
        ),
        use_container_width=True
    )
    
    everything_else = perf_summary[
        perf_summary["cohort_name"]
        != "Semiconductors"
    ]
    
    semi_return = (
    
        semis["weighted_ytd_return"].iloc[0]
    
        if not semis.empty
    
        else 0
    
    )
    
    other_weights = everything_else["total_weight"].sum()
    
    if other_weights > 0:
    
        other_return = (
    
            (
                everything_else["weighted_ytd_return"]
    
                * everything_else["total_weight"]
    
            ).sum()
    
            / other_weights
    
        )
    
    else:
    
        other_return = 0
    
    semi_weight = (
        filtered[
            filtered["cohort_name"] == "Semiconductors"
        ]["index_weight_pct"]
        .sum()
    )
    
    other_weight = (
        filtered[
            filtered["cohort_name"] != "Semiconductors"
        ]["index_weight_pct"]
        .sum()
    )
    
    implied_ndx = (
        semi_return * semi_weight / 100
        +
        other_return * other_weight / 100
    )
    
    # ---------------------------------------------
    # APPROXIMATE SHARE OF NASDAQ GAINS
    # ---------------------------------------------
    
    semi_raw = semi_weight * semi_return
    other_raw = other_weight * other_return
    
    total_raw = semi_raw + other_raw
    
    if total_raw > 0:
    
        semi_share_of_gains = (
            semi_raw / total_raw
        ) * 100
    
        other_share_of_gains = (
            other_raw / total_raw
        ) * 100
    
    else:
    
        semi_share_of_gains = 0
        other_share_of_gains = 0
        
    # ---------------------------------------------
    # MARKET PERFORMANCE
    # ---------------------------------------------
    
    st.divider()
    
    st.subheader("📈 Market Performance")
    
    semi_product = semi_weight * semi_return / 100
    other_product = other_weight * other_return / 100
    
    total_product = semi_product + other_product
    
    m1, m2, m3 = st.columns(3)
    
    m1.metric(
        "Official Nasdaq-100 YTD Return",
        f"{official_ndx_ytd:.1f}%"
    )
    
    m2.metric(
        "Historical Chain-Linked Return",
        f"{chain_linked_ndx:.1f}%"
    )
    
    m3.metric(
        "Current Weight Decomposition",
        f"{total_product:.1f}%"
    )
    
    market_perf = pd.DataFrame({
        "Group": [
            "Semiconductors",
            "Non-Semiconductors",
            "Total"
        ],
        "Weight (%)": [
            semi_weight,
            other_weight,
            100.0
        ],
        "Return (%)": [
            semi_return,
            other_return,
            None
        ],
        "Product (%)": [
            semi_product,
            other_product,
            total_product
        ],
        "Contribution (%)": [
            semi_product / total_product * 100,
            other_product / total_product * 100,
            100.0
        ]
    })
    
    st.dataframe(
        market_perf.style.format({
            "Weight (%)": "{:.1f}",
            "Return (%)": "{:.1f}",
            "Product (%)": "{:.2f}",
            "Contribution (%)": "{:.1f}"
        }),
        use_container_width=True
    )
    
    st.caption(
        "Contribution = (Weight × Return) ÷ Total Contribution."
    )
    
    st.caption(
        "Methodology Note: "
        "Current Weight Decomposition uses today's Nasdaq-100 "
        "weights multiplied by constituent YTD returns. "
        "Historical Chain-Linked Return uses historical daily "
        "weights and daily stock returns from 30-Jan-2026 onwards."
    )
    
    # ---------------------------------------------
    # HISTORICAL PERFORMANCE ATTRIBUTION
    # ---------------------------------------------
    
    import numpy as np
    
    hist_attr = historical_attr.copy()
    
    hist_attr["historical_return"] = (
        np.exp(hist_attr["total_log_return"]) - 1
    ) * 100
    
    component_total = (
        hist_attr["historical_return"].sum()
    )
    
    total_hist_return = chain_linked_ndx
    
    hist_attr["share_of_return"] = (
        hist_attr["historical_return"]
        / component_total
    ) * 100
    
    # Put semis first
    hist_attr["sort_order"] = hist_attr["grp"].map({
        "Semiconductors": 1,
        "Non-Semiconductors": 2
    })
    
    hist_attr = hist_attr.sort_values("sort_order")
    
    # Keep only display columns
    hist_attr = hist_attr[
        [
            "grp",
            "historical_return",
            "share_of_return"
        ]
    ]
    
    # Friendly names
    hist_attr = hist_attr.rename(columns={
        "grp": "Group",
        "historical_return": "Historical Contribution (%)",
        "share_of_return": "Share of Historical Return (%)"
    })
    
    # Add total row
    total_row = pd.DataFrame({
        "Group": ["Total"],
        "Historical Contribution (%)": [total_hist_return],
        "Share of Historical Return (%)": [100.0]
    })
    
    hist_attr = pd.concat(
        [hist_attr, total_row],
        ignore_index=True
    )
    
    st.subheader("📈 Historical Performance Attribution")
    
    st.dataframe(
        hist_attr.style.format({
            "Historical Contribution (%)": "{:.1f}",
            "Share of Historical Return (%)": "{:.1f}"
        }),
        use_container_width=True
    )
    
    st.caption(
        "Uses daily constituent weights and daily stock returns "
        "from 30-Jan-2026 onwards. Values reconcile to the "
        "Historical Chain-Linked Return metric."
        "Historical contributions are calculated independently by cohort;"
        "due to compounding, contributions may not sum exactly to the total Historical Chain-Linked Return."
    )
    
    # --------------------------------------------------
    # ROLE SUMMARY
    # --------------------------------------------------
    
    st.divider()
    st.subheader("🧩 Role-Level Summary")
    
    role_summary = (
        filtered.groupby("role_bucket", dropna=False)
        .agg(
            total_weight=("index_weight_pct","sum"),
            real_exposure=("real_value","sum"),
            synthetic_exposure=("synthetic_value","sum"),
            net_exposure=("net_position_value","sum"),
            median_upside=("pct_to_best_target","median")
        )
        .reset_index()
        .sort_values("total_weight",ascending=False)
    )
    
    st.dataframe(
        role_summary.style.format({
            "total_weight":"{:.2f}%",
            "real_exposure":"{:,.0f}",
            "synthetic_exposure":"{:,.0f}",
            "net_exposure":"{:,.0f}",
            "median_upside":"{:.2f}%"
        }),
        use_container_width=True
    )
    
    # --------------------------------------------------
    # COHORT % OF NASDAQ-100
    # --------------------------------------------------
    
    st.divider()
    st.subheader("📊 Cohort % of Nasdaq-100")
    
    cohort_summary = (
        df.groupby("cohort_name", dropna=False)
        .agg(
            cohort_weight_pct=("index_weight_pct", "sum"),
            constituent_count=("ticker", "count")
        )
        .reset_index()
        .sort_values("cohort_weight_pct", ascending=False)
    )
    
    # Format nicely
    st.dataframe(
        cohort_summary.style.format({
            "cohort_weight_pct": "{:.2f}%"
        }),
        use_container_width=True
    )
    
    # --------------------------------------------------
    # COHORT TRP TENSION
    # --------------------------------------------------
    
    st.subheader("📊 Cohort TRP Tension (Overlay Adjusted)")
    
    if trp_df.empty:
    
        st.warning(
    
            "No TRP cohort data available."
    
        )
    
    else:
    
        if "cohort_name" not in trp_df.columns:
    
            st.error(
    
                "Missing cohort_name column in TRP dataset."
    
            )
    
        else:
    
            cohort_trp_rows = []
    
            valid_cohorts = (
    
                trp_df["cohort_name"]
    
                .dropna()
    
                .unique()
    
            )
    
            for cohort in sorted(valid_cohorts):
    
                cohort_df = trp_df[
    
                    trp_df["cohort_name"] == cohort
    
                ]
    
                gross_exposure = (
    
                    cohort_df["net_position_value"]
    
                    .abs()
    
                    .sum()
    
                )
    
                if gross_exposure > 0:
    
                    weighted_trp = (
    
                        (
                            cohort_df["net_position_value"]
    
                            * cohort_df["pct_to_best_target"]
    
                        ).sum()
    
                        / gross_exposure
    
                    )
    
                else:
    
                    weighted_trp = 0
    
                cohort_trp_rows.append({
    
                    "cohort_name": cohort,
    
                    "weighted_trp": weighted_trp
    
                })
    
            if len(cohort_trp_rows) == 0:
    
                st.warning(
    
                    "No valid cohort TRP rows calculated."
    
                )
    
            else:
    
                cohort_trp = pd.DataFrame(
    
                    cohort_trp_rows
    
                )
    
                cohort_trp = cohort_trp.sort_values(
    
                    "weighted_trp",
    
                    ascending=False
    
                )
    
                st.dataframe(
    
                    cohort_trp.style.format({
    
                        "weighted_trp": "{:.2f}%"
    
                    }),
    
                    use_container_width=True
    
                )

    # --------------------------------------------------
    # FOOTER
    # --------------------------------------------------
    
    st.caption(
        f"Encore Analytics • Nasdaq-100 Market State • Generated {date.today().isoformat()}"
    )
    
# --------------------------------------------------
# TAB 2
# --------------------------------------------------

with tabs[1]:

    st.title("📊 Analyst Revisions & Market Snapshot")

    revisions_df = load_latest_analyst_revisions()

    if not revisions_df.empty:

        st.subheader(
            f"Analyst Revisions ({revisions_df['snapshot_date'].iloc[0]})"
        )

        st.dataframe(
            revisions_df.drop(columns=["snapshot_date"]),
            use_container_width=True
        )

    snapshot_df = load_latest_market_snapshot()

    if not snapshot_df.empty:

        st.subheader(
            f"Market Snapshot ({snapshot_df['snapshot_date'].iloc[0]})"
        )

        st.dataframe(
            snapshot_df.drop(columns=["snapshot_date"]),
            use_container_width=True
        )
