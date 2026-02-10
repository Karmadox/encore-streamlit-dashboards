import streamlit as st
import pandas as pd
import psycopg2
from datetime import date

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------

st.set_page_config(
    page_title="Nasdaq-100 Market State",
    layout="wide",
)

# --------------------------------------------------
# DB CONFIG
# --------------------------------------------------

DB_CONFIG = st.secrets["db"]

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

# --------------------------------------------------
# DATA LOADERS
# --------------------------------------------------

@st.cache_data(ttl=300)
def load_latest_snapshot_date():
    sql = """
        SELECT MAX(snapshot_date) AS snapshot_date
        FROM encoredb.ndx_market_snapshot
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)["snapshot_date"].iloc[0]

@st.cache_data(ttl=300)
def load_market_state(snapshot_date):
    sql = """
        SELECT
            snapshot_date,
            ticker,
            sector_name,
            cohort_name,
            role_bucket,
            index_rank,
            index_weight_pct,
            cumulative_weight_pct,
            last_price,
            pct_change_1d,
            pct_change_5d,
            pct_change_1m,
            pct_change_ytd,
            pct_from_52w_high,
            best_target_price,
            pct_to_best_target,
            analyst_count,
            best_analyst_rating,
            next_earnings_date,
            days_to_earnings
        FROM encoredb.v_ndx_canonical_market_state_enriched
        WHERE snapshot_date = %s
        ORDER BY index_rank
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(snapshot_date,))

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------

snapshot_date = load_latest_snapshot_date()
df = load_market_state(snapshot_date)

# --------------------------------------------------
# HEADER
# --------------------------------------------------

st.title("📈 Nasdaq-100 — Market State")
st.caption(f"As of end of day: {snapshot_date.strftime('%d %b %Y')}")

# --------------------------------------------------
# HOW TO READ THIS CHART
# --------------------------------------------------

with st.expander("ℹ️ How to read this chart", expanded=False):
    st.markdown("""
**What this dashboard shows**

A point-in-time view of the Nasdaq-100, combining:
- index structure (weights & ranks)
- market positioning (price, momentum, distance from highs)
- expectations (analyst targets & ratings)
- near-term risk (earnings timing)

**Key fields**

- **Last Price**  
  End-of-day price for the snapshot date.

- **% Change (1D / 5D / 1M / YTD)**  
  Performance over different horizons — distinguishes flow vs structure.

- **% from 52W High**  
  Near zero = extended; deeply negative = lagging / potential mean reversion.

- **% to Best Target**  
  Difference between price and most optimistic analyst target.  
  + = upside expected, − = expectations risk.

- **Analyst Rating**  
  Bloomberg consensus (1 = Strong Buy, 5 = Sell).

- **Days to Earnings**  
  Time to next expected earnings report.
""")

st.divider()

# --------------------------------------------------
# GLOBAL SUMMARY METRICS
# --------------------------------------------------

top5_weight = df.loc[df["index_rank"] <= 5, "index_weight_pct"].sum()
top10_weight = df.loc[df["index_rank"] <= 10, "index_weight_pct"].sum()
pct_near_high = (df["pct_from_52w_high"] >= -10).mean() * 100
earnings_7d = df["days_to_earnings"].between(0, 7).sum()
earnings_14d = df["days_to_earnings"].between(0, 14).sum()

# --------------------------------------------------
# FILTERS
# --------------------------------------------------

col_f1, col_f2, col_f3 = st.columns(3)

with col_f1:
    role_filter = st.multiselect(
        "Role bucket",
        sorted(df["role_bucket"].dropna().unique()),
        default=[]
    )

with col_f2:
    max_rank = st.slider(
        "Show top N index constituents (by weight)",
        min_value=1,
        max_value=101,
        value=101,
        help="Filters to the top N Nasdaq-100 constituents by index weight."
    )

with col_f3:
    earnings_filter = st.checkbox("Only instruments with earnings ≤ 14 days")

filtered = df.copy()

if role_filter:
    filtered = filtered[filtered["role_bucket"].isin(role_filter)]

filtered = filtered[filtered["index_rank"] <= max_rank]

if earnings_filter:
    filtered = filtered[filtered["days_to_earnings"].between(0, 14)]

# --------------------------------------------------
# FILTER-AWARE METRICS
# --------------------------------------------------

selected_weight = filtered["index_weight_pct"].sum()

c1, c2, c3, c4, c5, c6 = st.columns(6)

c1.metric("Top 5 weight", f"{top5_weight:.1f}%")
c2.metric("Top 10 weight", f"{top10_weight:.1f}%")
c3.metric(
    "Selected weight",
    f"{selected_weight:.1f}%",
    help="Total Nasdaq-100 index weight of the currently selected instruments"
)
c4.metric("% within 10% of 52W high", f"{pct_near_high:.0f}%")
c5.metric("Earnings ≤ 7 days", earnings_7d)
c6.metric("Earnings ≤ 14 days", earnings_14d)

st.divider()

# --------------------------------------------------
# FORMAT HELPERS
# --------------------------------------------------

def format_signed_pct(x):
    if pd.isna(x):
        return ""
    return f"{x:+.1f}%"

def color_signed_pct(x):
    if pd.isna(x):
        return ""
    if x > 0:
        return "color: #166534;"
    if x < 0:
        return "color: #991b1b;"
    return ""

# --------------------------------------------------
# MAIN TABLE
# --------------------------------------------------

st.subheader("📋 Canonical Market State")

display_cols = [
    "ticker",
    "sector_name",
    "cohort_name",
    "role_bucket",
    "index_rank",
    "index_weight_pct",
    "last_price",
    "pct_change_1d",
    "pct_change_5d",
    "pct_change_1m",
    "pct_change_ytd",
    "pct_from_52w_high",
    "pct_to_best_target",
    "analyst_count",
    "best_analyst_rating",
    "days_to_earnings",
]

styled = (
    filtered[display_cols]
    .style
    .format({
        "index_weight_pct": "{:.3f}",
        "last_price": "{:.2f}",
        "pct_change_1d": "{:.2f}%",
        "pct_change_5d": "{:.2f}%",
        "pct_change_1m": "{:.2f}%",
        "pct_change_ytd": "{:.2f}%",
        "pct_from_52w_high": "{:.2f}%",
        "pct_to_best_target": format_signed_pct,
    })
    .applymap(color_signed_pct, subset=["pct_to_best_target"])
)

st.dataframe(styled, use_container_width=True)

# --------------------------------------------------
# ROLE-LEVEL AGGREGATION
# --------------------------------------------------

st.divider()
st.subheader("🧩 Role-Level Summary")

role_summary = (
    df
    .groupby("role_bucket", dropna=False)
    .agg(
        total_weight=("index_weight_pct", "sum"),
        avg_pct_from_high=("pct_from_52w_high", "mean"),
        pct_near_high=("pct_from_52w_high", lambda x: (x >= -10).mean() * 100),
        earnings_14d=("days_to_earnings", lambda x: x.between(0, 14).sum()),
        median_upside=("pct_to_best_target", "median")
    )
    .reset_index()
    .sort_values("total_weight", ascending=False)
)

st.dataframe(
    role_summary.style.format({
        "total_weight": "{:.2f}%",
        "avg_pct_from_high": "{:.2f}%",
        "pct_near_high": "{:.0f}%",
        "median_upside": format_signed_pct,
    }),
    use_container_width=True
)

# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.caption(
    f"Encore Analytics • Nasdaq-100 Market State • Generated {date.today().isoformat()}"
)