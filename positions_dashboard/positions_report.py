import streamlit as st
import pandas as pd
import psycopg2
from datetime import date

# -------------------------------------------------
# STREAMLIT CONFIG
# -------------------------------------------------
st.set_page_config(
    page_title="Encore Positions Dashboard",
    layout="wide",
)

st.title("📊 Encore – Positions Dashboard")

# -------------------------------------------------
# DATABASE CONNECTION (Streamlit Secrets)
# -------------------------------------------------
def get_conn():
    return psycopg2.connect(
        dbname=st.secrets["db"]["dbname"],
        user=st.secrets["db"]["user"],
        password=st.secrets["db"]["password"],
        host=st.secrets["db"]["host"],
        port=st.secrets["db"]["port"],
    )

# -------------------------------------------------
# DATA LOADERS
# -------------------------------------------------
@st.cache_data(ttl=60)
def load_latest_snapshot():
    sql = """
        SELECT *
        FROM encoredb.positions_snapshot
        WHERE snapshot_ts = (
            SELECT MAX(snapshot_ts)
            FROM encoredb.positions_snapshot
        )
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data(ttl=60)
def load_intraday_snapshots():
    sql = """
        SELECT *
        FROM encoredb.positions_snapshot
        WHERE snapshot_date = CURRENT_DATE
        ORDER BY snapshot_ts
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data(ttl=300)
def load_commtech_cohorts():
    sql = """
        SELECT
            i.ticker,
            c.cohort_name,
            w.weight_pct
        FROM encoredb.instrument_cohort_weights w
        JOIN encoredb.cohorts c
            ON w.cohort_id = c.cohort_id
        JOIN encoredb.instruments i
            ON w.instrument_id = i.instrument_id
        JOIN encoredb.sectors s
            ON c.sector_id = s.sector_id
        WHERE s.sector_code = 'COMM_TECH'
          AND w.effective_date = (
                SELECT MAX(w2.effective_date)
                FROM encoredb.instrument_cohort_weights w2
                WHERE w2.instrument_id = w.instrument_id
                  AND w2.cohort_id = w.cohort_id
                  AND w2.effective_date <= CURRENT_DATE
          )
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data(ttl=60)
def load_notional_totals():
    sql = """
        SELECT
            SUM(ABS(gross_notional)) AS gross_notional,
            SUM(nmv) AS net_notional
        FROM encoredb.positions_snapshot
        WHERE snapshot_ts = (
            SELECT MAX(snapshot_ts)
            FROM encoredb.positions_snapshot
        )
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

# -------------------------------------------------
# PRICE CHANGE BUCKETS
# -------------------------------------------------
def classify_move(x):
    if x <= -3:
        return "< -3%"
    elif -3 < x <= -2:
        return "-3% to -2%"
    elif -2 < x <= -1:
        return "-2% to -1%"
    elif -1 < x < 1:
        return "Neutral"
    elif 1 <= x < 2:
        return "1% to 2%"
    elif 2 <= x < 3:
        return "2% to 3%"
    else:
        return "> 3%"

# -------------------------------------------------
# LOAD DATA
# -------------------------------------------------
latest = load_latest_snapshot()
intraday = load_intraday_snapshots()
notional_totals = load_notional_totals()

if latest.empty:
    st.warning("No position data available yet.")
    st.stop()

# -------------------------------------------------
# OVERVIEW – LATEST SNAPSHOT
# -------------------------------------------------
st.header("🕒 Latest Snapshot Overview")

latest["move_bucket"] = latest["price_change_pct"].apply(classify_move)

gross_notional = notional_totals.loc[0, "gross_notional"]
net_notional = notional_totals.loc[0, "net_notional"]

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Positions", len(latest))

with col2:
    st.metric(
        "Gross Notional (USD)",
        f"${gross_notional/1e6:,.1f}m"
    )

with col3:
    st.metric(
        "Net Notional (USD)",
        f"${net_notional/1e6:,.1f}m"
    )

with col4:
    st.metric(
        "Avg Price Move (%)",
        round(latest["price_change_pct"].mean(), 2)
    )

bucket_summary = (
    latest
    .groupby("move_bucket")
    .size()
    .reset_index(name="count")
)

st.subheader("📉 Price Move Distribution")
st.bar_chart(bucket_summary.set_index("move_bucket"))

# -------------------------------------------------
# INTRADAY – PORTFOLIO TREND
# -------------------------------------------------
st.header("⏱️ Intraday Portfolio Performance (Today)")

intraday_portfolio = (
    intraday
    .groupby("snapshot_ts")
    .agg(
        avg_move=("price_change_pct", "mean"),
        total_nmv=("nmv", "sum"),
        total_gross=("gross_notional", lambda x: x.abs().sum()),
    )
    .reset_index()
)

st.line_chart(
    intraday_portfolio.set_index("snapshot_ts")[["total_gross", "total_nmv"]],
    height=300
)

# -------------------------------------------------
# INTRADAY – SECTOR VIEW
# -------------------------------------------------
st.header("🏭 Intraday Sector Performance")

sector = st.selectbox(
    "Select Sector",
    sorted(intraday["egm_sector_v2"].dropna().unique())
)

sector_intraday = (
    intraday[intraday["egm_sector_v2"] == sector]
    .groupby("snapshot_ts")
    .agg(avg_move=("price_change_pct", "mean"))
    .reset_index()
)

st.line_chart(
    sector_intraday.set_index("snapshot_ts"),
    height=300
)

# -------------------------------------------------
# INTRADAY – COMM/TECH COHORT VIEW
# -------------------------------------------------
st.header("🧩 Comm/Tech – Intraday Cohort Performance")

cohorts = load_commtech_cohorts()

ct_intraday = intraday.merge(
    cohorts,
    on="ticker",
    how="inner"
)

if ct_intraday.empty:
    st.info("No Comm/Tech cohort data available yet.")
else:
    cohort = st.selectbox(
        "Select Comm/Tech Cohort",
        sorted(ct_intraday["cohort_name"].unique())
    )

    cohort_intraday = (
        ct_intraday[ct_intraday["cohort_name"] == cohort]
        .groupby("snapshot_ts")
        .apply(
            lambda x: (
                x["price_change_pct"] * x["weight_pct"] / 100
            ).mean()
        )
        .reset_index(name="weighted_move")
    )

    st.line_chart(
        cohort_intraday.set_index("snapshot_ts"),
        height=300
    )

# -------------------------------------------------
# DETAIL TABLE – LATEST SNAPSHOT
# -------------------------------------------------
st.header("📋 Latest Positions Detail")

st.dataframe(
    latest[
        [
            "ticker",
            "description",
            "egm_sector_v2",
            "quantity",
            "price_change_pct",
            "gross_notional",
            "nmv",
            "move_bucket",
        ]
    ].sort_values("price_change_pct"),
    use_container_width=True,
)