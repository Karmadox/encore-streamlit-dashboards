import streamlit as st
import pandas as pd
import psycopg2
from streamlit_autorefresh import st_autorefresh

# -------------------------------------------------
# STREAMLIT CONFIG
# -------------------------------------------------
st.set_page_config(
    page_title="Encore Positions Dashboard",
    layout="wide",
)

st.title("📊 Encore – Positions Dashboard")

# Auto-refresh every 5 minutes
st_autorefresh(interval=5 * 60 * 1000, key="positions_refresh")

# -------------------------------------------------
# DATABASE CONNECTION
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
def load_intraday():
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
            w.weight_pct,
            w.is_primary
        FROM encoredb.instrument_cohort_weights w
        JOIN encoredb.cohorts c ON w.cohort_id = c.cohort_id
        JOIN encoredb.instruments i ON w.instrument_id = i.instrument_id
        JOIN encoredb.sectors s ON c.sector_id = s.sector_id
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

# -------------------------------------------------
# MOVE BUCKETS (NO NEUTRAL)
# -------------------------------------------------
def classify_move(x):
    if pd.isna(x):
        return "< 1% up"

    if x > 3:
        return "> 3% up"
    elif 2 < x <= 3:
        return "2–3% up"
    elif 1 < x <= 2:
        return "1–2% up"
    elif 0 <= x <= 1:
        return "< 1% up"
    elif -1 <= x < 0:
        return "< 1% down"
    elif -2 <= x < -1:
        return "1–2% down"
    elif -3 <= x < -2:
        return "2–3% down"
    else:
        return "> 3% down"

BUCKET_ORDER = [
    "> 3% up",
    "2–3% up",
    "1–2% up",
    "< 1% up",
    "< 1% down",
    "1–2% down",
    "2–3% down",
    "> 3% down",
]

# -------------------------------------------------
# LOAD DATA
# -------------------------------------------------
intraday = load_intraday()

if intraday.empty:
    st.warning("No position data available yet.")
    st.stop()

# ---- timestamps
intraday["snapshot_ts"] = pd.to_datetime(intraday["snapshot_ts"])
intraday["time_label"] = (
    intraday["snapshot_ts"]
    .dt.tz_convert("US/Central")
    .dt.strftime("%H:%M")
)

# ---- normalize Enfusion fractional price change
intraday["price_change_pct"] = intraday["price_change_pct"] * 100
intraday["move_bucket"] = intraday["price_change_pct"].apply(classify_move)

latest_ts = intraday["snapshot_ts"].max()
latest = intraday[intraday["snapshot_ts"] == latest_ts].copy()

# -------------------------------------------------
# TABS
# -------------------------------------------------
tab_sector, tab_price = st.tabs(
    ["🏭 Sector Driven", "📈 Price Change Driven"]
)

# =================================================
# TAB 1 — SECTOR DRIVEN (OPTION A)
# =================================================
with tab_sector:
    st.header("🏭 Sector-Driven Intraday Performance")

    st.markdown(
        """
**Methodology**

Sector performance is calculated using **PnL normalized by exposure**: