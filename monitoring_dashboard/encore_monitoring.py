import streamlit as st
import pandas as pd
import psycopg2
from datetime import date

# =========================
# STREAMLIT CONFIG
# =========================
st.set_page_config(
    page_title="Encore Monitoring – Security Master",
    layout="wide"
)

st.title("🧬 Encore Monitoring — Security Master")

# =========================
# DB CONNECTION
# =========================
def get_conn():
    return psycopg2.connect(**st.secrets["db"])

# =========================
# LOAD LATEST SNAPSHOT DATE
# =========================
@st.cache_data(ttl=300)
def load_latest_snapshot_date():
    sql = """
        SELECT MAX(snapshot_date)
        FROM encoredb.positions_eod_snapshot
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn).iloc[0, 0]

snapshot_date = load_latest_snapshot_date()
st.caption(f"📅 Latest positions snapshot: **{snapshot_date}**")

# ==========================================================
# SECTION 1 — SECURITY MASTER GAPS
# ==========================================================
st.header("🚨 Instruments Requiring Classification")

@st.cache_data(ttl=300)
def load_security_master_gaps(snapshot_date):
    sql = """
        SELECT
            e.instrument_id,
            i.ticker,
            i.name,
            MIN(e.snapshot_date) AS first_seen,
            MAX(e.snapshot_date) AS last_seen
        FROM encoredb.positions_eod_snapshot e
        JOIN encoredb.instruments i
          ON i.instrument_id = e.instrument_id
        LEFT JOIN encoredb.instrument_cohort_weights w
          ON w.instrument_id = e.instrument_id
         AND w.is_primary = true
         AND w.effective_date <= e.snapshot_date
        WHERE e.snapshot_date = %s
          AND w.instrument_id IS NULL
        GROUP BY e.instrument_id, i.ticker, i.name
        ORDER BY first_seen
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(snapshot_date,))

gaps = load_security_master_gaps(snapshot_date)

if gaps.empty:
    st.success("✅ All instruments in the latest snapshot are fully classified.")
else:
    st.error(f"❌ {len(gaps)} instrument(s) missing sector / cohort assignment")
    st.dataframe(gaps, use_container_width=True)

# ==========================================================
# SECTION 2 — SECTOR → COHORT → INSTRUMENT BROWSER
# ==========================================================
st.header("🧭 Sector → Cohort → Instrument Reference")

@st.cache_data(ttl=300)
def load_sectors():
    sql = """
        SELECT sector_id, sector_name
        FROM encoredb.sectors
        ORDER BY sector_name
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

@st.cache_data(ttl=300)
def load_cohorts(sector_name):
    sql = """
        SELECT
            c.cohort_id,
            c.cohort_name
        FROM encoredb.cohorts c
        JOIN encoredb.sectors s
          ON s.sector_id = c.sector_id
        WHERE s.sector_name = %s
        ORDER BY c.cohort_name
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(sector_name,))

@st.cache_data(ttl=300)
def load_cohort_instruments(cohort_id):
    sql = """
        SELECT
            i.ticker,
            i.name,
            w.weight_pct,
            w.is_primary,
            w.effective_date,
            w.source
        FROM encoredb.instrument_cohort_weights w
        JOIN encoredb.instruments i
          ON i.instrument_id = w.instrument_id
        WHERE w.cohort_id = %s
          AND w.effective_date = (
              SELECT MAX(w2.effective_date)
              FROM encoredb.instrument_cohort_weights w2
              WHERE w2.instrument_id = w.instrument_id
                AND w2.cohort_id = w.cohort_id
          )
        ORDER BY w.is_primary DESC, w.weight_pct DESC
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(cohort_id,))

# --------------------------
# UI FLOW
# --------------------------
sectors_df = load_sectors()
sector_name = st.selectbox("Select Sector", sectors_df["sector_name"])

cohorts_df = load_cohorts(sector_name)

if cohorts_df.empty:
    st.info("No cohorts defined for this sector.")
else:
    cohort_name = st.selectbox("Select Cohort", cohorts_df["cohort_name"])
    cohort_id = cohorts_df.loc[
        cohorts_df["cohort_name"] == cohort_name, "cohort_id"
    ].iloc[0]

    instruments_df = load_cohort_instruments(cohort_id)

    if instruments_df.empty:
        st.warning("No instruments assigned to this cohort.")
    else:
        st.markdown(f"**Instruments in `{cohort_name}`**")
        st.dataframe(instruments_df, use_container_width=True)

        total_weight = instruments_df["weight_pct"].sum()
        if abs(total_weight - 1.0) > 0.001:
            st.warning(f"⚠️ Weight sum = {total_weight:.3f} (expected 1.000)")
        else:
            st.success("✅ Cohort weights sum to 1.000")