import streamlit as st
import pandas as pd
import psycopg2
from datetime import date

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

st.set_page_config(
    page_title="Encore Monitoring – Security Master",
    layout="wide"
)

DB_CONFIG = st.secrets["db"]

# --------------------------------------------------
# DB CONNECTION
# --------------------------------------------------

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def sql_param(x):
    """Convert numpy scalars to native Python types"""
    if hasattr(x, "item"):
        return x.item()
    return x

# --------------------------------------------------
# DATA LOADERS
# --------------------------------------------------

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
def load_cohorts(sector_id):
    sql = """
        SELECT cohort_id, cohort_name
        FROM encoredb.cohorts
        WHERE sector_id = %s
        ORDER BY cohort_name
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(sql_param(sector_id),))

@st.cache_data(ttl=300)
def load_instruments_for_cohort(cohort_id):
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
        ORDER BY w.is_primary DESC, w.weight_pct DESC, i.ticker
    """
    with get_conn() as conn:
        return pd.read_sql(
            sql,
            conn,
            params=(sql_param(cohort_id),)
        )

@st.cache_data(ttl=300)
def load_missing_assignments():
    sql = """
        SELECT DISTINCT
            i.instrument_id,
            i.ticker,
            i.name
        FROM encoredb.positions_eod_snapshot p
        JOIN encoredb.instruments i
          ON i.instrument_id = p.instrument_id
        LEFT JOIN encoredb.instrument_cohort_weights w
          ON w.instrument_id = i.instrument_id
         AND w.is_primary = true
         AND w.effective_date <= p.snapshot_date
        WHERE w.instrument_id IS NULL
        ORDER BY i.ticker
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

# --------------------------------------------------
# UI
# --------------------------------------------------

st.title("🛡️ Encore Monitoring – Security Master")

tabs = st.tabs([
    "🚨 Unassigned Instruments",
    "🏭 Sector → Cohort → Instruments"
])

# ==================================================
# TAB 1 — MISSING ASSIGNMENTS
# ==================================================
with tabs[0]:
    st.subheader("🚨 Instruments Missing Sector / Cohort Assignment")

    missing = load_missing_assignments()

    if missing.empty:
        st.success("✅ All instruments are correctly assigned.")
    else:
        st.warning(f"⚠ {len(missing)} instruments require attention")
        st.dataframe(missing, use_container_width=True)

# ==================================================
# TAB 2 — SECTOR → COHORT → INSTRUMENTS
# ==================================================
with tabs[1]:
    st.subheader("🏭 Security Master Explorer")

    sectors = load_sectors()

    sel_sector = st.selectbox(
        "Select Sector",
        sectors["sector_name"],
        key="sector_select"
    )

    sector_id = sectors.loc[
        sectors["sector_name"] == sel_sector,
        "sector_id"
    ].iloc[0]

    cohorts = load_cohorts(sector_id)

    if cohorts.empty:
        st.info("No cohorts defined for this sector.")
        st.stop()

    sel_cohort = st.selectbox(
        "Select Cohort",
        cohorts["cohort_name"],
        key="cohort_select"
    )

    cohort_id = cohorts.loc[
        cohorts["cohort_name"] == sel_cohort,
        "cohort_id"
    ].iloc[0]

    instruments = load_instruments_for_cohort(cohort_id)

    if instruments.empty:
        st.info("No instruments assigned to this cohort.")
    else:
        st.markdown(
            """
            **Legend**
            - ⭐ `is_primary = true`
            - `weight_pct` is fractional (1.0 = 100%)
            """
        )

        st.dataframe(
            instruments,
            use_container_width=True
        )

# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.caption(
    f"Data as of {date.today().isoformat()} • Encore Internal Monitoring"
)