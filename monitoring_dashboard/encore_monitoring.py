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
        return pd.read_sql(sql, conn, params=(sql_param(cohort_id),))

@st.cache_data(ttl=300)
def load_security_master_issues():
    """
    Return instruments in latest positions that have
    missing / ambiguous sector or cohort assignment,
    with an explicit reason.
    """
    sql = """
        WITH latest_positions AS (
            SELECT *
            FROM encoredb.positions_eod_snapshot
            WHERE snapshot_date = (
                SELECT MAX(snapshot_date)
                FROM encoredb.positions_eod_snapshot
            )
        ),

        primary_candidates AS (
            SELECT
                w.instrument_id,
                w.cohort_id,
                w.effective_date
            FROM encoredb.instrument_cohort_weights w
            WHERE w.is_primary = true
        ),

        primary_valid AS (
            SELECT
                p.instrument_id,
                MAX(w.effective_date) AS effective_date
            FROM latest_positions p
            LEFT JOIN primary_candidates w
              ON w.instrument_id = p.instrument_id
             AND w.effective_date <= p.snapshot_date
            GROUP BY p.instrument_id
        ),

        primary_count AS (
            SELECT
                p.instrument_id,
                COUNT(*) AS primary_count
            FROM latest_positions p
            JOIN primary_candidates w
              ON w.instrument_id = p.instrument_id
             AND w.effective_date <= p.snapshot_date
            GROUP BY p.instrument_id
        )

        SELECT
            i.ticker,
            i.name,

            CASE
                WHEN NOT EXISTS (
                    SELECT 1
                    FROM encoredb.instrument_cohort_weights w
                    WHERE w.instrument_id = p.instrument_id
                )
                    THEN 'No cohort assignments exist'

                WHEN pv.effective_date IS NULL
                    THEN 'Primary cohort exists but only in the future'

                WHEN pc.primary_count > 1
                    THEN 'Multiple primary cohorts valid for date'

                WHEN s.sector_id IS NULL
                    THEN 'Primary cohort has no sector'

                ELSE 'Unknown issue'
            END AS issue_reason

        FROM latest_positions p
        JOIN encoredb.instruments i
          ON i.instrument_id = p.instrument_id

        LEFT JOIN primary_valid pv
          ON pv.instrument_id = p.instrument_id

        LEFT JOIN primary_count pc
          ON pc.instrument_id = p.instrument_id

        LEFT JOIN encoredb.instrument_cohort_weights w
          ON w.instrument_id = p.instrument_id
         AND w.is_primary = true
         AND w.effective_date = pv.effective_date

        LEFT JOIN encoredb.cohorts c
          ON c.cohort_id = w.cohort_id

        LEFT JOIN encoredb.sectors s
          ON s.sector_id = c.sector_id

        WHERE
            pv.effective_date IS NULL
            OR pc.primary_count > 1
            OR s.sector_id IS NULL

        ORDER BY i.ticker
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

# --------------------------------------------------
# UI
# --------------------------------------------------

st.title("🛡️ Encore Monitoring – Security Master")

tabs = st.tabs([
    "🚨 Instruments Requiring Attention",
    "🏭 Sector → Cohort → Instruments"
])

# ==================================================
# TAB 1 — SECURITY MASTER ISSUES
# ==================================================
with tabs[0]:
    st.subheader("🚨 Instruments Requiring Attention")

    issues = load_security_master_issues()

    if issues.empty:
        st.success("✅ All instruments have valid sector & cohort assignments.")
    else:
        st.warning(f"⚠ {len(issues)} instruments require attention")

        st.dataframe(
            issues,
            use_container_width=True
        )

        with st.expander("ℹ️ How to interpret issues"):
            st.markdown(
                """
                **Issue meanings**
                - **No cohort assignments exist**  
                  → Instrument never mapped in `instrument_cohort_weights`
                - **Primary cohort exists but only in the future**  
                  → Missing historical backfill
                - **Multiple primary cohorts valid for date**  
                  → Duplicate primary mappings
                - **Primary cohort has no sector**  
                  → Cohort definition incomplete
                """
            )

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