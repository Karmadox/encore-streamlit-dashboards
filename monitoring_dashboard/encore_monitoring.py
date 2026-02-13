import streamlit as st
import pandas as pd
import psycopg2
from datetime import date
import streamlit_autorefresh

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

    else:
        return True


if not check_password():
    st.stop()

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

st.set_page_config(
    page_title="Encore Monitoring – Security Master",
    layout="wide"
)

# Auto refresh every 60 seconds
streamlit_autorefresh.st_autorefresh(interval=60000, key="monitor_refresh")

DB_CONFIG = st.secrets["db"]

# --------------------------------------------------
# DB CONNECTION
# --------------------------------------------------

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def sql_param(x):
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
    sql = """
        WITH latest_positions AS (
            SELECT *
            FROM encoredb.positions_eod_snapshot
            WHERE snapshot_date = (
                SELECT MAX(snapshot_date)
                FROM encoredb.positions_eod_snapshot
            )
        )
        SELECT i.ticker, i.name
        FROM latest_positions p
        JOIN encoredb.instruments i
          ON i.instrument_id = p.instrument_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM encoredb.instrument_cohort_weights w
            WHERE w.instrument_id = p.instrument_id
        )
        ORDER BY i.ticker
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

# --------------------------------------------------
# TASK MONITORING LOADER
# --------------------------------------------------

@st.cache_data(ttl=60)
def load_task_status():
    sql = """
        SELECT *
        FROM encoredb.task_execution_log
        WHERE run_start >= NOW() - INTERVAL '1 day'
        ORDER BY run_start DESC
    """
    with get_conn() as conn:
        df = pd.read_sql(sql, conn)

    if df.empty:
        return df

    latest = (
        df.sort_values("run_start", ascending=False)
          .groupby("task_name")
          .first()
          .reset_index()
    )

    latest["run_start"] = pd.to_datetime(latest["run_start"])
    latest["run_end"] = pd.to_datetime(latest["run_end"])

    now = pd.Timestamp.utcnow()

    latest["minutes_since_last_run"] = (
        (now - latest["run_start"]).dt.total_seconds() / 60
    ).round(1)

    # Health logic (3-min job rule)
    def health(row):
        if row["status"] == "FAILED":
            return "🔴 FAILED"
        if row["status"] == "RUNNING":
            return "🟡 RUNNING"
        if row["minutes_since_last_run"] > 6:
            return "🟠 STALE"
        return "🟢 HEALTHY"

    latest["health"] = latest.apply(health, axis=1)

    return latest

# --------------------------------------------------
# UI
# --------------------------------------------------

st.title("🛡️ Encore Monitoring")

tabs = st.tabs([
    "🚨 Instruments Requiring Attention",
    "🏭 Sector → Cohort → Instruments",
    "🖥 Task Monitoring"
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
        st.dataframe(issues, use_container_width=True)

# ==================================================
# TAB 2 — SECURITY MASTER EXPLORER
# ==================================================
with tabs[1]:

    st.subheader("🏭 Security Master Explorer")

    sectors = load_sectors()

    sel_sector = st.selectbox("Select Sector", sectors["sector_name"])

    sector_id = sectors.loc[
        sectors["sector_name"] == sel_sector,
        "sector_id"
    ].iloc[0]

    cohorts = load_cohorts(sector_id)

    if cohorts.empty:
        st.info("No cohorts defined.")
    else:
        sel_cohort = st.selectbox("Select Cohort", cohorts["cohort_name"])
        cohort_id = cohorts.loc[
            cohorts["cohort_name"] == sel_cohort,
            "cohort_id"
        ].iloc[0]

        instruments = load_instruments_for_cohort(cohort_id)

        if instruments.empty:
            st.info("No instruments assigned.")
        else:
            st.dataframe(instruments, use_container_width=True)

# ==================================================
# TAB 3 — TASK MONITORING
# ==================================================
with tabs[2]:

    st.subheader("🖥 Windows Task Monitoring")

    tasks = load_task_status()

    if tasks.empty:
        st.warning("No task executions found.")
    else:
        st.dataframe(
            tasks[
                [
                    "task_name",
                    "health",
                    "status",
                    "run_start",
                    "run_end",
                    "runtime_seconds",
                    "rows_processed",
                    "minutes_since_last_run"
                ]
            ],
            use_container_width=True
        )

        st.markdown(
            """
            **Health Definitions**
            - 🟢 HEALTHY → Ran successfully within expected window  
            - 🟠 STALE → Missed expected schedule  
            - 🔴 FAILED → Last execution failed  
            - 🟡 RUNNING → Currently executing  
            """
        )

# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.caption(
    f"Data as of {date.today().isoformat()} • Encore Internal Monitoring"
)
