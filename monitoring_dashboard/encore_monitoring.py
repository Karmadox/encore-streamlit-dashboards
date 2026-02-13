import streamlit as st
import pandas as pd
import psycopg2
from datetime import date, datetime, timedelta, timezone
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
# TASK MONITORING LOADER (CORRECT TIMEZONE FIX)
# --------------------------------------------------

@st.cache_data(ttl=60)
def load_task_status():

    sql = """
        SELECT *
        FROM encoredb.task_execution_log
        ORDER BY run_start DESC
    """

    with get_conn() as conn:
        df = pd.read_sql(sql, conn)

    if df.empty:
        return df

    # Convert to datetime (assume DB stores UTC)
    df["run_start"] = pd.to_datetime(df["run_start"], utc=True, errors="coerce")
    df["run_end"] = pd.to_datetime(df["run_end"], utc=True, errors="coerce")

    # Keep only last 24 hours (Python side, not SQL side)
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)
    df = df[df["run_start"] >= cutoff]

    if df.empty:
        return df

    # Latest run per task
    latest = (
        df.sort_values("run_start", ascending=False)
          .groupby("task_name")
          .first()
          .reset_index()
    )

    now = pd.Timestamp.now(tz="UTC")

    latest["minutes_since_last_run"] = (
        (now - latest["run_start"]).dt.total_seconds() / 60
    ).round(1)

    # Health logic
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
# PLACEHOLDER SECURITY TAB (UNCHANGED)
# --------------------------------------------------

@st.cache_data(ttl=300)
def load_security_master_issues():
    sql = """
        SELECT ticker, name
        FROM encoredb.instruments
        LIMIT 0
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

# --------------------------------------------------
# UI
# --------------------------------------------------

st.title("🛡️ Encore Monitoring")

tabs = st.tabs([
    "🚨 Instruments Requiring Attention",
    "🖥 Task Monitoring"
])

# ==================================================
# TAB 1
# ==================================================
with tabs[0]:
    st.subheader("🚨 Instruments Requiring Attention")
    issues = load_security_master_issues()
    if issues.empty:
        st.success("✅ All instruments configured correctly.")
    else:
        st.dataframe(issues, use_container_width=True)

# ==================================================
# TAB 2 — TASK MONITORING
# ==================================================
with tabs[1]:

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
            - 🟠 STALE → Missed expected schedule (>6 minutes for 3-min job)  
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
