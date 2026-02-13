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
# ENTERPRISE TASK MONITORING
# --------------------------------------------------

@st.cache_data(ttl=60)
def load_task_status():

    sql = """
        WITH latest_exec AS (
            SELECT DISTINCT ON (task_name)
                task_name,
                status,
                run_start,
                run_end,
                runtime_seconds,
                rows_processed
            FROM encoredb.task_execution_log
            ORDER BY task_name, run_start DESC
        )

        SELECT
            r.task_name,
            r.enabled,
            r.last_run_time,
            r.next_run_time,
            r.last_task_result,
            e.status,
            e.run_start,
            e.run_end,
            e.runtime_seconds,
            e.rows_processed
        FROM encoredb.task_scheduler_registry r
        LEFT JOIN latest_exec e
            ON r.task_name = e.task_name
        ORDER BY r.task_name;
    """

    with get_conn() as conn:
        df = pd.read_sql(sql, conn)

    if df.empty:
        return df

    # ---------------------------------------
    # Convert CST timestamps → UTC
    # ---------------------------------------

    for col in ["run_start", "run_end", "last_run_time", "next_run_time"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        df[col] = (
            df[col]
            .dt.tz_localize("America/Chicago", nonexistent="NaT", ambiguous="NaT")
            .dt.tz_convert("UTC")
        )

    now = pd.Timestamp.utcnow()

    # ---------------------------------------
    # Enterprise Health Logic
    # ---------------------------------------

    def health(row):

        # Disabled
        if row["enabled"] == False:
            return "⚪ DISABLED"

        # Windows failure
        if row["last_task_result"] not in (0, None):
            return "🔴 WINDOWS FAILED"

        # Script failure
        if row["status"] == "FAILED":
            return "🔴 SCRIPT FAILED"

        # Script running
        if row["status"] == "RUNNING":
            return "🟡 RUNNING"

        # Windows ran but script not logging yet
        if pd.isnull(row["run_start"]) and pd.notnull(row["last_run_time"]):
            return "🟢 HEALTHY (WINDOWS)"

        # Only check missed schedule if script logging exists
        if pd.notnull(row["run_start"]) and pd.notnull(row["next_run_time"]):

            if (
                now > row["next_run_time"] + pd.Timedelta(minutes=2)
                and row["run_start"] < row["next_run_time"]
            ):
                return "🟠 MISSED SCHEDULE"

        return "🟢 HEALTHY"

    df["health"] = df.apply(health, axis=1)

    # Minutes since last script run (safe calculation)
    df["minutes_since_last_run"] = (
        (now - df["run_start"]).dt.total_seconds() / 60
    ).round(1)

    return df


# --------------------------------------------------
# UI
# --------------------------------------------------

st.title("🛡️ Encore Monitoring")

tabs = st.tabs(["🖥 Task Monitoring"])

with tabs[0]:

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
                    "enabled",
                    "status",
                    "last_task_result",
                    "run_start",
                    "run_end",
                    "runtime_seconds",
                    "rows_processed",
                    "last_run_time",
                    "next_run_time",
                    "minutes_since_last_run"
                ]
            ],
            use_container_width=True
        )

        st.markdown(
            """
            **Health Definitions**
            - 🟢 HEALTHY → Windows + Script OK  
            - 🟢 HEALTHY (WINDOWS) → Windows ran, script not logging  
            - 🟠 MISSED SCHEDULE → Script logging exists and missed next scheduled run  
            - 🔴 WINDOWS FAILED → Task Scheduler failure  
            - 🔴 SCRIPT FAILED → Python execution failure  
            - 🟡 RUNNING → Currently executing  
            - ⚪ DISABLED → Disabled in Windows Task Scheduler  
            """
        )

# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.caption(
    f"Data as of {date.today().isoformat()} • Encore Internal Monitoring"
)