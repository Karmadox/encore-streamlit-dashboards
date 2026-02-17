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
# DATA LOADERS – SECURITY MASTER
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


# 🔥 OPTIMIZED + CORRECT PRIMARY LOGIC
@st.cache_data(ttl=300)
def load_instruments_for_cohort(cohort_id):

    sql = """
        WITH latest_weights AS (
            SELECT *
            FROM encoredb.instrument_cohort_weights
            WHERE cohort_id = %s
              AND effective_date = (
                  SELECT MAX(effective_date)
                  FROM encoredb.instrument_cohort_weights
                  WHERE cohort_id = %s
              )
        ),
        primary_flags AS (
            SELECT instrument_id
            FROM encoredb.instrument_cohort_weights
            WHERE cohort_id = %s
              AND is_primary = TRUE
        )

        SELECT
            i.ticker,
            i.name,
            w.weight_pct,
            CASE
                WHEN p.instrument_id IS NOT NULL THEN TRUE
                ELSE FALSE
            END AS is_primary,
            w.effective_date,
            w.source
        FROM latest_weights w
        JOIN encoredb.instruments i
          ON i.instrument_id = w.instrument_id
        LEFT JOIN primary_flags p
          ON p.instrument_id = w.instrument_id
        ORDER BY is_primary DESC, w.weight_pct DESC, i.ticker;
    """

    with get_conn() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params=(
                sql_param(cohort_id),
                sql_param(cohort_id),
                sql_param(cohort_id),
            ),
        )

    # Ensure Streamlit renders checkbox correctly
    if "is_primary" in df.columns:
        df["is_primary"] = df["is_primary"].fillna(False).astype(bool)

    return df


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

    for col in ["run_start", "run_end", "last_run_time", "next_run_time"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        df[col] = (
            df[col]
            .dt.tz_localize("America/Chicago", nonexistent="NaT", ambiguous="NaT")
            .dt.tz_convert("UTC")
        )

    now = pd.Timestamp.utcnow()

    def health(row):

        if row["enabled"] == False:
            return "⚪ DISABLED"

        if row["last_task_result"] not in (0, None):
            return "🔴 WINDOWS FAILED"

        if row["status"] == "FAILED":
            return "🔴 SCRIPT FAILED"

        if row["status"] == "RUNNING":
            return "🟡 RUNNING"

        if pd.isnull(row["run_start"]) and pd.notnull(row["last_run_time"]):
            return "🟢 HEALTHY (WINDOWS)"

        if pd.notnull(row["run_start"]) and pd.notnull(row["next_run_time"]):
            if (
                now > row["next_run_time"] + pd.Timedelta(minutes=2)
                and row["run_start"] < row["next_run_time"]
            ):
                return "🟠 MISSED SCHEDULE"

        return "🟢 HEALTHY"

    df["health"] = df.apply(health, axis=1)

    df["minutes_since_last_run"] = (
        (now - df["run_start"]).dt.total_seconds() / 60
    ).round(1)

    return df

# --------------------------------------------------
# UI
# --------------------------------------------------

st.title("🛡️ Encore Monitoring")

tabs = st.tabs([
    "🚨 Instruments Requiring Attention",
    "🏭 Sector → Cohort → Instruments",
    "🖥 Task Monitoring"
])

# --------------------------------------------------
# TAB 1
# --------------------------------------------------

with tabs[0]:

    st.subheader("🚨 Instruments Requiring Attention")

    issues = load_security_master_issues()

    if issues.empty:
        st.success("✅ All instruments have valid sector & cohort assignments.")
    else:
        st.warning(f"⚠ {len(issues)} instruments require attention")
        st.dataframe(issues, use_container_width=True)

# --------------------------------------------------
# TAB 2
# --------------------------------------------------

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

# --------------------------------------------------
# TAB 3
# --------------------------------------------------

with tabs[2]:

    st.subheader("🖥 Windows Task Monitoring")

    tasks = load_task_status()

    if tasks.empty:
        st.warning("No task executions found.")
    else:

        display_df = tasks.copy()

        # 🔥 UI-ONLY conversion to CST/CDT
        display_timezone = "America/Chicago"

        time_cols = [
            "run_start",
            "run_end",
            "last_run_time",
            "next_run_time"
        ]

        for col in time_cols:
            if col in display_df.columns:
                display_df[col] = pd.to_datetime(display_df[col], errors="coerce")

                # If timezone-aware → convert
                if display_df[col].dt.tz is not None:
                    display_df[col] = display_df[col].dt.tz_convert(display_timezone)
                else:
                    # If naive → assume UTC and convert
                    display_df[col] = (
                        display_df[col]
                        .dt.tz_localize("UTC")
                        .dt.tz_convert(display_timezone)
                    )

        st.dataframe(
            display_df[
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