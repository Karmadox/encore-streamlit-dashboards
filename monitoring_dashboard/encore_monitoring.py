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

@st.cache_data(ttl=300)
def load_cohort_lookup():
    sql = """
        SELECT cohort_code, cohort_name
        FROM encoredb.cohorts
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

@st.cache_data(ttl=300)
def load_instruments_by_cohort():

    sql = """
        SELECT
            c.cohort_code,
            c.cohort_name,
            i.ticker,
            w.weight_pct
        FROM encoredb.instrument_cohort_weights w
        JOIN encoredb.instruments i
            ON i.instrument_id = w.instrument_id
        JOIN encoredb.cohorts c
            ON c.cohort_id = w.cohort_id
        WHERE w.is_primary = TRUE
          AND i.active_flag = TRUE
    """

    with get_conn() as conn:
        return pd.read_sql(sql, conn)

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

@st.cache_data(ttl=300)
def load_encore_universe():

    sql = """
        SELECT
            s.sector_name   AS sector,
            c.cohort_name   AS cohort,
            i.ticker,
            i.name,
            w.weight_pct
        FROM encoredb.instrument_cohort_weights_current w
        JOIN encoredb.instruments i
          ON i.instrument_id = w.instrument_id
        JOIN encoredb.cohorts c
          ON c.cohort_id = w.cohort_id
        JOIN encoredb.sectors s
          ON s.sector_id = c.sector_id
        WHERE i.active_flag = TRUE
        ORDER BY
            s.sector_name,
            c.cohort_name,
            w.weight_pct DESC,
            i.ticker;
    """

    with get_conn() as conn:
        df = pd.read_sql(sql, conn)

    return df
    
# --------------------------------------------------
# ENTERPRISE TASK MONITORING
# --------------------------------------------------

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
    
    now = pd.Timestamp.utcnow()

    def health(row):

        if row["enabled"] == False:
            return "⚪ DISABLED"

        if row["last_task_result"] not in (0, None):
            if row["status"] == "SUCCESS":
                return "🟢 HEALTHY"
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
    "🖥 Task Monitoring",
    "📡 Signal Alerts",
    "🧠 Consumer Regime Monitor"   # 👈 NEW TAB
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
    # DOWNLOAD ENCORE UNIVERSE
    # --------------------------------------------------

    st.markdown("---")
    st.subheader("📥 Export")

    universe_df = load_encore_universe()

    if not universe_df.empty:

        csv_data = universe_df.to_csv(index=False).encode("utf-8")

        st.download_button(
            label="⬇ Download Encore Universe",
            data=csv_data,
            file_name=f"encore_universe_{date.today().isoformat()}.csv",
            mime="text/csv"
        )
    else:
        st.info("Universe currently empty.")
        
# --------------------------------------------------
# TAB 3
# --------------------------------------------------

with tabs[2]:

    st.subheader("🖥 Windows Task Monitoring")

    tasks = load_task_status()
    st.write(tasks)

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

@st.cache_data(ttl=60)
def load_signal_alerts():
    sql = """
        SELECT date, signal_name, alert_text, severity, created_at
        FROM encoredb_signals.signal_alerts
        ORDER BY created_at DESC
        LIMIT 100
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

with tabs[3]:

    st.subheader("📡 Active Signals")

    # --- EXPANDER (INTERPRETATION ONLY) ---
    with st.expander("🧭 How to interpret this tab", expanded=False):

        st.markdown("""
        This tab highlights **potential stress or regime shifts** in the consumer and macro environment.

        Alerts are **not trade signals**.  
        They indicate **where attention may be required**.

        ---
        ### **Signal Types**

        - ⛽ Gasoline Shock  
        - 🛒 Discretionary Stress  
        - 📉 Rates Pressure  
        - 📊 Volatility Regime Shift  

        ---
        ### **Severity**

        - 🔴 HIGH → regime-relevant  
        - 🟠 MEDIUM → early signal  

        ---
        ### **Thinking Framework**

        signal → persistence → transmission → impact
        """)

    # =========================
    # MAPPING FUNCTION
    # =========================

    def map_implication(signal_name):
        mapping = {
            "gasoline_shock": "Consumer disposable income under pressure → risk to low-income cohorts",
            "discretionary_stress": "Trade-down behaviour → pressure on discretionary retail",
            "rates_spike": "Tighter financial conditions → housing / credit-sensitive slowdown",
            "vol_regime": "Risk-off environment → broader demand uncertainty",
            "recession_search_spike": "Rising recession fear → sentiment deterioration",
            "recession_search_elevated": "Elevated macro concern → caution building",
            "recession_search_decline": "Macro concern easing → sentiment stabilising / improving"
        }
        return mapping.get(signal_name, "General monitoring signal")

    def map_cohort_codes(signal_name):
    
        # -------------------------
        # RULE-BASED MATCHING
        # -------------------------
    
        if "gasoline" in signal_name:
            return [
                "CONS_SERV_REST",
                "CONS_DISC_BROAD",
                "CONS_DISC_SPEC"
            ]
    
        elif "discretionary" in signal_name:
            return [
                "CONS_DISC_BROAD",
                "CONS_DISC_SPEC",
                "CONS_DISC_BRAND"
            ]
    
        elif "rates" in signal_name:
            return [
                "AUTO_MOBILITY",
                "CAPITAL_GOODS",
                "BANK_REGIONAL"
            ]
    
        elif "vol" in signal_name:
            return [
                "EQ_INDEX_BROAD",
                "VOLATILITY"
            ]
    
        elif "recession" in signal_name:
            return [
                "CONS_DISC_BROAD",
                "CONS_DISC_BRAND",
                "CONS_SERV_TRAVEL"
            ]
    
        return []
        
    # =========================
    # ALERTS
    # =========================
    
    alerts = load_signal_alerts()
    
    if alerts.empty:
        st.success("No active alerts.")
    
    else:
        # -----------------------------
        # ADD INTERPRETATION FIELDS FIRST
        # -----------------------------
    
        alerts["implication"] = alerts["signal_name"].apply(map_implication)
    
        cohorts_df = load_cohort_lookup()
        inst_df = load_instruments_by_cohort()
    
        def resolve_cohort_names(signal_name):
            codes = map_cohort_codes(signal_name)
    
            names = cohorts_df[
                cohorts_df["cohort_code"].isin(codes)
            ]["cohort_name"].tolist()
    
            return ", ".join(names) if names else "General market"
            
        def get_example_tickers(signal_name):

            codes = map_cohort_codes(signal_name)
        
            df = inst_df[
                inst_df["cohort_code"].isin(codes)
            ].copy()
        
            if df.empty:
                return "No mapped instruments"
        
            df = df.drop_duplicates("ticker")
        
            top = df.head(5)
        
            return ", ".join(top["ticker"].tolist())
    
        alerts["cohort_impact"] = alerts["signal_name"].apply(resolve_cohort_names)
        alerts["example_names"] = alerts["signal_name"].apply(get_example_tickers)
    
        # -----------------------------
        # DISPLAY (NOW SAFE)
        # -----------------------------
    
        st.warning(f"{len(alerts)} recent alerts")
    
        st.dataframe(
            alerts[
                [
                    "date",
                    "signal_name",
                    "severity",
                    "alert_text",
                    "cohort_impact",
                    "example_names",
                    "created_at"
                ]
            ],
            use_container_width=True
        )
    
    # =========================
    # NARRATIVE
    # =========================

    st.subheader("🧠 🧠 Current Environment")

    narrative = []
    for _, row in alerts.iterrows():
        if row["cohort_impact"] == "General market":
            continue  # skip weak signals

        narrative.append(
            f"- {row['alert_text']} → {row['cohort_impact']} → {row['example_names']}"
        )
    st.markdown("### Current Read:")
    st.markdown("\n".join(narrative))
    
    # =========================
    # LANGUAGE SIGNALS
    # =========================

    st.subheader("🔍 Language / Search Signals")

    @st.cache_data(ttl=60)
    def load_language_signals():
        sql = """
            SELECT
                timestamp,
                keyword,
                frequency,
                normalized_score,
                source
            FROM encoredb_signals.language_signals
            ORDER BY timestamp DESC
            LIMIT 200
        """
        with get_conn() as conn:
            return pd.read_sql(sql, conn)

    lang = load_language_signals()

    if lang.empty:
        st.info("No language signals available.")

    else:
        # -----------------------------
        # PREP
        # -----------------------------
        lang["timestamp"] = pd.to_datetime(lang["timestamp"])
        lang = lang.sort_values("timestamp")

        latest_rows = []

        for kw in lang["keyword"].unique():

            df_kw = lang[lang["keyword"] == kw].copy()

            df_kw["mean_4w"] = df_kw["normalized_score"].rolling(4).mean()
            df_kw["std_4w"] = df_kw["normalized_score"].rolling(4).std()

            df_kw["zscore"] = (
                (df_kw["normalized_score"] - df_kw["mean_4w"]) /
                df_kw["std_4w"]
            )

            df_kw["roc_4w"] = df_kw["normalized_score"].pct_change(4)

            latest = df_kw.iloc[-1]

            latest_rows.append({
                "keyword": kw,
                "level": latest["normalized_score"],
                "zscore": latest["zscore"],
                "roc_4w": latest["roc_4w"]
            })

        summary = pd.DataFrame(latest_rows)

        # -----------------------------
        # CLASSIFICATION
        # -----------------------------
        def classify(row):
            if pd.notnull(row["zscore"]) and row["zscore"] > 2:
                return "🔴 Spike"
            elif pd.notnull(row["zscore"]) and row["zscore"] > 1:
                return "🟠 Elevated"
            elif pd.notnull(row["roc_4w"]) and row["roc_4w"] > 0.3:
                return "🟡 Rising"
            else:
                return "🟢 Normal"

        summary["signal"] = summary.apply(classify, axis=1)

        # -----------------------------
        # DISPLAY
        # -----------------------------
        st.dataframe(summary, use_container_width=True)
        
# --------------------------------------------------
# TAB 5 – CONSUMER REGIME MONITOR
# --------------------------------------------------

@st.cache_data(ttl=60)
def load_latest_signals():
    sql = """
        SELECT *
        FROM encoredb_signals.signal_features
        ORDER BY date DESC
        LIMIT 1
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

with tabs[4]:

    st.subheader("🧠 Consumer Regime Monitor")

    # -----------------------------
    # INTERPRETATION GUIDE
    # -----------------------------

    with st.expander("🧭 How to interpret this dashboard", expanded=False):

        st.markdown("""
        This dashboard tracks **consumer stress across multiple domains**.

        ---
        **Signals**
        - Gasoline → income pressure
        - XLY/XLP → discretionary vs staples
        - Rates → financing conditions
        - VIX → uncertainty

        ---
        **Framework**
        signal → persistence → transmission → impact

        ---
        **Example**
        Gas ↑ + XLY ↓ →  
        → consumer stress  
        → discretionary slowdown  
        """)
        
    df = load_latest_signals()

    if df.empty:
        st.warning("No signal data available.")
        st.stop()

    row = df.iloc[0]

    gasoline_5d = row.get("gasoline_5d")
    xly_xlp_10d = row.get("xly_xlp_10d")
    rates_2y_10d = row.get("rates_2y_10d")
    vix_level = row.get("vix_level")

    # -----------------------------
    # REGIME CLASSIFICATION
    # -----------------------------

    regime = "⚪ Mixed / Unclear"

    if gasoline_5d is not None and xly_xlp_10d is not None:
        if gasoline_5d > 0.05 and xly_xlp_10d < -0.02:
            regime = "🔴 Consumer Stress Rising"
        elif gasoline_5d < 0 and xly_xlp_10d > 0:
            regime = "🟢 Consumer Relief"
        elif gasoline_5d > 0 and xly_xlp_10d < 0:
            regime = "🟠 Early Stress Signals"

    st.markdown(f"## {regime}")

    st.markdown("---")

    # -----------------------------
    # METRICS
    # -----------------------------

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("⛽ Gasoline 5d", f"{gasoline_5d:.2%}" if gasoline_5d else "—")
    col2.metric("🛒 XLY/XLP (10d)", f"{xly_xlp_10d:.2%}" if xly_xlp_10d else "—")
    col3.metric("📉 2Y Rates (10d)", f"{rates_2y_10d:.2%}" if rates_2y_10d else "—")
    col4.metric("📊 VIX Level", f"{vix_level:.2f}" if vix_level else "—")

    st.markdown("---")

    # -----------------------------
    # COHORT IMPACT
    # -----------------------------

    impacted = []

    if gasoline_5d is not None and gasoline_5d > 0.05:
        impacted += [
            "Fast Food (traffic pressure)",
            "Discount Retail (basket compression)",
            "Lower-income consumers"
        ]

    if xly_xlp_10d is not None and xly_xlp_10d < -0.02:
        impacted += [
            "Discretionary Retail",
            "Apparel / E-commerce",
            "Casual Dining"
        ]

    if rates_2y_10d is not None and rates_2y_10d > 0.03:
        impacted += [
            "Housing / Durables",
            "Autos"
        ]

    if vix_level is not None and vix_level > 20:
        impacted += [
            "Broad risk assets",
            "Consumer confidence"
        ]

    impacted = list(set(impacted))

    st.markdown("### 📦 Likely Impacted Cohorts")

    if impacted:
        for i in impacted:
            st.markdown(f"- {i}")
    else:
        st.markdown("No clear cohort stress signals.")

    st.markdown("---")

    # -----------------------------
    # LANGUAGE SNAPSHOT
    # -----------------------------

    st.markdown("### 🧠 Narrative Signals")
    
    @st.cache_data(ttl=300)
    def load_latest_language():
        sql = """
            SELECT keyword, normalized_score
            FROM encoredb_signals.language_signals
            WHERE timestamp = (
                SELECT MAX(timestamp)
                FROM encoredb_signals.language_signals
            )
        """
        with get_conn() as conn:
            return pd.read_sql(sql, conn)

    lang = load_latest_language()

    if lang.empty:
        st.info("No language signals yet.")
    else:
        for _, r in lang.iterrows():
            st.markdown(f"- {r['keyword']}: {r['normalized_score']:.1f}")

        
# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.caption(
    f"Data as of {date.today().isoformat()} • Encore Internal Monitoring"
)
