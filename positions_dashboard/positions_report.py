import streamlit as st
import pandas as pd
import psycopg2
from streamlit_autorefresh import st_autorefresh
from datetime import date
import streamlit.components.v1 as components

# -------------------------------------------------
# STREAMLIT CONFIG
# -------------------------------------------------
st.set_page_config(page_title="Encore Positions Dashboard", layout="wide")
st.title("📊 Encore – Positions Dashboard")

# -------------------------------------------------
# DATABASE CONNECTION
# -------------------------------------------------
def get_conn():
    return psycopg2.connect(**st.secrets["db"])

TIME_BUCKETS = [
    f"{h:02d}:{m:02d}"
    for h in range(9, 15)
    for m in (0, 30)
] + ["15:00"]

# -------------------------------------------------
# DATE HANDLING
# -------------------------------------------------
@st.cache_data(ttl=300)
def load_available_dates():
    sql = """
        SELECT DISTINCT snapshot_date
        FROM encoredb.positions_snapshot
        ORDER BY snapshot_date DESC
    """
    with get_conn() as conn:
        df = pd.read_sql(sql, conn)
    return df["snapshot_date"].tolist()

available_dates = load_available_dates()
if not available_dates:
    st.error("No snapshot data available.")
    st.stop()

selected_date = st.selectbox("📅 Select snapshot date", available_dates, index=0)

is_today = selected_date == date.today()
if st.checkbox("🔄 Auto-refresh every 5 minutes", value=is_today, disabled=not is_today):
    st_autorefresh(interval=5 * 60 * 1000, key="positions_refresh")

def safe_select(df, cols):
    """Return only columns that actually exist in df"""
    return df[[c for c in cols if c in df.columns]]

def safe_sort(df, preferred_cols):
    """
    Sort df by the first available column in preferred_cols.
    If none exist, return df unchanged.
    """
    for col in preferred_cols:
        if col in df.columns:
            return df.sort_values(col)
    return df

def safe_sort(df, preferred_col):
    if df.empty:
        return df
    if preferred_col in df.columns:
        return df.sort_values(preferred_col)
    return df.sort_values(df.columns[0])

# -------------------------------------------------
# DATA LOADERS
# -------------------------------------------------
@st.cache_data(ttl=60)
def load_intraday(snapshot_date):
    sql = """
        SELECT *
        FROM encoredb.positions_snapshot
        WHERE snapshot_date = %s
        ORDER BY snapshot_ts
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(snapshot_date,))

@st.cache_data(ttl=300)
def load_cohorts_for_sector(sector_name, as_of_date):
    sql = """
        SELECT
            i.ticker,
            c.cohort_name,
            w.weight_pct,
            w.is_primary
        FROM encoredb.instrument_cohort_weights w
        JOIN encoredb.cohorts c ON w.cohort_id = c.cohort_id
        JOIN encoredb.sectors s ON c.sector_id = s.sector_id
        JOIN encoredb.instruments i ON w.instrument_id = i.instrument_id
        WHERE s.sector_name = %s
          AND w.effective_date = (
              SELECT MAX(w2.effective_date)
              FROM encoredb.instrument_cohort_weights w2
              WHERE w2.instrument_id = w.instrument_id
                AND w2.cohort_id = w.cohort_id
                AND w2.effective_date <= %s
          )
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(sector_name, as_of_date))

@st.cache_data(ttl=300)
def sector_has_cohorts(sector_name):
    sql = """
        SELECT COUNT(*) > 0
        FROM encoredb.cohorts c
        JOIN encoredb.sectors s ON c.sector_id = s.sector_id
        WHERE s.sector_name = %s
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(sector_name,)).iloc[0, 0]

@st.cache_data(ttl=600)
def load_intraday_history():
    sql = """
        SELECT *
        FROM encoredb.positions_snapshot
        WHERE EXTRACT(ISODOW FROM snapshot_date) BETWEEN 1 AND 5
        ORDER BY snapshot_date, snapshot_ts
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

@st.cache_data(ttl=300)
def load_daily_eod():
    sql = """
        SELECT
            e.snapshot_date,
            e.snapshot_ts,
            i.ticker,
            i.name AS description,

            -- position metrics
            e.quantity,
            e.gross_notional,
            e.net_notional,
            e.pnl_day,
            e.effective_price_change_pct,

            -- directional state
            e.dir_short,
            e.dir_medium,
            e.dir_structural,
            e.alignment_flag,
            e.confidence_level,

            -- sector (derived via cohorts)
            s.sector_name AS egm_sector_v2

        FROM encoredb.positions_eod_snapshot e
        JOIN encoredb.instruments i
          ON e.instrument_id = i.instrument_id

        -- sector via primary cohort
        LEFT JOIN encoredb.instrument_cohort_weights w
          ON w.instrument_id = i.instrument_id
         AND w.is_primary = true

        LEFT JOIN encoredb.cohorts c
          ON c.cohort_id = w.cohort_id

        LEFT JOIN encoredb.sectors s
          ON s.sector_id = c.sector_id

        WHERE EXTRACT(ISODOW FROM e.snapshot_date) BETWEEN 1 AND 5
        ORDER BY e.snapshot_date
    """

    with get_conn() as conn:
        return pd.read_sql(sql, conn)

# -------------------------------------------------
# MOVE BUCKETS
# -------------------------------------------------
def classify_move(x):
    if pd.isna(x): return "< 1% up"
    if x > 3: return "> 3% up"
    if 2 < x <= 3: return "2–3% up"
    if 1 < x <= 2: return "1–2% up"
    if 0 <= x <= 1: return "< 1% up"
    if -1 <= x < 0: return "< 1% down"
    if -2 <= x < -1: return "1–2% down"
    if -3 <= x < -2: return "2–3% down"
    return "> 3% down"

BUCKET_ORDER = [
    "> 3% up", "2–3% up", "1–2% up", "< 1% up",
    "< 1% down", "1–2% down", "2–3% down", "> 3% down",
]

BUCKET_SYMBOL = {
    "> 3% up": "▲▲▲▲",
    "2–3% up": "▲▲▲",
    "1–2% up": "▲▲",
    "< 1% up": "▲",
    "< 1% down": "▼",
    "1–2% down": "▼▼",
    "2–3% down": "▼▼▼",
    "> 3% down": "▼▼▼▼",
}

BUCKET_COLOR = {
    "> 3% up": "#1a7f37",
    "2–3% up": "#2e7d32",
    "1–2% up": "#388e3c",
    "< 1% up": "#4caf50",
    "< 1% down": "#e53935",
    "1–2% down": "#d32f2f",
    "2–3% down": "#c62828",
    "> 3% down": "#b71c1c",
}

# -------------------------------------------------
# HEATMAP
# -------------------------------------------------
def render_heatmap(df, title):
    st.markdown(f"**{title}**")

    if df.empty:
        st.info("No data available.")
        return

    html = """
    <div style="
        background:white;
        padding:6px;
        border-radius:6px;
        overflow-x:auto;
        overflow-y:auto;
        max-height:420px;
        border:1px solid #eee;
    ">
      <table style="
        width:max-content;
        min-width:100%;
        border-collapse:collapse;
        font-size:12px;
      ">
        <thead>
          <tr>
            <th style="text-align:left;padding:4px;position:sticky;left:0;background:white;z-index:2;">
              Name
            </th>
    """

    for c in df.columns:
        html += f"""
        <th style="
            padding:4px;
            text-align:center;
            white-space:nowrap;
        ">{c}</th>
        """

    html += "</tr></thead><tbody>"

    for idx, row in df.iterrows():
        html += f"""
        <tr>
          <td style="
              padding:4px;
              font-weight:600;
              position:sticky;
              left:0;
              background:white;
              z-index:1;
          ">{idx}</td>
        """

        for v in row:
            html += f"""
            <td style="
                padding:4px;
                text-align:center;
                color:{BUCKET_COLOR.get(v,'#000')};
                white-space:nowrap;
            ">
                {BUCKET_SYMBOL.get(v,'')}
            </td>
            """

        html += "</tr>"

    html += "</tbody></table></div>"

    components.html(html, height=450, scrolling=False)
    
# -------------------------------------------------
# LOAD DATA
# -------------------------------------------------
intraday = load_intraday(selected_date)

from datetime import time

TRADING_START = time(9, 0)
TRADING_END   = time(15, 59, 59)

intraday["snapshot_ts"] = pd.to_datetime(intraday["snapshot_ts"], utc=True)

# Convert once to CST (authoritative timestamp)
intraday["snapshot_cst"] = intraday["snapshot_ts"].dt.tz_convert("US/Central")

# Filter to selected CST date
intraday = intraday[
    intraday["snapshot_cst"].dt.date == selected_date
].copy()

# Filter to regular trading hours (09:00–15:00 CST)
intraday = intraday[
    intraday["snapshot_cst"].dt.time.between(
        TRADING_START,
        TRADING_END
    )
].copy()

# Fixed 30-minute buckets for heatmaps
intraday["time_label"] = (
    intraday["snapshot_cst"]
    .dt.floor("30min")
    .dt.strftime("%H:%M")
)

intraday["effective_price_change_pct"] = intraday["price_change_pct"] * 100
intraday.loc[intraday["quantity"] < 0, "effective_price_change_pct"] *= -1
intraday["move_bucket"] = intraday["effective_price_change_pct"].apply(classify_move)

# -------------------------------------------------
# DEFINE LATEST SNAPSHOT *WITHIN TRADING HOURS*
# -------------------------------------------------
latest_ts = intraday["snapshot_cst"].max()

latest = intraday[
    intraday["snapshot_cst"] == latest_ts
].copy()

# -------------------------------------------------
# TABS
# -------------------------------------------------
tab_sector, tab_daily, tab_price = st.tabs(["🏭 Sector Driven", "📆 Daily Sector Driven", "📈 Price Change Driven"])

# =================================================
# TAB 1 — SECTOR DRIVEN
# =================================================
with tab_sector:

    st.markdown("## 🏭 Sector-Driven Intraday Performance")

    # -------------------------------
    # LEGEND / METHODOLOGY
    # -------------------------------
    with st.expander("ℹ️ How to read this view", expanded=False):
        st.markdown(
            """
            **Arrow legend**

            <span style="color:#1a7f37; font-weight:700;">▲</span> Up less than 1%  
            <span style="color:#1a7f37; font-weight:700;">▲▲</span> Up 1–2%  
            <span style="color:#1a7f37; font-weight:700;">▲▲▲</span> Up 2–3%  
            <span style="color:#1a7f37; font-weight:700;">▲▲▲▲</span> Up more than 3%  

            <span style="color:#c62828; font-weight:700;">▼</span> Down less than 1%  
            <span style="color:#c62828; font-weight:700;">▼▼</span> Down 1–2%  
            <span style="color:#c62828; font-weight:700;">▼▼▼</span> Down 2–3%  
            <span style="color:#c62828; font-weight:700;">▼▼▼▼</span> Down more than 3%  

            ---

            **How sector and cohort movements are calculated**

            **Σ Daily P&L ÷ Σ |Gross Notional|**

            - Correctly accounts for long and short positions  
            - Treats shorts as benefiting from price declines  
            - Weights by economic exposure  
            """,
            unsafe_allow_html=True,
        )

    # -------------------------------
    # SECTOR HEATMAP + SELECTOR
    # -------------------------------
    with st.container():
        sector_ret = (
            intraday
            .groupby(["snapshot_ts", "time_label", "egm_sector_v2"])
            .agg(
                pnl=("daily_pnl", "sum"),
                gross=("gross_notional", lambda x: x.abs().sum()),
            )
            .reset_index()
        )

        sector_ret["bucket"] = (
            100 * sector_ret["pnl"] / sector_ret["gross"]
        ).apply(classify_move)

        TIME_GRID = TIME_BUCKETS

        sector_matrix = (
            sector_ret
            .pivot(index="egm_sector_v2", columns="time_label", values="bucket")
            .reindex(columns=TIME_GRID)
        )

        render_heatmap(sector_matrix, "🏭 Sector Heatmap")

        sel_sector = st.selectbox(
            "Select Sector",
            sector_matrix.index,
            key="intraday_sector_select",
        )

    # -------------------------------
    # COHORT / INSTRUMENT VIEW
    # -------------------------------
    with st.container():

        if sector_has_cohorts(sel_sector):

            cohorts = load_cohorts_for_sector(sel_sector, selected_date)
            ct = intraday.merge(cohorts, on="ticker", how="inner")

            cohort_ret = (
                ct.groupby(["snapshot_ts", "time_label", "cohort_name"])
                .agg(
                    pnl=("daily_pnl", "sum"),
                    gross=("gross_notional", lambda x: x.abs().sum()),
                )
                .reset_index()
            )

            cohort_ret["bucket"] = (
                100 * cohort_ret["pnl"] / cohort_ret["gross"]
            ).apply(classify_move)

            cohort_matrix = (
                cohort_ret
                .pivot(index="cohort_name", columns="time_label", values="bucket")
                .reindex(columns=TIME_GRID)
            )

            render_heatmap(cohort_matrix, f"🧩 {sel_sector} — Cohorts")

            sel_cohort = st.selectbox(
                "Select Cohort",
                cohort_matrix.index,
                key="intraday_cohort_select",
            )

            cohort_latest = (
                latest.merge(cohorts, on="ticker", how="inner")
                .query("cohort_name == @sel_cohort")
            )

            st.markdown(f"**📋 Instrument Detail — {sel_cohort}**")

            df = safe_select(
                cohort_latest,
                [
                    "ticker",
                    "description",
                    "quantity",
                    "effective_price_change_pct",
                    "nmv",
                    "weight_pct",
                    "is_primary",
                ],
            )

            st.dataframe(
                safe_sort(df, "effective_price_change_pct"),
                width="stretch",
            )

        else:
            st.markdown(f"**📋 Instrument Detail — {sel_sector}**")

            df = safe_select(
                latest[latest["egm_sector_v2"] == sel_sector],
                [
                    "ticker",
                    "description",
                    "quantity",
                    "effective_price_change_pct",
                    "nmv",
                ],
            )

            st.dataframe(
                safe_sort(df, "effective_price_change_pct"),
                width="stretch",
            )

# =================================================
# TAB 2 — DAILY SECTOR-DRIVEN PERFORMANCE (EOD)
# =================================================
with tab_daily:

    st.markdown("## 📆 Daily Sector-Driven Performance")

    with st.expander("ℹ️ How this view is calculated", expanded=False):
        st.markdown(
            """
            **Daily movement definition**

            **Σ Daily P&L ÷ Σ |Gross Notional|**

            - Uses end-of-day snapshots only  
            - One authoritative value per trading day  
            - Correctly handles long and short positions  
            - Stable historical record (no recomputation drift)  
            """
        )

    # -------------------------------
    # LOAD EOD DATA
    # -------------------------------
    daily = load_daily_eod()

    if daily.empty:
        st.info("No daily EOD data available.")
        st.stop()

    # -------------------------------
    # DATE WINDOW (SCROLL CONTROL)
    # -------------------------------
    all_dates = sorted(daily["snapshot_date"].unique())
    default_window = min(20, len(all_dates))

    window = st.slider(
        "Number of trading days to display",
        min_value=5,
        max_value=len(all_dates),
        value=default_window,
        key="daily_window_slider",
    )

    visible_dates = all_dates[-window:]

    # -------------------------------
    # DAILY SECTOR HEATMAP + SELECTOR
    # -------------------------------
    with st.container():

        sector_daily = compute_daily_sector_buckets(daily)

        sector_matrix = (
            sector_daily
            .pivot(
                index="egm_sector_v2",
                columns="snapshot_date",
                values="bucket",
            )
            .reindex(columns=visible_dates)
        )

        render_heatmap(sector_matrix, "📆 Daily Sector Trend")

        sel_sector = st.selectbox(
            "Select Sector (Daily)",
            sector_matrix.index,
            key="daily_sector_select",
        )

    latest_day = max(visible_dates)

    # -------------------------------
    # COHORT / INSTRUMENT VIEW
    # -------------------------------
    with st.container():

        sector_rows = daily[
            (daily["egm_sector_v2"] == sel_sector)
            & (daily["snapshot_date"] == latest_day)
        ].copy()

        if sector_has_cohorts(sel_sector):

            cohorts = load_cohorts_for_sector(sel_sector, latest_day)
            ct = sector_rows.merge(cohorts, on="ticker", how="inner")

            # ---------------------------
            # COHORT DAILY HEATMAP
            # ---------------------------
            cohort_daily = (
                daily.merge(cohorts, on="ticker", how="inner")
                .groupby(["snapshot_date", "cohort_name"])
                .agg(
                    pnl=("pnl_day", "sum"),
                    gross=("gross_notional", lambda x: x.abs().sum()),
                )
                .reset_index()
            )

            cohort_daily["ret_pct"] = 100 * cohort_daily["pnl"] / cohort_daily["gross"]
            cohort_daily["bucket"] = cohort_daily["ret_pct"].apply(classify_move)

            cohort_matrix = (
                cohort_daily
                .pivot(
                    index="cohort_name",
                    columns="snapshot_date",
                    values="bucket",
                )
                .reindex(columns=visible_dates)
            )

            render_heatmap(cohort_matrix, f"📆 {sel_sector} — Daily Cohorts")

            sel_cohort = st.selectbox(
                "Select Cohort (Daily)",
                cohort_matrix.index,
                key="daily_cohort_select",
            )

            instrument_rows = ct[ct["cohort_name"] == sel_cohort]

            st.markdown(
                f"**📋 Instrument Contribution — {sel_cohort} ({latest_day})**"
            )

            df = safe_select(
                instrument_rows,
                [
                    "ticker",
                    "description",
                    "quantity",
                    "effective_price_change_pct",
                    "net_notional",
                    "weight_pct",
                    "is_primary",
                ],
            )

            st.dataframe(
                safe_sort(df, "effective_price_change_pct"),
                width="stretch",
            )

        else:
            st.markdown(
                f"**📋 Instrument Contribution — {sel_sector} ({latest_day})**"
            )

            df = safe_select(
                sector_rows,
                [
                    "ticker",
                    "description",
                    "quantity",
                    "effective_price_change_pct",
                    "net_notional",
                ],
            )

            st.dataframe(
                safe_sort(df, "effective_price_change_pct"),
                width="stretch",
            )

# ============================
# TAB 3 — PRICE CHANGE
# ============================
with tab_price:

    st.header("📈 Price Change–Driven Analysis")

    # --------------------------------
    # PRICE MOVE DISTRIBUTION
    # --------------------------------
    bucket_table = (
    intraday
    .groupby(["time_label", "move_bucket"])
    .agg(names=("ticker", "nunique"))
    .reset_index()
    .pivot(index="move_bucket", columns="time_label", values="names")
    .reindex(index=BUCKET_ORDER, columns=TIME_GRID)
    .fillna(0)
    .astype(int)
    )
    
    st.dataframe(bucket_table, width="stretch")

    sel_bucket = st.selectbox("Select Price Bucket", BUCKET_ORDER)
    bucket_df = latest[latest["move_bucket"] == sel_bucket]

    # --------------------------------
    # SECTOR BREAKDOWN
    # --------------------------------
    sector_view = (
        bucket_df
        .groupby("egm_sector_v2")
        .agg(
            names=("ticker", "nunique"),
            net_nmv=("nmv", "sum"),
            avg_move=("effective_price_change_pct", "mean"),
        )
        .reset_index()
        .sort_values("net_nmv", ascending=False)
    )

    st.subheader(f"🏭 Sector Breakdown — {sel_bucket}")
    st.dataframe(sector_view, width="stretch")

    sel_sector = st.selectbox(
        "Select Sector",
        sector_view["egm_sector_v2"],
        key="price_sector_select",
    )

    sector_df = bucket_df[bucket_df["egm_sector_v2"] == sel_sector]

    # --------------------------------
    # SECTOR WITH COHORTS
    # --------------------------------
    if sector_has_cohorts(sel_sector):
        cohorts = load_cohorts_for_sector(sel_sector, selected_date)
        ct_df = sector_df.merge(cohorts, on="ticker", how="inner")

        cohort_view = (
            ct_df
            .groupby("cohort_name")
            .agg(
                names=("ticker", "nunique"),
                net_nmv=("nmv", "sum"),
                avg_move=("effective_price_change_pct", "mean"),
            )
            .reset_index()
            .sort_values("net_nmv", ascending=False)
        )

        st.subheader("🧩 Cohort Breakdown")
        st.dataframe(cohort_view, width="stretch")

        sel_cohort = st.selectbox(
            "Select Cohort",
            cohort_view["cohort_name"],
            key="price_cohort_select",
        )

        instrument_df = ct_df[ct_df["cohort_name"] == sel_cohort]

        df = safe_select(
            instrument_df,
            [
                "ticker",
                "description",
                "quantity",
                "effective_price_change_pct",
                "nmv",
                "weight_pct",
                "is_primary",
            ],
        )

        st.subheader(f"📋 Instrument Detail — {sel_cohort}")
        st.dataframe(
            safe_sort(df, "effective_price_change_pct"),
            width="stretch",
        )

    # --------------------------------
    # SECTOR WITHOUT COHORTS
    # --------------------------------
    else:
        df = safe_select(
            sector_df,
            [
                "ticker",
                "description",
                "quantity",
                "effective_price_change_pct",
                "nmv",
            ],
        )

        st.subheader(f"📋 Instrument Detail — {sel_sector}")
        st.dataframe(
            safe_sort(df, "effective_price_change_pct"),
            width="stretch",
        )