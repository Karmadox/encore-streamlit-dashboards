import streamlit as st
import pandas as pd
import psycopg2
from streamlit_autorefresh import st_autorefresh
from datetime import date

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

selected_date = st.selectbox(
    "📅 Select snapshot date",
    available_dates,
    index=0,  # latest date by default
)

is_today = selected_date == date.today()

auto_refresh = st.checkbox(
    "🔄 Auto-refresh every 5 minutes",
    value=is_today,
    disabled=not is_today,
)

if auto_refresh and is_today:
    st_autorefresh(interval=5 * 60 * 1000, key="positions_refresh")

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
                AND w2.effective_date <= %s
          )
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(selected_date,))

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

ARROW = {k: ("▲" if "up" in k else "▼") for k in BUCKET_ORDER}

BUCKET_COLOR = {
    "> 3% up": "#1a7f37",
    "2–3% up": "#4caf50",
    "1–2% up": "#8bc34a",
    "< 1% up": "#e8f5e9",
    "< 1% down": "#fdecea",
    "1–2% down": "#f28b82",
    "2–3% down": "#e57373",
    "> 3% down": "#c62828",
}

# -------------------------------------------------
# HEATMAP RENDERER
# -------------------------------------------------
def render_heatmap(df, title):
    st.subheader(title)
    html = "<div style='background:white; padding:8px; border-radius:6px;'>"
    html += "<table style='border-collapse:collapse; width:100%;'>"
    html += "<tr><th style='text-align:left;'>Name</th>"
    for c in df.columns:
        html += f"<th style='text-align:center;'>{c}</th>"
    html += "</tr>"

    for idx, row in df.iterrows():
        html += f"<tr><td style='font-weight:600;'>{idx}</td>"
        for val in row:
            color = BUCKET_COLOR.get(val, "#fff")
            arrow = ARROW.get(val, "")
            html += (
                f"<td style='background:{color}; text-align:center; "
                f"border:1px solid #ddd; color:#000;'>"
                f"{arrow} {val}</td>"
            )
        html += "</tr>"
    html += "</table></div>"
    st.markdown(html, unsafe_allow_html=True)

# -------------------------------------------------
# LOAD & NORMALISE DATA
# -------------------------------------------------
intraday = load_intraday(selected_date)

if intraday.empty:
    st.warning("No data available for selected date.")
    st.stop()

intraday["snapshot_ts"] = pd.to_datetime(intraday["snapshot_ts"])
intraday["time_label"] = (
    intraday["snapshot_ts"]
    .dt.tz_convert("US/Central")
    .dt.strftime("%H:%M")
)

# Enfusion fixes
intraday["price_change_pct"] *= 100
intraday["effective_price_change_pct"] = intraday["price_change_pct"]
intraday.loc[intraday["quantity"] < 0, "effective_price_change_pct"] *= -1
intraday["move_bucket"] = intraday["effective_price_change_pct"].apply(classify_move)

latest = intraday[intraday["snapshot_ts"] == intraday["snapshot_ts"].max()]

# -------------------------------------------------
# TABS
# -------------------------------------------------
tab_sector, tab_price = st.tabs(["🏭 Sector Driven", "📈 Price Change Driven"])

# =================================================
# TAB 1 — SECTOR DRIVEN
# =================================================
with tab_sector:
    st.header("🏭 Sector-Driven Intraday Performance")

    sector_ret = (
        intraday.groupby(["snapshot_ts", "time_label", "egm_sector_v2"])
        .agg(
            pnl=("daily_pnl", "sum"),
            gross=("gross_notional", lambda x: x.abs().sum()),
        )
        .reset_index()
    )

    sector_ret["ret_pct"] = (
        100 * sector_ret["pnl"] / sector_ret["gross"].replace(0, pd.NA)
    )
    sector_ret["bucket"] = sector_ret["ret_pct"].apply(classify_move)

    sector_matrix = sector_ret.pivot(
        index="egm_sector_v2",
        columns="time_label",
        values="bucket"
    ).sort_index()

    render_heatmap(sector_matrix, "🏭 Sector Heatmap")

    sel_sector = st.selectbox("🔎 Select Sector", sector_matrix.index)

    if sel_sector != "Comm/Tech":
        st.subheader("📋 Instrument Detail")
        st.dataframe(
            latest[latest["egm_sector_v2"] == sel_sector][
                ["ticker", "description", "quantity",
                 "effective_price_change_pct", "nmv"]
            ].sort_values("effective_price_change_pct"),
            width="stretch",
        )
    else:
        cohorts = load_commtech_cohorts()
        ct = intraday.merge(cohorts, on="ticker", how="inner")

        cohort_ret = (
            ct.groupby(["snapshot_ts", "time_label", "cohort_name"])
            .agg(
                pnl=("daily_pnl", "sum"),
                gross=("gross_notional", lambda x: x.abs().sum()),
            )
            .reset_index()
        )

        cohort_ret["ret_pct"] = (
            100 * cohort_ret["pnl"] / cohort_ret["gross"].replace(0, pd.NA)
        )
        cohort_ret["bucket"] = cohort_ret["ret_pct"].apply(classify_move)

        cohort_matrix = cohort_ret.pivot(
            index="cohort_name",
            columns="time_label",
            values="bucket"
        ).sort_index()

        render_heatmap(cohort_matrix, "🧩 Comm/Tech Cohort Heatmap")

        sel_cohort = st.selectbox("Select Cohort", cohort_matrix.index)
        cohort_latest = latest.merge(cohorts, on="ticker").query(
            "cohort_name == @sel_cohort"
        )

        st.subheader(f"📋 Instrument Detail — {sel_cohort}")
        st.dataframe(
            cohort_latest[
                ["ticker", "description", "quantity",
                 "effective_price_change_pct", "nmv",
                 "weight_pct", "is_primary"]
            ].sort_values("effective_price_change_pct"),
            width="stretch",
        )

# =================================================
# TAB 2 — PRICE CHANGE DRIVEN
# =================================================
with tab_price:
    st.header("📈 Price Change–Driven Analysis")

    bucket_table = (
        intraday.groupby(["time_label", "move_bucket"])
        .agg(names=("ticker", "nunique"))
        .reset_index()
        .pivot(index="move_bucket", columns="time_label", values="names")
        .reindex(BUCKET_ORDER)
        .fillna(0)
        .astype(int)
    )
    st.dataframe(bucket_table, width="stretch")

    sel_bucket = st.selectbox("Select Price-Move Bucket", BUCKET_ORDER)
    bucket_df = latest[latest["move_bucket"] == sel_bucket]

    sector_view = (
        bucket_df.groupby("egm_sector_v2")
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

    sel_sector = st.selectbox("Select Sector", sector_view["egm_sector_v2"])
    sector_df = bucket_df[bucket_df["egm_sector_v2"] == sel_sector]

    if sel_sector == "Comm/Tech":
        cohorts = load_commtech_cohorts()
        ct_df = sector_df.merge(cohorts, on="ticker", how="inner")

        cohort_view = (
            ct_df.groupby("cohort_name")
            .agg(
                names=("ticker", "nunique"),
                net_nmv=("nmv", "sum"),
                avg_move=("effective_price_change_pct", "mean"),
            )
            .reset_index()
            .sort_values("net_nmv", ascending=False)
        )

        st.subheader("🧩 Comm/Tech — Cohort Breakdown")
        st.dataframe(cohort_view, width="stretch")

        sel_cohort = st.selectbox("Select Cohort", cohort_view["cohort_name"])
        sector_df = ct_df[ct_df["cohort_name"] == sel_cohort]

    st.subheader("📋 Instrument Detail")
    st.dataframe(
        sector_df[
            ["ticker", "description", "quantity",
             "effective_price_change_pct", "nmv"]
        ].sort_values("effective_price_change_pct"),
        width="stretch",
    )