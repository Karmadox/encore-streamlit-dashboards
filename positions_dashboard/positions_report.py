import streamlit as st
import pandas as pd
import psycopg2
from streamlit_autorefresh import st_autorefresh

# -------------------------------------------------
# STREAMLIT CONFIG
# -------------------------------------------------
st.set_page_config(page_title="Encore Positions Dashboard", layout="wide")
st.title("📊 Encore – Positions Dashboard")

auto_refresh = st.checkbox("🔄 Auto-refresh every 5 minutes", value=True)
if auto_refresh:
    st_autorefresh(interval=5 * 60 * 1000, key="positions_refresh")

# -------------------------------------------------
# DATABASE CONNECTION
# -------------------------------------------------
def get_conn():
    return psycopg2.connect(**st.secrets["db"])

# -------------------------------------------------
# DATA LOADERS
# -------------------------------------------------
@st.cache_data(ttl=60)
def load_intraday():
    sql = """
        SELECT *
        FROM encoredb.positions_snapshot
        WHERE snapshot_date = CURRENT_DATE
        ORDER BY snapshot_ts
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

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
                AND w2.effective_date <= CURRENT_DATE
          )
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

# -------------------------------------------------
# MOVE BUCKETS + SYMBOLS
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

ARROW = {
    "> 3% up": "▲",
    "2–3% up": "▲",
    "1–2% up": "▲",
    "< 1% up": "▲",
    "< 1% down": "▼",
    "1–2% down": "▼",
    "2–3% down": "▼",
    "> 3% down": "▼",
}

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
# HEATMAP RENDERER (DARK MODE SAFE)
# -------------------------------------------------
def render_heatmap(df, title):
    if df.empty:
        st.info("No data available")
        return

    st.subheader(title)

    html = """
    <div style="background-color:white; padding:8px; border-radius:6px;">
    <table style="border-collapse:collapse; width:100%; font-size:14px;">
    <tr>
        <th style="text-align:left; padding:6px;">Name</th>
    """

    for c in df.columns:
        html += f"<th style='padding:6px; text-align:center;'>{c}</th>"
    html += "</tr>"

    for idx, row in df.iterrows():
        html += f"<tr><td style='padding:6px; font-weight:600;'>{idx}</td>"
        for val in row:
            bg = BUCKET_COLOR.get(val, "#ffffff")
            arrow = ARROW.get(val, "")
            html += (
                "<td style='padding:6px; text-align:center; "
                f"background:{bg}; color:#000; border:1px solid #ddd;'>"
                f"{arrow} {val}</td>"
            )
        html += "</tr>"

    html += "</table></div>"
    st.markdown(html, unsafe_allow_html=True)

# -------------------------------------------------
# LOAD & NORMALISE DATA
# -------------------------------------------------
intraday = load_intraday()
intraday["snapshot_ts"] = pd.to_datetime(intraday["snapshot_ts"])
intraday["time_label"] = intraday["snapshot_ts"].dt.tz_convert("US/Central").dt.strftime("%H:%M")

cst_today = pd.Timestamp.now(tz="US/Central").normalize()
intraday = intraday[intraday["snapshot_ts"].dt.tz_convert("US/Central") >= cst_today]

# Enfusion fixes
intraday["price_change_pct"] *= 100
intraday["effective_price_change_pct"] = intraday["price_change_pct"]
intraday.loc[intraday["quantity"] < 0, "effective_price_change_pct"] *= -1
intraday["move_bucket"] = intraday["effective_price_change_pct"].apply(classify_move)

latest_ts = intraday["snapshot_ts"].max()
latest = intraday[intraday["snapshot_ts"] == latest_ts]

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
        intraday
        .groupby(["snapshot_ts", "time_label", "egm_sector_v2"])
        .agg(
            pnl=("daily_pnl", "sum"),
            gross=("gross_notional", lambda x: x.abs().sum()),
        )
        .reset_index()
    )

    sector_ret["ret_pct"] = 100 * sector_ret["pnl"] / sector_ret["gross"].replace(0, pd.NA)
    sector_ret["bucket"] = sector_ret["ret_pct"].apply(classify_move)

    sector_matrix = sector_ret.pivot(
        index="egm_sector_v2", columns="time_label", values="bucket"
    ).sort_index()

    render_heatmap(sector_matrix, "🏭 Sector Heatmap")

# =================================================
# TAB 2 — PRICE CHANGE DRIVEN
# =================================================
with tab_price:
    st.header("📈 Price Change–Driven Analysis")

    bucket_table = (
        intraday
        .groupby(["time_label", "move_bucket"])
        .agg(names=("ticker", "nunique"))
        .reset_index()
        .pivot(index="move_bucket", columns="time_label", values="names")
        .fillna(0)
        .astype(int)
    )

    st.dataframe(bucket_table, width="stretch")