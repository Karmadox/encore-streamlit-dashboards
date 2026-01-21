import streamlit as st
import pandas as pd
import psycopg2
from streamlit_autorefresh import st_autorefresh

# -------------------------------------------------
# STREAMLIT CONFIG
# -------------------------------------------------
st.set_page_config(
    page_title="Encore Positions Dashboard",
    layout="wide",
)

st.title("📊 Encore – Positions Dashboard")

# Auto-refresh every 5 minutes
st_autorefresh(interval=5 * 60 * 1000, key="positions_refresh")

# -------------------------------------------------
# DATABASE CONNECTION
# -------------------------------------------------
def get_conn():
    return psycopg2.connect(
        dbname=st.secrets["db"]["dbname"],
        user=st.secrets["db"]["user"],
        password=st.secrets["db"]["password"],
        host=st.secrets["db"]["host"],
        port=st.secrets["db"]["port"],
    )

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
# MOVE BUCKETS
# -------------------------------------------------
def classify_move(x):
    if pd.isna(x):
        return "Neutral"

    if x > 3:
        return "> 3% up"
    elif 2 < x <= 3:
        return "2–3% up"
    elif 1 < x <= 2:
        return "1–2% up"
    elif 0 < x <= 1:
        return "< 1% up"
    elif x == 0:
        return "Neutral"
    elif -1 <= x < 0:
        return "< 1% down"
    elif -2 <= x < -1:
        return "1–2% down"
    elif -3 <= x < -2:
        return "2–3% down"
    else:
        return "> 3% down"

BUCKET_ORDER = [
    "> 3% up",
    "2–3% up",
    "1–2% up",
    "< 1% up",
    "< 1% down",
    "1–2% down",
    "2–3% down",
    "> 3% down",
]

# -------------------------------------------------
# LOAD DATA
# -------------------------------------------------
intraday = load_intraday()

if intraday.empty:
    st.warning("No position data available yet.")
    st.stop()

# ---- normalize timestamps
intraday["snapshot_ts"] = pd.to_datetime(intraday["snapshot_ts"])

intraday["time_label"] = (
    intraday["snapshot_ts"]
    .dt.tz_convert("US/Central")
    .dt.strftime("%H:%M")
)

# -------------------------------------------------
# NORMALISE ENFUSION PRICE CHANGE
# Enfusion export gives fractional values (0.01 = 1%)
# Convert to true percentage ONCE
# -------------------------------------------------
intraday["price_change_pct"] = intraday["price_change_pct"] * 100

# ---- ALWAYS derive move_bucket after normalization
intraday["move_bucket"] = intraday["price_change_pct"].apply(classify_move)

# ---- latest snapshot
latest_ts = intraday["snapshot_ts"].max()
latest = intraday[intraday["snapshot_ts"] == latest_ts].copy()

# -------------------------------------------------
# TABS
# -------------------------------------------------
tab_sector, tab_price = st.tabs(
    ["🏭 Sector Driven", "📈 Price Change Driven"]
)

# =================================================
# TAB 1 — SECTOR DRIVEN
# =================================================
with tab_sector:
    st.header("🏭 Sector-Driven Intraday Performance")

    st.caption(
        "Sector value is calculated as Σ Market Value per sector. "
        "Moves are measured versus the previous 30-minute snapshot."
    )

    sector_values = (
        intraday
        .assign(sector_value=intraday["market_value"])
        .groupby(["snapshot_ts", "time_label", "egm_sector_v2"])
        .agg(total_value=("sector_value", "sum"))
        .reset_index()
        .sort_values("snapshot_ts")
    )

    # ---- previous snapshot comparison
    sector_values["prev_value"] = (
        sector_values
        .groupby("egm_sector_v2")["total_value"]
        .shift(1)
    )

    # ---- compute pct change SAFELY (no divide-by-zero possible)
    sector_values["pct_change"] = 0.0

    mask = sector_values["prev_value"].notna() & (sector_values["prev_value"] != 0)

    sector_values.loc[mask, "pct_change"] = (
        (sector_values.loc[mask, "total_value"]
        - sector_values.loc[mask, "prev_value"])
        / sector_values.loc[mask, "prev_value"]
        * 100
    )

    sector_values["move_bucket"] = sector_values["pct_change"].apply(classify_move)

    pivot = (
        sector_values
        .pivot(
            index="egm_sector_v2",
            columns="time_label",
            values="move_bucket"
        )
        .sort_index()
    )

    st.dataframe(pivot, width="stretch")

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
        .reindex(BUCKET_ORDER)
        .fillna(0)
        .astype(int)
    )

    st.caption(
        "Counts show number of names in each price-move bucket "
        "at each 30-minute snapshot (CST)."
    )

    st.dataframe(bucket_table, width="stretch")

    # ---- drill-down
    selected_bucket = st.selectbox(
        "Select Price-Move Bucket",
        BUCKET_ORDER,
    )

    bucket_df = latest[latest["move_bucket"] == selected_bucket].copy()

    st.subheader(f"🏭 Sector Breakdown – {selected_bucket}")

    sector_view = (
        bucket_df
        .groupby("egm_sector_v2")
        .agg(
            names=("ticker", "nunique"),
            net_nmv=("nmv", "sum"),
            avg_move=("price_change_pct", "mean"),
        )
        .reset_index()
        .sort_values("net_nmv", ascending=False)
    )

    st.dataframe(sector_view, width="stretch")

    selected_sector = st.selectbox(
        "Select Sector",
        sector_view["egm_sector_v2"].dropna().unique()
    )

    sector_df = bucket_df[bucket_df["egm_sector_v2"] == selected_sector].copy()

    # ---- Comm/Tech cohorts
    if selected_sector == "Comm/Tech":
        st.subheader("🧩 Comm/Tech – Cohort Breakdown")

        cohorts = load_commtech_cohorts()
        ct_df = sector_df.merge(cohorts, on="ticker", how="inner")

        if not ct_df.empty:
            cohort_view = (
                ct_df
                .groupby("cohort_name")
                .agg(
                    names=("ticker", "nunique"),
                    net_nmv=("nmv", "sum"),
                    avg_move=("price_change_pct", "mean"),
                )
                .reset_index()
                .sort_values("net_nmv", ascending=False)
            )

            st.dataframe(cohort_view, width="stretch")

            selected_cohort = st.selectbox(
                "Select Cohort",
                cohort_view["cohort_name"].unique()
            )

            sector_df = ct_df[ct_df["cohort_name"] == selected_cohort].copy()

    st.subheader("📋 Instrument Detail (Latest Snapshot)")

    cols = [
        "ticker",
        "description",
        "egm_sector_v2",
        "quantity",
        "price_change_pct",
        "nmv",
    ]

    if "weight_pct" in sector_df.columns:
        cols += ["weight_pct", "is_primary"]

    st.dataframe(
        sector_df[cols].sort_values("price_change_pct"),
        width="stretch",
    )