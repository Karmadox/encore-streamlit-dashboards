import streamlit as st
import pandas as pd
import psycopg2

# -------------------------------------------------
# STREAMLIT CONFIG
# -------------------------------------------------
st.set_page_config(
    page_title="Encore Positions Dashboard",
    layout="wide",
)

st.title("📊 Encore – Positions Dashboard")

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
            w.weight_pct
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
# PRICE MOVE BUCKETS
# -------------------------------------------------
def classify_move(x):
    if x > 3:
        return "> 3% up"
    elif 2 < x <= 3:
        return "2–3% up"
    elif 1 < x <= 2:
        return "1–2% up"
    elif 0 < x <= 1:
        return "< 1% up"
    elif -1 <= x < 0:
        return "< 1% down"
    elif -2 <= x < -1:
        return "1–2% down"
    elif -3 <= x < -2:
        return "2–3% down"
    else:
        return "> 3% down"

# -------------------------------------------------
# INTRADAY BUCKET TABLE
# -------------------------------------------------
def build_intraday_bucket_table(df):
    tmp = (
        df.groupby(["snapshot_ts", "move_bucket"])
        .agg(names=("ticker", "nunique"))
        .reset_index()
    )

    tmp["time_label"] = (
        pd.to_datetime(tmp["snapshot_ts"])
        .dt.tz_convert("US/Central")
        .dt.strftime("%H:%M")
    )

    pivot = (
        tmp.pivot(
            index="move_bucket",
            columns="time_label",
            values="names"
        )
        .fillna(0)
        .astype(int)
    )

    bucket_order = [
        "> 3% up",
        "2–3% up",
        "1–2% up",
        "< 1% up",
        "< 1% down",
        "1–2% down",
        "2–3% down",
        "> 3% down",
    ]

    return pivot.reindex(bucket_order)

# -------------------------------------------------
# LOAD DATA
# -------------------------------------------------
intraday = load_intraday()

if intraday.empty:
    st.warning("No position data available yet.")
    st.stop()

intraday["move_bucket"] = intraday["price_change_pct"].apply(classify_move)

latest_ts = intraday["snapshot_ts"].max()
latest = intraday[intraday["snapshot_ts"] == latest_ts]

# -------------------------------------------------
# PORTFOLIO – INTRADAY SUMMARY
# -------------------------------------------------
st.header("📌 Portfolio Price-Move Summary (Intraday)")

bucket_table = build_intraday_bucket_table(intraday)

st.caption(
    "Counts represent number of names per price-move bucket at each 30-minute snapshot (CST). "
    "Right-most column is the latest snapshot."
)

st.dataframe(bucket_table, use_container_width=True)

# -------------------------------------------------
# DRILL-DOWN (LATEST SNAPSHOT)
# -------------------------------------------------
st.header("🔎 Drill-Down (Latest Snapshot)")

selected_bucket = st.selectbox(
    "Select Price-Move Bucket",
    bucket_table.index.dropna()
)

bucket_df = latest[latest["move_bucket"] == selected_bucket]

# -------------------------------------------------
# SECTOR VIEW
# -------------------------------------------------
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

st.dataframe(sector_view, use_container_width=True)

selected_sector = st.selectbox(
    "Select Sector",
    sector_view["egm_sector_v2"].dropna().unique()
)

sector_df = bucket_df[bucket_df["egm_sector_v2"] == selected_sector]

# -------------------------------------------------
# COMM/TECH → COHORT VIEW
# -------------------------------------------------
if selected_sector == "Comm/Tech":
    st.subheader("🧩 Comm/Tech – Cohort Breakdown")

    cohorts = load_commtech_cohorts()
    ct_df = sector_df.merge(cohorts, on="ticker", how="inner")

    if ct_df.empty:
        st.info("No Comm/Tech cohort data available.")
    else:
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

        st.dataframe(cohort_view, use_container_width=True)

        selected_cohort = st.selectbox(
            "Select Cohort",
            cohort_view["cohort_name"].unique()
        )

        final_df = ct_df[ct_df["cohort_name"] == selected_cohort]
else:
    final_df = sector_df

# -------------------------------------------------
# FINAL INSTRUMENT VIEW
# -------------------------------------------------
st.subheader("📋 Instrument Detail (Latest Snapshot)")

st.dataframe(
    final_df[
        [
            "ticker",
            "description",
            "egm_sector_v2",
            "quantity",
            "price_change_pct",
            "nmv",
        ]
    ].sort_values("price_change_pct"),
    use_container_width=True
)