import streamlit as st
import pandas as pd
import psycopg2
from datetime import date

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
# PAGE CONFIG
# --------------------------------------------------

st.set_page_config(
    page_title="Nasdaq-100 Market State",
    layout="wide",
)

# --------------------------------------------------
# DB CONFIG
# --------------------------------------------------

DB_CONFIG = st.secrets["db"]

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

# --------------------------------------------------
# DATA LOADERS
# --------------------------------------------------

@st.cache_data(ttl=300)
def load_latest_snapshot_date():
    sql = """
        SELECT MAX(snapshot_date) AS snapshot_date
        FROM encoredb.ndx_market_snapshot
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)["snapshot_date"].iloc[0]


@st.cache_data(ttl=300)
def load_market_state(snapshot_date):
    sql = """
        SELECT
            snapshot_date,
            ticker,
            sector_name,
            cohort_name,
            role_bucket,
            index_rank,
            index_weight_pct,
            cumulative_weight_pct,
            last_price,
            pct_change_1d,
            pct_change_5d,
            pct_change_1m,
            pct_change_ytd,
            pct_from_52w_high,
            best_target_price,
            pct_to_best_target,
            analyst_count,
            best_analyst_rating,
            next_earnings_date,
            days_to_earnings
        FROM encoredb.v_ndx_canonical_market_state_enriched
        WHERE snapshot_date = %s
        ORDER BY index_rank
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(snapshot_date,))


@st.cache_data(ttl=60)
def load_positions():
    sql = """
        SELECT
            ticker,
            SUM(quantity) AS notional_quantity
        FROM encoredb.positions_snapshot_latest
        GROUP BY ticker
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------

snapshot_date = load_latest_snapshot_date()
df = load_market_state(snapshot_date)
positions_df = load_positions()

# --------------------------------------------------
# 🔥 MERGE POSITIONS
# --------------------------------------------------

df = df.merge(
    positions_df,
    on="ticker",
    how="left"
)

df["notional_quantity"] = df["notional_quantity"].fillna(0)

# Latest value = quantity × last_price
df["latest_value"] = df["notional_quantity"] * df["last_price"]

# --------------------------------------------------
# 🔥 COMBINE GOOG + GOOGL
# --------------------------------------------------

goog_mask = df["ticker"].isin(["GOOG", "GOOGL"])

if goog_mask.sum() == 2:

    goog_rows = df[goog_mask].copy()
    total_weight = goog_rows["index_weight_pct"].sum()

    combined_row = goog_rows.iloc[0].copy()
    combined_row["ticker"] = "GOOG/GOOGL"
    combined_row["index_weight_pct"] = total_weight
    combined_row["index_rank"] = goog_rows["index_rank"].min()

    weight = goog_rows["index_weight_pct"]
    weighted_avg = lambda col: (goog_rows[col] * weight).sum() / total_weight

    for col in [
        "last_price",
        "pct_change_1d",
        "pct_change_5d",
        "pct_change_1m",
        "pct_change_ytd",
        "pct_from_52w_high",
        "pct_to_best_target",
        "best_target_price",
    ]:
        combined_row[col] = weighted_avg(col)

    combined_row["analyst_count"] = goog_rows["analyst_count"].sum()
    combined_row["days_to_earnings"] = goog_rows["days_to_earnings"].min()

    # 🔥 combine positions too
    combined_row["notional_quantity"] = goog_rows["notional_quantity"].sum()
    combined_row["latest_value"] = combined_row["notional_quantity"] * combined_row["last_price"]

    df = df[~goog_mask]
    df = pd.concat([df, pd.DataFrame([combined_row])], ignore_index=True)
    df = df.sort_values("index_rank").reset_index(drop=True)

# --------------------------------------------------
# HEADER
# --------------------------------------------------

st.title("📈 Nasdaq-100 — Market State")
st.caption(f"As of end of day: {snapshot_date.strftime('%d %b %Y')}")

st.divider()

# --------------------------------------------------
# MAIN TABLE
# --------------------------------------------------

st.subheader("📋 Canonical Market State")

display_cols = [
    "ticker",
    "sector_name",
    "cohort_name",
    "role_bucket",
    "index_rank",
    "index_weight_pct",
    "notional_quantity",      # 🔥 NEW
    "last_price",
    "latest_value",           # 🔥 NEW
    "best_target_price",
    "pct_change_1d",
    "pct_change_5d",
    "pct_change_1m",
    "pct_change_ytd",
    "pct_from_52w_high",
    "pct_to_best_target",
    "analyst_count",
    "best_analyst_rating",
    "days_to_earnings",
]

styled = (
    df[display_cols]
    .style
    .format({
        "index_weight_pct": "{:.3f}",
        "notional_quantity": "{:,.0f}",
        "last_price": "{:.2f}",
        "latest_value": "{:,.0f}",
        "best_target_price": "{:.2f}",
        "pct_change_1d": "{:.2f}%",
        "pct_change_5d": "{:.2f}%",
        "pct_change_1m": "{:.2f}%",
        "pct_change_ytd": "{:.2f}%",
        "pct_from_52w_high": "{:.2f}%",
        "pct_to_best_target": "{:+.1f}%",
    })
)

st.dataframe(styled, use_container_width=True)

# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.caption(
    f"Encore Analytics • Nasdaq-100 Market State • Generated {date.today().isoformat()}"
)