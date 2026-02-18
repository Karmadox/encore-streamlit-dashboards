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
        SELECT *
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
            SUM(quantity) AS quantity
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
positions = load_positions()

# --------------------------------------------------
# MERGE REAL EQUITY POSITIONS
# --------------------------------------------------

df = df.merge(
    positions,
    on="ticker",
    how="left"
)

df["quantity"] = df["quantity"].fillna(0)

# Real dollar value
df["real_value"] = df["quantity"] * df["last_price"]

# --------------------------------------------------
# 🔥 SYNTHETIC NQH6 POSITION (USING CONTRACT QUANTITY)
# --------------------------------------------------

NQ_MULTIPLIER = 20

nq_position = positions[positions["ticker"] == "NQH6"]

synthetic_index_notional = 0

if not nq_position.empty:

    nq_contracts = nq_position["quantity"].iloc[0]  # e.g. -70

    # Use NQ last price from market snapshot
    nq_row = df[df["ticker"] == "NQ1 Index"]

    if not nq_row.empty:
        nq_price = nq_row["last_price"].iloc[0]
        synthetic_index_notional = nq_contracts * nq_price * NQ_MULTIPLIER

# Convert weight % to decimal
df["weight_decimal"] = df["index_weight_pct"] / 100

# Apportioned synthetic dollar exposure
df["synthetic_value"] = df["weight_decimal"] * synthetic_index_notional

# Synthetic share equivalent
df["synthetic_quantity"] = df["synthetic_value"] / df["last_price"]

df["synthetic_quantity"] = df["synthetic_quantity"].fillna(0)
df["synthetic_value"] = df["synthetic_value"].fillna(0)

# --------------------------------------------------
# NET POSITION (REAL – SYNTHETIC)
# --------------------------------------------------

df["net_position_value"] = df["real_value"] - df["synthetic_value"]

# --------------------------------------------------
# COMBINE GOOG + GOOGL
# --------------------------------------------------

goog_mask = df["ticker"].isin(["GOOG", "GOOGL"])

if goog_mask.sum() == 2:

    goog_rows = df[goog_mask].copy()
    combined = goog_rows.iloc[0].copy()

    combined["ticker"] = "GOOG/GOOGL"

    numeric_cols = [
        "index_weight_pct",
        "real_value",
        "synthetic_value",
        "net_position_value",
        "quantity",
        "synthetic_quantity",
    ]

    for col in numeric_cols:
        combined[col] = goog_rows[col].sum()

    combined["index_rank"] = goog_rows["index_rank"].min()

    df = df[~goog_mask]
    df = pd.concat([df, pd.DataFrame([combined])], ignore_index=True)
    df = df.sort_values("index_rank").reset_index(drop=True)

# --------------------------------------------------
# HEADER
# --------------------------------------------------

st.title("📈 Nasdaq-100 — Market State")
st.caption(f"As of end of day: {snapshot_date.strftime('%d %b %Y')}")

# --------------------------------------------------
# MAIN TABLE
# --------------------------------------------------

st.subheader("📋 Canonical Market State + Synthetic Futures Overlay")

display_cols = [
    "ticker",
    "index_rank",
    "index_weight_pct",
    "last_price",
    "quantity",
    "real_value",
    "synthetic_quantity",
    "synthetic_value",
    "net_position_value",
]

styled = (
    df[display_cols]
    .style
    .format({
        "index_weight_pct": "{:.3f}",
        "last_price": "{:.2f}",
        "quantity": "{:,.0f}",
        "real_value": "{:,.0f}",
        "synthetic_quantity": "{:,.2f}",
        "synthetic_value": "{:,.0f}",
        "net_position_value": "{:,.0f}",
    })
)

st.dataframe(styled, use_container_width=True)

# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.caption(
    f"Encore Analytics • Nasdaq-100 Market State • Generated {date.today().isoformat()}"
)