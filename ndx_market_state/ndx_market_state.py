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
    return True

if not check_password():
    st.stop()

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------

st.set_page_config(page_title="Nasdaq-100 Market State", layout="wide")

DB_CONFIG = st.secrets["db"]

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

# --------------------------------------------------
# DATA LOADERS
# --------------------------------------------------

@st.cache_data(ttl=300)
def load_latest_snapshot_date():
    sql = "SELECT MAX(snapshot_date) FROM encoredb.ndx_market_snapshot"
    with get_conn() as conn:
        return pd.read_sql(sql, conn).iloc[0, 0]

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

@st.cache_data(ttl=300)
def load_revisions(snapshot_date):
    sql = """
        SELECT r.*, i.ticker
        FROM encoredb.ndx_analyst_revisions r
        JOIN encoredb.instruments i
          ON r.instrument_id = i.instrument_id
        WHERE r.snapshot_date = %s
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(snapshot_date,))

@st.cache_data(ttl=60)
def load_positions():
    sql = """
        SELECT ticker, SUM(quantity) AS quantity
        FROM encoredb.positions_snapshot_latest
        GROUP BY ticker
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

@st.cache_data(ttl=60)
def load_nq_index_level():
    sql = """
        SELECT close
        FROM encoredb.marketdata_intraday
        WHERE security = 'NQ1 Index'
        ORDER BY timestamp DESC
        LIMIT 1
    """
    with get_conn() as conn:
        df = pd.read_sql(sql, conn)
        return None if df.empty else df["close"].iloc[0]

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------

snapshot_date = load_latest_snapshot_date()
df = load_market_state(snapshot_date)
revisions = load_revisions(snapshot_date)
positions = load_positions()
nq_index_level = load_nq_index_level()

# Merge revisions
df = df.merge(revisions, on="ticker", how="left")

# --------------------------------------------------
# REVISION SIGNAL ENGINE
# --------------------------------------------------

def revision_signal(row):

    breadth = row.get("revision_breadth_1m")
    delta = row.get("target_delta_1m_pct")

    if pd.isna(breadth):
        return ""

    if breadth > 0.3 and delta > 3:
        return "🟢⬆⬆"
    if breadth > 0.1:
        return "🟢⬆"
    if breadth < -0.3 and delta < -3:
        return "🔴⬇⬇"
    if breadth < -0.1:
        return "🔴⬇"

    return ""

df["revision_signal"] = df.apply(revision_signal, axis=1)

# --------------------------------------------------
# MERGE REAL POSITIONS
# --------------------------------------------------

df = df.merge(positions, on="ticker", how="left")
df["quantity"] = df["quantity"].fillna(0)
df["real_value"] = df["quantity"] * df["last_price"]

# --------------------------------------------------
# SYNTHETIC FUTURES OVERLAY
# --------------------------------------------------

NQ_MULTIPLIER = 20
synthetic_index_notional = 0

nq_row = positions[positions["ticker"].str.contains("NQH6", na=False)]

if not nq_row.empty and nq_index_level is not None:
    nq_contracts = nq_row["quantity"].iloc[0]
    synthetic_index_notional = nq_contracts * nq_index_level * NQ_MULTIPLIER

df["weight_decimal"] = df["index_weight_pct"] / 100
df["synthetic_value"] = df["weight_decimal"] * synthetic_index_notional
df["synthetic_quantity"] = df["synthetic_value"] / df["last_price"]

df["synthetic_value"] = df["synthetic_value"].fillna(0)
df["synthetic_quantity"] = df["synthetic_quantity"].fillna(0)

df["net_position_value"] = df["real_value"] + df["synthetic_value"]

# --------------------------------------------------
# HEADER
# --------------------------------------------------

st.title("📈 Nasdaq-100 — Market State")
st.caption(f"As of end of day: {snapshot_date.strftime('%d %b %Y')}")

# --------------------------------------------------
# HOW TO READ
# --------------------------------------------------

with st.expander("ℹ️ How to read this chart"):
    st.markdown("""
Combines:

• Index structure  
• Momentum & analyst expectations  
• Real portfolio exposure  
• Synthetic NQ futures overlay  
• Analyst revision dynamics  

**Net = Real Equity + Synthetic Allocation**

### 🔔 Revision Symbols

• 🟢⬆⬆ → Broad and strong positive revisions  
• 🟢⬆ → Mild positive revision trend  
• 🔴⬇⬇ → Broad and strong negative revisions  
• 🔴⬇ → Mild negative revision trend  

Breadth measures how many analysts are revising up vs down.  
Target delta measures magnitude of target change.
""")

st.divider()

# --------------------------------------------------
# MAIN TABLE
# --------------------------------------------------

st.subheader("📋 Canonical Market State + Synthetic Overlay")

display_cols = [
    "ticker",
    "revision_signal",
    "sector_name","cohort_name","role_bucket",
    "index_rank","index_weight_pct","last_price",
    "quantity","real_value",
    "synthetic_quantity","synthetic_value","net_position_value",
    "best_target_price",
    "target_delta_1m_pct",
    "revision_breadth_1m",
    "analyst_count",
    "days_to_earnings"
]

table_df = df[display_cols].copy()
table_df = table_df.set_index("ticker")

st.dataframe(table_df, use_container_width=True)

# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.caption(
    f"Encore Analytics • Nasdaq-100 Market State • Generated {date.today().isoformat()}"
)