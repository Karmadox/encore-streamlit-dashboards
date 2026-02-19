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
positions = load_positions()
nq_index_level = load_nq_index_level()

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
# COMBINE GOOG + GOOGL (DISPLAY AS GOOGL)
# --------------------------------------------------

goog_mask = df["ticker"].isin(["GOOG", "GOOGL"])

if goog_mask.sum() == 2:
    goog_rows = df[goog_mask].copy()
    combined = goog_rows.iloc[0].copy()

    combined["ticker"] = "GOOGL"

    # Sum exposure-related fields
    sum_cols = [
        "index_weight_pct",
        "real_value",
        "synthetic_value",
        "net_position_value",
        "quantity",
        "synthetic_quantity"
    ]

    for col in sum_cols:
        if col in df.columns:
            combined[col] = goog_rows[col].sum()

    # Weight-average revision & target fields
    weight_col = "index_weight_pct"

    revision_cols = [
        "target_delta_1m_pct",
        "target_delta_3m_pct",
        "revision_breadth_1m",
        "revision_breadth_3m"
    ]

    for col in revision_cols:
        if col in df.columns:
            weights = goog_rows[weight_col]
            values = goog_rows[col]
            combined[col] = (values * weights).sum() / weights.sum()

    # Analyst count summed
    if "analyst_count" in df.columns:
        combined["analyst_count"] = goog_rows["analyst_count"].sum()

    # Keep best rank
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

### 🔔 Revision Metrics

• Target Δ (1M / 3M) → % change in consensus target  
• Breadth (1M / 3M) → (Up − Down) / Analyst Count  

### 🔔 Signal Symbols

• ▲▲▲ → Strong & broad upward revisions  
• ▲▲ → Moderate upward revisions  
• ▲ → Mild positive revisions  
• ▼▼▼ → Strong & broad downward revisions  
• ▼▼ → Moderate downward revisions  
• ▼ → Mild negative revisions
""")

st.divider()

# --------------------------------------------------
# GLOBAL METRICS
# --------------------------------------------------

top5_weight = df.loc[df["index_rank"] <= 5, "index_weight_pct"].sum()
top10_weight = df.loc[df["index_rank"] <= 10, "index_weight_pct"].sum()
pct_near_high = (df["pct_from_52w_high"] >= -10).mean() * 100
earnings_14d = df["days_to_earnings"].between(0,14).sum()

total_real = df["real_value"].sum()
total_synth = df["synthetic_value"].sum()
total_net = df["net_position_value"].sum()

c1,c2,c3,c4,c5,c6 = st.columns(6)
c1.metric("Top 5 weight", f"{top5_weight:.1f}%")
c2.metric("Top 10 weight", f"{top10_weight:.1f}%")
c3.metric("% within 10% of high", f"{pct_near_high:.0f}%")
c4.metric("Earnings ≤14d", earnings_14d)
c5.metric("Real Exposure", f"{total_real:,.0f}")
c6.metric("Net Exposure", f"{total_net:,.0f}")

st.divider()

# --------------------------------------------------
# MAIN TABLE
# --------------------------------------------------

st.subheader("📋 Canonical Market State + Synthetic Overlay")

display_cols = [
    "ticker",
    "sector_name","cohort_name","role_bucket",
    "index_rank","index_weight_pct","last_price",
    "pct_change_1d","pct_change_5d",
    "pct_change_1m","pct_change_ytd","pct_from_52w_high",
    "quantity","real_value",
    "synthetic_quantity","synthetic_value","net_position_value",
    "best_target_price","pct_to_best_target",

    # 1M
    "target_delta_1m_pct",
    "revision_breadth_1m",

    # 3M
    "target_delta_3m_pct",
    "revision_breadth_3m",

    # Signal after raw data
    "revision_signal",

    "analyst_count","best_analyst_rating",
    "days_to_earnings"
]

available_cols = [c for c in display_cols if c in df.columns]
table_df = df[available_cols].copy()
table_df = table_df.set_index("ticker")

st.dataframe(table_df, use_container_width=True)

# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.caption(
    f"Encore Analytics • Nasdaq-100 Market State • Generated {date.today().isoformat()}"
)