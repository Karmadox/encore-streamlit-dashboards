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

### 🔔 Revision Symbols (from SQL engine)

• ▲▲▲ → Strong & broad upward revisions  
• ▲▲ → Moderate upward revisions  
• ▲ → Mild positive revisions  
• ▼▼▼ → Strong & broad downward revisions  
• ▼▼ → Moderate downward revisions  
• ▼ → Mild negative revisions  

Breadth = (Up − Down) / Analyst Count  
Target delta = % change in consensus target (1M)
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
# FILTERS
# --------------------------------------------------

col1,col2,col3,col4 = st.columns(4)

with col1:
    role_filter = st.multiselect("Role bucket",
        sorted(df["role_bucket"].dropna().unique()))

with col2:
    cohort_filter = st.multiselect("Cohort",
        sorted(df["cohort_name"].dropna().unique()))

with col3:
    max_rank = st.slider("Show top N constituents",1,101,101)

with col4:
    earnings_filter = st.checkbox("Only earnings ≤14 days")

filtered = df.copy()

if role_filter:
    filtered = filtered[filtered["role_bucket"].isin(role_filter)]
if cohort_filter:
    filtered = filtered[filtered["cohort_name"].isin(cohort_filter)]

filtered = filtered[filtered["index_rank"] <= max_rank]

if earnings_filter:
    filtered = filtered[filtered["days_to_earnings"].between(0,14)]

# --------------------------------------------------
# MAIN TABLE
# --------------------------------------------------

st.subheader("📋 Canonical Market State + Synthetic Overlay")

display_cols = [
    "ticker",
    "revision_signal",
    "sector_name","cohort_name","role_bucket",
    "index_rank","index_weight_pct","last_price",
    "pct_change_1d","pct_change_5d",
    "pct_change_1m","pct_change_ytd","pct_from_52w_high",
    "quantity","real_value",
    "synthetic_quantity","synthetic_value","net_position_value",
    "best_target_price","pct_to_best_target",
    "target_delta_1m_pct","revision_breadth_1m",
    "analyst_count","best_analyst_rating",
    "days_to_earnings"
]

available_cols = [c for c in display_cols if c in filtered.columns]
table_df = filtered[available_cols].copy()
table_df = table_df.set_index("ticker")

st.dataframe(table_df, use_container_width=True)

# --------------------------------------------------
# FILTERED TOTALS
# --------------------------------------------------

st.markdown("### 📊 Selected Totals")

c1,c2,c3 = st.columns(3)
c1.metric("Real (Selected)", f"{filtered['real_value'].sum():,.0f}")
c2.metric("Synthetic (Selected)", f"{filtered['synthetic_value'].sum():,.0f}")
c3.metric("Net (Selected)", f"{filtered['net_position_value'].sum():,.0f}")

# --------------------------------------------------
# ROLE SUMMARY
# --------------------------------------------------

st.divider()
st.subheader("🧩 Role-Level Summary")

role_summary = (
    filtered.groupby("role_bucket", dropna=False)
    .agg(
        total_weight=("index_weight_pct","sum"),
        real_exposure=("real_value","sum"),
        synthetic_exposure=("synthetic_value","sum"),
        net_exposure=("net_position_value","sum"),
        median_upside=("pct_to_best_target","median")
    )
    .reset_index()
    .sort_values("total_weight",ascending=False)
)

st.dataframe(role_summary, use_container_width=True)

# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.caption(
    f"Encore Analytics • Nasdaq-100 Market State • Generated {date.today().isoformat()}"
)