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
# COMBINE GOOG + GOOGL → GOOGL
# --------------------------------------------------

goog_mask = df["ticker"].isin(["GOOG", "GOOGL"])

if goog_mask.sum() == 2:
    goog_rows = df[goog_mask].copy()
    combined = goog_rows.iloc[0].copy()
    combined["ticker"] = "GOOGL"

    sum_cols = [
        "index_weight_pct","real_value","synthetic_value",
        "net_position_value","quantity","synthetic_quantity"
    ]

    for col in sum_cols:
        combined[col] = goog_rows[col].sum()

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

    combined["analyst_count"] = goog_rows["analyst_count"].sum()
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
# SYNTHETIC DISCLOSURE
# --------------------------------------------------

if not nq_row.empty and nq_index_level is not None:
    nq_contracts = nq_row["quantity"].iloc[0]
    direction = "Short" if nq_contracts < 0 else "Long"

    st.markdown(f"""
### 🧮 Synthetic Hedge Overlay

We are currently **{direction} {abs(int(nq_contracts))} NQH6 contracts**

• Index level used: {nq_index_level:,.2f}  
• Contract multiplier: {NQ_MULTIPLIER}  
• Total synthetic notional: {synthetic_index_notional:,.0f}
""")

# --------------------------------------------------
# HOW TO READ
# --------------------------------------------------

with st.expander("ℹ️ How to read this chart"):
    st.markdown("""
Combines:

• **Index structure** — Rank & weight inside the Nasdaq-100  
• **Momentum** — 1D / 5D / 1M / YTD price change  
• **Analyst expectations** — Target price & upside  
• **Revision dynamics** — How analyst consensus is shifting  
• **Real portfolio exposure** — Actual equity holdings  
• **Synthetic NQ overlay** — Futures hedge apportioned by index weight  

---

### 🔢 Core Concept

**Net Exposure = Real Equity + Synthetic Allocation**

Synthetic exposure distributes the NQ futures position proportionally across constituents based on index weight.

---

## 📊 Revision Breadth (Primary Signal)

Revision Breadth measures **net directional analyst agreement**:

\[
(Up Revisions − Down Revisions) / Analyst Count
\]

Interpretation:

• +1.0 → All analysts revising up  
•  0.0 → Balanced revisions  
• −1.0 → All analysts revising down  

---

## 🔔 Symbol Classification (Based on 1M Breadth Only)

Symbols reflect **net consensus shift**, not magnitude of target change:

• ▲▲▲ → Breadth ≥ +0.30 (Strong positive shift)  
• ▲▲ → +0.10 to +0.30 (Moderate positive shift)  
• — → −0.10 to +0.10 (Neutral / mixed)  
• ▼▼ → −0.30 to −0.10 (Moderate negative shift)  
• ▼▼▼ → ≤ −0.30 (Strong negative shift)  

No symbol = Neutral revision regime.

---

### 📈 Target Δ (1M / 3M)

Target Delta shows **magnitude** of target change, but does not drive the symbol classification.

• Use it to gauge size of expectation change  
• Use breadth to gauge analyst agreement  

---

### 🧠 How to Interpret

• High breadth + rising targets → Strengthening conviction  
• High breadth + flat targets → Early expectation shift  
• Low breadth + large delta → Narrow / isolated revisions  
• 3M confirming 1M → Structural shift  
• 1M diverging from 3M → Potential turning point
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
    "ticker","sector_name","cohort_name","role_bucket",
    "index_rank","index_weight_pct","last_price",
    "pct_change_1d","pct_change_5d","pct_change_1m",
    "pct_change_ytd","pct_from_52w_high",
    "quantity","real_value","synthetic_quantity",
    "synthetic_value","net_position_value",
    "best_target_price","pct_to_best_target",
    "target_delta_1m_pct","revision_breadth_1m",
    "target_delta_3m_pct","revision_breadth_3m",
    "revision_signal",
    "analyst_count","best_analyst_rating",
    "days_to_earnings"
]

table_df = filtered[[c for c in display_cols if c in filtered.columns]]
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

st.dataframe(
    role_summary.style.format({
        "total_weight":"{:.2f}%",
        "real_exposure":"{:,.0f}",
        "synthetic_exposure":"{:,.0f}",
        "net_exposure":"{:,.0f}",
        "median_upside":"{:.2f}%"
    }),
    use_container_width=True
)

# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.caption(
    f"Encore Analytics • Nasdaq-100 Market State • Generated {date.today().isoformat()}"
)