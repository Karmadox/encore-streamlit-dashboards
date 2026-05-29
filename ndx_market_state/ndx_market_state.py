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
        FROM encoredb.v_index_canonical_market_state_enriched
        WHERE index_name = 'NASDAQ100'
        AND snapshot_date = %s
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

@st.cache_data(ttl=300)
def load_official_ndx_ytd():

    sql = """

        SELECT

            pct_ytd

        FROM encoredb.index_performance_snapshot

        WHERE security = 'NDX Index'

        ORDER BY snapshot_date DESC

        LIMIT 1

    """

    with get_conn() as conn:

        df = pd.read_sql(sql, conn)

    return (

        df["pct_ytd"].iloc[0]

        if not df.empty

        else None

    )
    
# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------

snapshot_date = load_latest_snapshot_date()
df = load_market_state(snapshot_date)
positions = load_positions()
nq_index_level = load_nq_index_level()
official_ndx_ytd = load_official_ndx_ytd()

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

nq_row = positions[positions["ticker"].str.startswith("NQ", na=False)]

net_contracts = 0
synthetic_summary_text = "No NQ futures position"

if not nq_row.empty and nq_index_level is not None:

    net_contracts = nq_row["quantity"].sum()

    # Build contract text summary
    contract_lines = []
    for _, row in nq_row.iterrows():
        contract_lines.append(f"{int(row['quantity'])} {row['ticker']}")

    synthetic_summary_text = " / ".join(contract_lines)

    synthetic_index_notional = net_contracts * nq_index_level * NQ_MULTIPLIER

df["weight_decimal"] = df["index_weight_pct"] / 100
df["synthetic_value"] = df["weight_decimal"] * synthetic_index_notional
df["synthetic_quantity"] = df["synthetic_value"] / df["last_price"]

df["synthetic_value"] = df["synthetic_value"].fillna(0)
df["synthetic_quantity"] = df["synthetic_quantity"].fillna(0)
df["net_position_value"] = df["real_value"] + df["synthetic_value"]

# --------------------------------------------------
# PORTFOLIO TRP TENSION (GROSS-WEIGHTED, DIRECTION-AWARE)
# --------------------------------------------------

# --------------------------------------------------
# EQUITY-ONLY TRP (STOCK BOOK)
# --------------------------------------------------

equity_df = df[df["real_value"] != 0].copy()
equity_df = equity_df[equity_df["pct_to_best_target"].notna()]

equity_gross_exposure = equity_df["real_value"].abs().sum()

if equity_gross_exposure > 0:
    equity_trp = (
        (equity_df["real_value"] * equity_df["pct_to_best_target"]).sum()
        / equity_gross_exposure
    )
else:
    equity_trp = 0

trp_df = df.copy()

# Only include actual exposure
trp_df = trp_df[trp_df["net_position_value"] != 0]

# Must have analyst target data
trp_df = trp_df[trp_df["pct_to_best_target"].notna()]

gross_exposure = trp_df["net_position_value"].abs().sum()

if gross_exposure > 0:
    portfolio_trp = (
        (trp_df["net_position_value"] * trp_df["pct_to_best_target"])
        .sum()
        / gross_exposure
    )
else:
    portfolio_trp = 0

# --------------------------------------------------
# HEDGE IMPACT
# --------------------------------------------------

hedge_impact = equity_trp - portfolio_trp

# --------------------------------------------------
# HEADER
# --------------------------------------------------

st.title("📈 Nasdaq-100 — Market State")
st.caption(f"As of end of day: {snapshot_date.strftime('%d %b %Y')}")

# --------------------------------------------------
# SYNTHETIC OVERLAY SUMMARY (NEW SECTION)
# --------------------------------------------------

st.markdown("### 🧾 Synthetic Overlay")

colA, colB, colC = st.columns(3)

colA.metric("Contracts", synthetic_summary_text)
colB.metric("Net NQ Contracts", f"{int(net_contracts)}")
colC.metric("Synthetic Notional", f"{synthetic_index_notional:,.0f}")

st.divider()

col1, col2, col3 = st.columns(3)

col1.metric("Equity TRP (Stock Book)", f"{equity_trp:.2f}%")
col2.metric("Total TRP (Incl Overlay)", f"{portfolio_trp:.2f}%")
col3.metric("Hedge Dampening Effect", f"{hedge_impact:.2f}%")

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
# SELECTED COHORT % METRIC
# --------------------------------------------------

if cohort_filter:
    selected_weight = filtered["index_weight_pct"].sum()
    total_weight = df["index_weight_pct"].sum()
    selected_pct = selected_weight / total_weight * 100

    st.metric(
        "Selected Cohort % of Nasdaq-100",
        f"{selected_pct:.2f}%"
    )

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
# YTD PERFORMANCE DECOMPOSITION
# --------------------------------------------------

st.divider()
st.subheader("📈 Nasdaq YTD Performance Decomposition")

perf_summary = (

    filtered.groupby("cohort_name", dropna=False)

    .agg(

        avg_ytd_return=(

            "pct_change_ytd",

            "mean"

        ),

        total_weight=(

            "index_weight_pct",

            "sum"

        )

    )

    .reset_index()

)

# ---------------------------------------------
# Weighted YTD Return
# ---------------------------------------------

weighted_returns = []

index_contributions = []

for cohort in perf_summary["cohort_name"]:

    cohort_df = filtered[
        filtered["cohort_name"] == cohort
    ]

    total_weight = cohort_df[
        "index_weight_pct"
    ].sum()

    if total_weight > 0:

        weighted_return = (

            (
                cohort_df["pct_change_ytd"]
                * cohort_df["index_weight_pct"]
            ).sum()

            / total_weight

        )

        contribution = (

            (
                cohort_df["pct_change_ytd"]
                * cohort_df["index_weight_pct"]
            ).sum()

            / 100

        )

    else:

        weighted_return = None
        contribution = None

    weighted_returns.append(weighted_return)

    index_contributions.append(contribution)

perf_summary["weighted_ytd_return"] = weighted_returns

perf_summary["index_contribution"] = index_contributions

# ---------------------------------------------
# Sort
# ---------------------------------------------

perf_summary = perf_summary.sort_values(

    "index_contribution",

    ascending=False

)

perf_summary = perf_summary.reset_index(drop=True)

# ---------------------------------------------
# Display
# ---------------------------------------------

st.dataframe(

    perf_summary.style.format({

        "avg_ytd_return": "{:.2f}%",

        "weighted_ytd_return": "{:.2f}%",

        "index_contribution": "{:.2f}%",

        "total_weight": "{:.2f}%"

    }),

    use_container_width=True

)

# ---------------------------------------------
# Semiconductor vs Rest
# ---------------------------------------------

semis = perf_summary[
    perf_summary["cohort_name"]
    == "Semiconductors"
]

semi_df = filtered[
    filtered["cohort_name"] == "Semiconductors"
].copy()

semi_df["weighted_contribution"] = (
    semi_df["pct_change_ytd"]
    * semi_df["index_weight_pct"]
)

st.subheader("🔬 Semiconductor Cohort Decomposition")

st.dataframe(
    semi_df[
        [
            "ticker",
            "index_weight_pct",
            "pct_change_ytd",
            "weighted_contribution"
        ]
    ]
    .sort_values(
        "weighted_contribution",
        ascending=False
    ),
    use_container_width=True
)

everything_else = perf_summary[
    perf_summary["cohort_name"]
    != "Semiconductors"
]

semi_return = (

    semis["weighted_ytd_return"].iloc[0]

    if not semis.empty

    else 0

)

other_weights = everything_else["total_weight"].sum()

if other_weights > 0:

    other_return = (

        (
            everything_else["weighted_ytd_return"]

            * everything_else["total_weight"]

        ).sum()

        / other_weights

    )

else:

    other_return = 0

semi_weight = (
    filtered[
        filtered["cohort_name"] == "Semiconductors"
    ]["index_weight_pct"]
    .sum()
)

other_weight = (
    filtered[
        filtered["cohort_name"] != "Semiconductors"
    ]["index_weight_pct"]
    .sum()
)

implied_ndx = (
    semi_return * semi_weight / 100
    +
    other_return * other_weight / 100
)

# ---------------------------------------------
# APPROXIMATE SHARE OF NASDAQ GAINS
# ---------------------------------------------

semi_raw = semi_weight * semi_return
other_raw = other_weight * other_return

total_raw = semi_raw + other_raw

if total_raw > 0:

    semi_share_of_gains = (
        semi_raw / total_raw
    ) * 100

    other_share_of_gains = (
        other_raw / total_raw
    ) * 100

else:

    semi_share_of_gains = 0
    other_share_of_gains = 0
    
# ---------------------------------------------
# KPI Cards
# ---------------------------------------------

# ---------------------------------------------
# MARKET PERFORMANCE
# ---------------------------------------------

st.markdown("##### Market Performance")

c1, c2, c3 = st.columns(3)

c1.metric(
    "Nasdaq-100 YTD",
    f"{official_ndx_ytd:.1f}%"
)

c2.metric(
    "Semiconductor Weight",
    f"{semi_weight:.1f}%"
)

c3.metric(
    "Ex-Semiconductor Weight",
    f"{other_weight:.1f}%"
)

# ---------------------------------------------
# MARKET LEADERSHIP
# ---------------------------------------------

st.markdown("##### Market Leadership")

c4, c5, c6 = st.columns(3)

c4.metric(
    "NDX Weighted Semiconductor Return",
    f"{semi_return:.1f}%"
)

c5.metric(
    "NDX Weighted Ex-Semiconductor Return",
    f"{other_return:.1f}%"
)

c6.metric(
    "Performance Spread",
    f"{semi_return - other_return:.1f}%"
)

# ---------------------------------------------
# CONTRIBUTION FRAMEWORK
# ---------------------------------------------

semi_contribution_score = (
    semi_weight * semi_return
) / 100

other_contribution_score = (
    other_weight * other_return
) / 100

total_contribution_score = (
    semi_contribution_score
    + other_contribution_score
)

if total_contribution_score > 0:

    semi_share = (
        semi_contribution_score
        / total_contribution_score
    ) * 100

    other_share = (
        other_contribution_score
        / total_contribution_score
    ) * 100

else:

    semi_share = 0
    other_share = 0

st.markdown(
    "##### Current-Weight Contribution Framework"
)

c7, c8, c9 = st.columns(3)

c7.metric(
    "Semi Contribution Score",
    f"{semi_contribution_score:.1f}"
)

c8.metric(
    "Ex-Semi Contribution Score",
    f"{other_contribution_score:.1f}"
)

c9.metric(
    "Semi Share of Contribution Score",
    f"{semi_share:.0f}%"
)

st.caption(
    "Contribution scores are calculated as "
    "current NDX weight × YTD return. "
    "These are leadership diagnostics and "
    "do not represent official Nasdaq attribution."
)

st.caption(
    "Methodology Note "
    "Semiconductor returns are calculated using current Nasdaq-100 constituent weights rather than semiconductor market-cap weights. "
    "For reference, a Bloomberg market-cap-weighted semiconductor basket returned approximately +64% YTD versus +83% under the Nasdaq-weighted framework. "
)

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
# COHORT % OF NASDAQ-100
# --------------------------------------------------

st.divider()
st.subheader("📊 Cohort % of Nasdaq-100")

cohort_summary = (
    df.groupby("cohort_name", dropna=False)
    .agg(
        cohort_weight_pct=("index_weight_pct", "sum"),
        constituent_count=("ticker", "count")
    )
    .reset_index()
    .sort_values("cohort_weight_pct", ascending=False)
)

# Format nicely
st.dataframe(
    cohort_summary.style.format({
        "cohort_weight_pct": "{:.2f}%"
    }),
    use_container_width=True
)

# --------------------------------------------------
# COHORT TRP TENSION
# --------------------------------------------------

st.subheader("📊 Cohort TRP Tension (Overlay Adjusted)")

if trp_df.empty:

    st.warning(

        "No TRP cohort data available."

    )

else:

    if "cohort_name" not in trp_df.columns:

        st.error(

            "Missing cohort_name column in TRP dataset."

        )

    else:

        cohort_trp_rows = []

        valid_cohorts = (

            trp_df["cohort_name"]

            .dropna()

            .unique()

        )

        for cohort in sorted(valid_cohorts):

            cohort_df = trp_df[

                trp_df["cohort_name"] == cohort

            ]

            gross_exposure = (

                cohort_df["net_position_value"]

                .abs()

                .sum()

            )

            if gross_exposure > 0:

                weighted_trp = (

                    (
                        cohort_df["net_position_value"]

                        * cohort_df["pct_to_best_target"]

                    ).sum()

                    / gross_exposure

                )

            else:

                weighted_trp = 0

            cohort_trp_rows.append({

                "cohort_name": cohort,

                "weighted_trp": weighted_trp

            })

        if len(cohort_trp_rows) == 0:

            st.warning(

                "No valid cohort TRP rows calculated."

            )

        else:

            cohort_trp = pd.DataFrame(

                cohort_trp_rows

            )

            cohort_trp = cohort_trp.sort_values(

                "weighted_trp",

                ascending=False

            )

            st.dataframe(

                cohort_trp.style.format({

                    "weighted_trp": "{:.2f}%"

                }),

                use_container_width=True

            )

# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.caption(
    f"Encore Analytics • Nasdaq-100 Market State • Generated {date.today().isoformat()}"
)
