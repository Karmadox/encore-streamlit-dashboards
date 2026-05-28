import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go

# =========================================================
# PAGE CONFIG
# =========================================================

st.set_page_config(
    page_title="Earnings Expectation Dislocation Framework",
    layout="wide"
)

# =========================================================
# PASSWORD PROTECTION
# =========================================================

def check_password():

    def password_entered():

        if (
            st.session_state["password"]
            == st.secrets["auth"]["password"]
        ):

            st.session_state["password_correct"] = True

            del st.session_state["password"]

        else:

            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:

        st.text_input(
            "Password",
            type="password",
            on_change=password_entered,
            key="password"
        )

        return False

    elif not st.session_state["password_correct"]:

        st.text_input(
            "Password",
            type="password",
            on_change=password_entered,
            key="password"
        )

        st.error("Incorrect password")

        return False

    else:

        return True


if not check_password():

    st.stop()

# =========================================================
# STYLING
# =========================================================

st.markdown(
    """
    <style>

    .main {
        background-color: #0E1117;
        color: white;
    }

    div[data-testid="metric-container"] {
        background-color: #1A1D24;
        border: 1px solid #2C2F36;
        padding: 15px;
        border-radius: 10px;
    }

    </style>
    """,
    unsafe_allow_html=True
)

# =========================================================
# DATABASE CONNECTION
# =========================================================

@st.cache_resource
def get_conn():

    return psycopg2.connect(
        dbname=st.secrets["db"]["database"],
        user=st.secrets["db"]["user"],
        password=st.secrets["db"]["password"],
        host=st.secrets["db"]["host"],
        port=st.secrets["db"]["port"]
    )

# =========================================================
# LOAD DATA
# =========================================================

@st.cache_data(ttl=300)
def load_data():

    conn = get_conn()

    sql = """

    select

        ivr.ticker,
        ivr.earnings_date,
        ivr.capture_date,

        ivr.implied_move_pct,
        ivr.realized_move_pct,

        ivr.implied_vs_realized_ratio,
        ivr.implied_vs_realized_spread,

        ivr.expectation_failure,

        er.gamma_regime,
        er.vix_regime,
        er.dispersion_regime,

        er.vix_close

    from research.earnings_implied_vs_realized ivr

    left join research.earnings_regimes er

        on ivr.ticker = er.ticker
       and ivr.earnings_date = er.earnings_date

    where ivr.ticker in ('INTC', 'DELL')

    order by
        ivr.earnings_date desc

    """

    df = pd.read_sql(sql, conn)

    return df


df = load_data()

# =========================================================
# HEADER
# =========================================================

st.title(
    "Earnings Expectation Dislocation Framework"
)

st.caption(
    "Implied vs Realized Earnings Risk Analysis"
)

st.markdown("---")

# =========================================================
# HOW TO READ THIS DASHBOARD
# =========================================================

with st.expander(
    "How to Read This Dashboard",
    expanded=True
):

    st.markdown("""

    ### Framework Objective

    This dashboard measures whether the options market
    correctly priced earnings risk.

    The core framework compares:

    - **Implied Move**
      → the move expected by the options market before earnings

    versus

    - **Realized Move**
      → the actual post-earnings stock move

    ---

    ### Core Interpretation

    #### Implied vs Realized Ratio

    ```text
    Ratio > 1.0
    ```

    Means:

    - realized volatility exceeded expectations
    - earnings risk was underpriced
    - the market was surprised

    Higher values indicate larger expectation failures.

    ---

    #### Ratio < 1.0

    Means:

    - implied volatility overpriced the event
    - realized volatility was muted
    - options implied too much risk

    ---

    ### Expectation Failure

    An earnings event is classified as an
    **Expectation Failure** when:

    ```text
    Realized Move > Implied Move
    ```

    These are the events most relevant for:

    - volatility dislocations
    - post-earnings repricing
    - convexity opportunities
    - short gamma stress

    ---

    ### Regime Definitions

    #### Gamma Regime

    Measures dealer positioning dynamics.

    - **LONG_GAMMA**
      → dealer positioning dampens volatility

    - **SHORT_GAMMA**
      → dealer positioning amplifies volatility

    Historically, short gamma environments
    produce larger earnings dislocations.

    ---

    #### VIX Regime

    Market volatility state.

    - LOW_VOL
    - NORMAL_VOL
    - HIGH_VOL

    Higher volatility regimes often produce
    larger realized earnings moves.

    ---

    #### Dispersion Regime

    Measures cross-sectional earnings volatility.

    - LOW_DISPERSION
    - NORMAL_DISPERSION
    - HIGH_DISPERSION

    High dispersion environments indicate
    elevated single-stock instability.

    ---

    ### Scatter Plot

    The diagonal line represents:

    ```text
    perfect earnings pricing
    ```

    Points ABOVE the line:

    - realized volatility exceeded implied volatility
    - risk was underpriced

    Points BELOW the line:

    - implied volatility overpriced realized risk

    ---

    ### Regime Heatmap

    Displays average instability ratios
    across volatility regimes.

    Higher values indicate environments where:

    - earnings pricing repeatedly failed
    - realized volatility exceeded expectations

    ---

    ### Analog Regime Engine

    The Analog Engine groups historical earnings events
    by similar volatility conditions.

    It helps identify:

    - historical analogs
    - recurring instability environments
    - regime-dependent earnings behavior

    This is intended as a volatility
    and risk-framing tool rather than a prediction engine.

    """)
    
# =========================================================
# KPI METRICS
# =========================================================

recent_df = df[
    pd.to_datetime(df["earnings_date"])
    >= pd.Timestamp("2024-01-01")
]

intc_df = recent_df[
    recent_df["ticker"] == "INTC"
]

dell_df = recent_df[
    recent_df["ticker"] == "DELL"
]

intc_ratio = round(
    intc_df["implied_vs_realized_ratio"].mean(),
    2
)

dell_ratio = round(
    dell_df["implied_vs_realized_ratio"].mean(),
    2
)

short_gamma_df = df[
    df["gamma_regime"] == "SHORT_GAMMA"
]

if len(short_gamma_df) > 0:

    short_gamma_failure = round(
        100
        * short_gamma_df["expectation_failure"].mean(),
        1
    )

else:

    short_gamma_failure = 0

largest_row = df.loc[
    df["realized_move_pct"].abs().idxmax()
]

largest_move = round(
    100
    * largest_row["realized_move_pct"],
    1
)

largest_label = (
    f"{largest_row['ticker']} "
    f"{largest_move}%"
)

# =========================================================
# KPI DISPLAY
# =========================================================

col1, col2, col3, col4 = st.columns(4)

with col1:

    st.metric(
        "INTC Avg Instability Ratio",
        f"{intc_ratio}x"
    )

with col2:

    st.metric(
        "DELL Avg Instability Ratio",
        f"{dell_ratio}x"
    )

with col3:

    st.metric(
        "Short Gamma Failure Rate",
        f"{short_gamma_failure}%"
    )

with col4:

    st.metric(
        "Largest Earnings Break",
        largest_label
    )

st.markdown("---")

# =========================================================
# EVENT TABLE
# =========================================================

st.subheader("Earnings Event Audit Trail")

table_df = df.copy()

table_df["implied_move_pct"] = (
    table_df["implied_move_pct"] * 100
).round(2)

table_df["realized_move_pct"] = (
    table_df["realized_move_pct"] * 100
).round(2)

table_df["implied_vs_realized_ratio"] = (
    table_df["implied_vs_realized_ratio"]
).round(2)

table_df["regime"] = (

    table_df["gamma_regime"].fillna("UNKNOWN")

    + " | "

    + table_df["vix_regime"].fillna("UNKNOWN")
)

st.dataframe(

    table_df[
        [
            "ticker",
            "earnings_date",
            "implied_move_pct",
            "realized_move_pct",
            "implied_vs_realized_ratio",
            "expectation_failure",
            "regime"
        ]
    ],

    use_container_width=True,
    height=450
)

st.markdown("---")

# =========================================================
# SCATTER PLOT
# =========================================================

st.subheader("Implied vs Realized Earnings Moves")

scatter_df = df.copy()

scatter_df["implied_pct"] = (
    scatter_df["implied_move_pct"] * 100
)

scatter_df["realized_pct"] = (
    scatter_df["realized_move_pct"].abs() * 100
)

fig_scatter = px.scatter(

    scatter_df,

    x="implied_pct",
    y="realized_pct",

    color="ticker",

    hover_data=[
        "earnings_date",
        "gamma_regime",
        "vix_regime"
    ],

    labels={
        "implied_pct": "Implied Move %",
        "realized_pct": "Realized Move %"
    }
)

max_axis = max(

    scatter_df["implied_pct"].max(),
    scatter_df["realized_pct"].max()
)

fig_scatter.add_trace(

    go.Scatter(

        x=[0, max_axis],
        y=[0, max_axis],

        mode="lines",

        name="Perfect Pricing",

        line=dict(
            dash="dash"
        )
    )
)

st.plotly_chart(
    fig_scatter,
    use_container_width=True
)

st.markdown("---")

# =========================================================
# REGIME HEATMAP
# =========================================================

st.subheader("Regime Heatmap")

heatmap_df = (

    df.groupby(
        [
            "gamma_regime",
            "vix_regime"
        ]
    )["implied_vs_realized_ratio"]

    .mean()

    .reset_index()
)

heatmap_pivot = heatmap_df.pivot(

    index="gamma_regime",
    columns="vix_regime",
    values="implied_vs_realized_ratio"
)

fig_heatmap = px.imshow(

    heatmap_pivot,

    text_auto=".2f",

    aspect="auto",

    labels=dict(
        color="Avg Ratio"
    )
)

st.plotly_chart(
    fig_heatmap,
    use_container_width=True
)

st.markdown("---")

# =========================================================
# TIME SERIES
# =========================================================

st.subheader("Instability Through Time")

ts_df = df.copy()

ts_df["implied_pct"] = (
    ts_df["implied_move_pct"] * 100
)

ts_df["realized_pct"] = (
    ts_df["realized_move_pct"].abs() * 100
)

fig_ts = go.Figure()

for ticker in ["INTC", "DELL"]:

    subset = ts_df[
        ts_df["ticker"] == ticker
    ]

    fig_ts.add_trace(

        go.Scatter(

            x=subset["earnings_date"],
            y=subset["implied_pct"],

            mode="lines+markers",

            name=f"{ticker} Implied"
        )
    )

    fig_ts.add_trace(

        go.Scatter(

            x=subset["earnings_date"],
            y=subset["realized_pct"],

            mode="lines+markers",

            name=f"{ticker} Realized"
        )
    )

st.plotly_chart(
    fig_ts,
    use_container_width=True
)

st.markdown("---")

# =========================================================
# ANALOG ENGINE
# =========================================================

st.subheader("Analog Regime Engine")

df["analog_regime"] = (

    df["gamma_regime"].fillna("UNKNOWN")

    + " + "

    + df["vix_regime"].fillna("UNKNOWN")

    + " + "

    + df["dispersion_regime"].fillna("UNKNOWN")
)

analog_df = (

    df.groupby("analog_regime")

    .agg(

        sample_size=(
            "ticker",
            "count"
        ),

        avg_realized_move=(
            "realized_move_pct",
            lambda x: x.abs().mean()
        ),

        failure_rate=(
            "expectation_failure",
            "mean"
        )

    )

    .reset_index()
)

analog_df["avg_realized_move"] = (
    analog_df["avg_realized_move"] * 100
).round(2)

analog_df["failure_rate"] = (
    analog_df["failure_rate"] * 100
).round(1)

analog_df = analog_df.sort_values(

    "failure_rate",
    ascending=False
)

st.dataframe(
    analog_df,
    use_container_width=True
)

st.markdown("---")

# =========================================================
# FOOTER
# =========================================================

st.caption(
    "Encore Earnings Instability Research Framework"
)
