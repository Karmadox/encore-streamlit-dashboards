import streamlit as st
import pandas as pd
import psycopg2
import numpy as np

# =================================================
# SIMPLE PASSWORD AUTH
# =================================================

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

# =================================================
# PAGE CONFIG
# =================================================

st.set_page_config(
    page_title="Earnings Risk Dashboard",
    layout="wide"
)

st.title("📊 Earnings Risk Management Dashboard")

# =================================================
# DATABASE CONNECTION
# =================================================

@st.cache_data(ttl=300)
def run_query(query):
    conn = psycopg2.connect(**st.secrets["db"])
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# =================================================
# DEFINE LAST QUARTER
# =================================================

last_quarter_start = "2026-01-01"
last_quarter_end   = "2026-03-31"

# =================================================
# SECTION 1 — EXECUTIVE SUMMARY
# =================================================

st.header("Executive Summary – Last Quarter")

summary_query = f"""
with event_level as (
    select
        e.instrument_id,
        i.ticker,
        e.earnings_date,
        pos.fair_value as position_value,
        (p30.close_price / p0.close_price - 1) as ret_1m,
        pos.fair_value * (p30.close_price / p0.close_price - 1) as pnl_1m
    from encoredb.historical_earnings e
    join encoredb.instruments i
        on e.instrument_id = i.instrument_id
    join encoredb.portfoliohistory pos
        on pos.ticker = i.ticker
        and pos.date = e.earnings_date
    join encoredb.equity_daily_prices p0
        on p0.instrument_id = e.instrument_id
        and p0.trade_date = e.earnings_date
    join encoredb.equity_daily_prices p30
        on p30.instrument_id = e.instrument_id
        and p30.trade_date = e.earnings_date + interval '30 days'
    where e.earnings_date between '{last_quarter_start}' and '{last_quarter_end}'
)

select
    sum(pnl_1m) as total_pnl,
    stddev(pnl_1m) * sqrt(count(*)) as quarter_volatility_estimate,
    count(*) as number_of_events
from event_level
"""

summary = run_query(summary_query)

if not summary.empty:
    total_pnl = summary["total_pnl"].iloc[0]
    vol_est = summary["quarter_volatility_estimate"].iloc[0]
    events = summary["number_of_events"].iloc[0]

    signal_ratio = total_pnl / vol_est if vol_est and vol_est != 0 else 0

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Earnings P&L (1m)", f"${total_pnl:,.0f}")
    col2.metric("Earnings Volatility Est.", f"${vol_est:,.0f}")
    col3.metric("Signal / Noise", f"{signal_ratio:.2f}")
    col4.metric("Number of Events", int(events))

    if signal_ratio < 0.5:
        st.warning("⚠ Weak risk-adjusted compensation from earnings exposure.")
    elif signal_ratio < 1:
        st.info("Earnings moderately compensated relative to volatility.")
    else:
        st.success("Strong risk-adjusted earnings performance.")

# =================================================
# SECTION 2 — EVENT LEVEL DETAIL
# =================================================

st.header("Last Quarter Earnings Events")

events_query = f"""
select
    i.ticker,
    e.earnings_date,
    pos.fair_value as position_value,
    (p1.close_price / p0.close_price - 1) as ret_1d,
    (p5.close_price / p0.close_price - 1) as ret_1w,
    (p30.close_price / p0.close_price - 1) as ret_1m,
    pos.fair_value * (p30.close_price / p0.close_price - 1) as pnl_1m
from encoredb.historical_earnings e
join encoredb.instruments i
    on e.instrument_id = i.instrument_id
join encoredb.portfoliohistory pos
    on pos.ticker = i.ticker
    and pos.date = e.earnings_date
join encoredb.equity_daily_prices p0
    on p0.instrument_id = e.instrument_id
    and p0.trade_date = e.earnings_date
left join encoredb.equity_daily_prices p1
    on p1.instrument_id = e.instrument_id
    and p1.trade_date = e.earnings_date + interval '1 day'
left join encoredb.equity_daily_prices p5
    on p5.instrument_id = e.instrument_id
    and p5.trade_date = e.earnings_date + interval '5 days'
left join encoredb.equity_daily_prices p30
    on p30.instrument_id = e.instrument_id
    and p30.trade_date = e.earnings_date + interval '30 days'
where e.earnings_date between '{last_quarter_start}' and '{last_quarter_end}'
order by pnl_1m desc
"""

events_df = run_query(events_query)

st.dataframe(events_df, use_container_width=True)

# =================================================
# SECTION 3 — STRUCTURAL EARNINGS PROFILE
# =================================================

st.header("Structural Earnings Profile (Trailing History)")

profile_query = """
with event_level as (
    select
        i.ticker,
        pos.fair_value * (p30.close_price / p0.close_price - 1) as pnl_1m
    from encoredb.historical_earnings e
    join encoredb.instruments i
        on e.instrument_id = i.instrument_id
    join encoredb.portfoliohistory pos
        on pos.ticker = i.ticker
        and pos.date = e.earnings_date
    join encoredb.equity_daily_prices p0
        on p0.instrument_id = e.instrument_id
        and p0.trade_date = e.earnings_date
    join encoredb.equity_daily_prices p30
        on p30.instrument_id = e.instrument_id
        and p30.trade_date = e.earnings_date + interval '30 days'
)

select
    ticker,
    avg(pnl_1m) as avg_event_pnl,
    stddev(pnl_1m) as pnl_vol,
    avg(pnl_1m) / nullif(stddev(pnl_1m),0) as pnl_sharpe_proxy,
    count(*) as events
from event_level
group by ticker
having count(*) >= 2
order by pnl_sharpe_proxy
"""

profile_df = run_query(profile_query)

st.dataframe(profile_df, use_container_width=True)

# Highlight structurally weak names
negatives = profile_df[profile_df["pnl_sharpe_proxy"] < 0]["ticker"].tolist()
positives = profile_df[profile_df["pnl_sharpe_proxy"] > 1]["ticker"].tolist()

st.markdown("### 🔻 Structurally Negative Earnings Convexity")
st.write(", ".join(negatives) if negatives else "None")

st.markdown("### 🔺 Structurally Positive Earnings Convexity")
st.write(", ".join(positives) if positives else "None")

# =================================================
# SECTION 4 — CURRENT QUARTER (REPORTED)
# =================================================

st.header("Current Quarter – Reported Earnings")

current_query = """
select
    i.ticker,
    e.earnings_date
from encoredb.historical_earnings e
join encoredb.instruments i
    on e.instrument_id = i.instrument_id
where date_trunc('quarter', e.earnings_date) =
      date_trunc('quarter', current_date)
order by e.earnings_date desc
"""

current_df = run_query(current_query)

st.dataframe(current_df, use_container_width=True)

# =================================================
# STRATEGIC MESSAGE BLOCK
# =================================================

st.markdown("---")
st.markdown("## Strategic Takeaways")

st.markdown("""
- Earnings exposure is a material driver of portfolio volatility.
- Risk-adjusted compensation varies significantly by name.
- Several names show persistent negative earnings convexity.
- Proactive sizing adjustments into earnings may materially improve Sharpe.
- Concentration in large post-earnings winners drives a significant share of total P&L.
""")
