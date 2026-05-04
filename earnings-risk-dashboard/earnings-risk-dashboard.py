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
# CORE EVENT ENGINE (ANCHOR + TRADING DAYS)
# =================================================

base_event_cte = """
with earnings_events as (

    select
        e.instrument_id,
        i.ticker,
        e.earnings_date,
        e.announcement_time,

        case
            when e.announcement_time ilike '%Aft%'
                 or e.announcement_time >= '16:00'
            then (
                select min(trade_date)
                from encoredb.equity_daily_prices p2
                where p2.instrument_id = e.instrument_id
                  and p2.trade_date > e.earnings_date
            )
            else e.earnings_date
        end as anchor_date

    from encoredb.historical_earnings e
    join encoredb.instruments i
        on e.instrument_id = i.instrument_id
),

event_prices as (

    select
        ev.*,
        p0.close_price as px_t,

        (select close_price from encoredb.equity_daily_prices
         where instrument_id = ev.instrument_id
           and trade_date > ev.anchor_date
         order by trade_date
         limit 1) as px_1d,

        (select close_price from encoredb.equity_daily_prices
         where instrument_id = ev.instrument_id
           and trade_date > ev.anchor_date
         order by trade_date
         offset 4 limit 1) as px_1w,

        (select close_price from encoredb.equity_daily_prices
         where instrument_id = ev.instrument_id
           and trade_date > ev.anchor_date
         order by trade_date
         offset 20 limit 1) as px_1m

    from earnings_events ev
    join encoredb.equity_daily_prices p0
        on p0.instrument_id = ev.instrument_id
       and p0.trade_date = ev.anchor_date
)
"""

# =================================================
# SECTION 1 — EXECUTIVE SUMMARY
# =================================================

st.header("Executive Summary – Last Quarter")

summary_query = base_event_cte + f"""
select
    sum(pos.fair_value * (ep.px_1m / ep.px_t - 1)) as total_pnl,
    stddev(pos.fair_value * (ep.px_1m / ep.px_t - 1)) * sqrt(count(*)) as quarter_volatility_estimate,
    count(*) as number_of_events
from event_prices ep
join encoredb.portfoliohistory pos
    on pos.ticker = ep.ticker
   and pos.date = ep.anchor_date
where ep.earnings_date between '{last_quarter_start}' and '{last_quarter_end}'
  and ep.px_1m is not null
"""

summary = run_query(summary_query)

if not summary.empty and summary["number_of_events"].iloc[0] > 0:
    total_pnl = summary["total_pnl"].iloc[0]
    vol_est = summary["quarter_volatility_estimate"].iloc[0]
    events = summary["number_of_events"].iloc[0]

    signal_ratio = total_pnl / vol_est if vol_est and vol_est != 0 else 0

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Earnings P&L (1m)", f"${total_pnl:,.0f}")
    col2.metric("Earnings Volatility Est.", f"${vol_est:,.0f}")
    col3.metric("Signal / Noise", f"{signal_ratio:.2f}")
    col4.metric("Number of Events", int(events))

# =================================================
# SECTION 2 — EVENT DETAIL
# =================================================

st.header("Last Quarter Earnings Events")

events_query = base_event_cte + f"""
select
    ep.ticker,
    ep.earnings_date,
    pos.fair_value as position_value,
    (ep.px_1d / ep.px_t - 1) as ret_1d,
    (ep.px_1w / ep.px_t - 1) as ret_1w,
    (ep.px_1m / ep.px_t - 1) as ret_1m,
    pos.fair_value * (ep.px_1m / ep.px_t - 1) as pnl_1m
from event_prices ep
join encoredb.portfoliohistory pos
    on pos.ticker = ep.ticker
   and pos.date = ep.anchor_date
where ep.earnings_date between '{last_quarter_start}' and '{last_quarter_end}'
order by pnl_1m desc
"""

events_df = run_query(events_query)

st.dataframe(events_df, use_container_width=True)

# =================================================
# SECTION 3 — STRUCTURAL PROFILE
# =================================================

st.header("Structural Earnings Profile (Trailing History)")

profile_query = base_event_cte + """
select
    ep.ticker,
    avg(pos.fair_value * (ep.px_1m / ep.px_t - 1)) as avg_event_pnl,
    stddev(pos.fair_value * (ep.px_1m / ep.px_t - 1)) as pnl_vol,
    avg(pos.fair_value * (ep.px_1m / ep.px_t - 1)) /
        nullif(stddev(pos.fair_value * (ep.px_1m / ep.px_t - 1)),0) as pnl_sharpe_proxy,
    count(*) as events
from event_prices ep
join encoredb.portfoliohistory pos
    on pos.ticker = ep.ticker
   and pos.date = ep.anchor_date
where ep.px_1m is not null
group by ep.ticker
having count(*) >= 2
order by pnl_sharpe_proxy
"""

profile_df = run_query(profile_query)

st.dataframe(profile_df, use_container_width=True)

negatives = profile_df[profile_df["pnl_sharpe_proxy"] < 0]["ticker"].tolist()
positives = profile_df[profile_df["pnl_sharpe_proxy"] > 1]["ticker"].tolist()

st.markdown("### 🔻 Structurally Negative Earnings Convexity")
st.write(", ".join(negatives) if negatives else "None")

st.markdown("### 🔺 Structurally Positive Earnings Convexity")
st.write(", ".join(positives) if positives else "None")

# =================================================
# CURRENT QUARTER
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
# STRATEGIC MESSAGE
# =================================================

st.markdown("---")
st.markdown("## Strategic Takeaways")

st.markdown("""
- Earnings exposure is a material driver of portfolio volatility.
- Risk-adjusted compensation varies significantly by name.
- Structural convexity differs meaningfully across tickers.
- Anchor-adjusted, trading-day-based analysis improves precision.
- Proactive sizing adjustments into earnings may materially improve Sharpe.
""")
