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

st.set_page_config(page_title="Earnings Risk Dashboard", layout="wide")
st.title("📊 Earnings Risk Management Dashboard")

st.markdown("""
<style>
div[data-testid="stDataFrame"] {
    font-size: 0.85rem;
}
</style>
""", unsafe_allow_html=True)

# =================================================
# DB CONNECTION
# =================================================

@st.cache_data(ttl=300)
def run_query(query):
    conn = psycopg2.connect(**st.secrets["db"])
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# =================================================
# FORMATTER
# =================================================

def format_event_table(df):
    if df.empty:
        return df

    for col in ["ret_1d","ret_1w","ret_1m","ret_3m"]:
        if col in df.columns:
            df[col] = (df[col] * 100).round(2)

    for col in ["pnl_1m","pnl_3m","position_value"]:
        if col in df.columns:
            df[col] = df[col].round(0)

    return df

# =================================================
# LAST QUARTER
# =================================================

last_quarter_start = "2026-01-01"
last_quarter_end   = "2026-03-31"

# =================================================
# CORE EVENT ENGINE (1D/1W/1M/3M)
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
         order by trade_date limit 1) as px_1d,

        (select close_price from encoredb.equity_daily_prices
         where instrument_id = ev.instrument_id
           and trade_date > ev.anchor_date
         order by trade_date offset 4 limit 1) as px_1w,

        (select close_price from encoredb.equity_daily_prices
         where instrument_id = ev.instrument_id
           and trade_date > ev.anchor_date
         order by trade_date offset 20 limit 1) as px_1m,

        (select close_price from encoredb.equity_daily_prices
         where instrument_id = ev.instrument_id
           and trade_date > ev.anchor_date
         order by trade_date offset 62 limit 1) as px_3m

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
    sum(pos.fair_value * (ep.px_1m / ep.px_t - 1)) as total_pnl_1m,
    sum(pos.fair_value * (ep.px_3m / ep.px_t - 1)) as total_pnl_3m,
    count(*) as number_of_events
from event_prices ep
join encoredb.portfoliohistory pos
    on pos.ticker = ep.ticker
   and pos.date = ep.anchor_date
where ep.earnings_date between '{last_quarter_start}' and '{last_quarter_end}'
"""

summary = run_query(summary_query)

if not summary.empty:
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Earnings P&L (1m)", f"${summary['total_pnl_1m'][0]:,.0f}")
    col2.metric("Total Earnings P&L (3m)", f"${summary['total_pnl_3m'][0]:,.0f}")
    col3.metric("Number of Events", int(summary['number_of_events'][0]))

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
    (ep.px_3m / ep.px_t - 1) as ret_3m,
    pos.fair_value * (ep.px_1m / ep.px_t - 1) as pnl_1m,
    pos.fair_value * (ep.px_3m / ep.px_t - 1) as pnl_3m
from event_prices ep
join encoredb.portfoliohistory pos
    on pos.ticker = ep.ticker
   and pos.date = ep.anchor_date
where ep.earnings_date between '{last_quarter_start}' and '{last_quarter_end}'
order by pnl_1m desc
"""

events_df = format_event_table(run_query(events_query))
st.dataframe(events_df, height=500, use_container_width=False)

# =================================================
# SECTION 3 — STRUCTURAL EARNINGS PROFILE
# =================================================

st.header("Structural Earnings Profile (Trailing History)")

profile_query = base_event_cte + """
select
    ep.ticker,
    avg(ep.px_1m / ep.px_t - 1) as avg_ret_1m,
    stddev(ep.px_1m / ep.px_t - 1) as vol_1m,
    avg(ep.px_1m / ep.px_t - 1) /
        nullif(stddev(ep.px_1m / ep.px_t - 1),0) as sharpe_proxy,
    count(*) as events
from event_prices ep
group by ep.ticker
having count(*) >= 2
order by sharpe_proxy
"""

profile_df = run_query(profile_query)

if not profile_df.empty:
    profile_df["avg_ret_1m"] = (profile_df["avg_ret_1m"] * 100).round(2)
    profile_df["vol_1m"] = (profile_df["vol_1m"] * 100).round(2)
    profile_df["sharpe_proxy"] = profile_df["sharpe_proxy"].round(2)

st.dataframe(profile_df, height=400, use_container_width=False)

# =================================================
# SECTION 4 — UPCOMING RISK (WITH STRUCTURAL FLAG)
# =================================================

st.header("Upcoming Earnings Risk Exposure (Next 30 Days)")

upcoming_query = """
with upcoming as (
    select
        i.ticker,
        e.earnings_date,
        pos.fair_value
    from encoredb.historical_earnings e
    join encoredb.instruments i
        on e.instrument_id = i.instrument_id
    join encoredb.portfoliohistory pos
        on pos.ticker = i.ticker
       and pos.date = current_date
    where e.earnings_date between current_date
          and current_date + interval '30 days'
)
select *
from upcoming
order by earnings_date
"""

upcoming_df = run_query(upcoming_query)

if not upcoming_df.empty:
    upcoming_df["position_value"] = upcoming_df["fair_value"].round(0)
    upcoming_df = upcoming_df.drop(columns=["fair_value"])

st.dataframe(upcoming_df, height=300, use_container_width=False)

# =================================================
# STRATEGIC MESSAGE
# =================================================

st.markdown("---")
st.markdown("## Strategic Takeaways")

st.markdown("""
- 1D / 1W / 1M / 3M forward earnings convexity now tracked.
- Structural earnings profile identifies persistent convexity bias.
- Upcoming exposure quantifies real-time earnings risk.
- Dashboard now supports proactive position sizing decisions.
""")
