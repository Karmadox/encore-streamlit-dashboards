import streamlit as st
import pandas as pd
import psycopg2
from pathlib import Path

# -------------------------------------------------
# PASSWORD AUTH (reuse your pattern)
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

# -------------------------------------------------
# PAGE CONFIG
# -------------------------------------------------

st.set_page_config(
    page_title="Encore Factor Risk Dashboard",
    layout="wide",
)

# -------------------------------------------------
# DATABASE CONNECTION
# -------------------------------------------------

def get_connection():
    return psycopg2.connect(
        dbname=st.secrets["db"]["dbname"],
        user=st.secrets["db"]["user"],
        password=st.secrets["db"]["password"],
        host=st.secrets["db"]["host"],
        port=st.secrets["db"]["port"],
    )

@st.cache_data(ttl=300)
def run_query(query):
    conn = None
    try:
        conn = get_connection()
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        if conn is not None:
            conn.rollback()   # Clear failed transaction state
        raise e
    finally:
        if conn is not None:
            conn.close()      # Always close connection

# -------------------------------------------------
# HEADER
# -------------------------------------------------

st.title("📊 Encore Factor Risk & Attribution")
st.divider()

# -------------------------------------------------
# HOW TO READ THIS VIEW
# -------------------------------------------------

with st.expander("📘 How to Read This View", expanded=True):

    st.markdown("## Model Definitions")

    st.markdown("**US4AxiomaMH**  \n"
                "Medium-Horizon fundamental risk model.  \n"
                "Designed to explain risk and returns over multi-month horizons using structural style and industry factors.")

    st.markdown("**US4AxiomaSH**  \n"
                "Short-Horizon statistical risk model.  \n"
                "Designed to capture shorter-term return dynamics, including additional statistical and short-term momentum factors.")

    st.markdown("""
    Comparing the two models helps assess whether performance is driven by:
    - Longer-term structural exposures (MH)
    - Shorter-term tactical or statistical effects (SH)
    """)

    st.divider()

    st.markdown("## Exposure Definitions")

    st.markdown("### NAV Normalized Exposure")
    st.latex(r"\frac{\sum (NMV_i \times Exposure_i)}{Portfolio\ NAV}")
    st.markdown("""
    Measures how much portfolio capital is exposed to each factor.

    - Positive → Net long exposure  
    - Negative → Net short exposure  
    - Magnitude reflects capital-weighted sensitivity  
    """)

    st.markdown("### Gross Normalized Exposure")
    st.latex(r"\frac{\sum (|GMV_i| \times Exposure_i)}{\sum |GMV_i|}")
    st.markdown("""
    Measures factor intensity independent of capital direction.

    - Ignores long vs short sign  
    - Reflects structural risk concentration  
    """)

    st.divider()

    st.markdown("## Return Attribution")

    st.markdown("### Daily Factor Contribution")
    st.latex(r"NAV\ Exposure \times Daily\ Factor\ Return")
    st.markdown("Explains how much each factor contributed to portfolio return.")

    st.markdown("### Specific Return")
    st.latex(r"Actual\ Return - Total\ Factor\ Return")
    st.markdown("""
    Measures stock-specific (idiosyncratic) performance.

    - Positive → Alpha beyond factor positioning  
    - Negative → Underperformance relative to exposures  
    """)

    st.divider()

    st.markdown("## Rolling 30-Day R²")
    st.latex(r"R^2 = Corr(Factor\ Return,\ Actual\ Return)^2")
    st.markdown("""
    Measures how much of return variance is explained by systematic factors over the past 30 trading days.

    - Higher R² → Returns are factor-driven  
    - Lower R² → Returns are more idiosyncratic  
    """)
    
    st.divider()

# -------------------------------------------------
# DATE FILTER
# -------------------------------------------------

latest_date = run_query(
    "SELECT MAX(date) AS max_date FROM encoredb.portfolio_factor_attribution_summary_mv"
)["max_date"][0]

st.subheader(f"📅 Latest Available Date: {latest_date}")

# -------------------------------------------------
# CURRENT SNAPSHOT
# -------------------------------------------------

st.subheader("🔎 Current Snapshot")

snapshot = run_query(f"""
SELECT *
FROM encoredb.portfolio_factor_attribution_summary_mv
WHERE date = '{latest_date}'
ORDER BY model_name
""")

st.dataframe(snapshot, use_container_width=True)

st.divider()

# -------------------------------------------------
# DAILY ATTRIBUTION TABLE
# -------------------------------------------------

st.subheader("📈 Daily Factor Attribution")

start_date = st.date_input("Start Date", pd.to_datetime("2026-01-01"))

attrib = run_query(f"""
SELECT *
FROM encoredb.portfolio_factor_pnl_attribution
WHERE date >= '{start_date}'
ORDER BY date DESC
""")

st.dataframe(attrib, use_container_width=True)

st.download_button(
    "⬇ Download Attribution CSV",
    attrib.to_csv(index=False),
    file_name="factor_attribution.csv",
    mime="text/csv"
)

st.divider()

# -------------------------------------------------
# YTD BUCKET ATTRIBUTION
# -------------------------------------------------

st.subheader("📦 YTD Bucket Attribution")

bucket = run_query(f"""
SELECT
    a.model_name,
    l.factor_type,
    SUM(a.factor_pnl_contribution) AS bucket_return
FROM encoredb.portfolio_factor_pnl_attribution a
JOIN encoredb.factor_type_lookup l
  ON l.model_name = a.model_name
 AND l.factor_name = a.factor_name
WHERE a.date >= '2026-01-01'
GROUP BY a.model_name, l.factor_type
ORDER BY a.model_name, l.factor_type
""")

st.dataframe(bucket, use_container_width=True)

st.download_button(
    "⬇ Download Bucket Attribution",
    bucket.to_csv(index=False),
    file_name="bucket_attribution.csv",
    mime="text/csv"
)

st.divider()

# -------------------------------------------------
# ROLLING R²
# -------------------------------------------------

st.subheader("📊 Rolling 30-Day R²")

rolling = run_query("""
SELECT *
FROM encoredb.portfolio_rolling_r2
WHERE end_date >= '2026-02-01'
ORDER BY end_date
""")

st.dataframe(rolling, use_container_width=True)

st.markdown("### Rolling 30-Day R² — Systematic vs Idiosyncratic Regime")

chart_data = rolling.pivot(
    index="end_date",
    columns="model_name",
    values="rolling_r2"
)

st.line_chart(chart_data)

st.caption(
    "Higher R² indicates returns are primarily factor-driven. "
    "Lower R² indicates greater idiosyncratic (stock-specific) influence."
)

st.divider()
st.caption("Encore Analytics • Internal Use Only")
