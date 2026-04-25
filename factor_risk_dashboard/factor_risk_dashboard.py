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

    st.markdown("""
    ## Model Definitions

    **US4AxiomaMH**  
    Medium-Horizon fundamental risk model.  
    Designed to explain risk and returns over multi-month horizons using structural style and industry factors.

    **US4AxiomaSH**  
    Short-Horizon statistical risk model.  
    Designed to capture shorter-term return dynamics, including additional statistical and short-term momentum factors.

    Comparing the two models helps assess whether performance is driven by:
    - Longer-term structural exposures (MH)
    - Shorter-term statistical / tactical effects (SH)

    ---

    ## Exposure Definitions

    ### NAV Normalized Exposure

    $$
    \frac{\sum ( \text{NMV}_i \times \text{Factor Exposure}_i )}{\text{Portfolio NAV}}
    $$

    Measures how much portfolio capital is exposed to each factor.

    • Positive value → Net long exposure to the factor  
    • Negative value → Net short exposure  
    • Magnitude reflects capital-weighted sensitivity  

    ---

    ### Gross Normalized Exposure

    $$
    \frac{\sum ( |\text{GMV}_i| \times \text{Factor Exposure}_i )}{\sum |\text{GMV}_i|}
    $$

    Measures factor intensity independent of capital direction.

    • Ignores long vs short sign  
    • Reflects how strongly the book is positioned around a factor  
    • Useful for understanding structural risk concentration  

    ---

    ## Return Attribution

    ### Daily Factor Contribution

    $$
    \text{NAV Exposure} \times \text{Daily Factor Return}
    $$

    Explains how much each factor contributed to portfolio return on a given day.

    Summing across all factors gives:

    $$
    \text{Total Factor Return}
    $$

    ---

    ### Specific Return

    $$
    \text{Actual Return} - \text{Total Factor Return}
    $$

    Measures idiosyncratic (stock-specific) performance not explained by the factor model.

    • Positive → Alpha beyond factor exposures  
    • Negative → Underperformance relative to factor positioning  

    ---

    ## Rolling 30-Day $R^2$

    $$
    R^2 = \text{Correlation}^2(\text{Factor Return}, \text{Actual Return})
    $$

    Measures how much of daily return variance is explained by systematic factors over the past 30 trading days.

    • Higher $R^2$ → Returns are largely factor-driven  
    • Lower $R^2$ → Returns are more idiosyncratic  
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

st.line_chart(
    rolling.pivot(index="end_date", columns="model_name", values="rolling_r2")
)

st.divider()
st.caption("Encore Analytics • Internal Use Only")
