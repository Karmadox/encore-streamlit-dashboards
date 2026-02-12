import streamlit as st
import psycopg2
import pandas as pd

# =====================================
# PAGE CONFIG
# =====================================

st.set_page_config(
    page_title="Encore Cross Asset Market State Dashboard",
    layout="wide"
)

# =====================================
# SIMPLE PASSWORD PROTECTION
# =====================================

def check_password():
    def password_entered():
        if st.session_state["password"] == st.secrets["dashboard_password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input(
            "Enter Password",
            type="password",
            on_change=password_entered,
            key="password",
        )
        return False
    elif not st.session_state["password_correct"]:
        st.text_input(
            "Enter Password",
            type="password",
            on_change=password_entered,
            key="password",
        )
        st.error("Incorrect password")
        return False
    else:
        return True

if not check_password():
    st.stop()

# =====================================
# DATABASE CONNECTION
# =====================================

@st.cache_data(ttl=600)
def load_data():

    conn = psycopg2.connect(
        dbname=st.secrets["db"]["dbname"],
        user=st.secrets["db"]["user"],
        password=st.secrets["db"]["password"],
        host=st.secrets["db"]["host"],
        port=st.secrets["db"]["port"],
    )

    query = """
        SELECT *
        FROM encoredb.v_market_state_cohort_summary
        ORDER BY avg_1d_pct DESC;
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df

df = load_data()

# =====================================
# COMMENTARY LOGIC
# =====================================

def interpret_row(row):
    comments = []

    if row["avg_1d_pct"] > 1:
        comments.append("Strong short-term momentum")
    elif row["avg_1d_pct"] < -1:
        comments.append("Short-term weakness")

    if row["avg_1m_pct"] > 3:
        comments.append("Positive medium-term trend")
    elif row["avg_1m_pct"] < -3:
        comments.append("Medium-term drawdown")

    if row["avg_pct_from_52w_high"] < -20:
        comments.append("Deeply below 52W highs")

    if row["pct_up_1d"] < 40:
        comments.append("Weak breadth")
    elif row["pct_up_1d"] > 60:
        comments.append("Broad participation")

    return " | ".join(comments)

df["Commentary"] = df.apply(interpret_row, axis=1)

# =====================================
# HEADER
# =====================================

st.title("Encore Earnings Regime Monitor")

st.markdown("""
This dashboard groups instruments by earnings sensitivity cohort and tracks
short-term momentum, medium-term trend, positioning compression, and breadth.

**How to read:**

- 1D = positioning / short-term flows  
- 1M = earnings revision trend  
- 3M = structural rotation  
- % from 52W high = positioning compression  
- Breadth = percentage of instruments positive on the day  
""")

# =====================================
# TABLE DISPLAY
# =====================================

st.subheader("Cohort Performance Summary")

st.dataframe(
    df,
    use_container_width=True,
)

# =====================================
# BAR CHART (1M Momentum)
# =====================================

st.subheader("Medium-Term Trend (1M %)")

chart_df = df.set_index("earnings_cohort")["avg_1m_pct"]

st.bar_chart(chart_df)

# =====================================
# COHORT DEFINITIONS
# =====================================

with st.expander("Cohort Construction Methodology"):

    st.markdown("""
    Cohorts group instruments by dominant earnings driver or macro sensitivity.

    - **growth_equity** → duration-sensitive large cap growth
    - **enterprise_software** → capex / SaaS earnings beta
    - **semiconductors** → AI & hardware cycle
    - **equal_weight** → domestic breadth proxy
    - **defensive_asset** → gold / safe-haven
    - **stress** → volatility regime indicator
    - **real_economy** → commodity cyclicals
    - **macro** → dollar liquidity proxy
    - **rates** → real rate expectations
    """)

# =====================================
# FOOTER
# =====================================

st.caption("Data Source: Bloomberg EOD | Updated daily at 4:30PM CST")