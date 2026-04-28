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

    else:
        return True


if not check_password():
    st.stop()

# -------------------------------------------------
# STREAMLIT CONFIG
# -------------------------------------------------

st.set_page_config(page_title="Portfolio Earnings", layout="wide")
st.title("📅 Portfolio Earnings Calendar")

# -------------------------------------------------
# DB CONNECTION
# -------------------------------------------------

def get_conn():
    return psycopg2.connect(**st.secrets["db"])

# -------------------------------------------------
# DATA LOADER
# -------------------------------------------------

@st.cache_data(ttl=300)
def load_earnings():

    sql = """
        SELECT
            p.ticker,
            i.name,
            p.earnings_date,
            p.as_of_date
        FROM encoredb.portfolio_earnings p
        JOIN encoredb.instruments i
          ON p.instrument_id = i.instrument_id
        WHERE p.as_of_date = (
            SELECT MAX(as_of_date)
            FROM encoredb.portfolio_earnings
        )
        ORDER BY p.earnings_date NULLS LAST;
    """

    with get_conn() as conn:
        return pd.read_sql(sql, conn)

df = load_earnings()

# -------------------------------------------------
# UI
# -------------------------------------------------

if df.empty:
    st.warning("No earnings data available.")
else:

    as_of = df["as_of_date"].iloc[0]
    st.caption(f"As of {as_of}")

    # Upcoming filter
    today = date.today()
    upcoming = df[df["earnings_date"] >= today]

    st.subheader("📌 Upcoming Earnings")
    st.dataframe(
        upcoming[["ticker", "name", "earnings_date"]],
        use_container_width=True
    )

    st.subheader("📋 Full Earnings Calendar")
    st.dataframe(
        df[["ticker", "name", "earnings_date"]],
        use_container_width=True
    )

    st.download_button(
        "Download CSV",
        df.to_csv(index=False),
        file_name="portfolio_earnings.csv",
        mime="text/csv"
    )
