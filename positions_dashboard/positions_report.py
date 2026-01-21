import streamlit as st
import pandas as pd
import psycopg2

# -------------------------------------------------
# DB
# -------------------------------------------------

def get_conn():
    return psycopg2.connect(
        dbname=st.secrets["db"]["dbname"],
        user=st.secrets["db"]["user"],
        password=st.secrets["db"]["password"],
        host=st.secrets["db"]["host"],
        port=st.secrets["db"]["port"],
    )

@st.cache_data(ttl=1800)  # 30 mins
def load_latest_positions():
    sql = """
        SELECT *
        FROM encoredb.positions_snapshot
        WHERE snapshot_ts = (
            SELECT max(snapshot_ts)
            FROM encoredb.positions_snapshot
        )
        ORDER BY egm_sector_v2, ticker;
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

# -------------------------------------------------
# STREAMLIT UI
# -------------------------------------------------
st.set_page_config(
    page_title="Encore Positions Dashboard",
    layout="wide"
)

st.title("📊 Encore Positions Dashboard")
st.caption("Latest 30-minute snapshot")

df = load_latest_positions()

st.write(f"Snapshot rows: **{len(df)}**")
st.dataframe(df, use_container_width=True)