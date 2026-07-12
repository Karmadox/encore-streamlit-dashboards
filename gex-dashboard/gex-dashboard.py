import streamlit as st
import pandas as pd
import psycopg2

st.title("Test")

conn = psycopg2.connect(
    dbname=st.secrets["db"]["database"],
    user=st.secrets["db"]["user"],
    password=st.secrets["db"]["password"],
    host=st.secrets["db"]["host"],
    port=st.secrets["db"]["port"],
    sslmode="require",
)

df = pd.read_sql_query(
    "SELECT * FROM research.gex_panel LIMIT 5",
    conn,
)

conn.close()

st.write(df)
