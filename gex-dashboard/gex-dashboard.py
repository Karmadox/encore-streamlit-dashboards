import streamlit as st
import pandas as pd
import psycopg2
from datetime import date

# -------------------------------------------------
# 🔐 SIMPLE PASSWORD AUTH
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
# CONFIG
# -------------------------------------------------

st.set_page_config(
    page_title="Dealer Gamma (GEX) Dashboard",
    page_icon="📊",
    layout="wide",
)

# -------------------------------------------------
# DB CONNECTION
# -------------------------------------------------

def get_conn():
    return psycopg2.connect(
        dbname=st.secrets["db"]["database"],
        user=st.secrets["db"]["user"],
        password=st.secrets["db"]["password"],
        host=st.secrets["db"]["host"],
        port=st.secrets["db"]["port"],
        sslmode="require"
    )

# -------------------------------------------------
# DATA LOADERS
# -------------------------------------------------

@st.cache_data(ttl=60)
def load_panel():
    try:
        with get_conn() as conn:
            df = pd.read_sql("SELECT * FROM research.gex_panel", conn)

        if df.empty:
            return df

        df["earnings_date"] = pd.to_datetime(df["earnings_date"]).dt.date
        df["asof_date"] = pd.to_datetime(df["asof_date"]).dt.date

        return df

    except Exception as e:
        st.error(f"🚨 DB ERROR (load_panel): {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def list_event_dates():
    try:
        with get_conn() as conn:
            df = pd.read_sql("""
                SELECT DISTINCT earnings_date
                FROM encoredb.portfolio_earnings
                WHERE earnings_date IS NOT NULL
                ORDER BY earnings_date
            """, conn)

        return df["earnings_date"].astype(str).tolist()

    except Exception as e:
        st.error(f"🚨 DB ERROR (event_dates): {e}")
        return []


@st.cache_data(ttl=60)
def load_names_for_date(date_str):
    try:
        with get_conn() as conn:
            df = pd.read_sql("""
                SELECT
                    p.ticker,
                    p.earnings_date,
                    i.name AS description
                FROM encoredb.portfolio_earnings p
                JOIN encoredb.instruments i
                  ON p.instrument_id = i.instrument_id
                WHERE p.earnings_date = %s
                AND p.as_of_date = (
                    SELECT MAX(as_of_date)
                    FROM encoredb.portfolio_earnings
                )
            """, conn, params=(date_str,))

        return df

    except Exception as e:
        st.error(f"🚨 DB ERROR (names): {e}")
        return pd.DataFrame()

# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def _gex_dollar_M(v):
    if pd.isna(v):
        return "—"
    return f"{'-' if v < 0 else '+'}${abs(v)/1e6:,.2f}M"


def _dealer_pos(v):
    if pd.isna(v) or abs(v) < 1e3:
        return "flat"
    return "+long γ" if v > 0 else "−short γ"


def _enrich_with_panel(names, panel, date_str):
    if names.empty:
        return names

    date_obj = pd.to_datetime(date_str).date()
    sub = panel[panel["earnings_date"] == date_obj]

    if sub.empty:
        out = names.copy()
        for c in ["spot", "gex", "gex_call", "gex_put", "n_strikes", "n_expiries"]:
            out[c] = pd.NA
        return out

    return names.merge(
        sub[["ticker", "spot", "gex", "gex_call", "gex_put", "n_strikes", "n_expiries"]],
        on="ticker",
        how="left",
    )


def _gex_table(df, show_event_date=False):

    if df.empty:
        return df

    if "gex" in df.columns:
        df = df.sort_values("gex", key=lambda x: x.abs(), ascending=False)

    cols = []
    if show_event_date:
        cols.append("earnings_date")

    cols += ["ticker", "description", "spot", "gex", "gex_call", "gex_put", "n_strikes", "n_expiries"]
    cols = [c for c in cols if c in df.columns]

    out = df[cols].copy()
    out["dealer γ"] = out["gex"].apply(_dealer_pos)

    return out.rename(columns={
        "earnings_date": "Earnings",
        "ticker": "Ticker",
        "description": "Name",
        "spot": "Spot",
        "gex": "GEX (total)",
        "gex_call": "GEX (calls)",
        "gex_put": "GEX (puts)",
        "n_strikes": "Strikes",
        "n_expiries": "Expiries",
    })


def _format_table(df):

    if df.empty:
        return df

    fmt = {}

    if "Spot" in df.columns:
        fmt["Spot"] = lambda v: f"${v:,.2f}" if pd.notna(v) else "—"

    for c in ("GEX (total)", "GEX (calls)", "GEX (puts)"):
        if c in df.columns:
            fmt[c] = _gex_dollar_M

    for c in ("Strikes", "Expiries"):
        if c in df.columns:
            fmt[c] = lambda v: f"{int(v)}" if pd.notna(v) else "—"

    return df.style.format(fmt)

# -------------------------------------------------
# MAIN DASHBOARD
# -------------------------------------------------

st.title("📊 Dealer Gamma (GEX) Dashboard")

panel = load_panel()
event_dates = list_event_dates()

if not event_dates:
    st.warning("No earnings dates found.")
    st.stop()

if panel.empty:
    st.warning("GEX panel is empty.")
    st.stop()

today = date.today()

upcoming = sorted([d for d in event_dates if pd.to_datetime(d).date() >= today])
past = sorted([d for d in event_dates if pd.to_datetime(d).date() < today], reverse=True)

# -------------------------------------------------
# SECTION 1
# -------------------------------------------------

st.markdown("## 🎯 Selected Day — Names Reporting + Dealer Gamma")

default_date = upcoming[0] if upcoming else past[0]

sel_date = st.selectbox(
    "Earnings date",
    options=upcoming + past,
    index=(upcoming + past).index(default_date),
)

show_all = st.toggle("Show names without GEX", value=False)

names = load_names_for_date(sel_date)
merged = _enrich_with_panel(names, panel, sel_date)

if names.empty:
    st.info(f"No names for {sel_date}")
else:

    n_with_gex = merged["gex"].notna().sum()

    if not show_all:
        merged = merged[merged["gex"].notna()]

    c1, c2, c3 = st.columns(3)

    coverage = (n_with_gex / len(names)) if len(names) > 0 else 0

    c1.metric("Names reporting", len(names))
    c2.metric("With GEX", f"{n_with_gex}/{len(names)}", delta=f"{coverage:.0%}")

    if n_with_gex > 0:
        agg = merged["gex"].sum()
        regime = "Long Gamma" if agg > 0 else "Short Gamma"
        c3.metric("Aggregate GEX", _gex_dollar_M(agg), delta=regime)

    st.dataframe(_format_table(_gex_table(merged)), use_container_width=True)

# -------------------------------------------------
# SECTION 2 (FIXED)
# -------------------------------------------------

st.markdown("## 📅 Upcoming Earnings — GEX Reads")

rows = []

for d in upcoming[:7]:
    n = load_names_for_date(d)
    if not n.empty:
        rows.append(_enrich_with_panel(n.assign(earnings_date=d), panel, d))

df_up = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

if not df_up.empty and "gex" in df_up.columns:
    if not show_all:
        df_up = df_up[df_up["gex"].notna()]

st.dataframe(
    _format_table(_gex_table(df_up, show_event_date=True)),
    use_container_width=True
)

# -------------------------------------------------
# SECTION 3 (FIXED)
# -------------------------------------------------

st.markdown("## 📉 Recent Earnings — GEX Reads")

rows = []

for d in past[:7]:
    n = load_names_for_date(d)
    if not n.empty:
        rows.append(_enrich_with_panel(n.assign(earnings_date=d), panel, d))

df_rec = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

# 🔥 FIXED SAFETY CHECK
if not df_rec.empty and "gex" in df_rec.columns:
    if not show_all:
        df_rec = df_rec[df_rec["gex"].notna()]

st.dataframe(
    _format_table(_gex_table(df_rec, show_event_date=True)),
    use_container_width=True
)

# -------------------------------------------------
# FOOTER
# -------------------------------------------------

st.divider()
st.caption("Data: Databento + Bloomberg via Postgres pipeline.")
