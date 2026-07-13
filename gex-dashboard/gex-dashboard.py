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

@st.cache_data(ttl=300)
def load_regimes():

    with get_conn() as conn:

        df = pd.read_sql("""

            SELECT

                ticker,
                earnings_date,

                gamma_regime,
                vix_regime,
                dispersion_regime

            FROM research.earnings_regimes

        """, conn)

    df["earnings_date"] = pd.to_datetime(
        df["earnings_date"]
    ).dt.date

    return df
    
# -------------------------------------------------
# MAIN DASHBOARD
# -------------------------------------------------

st.title("📊 Dealer Gamma (GEX) Dashboard")

panel = load_panel()

regimes = load_regimes()

panel = panel.merge(

    regimes,

    on=["ticker", "earnings_date"],

    how="left"
)

event_dates = list_event_dates()

if not event_dates:
    st.warning("No earnings dates found.")
    st.stop()

if panel.empty:
    st.warning("GEX panel is empty.")
    st.stop()

today = date.today()

upcoming = sorted([d for d in event_dates if pd.to_datetime(d).date() >= today])
cutoff = pd.to_datetime(today) - pd.Timedelta(days=30)

past = sorted(
    panel[
        (panel["earnings_date"] < today) &
        (panel["earnings_date"] >= cutoff.date())
    ]["earnings_date"].unique(),
    reverse=True
)

with st.expander("📘 How to read this dashboard", expanded=False):

    st.markdown("""
### 🧠 What is Dealer Gamma (GEX)?

Dealer Gamma measures how options dealers are positioned in a stock.

- **Positive GEX (+)** → Dealers are *long gamma*  
  → They hedge **against moves** → tends to **dampen volatility**

- **Negative GEX (−)** → Dealers are *short gamma*  
  → They hedge **with moves** → tends to **amplify volatility**

👉 In simple terms:
- **+GEX = stabilizing flows**
- **−GEX = destabilizing flows**

---

### 🎯 Why this matters for earnings

Earnings are major volatility events.

This dashboard helps answer:

> **Will dealer positioning dampen or amplify the post-earnings move?**

---

### 📊 How to interpret the dashboard

#### 1. Selected Day (top section)

- Shows all companies reporting on a chosen earnings date
- Key signals:
  - **Dealer γ column** → long / short gamma per name
  - **Aggregate GEX**:
    - **Positive** → more stable environment
    - **Negative** → higher risk of large moves

---

#### 2. Upcoming Earnings

- Forward-looking view (next ~7 earnings dates)
- Use this to:
  - Identify **fragile setups (short gamma)**
  - Size risk ahead of earnings

---

#### 3. Recent Earnings

- Shows **dealer positioning BEFORE earnings**
- Useful for:
  - Understanding past moves
  - Building intuition:
    - Did **short gamma → big move?**
    - Did **long gamma → muted reaction?**

---

### 🎯 4. Expected Move + Tail Risk (Analog Regimes)

This section answers:

> **“What happened historically in setups like this — and how reliable is that signal?”**

Each stock is mapped to a **historical analog regime** based on:
- Dealer positioning (**GEX**)
- A simple move/volatility bucket

We then compare against **past earnings events with similar setups**.

---

#### 🧩 Two layers of insight

The model uses **two sources of history**:

- **Ticker-specific (Source = Ticker)**  
  → Same stock, same regime  
  → **Highest quality signal**

- **Cross-sectional (Source = Cross)**  
  → Other stocks in same regime  
  → Used when ticker history is limited

---

#### 📊 Key fields

- **Expected Move**  
  → Average 1-day post-earnings move  
  → “What normally happens”

- **Tail Move (P90)**  
  → 90th percentile move  
  → “What happens in stressed outcomes”

- **Break Prob**  
  → % of times move exceeded ~3%  
  → Proxy for **event risk / convexity**

- **Source**  
  → **Ticker** = stock-specific history  
  → **Cross** = fallback to market analogs

---

#### 🚨 Risk Flags

- **✔️ Strong (Ticker)**  
  → Enough stock-specific history  
  → **Most reliable signal**

- **✔️ Strong (Cross)**  
  → Limited ticker data, but strong analog sample  

- **⚠️ Low Sample**  
  → Weak statistical backing  
  → Use **GEX intuition more than history**

- **⚠️ Short Gamma**  
  → Dealer positioning may **amplify moves**

- **🔥 False Stability**  
  → Long gamma setup  
  → BUT history shows moves still break  

---

#### ⚠️ Important nuance

Not all signals are equal:

- **Ticker + high observations** → high conviction  
- **Cross + high observations** → medium conviction  
- **Low observations (<30)** → low conviction  

👉 When data is weak:
> Rely more on **positioning (GEX)** than historical averages

---

### 🔁 View Modes

- **Earnings Day**  
  → Names reporting on selected date  

- **Full Universe**  
  → All stocks with GEX data  

- **Focus: INTC / DELL**  
  → Deep-dive into key names  

---

### ⚠️ Important notes

- GEX is **not directional** (does not predict up vs down)
- It is a **volatility / flow signal**
- Analog regimes are **probabilistic, not predictive**
- Best used alongside:
  - Positioning
  - Earnings expectations
  - Market regime

---

### 🧭 Rule of thumb

- **Negative GEX → expect larger moves**
- **Positive GEX → expect more contained moves**
- **Low sample → low confidence**
- **False stability → beware of surprise breakouts**

---

### 🏁 Bottom line

This dashboard shows:

> **Where dealer positioning and historical analogs suggest earnings moves may be amplified, suppressed — or mispriced — and how much confidence to place in that signal.**
""")
    


