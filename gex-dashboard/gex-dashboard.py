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
# SECTION 2
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

st.dataframe(_format_table(_gex_table(df_up, show_event_date=True)), use_container_width=True)

# -------------------------------------------------
# SECTION 3 — Recent Earnings (🔥 FIXED PROPERLY)
# -------------------------------------------------

st.markdown("## 📉 Recent Earnings — GEX Reads")

# 🔥 Use panel directly (NOT portfolio_earnings)
df_rec = panel[panel["earnings_date"] < today].copy()

if df_rec.empty:
    st.info("No recent earnings data available.")
else:

    # Optional: restrict to last 30 days (recommended)
    cutoff = pd.to_datetime(today) - pd.Timedelta(days=30)
    df_rec = df_rec[
        pd.to_datetime(df_rec["earnings_date"]) >= cutoff
    ]

    # Optional filter
    if not show_all and df_rec["gex"].notna().any():
        df_rec = df_rec[df_rec["gex"].notna()]

    # Sort by most recent date then biggest GEX
    df_rec = df_rec.sort_values(
        ["earnings_date", "gex"],
        ascending=[False, False]
    )

    st.dataframe(
        _format_table(_gex_table(df_rec, show_event_date=True)),
        use_container_width=True
    )

# -------------------------------------------------
# SECTION 4 — Expected Move + Tail Risk
# -------------------------------------------------

st.markdown("## 🎯 Expected Move + Tail Risk (Analog Regimes)")

MIN_OBS = 30

# -------------------------------------------------
# VIEW MODE
# -------------------------------------------------

view_mode = st.radio(
    "View",
    ["Earnings Day", "Full Universe", "Focus: INTC / DELL"],
    horizontal=True
)

st.caption(
    "Earnings Day = names reporting on selected date • "
    "Full Universe = all names • "
    "Focus = Intel + Dell deep dive"
)

# -------------------------------------------------
# LOAD REGIME ANALOG MAP
# -------------------------------------------------

@st.cache_data(ttl=300)
def load_regime_map():

    with get_conn() as conn:

        df = pd.read_sql("""

            SELECT

                concat(
                    coalesce(gamma_regime, 'UNKNOWN'),
                    '_',
                    coalesce(vix_regime, 'UNKNOWN'),
                    '_',
                    coalesce(dispersion_regime, 'UNKNOWN')
                ) as analog_regime,

                COUNT(*) AS n_obs,

                AVG(ABS(realized_move_1d)) AS avg_move,

                STDDEV(ABS(realized_move_1d)) AS vol,

                PERCENTILE_CONT(0.9)
                WITHIN GROUP (
                    ORDER BY ABS(realized_move_1d)
                ) AS p90_move,

                AVG(
                    CASE
                        WHEN ABS(realized_move_1d) > 0.03
                        THEN 1
                        ELSE 0
                    END
                ) AS break_rate

            FROM research.earnings_regimes

            WHERE realized_move_1d IS NOT NULL

            GROUP BY analog_regime

        """, conn)

    return df

# -------------------------------------------------
# LOAD TICKER HISTORY
# -------------------------------------------------

@st.cache_data(ttl=300)
def load_ticker_history():

    with get_conn() as conn:

        df = pd.read_sql("""

            SELECT

                ticker,

                earnings_date,

                concat(
                    coalesce(gamma_regime, 'UNKNOWN'),
                    '_',
                    coalesce(vix_regime, 'UNKNOWN'),
                    '_',
                    coalesce(dispersion_regime, 'UNKNOWN')
                ) as analog_regime,

                ABS(realized_move_1d) AS realized_move_1d

            FROM research.earnings_regimes

            WHERE realized_move_1d IS NOT NULL

        """, conn)

    return df

hist = load_ticker_history()
regime_map = load_regime_map()

# -------------------------------------------------
# DATA SELECTION
# -------------------------------------------------

if view_mode == "Earnings Day":

    if 'merged' not in locals() or merged.empty:
        st.info("No earnings data available.")
        st.stop()

    df_exp = merged.copy()

elif view_mode == "Full Universe":

    df_exp = panel.copy()

    df_exp["days_to_earnings"] = (
        pd.to_datetime(df_exp["earnings_date"])
        - pd.Timestamp.today()
    ).abs()

    df_exp = (
        df_exp
        .sort_values("days_to_earnings")
        .drop_duplicates(subset=["ticker"], keep="first")
    )

    df_exp["description"] = df_exp["ticker"]

else:

    df_exp = panel.copy()

    df_exp = df_exp[
        df_exp["ticker"].isin(["INTC", "DELL"])
    ]

    df_exp["days_to_earnings"] = (
        pd.to_datetime(df_exp["earnings_date"])
        - pd.Timestamp.today()
    ).abs()

    df_exp = (
        df_exp
        .sort_values("days_to_earnings")
        .drop_duplicates(subset=["ticker"], keep="first")
    )

    df_exp["description"] = df_exp["ticker"]

# -------------------------------------------------
# HARDEN REGIME COLUMNS
# -------------------------------------------------

for col in [
    "gamma_regime",
    "vix_regime",
    "dispersion_regime"
]:

    if col not in df_exp.columns:
        df_exp[col] = "UNKNOWN"

    df_exp[col] = (
        df_exp[col]
        .fillna("UNKNOWN")
        .astype(str)
    )

# -------------------------------------------------
# BUILD ANALOG REGIME
# -------------------------------------------------

df_exp["analog_regime"] = (

    df_exp["gamma_regime"]

    + "_"

    + df_exp["vix_regime"]

    + "_"

    + df_exp["dispersion_regime"]
)

# -------------------------------------------------
# TICKER ANALOG STATS
# -------------------------------------------------

def compute_ticker_stats(row):

    sub = hist[
        (hist["ticker"] == row["ticker"]) &
        (hist["analog_regime"] == row["analog_regime"])
    ]

    if sub.empty:

        return pd.Series([
            None,
            None,
            None,
            0
        ])

    return pd.Series([

        sub["realized_move_1d"].mean(),

        sub["realized_move_1d"].quantile(0.9),

        (
            sub["realized_move_1d"] > 0.03
        ).mean(),

        len(sub)

    ])

df_exp[[
    "t_avg",
    "t_p90",
    "t_brk",
    "t_obs"
]] = df_exp.apply(
    compute_ticker_stats,
    axis=1
)

# -------------------------------------------------
# MERGE CROSS-SECTIONAL ANALOGS
# -------------------------------------------------

df_exp = df_exp.merge(
    regime_map,
    on="analog_regime",
    how="left"
)

# -------------------------------------------------
# METRIC PICKER
# -------------------------------------------------

def pick_metric(
    row,
    ticker_col,
    cross_col,
    obs_col
):

    if row[obs_col] >= MIN_OBS:
        return row[ticker_col]

    return row[cross_col]

# -------------------------------------------------
# FINAL METRICS
# -------------------------------------------------

df_exp["expected_move"] = (

    df_exp.apply(

        lambda r: pick_metric(
            r,
            "t_avg",
            "avg_move",
            "t_obs"
        ),

        axis=1
    )

    * 100
)

df_exp["tail_move"] = (

    df_exp.apply(

        lambda r: pick_metric(
            r,
            "t_p90",
            "p90_move",
            "t_obs"
        ),

        axis=1
    )

    * 100
)

df_exp["break_prob"] = (

    df_exp.apply(

        lambda r: pick_metric(
            r,
            "t_brk",
            "break_rate",
            "t_obs"
        ),

        axis=1
    )

    * 100
)

df_exp["data_source"] = df_exp.apply(

    lambda r:

        "Ticker"

        if r["t_obs"] >= MIN_OBS

        else "Cross",

    axis=1
)

# -------------------------------------------------
# FALSE STABILITY
# -------------------------------------------------

df_exp["false_stability"] = (

    (df_exp["gex"] > 0)

    &

    (df_exp["break_prob"] > 30)

)

# -------------------------------------------------
# FORMATTERS
# -------------------------------------------------

def fmt_pct(v):

    if pd.isna(v):
        return "—"

    return f"{v:.1f}%"

def fmt_gex(v):

    if pd.isna(v):
        return "—"

    return f"{'-' if v < 0 else '+'}${abs(v)/1e6:,.1f}M"

def risk_label(row):

    if row["t_obs"] >= MIN_OBS:
        return "✔️ Strong (Ticker)"

    if row["n_obs"] >= MIN_OBS:
        return "✔️ Strong (Cross)"

    if row["gex"] < 0:
        return "⚠️ Short Gamma"

    return "⚠️ Low Sample"

# -------------------------------------------------
# DISPLAY TABLE
# -------------------------------------------------

df_show = df_exp.copy()

df_show["GEX"] = df_show["gex"].apply(fmt_gex)

df_show["Expected Move"] = (
    df_show["expected_move"]
    .apply(fmt_pct)
)

df_show["Tail Move (P90)"] = (
    df_show["tail_move"]
    .apply(fmt_pct)
)

df_show["Break Prob"] = (
    df_show["break_prob"]
    .apply(fmt_pct)
)

df_show["Risk"] = df_show.apply(
    risk_label,
    axis=1
)

df_show = df_show[[
    "ticker",
    "description",
    "GEX",
    "analog_regime",
    "Expected Move",
    "Tail Move (P90)",
    "Break Prob",
    "Risk",
    "data_source",
    "n_obs"
]]

df_show = df_show.rename(columns={

    "ticker": "Ticker",

    "description": "Name",

    "analog_regime": "Regime",

    "n_obs": "Obs",

    "data_source": "Source"

})

# -------------------------------------------------
# SAFE SORT
# -------------------------------------------------

if "Obs" in df_show.columns:

    df_show = df_show.sort_values(
        by="Obs",
        ascending=False
    )

# -------------------------------------------------
# RENDER
# -------------------------------------------------

st.dataframe(
    df_show,
    use_container_width=True
)

st.divider()

st.caption(
    "Data: Databento + Bloomberg via Postgres pipeline."
)
