"""
Dealer Gamma (GEX) Dashboard — per-ticker, per-day.

Read-only consumer of:
  outputs/risk/gex_panel.parquet              ← built by scripts/risk/build_gex_panel.py
  earnings-data/<date>/names.csv              ← built by scripts/thematic/build_forward_hedge_data.py

Run:
  streamlit run apps/dealer_gamma_app.py --server.port 7777

Three sections, mirroring the Earnings Risk Management dashboard layout:
  1. Today / selected day — names reporting + GEX read
  2. Upcoming (next 7 trading days that have at least one EGM earnings event)
  3. Recent prints (last 7 trading days that had an event), GEX measured pre-event
"""

from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
PANEL_PATH = ROOT / "outputs" / "risk" / "gex_panel.parquet"
EARNINGS_ROOT = ROOT / "earnings-data"

st.set_page_config(
    page_title="Dealer Gamma (GEX) Dashboard",
    page_icon="📊",
    layout="wide",
)


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=60)
def load_panel() -> pd.DataFrame:
    if not PANEL_PATH.exists():
        return pd.DataFrame()
    df = pd.read_parquet(PANEL_PATH)
    df["earnings_date"] = pd.to_datetime(df["earnings_date"]).dt.date
    df["asof_date"] = pd.to_datetime(df["asof_date"]).dt.date
    return df


@st.cache_data(ttl=60)
def load_names_for_date(date_str: str) -> pd.DataFrame:
    p = EARNINGS_ROOT / date_str / "names.csv"
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p)


@st.cache_data(ttl=60)
def list_event_dates() -> list[str]:
    if not EARNINGS_ROOT.exists():
        return []
    return sorted(p.name for p in EARNINGS_ROOT.iterdir()
                  if p.is_dir() and len(p.name) == 10)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _gex_dollar_M(v):
    if pd.isna(v):
        return "—"
    return f"{'-' if v < 0 else '+'}${abs(v)/1e6:,.2f}M"


def _dealer_pos(v):
    if pd.isna(v) or abs(v) < 1e3:
        return "flat"
    return "+long γ" if v > 0 else "−short γ"


def _enrich_with_panel(names: pd.DataFrame, panel: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """Merge per-ticker GEX onto the names DataFrame for one event date."""
    if names.empty:
        return names
    sub = panel[panel["earnings_date"].astype(str) == date_str]
    if sub.empty:
        out = names.copy()
        for c in ["spot", "gex", "gex_call", "gex_put", "n_strikes", "n_expiries"]:
            out[c] = pd.NA
        return out
    return names.merge(
        sub[["ticker", "spot", "gex", "gex_call", "gex_put", "n_strikes", "n_expiries"]],
        on="ticker", how="left",
    )


def _gex_table(df: pd.DataFrame, *, show_event_date: bool = False) -> pd.DataFrame:
    """Slim down to the columns we display + apply consistent formatting."""
    if df.empty:
        return df
    cols = []
    if show_event_date and "earnings_date" in df.columns:
        cols.append("earnings_date")
    cols += ["ticker", "egm_sector"] if "egm_sector" in df.columns else ["ticker"]
    if "real_value" in df.columns:
        cols.append("real_value")
    cols += ["spot", "gex", "gex_call", "gex_put", "n_strikes", "n_expiries"]
    keep = [c for c in cols if c in df.columns]
    out = df[keep].copy()

    # Compute dealer positioning sign as a separate column for at-a-glance
    if "gex" in out.columns:
        out["dealer γ"] = out["gex"].apply(_dealer_pos)

    rename = {
        "earnings_date": "Earnings",
        "ticker": "Ticker",
        "egm_sector": "Sector",
        "real_value": "NMV",
        "spot": "Spot",
        "gex": "GEX (total)",
        "gex_call": "GEX (calls)",
        "gex_put": "GEX (puts)",
        "n_strikes": "Strikes",
        "n_expiries": "Expiries",
    }
    out = out.rename(columns=rename)
    return out


def _format_table(df: pd.DataFrame):
    if df.empty:
        return df
    fmt = {}
    if "Spot" in df.columns:
        fmt["Spot"] = lambda v: f"${v:,.2f}" if pd.notna(v) else "—"
    if "NMV" in df.columns:
        fmt["NMV"] = lambda v: (f"{'-' if v < 0 else '+'}${abs(v)/1e6:,.2f}M") if pd.notna(v) else "—"
    for c in ("GEX (total)", "GEX (calls)", "GEX (puts)"):
        if c in df.columns:
            fmt[c] = _gex_dollar_M
    for c in ("Strikes", "Expiries"):
        if c in df.columns:
            fmt[c] = lambda v: f"{int(v)}" if pd.notna(v) else "—"
    return df.style.format(fmt)


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

st.title("📊 Dealer Gamma (GEX) Dashboard")
st.caption(
    "Per-ticker dealer-gamma exposure for EGM portfolio names with upcoming or recent earnings events. "
    "Computed from Databento OPRA chain definitions + cbbo-1m quotes + open interest (statistics, stat_type=9). "
    "Sign convention: +1 calls, −1 puts (dealers long calls, short puts). "
    "Magnitudes are dollar-gamma per 1% spot move."
)

panel = load_panel()
event_dates = list_event_dates()
if not event_dates:
    st.warning("No earnings-data dates available. Run `scripts/thematic/build_forward_hedge_data.py` first.")
    st.stop()
if panel.empty:
    st.warning("GEX panel is empty. Run `scripts/risk/build_gex_panel.py --window 14` to populate.")
    st.stop()

st.markdown(f"**Panel coverage:** {len(panel)} ticker-day cells across "
            f"{panel['earnings_date'].nunique()} event dates · "
            f"latest computed_at: {panel['computed_at'].max() if 'computed_at' in panel.columns else 'n/a'}")

today = pd.Timestamp.today().normalize().date()
upcoming = sorted([d for d in event_dates if pd.Timestamp(d).date() >= today])
past = sorted([d for d in event_dates if pd.Timestamp(d).date() < today], reverse=True)


# ---------- Section 1 — Today / selected day ----------
st.markdown("## 🎯 Selected Day — Names Reporting + Dealer Gamma")
default_date = upcoming[0] if upcoming else past[0]
sel_date = st.selectbox(
    "Earnings date",
    options=upcoming + past,
    index=(upcoming + past).index(default_date),
)
names = load_names_for_date(sel_date)
merged = _enrich_with_panel(names, panel, sel_date)

if names.empty:
    st.info(f"No names listed for {sel_date}.")
else:
    n_with_gex = merged["gex"].notna().sum() if "gex" in merged.columns else 0
    c1, c2, c3 = st.columns(3)
    c1.metric("Names reporting", f"{len(merged)}")
    c2.metric("With GEX read", f"{n_with_gex} / {len(merged)}")
    if n_with_gex > 0:
        agg = merged["gex"].sum()
        c3.metric("Aggregate GEX", _gex_dollar_M(agg))
    st.dataframe(
        _format_table(_gex_table(merged)),
        width="stretch",
        hide_index=False,
    )


# ---------- Section 2 — Upcoming (next 7 event dates) ----------
st.markdown("## 📅 Upcoming Earnings — GEX Reads")
upcoming_window = upcoming[:7]
if not upcoming_window:
    st.info("No upcoming earnings dates in the panel.")
else:
    rows = []
    for d in upcoming_window:
        names_d = load_names_for_date(d)
        if names_d.empty:
            continue
        rows.append(_enrich_with_panel(names_d.assign(earnings_date=d), panel, d))
    upc = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    st.dataframe(
        _format_table(_gex_table(upc, show_event_date=True)),
        width="stretch",
        hide_index=False,
    )


# ---------- Section 3 — Recent prints (most recent 7 event dates) ----------
st.markdown("## 📉 Recent Earnings — GEX Reads (Pre-Event)")
past_window = past[:7]
if not past_window:
    st.info("No past earnings dates in the panel.")
else:
    rows = []
    for d in past_window:
        names_d = load_names_for_date(d)
        if names_d.empty:
            continue
        rows.append(_enrich_with_panel(names_d.assign(earnings_date=d), panel, d))
    rec = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    st.dataframe(
        _format_table(_gex_table(rec, show_event_date=True)),
        width="stretch",
        hide_index=False,
    )


# ---------- Footer ----------
st.divider()
st.caption(
    "Data: Databento OPRA.PILLAR · "
    "Builder: `scripts/risk/build_gex_panel.py` (calls `databento_gex.gex_for(ticker, asof_date)`) · "
    "Asof = last close before earnings date · "
    "Filters: ≤60 DTE, ±30% moneyness, OI ≥ 1"
)
