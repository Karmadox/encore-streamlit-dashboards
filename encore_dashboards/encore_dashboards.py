import streamlit as st
from pathlib import Path
import streamlit.components.v1 as components

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
# PAGE CONFIG
# -------------------------------------------------
st.set_page_config(
    page_title="Encore Analytics Dashboards",
    layout="wide",
)

# -------------------------------------------------
# ENCORE BRAND STYLING
# -------------------------------------------------
st.markdown(
    """
    <style>
        :root {
            --encore-green: #1a7f37;
            --encore-green-light: #4caf50;
            --encore-grey: #6b7280;
            --encore-border: #e5e7eb;
        }

        .dashboard-card {
            border: 1px solid var(--encore-border);
            border-radius: 14px;
            padding: 22px;
            margin-bottom: 22px;
            background-color: #ffffff;
            box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        }

        .dashboard-title {
            font-size: 1.35rem;
            font-weight: 700;
            color: var(--encore-green);
            margin-bottom: 6px;
        }

        .dashboard-subtitle {
            font-size: 0.95rem;
            color: var(--encore-grey);
            margin-bottom: 14px;
        }

        .dashboard-list {
            margin-left: 18px;
            color: #111827;
            font-size: 0.95rem;
        }

        .dashboard-link a {
            display: inline-block;
            margin-top: 10px;
            font-weight: 600;
            text-decoration: none;
            color: var(--encore-green);
        }

        .dashboard-link a:hover {
            color: var(--encore-green-light);
            text-decoration: underline;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# -------------------------------------------------
# HEADER
# -------------------------------------------------
svg_path = Path(__file__).parent / "assets" / "Logomark.svg"
svg_code = svg_path.read_text()

header_html = f"""
<div style="
    display:flex;
    align-items:center;
    gap:24px;
    padding:8px 4px 16px 4px;
">
    <div style="width:140px; flex-shrink:0;">
        {svg_code}
    </div>

    <div>
        <h2 style="
            margin:0;
            font-size:1.9rem;
            font-weight:700;
            color:#1a7f37;
        ">
            Encore Analytics Dashboards
        </h2>

        <p style="
            margin-top:6px;
            font-size:0.95rem;
            color:#6b7280;
            max-width:720px;
        ">
            A unified entry point for Encore’s internal analytics dashboards, providing
            real-time insight into portfolio positions, market conditions, trading activity,
            and performance attribution.
        </p>
    </div>
</div>
"""

components.html(header_html, height=160)
st.divider()

# -------------------------------------------------
# ROW 1 — CORE MARKET & INDEX
# -------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""...""", unsafe_allow_html=True)

with col2:
    st.markdown("""...""", unsafe_allow_html=True)

with col3:
    st.markdown("""...""", unsafe_allow_html=True)

# -------------------------------------------------
# ROW 2 — OPERATIONS & CONTROLS
# -------------------------------------------------
col4, col5, col6 = st.columns(3)

with col4:
    st.markdown("""...""", unsafe_allow_html=True)

with col5:
    st.markdown("""...""", unsafe_allow_html=True)

# -------------------------------------------------
# NEW TRADE BLOTTER CARD
# -------------------------------------------------
with col6:
    st.markdown(
        """
        <div class="dashboard-card">
            <div class="dashboard-title">📊 Trade Blotter (FIFO)</div>
            <div class="dashboard-subtitle">
                Complete trade-level ledger with FIFO accounting and real-time P&L tracking.
            </div>
            <ul class="dashboard-list">
                <li>Full transaction history by ticker</li>
                <li>Long & short-aware FIFO matching</li>
                <li>Running position and gross notional tracking</li>
                <li>Realized, unrealized & total P&L visibility</li>
                <li>Institutional-grade trade audit trail</li>
            </ul>
            <div class="dashboard-link">
                👉 <a href="https://encore-tradeblotter.streamlit.app" target="_blank">
                    Open Trade Blotter
                </a>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# -------------------------------------------------
# FOOTER
# -------------------------------------------------
st.divider()
st.caption(
    "Encore Analytics • Internal use only • "
    "For questions or enhancements, contact the analytics team."
)
