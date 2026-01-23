import streamlit as st

# -------------------------------------------------
# PAGE CONFIG
# -------------------------------------------------
st.set_page_config(
    page_title="Encore Analytics Dashboards",
    layout="wide",
)

# -------------------------------------------------
# ENCORE BRAND STYLING (STREAMLIT-SAFE)
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

        .encore-muted {
            color: var(--encore-grey);
            font-size: 0.9rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# -------------------------------------------------
# HEADER / BRAND
# -------------------------------------------------
st.image("/mnt/data/Logomark.jpeg", width=160)

st.markdown(
    """
    ## Encore Analytics Dashboards

    <p class="encore-muted">
    A unified entry point for Encore’s internal analytics dashboards, providing
    real-time insight into portfolio positions, market conditions, and trading performance.
    </p>
    """,
    unsafe_allow_html=True,
)

st.divider()

# -------------------------------------------------
# DASHBOARD CARDS
# -------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    st.markdown(
        """
        <div class="dashboard-card">
            <div class="dashboard-title">📌 Positions Dashboard</div>
            <div class="dashboard-subtitle">
                Intraday portfolio monitoring by sector, cohort, and instrument.
            </div>
            <ul class="dashboard-list">
                <li>Sector-driven and price-driven views</li>
                <li>Long / short aware performance</li>
                <li>Comm/Tech cohort drill-downs</li>
                <li>30-minute intraday snapshots</li>
            </ul>
            <div class="dashboard-link">
                👉 <a href="https://encore-positionsdashboard.streamlit.app" target="_blank">
                    Open Positions Dashboard
                </a>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with col2:
    st.markdown(
        """
        <div class="dashboard-card">
            <div class="dashboard-title">🌍 Market Dashboard</div>
            <div class="dashboard-subtitle">
                Top-down market context and regime awareness.
            </div>
            <ul class="dashboard-list">
                <li>Market breadth and factor moves</li>
                <li>Cross-asset signals</li>
                <li>Macro and thematic context</li>
            </ul>
            <div class="dashboard-link">
                👉 <a href="https://encore-marketdashboard.streamlit.app" target="_blank">
                    Open Market Dashboard
                </a>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# -------------------------------------------------
# SECOND ROW — TRP
# -------------------------------------------------
st.markdown(
    """
    <div class="dashboard-card">
        <div class="dashboard-title">
            📉 TRP Dashboard
            <span style="font-size:0.9rem; color:#9ca3af;">(Work in Progress)</span>
        </div>
        <div class="dashboard-subtitle">
            Trading-related performance, execution quality, and risk analytics.
        </div>
        <ul class="dashboard-list">
            <li>Trade-level performance attribution</li>
            <li>Risk-adjusted execution analysis</li>
            <li>Post-trade review metrics</li>
        </ul>
        <div class="dashboard-link">
            👉 <a href="https://encore-trpdashboard.streamlit.app" target="_blank">
                Open TRP Dashboard
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