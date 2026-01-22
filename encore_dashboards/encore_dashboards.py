import streamlit as st

# -------------------------------------------------
# PAGE CONFIG
# -------------------------------------------------
st.set_page_config(
    page_title="Encore Analytics Dashboards",
    layout="wide",
)

# -------------------------------------------------
# BASIC BRAND STYLING (SAFE FOR STREAMLIT CLOUD)
# -------------------------------------------------
st.markdown(
    """
    <style>
        .dashboard-card {
            border: 1px solid #e6e6e6;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
            background-color: #ffffff;
        }
        .dashboard-title {
            font-size: 1.3rem;
            font-weight: 700;
            margin-bottom: 6px;
        }
        .dashboard-subtitle {
            font-size: 0.95rem;
            color: #555555;
            margin-bottom: 12px;
        }
        .dashboard-list {
            margin-left: 16px;
            color: #333333;
        }
        .dashboard-link a {
            font-weight: 600;
            text-decoration: none;
            color: #0f4c81;
        }
        .dashboard-link a:hover {
            text-decoration: underline;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# -------------------------------------------------
# HEADER
# -------------------------------------------------
st.markdown("## 📊 Encore Analytics Dashboards")
st.markdown(
    """
    A unified entry point for Encore’s internal analytics dashboards, providing
    real-time insight into portfolio positions, market conditions, and trading performance.
    """
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
# SECOND ROW (TRP / WIP)
# -------------------------------------------------
st.markdown(
    """
    <div class="dashboard-card">
        <div class="dashboard-title">📉 TRP Dashboard <span style="font-size:0.9rem; color:#999;">(Work in Progress)</span></div>
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