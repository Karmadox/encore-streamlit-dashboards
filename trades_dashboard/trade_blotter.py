import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from decimal import Decimal

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

# ==========================================================
# DATABASE CONFIG
# ==========================================================

DB_CONFIG = st.secrets["db"]

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

# ==========================================================
# DATABASE HELPERS
# ==========================================================

def get_all_tickers():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT i.ticker
        FROM encoredb.trades t
        JOIN encoredb.instruments i
            ON t.instrument_id = i.instrument_id
        ORDER BY i.ticker
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [r[0] for r in rows]


def load_trades_for_ticker(ticker):
    conn = get_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("""
        SELECT
            t.trade_id,
            t.trade_date,
            i.ticker,
            t.quantity,
            t.price,
            t.gross_commissions,
            t.gross_fees,
            t.gross_taxes
        FROM encoredb.trades t
        JOIN encoredb.instruments i
            ON t.instrument_id = i.instrument_id
        WHERE i.ticker = %s
        ORDER BY t.trade_date, t.trade_id
    """, (ticker,))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return pd.DataFrame(rows)

# ==========================================================
# ENHANCED LONG + SHORT FIFO ENGINE
# ==========================================================

def build_fifo_ledger(df):

    open_long_lots = []
    open_short_lots = []

    running_position = Decimal("0")
    realized_pnl_total = Decimal("0")

    ledger_rows = []

    for _, row in df.iterrows():

        qty = Decimal(str(row["quantity"]))
        price = Decimal(str(row["price"]))

        trade_notional = qty * price
        realized_pnl = Decimal("0")

        side_label = ""

        # ================================
        # BUY
        # ================================
        if qty > 0:

            buy_qty = qty

            if open_short_lots:
                side_label = "BUY (Cover Short)"
            else:
                side_label = "BUY (Open Long)"

            # Close shorts first
            while buy_qty > 0 and open_short_lots:
                lot = open_short_lots[0]
                matched = min(lot["remaining_qty"], buy_qty)

                pnl = matched * (lot["price"] - price)
                realized_pnl += pnl

                lot["remaining_qty"] -= matched
                buy_qty -= matched

                if lot["remaining_qty"] == 0:
                    open_short_lots.pop(0)

            # Open new long
            if buy_qty > 0:
                open_long_lots.append({
                    "remaining_qty": buy_qty,
                    "price": price
                })

            running_position += qty

        # ================================
        # SELL
        # ================================
        else:

            sell_qty = abs(qty)

            if open_long_lots:
                side_label = "SELL (Close Long)"
            else:
                side_label = "SELL (Open Short)"

            # Close longs first
            while sell_qty > 0 and open_long_lots:
                lot = open_long_lots[0]
                matched = min(lot["remaining_qty"], sell_qty)

                pnl = matched * (price - lot["price"])
                realized_pnl += pnl

                lot["remaining_qty"] -= matched
                sell_qty -= matched

                if lot["remaining_qty"] == 0:
                    open_long_lots.pop(0)

            # Open new short
            if sell_qty > 0:
                open_short_lots.append({
                    "remaining_qty": sell_qty,
                    "price": price
                })

            running_position += qty

        realized_pnl_total += realized_pnl

        # ================================
        # Unrealized PnL (mark to current trade price)
        # ================================
        unrealized_pnl = Decimal("0")

        for lot in open_long_lots:
            unrealized_pnl += lot["remaining_qty"] * (price - lot["price"])

        for lot in open_short_lots:
            unrealized_pnl += lot["remaining_qty"] * (lot["price"] - price)

        # ================================
        # Gross Notional
        # ================================
        gross_notional = abs(running_position) * price

        # ================================
        # Avg Cost (long only)
        # ================================
        remaining_cost = sum(
            lot["remaining_qty"] * lot["price"]
            for lot in open_long_lots
        )

        avg_cost = (
            remaining_cost / running_position
            if running_position > 0
            else Decimal("0")
        )

        ledger_rows.append({
            "Trade Date": row["trade_date"],
            "Side": side_label,
            "Quantity": float(qty),
            "Price": float(price),
            "Trade Notional": float(trade_notional),
            "Gross Notional": float(gross_notional),
            "Running Position": float(running_position),
            "Avg Cost Basis": float(avg_cost),
            "Realized PnL (Trade)": float(realized_pnl),
            "Total Realized PnL": float(realized_pnl_total),
            "Total Unrealized PnL": float(unrealized_pnl)
        })

    ledger_df = pd.DataFrame(ledger_rows)

    summary = {
        "Final Position": float(running_position),
        "Total Realized PnL": float(realized_pnl_total),
        "Final Unrealized PnL": float(unrealized_pnl),
        "Open Long Lots": len(open_long_lots),
        "Open Short Lots": len(open_short_lots)
    }

    return ledger_df, summary

# ==========================================================
# SMART NUMBER FORMATTER
# ==========================================================

def smart_format(x):
    if pd.isna(x):
        return ""
    try:
        x = float(x)
    except:
        return x

    if x.is_integer():
        return f"{x:,.0f}"
    else:
        return f"{x:,.2f}"

# ==========================================================
# STREAMLIT UI
# ==========================================================

st.set_page_config(layout="wide")
st.title("📊 FIFO Trade Blotter Ledger")

tickers = get_all_tickers()
selected_ticker = st.selectbox("Select Ticker", tickers)

if selected_ticker:

    df = load_trades_for_ticker(selected_ticker)

    if df.empty:
        st.warning("No trades found for this ticker.")
    else:
        ledger_df, summary = build_fifo_ledger(df)

        st.subheader(f"Trade Ledger — {selected_ticker}")

        numeric_cols = [
            "Quantity",
            "Price",
            "Trade Notional",
            "Gross Notional",
            "Running Position",
            "Avg Cost Basis",
            "Realized PnL (Trade)",
            "Total Realized PnL",
            "Total Unrealized PnL"
        ]

        st.dataframe(
            ledger_df.style.format(
                {col: smart_format for col in numeric_cols}
            ),
            use_container_width=True
        )

        st.subheader("Summary")

        col1, col2, col3 = st.columns(3)

        col1.metric("Final Position", smart_format(summary["Final Position"]))
        col2.metric("Total Realized PnL", f"${smart_format(summary['Total Realized PnL'])}")
        col3.metric("Unrealized PnL", f"${smart_format(summary['Final Unrealized PnL'])}")

