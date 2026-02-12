import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from decimal import Decimal, ROUND_HALF_UP

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
            t.price
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
# FIFO ENGINE (LONG + SHORT)
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

        # ==============================
        # BUY
        # ==============================
        if qty > 0:

            buy_qty = qty
            side_label = "BUY (Open Long)" if not open_short_lots else "BUY (Cover Short)"

            while buy_qty > 0 and open_short_lots:
                lot = open_short_lots[0]
                matched = min(lot["remaining_qty"], buy_qty)

                pnl = matched * (lot["price"] - price)
                realized_pnl += pnl

                lot["remaining_qty"] -= matched
                buy_qty -= matched

                if lot["remaining_qty"] == 0:
                    open_short_lots.pop(0)

            if buy_qty > 0:
                open_long_lots.append({
                    "remaining_qty": buy_qty,
                    "price": price
                })

            running_position += qty

        # ==============================
        # SELL
        # ==============================
        else:

            sell_qty = abs(qty)
            side_label = "SELL (Close Long)" if open_long_lots else "SELL (Open Short)"

            while sell_qty > 0 and open_long_lots:
                lot = open_long_lots[0]
                matched = min(lot["remaining_qty"], sell_qty)

                pnl = matched * (price - lot["price"])
                realized_pnl += pnl

                lot["remaining_qty"] -= matched
                sell_qty -= matched

                if lot["remaining_qty"] == 0:
                    open_long_lots.pop(0)

            if sell_qty > 0:
                open_short_lots.append({
                    "remaining_qty": sell_qty,
                    "price": price
                })

            running_position += qty

        realized_pnl_total += realized_pnl

        # ==============================
        # Unrealized PnL (mark to trade price)
        # ==============================
        unrealized_pnl = Decimal("0")

        for lot in open_long_lots:
            unrealized_pnl += lot["remaining_qty"] * (price - lot["price"])

        for lot in open_short_lots:
            unrealized_pnl += lot["remaining_qty"] * (lot["price"] - price)

        total_pnl = realized_pnl_total + unrealized_pnl
        gross_notional = abs(running_position) * price

        # Consistent rounding
        def r(x):
            return float(x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

        ledger_rows.append({
            "Trade Date": row["trade_date"],
            "Side": side_label,
            "Quantity": r(qty),
            "Price": r(price),
            "Trade Notional": r(trade_notional),
            "Gross Notional": r(gross_notional),
            "Running Position": r(running_position),
            "Realized PnL (Trade)": r(realized_pnl),
            "Total Realized PnL": r(realized_pnl_total),
            "Total Unrealized PnL": r(unrealized_pnl),
            "Total PnL (Realized + Unrealized)": r(total_pnl)
        })

    ledger_df = pd.DataFrame(ledger_rows)

    summary = {
        "Final Position": r(running_position),
        "Total Realized PnL": r(realized_pnl_total),
        "Final Unrealized PnL": r(unrealized_pnl),
        "Total PnL": r(total_pnl)
    }

    return ledger_df, summary

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
        st.warning("No trades found.")
    else:
        ledger_df, summary = build_fifo_ledger(df)

        st.subheader(f"Trade Ledger — {selected_ticker}")
        st.dataframe(ledger_df, use_container_width=True)

        st.subheader("Summary")

        col1, col2, col3, col4 = st.columns(4)

        col1.metric("Final Position", f"{summary['Final Position']:,.2f}")
        col2.metric("Realized PnL", f"${summary['Total Realized PnL']:,.2f}")
        col3.metric("Unrealized PnL", f"${summary['Final Unrealized PnL']:,.2f}")
        col4.metric("Total PnL", f"${summary['Total PnL']:,.2f}")



