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
# ENHANCED FIFO ENGINE (LONG + SHORT)
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
            side_label = "SELL (Close Long)" if open_long_lots else "SELL (Open Short)"_


