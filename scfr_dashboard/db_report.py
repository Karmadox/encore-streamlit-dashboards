"""Authenticated live-source view. Does not load the private snapshot."""
import streamlit as st
import pandas as pd
from db_source import STOCKS, completed_day, read_stock, DataUnavailable

@st.cache_data(ttl=300, max_entries=30, show_spinner=False)
def cached_stock(ticker, end, connection_settings):
    # Settings are hashed as part of the cache key; they are never returned or logged.
    return read_stock(connection_settings, ticker, end)

def render():
    st.title('SCFR — EncoreDB stock coverage')
    st.caption('Direct database view · Selected companies under review · Latest completed-day observations')
    if 'db' not in st.secrets:
        st.info('Add the existing [db] connection settings to this app’s private Streamlit secrets to enable direct reads. The saved pilot remains available in the Report source selector.')
        return
    if 'db_active_stock' not in st.session_state:
        st.session_state.db_active_stock='INTC'
    with st.form('db_stock_selection'):
        left,right=st.columns([3,1],vertical_alignment='bottom')
        selected=left.selectbox('Stock',STOCKS,index=STOCKS.index(st.session_state.db_active_stock),key='db_selected_stock')
        update=right.form_submit_button('Update report')
    settings=dict(st.secrets['db'])
    end=completed_day()
    if update:
        st.session_state.db_active_stock=selected
        cached_stock.clear(selected,end,settings)
    ticker=st.session_state.db_active_stock
    try:
        with st.spinner('Reading EncoreDB…'):
            result=cached_stock(ticker,end,settings)
    except DataUnavailable as exc:
        st.error(str(exc))
        return
    rows=result['rows']
    if not rows:
        st.warning(f'{ticker}: no retained rows in the requested interval.')
        return
    st.subheader(f'Currently showing: {ticker}')
    st.caption(f'Latest retained date: {rows[-1]["date"]} | Database read (UTC): {result["queried_at"]} | Update report bypasses the five-minute cache. It does not run ingestion or request Bloomberg data.')
    with st.expander('How to read this report',expanded=False):
        st.write('Prices and volume come directly from EncoreDB after login. Each point represents a stored date. The app excludes the current New York day, retains null values and never fills gaps. Stored-date coverage is not a claim of complete trading-session coverage.')
        st.write('The price chart shows the stored close, whose corporate-action adjustment basis is not yet established. Volume shows stored share counts with the existing pilot units assumption. The saved pilot contains the previously reviewed 3/9/27-session analysis; the live view does not extend its action assumptions to new stocks or dates.')
    report,evidence=st.tabs(['Database observations','Assumptions and coverage'])
    with report:
        st.info('Current SCFR interpretation: prices and activity are available for inspection. Live total returns, split-adjusted per-share returns and 3/9/27 return signals await compatible corporate-action evidence and price-adjustment treatment.')
        display=[r for r in rows if r['date']>='2026-01-01']
        if not display:
            st.warning('No observations in the 2026 display period.')
            return
        for key,title in [('close','Stored close — USD/share'),('volume','Stored volume — assumed shares')]:
            st.subheader(title)
            missing=sum(r[key] is None for r in display)
            share=missing/len(display)
            st.caption(f'Missing values on stored display dates: {missing}/{len(display)} ({share:.1%}). Absent session rows are not included in this count; calendar completeness has not been established.')
            if share>0.2:
                st.warning('Panel withheld: more than 20% of stored display dates have missing values.')
                continue
            if share>0.05:
                st.warning('More than 5% of stored display dates have missing values.')
            frame=pd.DataFrame({'date':pd.to_datetime([r['date'] for r in display]),'value':[float(r[key]) if r[key] is not None else None for r in display]})
            st.vega_lite_chart(frame,{'mark':{'type':'line','invalid':'break-paths-show-domains'},'encoding':{'x':{'field':'date','type':'temporal'},'y':{'field':'value','type':'quantitative'},'tooltip':[{'field':'date','type':'temporal'},{'field':'value','type':'quantitative'}]}},use_container_width=True)
    with evidence:
        st.write(f'EncoreDB instrument: {result["instrument_id"]}; Bloomberg coordinate: {result["bbg_security"]}.')
        st.write(f'Retained interval: {rows[0]["date"]} to {rows[-1]["date"]}; {len(rows)} rows. Requested upper bound: {result["requested_end"]}.')
        st.write('The table is current retained history, not historical point-in-time evidence. Prior source review identified adjustment-setting uncertainty and a writer capable of replacing closes. Missing volume is not zero trading activity. No corporate-action store is read by this view.')
        st.write('The 5%/20% stored-row display checks here are provisional coverage warnings, not the approved session-grid A10 calculation. Full session-calendar denominators and action-aware returns remain work in progress.')
        st.write('Authentication precedes every data path. Reads use TLS and a read-only transaction, followed by rollback and close; the app performs no database writes. Existing scheduled ingestion remains responsible for freshness.')
