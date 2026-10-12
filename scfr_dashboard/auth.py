import hmac
import streamlit as st

def require_password():
    try:
        expected = str(st.secrets['auth']['password'])
    except (KeyError, FileNotFoundError):
        st.error('Configure the app password in Streamlit Secrets before use.')
        st.stop()
    if not expected:
        st.error('An empty app password is not permitted.')
        st.stop()
    if st.session_state.get('scfr_authenticated'):
        return
    st.title('SCFR — private review')
    with st.form('login'):
        entered = st.text_input('Enter Password', type='password')
        submitted = st.form_submit_button('Login')
    if submitted:
        if hmac.compare_digest(entered.encode(), expected.encode()):
            st.session_state['scfr_authenticated'] = True
            st.rerun()
        st.error('Incorrect password')
    st.stop()
