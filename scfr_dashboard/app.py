import streamlit as st
st.set_page_config(page_title="SCFR | private review",layout="wide")
import pandas as pd
from auth import require_password
require_password()
from build import stocks,panels,ASSUMPTIONS,download
from model import panel_status
from reading import GUIDE,summary

st.title('INTC / DELL — descriptive PoC')
st.warning('REAL DATA · ASSUMPTION-QUALIFIED DRAFT · Private review only. Returns depend on assumed action completeness and share-line continuity.')
st.caption('2026 through 2 October | Retrospective selected examples | No predictive, causal or investment claims')
if 'active_stock' not in st.session_state:st.session_state.active_stock='INTC'
with st.form('stock_selection'):
 left,right=st.columns([3,1])
 selected=left.selectbox('Stock',['INTC','DELL'],key='selected_stock')
 submitted=right.form_submit_button('Update report')
if submitted:st.session_state.active_stock=selected
name=st.session_state.active_stock
rows=stocks()[name]
st.subheader(f'Currently showing: {name}')
st.caption(f'Last saved observation: {rows[-1]["date"]} | {name} PX_LAST: ${float(rows[-1]["close"]):,.2f}. Select a stock and press Update report. This changes the displayed saved data; it does not refresh Bloomberg.')
st.subheader('How to read this report')
for label,description in GUIDE:st.markdown(f'**{label}:** {description}')
st.subheader(f'{name} — SCFR observations at the latest origin')
latest,notes=summary(rows)
st.caption(f'Origin {latest[3]["date"]}; trailing inputs end {latest[3]["latest_input"]}. Price comparison includes the last saved observation. Conditional on the stated assumptions.')
cols=st.columns(3)
for col,h in zip(cols,(3,9,27)):col.metric(f'{h}-session total return',f'{float(latest[h]["total"]):.2%}')
for note in notes:st.write('• '+note)
st.sidebar.caption('PX_LAST convention accepted for pilot per Rob’s report of Derek’s confirmation. Official auction-close equivalence is not asserted.')
for file,label,mime in [('Report.html','Download complete report','text/html'),('Features.csv','Download both-stock features','text/csv'),('Assumptions.txt','Download assumptions','text/plain')]:
 st.download_button(label,download(file),file,mime)
report,evidence,findings=st.tabs(['Stock report','Assumptions and coverage','Observations'])
with report:
 st.info('Lagged 3/9/27 features end at the previous trading session. The cumulative curve uses the observation date and a 31 Dec 2025 base. These are different time conventions.')
 for title,dates,values,pct in panels(rows):
  st.subheader(name+' — '+title);stats=panel_status(values)
  st.caption(f'Missing {stats["nulls"]}/{stats["origins"]} ({stats["share"]:.1%}); all display origins retained. Conditional on stated assumptions.')
  if stats['withheld']:st.warning('Withheld above 20% missingness.');continue
  if stats['density_note']:st.warning('Missingness exceeds 5%.')
  df=pd.DataFrame({'date':pd.to_datetime(dates),'value':[float(v)*(100 if pct else 1) if v is not None else None for v in values]})
  st.vega_lite_chart(df,{'mark':{'type':'line','invalid':'break-paths-show-domains'},'encoding':{'x':{'field':'date','type':'temporal'},'y':{'field':'value','type':'quantitative','title':'Percent' if pct else 'Value'},'tooltip':[{'field':'date','type':'temporal'},{'field':'value','type':'quantitative'}]}},use_container_width=True)
with evidence:
 for a in ASSUMPTIONS:st.write('• '+a)
 st.write('503 matched input sessions per stock. INTC lacks price/volume on 1 October 2024; that does not affect the 2026 trailing windows. All 189 display origins have complete price dependencies. Source provenance is retained locally; no live refresh is connected.')
with findings:st.write(download('Observations.txt').decode('utf-8'));st.info('Explore cross-scale persistence and reversals next. No benchmark or cohort controls are included.')
