from pathlib import Path
import json,sys
import pandas as pd
import streamlit as st
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from foundation_monitor.engine import validate,oracle_metrics,shadow_rules,contagion_state,capture_age_hours
from foundation_monitor.store import read_snapshot

@st.cache_data(ttl=300)
def load():
 # Enable DB mode explicitly after provisioning. Never silently substitute a file on DB failure.
 import os
 if os.environ.get('FOUNDATION_MONITOR_DB')=='1':
  import psycopg2
  config=dict(st.secrets['db']);config.update(connect_timeout=10,sslmode='require')
  conn=psycopg2.connect(**config)
  try:return read_snapshot(conn),'EncoreDB • read-only snapshot query'
  finally:conn.close()
 return validate(json.loads((Path(__file__).parent/'data/snapshot.json').read_text())),'Sealed review snapshot • no automatic data refresh'

st.title('AI Infrastructure Foundation Monitor')
st.caption('Review release · Evidence first · Shadow diagnostics only')
try:snapshot,storage=load()
except Exception:
 st.error('Evidence store unavailable. No cached or empty result is being presented as current evidence.');st.stop()
rows=snapshot['observations'];rules=shadow_rules(rows);metrics=oracle_metrics(rows)
st.warning('Contagion assessment: '+contagion_state(rules)+' — the available evidence cannot establish whether CRWV stress is isolated or spreading.')
st.caption(f"{storage} | Evidence captured {snapshot['built_at'][:10]} | {snapshot['snapshot_id']}")
st.info('This first review is a curated baseline, not a live market monitor. Credit feeds, comparable financing history and basket returns are not connected. Source publication times and historical availability have not been established.')
with st.expander('How to read this monitor',expanded=False):
 st.write('Start with the observations below, then inspect each family. Reconciled means checked against the cited document—not independently audited or current. A historical filing remains historical even when downloaded today. Open the evidence register for dates, source locations and hashes.')
 st.write('UNAVAILABLE means an input is absent. INSUFFICIENT_EVIDENCE means the available input cannot support the requested inference. Neither means no stress. Shadow diagnostics never send notifications or drive trading decisions.')
 st.write('Funding commitments are not cash drawn; fit-out receipts are not recurring rent. GPU offers are not transactions, available capacity or utilization. No automatic refresh schedule is enabled in this review release.')
st.subheader('What the evidence currently tells us')
a,b,c=st.columns(3)
a.metric('ORCL capex covered by OCF',f"{metrics['OCF coverage']:.1%}",help='Quarter ended Aug 31, 2026. Includes specified customer prepayments.')
b.metric('Coverage excluding prepayments',f"{metrics['Coverage excluding specified prepayments']:.1%}")
c.metric('ORCL capex less OCF',f"${metrics['Capex less operating cash flow']/1000:.3f}bn",help='Cash-flow gap before financing; not a forecast of required new capital.')
st.write('**Review observations:** Oracle’s funding mix warrants monitoring; it does not itself establish credit deterioration. Applied Digital’s historical receipts include substantial fit-out activity. IREN financing depends on equipment acceptance. These are exposure channels to watch, not demonstrated contagion.')
labels=['Oracle','Project finance','Applied Digital','Neocloud','GPU economics','Evidence & gaps']
tabs=st.tabs(labels)
def evidence(family):
 subset=[r for r in rows if r['family']==family]
 df=pd.DataFrame(subset)
 st.dataframe(df[['entity','metric','value','unit','period','quality','note']],hide_index=True,width='stretch')
 for source in sorted({r['source_id'] for r in subset}):
  s=next(s for s in snapshot['sources'] if s['source_id']==source)
  st.markdown(f"[Source: {source}]({s['url']}) · captured {s['retrieved_at']}")
with tabs[0]:
 st.subheader('Oracle · funding mix is observable; credit stress is not yet measured')
 evidence('Oracle')
 st.bar_chart(pd.DataFrame({'Coverage':[metrics['Coverage excluding specified prepayments'],metrics['Specified prepayment coverage']]},index=['OCF excluding specified prepayments','Specified prepayments']),horizontal=True)
 st.caption('Derived ratios use USD millions for the same quarter. Prepayments are already in OCF. OCF minus specified prepayments is a diagnostic, not a GAAP measure.')
 with st.expander('Calculation details'):
  st.code('OCF coverage = 23,103 / 28,499\nCoverage excluding specified prepayments = (23,103 − 11,363) / 28,499\nPrepayment coverage = 11,363 / 28,499\nCapex less OCF = 28,499 − 23,103 = 5,396 (USD million)')
 st.error('UNAVAILABLE: CDS, bond OAS, peer-relative credit and qualified common/preferred return series.')
with tabs[1]:
 st.subheader('Project finance · one disclosed facility, no repricing conclusion')
 evidence('Project finance')
 st.warning('LOW_COMPARABILITY: no matched earlier facility. A 9% fixed rate cannot be treated as a spread or evidence that terms have worsened. Fees, OID, advance rates and funded balance are not reconciled.')
 st.write('Next checkpoint: equipment acceptance and staged drawdowns through December 31, 2026. Financing remains conditional; the facility ceiling is not cash received.')
with tabs[2]:
 st.subheader('Applied Digital · historical payment composition')
 evidence('Applied Digital')
 st.bar_chart(pd.DataFrame({'USD million':[73,12]},index=['Tenant fit-out','Partial-quarter rent']),horizontal=True)
 st.warning('INSUFFICIENT_EVIDENCE: these figures relate to November 2025. Current receivables, rent collection, construction and energization changes have not been reconciled. Conditional financing capacity is not unrestricted liquidity.')
with tabs[3]:
 st.subheader('IREN and neocloud breadth')
 evidence('Neocloud')
 st.dataframe(pd.DataFrame({'Issuer':['APLD','CIFR','CORZ','CRWV','IREN','NBIS','WULF'],'Equity/credit time series':['UNAVAILABLE']*7}),hide_index=True,width='stretch')
 st.write('Checkpoint: verify Horizon 2–4 acceptance against contractual deadlines and grace periods before labelling a delay. Current basket breadth and correlation cannot be calculated.')
with tabs[4]:
 st.subheader('GPU economics · initial offer observations')
 evidence('GPU economics')
 if max(capture_age_hours(r['retrieved_at']) for r in rows if r['family']=='GPU economics')>24:
  st.warning('STALE OFFER CAPTURE: older than 24 hours; not a current quote.')
 st.warning('INSUFFICIENT_EVIDENCE: no region-matched 4-week history, live availability, H200 comparable offer or resale series. Vast’s marketplace has not been normalized into comparable offers. Listing counts are not capacity.')
 st.markdown('[Vast pricing source](https://vast.ai/pricing) · captured in source register; excluded from comparisons.')
with tabs[5]:
 st.subheader('Evidence register and revision timeline')
 st.caption('Revision 1 is a baseline. Later corrections must create new versions; no earlier availability is implied.')
 st.dataframe(pd.DataFrame(rows)[['id','kind','period','retrieved_at','quality','locator','source_url','source_sha256','revision','supersedes']],hide_index=True,width='stretch',column_config={'source_url':st.column_config.LinkColumn('Source')})
 st.dataframe(pd.DataFrame(snapshot['sources']),hide_index=True,width='stretch',column_config={'url':st.column_config.LinkColumn('Source')})
 st.subheader('Unverified seeds — excluded from calculations')
 st.dataframe(pd.DataFrame(snapshot['unverified_seeds']),hide_index=True,width='stretch')
 st.download_button('Download evidence and provenance',json.dumps(snapshot,indent=2),'foundation_evidence.json','application/json')
 st.write('Remaining coverage: current CRWV baseline; current APLD filings; equity/credit histories; comparable deal terms; repeated region/contract-matched GPU observations. Subscription responses are pending and are not a dependency for reviewing this baseline.')
st.subheader('Shadow diagnostics')
st.dataframe(pd.DataFrame(rules),hide_index=True,width='stretch')
st.caption('Rules version shadow-1. Historical conditions are not current alerts. No calibrated severity, contagion escalation, digest, Slack notification or trade action is enabled.')
