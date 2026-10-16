from pathlib import Path
import json,sys
import pandas as pd
import streamlit as st
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from foundation_monitor.engine import validate,oracle_metrics,shadow_rules,contagion_state,capture_age_hours
from foundation_monitor.store import read_snapshot
from foundation_monitor.market import latest_market,comparable_index
from foundation_monitor.collect_gpu import history as gpu_history

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

@st.cache_data(ttl=300)
def load_observations():
 import os
 if os.environ.get('FOUNDATION_MONITOR_DB')!='1':return None,[]
 import psycopg2
 config=dict(st.secrets['db']);config.update(connect_timeout=10,sslmode='require')
 conn=psycopg2.connect(**config)
 try:
  market=latest_market(conn);gpu=gpu_history(conn)
  return market,gpu
 finally:conn.close()

st.title('AI Infrastructure Foundation Monitor')
st.caption('Review release · Evidence first · Shadow diagnostics only')
try:snapshot,storage=load()
except Exception:
 st.error('Evidence store unavailable. No cached or empty result is being presented as current evidence.');st.stop()
rows=snapshot['observations'];rules=shadow_rules(rows);metrics=oracle_metrics(rows)
st.warning('Contagion assessment: '+contagion_state(rules)+' — the available evidence cannot establish whether CRWV stress is isolated or spreading.')
st.caption(f"{storage} | Evidence captured {snapshot['built_at'][:10]} | {snapshot['snapshot_id']}")
st.info('Filing evidence is curated. EncoreDB closes are retained hourly and public GPU offers daily. Credit feeds and qualified total-return breadth remain unavailable. Capture times do not establish historical customer availability.')
try:market,gpu_captures=load_observations()
except Exception:
 market,gpu_captures=None,[]
 st.error('Market/GPU capture store unavailable; missing data are not interpreted as no stress.')
if st.button('Refresh retained observations'):
 load.clear();load_observations.clear();st.rerun()
with st.expander('How to read this monitor',expanded=False):
 st.write('Start with the observations below, then inspect each family. Reconciled means checked against the cited document—not independently audited or current. A historical filing remains historical even when downloaded today. Open the evidence register for dates, source locations and hashes.')
 st.write('UNAVAILABLE means an input is absent. INSUFFICIENT_EVIDENCE means the available input cannot support the requested inference. Neither means no stress. Shadow diagnostics never send notifications or drive trading decisions.')
 st.write('Funding commitments are not cash drawn; fit-out receipts are not recurring rent. GPU offers are not transactions, available capacity or utilization. The worker checks hourly for EncoreDB updates and captures Lambda offers at most once per 24 hours. Refresh retrieves the latest retained observations; it does not call a vendor.')
st.subheader('What the evidence currently tells us')
a,b,c=st.columns(3)
a.metric('ORCL capex covered by OCF',f"{metrics['OCF coverage']:.1%}",help='Quarter ended Aug 31, 2026. Includes specified customer prepayments.')
b.metric('Coverage excluding prepayments',f"{metrics['Coverage excluding specified prepayments']:.1%}")
c.metric('ORCL capex less OCF',f"${metrics['Capex less operating cash flow']/1000:.3f}bn",help='Cash-flow gap before financing; not a forecast of required new capital.')
st.write('**Review observations:** Oracle’s funding mix warrants monitoring; it does not itself establish credit deterioration. Applied Digital’s August-quarter revenue includes substantial fit-out activity; its October disclosure reports more delivered capacity. IREN financing depends on equipment acceptance. These are exposure channels to watch, not demonstrated contagion.')
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
 st.subheader('Project finance · disclosed terms, no repricing conclusion')
 evidence('Project finance')
 st.warning('LOW_COMPARABILITY: no matched earlier facility. A 9% fixed rate cannot be treated as a spread or evidence that terms have worsened. Fees, OID, advance rates and funded balance are not reconciled.')
 st.write('Next checkpoint: equipment acceptance and staged drawdowns through December 31, 2026. Financing remains conditional; the facility ceiling is not cash received.')
with tabs[2]:
 st.subheader('Applied Digital · latest reconciled quarter and delivered capacity')
 evidence('Applied Digital')
 st.bar_chart(pd.DataFrame({'USD million':[183.5,65.8,13.3]},index=['Fit-out revenue','Base rental revenue','Tenant recoveries']),horizontal=True)
 st.caption('Quarter ended August 31, 2026 • recognized HPC revenue, not cash receipts. October 7 release reports 250 MW operational at Polaris Forge 1 as of October 1.')
 st.warning('INSUFFICIENT_EVIDENCE: current payment delinquency and receivables aging are not established. November 2025 rows remain historical context. Ready-for-service is not utilization, and conditional financing capacity is not unrestricted liquidity.')
with tabs[3]:
 st.subheader('IREN and neocloud breadth')
 evidence('Neocloud')
 st.subheader('CoreWeave · latest reconciled quarterly baseline')
 evidence('CoreWeave')
 st.caption('June-quarter figures published August 11, 2026. These do not verify October bond pricing, customer defaults or current liquidity.')
 st.subheader('EncoreDB equity context')
 if market:
  st.caption('Captured '+market['retrieved_at']+' • '+market['source_table']+' • completed New York dates only')
  if capture_age_hours(market['retrieved_at'])>2:st.warning('STALE CAPTURE: collector has not refreshed within two hours.')
  st.dataframe(pd.DataFrame(market['coverage']),hide_index=True,width='stretch')
  available=sorted({r['ticker'] for r in market['prices']})
  selected=st.multiselect('Compare stored closes',available,default=[x for x in ('CRWV','NBIS','ORCL') if x in available])
  if selected:
   index=comparable_index(market['prices'],selected)
   if not index.empty:
    st.line_chart(index,width='stretch')
    st.caption('Stored close indexed to 100 on common first observed date '+str(index.index[0])+'. Gaps are not filled. This is price context, not a corporate-action-qualified return or contagion signal.')
  st.download_button('Download retained market observations',json.dumps(market,indent=2),'foundation_market.json','application/json')
 else:st.warning('UNAVAILABLE: retained EncoreDB market observations.')
 st.write('Checkpoint: verify Horizon 2–4 acceptance against contractual deadlines and grace periods before labelling a delay. Qualified total-return breadth and correlation remain unavailable.')
with tabs[4]:
 st.subheader('GPU economics · retained advertised offers')
 evidence('GPU economics')
 if gpu_captures:
  latest=gpu_captures[0]
  st.caption('Latest collection attempt: '+latest['captured_at']+' • '+latest['status'])
  if latest['status']!='SUCCESS':st.warning('Latest offer collection failed; earlier quotes are historical only.')
  offer_rows=[dict(captured_at=c['captured_at'],source_sha256=c['source_sha256'],**o) for c in gpu_captures for o in c['offers']]
  if offer_rows:st.dataframe(pd.DataFrame(offer_rows),hide_index=True,width='stretch')
  st.caption(str(len({c['captured_at'][:10] for c in gpu_captures if c['status']=='SUCCESS'}))+' distinct observation dates. Unknown region or changed configuration prevents a like-for-like trend.')
  st.download_button('Download GPU collection history',json.dumps(gpu_captures,indent=2),'foundation_gpu_history.json','application/json')
 else:st.warning('UNAVAILABLE: scheduled offer history. Baseline offers below are historical.')
 if (capture_age_hours(gpu_captures[0]['captured_at']) if gpu_captures else max(capture_age_hours(r['retrieved_at']) for r in rows if r['family']=='GPU economics'))>25:
  st.warning('STALE COLLECTION: no capture attempt within 25 hours. Earlier offers are historical.')
 st.warning('INSUFFICIENT_EVIDENCE: no region-matched 4-week history, live availability, H200 comparable offer or resale series. Vast’s marketplace has not been normalized into comparable offers. Listing counts are not capacity.')
 st.markdown('[Vast pricing source](https://vast.ai/pricing) · captured in source register; excluded from comparisons.')
with tabs[5]:
 st.subheader('Evidence register and revision timeline')
 st.caption('Snapshot v2 extends v1; earlier source facts and originals remain retained. New-period facts are separate observations, not corrections. Market and GPU captures are append-only through the collector; no earlier availability is implied.')
 st.dataframe(pd.DataFrame(rows)[['id','kind','period','retrieved_at','quality','locator','source_url','source_sha256','revision','supersedes']],hide_index=True,width='stretch',column_config={'source_url':st.column_config.LinkColumn('Source')})
 st.dataframe(pd.DataFrame(snapshot['sources']),hide_index=True,width='stretch',column_config={'url':st.column_config.LinkColumn('Source')})
 st.subheader('Unverified seeds — excluded from calculations')
 st.dataframe(pd.DataFrame(snapshot['unverified_seeds']),hide_index=True,width='stretch')
 st.download_button('Download evidence and provenance',json.dumps(snapshot,indent=2),'foundation_evidence.json','application/json')
 st.write('Remaining coverage: current credit conditions; five unmapped equity names; corporate-action-qualified returns; comparable deal terms; region/contract-matched GPU history. Subscription responses are pending and are not a dependency for reviewing this baseline.')
st.subheader('Shadow diagnostics')
st.dataframe(pd.DataFrame(rules),hide_index=True,width='stretch')
st.caption('Rules version shadow-1. Historical conditions are not current alerts. No calibrated severity, contagion escalation, digest, Slack notification or trade action is enabled.')
