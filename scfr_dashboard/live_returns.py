"""Private, date-bounded action evidence applied to read-only database prices."""
import base64,hashlib,json,zlib
from datetime import date
from fractions import Fraction
from model import features,panel_status,SCALES

SUPPORTED=('CHTR','CNC','ELV','HUM','INTC','MOH','T','TMUS','UNH','VZ')
CUTOFF='2026-10-12'
class EvidenceUnavailable(ValueError):pass

def load_evidence(settings):
    try:
        encoded=settings['evidence_zlib_base64']
        if not isinstance(encoded,str) or len(encoded)>100000:raise ValueError()
        d=zlib.decompressobj();raw=d.decompress(base64.b64decode(encoded,validate=True),200001)
        if len(raw)>200000 or not d.eof or d.unused_data:raise ValueError()
        if hashlib.sha256(raw).hexdigest()!=settings['evidence_sha256']:raise ValueError()
        e=json.loads(raw)
        if e['version']!=1 or e['end']!=CUTOFF or sorted(e['stocks'])!=list(SUPPORTED):raise ValueError()
        sessions=e['sessions']
        if not sessions or sorted(set(sessions))!=sessions or sessions[0]!='2024-10-01' or sessions[-1]!=CUTOFF:raise ValueError()
        for day in sessions:date.fromisoformat(day)
        for t,events in e['stocks'].items():
            seen=set()
            for event in events:
                day=event['ex_date'];amount=Fraction(event['amount'])
                if day not in sessions or day in seen or event['currency']!='USD' or amount<=0:raise ValueError()
                seen.add(day)
        return e
    except Exception:
        raise EvidenceUnavailable('Return evidence is missing or invalid. Prices and volume remain available.') from None

def calculate(observations,ticker,evidence):
    if ticker not in SUPPORTED:raise EvidenceUnavailable('Return history is not yet enabled for this stock.')
    bydate={r['date']:r for r in observations}
    if len(bydate)!=len(observations):raise EvidenceUnavailable('Duplicate price dates; returns withheld.')
    end=min(CUTOFF,max(bydate,default='2024-10-01'))
    events={r['ex_date']:r['amount'] for r in evidence['stocks'][ticker]}
    rows=[]
    for day in evidence['sessions']:
        if day>end:break
        close=bydate.get(day,{}).get('close')
        if close is not None and Fraction(close)<=0:raise EvidenceUnavailable('Invalid price; returns withheld.')
        rows.append({'date':day,'close':close,'actions':[('cash',events[day])] if day in events else []})
    panels={}
    for h in SCALES:
        values=[v for v in features(rows,h) if v['date']>='2026-01-01']
        panels[h]={'values':values,'total_status':panel_status([v['total'] for v in values]),'per_share_status':panel_status([v['per_share'] for v in values])}
    return {'end':end,'panels':panels}

def render(result,settings):
    import streamlit as st
    import pandas as pd
    ticker=result['ticker']
    st.subheader('3 / 9 / 27-session return analysis')
    if ticker=='CMCSA':
        st.info('CMCSA return analysis is deferred because of the Versant spin-off. Prices and volume remain available.');return
    if ticker not in SUPPORTED:
        st.info('Returns await resolution of the split-history exception for this stock. Raw dividend evidence has been reconciled.');return
    if settings is None:
        st.info('The owner must add the private return-evidence settings to enable these panels.');return
    try:
        evidence=load_evidence(settings);analysis=calculate(result['rows'],ticker,evidence)
    except EvidenceUnavailable as exc:
        st.warning(str(exc));return
    st.caption(f'Return evidence ends {analysis["end"]}. Each plotted origin uses only the preceding completed sessions, ending one session before that origin. Price and volume charts may be newer.')
    latest=[]
    for h,panel in analysis['panels'].items():
        values=panel['values']
        if values:
            r=values[-1]
            latest.append({'Scale':f'{h} sessions','Origin':r['date'],'Last input':r['latest_input'],'Total return':float(r['total']) if r['total'] is not None and not panel['total_status']['withheld'] else None,'Per-share price return':float(r['per_share']) if r['per_share'] is not None and not panel['per_share_status']['withheld'] else None})
    if latest:
        nums=[r['Total return'] for r in latest]
        if all(v is not None for v in nums):
            if all(v>0 for v in nums):text='Returns are positive across all three trailing scales.'
            elif all(v<0 for v in nums):text='Returns are negative across all three trailing scales.'
            else:text='The trailing scales differ in direction, indicating mixed recent and longer-window behaviour.'
            st.write(text+' This describes the selected history; it is not a predictive or investment signal.')
        st.dataframe(pd.DataFrame(latest).style.format({'Total return':'{:.2%}','Per-share price return':'{:.2%}'},na_rep='Unavailable'),hide_index=True,use_container_width=True)
    with st.expander('How to read the returns',expanded=False):
        st.write('Total return includes evidenced cash dividends on ex-date. Per-share price return excludes cash dividends. The window length is 3, 9 or 27 trading sessions; each window is recalculated at every session, so charts retain daily granularity. Daily total returns are rounded half-even to 18 decimal places before compounding. Missing sessions remain missing.')
        st.write('These are current-vintage descriptive histories. No in-window splits were returned for the enabled stocks. Other corporate-action coverage is not certified. Returns are limited to the reconciled evidence period and do not extend automatically when new database prices arrive.')
    for h,p in analysis['panels'].items():
        st.markdown(f'**{h}-session returns**')
        chart=[]
        for key,label,statuskey in [('total','Total return','total_status'),('per_share','Per-share price return','per_share_status')]:
            status=p[statuskey]
            st.caption(f'{label}: missing {status["nulls"]}/{status["origins"]} eligible session origins ({status["share"]:.1%}).' if status['origins'] else f'{label}: no eligible origins.')
            if status['withheld']:
                st.warning(f'{label} withheld: missingness exceeds 20%.');continue
            if status['density_note']:st.warning(f'{label}: missingness exceeds 5%.')
            chart.extend({'date':v['date'],'measure':label,'return':float(v[key]) if v[key] is not None else None} for v in p['values'])
        if chart:
            st.vega_lite_chart(pd.DataFrame(chart),{'mark':{'type':'line','invalid':'break-paths-show-domains'},'encoding':{'x':{'field':'date','type':'temporal'},'y':{'field':'return','type':'quantitative','axis':{'format':'.0%'}},'color':{'field':'measure','type':'nominal'},'tooltip':[{'field':'date','type':'temporal'},{'field':'measure','type':'nominal'},{'field':'return','type':'quantitative','format':'.2%'}]}},use_container_width=True)
