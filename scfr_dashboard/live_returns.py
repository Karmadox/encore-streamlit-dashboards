"""Private, date-bounded action evidence applied to read-only database prices."""
import base64,hashlib,json,zlib
from datetime import date, timedelta
from fractions import Fraction
from pathlib import Path
from model import features,panel_status,SCALES

SUPPORTED=('CHTR','CNC','ELV','HUM','INTC','MOH','T','TMUS','UNH','VZ')
CUTOFF='2026-10-12'
REFRESH_STOCKS=('CHWY','DELL','ZTS')
REFRESH_HASH='024d8dd827777be89ec2559f4827e87665d744e303fc8e7dd1c0034c6cd75301'
REFRESH_PATH=Path(__file__).with_name('private_three_stock_evidence.json')
class EvidenceUnavailable(ValueError):pass

def load_refresh():
    try:
        raw=REFRESH_PATH.read_bytes()
        if len(raw)>200000 or hashlib.sha256(raw).hexdigest()!=REFRESH_HASH:raise ValueError()
        e=json.loads(raw)
        if e['version']!=2 or e['end']!='2026-10-16' or sorted(e['stocks'])!=list(REFRESH_STOCKS):raise ValueError()
        c=e['split_confirmation']
        if c['start']!='2024-10-01' or c['end']!=e['end'] or c['no_splits_or_consolidations'] is not True:raise ValueError()
        if c['basis']!='user_supplied_bloomberg_confirmation':raise ValueError()
        sessions=e['sessions']
        if sessions!=sorted(set(sessions)) or sessions[0]!='2024-10-01' or sessions[-1]!=e['end']:raise ValueError()
        for day in sessions:date.fromisoformat(day)
        for events in e['stocks'].values():
            seen=set()
            for event in events:
                day=event['ex_date']
                if day in seen or day not in sessions or event['currency']!='USD' or Fraction(event['amount'])<=0:raise ValueError()
                seen.add(day)
        return e
    except Exception:
        raise EvidenceUnavailable('Refreshed action evidence is missing or invalid; returns withheld.') from None

def load_evidence(settings, *, dell=False):
    try:
        encoded=settings['evidence_zlib_base64']
        if not isinstance(encoded,str) or len(encoded)>100000:raise ValueError()
        d=zlib.decompressobj();raw=d.decompress(base64.b64decode(encoded,validate=True),200001)
        if len(raw)>200000 or not d.eof or d.unused_data:raise ValueError()
        if hashlib.sha256(raw).hexdigest()!=settings['evidence_sha256']:raise ValueError()
        e=json.loads(raw)
        if e['version']!=1 or e['end']!=CUTOFF or sorted(e['stocks'])!=(['DELL'] if dell else list(SUPPORTED)):raise ValueError()
        if dell and e.get('split_confirmation')!={'start':'2024-10-01','end':'2026-10-13','basis':'user_supplied_bloomberg_confirmation','no_splits':True}:raise ValueError()
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
    if ticker not in (*SUPPORTED,*REFRESH_STOCKS):raise EvidenceUnavailable('Return history is not yet enabled for this stock.')
    bydate={r['date']:r for r in observations}
    if len(bydate)!=len(observations):raise EvidenceUnavailable('Duplicate price dates; returns withheld.')
    end=min(evidence['end'],max(bydate,default='2024-10-01'))
    events={r['ex_date']:r['amount'] for r in evidence['stocks'][ticker]}
    rows=[]
    for day in evidence['sessions']:
        if day>end:break
        close=bydate.get(day,{}).get('close')
        if close is not None and Fraction(close)<=0:raise EvidenceUnavailable('Invalid price; returns withheld.')
        rows.append({'date':day,'close':close,'volume':bydate.get(day,{}).get('volume'),'actions':[('cash',events[day])] if day in events else []})
    panels={}
    for h in SCALES:
        values=[v for v in features(rows,h) if v['date']>='2026-01-01']
        panels[h]={'values':values,'total_status':panel_status([v['total'] for v in values]),'per_share_status':panel_status([v['per_share'] for v in values])}
    return {'end':end,'panels':panels,'rows':rows}

def extend_evidence(evidence,review,ticker,baseline_hash):
    if review is None:return evidence,'Daily action review not yet available; using the initial evidence cutoff.'
    try:
        if review['version']!=1 or review['baseline_hash']!=baseline_hash:raise ValueError()
        r=review['reviews'][ticker]
        if r['status']!='READY_NO_NEW_EVENTS':
            raise EvidenceUnavailable('Daily action reconciliation flagged this stock for review. Return panels are temporarily withheld; prices and volume remain available.')
        end=date.fromisoformat(r['candidate_end'])
        if r['reasons'] or r['candidate_end']!=review['end'] or not date(2026,10,12)<=end<=min(date.today(),date(2026,12,31)):raise ValueError()
        sessions=list(evidence['sessions']);day=date(2026,10,13)
        while day<=end:
            if day.weekday()<5 and day.isoformat() not in ('2026-11-26','2026-12-25'):sessions.append(day.isoformat())
            day+=timedelta(days=1)
        return {**evidence,'end':end.isoformat(),'sessions':sessions},f'Daily corporate-action and unadjusted-price reconciliation: {review["captured_at"]}. No newly effective or revised events admitted.'
    except EvidenceUnavailable:raise
    except Exception:raise EvidenceUnavailable('Daily action-review record is invalid; return panels withheld.') from None

def observations(analysis):
    """Narrative uses the same lagged endpoint and withholding gates as panels."""
    latest={h:p['values'][-1] for h,p in analysis['panels'].items() if p['values']}
    if set(latest)!=set(SCALES):return ['Insufficient history for cross-scale observations.']
    vals={h:r['total'] if not analysis['panels'][h]['total_status']['withheld'] else None for h,r in latest.items()}
    notes=[]
    if all(v is not None for v in vals.values()):
        r3,r9,r27=(vals[h] for h in SCALES)
        numbers=f'3 sessions {float(r3):+.2%}, 9 sessions {float(r9):+.2%}, and 27 sessions {float(r27):+.2%}.'
        if r3<0<r27:direction='Recent pullback within a longer-window gain'
        elif r27<0<r3:direction='Short-window rebound within a longer-window decline'
        elif all(v>0 for v in vals.values()):direction='Positive returns across all three scales'
        elif all(v<0 for v in vals.values()):direction='Negative returns across all three scales'
        elif all(v==0 for v in vals.values()):direction='No net change across the three scales'
        else:direction='Mixed or flat directions across the three scales'
        notes.append(f'**Direction:** {direction}: {numbers}')
    else:notes.append('**Direction:** Cross-scale interpretation is unavailable because a latest return is missing or its panel is withheld.')
    if vals[3] is not None and vals[27] is not None:
        a,b=latest[3]['rms'],latest[27]['rms']
        if a is not None and b is not None:
            relationship='larger' if a>b else 'smaller' if a<b else 'equal'
            notes.append(f'**Size of daily moves:** 3-session RMS is {float(a):.2%}, versus {float(b):.2%} over 27 sessions. Recent daily moves are {relationship} on this measure. RMS is unannualized and does not subtract the mean; it does not indicate direction.')
    eligible=[h for h in SCALES if vals[h] is not None and latest[h]['per_share'] is not None and not analysis['panels'][h]['per_share_status']['withheld']]
    if eligible:
        gaps={h:float(vals[h]-latest[h]['per_share'])*100 for h in eligible}
        if all(abs(v)<0.00005 for v in gaps.values()):
            notes.append('**Dividend effect:** Total and per-share price returns match to the displayed precision across the available windows; there is no visible cash-dividend contribution at this precision.')
        else:
            detail=', '.join(f'{h} sessions: {gaps[h]:+.3f} percentage points' for h in eligible)
            notes.append(f'**Dividend effect:** Total return minus per-share price return is {detail}. This reflects the cash-distribution contribution under the stated calculation conventions.')
    endpoint=latest[3]['latest_input']
    rows=[r for r in analysis['rows'] if endpoint and r['date']<=endpoint]
    year=[r for r in rows if r['date']>='2026-01-01']
    if year and year[-1]['close'] is not None:
        priced=[r for r in year if r['close'] is not None]
        peak=max(priced,key=lambda r:Fraction(r['close']))
        distance=Fraction(year[-1]['close'])/Fraction(peak['close'])-1
        notes.append(f'**Position in the observed price range:** The {endpoint} close is {float(distance):.2%} relative to the highest observed 2026 close, dated {peak["date"]}. This is distance from the observed peak, not maximum drawdown. Missing closes: {len(year)-len(priced)}/{len(year)} sessions.')
    if len(rows)>=27:
        volumes=[r.get('volume') for r in rows[-27:]]
        if all(v is not None for v in volumes):
            values=[Fraction(v) for v in volumes]
            if all(v>=0 for v in values) and sum(values)>0:
                ratio=(sum(values[-3:])/3)/(sum(values)/27)
                notes.append(f'**Trading activity:** Average reported volume over the latest 3 input sessions is {float(ratio):.2f} times the 27-session average. The periods overlap; volume alone does not establish buying pressure, selling pressure or cause.')
        else:notes.append('**Trading activity:** The 3-versus-27-session volume comparison is unavailable because volume is missing within the window.')
    return notes

def prepare_analysis(result, settings, comparison_origin=None):
    """Resolve evidence first; a requested common origin must never silently shift."""
    ticker=result['ticker']
    if ticker=='CMCSA':raise EvidenceUnavailable('CMCSA returns remain deferred for the Versant spin-off. Prices and volume are available.')
    if ticker not in (*SUPPORTED,*REFRESH_STOCKS):
        raise EvidenceUnavailable(f'{ticker}: split-history evidence is unresolved. New prices do not establish no-split coverage; returns remain withheld.')
    if settings is None:raise EvidenceUnavailable('Private return evidence is not configured. Prices and volume remain available.')
    if ticker in REFRESH_STOCKS and REFRESH_PATH.exists():
        evidence=load_refresh()
        note='Raw cash-dividend views reconciled through 16 October 2026. No-split coverage for CHWY, DELL and ZTS is based on the listing/date-specific Bloomberg confirmation supplied by Rob. Workbook execution time was not recorded. This fixed evidence package does not extend automatically.'
    elif ticker in ('CHWY','ZTS'):
        raise EvidenceUnavailable('Refreshed action evidence is not installed; returns withheld.')
    elif ticker=='DELL':
        evidence=load_evidence({'evidence_zlib_base64':settings.get('dell_evidence_zlib_base64'),'evidence_sha256':settings.get('dell_evidence_sha256')},dell=True)
        note='DELL uses separately reconciled cash dividends through 12 October and the supplied no-split confirmation through 13 October. Later return origins require renewed action evidence.'
    else:
        evidence=load_evidence(settings)
        evidence,note=extend_evidence(evidence,result.get('daily_review'),ticker,settings['evidence_sha256'])
    supported_end=min(evidence['end'],max((r['date'] for r in result['rows']),default='2024-10-01'))
    evidence_end=evidence['end']
    if comparison_origin is not None:
        selected=date.fromisoformat(comparison_origin)
        if selected.isoformat() not in evidence['sessions'] or comparison_origin>supported_end:
            raise EvidenceUnavailable(f'Comparison origin {comparison_origin} is unavailable for {ticker}. Latest supported origin: {supported_end}. Choose a supported trading session; no earlier date has been substituted.')
        evidence={**evidence,'end':comparison_origin}
    analysis=calculate(result['rows'],ticker,evidence)
    return analysis,note,evidence_end

def render(result,settings):
    import streamlit as st
    import pandas as pd
    ticker=result['ticker']
    st.subheader('3 / 9 / 27-session return analysis')
    rows=result['rows']
    latest_price=max((r['date'] for r in rows if r.get('close') is not None),default='Unavailable')
    latest_volume=max((r['date'] for r in rows if r.get('volume') is not None),default='Unavailable')
    st.caption(f'Prices through {latest_price} · Volume through {latest_volume}. Price freshness does not extend corporate-action evidence.')
    fixed=st.checkbox('Use a common comparison origin',key='scfr_fixed_origin')
    origin=None
    if fixed:
        chosen=st.date_input('Comparison origin (trading session)',value=date(2026,10,12),min_value=date(2026,1,1),max_value=date.fromisoformat(result['requested_end']),key='scfr_comparison_origin')
        origin=chosen.isoformat()
        st.caption('Keep this date selected when switching stocks. Unsupported dates are withheld rather than moved backwards. Every scale ends one session before this origin.')
    try:
        analysis,daily_note,evidence_end=prepare_analysis(result,settings,origin)
    except EvidenceUnavailable as exc:
        st.warning(str(exc));return
    st.caption(f'Corporate-action evidence through {evidence_end} · Display origin through {analysis["end"]}.')
    if analysis['end']<latest_price:
        st.info(f'Prices extend beyond the displayed returns ({latest_price} versus origin {analysis["end"]}). This reflects the evidence boundary or your comparison-date selection, not missing newer prices.')
    with st.expander('Evidence freshness and limitations',expanded=False):
        st.write(daily_note)
        st.write('Previously reconciled workbook dividends remain the baseline. Daily review can extend supported stocks only when no newly effective or revised event is flagged. This is current-vintage descriptive evidence, not proof of historical customer availability.')
    latest=[]
    for h,panel in analysis['panels'].items():
        values=panel['values']
        if values:
            r=values[-1]
            latest.append({'Scale':f'{h} sessions','Origin':r['date'],'Last input':r['latest_input'],'Total return':float(r['total']) if r['total'] is not None and not panel['total_status']['withheld'] else None,'Per-share price return':float(r['per_share']) if r['per_share'] is not None and not panel['per_share_status']['withheld'] else None})
    if latest:
        st.markdown(f'**Observations for {ticker} — origin {latest[0]["Origin"]}, inputs through {latest[0]["Last input"]}**')
        for observation in observations(analysis):
            st.markdown(f'- {observation}')
        st.caption('Descriptive observations, not calibrated signals or forecasts. The windows overlap and their returns should not be added together.')
        st.dataframe(pd.DataFrame(latest).style.format({'Total return':'{:.2%}','Per-share price return':'{:.2%}'},na_rep='Unavailable'),hide_index=True,use_container_width=True)
    with st.expander('How to read the returns',expanded=False):
        st.write('Total return includes evidenced cash dividends on ex-date. Per-share price return excludes cash dividends. The window length is 3, 9 or 27 trading sessions; each window is recalculated at every session, so charts retain daily granularity. Daily total returns are rounded half-even to 18 decimal places before compounding. Missing sessions remain missing.')
        st.write('These are current-vintage descriptive histories. No in-window splits were returned for the daily-enabled stocks. CHWY, DELL and ZTS use separately recorded, date-bounded Bloomberg confirmation supplied by Rob. Other corporate-action coverage is not certified. Their fixed evidence cutoff does not extend automatically. Daily-enabled stocks extend only after action and unadjusted-price reconciliation succeeds; new or revised actions require review.')
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
