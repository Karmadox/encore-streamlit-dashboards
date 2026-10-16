"""Descriptive stored-price arithmetic; no calibrated alerts or causal inference."""
from decimal import Decimal
from datetime import date

HORIZONS=(5,20)
def equity_context(market):
 if not market or not market.get('prices'):return {'rows':[],'observations':[],'end':None}
 by={};dates=set()
 for item in market['prices']:
  ticker=item['ticker'];day=item['date'];date.fromisoformat(day)
  value=Decimal(item['close'])
  if not value.is_finite() or value<=0:raise ValueError('Invalid stored close')
  series=by.setdefault(ticker,{})
  if day in series:raise ValueError('Duplicate stored date')
  series[day]=value;dates.add(day)
 dates=sorted(dates);end=dates[-1]
 coverage={c['ticker']:c['status'] for c in market['coverage']}
 output=[]
 for ticker in sorted(coverage):
  series=by.get(ticker,{})
  row={'ticker':ticker,'end_date':end,'capture_sha256':market['sha256'],'captured_at':market['retrieved_at'],'basis':'stored close; action treatment not qualified'}
  for n in HORIZONS:
   window=dates[-(n+1):]
   status='AVAILABLE'
   if coverage[ticker]!='STORED_CLOSE_AVAILABLE':status=coverage[ticker]
   elif len(window)<n+1:status='INSUFFICIENT_HISTORY'
   elif any(d not in series for d in window):status='MISSING_OBSERVATIONS'
   row[f'status_{n}']=status
   row[f'start_{n}']=window[0] if len(window)==n+1 else None
   row[f'change_{n}_pct']=float((series[end]/series[window[0]]-1)*100) if status=='AVAILABLE' else None
   row[f'start_close_{n}']=str(series[window[0]]) if status=='AVAILABLE' else None
  row['end_close']=str(series[end]) if end in series else None
  output.append(row)
 observations=[]
 for n in HORIZONS:
  eligible=[r for r in output if r[f'status_{n}']=='AVAILABLE']
  if not eligible:
   observations.append(f'{n}-observation comparison unavailable: no complete eligible series.');continue
  up=sum(r[f'change_{n}_pct']>0 for r in eligible);down=sum(r[f'change_{n}_pct']<0 for r in eligible)
  flat=len(eligible)-up-down;missing=len(output)-len(eligible)
  observations.append(f"From {eligible[0][f'start_{n}']} to {end}, {up} rose, {down} fell and {flat} were unchanged among {len(eligible)} comparable stocks ({missing} unavailable), over {n} observed price intervals.")
 crwv=next((r for r in output if r['ticker']=='CRWV'),None)
 if crwv:
  changes=[f"{n} intervals: {crwv[f'change_{n}_pct']:+.2f}%" for n in HORIZONS if crwv[f'status_{n}']=='AVAILABLE']
  if changes:observations.append('CoreWeave stored-price change — '+ '; '.join(changes)+'. These observations do not identify a financing cause or establish propagation.')
 return {'rows':output,'observations':observations,'end':end,'capture_sha256':market['sha256'],'method':'context-1: 100 × (end close / start close − 1); union of retained observation dates, complete window required; no fill'}
