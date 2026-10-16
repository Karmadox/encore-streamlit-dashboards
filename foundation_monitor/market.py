"""Read stored closes, without claiming corporate-action-qualified returns."""
from datetime import datetime,timedelta,timezone
from zoneinfo import ZoneInfo
from decimal import Decimal
import hashlib,json
TICKERS=('APLD','CIFR','CORZ','CRWV','IREN','NBIS','WULF','ORCL','DELL','HPE','NVDA','SMCI')
def read_market(config,connector=None):
 import psycopg2
 end=datetime.now(ZoneInfo('America/New_York')).date()-timedelta(days=1);start=end-timedelta(days=180)
 settings=dict(config);settings.update(sslmode='require',connect_timeout=10)
 conn=(connector or psycopg2.connect)(**settings);conn.set_session(readonly=True,autocommit=False,isolation_level='REPEATABLE READ')
 result={'retrieved_at':datetime.now(timezone.utc).isoformat(),'source_table':'encoredb.equity_daily_prices','start':str(start),'end':str(end),'coverage':[],'prices':[],'basis':'Stored close; adjustment and session completeness not independently verified'}
 try:
  with conn.cursor() as c:
   c.execute("SET LOCAL statement_timeout='10000ms'")
   c.execute('SELECT instrument_id,ticker,asset_class,currency,bbg_security,active_flag FROM encoredb.instruments WHERE upper(btrim(ticker))=ANY(%s)',(list(TICKERS),))
   identities=c.fetchall()
   for ticker in sorted(TICKERS):
    matches=[r for r in identities if r[1].strip().upper()==ticker]
    meta={'ticker':ticker,'rows':0,'latest_date':None,'status':'UNAVAILABLE_IDENTITY'}
    if len(matches)!=1:meta['status']='AMBIGUOUS_IDENTITY' if matches else 'UNAVAILABLE_IDENTITY';result['coverage'].append(meta);continue
    i,t,asset,ccy,bbg,active=matches[0]
    if asset not in ('Equity','Stock','Common Stock') or ccy!='USD' or bbg!=ticker+' US Equity' or active is not True:meta['status']='IDENTITY_REVIEW_REQUIRED';result['coverage'].append(meta);continue
    c.execute('SELECT trade_date,close_price FROM encoredb.equity_daily_prices WHERE instrument_id=%s AND trade_date BETWEEN %s AND %s ORDER BY trade_date LIMIT 202',(i,start,end))
    raw=c.fetchall();prices=validate_prices(raw,start,end)
    if prices is None:meta['status']='INVALID_OR_DUPLICATE_PRICES'
    elif not prices:meta['status']='NO_PRICES'
    else:
     meta.update(rows=len(prices),latest_date=prices[-1]['date'],status='STORED_CLOSE_AVAILABLE' if (end-raw[-1][0]).days<=4 else 'STALE_STORED_CLOSE',instrument_id=i)
     result['prices'].extend(dict(ticker=ticker,**p) for p in prices)
    result['coverage'].append(meta)
 finally:conn.rollback();conn.close()
 result['sha256']=hashlib.sha256(json.dumps(result,sort_keys=True,separators=(',',':')).encode()).hexdigest()
 return result

def validate_prices(raw,start,end):
 if len(raw)>201:return None
 out=[];previous=None
 for day,close in raw:
  if day<start or day>end or (previous is not None and day<=previous):return None
  previous=day
  if close is None:return None
  d=Decimal(str(close))
  if not d.is_finite() or d<=0:return None
  out.append({'date':day.isoformat(),'close':str(d)})
 return out

def comparable_index(prices,tickers):
 import pandas as pd
 df=pd.DataFrame([r for r in prices if r['ticker'] in tickers])
 if df.empty:return pd.DataFrame()
 wide=df.pivot(index='date',columns='ticker',values='close').astype(float)
 # Common base only; subsequent missing dates remain gaps, never forward-filled.
 common=wide.dropna()
 if common.empty:return pd.DataFrame()
 base=common.index[0];return wide.loc[base:].divide(wide.loc[base])*100

DDL='''CREATE TABLE IF NOT EXISTS foundation_monitor.market_captures (
 sha256 text PRIMARY KEY, captured_at timestamptz NOT NULL, payload jsonb NOT NULL);
'''
def retain_market(conn,config):
 with conn:
  with conn.cursor() as c:
   c.execute('SELECT pg_try_advisory_xact_lock(41863022)')
   if not c.fetchone()[0]:return 'ALREADY_RUNNING'
   c.execute('SELECT max(captured_at) FROM foundation_monitor.market_captures');last=c.fetchone()[0]
   if last and (datetime.now(timezone.utc)-last).total_seconds()<3300:return 'NOT_DUE'
   result=read_market(config)
   c.execute('INSERT INTO foundation_monitor.market_captures VALUES (%s,%s,%s::jsonb) ON CONFLICT (sha256) DO NOTHING',(result['sha256'],result['retrieved_at'],json.dumps(result)))
 return 'SUCCESS'

def latest_market(conn):
 conn.set_session(readonly=True,autocommit=False)
 try:
  with conn.cursor() as c:
   c.execute("SET LOCAL statement_timeout='8000ms'")
   c.execute('SELECT sha256,payload FROM foundation_monitor.market_captures ORDER BY captured_at DESC LIMIT 1');row=c.fetchone()
   if not row:return None
   sha,payload=row;unhashed={k:v for k,v in payload.items() if k!='sha256'}
   if hashlib.sha256(json.dumps(unhashed,sort_keys=True,separators=(',',':')).encode()).hexdigest()!=sha or payload['sha256']!=sha:raise ValueError('Market capture hash mismatch')
   return payload
 finally:conn.rollback()
