"""Once-daily public-offer capture worker; no vendor credentials or paid endpoint."""
import os,json,base64,zlib,tomllib,hashlib,time
from datetime import datetime,timezone
import requests,psycopg2
from .gpu import parse_offers
URL='https://lambda.ai/instances'
DDL='''CREATE TABLE IF NOT EXISTS foundation_monitor.gpu_captures (
 capture_id text PRIMARY KEY, captured_at timestamptz NOT NULL, status text NOT NULL,
 source_sha256 text, payload jsonb NOT NULL);
'''
def configuration():
 n=int(os.environ['SCFR_SECRETS_PARTS'])
 if not 1<=n<=32:raise ValueError('Configuration unavailable')
 settings=tomllib.loads(zlib.decompress(base64.b64decode(''.join(os.environ[f'SCFR_SECRETS_{i:03d}'] for i in range(n)))).decode())
 config=dict(settings['db']);config.update(sslmode='require',connect_timeout=10,options='-c statement_timeout=15000 -c lock_timeout=3000');return config

def capture(conn,get=requests.get,now=None):
 now=now or datetime.now(timezone.utc)
 with conn:
  with conn.cursor() as c:
   c.execute('SELECT pg_try_advisory_xact_lock(41863021)')
   if not c.fetchone()[0]:return 'ALREADY_RUNNING'
   c.execute('SELECT max(captured_at) FROM foundation_monitor.gpu_captures')
   last=c.fetchone()[0]
   if last and (now-last).total_seconds()<86400:return 'NOT_DUE'
   body=None;sha=None;offers=[];status='CAPTURE_FAILED'
   try:
    # Streaming limit bounds even an unexpected/error page; one request, no application retry.
    with get(URL,headers={'User-Agent':'Encore Research rob@encoregm.com'},timeout=(10,20),stream=True,allow_redirects=False) as r:
     if r.status_code!=200:raise ValueError('HTTP_FAILURE')
     parts=[];size=0
     for part in r.iter_content(65536):
      size+=len(part)
      if size>2_000_000:raise ValueError('RESPONSE_TOO_LARGE')
      parts.append(part)
     body=b''.join(parts);sha=hashlib.sha256(body).hexdigest()
    offers=parse_offers(body);status='SUCCESS'
   except Exception:
    status='PARSE_FAILED' if body is not None else 'CAPTURE_FAILED'
   payload=dict(captured_at=now.isoformat(),source_url=URL,source_sha256=sha,status=status,offers=offers,parser_version='lambda-visible-8x-v1',note='Advertised offers; region and capacity unknown. No inference from failure or absence.')
   if body is not None:
    c.execute('INSERT INTO foundation_monitor.sources VALUES (%s,%s,%s,%s) ON CONFLICT (sha256) DO NOTHING',(sha,body,now,URL))
   id=hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest()
   c.execute('INSERT INTO foundation_monitor.gpu_captures VALUES (%s,%s,%s,%s,%s::jsonb)',(id,now,status,sha,json.dumps(payload)))
 return status

def history(conn):
 conn.set_session(readonly=True,autocommit=False)
 try:
  with conn.cursor() as c:
   c.execute("SET LOCAL statement_timeout='8000ms'")
   c.execute('SELECT payload FROM foundation_monitor.gpu_captures ORDER BY captured_at DESC LIMIT 90')
   return [r[0] for r in c.fetchall()]
 finally:conn.rollback()

def main(loop=False):
 while True:
  conn=None
  try:
   config=configuration();conn=psycopg2.connect(**config);status=capture(conn)
   from .market import retain_market
   market_status=retain_market(conn,config)
   print(json.dumps({'worker':'foundation_market','status':market_status}),flush=True)
  except Exception:status='STORAGE_OR_CONFIGURATION_UNAVAILABLE'
  finally:
   if conn:conn.close()
  print(json.dumps({'worker':'foundation_gpu','status':status,'checked_at':datetime.now(timezone.utc).isoformat()}),flush=True)
  if not loop:return
  time.sleep(3600) # Hourly due check; at most one persisted public request per 24h, failures included.
if __name__=='__main__':
 import sys
 main('--loop' in sys.argv)
