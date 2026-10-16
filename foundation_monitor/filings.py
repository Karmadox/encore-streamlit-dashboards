"""Daily SEC recent-submissions discovery. Metadata never becomes verified financial evidence."""
import hashlib,json,re,time
from datetime import datetime,timedelta,timezone,date
import requests
ISSUERS={'ORCL':1341439,'APLD':1144879,'IREN':1878848,'CRWV':1769628}
FORMS={'8-K','10-Q','10-K','6-K','20-F','40-F'}
DDL='''CREATE TABLE IF NOT EXISTS foundation_monitor.filing_captures (
 sha256 text PRIMARY KEY,ticker text NOT NULL,captured_at timestamptz NOT NULL,payload jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS foundation_monitor.filing_versions (
 ticker text NOT NULL,accession text NOT NULL,metadata_sha256 text NOT NULL,
 first_seen timestamptz NOT NULL,PRIMARY KEY(ticker,accession,metadata_sha256));
'''
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def parse(body,ticker,now):
 data=json.loads(body)
 if int(data['cik'])!=ISSUERS[ticker]:raise ValueError('CIK_MISMATCH')
 recent=data['filings']['recent'];keys=('accessionNumber','filingDate','reportDate','acceptanceDateTime','form','primaryDocument')
 sizes={len(recent[k]) for k in keys if isinstance(recent[k],list)}
 if len(sizes)!=1 or not all(isinstance(recent[k],list) for k in keys) or max(sizes)>10000:raise ValueError('COLUMN_SHAPE')
 start=(now.date()-timedelta(days=90)).isoformat();out=[];seen=set()
 for i in range(len(recent['form'])):
  day=recent['filingDate'][i];date.fromisoformat(day)
  if not start<=day<=now.date().isoformat() or recent['form'][i].removesuffix('/A') not in FORMS:continue
  acc=recent['accessionNumber'][i];doc=recent['primaryDocument'][i]
  if not re.fullmatch(r'\d{10}-\d{2}-\d{6}',acc) or acc in seen:raise ValueError('ACCESSION')
  if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*',doc) or '..' in doc:raise ValueError('DOCUMENT')
  seen.add(acc);accepted=recent['acceptanceDateTime'][i] or None
  if accepted:datetime.fromisoformat(accepted.replace('Z','+00:00'))
  row=dict(ticker=ticker,accession=acc,form=recent['form'][i],filing_date=day,report_date=recent['reportDate'][i],sec_acceptance_time=accepted,
   url=f"https://www.sec.gov/Archives/edgar/data/{ISSUERS[ticker]}/{acc.replace('-','')}/{doc}",review_status='NOT_REVIEWED')
  row['metadata_sha256']=digest(row);out.append(row)
 if len(out)>500:raise ValueError('RESULT_LIMIT')
 return sorted(out,key=lambda r:(r['filing_date'],r['accession']),reverse=True)

def capture_one(conn,ticker,get=requests.get,now=None):
 now=now or datetime.now(timezone.utc);url=f'https://data.sec.gov/submissions/CIK{ISSUERS[ticker]:010d}.json'
 with conn:
  with conn.cursor() as c:
   c.execute('SELECT pg_try_advisory_xact_lock(%s)',(41864000+list(ISSUERS).index(ticker),))
   if not c.fetchone()[0]:return 'ALREADY_RUNNING'
   c.execute('SELECT max(captured_at) FROM foundation_monitor.filing_captures WHERE ticker=%s',(ticker,));last=c.fetchone()[0]
   if last and (now-last).total_seconds()<86400:return 'NOT_DUE'
   c.execute('SELECT accession,metadata_sha256,first_seen FROM foundation_monitor.filing_versions WHERE ticker=%s',(ticker,));known={(a,h):ts for a,h,ts in c.fetchall()};accessions={a for a,h in known}
   # First successful empty index is also a baseline, not a subsequent first run.
   c.execute("SELECT EXISTS(SELECT 1 FROM foundation_monitor.filing_captures WHERE ticker=%s AND payload->>'status'='SUCCESS')",(ticker,));baseline=not c.fetchone()[0]
   body=None;raw_sha=None;rows=[];status='CAPTURE_FAILED';http=None
   try:
    deadline=time.monotonic()+30
    with get(url,headers={'User-Agent':'Encore Research rob@encoregm.com'},timeout=(5,15),stream=True,allow_redirects=False) as response:
     http=response.status_code
     if http!=200:raise ValueError('HTTP')
     chunks=[];size=0
     for chunk in response.iter_content(65536):
      size+=len(chunk)
      if size>2_000_000 or time.monotonic()>deadline:raise ValueError('RESPONSE_LIMIT')
      chunks.append(chunk)
     body=b''.join(chunks);raw_sha=hashlib.sha256(body).hexdigest()
    rows=parse(body,ticker,now);status='SUCCESS'
   except Exception:status='PARSE_FAILED' if body is not None else 'CAPTURE_FAILED'
   for row in rows:
    key=(row['accession'],row['metadata_sha256']);first=known.get(key,now)
    row['first_observed_at']=first.isoformat()
    row['discovery_status']='UNCHANGED' if key in known else ('BASELINE' if baseline else ('METADATA_CHANGED' if row['accession'] in accessions else 'NEWLY_OBSERVED'))
    c.execute('INSERT INTO foundation_monitor.filing_versions VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING',(ticker,*key,first))
   payload=dict(ticker=ticker,captured_at=now.isoformat(),source_url=url,source_sha256=raw_sha,status=status,http_status=http,rows=rows,
    coverage='Recent SEC index only; selected periodic/current forms and amendments, trailing 90 calendar days; no archive pagination, exhibits or full-text retrieval',
    note='First observed here is not historical public availability. New metadata is not a verified financial fact or alert.')
   if body is not None:c.execute('INSERT INTO foundation_monitor.sources VALUES (%s,%s,%s,%s) ON CONFLICT (sha256) DO NOTHING',(raw_sha,body,now,url))
   c.execute('INSERT INTO foundation_monitor.filing_captures VALUES (%s,%s,%s,%s::jsonb)',(digest(payload),ticker,now,json.dumps(payload)))
 return status

def latest(conn):
 conn.set_session(readonly=True,autocommit=False)
 try:
  with conn.cursor() as c:
   c.execute("SET LOCAL statement_timeout='8000ms'")
   c.execute('SELECT DISTINCT ON (ticker) sha256,payload FROM foundation_monitor.filing_captures ORDER BY ticker,captured_at DESC')
   out=[]
   for sha,payload in c.fetchall():
    if digest(payload)!=sha:raise ValueError('Filing capture integrity mismatch')
    out.append(payload)
   return out
 finally:conn.rollback()
