import json,unittest
from datetime import datetime,timezone
from foundation_monitor.filings import parse,capture_one,digest
NOW=datetime(2026,10,16,tzinfo=timezone.utc)
def sample():return {'cik':1341439,'filings':{'recent':{'accessionNumber':['0001341439-26-000001'],'filingDate':['2026-10-15'],'reportDate':['2026-08-31'],'acceptanceDateTime':['2026-10-15T16:30:00Z'],'form':['8-K'],'primaryDocument':['report.htm']}}}
class FilingsTests(unittest.TestCase):
 def test_source_link_and_unreviewed(self):
  r=parse(json.dumps(sample()),'ORCL',NOW)[0];self.assertEqual(r['review_status'],'NOT_REVIEWED');self.assertIn('/1341439/000134143926000001/report.htm',r['url'])
 def test_wrong_issuer(self):
  with self.assertRaises(ValueError):parse(json.dumps(sample()),'CRWV',NOW)
 def test_column_mismatch(self):
  s=sample();s['filings']['recent']['form']=[]
  with self.assertRaises(ValueError):parse(json.dumps(s),'ORCL',NOW)
 def test_unsupported_form(self):
  s=sample();s['filings']['recent']['form']=['4'];self.assertEqual(parse(json.dumps(s),'ORCL',NOW),[])
 def test_amendment_retained(self):
  s=sample();s['filings']['recent']['form']=['10-Q/A'];self.assertEqual(parse(json.dumps(s),'ORCL',NOW)[0]['form'],'10-Q/A')
 def test_old_and_future_excluded(self):
  for day in ['2025-01-01','2026-10-17']:
   s=sample();s['filings']['recent']['filingDate']=[day];self.assertEqual(parse(json.dumps(s),'ORCL',NOW),[])
 def test_document_path_refused(self):
  s=sample();s['filings']['recent']['primaryDocument']=['../x.htm']
  with self.assertRaises(ValueError):parse(json.dumps(s),'ORCL',NOW)
 def test_null_acceptance_not_invented(self):
  s=sample();s['filings']['recent']['acceptanceDateTime']=[''];self.assertIsNone(parse(json.dumps(s),'ORCL',NOW)[0]['sec_acceptance_time'])
 def test_changed_metadata_changes_hash(self):
  s=sample();a=parse(json.dumps(s),'ORCL',NOW)[0];s['filings']['recent']['reportDate']=['2026-09-30'];b=parse(json.dumps(s),'ORCL',NOW)[0];self.assertNotEqual(a['metadata_sha256'],b['metadata_sha256'])
 def test_not_due_no_http(self):
  class C:
   def __enter__(self):return self
   def __exit__(self,*a):pass
   def cursor(self):return self
   def execute(self,q,*a):self.q=q
   def fetchone(self):return (True,) if 'lock' in self.q else (NOW,)
  def fail(*a,**kw):raise AssertionError('Unexpected HTTP')
  self.assertEqual(capture_one(C(),'ORCL',get=fail,now=NOW),'NOT_DUE')

class CaptureTests(unittest.TestCase):
 def run_capture(self,status=200,body=None,known=(),baseline=True):
  class C:
   def __init__(self):self.calls=[]
   def __enter__(self):return self
   def __exit__(self,*a):pass
   def cursor(self):return self
   def execute(self,q,args=()):self.q=q;self.calls.append((q,args))
   def fetchone(self):
    if 'advisory' in self.q:return (True,)
    if 'EXISTS' in self.q:return (not baseline,)
    return (None,)
   def fetchall(self):return list(known)
  class R:
   status_code=status
   def __enter__(self):return self
   def __exit__(self,*a):pass
   def iter_content(self,*a):yield body if body is not None else json.dumps(sample()).encode()
  c=C();result=capture_one(c,'ORCL',get=lambda *a,**k:R(),now=NOW)
  payload=json.loads(c.calls[-1][1][-1]);return result,payload,c.calls
 def test_baseline_not_new_alert(self):
  status,p,_=self.run_capture();self.assertEqual(status,'SUCCESS');self.assertEqual(p['rows'][0]['discovery_status'],'BASELINE');self.assertEqual(p['rows'][0]['review_status'],'NOT_REVIEWED')
 def test_http_failure_is_not_empty_success(self):
  status,p,calls=self.run_capture(status=403);self.assertEqual(status,'CAPTURE_FAILED');self.assertEqual(p['http_status'],403);self.assertFalse(any('INSERT INTO foundation_monitor.filing_versions' in q for q,a in calls))
 def test_bad_json_retains_source_as_parse_failure(self):
  status,p,calls=self.run_capture(body=b'bad');self.assertEqual(status,'PARSE_FAILED');self.assertTrue(p['source_sha256']);self.assertTrue(any('INSERT INTO foundation_monitor.sources' in q for q,a in calls))
 def test_new_and_changed(self):
  _,p,_=self.run_capture(baseline=False);self.assertEqual(p['rows'][0]['discovery_status'],'NEWLY_OBSERVED')
  _,p,_=self.run_capture(baseline=False,known=[('0001341439-26-000001','old',NOW)]);self.assertEqual(p['rows'][0]['discovery_status'],'METADATA_CHANGED')
 def test_unchanged_preserves_first_seen(self):
  row=parse(json.dumps(sample()),'ORCL',NOW)[0];first=datetime(2026,10,15,tzinfo=timezone.utc)
  _,p,_=self.run_capture(baseline=False,known=[(row['accession'],row['metadata_sha256'],first)])
  self.assertEqual(p['rows'][0]['discovery_status'],'UNCHANGED');self.assertEqual(p['rows'][0]['first_observed_at'],first.isoformat())

if __name__=='__main__':unittest.main()
