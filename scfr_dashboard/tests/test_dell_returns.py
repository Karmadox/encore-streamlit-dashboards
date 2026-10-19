import unittest,sys,json,base64,zlib,hashlib
from pathlib import Path
from datetime import date,timedelta
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from live_returns import load_evidence,calculate,EvidenceUnavailable
class DellReturns(unittest.TestCase):
 def evidence(self):
  sessions=[];d=date(2024,10,1)
  while d<=date(2026,10,12):
   if d.weekday()<5:sessions.append(d.isoformat())
   d+=timedelta(days=1)
  return {'version':1,'end':'2026-10-12','sessions':sessions,'stocks':{'DELL':[{'ex_date':'2026-07-21','amount':'0.63','currency':'USD'}]},'split_confirmation':{'start':'2024-10-01','end':'2026-10-13','basis':'user_supplied_bloomberg_confirmation','no_splits':True}}
 def packed(self,e):
  raw=json.dumps(e).encode()
  return {'evidence_zlib_base64':base64.b64encode(zlib.compress(raw)).decode(),'evidence_sha256':hashlib.sha256(raw).hexdigest()}
 def test_dated_dell_evidence_and_cutoff(self):
  e=load_evidence(self.packed(self.evidence()),dell=True)
  obs=[{'date':d,'close':'100'} for d in e['sessions']]+[{'date':'2026-10-13','close':'9999'}]
  result=calculate(obs,'DELL',e)
  self.assertEqual(result['end'],'2026-10-12')
  self.assertEqual(result['rows'][-1]['date'],'2026-10-12')
  july=next(v for v in result['panels'][3]['values'] if v['date']=='2026-07-22')
  self.assertGreater(july['total'],0);self.assertEqual(july['per_share'],0)
 def test_no_split_evidence_required(self):
  e=self.evidence();del e['split_confirmation']
  with self.assertRaises(EvidenceUnavailable):load_evidence(self.packed(e),dell=True)
 def test_not_interchangeable_with_daily_baseline(self):
  with self.assertRaises(EvidenceUnavailable):load_evidence(self.packed(self.evidence()))
 def test_future_dividend_rejected(self):
  e=self.evidence();e['stocks']['DELL'][0]['ex_date']='2026-10-20'
  with self.assertRaises(EvidenceUnavailable):load_evidence(self.packed(e),dell=True)
 def test_hash_mismatch_rejected(self):
  p=self.packed(self.evidence());p['evidence_sha256']='0'*64
  with self.assertRaises(EvidenceUnavailable):load_evidence(p,dell=True)
