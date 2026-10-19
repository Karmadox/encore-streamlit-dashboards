import unittest,json,zlib,base64,hashlib,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from live_returns import prepare_analysis,EvidenceUnavailable
from test_dell_returns import DellReturns
class Comparison(unittest.TestCase):
 def setUp(self):
  e=DellReturns().evidence();b=json.dumps(e).encode()
  self.settings={'dell_evidence_zlib_base64':base64.b64encode(zlib.compress(b)).decode(),'dell_evidence_sha256':hashlib.sha256(b).hexdigest()}
  self.result={'ticker':'DELL','rows':[{'date':d,'close':'100','volume':'1000'} for d in e['sessions']]+[{'date':'2026-10-16','close':'200','volume':'1000'}],'requested_end':'2026-10-18'}
 def test_latest_keeps_evidence_bound(self):
  a,_,end=prepare_analysis(self.result,self.settings)
  self.assertEqual(a['end'],'2026-10-12');self.assertEqual(end,'2026-10-12')
 def test_common_origin_and_lag(self):
  a,_,_=prepare_analysis(self.result,self.settings,'2026-10-09')
  for h,p in a['panels'].items():
   self.assertEqual(p['values'][-1]['date'],'2026-10-09');self.assertEqual(p['values'][-1]['latest_input'],'2026-10-08')
 def test_later_origin_refused_not_backdated(self):
  with self.assertRaises(EvidenceUnavailable):prepare_analysis(self.result,self.settings,'2026-10-16')
 def test_weekend_refused(self):
  with self.assertRaises(EvidenceUnavailable):prepare_analysis(self.result,self.settings,'2026-10-10')
 def test_unknown_evidence_stays_blocked(self):
  for t in ['CHWY','ZTS','SPCX','CMCSA']:
   with self.subTest(t=t),self.assertRaises(EvidenceUnavailable):prepare_analysis({**self.result,'ticker':t},self.settings)
 def test_no_private_evidence(self):
  with self.assertRaises(EvidenceUnavailable):prepare_analysis(self.result,None)
if __name__=='__main__':unittest.main()
