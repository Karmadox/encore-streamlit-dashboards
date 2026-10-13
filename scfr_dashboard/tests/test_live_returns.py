import unittest,sys,json,zlib,base64,hashlib
from pathlib import Path
from datetime import date,timedelta
from fractions import Fraction
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from live_returns import load_evidence,calculate,SUPPORTED,EvidenceUnavailable
from model import daily
class Returns(unittest.TestCase):
 def evidence(self):
  sessions=[(date(2025,11,1)+timedelta(days=i)).isoformat() for i in range(80)]
  return {'sessions':sessions,'stocks':{t:[] for t in SUPPORTED}}
 def test_cash_on_ex_date(self):
  self.assertEqual(daily('100','99',[('cash','1')]),0)
  self.assertEqual(daily('100','99',[]),Fraction(-1,100))
 def test_lag_and_missing(self):
  e=self.evidence();obs=[{'date':d,'close':'100'} for d in e['sessions']]
  last=obs[-1]['date'];obs[-1]['close']='500'
  r=calculate(obs,'INTC',e)['panels'][3]['values'][-1]
  self.assertEqual(r['total'],0);self.assertNotEqual(r['latest_input'],last)
  obs.pop(-2)
  r=calculate(obs,'INTC',e)['panels'][3]['values'][-1]
  self.assertIsNone(r['total']);self.assertIsNone(r['per_share'])
 def test_missing_interior_price(self):
  e=self.evidence();obs=[{'date':d,'close':'100'} for d in e['sessions']];obs.pop(-3)
  r=calculate(obs,'INTC',e)['panels'][3]['values'][-1]
  self.assertIsNone(r['total']);self.assertEqual(r['per_share'],0)
 def test_deferred(self):
  with self.assertRaises(EvidenceUnavailable):calculate([],'CMCSA',self.evidence())
 def test_corrupt_evidence(self):
  with self.assertRaises(EvidenceUnavailable):load_evidence({'evidence_zlib_base64':'bad','evidence_sha256':'0'*64})
 def test_cutoff(self):
  e=self.evidence();e['sessions']+=['2026-10-12','2026-10-13']
  obs=[{'date':d,'close':'100'} for d in e['sessions']]
  self.assertEqual(calculate(obs,'INTC',e)['end'],'2026-10-12')
if __name__=='__main__':unittest.main()
