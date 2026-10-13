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
  return {'end':'2026-10-12','sessions':sessions,'stocks':{t:[] for t in SUPPORTED}}
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
class Narratives(unittest.TestCase):
 def test_pullback_and_common_endpoint(self):
  from live_returns import observations
  from decimal import Decimal
  panels={h:{'values':[{'total':v,'per_share':v,'rms':Decimal('.02'),'latest_input':'2026-10-09'}],'total_status':{'withheld':False},'per_share_status':{'withheld':False}} for h,v in [(3,Fraction('-0.0693')),(9,Fraction('-0.0976')),(27,Fraction('0.1768'))]}
  rows=[{'date':'2026-10-08','close':'100','volume':'10'},{'date':'2026-10-09','close':'90','volume':'10'},{'date':'2026-10-12','close':'1000','volume':'1000000'}]
  notes=observations({'panels':panels,'rows':rows})
  self.assertIn('pullback',' '.join(notes));self.assertIn('-10.00%',' '.join(notes));self.assertNotIn('1000',' '.join(notes))
  panels[3]['total_status']['withheld']=True
  notes=observations({'panels':panels,'rows':rows})
  self.assertIn('unavailable',notes[0]);self.assertNotIn('**Size of daily moves:**',' '.join(notes))
 def test_volume_missing_is_not_zero(self):
  from live_returns import calculate,observations
  e=Returns().evidence();rows=[{'date':d,'close':'100','volume':'10'} for d in e['sessions']]
  rows[-2]['volume']=None
  self.assertIn('volume is missing',' '.join(observations(calculate(rows,'INTC',e))))
class DailyExtension(unittest.TestCase):
 def test_approved_extension_and_review_hold(self):
  from live_returns import extend_evidence
  e={'end':'2026-10-12','sessions':['2026-10-12'],'stocks':{}}
  r={'version':1,'baseline_hash':'bound','captured_at':'2026-10-13','end':'2026-10-13','reviews':{'INTC':{'status':'READY_NO_NEW_EVENTS','candidate_end':'2026-10-13','reasons':[]}}}
  extended,note=extend_evidence(e,r,'INTC','bound')
  self.assertEqual(extended['sessions'],['2026-10-12','2026-10-13']);self.assertEqual(e['end'],'2026-10-12')
  r['reviews']['INTC']['status']='REVIEW_REQUIRED'
  with self.assertRaises(EvidenceUnavailable):extend_evidence(e,r,'INTC','bound')
 def test_binding_refused(self):
  from live_returns import extend_evidence
  with self.assertRaises(EvidenceUnavailable):extend_evidence({}, {'version':1,'baseline_hash':'wrong'},'INTC','bound')
if __name__=='__main__':unittest.main()
