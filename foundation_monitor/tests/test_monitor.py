import unittest,json,copy
from pathlib import Path
from foundation_monitor.engine import validate,oracle_metrics,shadow_rules,contagion_state,digest,capture_age_hours
from datetime import datetime,timezone
S=json.loads((Path(__file__).parents[1]/'data/snapshot.json').read_text())
class Tests(unittest.TestCase):
 def test_source_bindings(self):validate(S)
 def test_no_double_subtraction(self):
  m=oracle_metrics(S['observations']);self.assertEqual(m['Capex less operating cash flow'],5396);self.assertAlmostEqual(m['Coverage excluding specified prepayments'],11740/28499)
 def test_period_mismatch_refused(self):
  s=copy.deepcopy(S);s['observations'][0]['period']='annual'
  with self.assertRaises(ValueError):oracle_metrics(s['observations'])
 def test_unverified_refused(self):
  s=copy.deepcopy(S);s['observations'][0]['quality']='UNVERIFIED_SEED'
  with self.assertRaises(ValueError):oracle_metrics(s['observations'])
 def test_missing_is_not_no_stress(self):self.assertEqual(contagion_state(shadow_rules(S['observations'])),'INSUFFICIENT_EVIDENCE')
 def test_no_fake_market_rules(self):
  rules=shadow_rules(S['observations']);self.assertEqual(rules[1]['status'],'UNAVAILABLE');self.assertEqual(rules[-1]['status'],'INSUFFICIENT_EVIDENCE')
 def test_revisions_change_digest(self):
  s=copy.deepcopy(S);s['observations'][0]['value']+=1;self.assertNotEqual(digest(s),digest(S))
 def test_hash_rebind_refused(self):
  s=copy.deepcopy(S);s['observations'][0]['source_sha256']='x'
  with self.assertRaises(ValueError):validate(s)
 def test_duplicate_refused(self):
  s=copy.deepcopy(S);s['observations'].append(s['observations'][0])
  with self.assertRaises(ValueError):validate(s)
 def test_offer_staleness(self):self.assertEqual(capture_age_hours('2026-10-14T00:00:00+00:00',datetime(2026,10,16,tzinfo=timezone.utc)),48)
 def test_ui(self):
  from streamlit.testing.v1 import AppTest
  app=AppTest.from_file(str(Path(__file__).parents[1]/'app.py')).run(timeout=20)
  self.assertFalse(app.exception);self.assertEqual(len(app.tabs),6);self.assertEqual(len(app.metric),3)
if __name__=='__main__':unittest.main()
