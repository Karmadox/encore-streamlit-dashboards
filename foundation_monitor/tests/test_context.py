import unittest
from datetime import date,timedelta
from foundation_monitor.context import equity_context

def sample():
 days=[(date(2026,1,1)+timedelta(days=i)).isoformat() for i in range(21)]
 return dict(sha256='a'*64,retrieved_at='2026-02-01T00:00:00Z',coverage=[{'ticker':t,'status':'STORED_CLOSE_AVAILABLE'} for t in ['CRWV','B']],prices=[{'ticker':t,'date':d,'close':str(100+i)} for t in ['CRWV','B'] for i,d in enumerate(days)])
class ContextTests(unittest.TestCase):
 def test_exact_endpoint_and_interval_count(self):
  r=equity_context(sample())['rows'][0]
  self.assertAlmostEqual(r['change_20_pct'],20);self.assertAlmostEqual(r['change_5_pct'],100*(120/115-1));self.assertEqual(r['start_20'],'2026-01-01')
 def test_missing_inside_window_refused(self):
  p=sample();p['prices']=[r for r in p['prices'] if not(r['ticker']=='B' and r['date']=='2026-01-19')]
  r=equity_context(p)['rows'][0];self.assertEqual(r['status_5'],'MISSING_OBSERVATIONS');self.assertIsNone(r['change_5_pct'])
 def test_long_gap_not_short_gap(self):
  p=sample();p['prices']=[r for r in p['prices'] if not(r['ticker']=='B' and r['date']=='2026-01-02')]
  r=equity_context(p)['rows'][0];self.assertEqual(r['status_5'],'AVAILABLE');self.assertEqual(r['status_20'],'MISSING_OBSERVATIONS')
 def test_stale_end_is_not_independent_window(self):
  p=sample();p['prices']=[r for r in p['prices'] if not(r['ticker']=='B' and r['date']=='2026-01-21')]
  self.assertEqual(equity_context(p)['rows'][0]['status_5'],'MISSING_OBSERVATIONS')
 def test_coverage_refusal(self):
  p=sample();p['coverage'][1]['status']='STALE_STORED_CLOSE'
  self.assertIsNone(equity_context(p)['rows'][0]['change_5_pct'])
 def test_duplicates(self):
  p=sample();p['prices'].append(p['prices'][0])
  with self.assertRaises(ValueError):equity_context(p)
 def test_nonfinite(self):
  p=sample();p['prices'][0]['close']='NaN'
  with self.assertRaises(ValueError):equity_context(p)
 def test_insufficient(self):
  p=sample();p['prices']=p['prices'][:4]
  self.assertEqual(equity_context(p)['rows'][0]['status_20'],'INSUFFICIENT_HISTORY')
 def test_empty(self):self.assertEqual(equity_context(None)['rows'],[])
 def test_all_up_is_not_contagion(self):
  r=equity_context(sample());self.assertIn('2 rose, 0 fell',r['observations'][0]);self.assertIn('do not identify',r['observations'][-1])
if __name__=='__main__':unittest.main()
