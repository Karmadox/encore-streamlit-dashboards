import unittest
from datetime import date
from foundation_monitor.gpu import parse_offers
from foundation_monitor.market import validate_prices,comparable_index
from foundation_monitor.collect_gpu import capture

def html(price='3.99',count='8x'):
 def row(model,price):
  return '<tr data-plan="'+model+'">'+''.join('<td data-label="'+k+'">'+v+'</td>' for k,v in [('VRAM/GPU','80 GB'),('vCPUs','208'),('RAM','1800 GiB'),('STORAGE','22 TiB SSD'),('PRICE/GPU/HR*','$'+price)])+'</tr>'
 return ('<button role="tab" aria-controls="p">'+count+'</button><div role="tabpanel" id="p"><table>'+row('NVIDIA H100 SXM',price)+row('NVIDIA B200 SXM6','6.69')+'</table></div>').encode()
class UpdateTests(unittest.TestCase):
 def test_parser_binds_gpu_count(self):self.assertEqual(parse_offers(html())[0]['usd_node_hour'],'31.92')
 def test_not_4_gpu(self):
  with self.assertRaises(ValueError):parse_offers(html(count='4x'))
 def test_schema_changes_refused(self):
  with self.assertRaises(ValueError):parse_offers(html(price='Contact sales'))
 def test_region_not_invented(self):self.assertEqual(parse_offers(html())[0]['region'],'UNKNOWN')
 def test_duplicates_refused(self):self.assertIsNone(validate_prices([(date(2026,1,2),10),(date(2026,1,2),11)],date(2026,1,1),date(2026,1,5)))
 def test_invalid_prices_refused(self):
  for value in [0,-1,None,'NaN','Infinity']:
   self.assertIsNone(validate_prices([(date(2026,1,2),value)],date(2026,1,1),date(2026,1,5)))
 def test_common_base(self):
  out=comparable_index([{'ticker':'A','date':'2026-01-01','close':'10'},{'ticker':'A','date':'2026-01-02','close':'20'},{'ticker':'B','date':'2026-01-02','close':'5'}],['A','B'])
  self.assertEqual(list(out.index),['2026-01-02']);self.assertTrue((out.iloc[0]==100).all())
 def test_missing_not_filled(self):
  out=comparable_index([{'ticker':'A','date':'2026-01-01','close':'10'},{'ticker':'B','date':'2026-01-01','close':'5'},{'ticker':'A','date':'2026-01-02','close':'20'}],['A','B'])
  self.assertTrue(out.loc['2026-01-02'].isna()['B'])
 def test_capture_not_due_no_http(self):
  from datetime import datetime,timezone
  now=datetime.now(timezone.utc)
  class C:
   def __enter__(self):return self
   def __exit__(self,*a):pass
   def execute(self,*a):self.q=a[0]
   def fetchone(self):return (True,) if 'lock' in self.q else (now,)
   def cursor(self):return self
  def no_http(*a,**k):raise AssertionError('Unexpected request')
  self.assertEqual(capture(C(),get=no_http,now=now),'NOT_DUE')
if __name__=='__main__':unittest.main()
