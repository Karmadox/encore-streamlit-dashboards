import sys,unittest
from pathlib import Path
from datetime import date
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from db_source import read_stock,DataUnavailable
class Conn:
 class Info:ssl_in_use=True
 info=Info()
 def __init__(self):self.sql=[];self.cleanup=[];self.identities=[(21,'INTC',None,'Equity','USD','INTC US Equity',True)];self.rows=[(date(2026,10,12),'10.5',None)];self.guard=('postgres','on')
 def set_session(self,**kw):assert kw==dict(readonly=True,autocommit=False,isolation_level='REPEATABLE READ')
 def cursor(self):return self
 def __enter__(self):return self
 def __exit__(self,*a):pass
 def execute(self,s,p=None):self.sql.append((s,p))
 def fetchone(self):return self.guard
 def fetchmany(self,n):return self.identities if 'FROM encoredb.instruments' in self.sql[-1][0] else self.rows
 def rollback(self):self.cleanup.append('rollback')
 def close(self):self.cleanup.append('close')
class Tests(unittest.TestCase):
 def setUp(self):self.c=Conn();self.settings=dict(host='example.invalid',dbname='postgres',user='fake',password='fake')
 def run_read(self):return read_stock(self.settings,'INTC',date(2026,10,12),lambda **kw:self.c)
 def test_read_and_cleanup(self):
  r=self.run_read();self.assertEqual(r['rows'][0]['close'],'10.5');self.assertIsNone(r['rows'][0]['volume']);self.assertEqual(self.c.cleanup,['rollback','close']);self.assertEqual(len(self.c.sql),7)
 def test_daily_review_read(self):
  proof={'version':1,'reviews':{}}
  def fetchone():
   q=self.c.sql[-1][0]
   if 'to_regclass' in q or 'has_table_privilege' in q:return (True,)
   if 'SELECT review' in q:return (proof,)
   return self.c.guard
  self.c.fetchone=fetchone
  self.assertEqual(self.run_read()['daily_review'],proof)
  self.assertEqual(self.c.cleanup,['rollback','close'])
 def test_review_privilege_absent(self):
  def fetchone():
   q=self.c.sql[-1][0]
   if 'to_regclass' in q:return (True,)
   if 'has_table_privilege' in q:return (False,)
   return self.c.guard
  self.c.fetchone=fetchone
  self.assertIsNone(self.run_read()['daily_review'])
  self.assertFalse(any('SELECT review' in q for q,p in self.c.sql))
 def test_duplicate_identity(self):
  self.c.identities*=2
  with self.assertRaises(DataUnavailable):self.run_read()
  self.assertEqual(self.c.cleanup,['rollback','close']);self.assertEqual(len(self.c.sql),5)
 def test_missing(self):
  self.c.identities=[]
  with self.assertRaises(DataUnavailable):self.run_read()
 def test_wrong_currency(self):
  self.c.identities=[(21,'INTC',None,'Equity','EUR','INTC US Equity',True)]
  with self.assertRaises(DataUnavailable):self.run_read()
 def test_row_cap(self):
  self.c.rows*=1601
  with self.assertRaises(DataUnavailable):self.run_read()
 def test_duplicate_dates(self):
  self.c.rows*=2
  with self.assertRaises(DataUnavailable):self.run_read()
 def test_invalid_numeric(self):
  self.c.rows=[(date(2026,10,12),'NaN',0)]
  with self.assertRaises(DataUnavailable):self.run_read()
 def test_readonly(self):
  self.c.guard=('postgres','off')
  with self.assertRaises(DataUnavailable):self.run_read()
  self.assertEqual(len(self.c.sql),4)
 def test_no_tls(self):
  self.settings['sslmode']='disable'
  with self.assertRaises(DataUnavailable):self.run_read()
  self.assertEqual(self.c.sql,[])
 def test_no_error_leak(self):
  def bad(**kw):raise RuntimeError('super-secret-password')
  with self.assertRaises(DataUnavailable) as ex:read_stock(self.settings,'INTC',connector=bad)
  self.assertNotIn('super-secret',str(ex.exception))
 def test_cleanup_failure_withholds(self):
  def bad():raise RuntimeError('secret')
  self.c.rollback=bad
  with self.assertRaises(DataUnavailable):self.run_read()
  self.assertIn('close',self.c.cleanup)
if __name__=='__main__':unittest.main()
