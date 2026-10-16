"""Parse only visibly labelled 8-GPU Lambda offers. Unknown dimensions stay unknown."""
from html.parser import HTMLParser
from decimal import Decimal
import re
class Parser(HTMLParser):
 def __init__(self):
  super().__init__();self.stack=[];self.buttons={};self.button=None;self.panel=None;self.row=None;self.field=None;self.rows=[]
 def handle_starttag(self,tag,attrs):
  a=dict(attrs)
  if tag not in ('br','img','meta','input','link','hr','source','wbr'):self.stack.append((tag,self.panel))
  if a.get('role')=='tabpanel':self.panel=a.get('id')
  if tag=='button' and a.get('role')=='tab':self.button=[a.get('aria-controls'),'']
  if tag=='tr' and a.get('data-plan'):self.row={'model':a['data-plan'],'panel':self.panel}
  if self.row is not None and tag in ('td','th') and a.get('data-label'):self.field=a['data-label'];self.row[self.field]=''
 def handle_data(self,data):
  if self.button is not None:self.button[1]+=data
  if self.row is not None and self.field:self.row[self.field]+=data
 def handle_endtag(self,tag):
  if tag=='button' and self.button is not None:self.buttons[self.button[0]]=self.button[1].strip();self.button=None
  if tag in ('td','th'):self.field=None
  if tag=='tr' and self.row is not None:self.rows.append(self.row);self.row=None
  for i in range(len(self.stack)-1,-1,-1):
   if self.stack[i][0]==tag:self.panel=self.stack[i][1];del self.stack[i:];break

def parse_offers(body):
 p=Parser();p.feed(body.decode('utf-8'));out=[]
 for r in p.rows:
  if p.buttons.get(r['panel'])!='8x' or r['model'] not in ('NVIDIA H100 SXM','NVIDIA B200 SXM6'):continue
  price=r.get('PRICE/GPU/HR*','').strip()
  if not re.fullmatch(r'\$\d+(\.\d{1,4})?',price):raise ValueError('PRICE_SCHEMA_CHANGED')
  amount=Decimal(price[1:])
  if not 0<amount<1000:raise ValueError('INVALID_PRICE')
  for key in ('VRAM/GPU','vCPUs','RAM','STORAGE'):
   if not r.get(key,'').strip():raise ValueError('MISSING_CONFIGURATION')
  out.append(dict(provider='Lambda',model=r['model'],gpu_count=8,usd_gpu_hour=str(amount),usd_node_hour=str(amount*8),vram=r['VRAM/GPU'].strip(),vcpus=r['vCPUs'].strip(),ram=r['RAM'].strip(),storage=r['STORAGE'].strip(),region='UNKNOWN',availability='NOT_OBSERVED',interconnect='NOT_VERIFIED',contract='Advertised on-demand; tax excluded',quality='OBSERVED_OFFER_PARTIAL'))
 if len(out)!=2 or len({r['model'] for r in out})!=2:raise ValueError('OFFER_SCHEMA_CHANGED')
 return out
