"""Pure calculations. Unknown evidence never becomes a negative stress signal."""
from datetime import datetime, timezone
import hashlib,json,math
RULE_VERSION='shadow-1'
def digest(value):
 return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def oracle_metrics(rows):
 by={r['id']:r for r in rows}
 wanted=[by[k] for k in ('orcl_ocf','orcl_capex','orcl_prepay')]
 if any(r['quality']!='RECONCILED_TO_SOURCE' for r in wanted):raise ValueError('Unverified inputs')
 if len({(r['period'],r['unit'],r['source_sha256']) for r in wanted})!=1:raise ValueError('Incompatible inputs')
 ocf,capex,prepay=[r['value'] for r in wanted]
 if not all(isinstance(x,(float,int)) and math.isfinite(x) for x in (ocf,capex,prepay)) or capex<=0:raise ValueError('Invalid inputs')
 return {'OCF coverage':ocf/capex,'Coverage excluding specified prepayments':(ocf-prepay)/capex,'Specified prepayment coverage':prepay/capex,'Capex less operating cash flow':capex-ocf}
def shadow_rules(rows):
 metrics=oracle_metrics(rows)
 return [dict(rule='ORCL_OCF_BELOW_CAPEX',status='OBSERVED_HISTORICAL_CONDITION' if metrics['Capex less operating cash flow']>0 else 'NOT_OBSERVED',basis='Quarter ended 2026-08-31; OCF < capex',evidence='orcl_ocf, orcl_capex',version=RULE_VERSION),
 dict(rule='ORCL_CREDIT_WIDENING',status='UNAVAILABLE',basis='No qualified CDS/OAS series',evidence='None',version=RULE_VERSION),
 dict(rule='PROJECT_TERMS_DETERIORATION',status='INSUFFICIENT_EVIDENCE',basis='No comparable prior financing terms',evidence='iren_facility, iren_rate, iren_tenor',version=RULE_VERSION),
 dict(rule='APLD_PAYMENT_DETERIORATION',status='INSUFFICIENT_EVIDENCE',basis='Historical quarter only; no current payment series',evidence='apld_payment, apld_rent',version=RULE_VERSION),
 dict(rule='NEOCLOUD_BREADTH',status='UNAVAILABLE',basis='No qualified basket price/credit history',evidence='None',version=RULE_VERSION),
 dict(rule='GPU_FOUR_WEEK_DECLINE',status='INSUFFICIENT_EVIDENCE',basis='Single advertised-offer capture; region and availability unknown',evidence='lambda_h100, lambda_b200',version=RULE_VERSION)]
def contagion_state(rules):
 # This review release deliberately has no calibrated contagion classifier.
 return 'INSUFFICIENT_EVIDENCE'
def capture_age_hours(timestamp,now=None):
 then=datetime.fromisoformat(timestamp);now=now or datetime.now(timezone.utc)
 return max(0,(now-then).total_seconds()/3600)
def validate(snapshot):
 rows=snapshot['observations'];ids=[r['id'] for r in rows]
 if len(ids)!=len(set(ids)):raise ValueError('Duplicate observation')
 sources={s['source_id']:s for s in snapshot['sources']}
 for r in rows:
  s=sources[r['source_id']]
  if r['source_sha256']!=s['sha256'] or r['source_url']!=s['url']:raise ValueError('Source mismatch')
  if not r['locator'] or not r['period']:raise ValueError('Missing evidence coordinates')
 oracle_metrics(rows)
 return snapshot
