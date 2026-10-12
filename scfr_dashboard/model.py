"""Synthetic-only measurement prototype. Not an approved production calculator."""
from fractions import Fraction
from decimal import Decimal, localcontext
from collections import Counter

SCALES = (3, 9, 27)

def quantize18(value):
    """Exact rational half-even rounding; no intermediate decimal division."""
    scaled = Fraction(value) * 10**18
    sign = -1 if scaled < 0 else 1
    q, r = divmod(abs(scaled.numerator), scaled.denominator)
    if 2*r > scaled.denominator or (2*r == scaled.denominator and q % 2):
        q += 1
    return Fraction(sign*q, 10**18)

def daily(previous, current, actions):
    if previous is None or current is None or actions is None:
        return None
    q, cash = Fraction(1), Fraction(0)
    for kind, value in actions:  # Already evidenced sequence; never sorted here.
        value = Fraction(value)
        if kind == 'split':
            if value <= 0: raise ValueError('invalid split')
            q *= value
        elif kind == 'cash':
            if value < 0: raise ValueError('invalid distribution')
            cash += q*value
        else: raise ValueError('unsupported action')
    if Fraction(previous) <= 0 or Fraction(current) <= 0: raise ValueError('invalid price')
    return quantize18((q*Fraction(current)+cash)/Fraction(previous)-1)

def panel_status(values):
    n, nulls = len(values), sum(v is None for v in values)
    return {'origins':n,'nulls':nulls,'share':nulls/n if n else None,
            'withheld':bool(n and nulls*5 > n),
            'density_note':bool(n and nulls*20 > n)}

def features(rows, h):
    """Rows are a complete supplied session grid. Gaps must not be dropped."""
    d = [None] + [daily(rows[i-1]['close'],rows[i]['close'],rows[i]['actions']) for i in range(1,len(rows))]
    output=[]
    for t, row in enumerate(rows):
        start, end = t-h-1, t-1
        record={'date':row['date'],'latest_input':rows[end]['date'] if end>=0 else None,'total':None,'per_share':None,'rms':None,'total_cause':None,'per_share_cause':None}
        if start < 0:
            record.update(total_cause='Synthetic warm-up',per_share_cause='Synthetic warm-up');output.append(record);continue
        span=rows[start+1:end+1]
        returns=d[start+1:end+1]
        if all(v is not None for v in returns):
            product=Fraction(1)
            for value in returns: product *= 1+value
            record['total']=quantize18(product-1)
            with localcontext() as ctx:
                ctx.prec=60
                mean=sum(v*v for v in returns)/h
                record['rms']=(Decimal(mean.numerator)/Decimal(mean.denominator)).sqrt()
        else: record['total_cause']='Synthetic missing daily dependency'
        split_factor=Fraction(1)
        if rows[start]['close'] is not None and rows[end]['close'] is not None and all(r['actions'] is not None for r in span):
            for r in span:
                for kind,value in r['actions']:
                    if kind=='split':split_factor*=Fraction(value)
            record['per_share']=quantize18(Fraction(rows[end]['close'])*split_factor/Fraction(rows[start]['close'])-1)
        else: record['per_share_cause']='Synthetic missing endpoint or action coverage'
        output.append(record)
    return output

def cumulative_segments(rows):
    """Observation-date TR; missing return ends chain. New base is explicit."""
    result=[];base=None;product=Fraction(1)
    for i,row in enumerate(rows):
        value=None
        ret=daily(rows[i-1]['close'],row['close'],row['actions']) if i else None
        if base is not None and ret is not None:
            product*=1+ret;value=quantize18(product-1)
        else:
            base=None;product=Fraction(1)
            if row['close'] is not None:
                # A new priced observation can anchor a future chain. This
                # zero is a declared base, not a substituted missing return.
                base=row['date'];value=Fraction(0)
        result.append({'date':row['date'],'base':base,'value':value,
                       'is_base':base==row['date'],'daily_return':ret})
    return result
