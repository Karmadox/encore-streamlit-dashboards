from model import features

GUIDE = [
 ('Price', 'The saved unadjusted PX_LAST price. This shows the price path, not a return including dividends.'),
 ('Cumulative total return', 'The compounded gain or loss since 31 December 2025, including the assumed cash dividends. This curve includes each plotted day.'),
 ('3 / 9 / 27-session total returns', 'Short, medium and longer trailing views. Each origin uses information only through the previous session. Matching signs show direction aligned across scales; opposing signs show a shorter move against the longer trend. These are observations, not trading rules.'),
 ('Per-share price return', 'Price change excluding cash dividends, with assumed split treatment. Compare with total return to see the cash-distribution contribution.'),
 ('RMS daily returns', 'The typical magnitude of daily total-return moves within the window, without subtracting the average. Higher means larger moves, not necessarily a falling price. It is unannualized and is not conventional demeaned volatility.'),
 ('Volume', 'Bloomberg-reported activity in assumed shares. Spikes show unusual activity to investigate; they do not identify buying pressure or its cause.'),
 ('Missingness and assumptions', 'Every panel reports missing observations. Zero missing values means input availability, not verified corporate-action completeness. Read the assumptions tab before interpreting the results.')
]

def summary(rows):
 latest={h:features(rows,h)[-1] for h in (3,9,27)}
 vals=[latest[h]['total'] for h in (3,9,27)]
 if any(v is None for v in vals):direction='Cross-scale interpretation unavailable because a return is missing.'
 elif all(v>0 for v in vals):direction='All three scales are positive: the latest short, medium and longer trailing directions are aligned upward.'
 elif all(v<0 for v in vals):direction='All three scales are negative: the latest short, medium and longer trailing directions are aligned downward.'
 elif vals[0]<0 and vals[2]>0:direction='The 3-session return is negative while the 27-session return is positive: a short pullback within a positive longer window.'
 elif vals[0]>0 and vals[2]<0:direction='The 3-session return is positive while the 27-session return is negative: a short rebound within a negative longer window.'
 else:direction='Directions differ across scales: the latest windows do not show a uniformly aligned trend.'
 rms3,rms27=latest[3]['rms'],latest[27]['rms']
 magnitude=f'The 3-session RMS is {float(rms3):.2%}, versus {float(rms27):.2%} over 27 sessions: recent daily move magnitude is '+('higher.' if rms3>rms27 else 'lower or equal.')
 display=[r for r in rows if r['date']>='2026-01-01']
 peak=max(display,key=lambda r:float(r['close']))
 draw=float(display[-1]['close'])/float(peak['close'])-1
 return latest,[direction,magnitude,f'The latest price is {draw:.2%} relative to the highest observed 2026 closing price ({peak["date"]}). This is an endpoint comparison, not maximum drawdown.', 'SCFR use: inspect how direction and move magnitude change across scales. No calibrated threshold, forecast, causal attribution or buy/sell signal is established.']
