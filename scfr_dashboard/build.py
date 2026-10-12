import base64, gzip, hashlib, json
import streamlit as st
from model import features,cumulative_segments,SCALES

def load_snapshot():
    try:
        raw=base64.b64decode(st.secrets['scfr']['snapshot_gzip_base64'],validate=True)
        if hashlib.sha256(raw).hexdigest()!=st.secrets['scfr']['snapshot_sha256']:
            raise ValueError('hash mismatch')
        return json.loads(gzip.decompress(raw))
    except Exception:
        st.error('Private snapshot is missing or invalid. Contact the app owner.')
        st.stop()

SNAPSHOT=load_snapshot()
ASSUMPTIONS=SNAPSHOT['assumptions']
def stocks():return SNAPSHOT['stocks']
def download(name):return SNAPSHOT['files'][name].encode('utf-8')

def panels(rows):
    display = [r for r in rows if r['date'] >= '2026-01-01']
    dates = [r['date'] for r in display]
    yield ('Unadjusted PX_LAST — USD/share', dates, [r['close'] for r in display], False)
    base = next((i for (i, r) in enumerate(rows) if r['date'] == '2025-12-31'))
    curve = cumulative_segments(rows[base:])[1:]
    yield ('Cumulative total return — base 31 Dec 2025', dates, [r['value'] for r in curve], True)
    for h in SCALES:
        records = [r for r in features(rows, h) if r['date'] >= '2026-01-01']
        for (key, label) in [('total', 'Total return'), ('per_share', 'Per-share price return'), ('rms', 'Unannualized RMS daily total returns')]:
            yield (f'{label} · {h} sessions · lagged to previous session', dates, [r[key] for r in records], True)
    yield ('Bloomberg-reported volume — assumed shares', dates, [r['volume'] for r in display], False)
