"""Append-only snapshot custody in a dedicated PostgreSQL schema.
Dashboard connections are read-only. Provision/import is a separate operator step.
"""
import json,hashlib
from pathlib import Path
from .engine import digest,validate
DDL='''CREATE SCHEMA IF NOT EXISTS foundation_monitor;
CREATE TABLE IF NOT EXISTS foundation_monitor.sources (
 sha256 text PRIMARY KEY, body bytea NOT NULL, first_captured_at timestamptz NOT NULL,
 url text NOT NULL, CHECK(length(sha256)=64));
CREATE TABLE IF NOT EXISTS foundation_monitor.snapshots (
 sha256 text PRIMARY KEY, snapshot_id text NOT NULL UNIQUE,
 recorded_at timestamptz NOT NULL DEFAULT now(), payload jsonb NOT NULL);
'''
def import_snapshot(conn,snapshot,raw_dir):
 validate(snapshot);sha=digest(snapshot)
 # Validate every byte before beginning a database mutation.
 bodies=[]
 for s in snapshot['sources']:
  raw=(Path(raw_dir)/(s['sha256']+'.html')).read_bytes()
  if hashlib.sha256(raw).hexdigest()!=s['sha256']:raise ValueError('Raw source hash mismatch')
  bodies.append((s,raw))
 with conn:
  with conn.cursor() as cur:
   cur.execute(DDL)
   cur.execute('SELECT sha256 FROM foundation_monitor.snapshots WHERE snapshot_id=%s',(snapshot['snapshot_id'],))
   prior=cur.fetchone()
   if prior and prior[0]!=sha:raise ValueError('Immutable snapshot ID collision')
   for s,raw in bodies:
    cur.execute('INSERT INTO foundation_monitor.sources VALUES (%s,%s,%s,%s) ON CONFLICT (sha256) DO NOTHING',(s['sha256'],raw,s['retrieved_at'],s['url']))
   cur.execute('INSERT INTO foundation_monitor.snapshots (sha256,snapshot_id,payload) VALUES (%s,%s,%s::jsonb) ON CONFLICT (sha256) DO NOTHING',(sha,snapshot['snapshot_id'],json.dumps(snapshot)))
 return sha

def read_snapshot(conn):
 conn.set_session(readonly=True,autocommit=False)
 try:
  with conn.cursor() as cur:
   cur.execute("SET LOCAL statement_timeout='8000ms'")
   cur.execute('SELECT sha256,payload FROM foundation_monitor.snapshots ORDER BY recorded_at DESC LIMIT 1')
   row=cur.fetchone()
   if not row:raise ValueError('No snapshot')
   sha,payload=row
   if digest(payload)!=sha:raise ValueError('Snapshot integrity mismatch')
   return validate(payload)
 finally:conn.rollback()
