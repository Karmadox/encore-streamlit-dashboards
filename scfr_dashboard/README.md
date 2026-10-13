# SCFR fixed-snapshot review dashboard

Entry point: `scfr_dashboard/app.py`.

Public repository contains code only. Do not commit Bloomberg exports, calculated
reports, snapshot contents, or passwords. Authentication runs before snapshot
loading, chart generation or download controls.

Configure Streamlit Secrets with the existing dashboard convention:

```toml
[auth]
password = "SET_PRIVATELY"

[scfr]
snapshot_gzip_base64 = "SET_FROM_PRIVATE_REVIEW_PACKAGE"
snapshot_sha256 = "SHA256_OF_COMPRESSED_SNAPSHOT"
```

The snapshot is a private compressed JSON package, encoded for transport in
Secrets; base64 is not encryption. Keep it out of Git and issue comments.
Deploy with Python 3.11. No database or Bloomberg credentials are required.
There is no live retrieval or automatic refresh. The Update report button
switches the selected stock within the fixed snapshot.

Before sharing, verify that a signed-out session sees only the login page,
incorrect passwords expose no data, and both stock selections work after login.
The shared-password approach follows the other dashboards; it does not provide
individual reviewer identity or individual revocation. Rotate the secret to
revoke shared access. Publishing and configuring real secrets remain pending.

## Optional direct EncoreDB view

The Report source selector now separates the reviewed saved pilot from current
EncoreDB observations. The saved pilot remains the default and is unchanged.
Database imports and reads occur only after authentication and selection of the
EncoreDB view. Add the same private `[db]` settings used by the existing dashboards:

```toml
[db]
host = "SET_PRIVATELY"
port = 5432
dbname = "SET_PRIVATELY"
user = "SET_PRIVATELY"
password = "SET_PRIVATELY"
sslmode = "require"
```

Keep `[auth]` and the existing `[scfr]` snapshot values. Do not paste credentials
into source code, issues or chat. The database endpoint must permit connections
from the Streamlit host; this change does not modify firewall or database grants.
A read-only transaction is enforced, but does not make an administrative login a
least-privilege account. Prefer an existing usable read-only credential if one is
available; no account creation is performed here.

The new view reads only the selected stock from the 15-name allowlist and excludes
the current New York day. It uses TLS, ten-second connection/statement limits,
read-only repeatable read, a 1,600-row ceiling, rollback and close. Identity
ambiguity and validation errors withhold results. No automatic retry is performed.
Data cache lasts five minutes; Update report evicts the selected stock/date/settings
cache entry and reads again. No ingestion or vendor request is triggered. Results
stay server-side except for chart data sent to authenticated viewers. Shared-password
sessions share the cache. Secrets participate in its key, not its returned value.

This first database view shows stored close and volume observations. Adjustment
basis, complete session grids and corporate-action coverage remain unresolved;
it does not compute or relabel these as verified total returns or per-share returns.
The reviewed 3/9/27 analysis remains accessible in the snapshot. Completing the
live analytics requires compatible action evidence and an explicit calendar grid.
The database view has no snapshot report download, avoiding mixed-source exports.

### Bounded EncoreDB return panels

Append the separately supplied private `[scfr_returns]` section to Streamlit Secrets. It contains `evidence_zlib_base64` and `evidence_sha256`. Do not commit the evidence or secrets to this public repository. Existing `[auth]`, `[db]` and `[scfr]` settings remain in place.

The current evidence supports CHTR, CNC, ELV, HUM, INTC, MOH, T, TMUS, UNH and VZ through 12 October 2026. It is a current-vintage descriptive package with explicitly assumed broader action completeness. CMCSA returns are deferred; DELL, ZTS, CHWY and SPCX retain split-history exceptions. New database dates do not extend return eligibility. Daily-granularity panels show rolling 3/9/27-session total returns and separately labelled per-share price returns, using the existing lag convention. Missing calendar sessions remain in the denominator.
