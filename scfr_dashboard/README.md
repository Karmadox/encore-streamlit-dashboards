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
