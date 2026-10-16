# AI Infrastructure Foundation Monitor — review release

An evidence-backed first review inside Encore's private Azure Streamlit hub. All five signal families are visible. This release is a curated, source-reconciled baseline, not a continuously refreshed market monitor. Missing inputs block inferences; no paid feed, calibrated production threshold or notification is enabled.

## Ownership and storage

Engineering owns the separate `foundation_monitor` schema in existing Encore PostgreSQL. It contains original source bytes keyed by SHA-256 and immutable-by-import-contract snapshot versions. It does not change positions, SCFR or security-master records. This remains the existing database hosting arrangement; an Azure database migration is not part of this release.

`store.import_snapshot` is an operator-only import: it validates all raw hashes first, refuses changed content under an existing snapshot ID, and imports atomically. Corrections require a new snapshot ID and explicit observation revisions/supersession. Database owners can still alter tables; this is not a WORM store. The dashboard uses a read-only transaction, bounded statement timeout, integrity check and five-minute cache. A DB error does not fall back to bundled data.

Set `FOUNDATION_MONITOR_DB=1` in the private hub runtime to read from existing `st.secrets['db']`. With that flag absent, the app explicitly identifies the sealed local review snapshot. No secrets belong in Git. Authentication is inherited from the Azure hub; this module should not be independently published publicly.

## Evidence and calculations

14 observations were reconciled against three filings/issuer releases and a Lambda offer page on 2026-10-16. Five exact public source captures (including Vast, not used for comparisons) are retained in PostgreSQL. The JSON carries dates, source links, locators, source hashes, capture timestamps, evidence types, status and revision IDs. Source publication timestamps remain unknown; capture does not establish historical availability.

Oracle: prepayments already enter OCF; the capex-minus-OCF gap does not subtract them twice. Ex-prepayment coverage is separately labelled. All inputs use the same quarter and USD-million basis.

APLD: the cited quarter ended November 2025; fit-out and rent remain separate. Conditional preferred capital is not cash.

IREN: facility ceiling, fixed rate and tenor are not funded balance, spread or evidence of repricing. Equipment acceptance and grace periods matter.

GPU: single advertised 8-GPU offer capture, unverified geography/live capacity; no synthetic four-week trend. Offers older than 24 hours are flagged stale. The 24-hour label is a presentation convention, not a calibrated trading threshold.

Unreconciled seed figures remain visible and excluded. No contagion classifier is calibrated or enabled. The shadow table distinguishes historical arithmetic from unavailable and insufficient inputs.

## Local review and tests

From repository root:

```
python -m unittest discover -s foundation_monitor/tests -v
streamlit run foundation_monitor/app.py
```

For the Azure hub add `foundation_monitor/app.py` to `reports.json` with title `AI Infrastructure Foundation Monitor` and path `foundation-monitor`. The existing hub keeps Home first and sorts reports alphabetically.

## Next iteration

Reconcile current CRWV and APLD source material, retain repeated GPU observations with matched geography/contract/configuration, and connect existing qualified market series. Add a scheduled capture/import job after reviewing this initial evidence surface. Provider subscriptions, live notifications and calibrated thresholds remain outside this approval.
