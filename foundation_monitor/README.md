# AI Infrastructure Foundation Monitor — review release

An evidence-backed first review inside Encore's private Azure Streamlit hub. All five signal families are visible. Financial disclosures are a curated, source-reconciled baseline; market captures refresh hourly from EncoreDB and public GPU offers are captured daily. Missing inputs block inferences; no paid feed, calibrated production threshold or notification is enabled.

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

## Review update v2 — October 16, 2026

The second evidence snapshot preserves v1 and adds ten facts from CoreWeave's August 11 quarterly release and Applied Digital's October 7 release. New-period facts do not replace older-period facts. Publication dates are known for these releases, but exact publication timestamps and historical customer availability remain unverified. No claim is made that this is an exhaustive search of subsequent disclosures.

The private Azure app runs `python -m foundation_monitor.collect_gpu --loop` alongside Streamlit. It checks hourly, retains an EncoreDB market capture at most once per 55 minutes, and requests Lambda's public page at most once per 24 hours (failed attempts count). PostgreSQL advisory locks prevent duplicate concurrent collectors. Existing app settings supply database credentials in memory. The app's existing Always On setting is required for continuous operation; collection pauses when the app is stopped. Collector statuses are written to the protected Azure LogFiles directory. No notifications are sent. This is a lightweight first collector, not a separately managed worker service.

`market_captures` stores exact rows returned and a digest; `gpu_captures` stores captured offers or failures and links retained source bytes. The dashboard reads only these records; clicking Refresh does not call providers. Captures are append-only through the application contract, with no destructive expiry job. Future retention sizing and migration remain an engineering follow-up.

Market scope is fixed to 12 monitored names and at most 180 calendar days through the preceding New York date. Unknown/ambiguous mappings, invalid prices and stale observations are explicit. Available prices are indexed from a common observed start date for selected names. These are stored-close comparisons, not total returns or a qualified contagion measure. No dividend/split assumptions, missing-date fill, breadth inference or security-master write is introduced. At v2 launch, five names had no instrument mapping; the coverage update below resolves that gap.

Lambda's parser binds the visible 8x tab to its rows and records GPU model/count, VRAM, CPU/RAM/storage and price units. Unknown geography, interconnect and live availability remain unknown. Schema changes fail rather than silently substitute a configuration. One capture day cannot establish a trend. A collection age above 25 hours and a market capture age above two hours are operational freshness labels, not market thresholds.

## Equity coverage update — October 16, 2026

Bloomberg Desktop API confirmed the current ticker, name, USD currency, common-stock type and FIGI for APLD, CIFR, CORZ, IREN and WULF. The private bounded operator script (retained with execution evidence, outside this public repository) then added only those five previously absent identities and 625 closing-price rows to the existing EncoreDB tables: 125 observations per name from April 20 through October 15, 2026. No existing identity or price row was overwritten. The exact returned prices, request adjustment settings, capture timestamp and identity references are retained under a SHA-256 in `foundation_monitor.equity_backfills`.

The backfill explicitly disabled DPDF, dividend and split adjustment. Existing series and later scheduled loads retain their own ingestion conventions; this is still stored-price context, not a homogeneous qualified return series. Corporate-action validation and historical listing/availability qualification remain separate. Historical identity changes are not ruled out by current identity confirmation.

The existing general Equity Price Loader selects the instrument master in the retained repository source. Its Windows task was Ready, last result 0, with its next run scheduled for October 16 at 18:30 (VM scheduler time). The five new identities are therefore eligible for that loader without adding a duplicate scheduled job. This does not certify the next run's outcome; inspect subsequent data timestamps before claiming continued freshness. The hourly monitor collector consumes the stored prices and retains coverage separately.

The onboarding script is deliberately not a recurring job: an existing identity causes refusal before writes. Its transaction binds identity inserts, price inserts and the retained receipt together. Operators must review a new universe/date scope rather than rerun this completed five-name backfill.

## Price-context update — October 16, 2026

`context.py` derives five- and twenty-observation-interval stored-price changes using common endpoints from the union of retained dates. Each interval needs n+1 prices and every observed date inside the window. Missing dates or stale source status withhold that stock/window; dates missing from all stocks cannot be detected without a calendar. These are descriptive display windows, not the SCFR 3/9/27 specification, calibrated alerts or action-qualified total returns. Corporate actions and heterogeneous ingestion settings can affect the comparison.

The dashboard now presents up/down/unchanged counts with explicit available/unavailable denominators and CoreWeave's two horizon changes. A downloadable calculation record includes endpoint prices/dates, method version and source-capture hash. It does not infer financing causes, escalation or contagion. A refresh-status table distinguishes curated filing evidence, database-capture age, per-stock price dates and the latest successful GPU capture. The obsolete five-unmapped-stock warning is removed.

## SEC disclosure watch — October 16, 2026

The existing hourly worker now checks four SEC submissions indexes at most once per issuer per 24 hours: Oracle, Applied Digital, IREN and CoreWeave. Failed attempts count toward the interval. Requests identify Encore/contact, have byte/time limits and no application retry, and are separated by one second. The service uses the [SEC public submissions API](https://www.sec.gov/search-filings/edgar-application-programming-interfaces); no new subscription or API key is needed.

Scope is the recent index and trailing 90 calendar days of 8-K, 10-Q, 10-K, 6-K, 20-F and 40-F, including amendments. No historical index pagination, filing text or exhibits are automatically fetched. A zero result means no matching metadata in this scope, not no material development. Issuer identity, column lengths, accession IDs, dates and document paths are validated. HTTP/schema failures remain separate from successful empty results.

`filing_captures` retains each result/failure with a digest and raw-source link. `filing_versions` preserves first-observed times for each accession/metadata hash. Baseline, newly observed, changed metadata and unchanged records are distinguished. The UI verifies capture digests. The first baseline contains 17 matching entries across four issuers. These are document-review candidates, not newly verified financial observations; curated figures, shadow rules and notifications remain untouched. SEC acceptance times do not establish historical customer availability, and local first observation is explicitly distinct.

Provision `filings.DDL` before deploying the collector. Initial public captures and database-backed rendering succeeded locally. The first subsequent scheduled cloud request remains to be observed; a worker startup/due check alone does not establish successful future SEC access. Collection stops when the app stops; existing Always On is retained. Failures and captures older than 25 hours are visible in the disclosure-watch status table.
