# Sales Report v2 — Deployed Architecture

> Authoritative reference for the production sales-report stack as of the
> Option B simplification. Replaces ad-hoc tribal knowledge across past
> hotfix branches.

---

## 1. Components at a Glance

| Layer | Component | Purpose |
|---|---|---|
| Source of truth | **SharePoint** — `qmsmedicosmetics.sharepoint.com/sites/DATAANDREPORTING/Shared Documents/SAP Extracts` | Daily SAP extract drops + mapping CSVs |
| Cold cache | **Azure Blob `reporting-inputs`** | Last-known-good copy of every SP input (auto-mirrored) |
| Generator | **Azure Function `refresh_unified_v2_timer`** | Runs `src/full_report_v2.py` once per business day, produces all artefacts |
| Artefact store | **Azure Blob `reporting-outputs`** | Single, authoritative copy of every generated report (HTML/PDF/CSV/JSON) |
| Email dispatch | **Azure Functions** `dispatch_reports`, `core_market_reports`, `dispatch_usa_spa_reports` | Pure consumers — hydrate from blob, send email |
| Web UI | **FastAPI `fastapi_web_app/main.py`** | Reads from `reporting-outputs` blob; serves to browsers |

All Azure resources live in resource group `qms-dispatch-reports_group`,
region **Germany West Central**, on a Linux **Consumption** plan.
Time zone is **Europe/Berlin** throughout.

---

## 2. Data Flow

```
                    ┌────────────────────────────────────────┐
                    │           SharePoint (SoT)             │
                    │  /SAP Extracts/new_unified_dbo_qry_*   │
                    │  /SAP Extracts/entity_mappings.csv     │
                    │  /SAP Extracts/py25_regional_mappings  │
                    │  /SAP Extracts/budget_*_processed.csv  │
                    └─────────────────┬──────────────────────┘
                                      │  (1) downloaded by full_report_v2.py
                                      ▼
                    ┌────────────────────────────────────────┐
                    │   refresh_unified_v2_timer (08:45)     │
                    │      → src/full_report_v2.py           │
                    │   • SP download                        │
                    │   • mirror to reporting-inputs blob ◄──┼──┐ self-healing
                    │   • run mapping + report generators    │  │ fallback path
                    │   • upload artefacts to               ─┼──┘ used if SP
                    │     reporting-outputs blob             │     is down on a
                    └─────────────────┬──────────────────────┘     future run
                                      │
                                      ▼
                    ┌────────────────────────────────────────┐
                    │      Azure Blob: reporting-outputs     │
                    │  qry_unified_mapped_*.csv              │
                    │  management_report_core_markets_*.html │
                    │  management_report_core_markets_*.pdf  │
                    │  management_report_usa_spa_*.html      │
                    │  combined_management_report_*.csv      │
                    │  *_summary.json                        │
                    └────────┬─────────────────┬─────────────┘
                             │                 │
                ┌────────────┘                 └─────────────┐
                ▼                                            ▼
   ┌─────────────────────────┐                  ┌──────────────────────────┐
   │  dispatch_reports 09:15 │                  │   FastAPI web app        │
   │  core_market_reports    │                  │   (auto-refresh loop)    │
   │  dispatch_usa_spa 16:00 │                  │   serves to browsers     │
   │                         │                  │                          │
   │  • download_outputs_    │                  │  • download_outputs_     │
   │    from_blob()          │                  │    from_blob()           │
   │  • build email body     │                  │  • render dashboard      │
   │  • send via Graph API   │                  │                          │
   └─────────────────────────┘                  └──────────────────────────┘
```

### Why this shape (Option B)

Earlier code had every consumer (web app + each dispatch function) call
`refresh_reports()` which spawned `full_report_v2.py` as a subprocess.
That caused:

* ~1 hour latency between the cron tick and the email actually leaving
  (the dispatch function was regenerating 4 reports from scratch first).
* Dispatch and web app could see *different* numbers because each
  regenerated independently.
* Source-of-truth ambiguity: `reporting-inputs` was checked **before**
  SharePoint, so a stale blob silently overruled SAP.

Under Option B:

* **One generator** (`refresh_unified_v2_timer`) writes to **one store**
  (`reporting-outputs`).
* **All consumers** read the same store — guaranteed identical numbers.
* **SharePoint stays authoritative** — the blob is a fallback that the
  generator self-populates on every successful run.

---

## 3. Schedule

All cron expressions are evaluated against the function app's
`WEBSITE_TIME_ZONE` (`Europe/Berlin`). NCRONTAB syntax has 6 fields
(`sec min hour dom month dow`).

| Function | App-Setting key | Recommended value | Local time |
|---|---|---|---|
| `refresh_unified_v2_timer` | `V2_UNIFIED_REFRESH_SCHEDULE` | `0 45 8 * * 1-5` | 08:45 Mon–Fri |
| `dispatch_reports` | `REPORT_DISPATCH_SCHEDULE` | `0 15 9 * * 1-5` | 09:15 Mon–Fri |
| `core_market_reports` | `CORE_MARKET_DISPATCH_SCHEDULE` | `0 15 9 * * 1-5` | 09:15 Mon–Fri |
| `dispatch_usa_spa_reports` | `USA_SPA_DISPATCH_SCHEDULE` | `0 0 16 * * 1-5` | 16:00 Mon–Fri |

The 30-minute gap between the timer finish (~08:45 + ≤15 min run) and
the dispatch trigger (09:15) is the safety margin. End-of-day USA Spa
runs at 16:00 reads whatever the morning timer produced — it does not
trigger a refresh.

To change a schedule:

```powershell
az functionapp config appsettings set `
  --name qms-dispatch-reports `
  --resource-group qms-dispatch-reports_group `
  --settings V2_UNIFIED_REFRESH_SCHEDULE="0 45 8 * * 1-5"
```

A function-app restart is required for schedule changes to take effect.

---

## 4. Function-by-Function Reference

### 4.1 `refresh_unified_v2_timer`

**File:** [azure_functions/refresh_unified_v2_timer/__init__.py](../azure_functions/refresh_unified_v2_timer/__init__.py)

* Timer-triggered. Cron from `V2_UNIFIED_REFRESH_SCHEDULE`.
* Defensive guard `_in_refresh_window`: refuses to run on weekends or
  outside 06:00–20:00 Berlin (overridable via
  `V2_UNIFIED_REFRESH_DISABLE_WINDOW=true`).
* Spawns `src/full_report_v2.py` as a subprocess with:
  * `--report-type` from `V2_UNIFIED_REFRESH_REPORT_TYPE` (default `MTD`)
  * `--output-tag` from `V2_UNIFIED_REFRESH_OUTPUT_TAG`
  * `--schema-mode` from `V2_UNIFIED_SCHEMA_MODE` (default `strict`)
  * `--dry-run` if `V2_UNIFIED_DRY_RUN=true`
* Inherits all process env, plus an explicit `REPORT_OUTPUT_DIR`
  override if `V2_UNIFIED_REFRESH_OUTPUTS_PATH` is set.
* Timeout: `V2_UNIFIED_REFRESH_TIMEOUT_SECONDS` (default 1800 s).

### 4.2 `dispatch_reports`

**File:** [azure_functions/dispatch_reports/__init__.py](../azure_functions/dispatch_reports/__init__.py)

* Timer-triggered. Cron from `REPORT_DISPATCH_SCHEDULE`.
* Hydrates `outputs_dir` via `download_outputs_from_blob(outputs_dir)`.
* Builds the management email body from
  `management_report_*.html` files found in that dir.
* Attaches CSVs matching `KEY_CSV_PATTERNS` (or
  `REPORT_DISPATCH_ATTACHMENT_PATTERNS`).
* Sends via Microsoft Graph
  (`graph_client.send_via_graph` → MSAL client-credential flow).
* Recipients: `REPORT_DISPATCH_RECIPIENTS` (override with
  `TEST_REPORT_DISPATCH_RECIPIENTS` for staging).
* Subject: `QMS Management Sales Report DD.MM.YYYY` (or `EOM …` when
  `V2_UNIFIED_REFRESH_REPORT_TYPE=EOM`).
* **Escape hatch:** `DISPATCH_REFRESH_BEFORE_SEND=true` re-enables the
  legacy in-line `refresh_reports()` call. Slow; use only for ad-hoc
  recovery when the timer has failed.

### 4.3 `core_market_reports`

**File:** [azure_functions/core_market_reports/__init__.py](../azure_functions/core_market_reports/__init__.py)

* Same shape as `dispatch_reports` but:
  * Recipients: `CORE_MARKET_DISPATCH_RECIPIENTS`
  * HTML body from `CORE_MARKET_HTML_PATTERNS` (default
    `management_report_core_markets_*.html`)
  * PDF attachment from `CORE_MARKET_PDF_PATTERNS` unless
    `CORE_MARKET_SEND_PDF=false`
  * Escape hatch: `CORE_MARKET_REFRESH_BEFORE_SEND=true`

### 4.4 `dispatch_usa_spa_reports`

**File:** [azure_functions/dispatch_usa_spa_reports/__init__.py](../azure_functions/dispatch_usa_spa_reports/__init__.py)

* Same shape, no PDF attachment (HTML-only inline body).
* Recipients: `USA_SPA_DISPATCH_RECIPIENTS`
* Escape hatch: `USA_SPA_REFRESH_BEFORE_SEND=true`

### 4.5 FastAPI web app

**File:** [fastapi_web_app/main.py](../fastapi_web_app/main.py)

* Hosted as a separate App Service (or container) — **not** an Azure
  Function.
* Hourly auto-refresh loop pulls the latest artefacts from
  `reporting-outputs` blob into `BLOB_CACHE_DIR` and renders dashboards.
* Same blob → same numbers as the email recipients.

---

## 5. `src/full_report_v2.py` — The Generator

**File:** [src/full_report_v2.py](../src/full_report_v2.py)

End-to-end pipeline:

1. **Resolve unified source CSV** (`resolve_unified_source_path`)
   * Tries SharePoint first
   * On SP success → uploads the file to `reporting-inputs/unified/<name>`
   * On SP failure (and not `V2_UNIFIED_REQUIRE_SHAREPOINT`) → falls
     back to `reporting-inputs/unified/<name>`
   * Last resort: local `data/inputs/<name>` (refused if
     `V2_UNIFIED_REQUIRE_SHAREPOINT=true`)
2. **Validate ingestion** (`run_ingestion_validations`)
3. **Resolve mapping CSVs** (`resolve_mapping_file`,
   `resolve_input_file`) — same SP-first / blob-fallback / local
   priority. Successful SP downloads are auto-mirrored to
   `reporting-inputs/mappings/...`.
4. **Apply mappings** (`apply_mappings`) — produces
   `qry_unified_mapped_*.csv`.
5. **Generate reports**:
   * `USASpaReportGenerator` → `management_report_usa_spa_*.html`
   * `CoreMarketReportGenerator` →
     `management_report_core_markets_*.html` + `.pdf`
   * `ManagementReportGenerator` → other receivables/management views
6. **Build combined CSV** (`build_combined_dataframe`) →
   `combined_management_report_*.csv`
7. **Write run summary** → `<run_id>_summary.json`
8. **Upload all artefacts to `reporting-outputs` blob**
   (`_upload_outputs_to_blob`).

### Self-healing fallback

The mirror-on-success pattern means the cold cache in `reporting-inputs`
is always within one successful run of SharePoint. Even if SP is down
for a full week, the pipeline still produces a report from the last good
extract — the only thing that becomes stale is the underlying data.

To force SP-only mode (no fallback): keep
`V2_UNIFIED_REQUIRE_SHAREPOINT=true` (already set in production).

---

## 6. App Settings Reference

### 6.1 Schedules
| Key | Value | Notes |
|---|---|---|
| `V2_UNIFIED_REFRESH_SCHEDULE` | `0 45 8 * * 1-5` | NCRONTAB |
| `REPORT_DISPATCH_SCHEDULE` | `0 15 9 * * 1-5` | |
| `CORE_MARKET_DISPATCH_SCHEDULE` | `0 15 9 * * 1-5` | |
| `USA_SPA_DISPATCH_SCHEDULE` | `0 0 16 * * 1-5` | |
| `WEBSITE_TIME_ZONE` | `W. Europe Standard Time` | enables Berlin-local cron |
| `V2_UNIFIED_REFRESH_TIMEZONE` | `Europe/Berlin` | for `_in_refresh_window` |

### 6.2 SharePoint
| Key | Description |
|---|---|
| `SHAREPOINT_SITE_URL` | `https://qmsmedicosmetics.sharepoint.com/sites/DATAANDREPORTING` |
| `SHAREPOINT_TENANT_ID` | Azure AD tenant GUID |
| `SHAREPOINT_CLIENT_ID` | App registration client ID |
| `SHAREPOINT_CLIENT_SECRET` | App registration secret |
| `SHAREPOINT_REQUEST_TIMEOUT_SECONDS` | default `30` |
| `V2_UNIFIED_REQUIRE_SHAREPOINT` | `true` in prod — refuses fallback when SP fails |

### 6.3 Blob storage
| Key | Description |
|---|---|
| `AZURE_STORAGE_REPORTING_CONNECTION_STRING` | Connection string for the storage account hosting both containers |
| `REPORTING_INPUTS_BLOB_CONTAINER` | default `reporting-inputs` |
| `REPORTING_OUTPUTS_BLOB_CONTAINER` | default `reporting-outputs` |

### 6.4 Generator behaviour
| Key | Default | Description |
|---|---|---|
| `V2_UNIFIED_REFRESH_REPORT_TYPE` | `MTD` | `MTD` or `EOM` |
| `V2_UNIFIED_REFRESH_OUTPUT_TAG` | `function-timer` | Stamped into output filenames |
| `V2_UNIFIED_SCHEMA_MODE` | `strict` | `strict` or `flexible` |
| `V2_UNIFIED_DRY_RUN` | `false` | If true, skips writing artefacts |
| `V2_UNIFIED_REFRESH_TIMEOUT_SECONDS` | `1800` | Subprocess timeout |
| `V2_UNIFIED_REFRESH_OUTPUTS_PATH` | _(empty)_ | Overrides `REPORT_OUTPUT_DIR` |
| `V2_UNIFIED_REFRESH_DISABLE_WINDOW` | `false` | Bypass weekday/hour guard |

### 6.5 Dispatch behaviour
| Key | Default | Description |
|---|---|---|
| `REPORT_DISPATCH_RECIPIENTS` | _(required)_ | Comma/semicolon list |
| `CORE_MARKET_DISPATCH_RECIPIENTS` | _(required)_ | |
| `USA_SPA_DISPATCH_RECIPIENTS` | _(required)_ | |
| `TEST_REPORT_DISPATCH_RECIPIENTS` | _(empty)_ | Test override |
| `TEST_CORE_MARKETS_RECIPIENTS` | _(empty)_ | Test override |
| `TEST_USA_SPA_RECIPIENTS` | _(empty)_ | Test override |
| `REPORT_DISPATCH_OUTPUTS_PATH` | _(empty)_ | Override outputs dir; falls back to `/tmp/outputs` on Consumption plan |
| `DISPATCH_REFRESH_BEFORE_SEND` | `false` | **Legacy escape hatch** — re-enable in-line regen |
| `CORE_MARKET_REFRESH_BEFORE_SEND` | `false` | Same escape hatch |
| `USA_SPA_REFRESH_BEFORE_SEND` | `false` | Same escape hatch |
| `CORE_MARKET_SEND_PDF` | `true` | Set `false` to skip PDF attachment |
| `REPORT_DISPATCH_BODY` | _(default copy)_ | Plain-text intro |
| `REPORT_DISPATCH_SUBJECT` | _(generated)_ | Override default subject |

### 6.6 Microsoft Graph (email send)
| Key | Description |
|---|---|
| `GRAPH_TENANT_ID` | Azure AD tenant for Graph |
| `GRAPH_CLIENT_ID` | App registration with `Mail.Send` |
| `GRAPH_CLIENT_SECRET` | Secret |
| `GRAPH_SENDER_UPN` | Mailbox to send `from` |

---

## 7. Failure Modes & Recovery

| Symptom | Likely cause | Recovery |
|---|---|---|
| Email arrives with yesterday's date | Timer didn't run today (e.g. function app stopped) | Restart function app, then trigger `refresh_unified_v2_timer` manually via Azure Portal |
| Email arrives with stale numbers | SP was down at timer time → blob fallback used | Verify SP, then trigger timer manually; the SP→blob mirror restores the cycle |
| Dispatch sent with no attachments | `download_outputs_from_blob` returned 0 (blob empty) and outputs dir was empty | Check `AZURE_STORAGE_REPORTING_CONNECTION_STRING` and that the timer succeeded earlier |
| 1-hour delay between cron tick and email | One of `*_REFRESH_BEFORE_SEND=true` is set somewhere | Unset the legacy flag in App Settings |
| `V2_UNIFIED_REQUIRE_SHAREPOINT` errors | SP creds rotated / expired | Update `SHAREPOINT_CLIENT_SECRET`, restart timer |
| Timer skips silently in logs | Outside `_in_refresh_window` (weekend/off-hours) | Expected — set `V2_UNIFIED_REFRESH_DISABLE_WINDOW=true` to bypass for ad-hoc runs |
| Dispatch can't write to outputs dir | Cold-start on Consumption plan; wwwroot is read-only | Already handled — `resolve_outputs_path` falls back to `/tmp/outputs` |

---

## 8. Manual Operations

### Trigger a generator run on demand

```powershell
# From Azure Portal → Function App → refresh_unified_v2_timer → "Code + Test" → Run
# Or via the master key:
$key = az functionapp keys list --name qms-dispatch-reports `
  --resource-group qms-dispatch-reports_group --query masterKey -o tsv
Invoke-RestMethod -Method POST `
  -Uri "https://qms-dispatch-reports.azurewebsites.net/admin/functions/refresh_unified_v2_timer?code=$key" `
  -Body '{}' -ContentType 'application/json'
```

### Trigger a dispatch on demand (uses current blob contents)

Same pattern, swap function name. Set
`TEST_REPORT_DISPATCH_RECIPIENTS=you@qmsmedicosmetics.com` first if you
don't want to email the whole list.

### Force a regenerate-then-send (legacy path)

```powershell
az functionapp config appsettings set `
  --name qms-dispatch-reports `
  --resource-group qms-dispatch-reports_group `
  --settings DISPATCH_REFRESH_BEFORE_SEND=true
# trigger dispatch_reports
# remember to reset:
az functionapp config appsettings set `
  --name qms-dispatch-reports `
  --resource-group qms-dispatch-reports_group `
  --settings DISPATCH_REFRESH_BEFORE_SEND=false
```

---

## 9. Change Log — Option B Migration

What this commit changed (vs. the previous main branch):

1. **`src/full_report_v2.py`**
   * Added `_mirror_to_blob_inputs(local_path, blob_name)` helper.
   * **Inverted priority** in `resolve_input_file`: SharePoint first, blob
     fallback, local last. Successful SP downloads now auto-mirror to
     `reporting-inputs`.
   * Added blob fallback + auto-mirror to `resolve_unified_source_path`
     (previously SP-or-local only).
2. **`azure_functions/dispatch_reports/report_collector.py`**
   * Added `download_outputs_from_blob(outputs_dir)` — pulls all blobs
     from `reporting-outputs` into the local outputs dir.
3. **`azure_functions/dispatch_reports/__init__.py`**
   * `refresh_reports(outputs_dir)` call replaced with
     `download_outputs_from_blob(outputs_dir)`. Preserved behind
     `DISPATCH_REFRESH_BEFORE_SEND=true` for emergencies.
   * **Eliminates the ~1-hour email delay.**
4. **`azure_functions/core_market_reports/__init__.py`**
   * Same replacement; flag is `CORE_MARKET_REFRESH_BEFORE_SEND`.
5. **`azure_functions/dispatch_usa_spa_reports/__init__.py`**
   * Same replacement; flag is `USA_SPA_REFRESH_BEFORE_SEND`.
6. **`azure_functions/refresh_unified_v2_timer/__init__.py`**
   * Removed the hard-coded `minute == 15` constraint in
     `_in_refresh_window` (was incompatible with any other schedule).
   * Widened window to 06:00–20:00 Berlin Mon–Fri.
   * Added `V2_UNIFIED_REFRESH_DISABLE_WINDOW` bypass.

### Required App-Setting updates after deploy

```powershell
az functionapp config appsettings set `
  --name qms-dispatch-reports `
  --resource-group qms-dispatch-reports_group `
  --settings `
    V2_UNIFIED_REFRESH_SCHEDULE="0 45 8 * * 1-5" `
    REPORT_DISPATCH_SCHEDULE="0 15 9 * * 1-5" `
    CORE_MARKET_DISPATCH_SCHEDULE="0 15 9 * * 1-5" `
    USA_SPA_DISPATCH_SCHEDULE="0 0 16 * * 1-5" `
    DISPATCH_REFRESH_BEFORE_SEND="false" `
    CORE_MARKET_REFRESH_BEFORE_SEND="false" `
    USA_SPA_REFRESH_BEFORE_SEND="false"
# Then restart so the new cron schedules take effect:
az functionapp restart `
  --name qms-dispatch-reports `
  --resource-group qms-dispatch-reports_group
```

### Expected daily timeline after deploy

| Time (Berlin) | What happens |
|---|---|
| 08:45 | `refresh_unified_v2_timer` fires → downloads SAP extract → mirrors to `reporting-inputs` blob → generates all artefacts → uploads to `reporting-outputs` blob (typical run: 5–15 min) |
| 09:15 | `dispatch_reports` and `core_market_reports` fire → each hydrates from `reporting-outputs` blob (seconds) → sends email immediately |
| 16:00 | `dispatch_usa_spa_reports` fires → reads same morning artefacts from `reporting-outputs` → sends USA Spa email |
| any time | FastAPI web app's hourly auto-refresh pulls the same blobs |
