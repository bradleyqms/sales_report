# Runbook — "Bayern shows blank on the web app"

> Symptom: Core Markets table on `https://qms-sales-report.azurewebsites.net/`
> shows a blank or zero row for `Bayern` while other sub-regions render
> normally; the latest scheduled report run via the function app **does**
> show Bayern correctly.

## TL;DR — first action

1. Hit `https://qms-sales-report.azurewebsites.net/healthz/mappings`
2. Inspect the JSON. If `blob_configured` is `false` or any file's
   `effective_source` is `local` or `missing`, **the web app is not reading
   the blob copy of the mappings**. Jump to "Fix A" below.
3. If `blob_configured` is `true` and `effective_source` is `blob`, jump to
   "Fix B".

---

## Decision tree

```
            ┌─────────────────────────────────────┐
            │ Bayern blank on web app /          │
            │ /coremarkets ?                     │
            └──────────────────┬──────────────────┘
                               │
                  GET /healthz/mappings
                               │
        ┌──────────────────────┼─────────────────────────┐
        │                      │                         │
 blob_configured=false   effective_source=blob       blob row_count
        │                  but Bayern still blank   differs from local
        ▼                      │                         ▼
   FIX A                       ▼                    FIX C
   set 3 web app           FIX B                    SharePoint copy
   appsettings             check py25              is stale — run
   + restart               mapping CSV +            mapping_inputs_sync
                          dropna guard              manually
```

---

## Fix A — Web app missing storage credentials

The web app's report subprocess inherits its parent process env. If the parent
is missing `AZURE_STORAGE_REPORTING_CONNECTION_STRING`, the subprocess
silently falls back to the bundled local copy of `py25_regional_mappings.csv`,
which is usually stale.

```powershell
$conn = (az functionapp config appsettings list `
  --name qms-dispatch-reports --resource-group qms-dispatch-reports_group `
  --query "[?name=='AZURE_STORAGE_REPORTING_CONNECTION_STRING'].value" -o tsv)

az webapp config appsettings set `
  --name qms-sales-report --resource-group DefaultResourceGroup-DEWC `
  --settings `
    AZURE_STORAGE_REPORTING_CONNECTION_STRING="$conn" `
    REPORTING_INPUTS_BLOB_CONTAINER=reporting-inputs `
    MAPPING_SYNC_SOURCE_OF_TRUTH=blob `
    ALLOW_LOCAL_MAPPING_FALLBACK=0
az webapp restart --name qms-sales-report --resource-group DefaultResourceGroup-DEWC
```

Verify:
```powershell
curl https://qms-sales-report.azurewebsites.net/healthz/mappings
```
You should see `"effective_source": "blob"` for both files and a non-zero `row_count`.

Then click **Run Report** on the web UI (or wait for the next auto-refresh).
Confirm Bayern populates in `/coremarkets`.

---

## Fix B — Mapping is reaching the web app, but Bayern row is missing in the source

1. Open `py25_regional_mappings.csv` in SharePoint.
2. Confirm the row `Iannis,Bayern` exists. (See
   [MAPPINGS_SCHEMA.md](../MAPPINGS_SCHEMA.md#known-critical-rows-do-not-delete).)
3. If absent, add it. Do **not** edit the blob copy directly — the SP→blob
   sync will overwrite your edit on the next tick.
4. Trigger an immediate sync:
   - Azure Portal → `qms-dispatch-reports` → Functions → `mapping_inputs_sync` → "Code + Test" → Test/Run.
5. Re-run report from the web UI.

If `Iannis,Bayern` is present but Bayern is still blank, check the function
app logs for the warning:

```
core_market_report: N prior_df rows remain UNMAPPED to Sub_Region after entity_mappings fallback. Unmapped employees ...
```

The list of employees in that warning identifies which mapping rows are
actually missing.

---

## Fix C — SharePoint is stale relative to blob (or vice-versa)

Compare:
```powershell
curl -s https://qms-sales-report.azurewebsites.net/healthz/mappings | jq '.files'
```

If the blob `row_count` is materially different from the SharePoint copy,
either the timer hasn't run or someone bypassed it.

1. Manually re-run `mapping_inputs_sync`.
2. If still divergent, inspect the function logs for upload errors.
3. Last resort — overwrite the blob from a known-good local file via Kudu
   (`fastapi_web_app/data/inputs/mappings/`) but immediately re-sync from
   SharePoint afterwards so SharePoint remains the source of truth.

---

## Permanent guard rails (already in place)

- `core_market_report.py` now logs every prior-year employee that ends up
  unmapped after the entity-mappings fallback.
- `full_report_v2.resolve_input_file` refuses to use the local fallback when
  `ALLOW_LOCAL_MAPPING_FALLBACK=0` and a blob path was requested.
- `/healthz/mappings` exposes `effective_source`, `etag`, `last_modified`,
  and `row_count` for every mapping file.

If this incident recurs after all three guard rails are active, update this
runbook with the new failure mode.
