# AGENTS.md — Orientation for AI Coding Agents

> Read this file first before changing anything in this repo. It is short
> on purpose. Each section points to the canonical document.

## What this repo is

Production sales reporting stack (Python / FastAPI / Azure Functions). It
pulls SAP extracts and mapping files from SharePoint, generates HTML/PDF/CSV
reports, stores them in Azure Blob, and dispatches them via email and a web UI.

## The 30-second mental model

```
SharePoint → reporting-inputs blob → full_report_v2.py → reporting-outputs blob → web UI + dispatchers
```

Full diagram and component table: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Where to look for what

| If you need to... | Read |
|---|---|
| Understand the deployed components and data flow | [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) |
| Know which env var is required on which host | [docs/ENV_MATRIX.md](docs/ENV_MATRIX.md) (human) + [docs/env_matrix.yaml](docs/env_matrix.yaml) (machine) |
| Validate a mapping CSV change | [docs/MAPPINGS_SCHEMA.md](docs/MAPPINGS_SCHEMA.md) |
| Diagnose a "blank region / blank KPI" incident | [docs/RUNBOOKS/bayern_blank.md](docs/RUNBOOKS/bayern_blank.md) |
| Run an end-of-month dispatch | [docs/EOM_MONTHLY_RUNBOOK.md](docs/EOM_MONTHLY_RUNBOOK.md) |
| Trigger a manual web dispatch | [docs/GITHUB_WEB_DISPATCH_OPERATIONS.md](docs/GITHUB_WEB_DISPATCH_OPERATIONS.md) |
| Understand the data-access pattern (SP → blob → local) | [DATA_ACCESS_PATTERN.md](DATA_ACCESS_PATTERN.md) |

## Hard rules for agents

1. **SharePoint is the only source of truth for mapping CSVs.** Do not edit
   the blob copy or the local copy directly to "fix" production data. Edit
   in SharePoint, then trigger `mapping_inputs_sync` to publish.
2. **Never silently fall back.** Use the `ALLOW_LOCAL_MAPPING_FALLBACK` env
   var; never re-introduce silent local fallbacks in code.
3. **Log unmapped data, never `dropna()` it away.** Any time you reduce a
   DataFrame, log the dropped rows so the next incident is self-diagnosing.
4. **Update both `ENV_MATRIX.md` and `env_matrix.yaml`** when adding/removing
   environment variables. They are checked against each other in CI.
5. **Use `/healthz/mappings`** before and after any change touching the
   mapping pipeline. It is the canonical "did my fix actually take effect?"
   probe.
6. **Do not put secrets in this repo.** All connection strings live in App
   Service / Function App configuration.
7. **Time zone is Europe/Berlin everywhere.** The scheduling code assumes it.

## Changing the contract (mappings, env vars, blob layout)

Any change that affects the contract between layers must update:
- the relevant doc in `docs/`
- the `env_matrix.yaml` (if env vars touched)
- the runbook for the most-likely-affected incident
- the dispatch readiness check in `check_dispatch_readiness.py`

If you change a public web route (`/status`, `/metrics`, `/healthz/*`), also
update the auth allow-list at the top of `fastapi_web_app/main.py`.

## Quick verification after any change

```powershell
# Web app health
curl https://qms-sales-report.azurewebsites.net/healthz/mappings

# Pre-flight before email send
python check_dispatch_readiness.py
```
