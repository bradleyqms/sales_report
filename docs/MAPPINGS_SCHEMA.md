# Mappings Schema

> Machine-checkable schema for the two mapping files that drive segmentation
> in the report. If you propose changes to either file, validate against
> this schema first.

Both files live in:
- SharePoint: `/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/`
- Blob: `reporting-inputs` container, `mappings/` prefix
- Local: `data/inputs/mappings/`

The function app `mapping_inputs_sync` timer keeps the blob copy in lock-step
with SharePoint.

---

## 1. `entity_mappings.csv`

The flat-file lookup that resolves an SAP `Sales Employee` to its dimensional
attributes. Currently 376 rows.

### Columns (required unless noted)

| Column | Type | Allowed values | Notes |
|---|---|---|---|
| `Sales_Employee_Cleaned` | string | non-empty, unique | Primary key. Trim whitespace, preserve case. |
| `Account` | string | non-empty | SAP customer label. |
| `Region` | string | one of: `Core Markets`, `Export`, `UK`, `US`, `Ecommerce`, `Interco` | Top-level segment. |
| `Sub Region` | string | see "Sub Region values" below | May be blank only when `Region=Interco`. |
| `Country` | string | ISO country name | Optional but strongly recommended. |
| `Customer_Group` | string | free text | Optional. |
| `Notes` | string | free text | Optional. |

### Sub Region values (Core Markets segment)
`Bayern`, `Bayern - Other`, `Berlin`, `BeNeLux`, `France`, `Italy`, `Nord`,
`Nordrhein-Westfalen`, `Süd`, `Switzerland`, `West`.

### Validation rules
1. `Sales_Employee_Cleaned` is unique across the file.
2. If `Region` is `Core Markets`, `Sub Region` must be one of the values
   listed above and must not be blank.
3. No row should have all-blank attributes other than the key.
4. `Region=Interco` rows are excluded from segment KPIs by the rules engine.

### JSON Schema (subset)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "entity_mappings.csv row",
  "type": "object",
  "required": ["Sales_Employee_Cleaned", "Account", "Region"],
  "properties": {
    "Sales_Employee_Cleaned": { "type": "string", "minLength": 1 },
    "Account": { "type": "string", "minLength": 1 },
    "Region": {
      "type": "string",
      "enum": ["Core Markets", "Export", "UK", "US", "Ecommerce", "Interco"]
    },
    "Sub Region": { "type": ["string", "null"] },
    "Country": { "type": ["string", "null"] }
  },
  "allOf": [
    {
      "if": { "properties": { "Region": { "const": "Core Markets" } } },
      "then": { "required": ["Sub Region"], "properties": { "Sub Region": { "minLength": 1 } } }
    }
  ]
}
```

---

## 2. `py25_regional_mappings.csv`

Backfill mapping used to assign Sub Region to **prior-year** rows whose
`Sub_Region_Cleaned` is blank. Required because SAP did not record Sub Region
consistently in PY25.

### Columns

| Column | Type | Required | Notes |
|---|---|---|---|
| `Sales_Employee_Cleaned` | string | ✅ | Primary key. Must match values used in PY25 fact rows. |
| `Sub Region` | string | ✅ | Must match the Core Markets Sub Region enum above. |

### Known critical rows (do not delete)
| Sales_Employee_Cleaned | Sub Region |
|---|---|
| `Iannis` | `Bayern` |
| (representatives without a fixed home region) | `Bayern - Other` |

> Removing or blanking the `Iannis,Bayern` row is the documented cause of the
> "Bayern blank on web app" incident — see [RUNBOOKS/bayern_blank.md](./RUNBOOKS/bayern_blank.md).

---

## 3. Lifecycle

```
SharePoint (human edits)
   │
   ▼  mapping_inputs_sync timer (hourly)
Blob: reporting-inputs/mappings/
   │
   ├──▶ Function dispatchers  (read blob)
   ├──▶ Web app subprocess    (read blob, MAPPING_SYNC_SOURCE_OF_TRUTH=blob)
   └──▶ Developer fallback    (read local copy in data/inputs/mappings)
```

Editing rules:
1. **Always edit in SharePoint first.** The timer will mirror to blob within an hour.
2. To force-publish immediately, run the `mapping_inputs_sync` function manually from the Azure portal.
3. Verify the change reached production with `GET /healthz/mappings` on the web app.
