# Atlas ETL Suite — Pipeline B

SQL Server stored procedures + Python orchestration replacing 13 legacy SSIS packages for BILH's Atlas data governance platform.

## Architecture

Pipeline B eliminates all linked server dependencies. Python scripts extract data via pyodbc directly from Epic Clarity and Caboodle, then stored procedures handle staging transforms, merge, and post-processing locally.

```
Source Systems                    Atlas Server (10.247.4.56\SQL126)
─────────────                    ─────────────────────────────────
Epic Clarity  ──pyodbc──►  raw_v2.* ──► stage_v2.* ──► prd_v2.*
Epic Caboodle ──pyodbc──►     ▲              ▲              ▲
Power BI API  ──REST───►      │              │              │
CSV Share     ──pandas─►   Python         SQL SPs        SQL SPs
                          extractors     (staging)    (merge + post)
```

## Prerequisites

| Requirement | Details |
|-------------|---------|
| SQL Server | Atlas_Staging + Atlas_Prd databases on 10.247.4.56\SQL126 |
| Python | 3.11+ with pyodbc, pandas, requests |
| Network | TCP 1433 from Atlas server to EPICCLAPRD and EPICCDWPRD |
| Credentials | SVC_REPORTHUB_CLARITY (SQL Auth), SVC_REPORTHUB_CABOODLE (Windows Auth) |
| Azure AD | App registration for Power BI Activity Events API |

## Fresh Install

Run all SQL files in numeric order on Atlas_Staging:

```
01_create_etl_logging.sql        — etl schema + log table + 4 logging SPs
02_usp_Atlas_Setup.sql           — raw_v2/stage_v2 schemas + Setup SP
03_create_prd_v2_schema.sql      — prd_v2 schema + config tables
04_fix_DoNotOrphanTypes.sql      — corrects DoNotOrphanTypes column types
05_clarity_ddl.sql               — 19 raw_v2 + 3 stage_v2 Clarity tables
06_rundata_ddl.sql               — 8 raw_v2 RunData/PBI tables
10_usp_Atlas_LDAP.sql            — SP: Azure AD / Clarity user mapping
11_usp_Atlas_DatabaseObjects.sql — SP: database object staging
12_usp_Atlas_Clarity.sql         — SP: Clarity staging transforms
13_usp_Atlas_RunData.sql         — SP: RunData indexing/staging/merge
14_usp_Atlas_Merge.sql           — SP: 20 merge operations
15_usp_Atlas_PostProcessing.sql  — SP: orphan flags, URL overrides, visibility
20_usp_Atlas_Tableau.sql         — SP: optional (disabled by default)
21_usp_Atlas_SSRS.sql            — SP: optional (disabled by default)
30_usp_Atlas_ValidateAzureADMapping.sql — diagnostic
31_clarity_validation.sql        — validation queries
```

Then configure environment and run the pipeline:

```bash
# Set credentials (or configure in atlas_config.py)
export ATLAS_ENV=PROD
export PBI_TENANT_ID=your-tenant-id
export PBI_CLIENT_ID=your-client-id
export PBI_CLIENT_SECRET=your-secret

# Test connections
python atlas_orchestrator.py --test

# Run full pipeline
python atlas_orchestrator.py
```

## Pipeline Execution Order

The orchestrator runs these steps in sequence (fail-fast on any error):

| Phase | Step | Type |
|-------|------|------|
| Setup | usp_Atlas_Setup | SP |
| Extraction | atlas_clarity_extractor.py | Python |
| | usp_Atlas_Clarity | SP |
| | atlas_csv_loader.py | Python |
| | usp_Atlas_LDAP | SP |
| | atlas_db_objects_extractor.py | Python |
| | usp_Atlas_DatabaseObjects | SP |
| RunData | atlas_query_hierarchy.py | Python |
| | atlas_pbi_events.py | Python |
| | atlas_rundata_extractor.py | Python |
| | usp_Atlas_RunData | SP |
| Merge | usp_Atlas_Merge | SP |
| Post | usp_Atlas_PostProcessing | SP |

## File Inventory

**SQL (16 files)** — numbered by execution band with reserved gaps for future additions.

**Python (8 files):**

| File | Purpose |
|------|---------|
| `atlas_config.py` | Centralized configuration (v3.0) |
| `atlas_orchestrator.py` | Pipeline orchestrator (v4.1) |
| `atlas_clarity_extractor.py` | 8 Clarity extractions via pyodbc |
| `atlas_csv_loader.py` | 8 CSV flat-file loads |
| `atlas_db_objects_extractor.py` | sys.all_objects from 4 databases |
| `atlas_rundata_extractor.py` | Clarity + SlicerDicer run data |
| `atlas_pbi_events.py` | Power BI Activity Events API (v4.2) |
| `atlas_query_hierarchy.py` | SQL parsing for table lineage |

## Configuration

`atlas_config.py` uses Python dataclasses with environment variable overrides. Key settings:

```python
epic_clarity_server    = EPICCLAPRD.BILH.ITSYSTEMS.ORG       # env: EPIC_CLARITY_SERVER
epic_caboodle_server   = EPICCDWPRD.BILH.ITSYSTEMS.ORG\...   # env: EPIC_CABOODLE_SERVER
atlas_staging_server   = 10.247.4.56\SQL126                   # env: ATLAS_STAGING_SERVER
enable_ssrs            = False                                 # optional components
enable_ssas            = False                                 # disabled by default
enable_tableau         = False
enable_power_bi        = True
```

## Monitoring

All pipeline steps log to `etl.Atlas_ETL_Log` with a shared `ExecutionID` per run:

```sql
-- Today's pipeline runs
SELECT PackageName, StepName, DurationSeconds, RowsAffected, Status, ErrorMessage
FROM etl.Atlas_ETL_Log
WHERE CAST(StartTime AS DATE) = CAST(GETDATE() AS DATE)
ORDER BY StartTime;

-- Failed steps
SELECT * FROM etl.Atlas_ETL_Log WHERE Status = 'Failure' ORDER BY StartTime DESC;
```

## Documentation

| Document | Description |
|----------|-------------|
| `Atlas_ETL_Migration_Plan_v2.docx` | Base migration plan (February 2026) |
| `Atlas_Migration_Amendment_A.docx` | Pipeline B architecture decision (v2 — updated March 2026) |
| `Atlas_Migration_Plan_v2_Errata.docx` | Corrections to base plan reflecting final implementation |

## Author

Larry Duren — Senior Epic Analytics / Cogito Specialist
