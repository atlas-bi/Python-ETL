# Atlas ETL Migration — Project Conventions

> **Purpose:** This file captures the architectural patterns, coding standards, and design
> decisions established during the Atlas ETL migration (SSIS → SQL Server + Python).
> Any AI coding assistant or developer working in this repo should follow these conventions
> to maintain cohesion across all build phases.
>
> **Last Updated:** March 2026 | **Author:** Larry Duren

---

## 1. Project Overview

This project migrates 13 SSIS packages into 7 stored procedures + 3 Python scripts,
all orchestrated by a single SQL Server Agent job. The migration uses a **v2 schema
isolation strategy** — all new objects live in dedicated schemas (`etl`, `raw_v2`,
`stage_v2`, `prd_v2`) so the live SSIS process (`dbo.*`, `raw.*`, `Atlas_Prd`) is
never touched during development.

### Scope Decisions

- **Tableau extraction:** EXCLUDED by customer request
- **SSRS extraction:** EXCLUDED by customer request
- **SSAS (SlicerDicer):** EXCLUDED by customer request
- **Feature flags** in `atlas_config.py` control optional components (`enable_ssrs`,
  `enable_ssas`, `enable_tableau`, `enable_pbi_extraction`, `enable_query_hierarchy`)

### Key Infrastructure

| Component | Location |
|-----------|----------|
| Staging DB | `10.247.4.56\SQL126` → `Atlas_Staging` |
| Production DB | `Atlas_Prd` |
| Clarity linked server | `[EPICCLAPRD]` → `EPICCLAPRD.BILH.ITSYSTEMS.ORG` |
| CSV network share | `\\10.209.10.225\Shared\PRD\Cogito_Reporting\SSIS_Packages\Atlas\Files\` |
| AD domains | BILH, ITS, MR1 |

---

## 2. Schema Architecture

```
raw_v2.*          ← Landing zone: minimal transformation, direct from source
stage_v2.*        ← Transformed/denormalized: ready for merge into production
prd_v2.*          ← Production tables: final merge targets
etl.*             ← Infrastructure: logging table, logging SPs, ETL procedures
```

**CRITICAL:** Never write to `dbo.*`, `raw.*`, `stage.*`, or `Atlas_Prd`. These belong
to the live SSIS process.

---

## 3. SQL Stored Procedure Conventions

### 3.1 File Naming

```
NN_descriptor.sql

Examples:
  01_week3_clarity_ddl.sql       ← DDL scripts (CREATE TABLE)
  02_usp_Atlas_Clarity.sql       ← Stored procedure definitions
  05_week3_validation.sql        ← Post-deployment validation queries
```

Files are numbered to enforce execution order within each week.

### 3.2 Script Header Template

Every SQL file begins with this header block:

```sql
/*******************************************************************************
 * Atlas ETL Migration — Week N: <descriptive name>
 *
 * Replaces: <SSIS package name> (<component count summary>)
 *
 * <2-3 sentence description of what this script does>
 *
 * Dependencies:
 *   - <schemas, linked servers, prior scripts>
 *   - Logging: etl.usp_Atlas_LogStart, etl.usp_Atlas_LogEnd (Week 1)
 *
 * Execution:
 *   EXEC etl.<proc_name>;
 *   EXEC etl.<proc_name> @Debug = 1;
 *
 * Author:  Larry Duren
 * Date:    <month> 2026
 * Version: <major.minor> (Week N)
 ******************************************************************************/

USE Atlas_Staging;
GO
```

### 3.3 Procedure Signature Patterns

There are two signature patterns depending on the procedure's role:

**Pattern A — Orchestrator-called procedures (Week 1 foundation):**

```sql
CREATE PROCEDURE etl.usp_Atlas_<Name>
    @ExecutionID        UNIQUEIDENTIFIER = NULL,
    @RaiseErrorOnFail   BIT              = 1
AS
```

Used by: `usp_Atlas_Setup`, `usp_Atlas_LDAP`, `usp_Atlas_PostProcessing`

**Pattern B — Complex extraction/merge procedures (Weeks 2–4+):**

```sql
CREATE PROCEDURE etl.usp_Atlas_<Name>
    @ExecutionID        UNIQUEIDENTIFIER = NULL,
    @DG_DB              NVARCHAR(128)    = 'Atlas_Prd',
    @DG_STAGE_DB        NVARCHAR(128)    = 'Atlas_Staging',
    @ORG_AD_NAME        NVARCHAR(100)    = 'MR1',
    -- ... procedure-specific parameters with defaults ...
    @ExtractOnly        BIT              = 0,
    @Debug              BIT              = 0,
    @RaiseErrorOnFail   BIT              = 1
AS
```

Used by: `usp_Atlas_Clarity`, `usp_Atlas_RunData`, `usp_Atlas_Merge`,
`usp_Atlas_DatabaseObjects`

**Rules:**
- `@ExecutionID` is always first, always optional (auto-generates if NULL)
- `@Debug` enables diagnostic PRINT output without modifying data
- `@RaiseErrorOnFail` controls whether errors propagate to the orchestrator
- All parameters have sensible defaults so procedures can run standalone

### 3.4 Required SET Options

Every procedure body begins with:

```sql
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
```

`SET XACT_ABORT ON` ensures complete transaction rollback on errors, preventing
partially-committed states. This is mandatory for all ETL procedures.

### 3.5 Variable Declaration Block

Standard variable block immediately after SET options:

```sql
    DECLARE @ProcName       NVARCHAR(100) = N'usp_Atlas_<Name>';
    DECLARE @StepName       NVARCHAR(200);
    DECLARE @LogID          INT;           -- or BIGINT for Merge/RunData
    DECLARE @RowCount       INT;
    DECLARE @StartTime      DATETIME = GETDATE();
    DECLARE @StepStart      DATETIME;
    DECLARE @ErrorMessage   NVARCHAR(4000);
    DECLARE @ErrorSeverity  INT;
    DECLARE @ErrorState     INT;
```

### 3.6 Logging Framework Integration

Every procedure uses the Week 1 logging infrastructure. This is non-negotiable.

**Procedure start:**
```sql
    BEGIN TRY
        EXEC etl.usp_Atlas_LogStart
            @PackageName = @ProcName,
            @StepName    = N'Procedure Start',
            @LogID       = @LogID OUTPUT;
```

**After each step:**
```sql
        SET @StepName = N'Step N: <description>';
        SET @StepStart = GETDATE();

        -- ... actual work ...

        SET @RowCount = @@ROWCOUNT;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @StepName     = @StepName,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows ('
                  + CAST(DATEDIFF(SECOND, @StepStart, GETDATE()) AS VARCHAR(10)) + 's)';
```

**CATCH block (identical across all procedures):**
```sql
    END TRY
    BEGIN CATCH
        SET @ErrorMessage  = ERROR_MESSAGE();
        SET @ErrorSeverity = ERROR_SEVERITY();
        SET @ErrorState    = ERROR_STATE();

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @StepName     = @StepName,
            @RowsAffected = 0,
            @Status       = N'Failure',
            @ErrorMessage = @ErrorMessage;

        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
        RETURN 1;
    END CATCH;
END;
GO

PRINT 'Created etl.usp_Atlas_<Name>';
GO
```

### 3.7 Debug Mode Pattern

When `@Debug = 1`, procedures print diagnostics instead of modifying data:

```sql
        IF @Debug = 0
        BEGIN
            TRUNCATE TABLE raw_v2.<TableName>;

            INSERT INTO raw_v2.<TableName> (...)
            SELECT ... FROM [EPICCLAPRD].Clarity.dbo.<Source> WITH (NOLOCK);

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            SELECT @RowCount = COUNT(*)
            FROM [EPICCLAPRD].Clarity.dbo.<Source> WITH (NOLOCK);
        END;
```

### 3.8 Linked Server Queries

All Clarity queries use the four-part name format with `NOLOCK`:

```sql
FROM [EPICCLAPRD].Clarity.dbo.<TableName> WITH (NOLOCK)
```

Never hardcode server names outside the linked server alias.

### 3.9 DDL Pattern (CREATE TABLE scripts)

```sql
IF OBJECT_ID('<schema>.<TableName>', 'U') IS NOT NULL
    DROP TABLE <schema>.<TableName>;
GO

CREATE TABLE <schema>.<TableName> (
    <columns>
);
GO
```

Always DROP IF EXISTS before CREATE — no ALTER TABLE migrations.

### 3.10 Procedure Drop/Create Pattern

```sql
IF OBJECT_ID('etl.usp_Atlas_<Name>', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_<Name>;
GO

CREATE PROCEDURE etl.usp_Atlas_<Name>
```

---

## 4. Python Script Conventions

### 4.1 Required Packages

```
pyodbc    — SQL Server connectivity
pandas    — CSV processing, data manipulation
```

### 4.2 Module Structure

```python
"""
Atlas ETL Migration — Week N: <script_name>.py

Replaces: <SSIS component description>

<2-3 sentence description>

Dependencies:
    - Python 3.x with pyodbc, pandas
    - atlas_config.py for connection strings
    - <database prerequisites>

Usage:
    python <script_name>.py                    # Full execution
    python <script_name>.py --dry-run          # Validate only
    python <script_name>.py --verbose          # Extra logging

Author:  Larry Duren
Date:    <month> 2026
Version: <major.minor> (Week N)
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

try:
    import pyodbc
    import pandas as pd
except ImportError as e:
    print(f"ERROR: Required package not installed: {e}")
    print("Install with: pip install pyodbc pandas")
    sys.exit(1)
```

### 4.3 Configuration Access

All Python scripts import from `atlas_config.py`:

```python
from atlas_config import config

conn_str = config.get_atlas_staging_connection_string()
```

Never hardcode server names, database names, or file paths in scripts.

### 4.4 Logging to Atlas_ETL_Log

Python scripts log to the same `etl.Atlas_ETL_Log` table as stored procedures,
using the same logging SPs:

```python
def log_start(conn, exec_id, package_name, step_name, step_seq):
    cursor = conn.cursor()
    cursor.execute("""
        EXEC etl.usp_Atlas_LogStart
            @PackageName = ?,
            @StepName = ?,
            @LogID = ? OUTPUT
    """, package_name, step_name)
    # ... return LogID

def log_end(conn, log_id, rows, status='Success'):
    cursor = conn.cursor()
    cursor.execute("""
        EXEC etl.usp_Atlas_LogEnd
            @LogID = ?,
            @StepName = ?,
            @RowsAffected = ?,
            @Status = ?
    """, log_id, rows, status)
    conn.commit()
```

### 4.5 Bulk Insert Pattern (CSV Loading)

```python
# Use fast_executemany for performance
cursor.fast_executemany = True
cursor.executemany(insert_sql, rows)
conn.commit()
```

### 4.6 Orchestrator Pattern

The `atlas_orchestrator.py` calls stored procedures via:

```python
def execute_sp(proc_name, exec_id, params=None):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"SET LOCK_TIMEOUT {config.query_timeout_seconds * 1000}")
    # Build EXEC statement with parameters
    # ...
```

Python sub-scripts are invoked via `subprocess`:
```python
subprocess.run([sys.executable, script_path, '--arg', value], check=True)
```

---

## 5. Validation Script Conventions

Every week includes a validation script (`NN_weekN_validation.sql`) that runs
after deployment and verifies:

1. **Row count parity** — v2 tables vs existing production (target: within 0.1%)
2. **Raw extraction completeness** — all source tables loaded
3. **Staging transform integrity** — denormalized fields populated
4. **Sample field-level comparison** — spot-check 1000 rows per table

```sql
PRINT '================================================================';
PRINT 'Atlas ETL Migration — Week N Validation';
PRINT 'Run Time: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
PRINT '================================================================';
```

---

## 6. Build Phase Reference

| Week | Status | Deliverables |
|------|--------|-------------|
| 1 | COMPLETE | 4 schemas, 7 procedures (Setup, LDAP, PostProcessing, 4 Log SPs), 15 tables |
| 2 | COMPLETE | `usp_Atlas_DatabaseObjects`, `atlas_config.py`, `atlas_orchestrator.py`, `atlas_query_hierarchy.py` |
| 3 | COMPLETE | `usp_Atlas_Clarity` (8 extractions), `atlas_csv_loader.py` (8 CSVs), `usp_Atlas_ValidateAzureADMapping`, DDL (19 tables) |
| 4 | COMPLETE | `usp_Atlas_RunData`, `usp_Atlas_Merge` (20 tasks), `atlas_pbi_events.py`, DDL + validation |
| 5 | PLANNED | `atlas_orchestrator.py` final wiring, SQL Server Agent job, integration testing |
| 6 | PLANNED | Parallel validation, cutover planning, documentation |

### IT Dependencies (blocking execution testing)

1. **Python 3.11+** needs to be installed on Atlas server or dev workstation (pyodbc, pandas)
2. **Linked Server** from Atlas_Staging to `EPICCLAPRD.BILH.ITSYSTEMS.ORG` must be created

---

## 7. Cross-Reference Rules

When building new procedures or modifying existing ones:

1. **Always check existing table names** — reference the DDL scripts to ensure you're
   using the correct `raw_v2.*` and `stage_v2.*` table names
2. **Always use the established logging SPs** — never write custom logging;
   use `etl.usp_Atlas_LogStart` / `etl.usp_Atlas_LogEnd` / `etl.usp_Atlas_LogError`
3. **Match existing column names exactly** — the SSIS analysis documents define the
   source-to-target column mappings; don't invent new column names
4. **Preserve SSIS parameter mappings** — each procedure header documents which
   SSIS parameters map to which SP parameters (e.g., `@DG_DB` ← `Data_Governance_InitialCatalog`)
5. **Test with @Debug = 1 first** — all procedures support debug mode for dry-run validation

---

## 8. SSIS Package → Procedure Mapping

| SSIS Package | Migration Target | Type |
|-------------|-----------------|------|
| ETL-Setup | `etl.usp_Atlas_Setup` | Stored Procedure |
| ETL-LDAP | `etl.usp_Atlas_LDAP` | Stored Procedure |
| ETL-PostProcessing | `etl.usp_Atlas_PostProcessing` | Stored Procedure |
| ETL-Clarity | `etl.usp_Atlas_Clarity` + `atlas_csv_loader.py` | SP + Python |
| ETL-DatabaseObjects | `etl.usp_Atlas_DatabaseObjects` + `atlas_query_hierarchy.py` | SP + Python |
| ETL-RunData | `etl.usp_Atlas_RunData` + `atlas_pbi_events.py` | SP + Python |
| ETL-QueryHierarchy | `atlas_query_hierarchy.py` | Python Script |
| ETL-Merge | `etl.usp_Atlas_Merge` | Stored Procedure |
| Error_Processing | ELIMINATED — replaced by TRY-CATCH + `etl.Atlas_ETL_Log` | N/A |
| ETL-Tableau | EXCLUDED by customer | N/A |
| ETL-SSRS1 / ETL-SSRS2 | EXCLUDED by customer | N/A |
| ETL-SSAS | EXCLUDED by customer | N/A |

---

## 9. Common Pitfalls

- **Don't use T-SQL reserved words as aliases** — e.g., `KEY`, `VALUE`, `DATE` must be
  bracketed `[KEY]` or avoided entirely
- **Always include `WITH (NOLOCK)`** on Clarity linked server queries
- **Never use `SELECT *`** — always specify columns explicitly
- **`@@ROWCOUNT` must be captured immediately** after the INSERT/UPDATE/DELETE, before
  any other statement executes
- **Logging calls must happen even on failure** — the CATCH block logs before re-raising
- **Don't break the v2 isolation** — never write to schemas without the `_v2` suffix
  or to `etl.*` tables that aren't part of this migration
