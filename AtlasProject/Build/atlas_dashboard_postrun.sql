/*******************************************************************************
 * Atlas ETL v2.0 — Daily Operations Dashboard
 * Beth Israel Lahey Health
 *
 * Purpose:  Run after each pipeline execution to verify pipeline
 *           health, performance, and data integrity.
 *
 * Usage:    Open in SSMS connected to Atlas_Staging on
 *           10.247.4.56\SQL126 and execute (F5) after any
 *           pipeline run completes. Leave @TargetExecID = NULL
 *           to automatically analyze the most recent run, or
 *           set it to a specific ExecutionID to analyze a
 *           historical run.
 *
 * Reports:
 *   1.  Pipeline Summary      — Pass/fail verdict, duration, row totals
 *   3.  Phase Performance     — Duration and row counts by pipeline phase
 *   4.  Slowest Steps         — Bottleneck identification with benchmarks
 *   5A. Daily Row Count Check — Regression detection vs prior run
 *   5B. Day-1 Baseline        — Comparison to April 28, 2026 baseline
 *   5C. Operational Notes     — Reference notes for known data patterns
 *   6.  Execution Order       — Phase ordering verification (pass/fail)
 *   7.  Error Detail          — Full failure forensics
 *   8.  Run History           — Last 10 runs performance trend
 *
 * Target:   Atlas_Staging on 10.247.4.56\SQL126
 * Version:  4.0 — 2026-04-28
 ******************************************************************************/

USE Atlas_Staging;
GO

-- ============================================================================
-- PREAMBLE BLOCK A — etl.SSISBaselineFrozen
-- ============================================================================
-- Permanent snapshot of what SSIS produced on its last run. Created once,
-- populated once (only when empty), never repopulated. Sources at first-run
-- time: Atlas_Staging.raw.*, Atlas_Staging.stage.*, Atlas_Prd.dbo.*. After
-- cutover those sources will start to drift toward Pipeline B's outputs;
-- the IF NOT EXISTS population guard ensures we capture the SSIS state
-- once and never overwrite it.
-- ============================================================================

IF OBJECT_ID('etl.SSISBaselineFrozen', 'U') IS NULL
BEGIN
    CREATE TABLE etl.SSISBaselineFrozen (
        BaselineID      INT IDENTITY(1,1)   NOT NULL,
        SourceDatabase  NVARCHAR(128)       NOT NULL,
        SourceSchema    NVARCHAR(128)       NOT NULL,
        SourceTable     NVARCHAR(256)       NOT NULL,
        PipelineB_Table NVARCHAR(256)       NULL,
        SSISRowCount    BIGINT              NOT NULL,
        CaptureDate     DATETIME            NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_etl_SSISBaselineFrozen PRIMARY KEY CLUSTERED (BaselineID)
    );
    PRINT 'Created etl.SSISBaselineFrozen';
END;

IF NOT EXISTS (SELECT 1 FROM etl.SSISBaselineFrozen)
BEGIN
    DECLARE @FrozenCaptureDate DATETIME = GETDATE();

    -- 1. Atlas_Prd.dbo.* → prd_v2.* with same name-mapping logic as the
    --    deprecated etl.SSISBaseline preamble.
    INSERT INTO etl.SSISBaselineFrozen (SourceDatabase, SourceSchema, SourceTable, PipelineB_Table, SSISRowCount, CaptureDate)
    SELECT
        'Atlas_Prd'                                          AS SourceDatabase,
        t.TABLE_SCHEMA                                       AS SourceSchema,
        t.TABLE_NAME                                         AS SourceTable,
        'prd_v2.' + CASE t.TABLE_NAME
            WHEN 'ReportObject'                THEN 'ReportObjects'
            WHEN 'ReportObjectAttachment'      THEN 'ReportObjectAttachments'
            WHEN 'ReportObjectParameter'       THEN 'ReportObjectParameters'
            WHEN 'ReportObjectSubscription'    THEN 'ReportObjectSubscriptions'
            WHEN 'ReportObjectTag'             THEN 'ReportObjectTags'
            WHEN 'ReportObjectTagMembership'   THEN 'ReportObjectTagMemberships'
            WHEN 'ReportObjectGroup'           THEN 'UserGroups'
            WHEN 'ReportObjectGroupMembership' THEN 'UserGroupsMembership'
            ELSE t.TABLE_NAME
        END                                                  AS PipelineB_Table,
        p.rows                                               AS SSISRowCount,
        @FrozenCaptureDate                                   AS CaptureDate
    FROM   Atlas_Prd.INFORMATION_SCHEMA.TABLES t
    JOIN   Atlas_Prd.sys.partitions p
           ON OBJECT_ID('Atlas_Prd.' + t.TABLE_SCHEMA + '.' + t.TABLE_NAME) = p.object_id
           AND p.index_id IN (0, 1)
    WHERE  t.TABLE_SCHEMA = 'dbo'
      AND  t.TABLE_TYPE = 'BASE TABLE'
      AND  t.TABLE_NAME NOT LIKE 'sys%'
      AND  t.TABLE_NAME NOT LIKE '__MigrationHistory%'
      AND  t.TABLE_NAME NOT LIKE 'AspNet%';

    -- 2. Atlas_Staging.stage.* → stage_v2.*
    INSERT INTO etl.SSISBaselineFrozen (SourceDatabase, SourceSchema, SourceTable, PipelineB_Table, SSISRowCount, CaptureDate)
    SELECT
        'Atlas_Staging'                                      AS SourceDatabase,
        t.TABLE_SCHEMA                                       AS SourceSchema,
        t.TABLE_NAME                                         AS SourceTable,
        'stage_v2.' + t.TABLE_NAME                           AS PipelineB_Table,
        p.rows                                               AS SSISRowCount,
        @FrozenCaptureDate                                   AS CaptureDate
    FROM   INFORMATION_SCHEMA.TABLES t
    JOIN   sys.partitions p
           ON OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME) = p.object_id
           AND p.index_id IN (0, 1)
    WHERE  t.TABLE_SCHEMA = 'stage'
      AND  t.TABLE_TYPE = 'BASE TABLE';

    -- 3. Atlas_Staging.raw.* → raw_v2.* (suffix matching + explicit overrides)
    INSERT INTO etl.SSISBaselineFrozen (SourceDatabase, SourceSchema, SourceTable, PipelineB_Table, SSISRowCount, CaptureDate)
    SELECT
        'Atlas_Staging'                                      AS SourceDatabase,
        t.TABLE_SCHEMA                                       AS SourceSchema,
        t.TABLE_NAME                                         AS SourceTable,
        (SELECT TOP 1 'raw_v2.' + v2t.TABLE_NAME
         FROM   INFORMATION_SCHEMA.TABLES v2t
         WHERE  v2t.TABLE_SCHEMA = 'raw_v2'
           AND  v2t.TABLE_TYPE = 'BASE TABLE'
           AND  (   t.TABLE_NAME = v2t.TABLE_NAME
                 OR t.TABLE_NAME LIKE '%' + v2t.TABLE_NAME
                 OR t.TABLE_NAME LIKE '%-' + v2t.TABLE_NAME
                )
         ORDER BY LEN(v2t.TABLE_NAME) DESC
        )                                                    AS PipelineB_Table,
        p.rows                                               AS SSISRowCount,
        @FrozenCaptureDate                                   AS CaptureDate
    FROM   INFORMATION_SCHEMA.TABLES t
    JOIN   sys.partitions p
           ON OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME) = p.object_id
           AND p.index_id IN (0, 1)
    WHERE  t.TABLE_SCHEMA = 'raw'
      AND  t.TABLE_TYPE = 'BASE TABLE';

    -- 3b. Explicit raw_v2 / stage_v2 name mappings that suffix matching cannot resolve
    UPDATE etl.SSISBaselineFrozen SET PipelineB_Table = 'raw_v2.ClarityUserGroups'
    WHERE  SourceSchema = 'raw' AND SourceTable = 'clarity-user-groups';

    UPDATE etl.SSISBaselineFrozen SET PipelineB_Table = 'raw_v2.ClarityUsernameLinks'
    WHERE  SourceSchema = 'raw' AND SourceTable = 'Clarity_Username_Domainname_Links';

    UPDATE etl.SSISBaselineFrozen SET PipelineB_Table = 'raw_v2.ClarityComponentGroups'
    WHERE  SourceSchema = 'raw' AND SourceTable = 'clarity_server-clarity-component-groups';

    UPDATE etl.SSISBaselineFrozen SET PipelineB_Table = 'raw_v2.ClarityDashboardTypes'
    WHERE  SourceSchema = 'raw' AND SourceTable = 'clarity_server-clarity-dashboard-types';

    UPDATE etl.SSISBaselineFrozen SET PipelineB_Table = 'raw_v2.ClarityDashboardRoles'
    WHERE  SourceSchema = 'raw' AND SourceTable = 'clarity_server-clarity-dashboard-roles';

    UPDATE etl.SSISBaselineFrozen SET PipelineB_Table = 'stage_v2.ReportObjectUserGroupMembers'
    WHERE  SourceSchema = 'stage' AND SourceTable = 'ReportObjectUserGroups';

    PRINT 'SSISBaselineFrozen populated — will not repopulate.';
END
ELSE
    PRINT 'SSISBaselineFrozen already populated — skipped.';
PRINT '';


-- ============================================================================
-- PREAMBLE BLOCK B — etl.ProductionRowCountHistory (DDL only)
-- ============================================================================
-- Rolling per-run history of Atlas_Prd.dbo.* row counts for run-over-run
-- regression detection (Report 5A). Population happens AFTER @TargetExecID
-- is resolved (immediately below the configuration block).
-- ============================================================================

IF OBJECT_ID('etl.ProductionRowCountHistory', 'U') IS NULL
BEGIN
    CREATE TABLE etl.ProductionRowCountHistory (
        HistoryID       INT IDENTITY(1,1)   NOT NULL,
        ExecutionID     UNIQUEIDENTIFIER    NOT NULL,
        CaptureDate     DATETIME            NOT NULL DEFAULT GETDATE(),
        TableName       NVARCHAR(256)       NOT NULL,
        [RowCount]      BIGINT              NOT NULL,
        CONSTRAINT PK_etl_ProductionRowCountHistory PRIMARY KEY CLUSTERED (HistoryID)
    );
    CREATE NONCLUSTERED INDEX IX_ProdHistory_ExecID
        ON etl.ProductionRowCountHistory (ExecutionID);
    PRINT 'Created etl.ProductionRowCountHistory';
END;
PRINT '';


-- ============================================================================
-- CONFIGURATION: Set @TargetExecID to analyze a specific run, or leave NULL
-- ============================================================================
DECLARE @TargetExecID UNIQUEIDENTIFIER = NULL;  -- NULL = most recent run

-- Auto-resolve to most recent if not specified
IF @TargetExecID IS NULL
BEGIN
    SELECT TOP 1 @TargetExecID = ExecutionID
    FROM   etl.Atlas_ETL_Log
    ORDER  BY StartTime DESC;
END;


-- ============================================================================
-- PREAMBLE BLOCK B (population) — append @TargetExecID's Atlas_Prd row counts
-- ============================================================================
-- Idempotent on (ExecutionID): rerunning the dashboard against the same run
-- doesn't double-count.
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM etl.ProductionRowCountHistory
    WHERE  ExecutionID = @TargetExecID
)
BEGIN
    INSERT INTO etl.ProductionRowCountHistory (ExecutionID, CaptureDate, TableName, [RowCount])
    SELECT
        @TargetExecID                                        AS ExecutionID,
        GETDATE()                                            AS CaptureDate,
        t.TABLE_SCHEMA + '.' + t.TABLE_NAME                  AS TableName,
        p.rows                                               AS [RowCount]
    FROM   Atlas_Prd.INFORMATION_SCHEMA.TABLES t
    JOIN   Atlas_Prd.sys.partitions p
           ON OBJECT_ID('Atlas_Prd.' + t.TABLE_SCHEMA + '.' + t.TABLE_NAME) = p.object_id
           AND p.index_id IN (0, 1)
    WHERE  t.TABLE_SCHEMA = 'dbo'
      AND  t.TABLE_TYPE = 'BASE TABLE'
      AND  t.TABLE_SCHEMA + '.' + t.TABLE_NAME IN (
              'dbo.ReportObject',
              'dbo.ReportObjectHierarchy',
              'dbo.ReportObjectRunData',
              'dbo.ReportObjectRunDataBridge',
              'dbo.ReportObjectQuery',
              'dbo.ReportObjectParameters',
              'dbo.ReportObjectSubscriptions',
              'dbo.ReportObjectAttachments',
              'dbo.ReportObjectTagMemberships',
              'dbo.ReportObjectTags',
              'dbo.ReportObjectType',
              'dbo.User',
              'dbo.UserGroups',
              'dbo.UserGroupsMembership',
              'dbo.ReportGroupsMemberships',
              'dbo.DoNotOrphanTypes',
              'dbo.URLOverrides',
              'dbo.VisibilityRules'
          );

    PRINT 'ProductionRowCountHistory: appended ' + CAST(@@ROWCOUNT AS VARCHAR(10))
        + ' rows for ExecutionID ' + CAST(@TargetExecID AS VARCHAR(36));
END
ELSE
    PRINT 'ProductionRowCountHistory: rows already exist for ExecutionID '
        + CAST(@TargetExecID AS VARCHAR(36)) + ' — skipped append.';
PRINT '';


-- ============================================================================
-- REPORT 1: EXECUTION SUMMARY CARD
-- ============================================================================
-- Single-row overview of the entire pipeline run.

PRINT '================================================================';
PRINT 'REPORT 1: EXECUTION SUMMARY';
PRINT '================================================================';

SELECT '------------------------------------------------------------'
    AS [REPORT 1 — PIPELINE SUMMARY];

SELECT
    @TargetExecID                                        AS ExecutionID,
    MIN(StartTime)                                       AS PipelineStart,
    MAX(EndTime)                                         AS PipelineEnd,
    DATEDIFF(SECOND, MIN(StartTime), MAX(EndTime))       AS TotalSeconds,
    RIGHT('0' + CAST(DATEDIFF(SECOND, MIN(StartTime), MAX(EndTime)) / 3600 AS VARCHAR), 2)
        + ':' + RIGHT('0' + CAST((DATEDIFF(SECOND, MIN(StartTime), MAX(EndTime)) % 3600) / 60 AS VARCHAR), 2)
        + ':' + RIGHT('0' + CAST(DATEDIFF(SECOND, MIN(StartTime), MAX(EndTime)) % 60 AS VARCHAR), 2)
                                                         AS TotalDuration,
    COUNT(*)                                             AS TotalSteps,
    SUM(CASE WHEN Status = 'Success' THEN 1 ELSE 0 END) AS Succeeded,
    SUM(CASE WHEN Status = 'Failure' THEN 1 ELSE 0 END) AS Failed,
    SUM(CASE WHEN Status = 'Warning' THEN 1 ELSE 0 END) AS Warnings,
    SUM(ISNULL(RowsAffected, 0))                         AS TotalRowsProcessed,
    CASE
        WHEN SUM(CASE WHEN Status = 'Failure' THEN 1 ELSE 0 END) > 0
            THEN 'FAILED'
        WHEN SUM(CASE WHEN Status = 'Warning' THEN 1 ELSE 0 END) > 0
            THEN 'COMPLETED WITH WARNINGS'
        WHEN MAX(EndTime) IS NULL
            THEN 'INCOMPLETE (still running or aborted)'
        ELSE 'SUCCESS'
    END                                                  AS FinalVerdict
FROM   etl.Atlas_ETL_Log
WHERE  ExecutionID = @TargetExecID;


-- ============================================================================
-- REPORT 3: PERFORMANCE BY PIPELINE PHASE
-- ============================================================================
-- Groups steps into the Amendment A §A.3.4 pipeline phases for high-level
-- performance analysis. The phase assignment is based on PackageName patterns.

PRINT '';
PRINT '================================================================';
PRINT 'REPORT 3: PERFORMANCE BY PIPELINE PHASE';
PRINT '================================================================';

SELECT '------------------------------------------------------------'
    AS [REPORT 3 — PHASE PERFORMANCE];

SELECT
    CASE
        WHEN PackageName = 'ETL-Setup'                    THEN '1-Setup'
        WHEN PackageName = 'ETL-Clarity-Extract'          THEN '2-Extraction'
        WHEN PackageName = 'ETL-Clarity'                  THEN '2-Extraction'
        WHEN PackageName = 'usp_Atlas_Clarity'            THEN '2-Extraction'
        WHEN PackageName = 'ETL-Clarity-CSV'              THEN '2-Extraction'
        WHEN PackageName = 'ETL-Clarity-Hierarchy'        THEN '2-Extraction'
        WHEN PackageName = 'ETL-LDAP'                     THEN '2-Extraction'
        WHEN PackageName = 'usp_Atlas_LDAP'               THEN '2-Extraction'
        WHEN PackageName = 'ETL-DatabaseObjects-Extract'  THEN '2-Extraction'
        WHEN PackageName = 'ETL-DatabaseObjects'          THEN '2-Extraction'
        WHEN PackageName = 'usp_Atlas_DatabaseObjects'    THEN '2-Extraction'
        WHEN PackageName = 'ETL-QueryHierarchy'           THEN '3-RunData'
        WHEN PackageName = 'atlas_pbi_metadata'           THEN '3-RunData'
        WHEN PackageName = 'ETL-PowerBI'                  THEN '3-RunData'
        WHEN PackageName = 'atlas_pbi_user_identity'      THEN '3-RunData'
        WHEN PackageName = 'atlas_pbi_events'             THEN '3-RunData'
        WHEN PackageName = 'ETL-RunData-Extract'          THEN '3-RunData'
        WHEN PackageName = 'ETL-RunData'                  THEN '3-RunData'
        WHEN PackageName = 'usp_Atlas_RunData'            THEN '3-RunData'
        WHEN PackageName = 'ETL-Merge'                    THEN '4-Merge'
        WHEN PackageName = 'usp_Atlas_Merge'              THEN '4-Merge'
        WHEN PackageName = 'ETL-PostProcessing'           THEN '5-PostProcessing'
        WHEN PackageName = 'usp_Atlas_PostProcessing'     THEN '5-PostProcessing'
        WHEN PackageName = 'ETL-Clarity-Validate'         THEN '6-Validation'
        ELSE '9-Other'
    END                                                  AS PipelinePhase,
    COUNT(*)                                             AS StepCount,
    SUM(DATEDIFF(SECOND, StartTime, EndTime))            AS PhaseDurationSec,
    RIGHT('0' + CAST(SUM(DATEDIFF(SECOND, StartTime, EndTime)) / 60 AS VARCHAR), 2)
        + ':' + RIGHT('0' + CAST(SUM(DATEDIFF(SECOND, StartTime, EndTime)) % 60 AS VARCHAR), 2)
                                                         AS PhaseDurationMMSS,
    SUM(ISNULL(RowsAffected, 0))                         AS PhaseRowsProcessed,
    SUM(CASE WHEN Status = 'Failure' THEN 1 ELSE 0 END) AS Failures
FROM   etl.Atlas_ETL_Log
WHERE  ExecutionID = @TargetExecID
GROUP  BY
    CASE
        WHEN PackageName = 'ETL-Setup'                    THEN '1-Setup'
        WHEN PackageName = 'ETL-Clarity-Extract'          THEN '2-Extraction'
        WHEN PackageName = 'ETL-Clarity'                  THEN '2-Extraction'
        WHEN PackageName = 'usp_Atlas_Clarity'            THEN '2-Extraction'
        WHEN PackageName = 'ETL-Clarity-CSV'              THEN '2-Extraction'
        WHEN PackageName = 'ETL-Clarity-Hierarchy'        THEN '2-Extraction'
        WHEN PackageName = 'ETL-LDAP'                     THEN '2-Extraction'
        WHEN PackageName = 'usp_Atlas_LDAP'               THEN '2-Extraction'
        WHEN PackageName = 'ETL-DatabaseObjects-Extract'  THEN '2-Extraction'
        WHEN PackageName = 'ETL-DatabaseObjects'          THEN '2-Extraction'
        WHEN PackageName = 'usp_Atlas_DatabaseObjects'    THEN '2-Extraction'
        WHEN PackageName = 'ETL-QueryHierarchy'           THEN '3-RunData'
        WHEN PackageName = 'atlas_pbi_metadata'           THEN '3-RunData'
        WHEN PackageName = 'ETL-PowerBI'                  THEN '3-RunData'
        WHEN PackageName = 'atlas_pbi_user_identity'      THEN '3-RunData'
        WHEN PackageName = 'atlas_pbi_events'             THEN '3-RunData'
        WHEN PackageName = 'ETL-RunData-Extract'          THEN '3-RunData'
        WHEN PackageName = 'ETL-RunData'                  THEN '3-RunData'
        WHEN PackageName = 'usp_Atlas_RunData'            THEN '3-RunData'
        WHEN PackageName = 'ETL-Merge'                    THEN '4-Merge'
        WHEN PackageName = 'usp_Atlas_Merge'              THEN '4-Merge'
        WHEN PackageName = 'ETL-PostProcessing'           THEN '5-PostProcessing'
        WHEN PackageName = 'usp_Atlas_PostProcessing'     THEN '5-PostProcessing'
        WHEN PackageName = 'ETL-Clarity-Validate'         THEN '6-Validation'
        ELSE '9-Other'
    END
ORDER  BY PipelinePhase;


-- ============================================================================
-- REPORT 4: TOP 10 SLOWEST STEPS — Performance Optimization Targets
-- ============================================================================
-- Identifies the bottlenecks. If any single step exceeds 20% of total
-- pipeline time, flag it for optimization per §5.5 benchmarking criteria.

PRINT '';
PRINT '================================================================';
PRINT 'REPORT 4: TOP 10 SLOWEST STEPS';
PRINT '================================================================';

SELECT '------------------------------------------------------------'
    AS [REPORT 4 — SLOWEST STEPS (Benchmarks)];

DECLARE @TotalPipelineSec INT;
SELECT @TotalPipelineSec = DATEDIFF(SECOND, MIN(StartTime), MAX(EndTime))
FROM   etl.Atlas_ETL_Log
WHERE  ExecutionID = @TargetExecID;

SELECT TOP 10
    PackageName,
    StepName,
    DATEDIFF(SECOND, StartTime, EndTime)                 AS DurationSec,
    RIGHT('0' + CAST(DATEDIFF(SECOND, StartTime, EndTime) / 60 AS VARCHAR), 2)
        + ':' + RIGHT('0' + CAST(DATEDIFF(SECOND, StartTime, EndTime) % 60 AS VARCHAR), 2)
                                                         AS DurationMMSS,
    ISNULL(RowsAffected, 0)                              AS RowsAffected,
    CASE
        WHEN @TotalPipelineSec > 0
            THEN CAST(ROUND(100.0 * DATEDIFF(SECOND, StartTime, EndTime) / @TotalPipelineSec, 1) AS DECIMAL(5,1))
        ELSE 0
    END                                                  AS PctOfTotal,
    CASE
        WHEN @TotalPipelineSec > 0
         AND 100.0 * DATEDIFF(SECOND, StartTime, EndTime) / @TotalPipelineSec > 20.0
            THEN '** BOTTLENECK (>20%) **'
        ELSE ''
    END                                                  AS Flag,
    -- Per-step benchmarks established 2026-04-06 (commit cdf87f2 — Merge 1
    -- rewrite + bridge index work). Step-level regression watch independent
    -- of the run-total trend in Report 8. ETL-Clarity-Hierarchy benchmark
    -- (159s) added 2026-04-07 — single-step SP that runs all 14 hierarchy
    -- branches; PackageName 'ETL-Clarity-Hierarchy' logs one StepName
    -- 'Stage Hierarchies → ReportObjectHierarchyStaging (14 branches)'.
    -- Merge 1 benchmark revised 2026-04-08 from 287s to 370s: the original
    -- 287s baseline was captured before PBI reports were permanently added
    -- to prd_v2.ReportObjects. Post-PBI E2E runs consistently land at
    -- 358s/369s/389s, reflecting the additional ~3,611 PBI rows now part
    -- of the Merge 1 workload. 370s is the new stable baseline.
    CASE
        WHEN StepName LIKE '%Merge 1%'              THEN 370
        WHEN StepName LIKE '%Phase 12%'             THEN 329
        WHEN StepName LIKE '%Stage Hierarchies%'    THEN 159
        -- atlas_pbi_metadata benchmark: TBD after first live E2E run
        -- ETL-PowerBI benchmark: TBD after first live E2E run
        -- atlas_pbi_user_identity benchmark: TBD after first live E2E run
        ELSE NULL
    END                                                  AS BenchmarkSec,
    CASE
        WHEN StepName LIKE '%Merge 1%' AND DATEDIFF(SECOND, StartTime, EndTime) > 370 * 1.20
            THEN '** REGRESSION (>20% over 370s benchmark) **'
        WHEN StepName LIKE '%Phase 12%' AND DATEDIFF(SECOND, StartTime, EndTime) > 329 * 1.20
            THEN '** REGRESSION (>20% over 329s benchmark) **'
        WHEN StepName LIKE '%Stage Hierarchies%' AND DATEDIFF(SECOND, StartTime, EndTime) > 159 * 1.20
            THEN '** REGRESSION (>20% over 159s benchmark) **'
        WHEN StepName LIKE '%Merge 1%'
            THEN 'OK (benchmark: 370s, post-PBI baseline 2026-04-08)'
        WHEN StepName LIKE '%Phase 12%'
            THEN 'OK (benchmark: 329s)'
        WHEN StepName LIKE '%Stage Hierarchies%'
            THEN 'OK (benchmark: 159s)'
        ELSE ''
    END                                                  AS BenchmarkStatus
FROM   etl.Atlas_ETL_Log
WHERE  ExecutionID = @TargetExecID
  AND  EndTime IS NOT NULL
ORDER  BY DATEDIFF(SECOND, StartTime, EndTime) DESC;


-- ============================================================================
-- REPORT 5A: RUN-OVER-RUN REGRESSION DETECTION
-- ============================================================================
-- Compares this run's Atlas_Prd.dbo.* counts (etl.ProductionRowCountHistory
-- WHERE ExecutionID = @TargetExecID) against the immediately prior run's
-- counts (most recent ExecutionID before @TargetExecID).
--
-- Status thresholds:
--   '✓ STABLE'        |PctChange| <= 5%
--   '⚠ GROWTH >5%'    PctChange >  5%
--   '🔴 DROP >5%'     PctChange < -5%
--   '🔴 DROP >20%'    PctChange < -20%   (override)
--   'NO PRIOR RUN'    no earlier ExecutionID present
-- Ordered by PctChange ASC (largest drops first).

PRINT '';
PRINT '================================================================';
PRINT 'REPORT 5A: RUN-OVER-RUN REGRESSION DETECTION';
PRINT '================================================================';

SELECT '------------------------------------------------------------'
    AS [REPORT 5A — DAILY ROW COUNT CHECK (Run-over-Run)];

;WITH cur AS (
    SELECT TableName, [RowCount] AS CurrentRowCount
    FROM   etl.ProductionRowCountHistory
    WHERE  ExecutionID = @TargetExecID
      AND  TableName IN (
              'dbo.ReportObject',
              'dbo.ReportObjectHierarchy',
              'dbo.ReportObjectRunData',
              'dbo.ReportObjectRunDataBridge',
              'dbo.ReportObjectQuery',
              'dbo.ReportObjectParameters',
              'dbo.ReportObjectSubscriptions',
              'dbo.ReportObjectAttachments',
              'dbo.ReportObjectTagMemberships',
              'dbo.ReportObjectTags',
              'dbo.ReportObjectType',
              'dbo.User',
              'dbo.UserGroups',
              'dbo.UserGroupsMembership',
              'dbo.ReportGroupsMemberships',
              'dbo.DoNotOrphanTypes',
              'dbo.URLOverrides',
              'dbo.VisibilityRules'
          )
), prior_exec AS (
    SELECT TOP 1 ExecutionID, MIN(CaptureDate) AS PriorCaptureDate
    FROM   etl.ProductionRowCountHistory
    WHERE  ExecutionID <> @TargetExecID
      AND  CaptureDate < (SELECT MIN(CaptureDate)
                          FROM   etl.ProductionRowCountHistory
                          WHERE  ExecutionID = @TargetExecID)
    GROUP  BY ExecutionID
    ORDER  BY MIN(CaptureDate) DESC
), prv AS (
    SELECT h.TableName, h.[RowCount] AS PriorRowCount
    FROM   etl.ProductionRowCountHistory h
    JOIN   prior_exec pe ON pe.ExecutionID = h.ExecutionID
)
SELECT
    cur.TableName,
    cur.CurrentRowCount,
    prv.PriorRowCount,
    CASE WHEN prv.PriorRowCount IS NOT NULL
         THEN cur.CurrentRowCount - prv.PriorRowCount
         ELSE NULL END                                                AS RowDelta,
    CASE WHEN prv.PriorRowCount IS NULL OR prv.PriorRowCount = 0
         THEN NULL
         ELSE CAST((cur.CurrentRowCount - prv.PriorRowCount) * 100.0
                   / prv.PriorRowCount AS DECIMAL(7,2)) END           AS PctChange,
    CASE
        WHEN prv.PriorRowCount IS NULL                                          THEN 'NO PRIOR RUN'
        WHEN prv.PriorRowCount = 0 AND cur.CurrentRowCount = 0                  THEN '✓ STABLE'
        WHEN prv.PriorRowCount = 0                                              THEN '⚠ GROWTH >5%'
        WHEN (cur.CurrentRowCount - prv.PriorRowCount) * 100.0
             / prv.PriorRowCount < -20                                          THEN '🔴 DROP >20%'
        WHEN (cur.CurrentRowCount - prv.PriorRowCount) * 100.0
             / prv.PriorRowCount <  -5                                          THEN '🔴 DROP >5%'
        WHEN (cur.CurrentRowCount - prv.PriorRowCount) * 100.0
             / prv.PriorRowCount >   5                                          THEN '⚠ GROWTH >5%'
        ELSE '✓ STABLE'
    END                                                               AS Status
FROM   cur
LEFT  JOIN prv ON prv.TableName = cur.TableName
ORDER  BY
    CASE WHEN prv.PriorRowCount IS NULL OR prv.PriorRowCount = 0 THEN 1 ELSE 0 END,
    PctChange ASC;


-- ============================================================================
-- REPORT 5B: CUTOVER PARITY — Current vs Day-1 Production Baseline (2026-04-28)
-- ============================================================================
-- Baseline captured at first dashboard run post-cutover (2026-04-28).
-- Reflects Atlas ETL v2.0 Day-1 production state. SSIS comparison is not
-- available — use this as the Pipeline B Day-1 baseline for long-term
-- trend analysis.

PRINT '';
PRINT '================================================================';
PRINT 'REPORT 5B: CUTOVER PARITY — Current vs Day-1 Production Baseline (2026-04-28)';
PRINT '================================================================';

SELECT '------------------------------------------------------------'
    AS [REPORT 5B — DAY-1 BASELINE (April 28, 2026)];

IF NOT EXISTS (SELECT 1 FROM etl.SSISBaselineFrozen)
BEGIN
    PRINT '  No Day-1 Production Baseline (2026-04-28) available — etl.SSISBaselineFrozen is empty.';
    PRINT '  Run this dashboard once to capture the Day-1 state.';
END;

SELECT
    'dbo.' + t.TABLE_NAME                                AS TableName,
    p.rows                                               AS CurrentRowCount,
    b.SSISRowCount                                       AS SSISBaseline,
    CASE
        WHEN b.SSISRowCount IS NOT NULL THEN p.rows - b.SSISRowCount
        ELSE NULL
    END                                                  AS RowDelta,
    CASE
        WHEN b.SSISRowCount IS NULL OR b.SSISRowCount = 0 THEN NULL
        ELSE CAST((p.rows - b.SSISRowCount) * 100.0
                  / b.SSISRowCount AS DECIMAL(7,2))
    END                                                  AS PctDelta,
    CASE
        WHEN b.SSISRowCount IS NULL THEN 'N/A — no baseline'
        WHEN b.SSISRowCount = 0      THEN 'N/A — baseline zero'
        WHEN ABS((p.rows - b.SSISRowCount) * 100.0 / b.SSISRowCount) <= 5
            THEN '✓ WITHIN 5%'
        WHEN ABS((p.rows - b.SSISRowCount) * 100.0 / b.SSISRowCount) <= 20
            THEN '⚠ DELTA 5-20%'
        ELSE 'ℹ DELTA >20%'
    END                                                  AS Status
FROM   Atlas_Prd.INFORMATION_SCHEMA.TABLES t
JOIN   Atlas_Prd.sys.partitions p
       ON OBJECT_ID('Atlas_Prd.' + t.TABLE_SCHEMA + '.' + t.TABLE_NAME) = p.object_id
       AND p.index_id IN (0, 1)
LEFT JOIN etl.SSISBaselineFrozen b
       ON  b.SourceDatabase = 'Atlas_Prd'
       AND b.SourceSchema   = t.TABLE_SCHEMA
       AND b.SourceTable    = t.TABLE_NAME
WHERE  t.TABLE_SCHEMA = 'dbo'
  AND  t.TABLE_TYPE = 'BASE TABLE'
  AND  t.TABLE_SCHEMA + '.' + t.TABLE_NAME IN (
          'dbo.ReportObject',
          'dbo.ReportObjectHierarchy',
          'dbo.ReportObjectRunData',
          'dbo.ReportObjectRunDataBridge',
          'dbo.ReportObjectQuery',
          'dbo.ReportObjectParameters',
          'dbo.ReportObjectSubscriptions',
          'dbo.ReportObjectAttachments',
          'dbo.ReportObjectTagMemberships',
          'dbo.ReportObjectTags',
          'dbo.ReportObjectType',
          'dbo.User',
          'dbo.UserGroups',
          'dbo.UserGroupsMembership',
          'dbo.ReportGroupsMemberships',
          'dbo.DoNotOrphanTypes',
          'dbo.URLOverrides',
          'dbo.VisibilityRules'
      )
ORDER  BY
    CASE WHEN b.SSISRowCount IS NULL OR b.SSISRowCount = 0 THEN 1 ELSE 0 END,
    PctDelta DESC;

PRINT '';
PRINT '  NOTE: Baseline captured at first dashboard run post-cutover';
PRINT '        (2026-04-28). Reflects Atlas ETL v2.0 Day-1 production';
PRINT '        state. SSIS comparison is not available — use this as';
PRINT '        the Pipeline B Day-1 baseline for long-term trend analysis.';


-- ============================================================================
-- REPORT 6: EXECUTION ORDER VERIFICATION
-- ============================================================================
-- Validates that pipeline components ran in the correct sequence per the
-- atlas_orchestrator.py v4.1 execution order. Three checks:
--
--   6A — Cross-phase ordering: Phase N completed before Phase N+1 started.
--   6B — Within-phase component ordering: components within a phase respect
--         the orchestrator sequence (e.g., Clarity Extract before Clarity SP
--         before CSV Loader before LDAP).
--   6C — Extractor → SP handoff: each Python extractor finished before its
--         downstream SP started.
--
-- Orchestrator sequence (atlas_orchestrator.py v4.1):
--   Phase 1: ETL-Setup
--   Phase 2: ETL-Clarity-Extract → ETL-Clarity → ETL-Clarity-CSV
--            → ETL-LDAP → ETL-DatabaseObjects-Extract → ETL-DatabaseObjects
--   Phase 3: ETL-QueryHierarchy → atlas_pbi_metadata → ETL-PowerBI
--            → atlas_pbi_user_identity → atlas_pbi_events
--            → ETL-RunData-Extract → ETL-RunData
--   Phase 4: ETL-Merge
--   Phase 5: ETL-PostProcessing

PRINT '';
PRINT '================================================================';
PRINT 'REPORT 6: EXECUTION ORDER VERIFICATION';
PRINT '================================================================';

SELECT '------------------------------------------------------------'
    AS [REPORT 6 — EXECUTION ORDER VERIFICATION];

-- Build a reference table of expected component ordering.
-- Phase + ComponentSeq together define the full expected execution sequence.
-- ComponentSeq is the position within the phase (1-based).
DECLARE @ExpectedOrder TABLE (
    PackageName     NVARCHAR(100),
    Phase           INT,
    ComponentSeq    INT,
    DisplayLabel    NVARCHAR(60)
);

INSERT @ExpectedOrder VALUES
    ('ETL-Setup',                    1, 1, 'Setup'),
    ('ETL-Clarity-Extract',          2, 1, 'Clarity Extraction'),
    ('ETL-Clarity',                  2, 2, 'Clarity Staging'),
    ('usp_Atlas_Clarity',            2, 2, 'Clarity Staging'),           -- alternate PackageName
    ('ETL-Clarity-CSV',              2, 3, 'CSV Loader'),
    -- ETL-Clarity-Hierarchy added 2026-04-07 (D-4). Orchestrator v4.3 runs
    -- usp_Atlas_ClarityHierarchy after csv_loader and before LDAP — H6-H10
    -- branches require populated CSV tables. PackageName 'ETL-Clarity-Hierarchy'
    -- (12b_usp_Atlas_ClarityHierarchy.sql line 64). Inserted at seq 4; LDAP
    -- and DatabaseObjects shifted +1 to keep ComponentSeq contiguous for
    -- Report 6B's nxt.ComponentSeq = cur.ComponentSeq + 1 join.
    ('ETL-Clarity-Hierarchy',        2, 4, 'Clarity Hierarchy Staging'),
    ('ETL-LDAP',                     2, 5, 'LDAP User/Group Mapping'),
    ('usp_Atlas_LDAP',               2, 5, 'LDAP User/Group Mapping'),   -- alternate PackageName
    ('ETL-DatabaseObjects-Extract',  2, 6, 'DB Objects Extraction'),
    ('ETL-DatabaseObjects',          2, 7, 'DB Objects Staging'),
    ('usp_Atlas_DatabaseObjects',    2, 7, 'DB Objects Staging'),        -- alternate PackageName
    ('ETL-QueryHierarchy',           3, 1, 'Query Hierarchy'),
    -- atlas_pbi_metadata added 2026-04-07 (Phase 2 dashboard registration).
    -- Orchestrator commit 3e2d3f0 inserted it in phase_rundata immediately
    -- before atlas_pbi_events (both gated on enable_power_bi). Inserted here
    -- at seq (3,2); atlas_pbi_events, ETL-RunData-Extract, and ETL-RunData/
    -- usp_Atlas_RunData shifted +1 to keep ComponentSeq contiguous for
    -- Report 6B's nxt.ComponentSeq = cur.ComponentSeq + 1 join.
    ('atlas_pbi_metadata',           3, 2, 'PBI Metadata'),
    -- ETL-PowerBI added 2026-04-07 (Phase 4 dashboard registration).
    -- Orchestrator commit f83f71a inserted it in phase_rundata immediately
    -- after atlas_pbi_metadata and before atlas_pbi_events (all three gated
    -- on enable_power_bi). Inserted here at seq (3,3); atlas_pbi_events,
    -- ETL-RunData-Extract, and ETL-RunData/usp_Atlas_RunData shifted +1
    -- again to keep ComponentSeq contiguous for Report 6B.
    ('ETL-PowerBI',                  3, 3, 'PBI ReportObject Staging'),
    -- atlas_pbi_user_identity added 2026-04-07 (Phase 5 dashboard registration).
    -- Orchestrator commit 51e08cb inserted it in phase_rundata immediately
    -- after ETL-PowerBI and before atlas_pbi_events (all four gated on
    -- enable_power_bi). Inserted here at seq (3,4); atlas_pbi_events,
    -- ETL-RunData-Extract, and ETL-RunData/usp_Atlas_RunData shifted +1
    -- again to keep ComponentSeq contiguous for Report 6B.
    ('atlas_pbi_user_identity',      3, 4, 'PBI User Identity Enrichment'),
    ('atlas_pbi_events',             3, 5, 'PBI Activity Events'),
    ('ETL-RunData-Extract',          3, 6, 'RunData Extraction'),
    ('ETL-RunData',                  3, 7, 'RunData Staging'),
    ('usp_Atlas_RunData',            3, 7, 'RunData Staging'),           -- alternate PackageName
    ('ETL-Merge',                    4, 1, 'Production Merge'),
    ('usp_Atlas_Merge',              4, 1, 'Production Merge'),          -- alternate PackageName
    ('ETL-PostProcessing',           5, 1, 'Post-Processing'),
    ('usp_Atlas_PostProcessing',     5, 1, 'Post-Processing');           -- alternate PackageName

-- Build CTEs for component timing, then collapse all three ordering checks
-- (6A cross-phase, 6B within-phase, 6C extractor→SP handoff) into one
-- AllChecks set. Final SELECT shows only violations (Status <> 'OK'), with
-- a single 'All ordering checks passed' row if everything is in order.
;WITH ComponentTimes AS (
    SELECT
        eo.PackageName,
        eo.Phase,
        eo.ComponentSeq,
        eo.DisplayLabel,
        MIN(l.StartTime) AS FirstStart,
        MAX(l.EndTime)   AS LastEnd
    FROM   @ExpectedOrder eo
    JOIN   etl.Atlas_ETL_Log l
           ON  l.PackageName  = eo.PackageName
           AND l.ExecutionID  = @TargetExecID
    GROUP  BY eo.PackageName, eo.Phase, eo.ComponentSeq, eo.DisplayLabel
),
PhaseTimes AS (
    SELECT
        Phase,
        MIN(FirstStart)  AS PhaseStart,
        MAX(LastEnd)      AS PhaseEnd
    FROM   ComponentTimes
    GROUP  BY Phase
),
AllChecks AS (
    -- 6A: Cross-phase ordering
    SELECT
        '6A: Cross-Phase' AS CheckType,
        'Phase ' + CAST(cur.Phase AS VARCHAR) + ' → Phase ' + CAST(nxt.Phase AS VARCHAR)
                                                             AS CheckDetail,
        DATEDIFF(SECOND, cur.PhaseEnd, nxt.PhaseStart)       AS GapSeconds,
        CASE
            WHEN cur.PhaseEnd IS NULL
                THEN '** PRIOR PHASE DID NOT COMPLETE **'
            WHEN nxt.PhaseStart IS NULL
                THEN '** NEXT PHASE DID NOT START **'
            WHEN cur.PhaseEnd <= nxt.PhaseStart
                THEN 'OK'
            ELSE '** ORDERING VIOLATION — Phase ' + CAST(nxt.Phase AS VARCHAR)
                 + ' started before Phase ' + CAST(cur.Phase AS VARCHAR) + ' finished **'
        END                                                  AS Status
    FROM   PhaseTimes cur
    JOIN   PhaseTimes nxt ON nxt.Phase = cur.Phase + 1

    UNION ALL

    -- 6B: Within-phase component ordering
    SELECT
        '6B: Within-Phase' AS CheckType,
        cur.DisplayLabel + ' → ' + nxt.DisplayLabel          AS CheckDetail,
        DATEDIFF(SECOND, cur.FirstStart, nxt.FirstStart)     AS GapSeconds,
        CASE
            WHEN cur.FirstStart IS NULL
                THEN '** PRIOR COMPONENT DID NOT RUN **'
            WHEN nxt.FirstStart IS NULL
                THEN '** NEXT COMPONENT DID NOT RUN **'
            WHEN cur.FirstStart <= nxt.FirstStart
                THEN 'OK'
            ELSE '** ORDERING VIOLATION — ' + nxt.DisplayLabel
                 + ' started before ' + cur.DisplayLabel + ' **'
        END                                                  AS Status
    FROM   ComponentTimes cur
    JOIN   ComponentTimes nxt
           ON  nxt.Phase        = cur.Phase
           AND nxt.ComponentSeq = cur.ComponentSeq + 1

    UNION ALL

    -- 6C: Extractor → SP handoff
    SELECT
        '6C: Handoff' AS CheckType,
        ext.DisplayLabel + ' → ' + sp.DisplayLabel           AS CheckDetail,
        DATEDIFF(SECOND, ext.LastEnd, sp.FirstStart)         AS GapSeconds,
        CASE
            WHEN ext.LastEnd IS NULL
                THEN '** EXTRACTOR DID NOT COMPLETE **'
            WHEN sp.FirstStart IS NULL
                THEN '** SP DID NOT START **'
            WHEN ext.LastEnd <= sp.FirstStart
                THEN 'OK'
            ELSE '** ORDERING VIOLATION — SP started before extractor finished **'
        END                                                  AS Status
    FROM   ComponentTimes ext
    JOIN   ComponentTimes sp
           ON  (ext.PackageName = 'ETL-Clarity-Extract'         AND sp.PackageName IN ('ETL-Clarity', 'usp_Atlas_Clarity'))
           OR  (ext.PackageName = 'ETL-Clarity-CSV'             AND sp.PackageName IN ('ETL-LDAP', 'usp_Atlas_LDAP'))
           OR  (ext.PackageName = 'ETL-DatabaseObjects-Extract' AND sp.PackageName IN ('ETL-DatabaseObjects', 'usp_Atlas_DatabaseObjects'))
           OR  (ext.PackageName = 'ETL-RunData-Extract'         AND sp.PackageName IN ('ETL-RunData', 'usp_Atlas_RunData'))
           OR  (ext.PackageName = 'atlas_pbi_events'            AND sp.PackageName IN ('ETL-RunData', 'usp_Atlas_RunData'))
           OR  (ext.PackageName = 'ETL-QueryHierarchy'          AND sp.PackageName IN ('ETL-RunData', 'usp_Atlas_RunData'))
)
SELECT CheckType, CheckDetail, GapSeconds, Status
FROM   AllChecks
WHERE  Status <> 'OK'

UNION ALL

SELECT
    'Summary'                       AS CheckType,
    'All ordering checks passed'    AS CheckDetail,
    NULL                            AS GapSeconds,
    'OK'                            AS Status
WHERE NOT EXISTS (
    SELECT 1 FROM AllChecks WHERE Status <> 'OK'
)

ORDER BY CheckType, GapSeconds;


-- ============================================================================
-- REPORT 7: ERROR LOG DETAIL — Full Failure Forensics
-- ============================================================================
-- All errors and warnings with full message text.

PRINT '';
PRINT '================================================================';
PRINT 'REPORT 7: ERROR LOG DETAIL';
PRINT '================================================================';

SELECT '------------------------------------------------------------'
    AS [REPORT 7 — ERROR DETAIL];

SELECT
    PackageName,
    StepName,
    Status,
    StartTime,
    EndTime,
    DATEDIFF(SECOND, StartTime, ISNULL(EndTime, GETDATE())) AS DurationSec,
    RowsAffected,
    ErrorMessage
FROM   etl.Atlas_ETL_Log
WHERE  ExecutionID = @TargetExecID
  AND  Status IN ('Failure', 'Warning')
ORDER  BY StartTime;

-- If no errors:
IF NOT EXISTS (
    SELECT 1 FROM etl.Atlas_ETL_Log
    WHERE ExecutionID = @TargetExecID AND Status IN ('Failure', 'Warning')
)
    PRINT '  (No errors or warnings recorded for this execution.)';


-- ============================================================================
-- REPORT 8: HISTORICAL RUN COMPARISON — Performance Benchmarking
-- ============================================================================
-- Compares the target execution against the last 10 runs for trend analysis.
-- Use this for the §5.5 "no regressions > 20%" benchmarking requirement.

PRINT '';
PRINT '================================================================';
PRINT 'REPORT 8: HISTORICAL RUN COMPARISON (Last 10 Runs)';
PRINT '================================================================';

SELECT '------------------------------------------------------------'
    AS [REPORT 8 — RUN HISTORY (Last 10 Runs)];

;WITH RunSummary AS (
    SELECT
        ExecutionID,
        MIN(StartTime)                                   AS PipelineStart,
        MAX(EndTime)                                     AS PipelineEnd,
        DATEDIFF(SECOND, MIN(StartTime), MAX(EndTime))   AS TotalSeconds,
        COUNT(*)                                         AS StepCount,
        SUM(ISNULL(RowsAffected, 0))                     AS TotalRows,
        SUM(CASE WHEN Status = 'Failure' THEN 1 ELSE 0 END) AS Failures,
        DENSE_RANK() OVER (ORDER BY MIN(StartTime) DESC) AS RunRank
    FROM   etl.Atlas_ETL_Log
    -- Filter killed-run orphans: log entries with NULL EndTime are SP starts
    -- whose pipeline never completed (orchestrator killed, SP timeout, etc).
    -- Including them inflates StepCount and produces misleading DeltaVsPrevious
    -- values. Partial runs with at least one completed step still appear, but
    -- only their completed steps contribute to the summary.
    WHERE  EndTime IS NOT NULL
    GROUP  BY ExecutionID
)
SELECT
    RunRank,
    ExecutionID,
    PipelineStart,
    TotalSeconds,
    RIGHT('0' + CAST(TotalSeconds / 3600 AS VARCHAR), 2)
        + ':' + RIGHT('0' + CAST((TotalSeconds % 3600) / 60 AS VARCHAR), 2)
        + ':' + RIGHT('0' + CAST(TotalSeconds % 60 AS VARCHAR), 2)
                                                         AS DurationHHMMSS,
    StepCount,
    TotalRows,
    Failures,
    CASE WHEN ExecutionID = @TargetExecID THEN '<<< THIS RUN' ELSE '' END AS Marker,
    CASE
        WHEN RunRank = 1 THEN ''
        WHEN LAG(TotalSeconds) OVER (ORDER BY RunRank) > 0
            THEN CAST(CAST(
                (TotalSeconds - LAG(TotalSeconds) OVER (ORDER BY RunRank)) * 100.0
                / LAG(TotalSeconds) OVER (ORDER BY RunRank)
                AS DECIMAL(10,1)) AS VARCHAR) + '%'
        ELSE ''
    END                                                  AS DeltaVsPrevious
FROM   RunSummary
WHERE  RunRank <= 10
ORDER  BY RunRank;


-- ============================================================================
-- REPORT 5C: POST-CUTOVER OPERATIONAL NOTES
-- ============================================================================
-- Static reference text — not a query. Documents accepted architectural
-- deltas and run-over-run alarm thresholds for operators reading the
-- dashboard output. Moved to end of file so it appears last in the
-- Messages tab; label retained as "REPORT 5C" so existing documentation
-- references remain valid.

SELECT '------------------------------------------------------------'
    AS [REPORT 5C — OPERATIONAL NOTES];

PRINT '';
PRINT '================================================================';
PRINT 'REPORT 5C: POST-CUTOVER OPERATIONAL NOTES';
PRINT '================================================================';
PRINT '';
PRINT '1. ReportObjectRunData / ReportObjectRunDataBridge';
PRINT '   30-day rolling window only. Legacy accumulated 13+ months of';
PRINT '   PBI run history. Pipeline B PBI history grows from cutover';
PRINT '   forward. No backfill planned.';
PRINT '';
PRINT '2. ReportObjects (Power BI)';
PRINT '   Atlas ETL v2.0 produces one row per PBI report (~3,567 rows)';
PRINT '   using unique per-report BizKeys. Legacy SSIS produced 3';
PRINT '   category-level rows (118 rows). Legacy PBI rows are';
PRINT '   orphan-flagged at cutover.';
PRINT '';
PRINT '3. EDM / CogitoTools databases';
PRINT '   Excluded from DatabaseObjects extraction pending service';
PRINT '   account provisioning. Will re-appear automatically when the';
PRINT '   service account is provisioned.';
PRINT '';
PRINT '4. DoNotOrphanTypes';
PRINT '   Protected types: Power BI App (33), Power BI Paginated Report';
PRINT '   (34), Power BI Report (35), Documentation (38). These types';
PRINT '   are never orphan-flagged regardless of extraction status.';
PRINT '';
PRINT '5. Run-over-run alarm thresholds (Report 5A)';
PRINT '   Any Atlas_Prd.dbo table dropping >5% row count run-over-run';
PRINT '   should be investigated. Drops >20% are critical alarms.';
PRINT '';
