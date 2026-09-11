-- DEPRECATED: Baseline capture is now integrated into
-- atlas_dashboard_postrun.sql. This script is retained
-- for manual baseline refresh only.
/*******************************************************************************
 * Atlas ETL Pipeline — SSIS Baseline Row Count Capture
 *
 * Purpose:  Captures row counts from live SSIS schemas (Atlas_Prd.dbo,
 *           Atlas_Staging.stage, Atlas_Staging.raw) and maps them to their
 *           Pipeline B equivalents (prd_v2, stage_v2, raw_v2).
 *
 *           DEPRECATED as of v1.5: atlas_dashboard_postrun.sql now performs
 *           this capture automatically in its preamble. This standalone
 *           script is retained for manual/ad-hoc baseline refresh only.
 *
 * Target:   Atlas_Staging (10.247.4.56\SQL126)
 * Schema:   etl.SSISBaseline
 * Version:  1.0 — April 2026
 * Author:   Larry Duren / Atlas Migration Project
 *
 * Re-runnable: Yes — uses DELETE + INSERT to refresh the entire snapshot.
 *              Run before or after an SSIS pipeline execution to capture
 *              the baseline, then run atlas_dashboard_postrun.sql to compare.
 *
 * Cross-database: Queries Atlas_Prd.dbo.* (same SQL Server instance).
 ******************************************************************************/

USE Atlas_Staging;
GO

-- ============================================================================
-- DDL: Create etl.SSISBaseline if it doesn't exist
-- ============================================================================

IF OBJECT_ID('etl.SSISBaseline', 'U') IS NULL
BEGIN
    CREATE TABLE etl.SSISBaseline (
        BaselineID      INT IDENTITY(1,1)   NOT NULL,
        SourceDatabase  NVARCHAR(128)       NOT NULL,   -- 'Atlas_Prd' or 'Atlas_Staging'
        SourceSchema    NVARCHAR(128)       NOT NULL,   -- 'dbo', 'stage', 'raw'
        SourceTable     NVARCHAR(256)       NOT NULL,   -- actual SSIS table name
        PipelineB_Table NVARCHAR(256)       NULL,       -- mapped prd_v2/stage_v2/raw_v2 name
        SSISRowCount    BIGINT              NOT NULL,
        CaptureDate     DATETIME            NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_etl_SSISBaseline PRIMARY KEY CLUSTERED (BaselineID)
    );
    PRINT 'Created etl.SSISBaseline';
END
ELSE
    PRINT 'etl.SSISBaseline already exists';
GO


-- ============================================================================
-- CAPTURE: Snapshot current SSIS row counts
-- ============================================================================
-- Refreshes the entire baseline (DELETE + INSERT).
-- CaptureDate records when this snapshot was taken.

PRINT '';
PRINT 'Capturing SSIS baseline row counts...';
PRINT 'Start: ' + CONVERT(VARCHAR(30), GETDATE(), 121);

DELETE FROM etl.SSISBaseline;

DECLARE @CaptureDate DATETIME = GETDATE();

-- ────────────────────────────────────────────────────────────────────────────
-- 1. Atlas_Prd.dbo.* → prd_v2.*
-- Known name mappings (singular → plural, etc.)
-- ────────────────────────────────────────────────────────────────────────────

INSERT INTO etl.SSISBaseline (SourceDatabase, SourceSchema, SourceTable, PipelineB_Table, SSISRowCount, CaptureDate)
SELECT
    'Atlas_Prd'                                          AS SourceDatabase,
    t.TABLE_SCHEMA                                       AS SourceSchema,
    t.TABLE_NAME                                         AS SourceTable,
    'prd_v2.' + CASE t.TABLE_NAME
        -- Known singular → plural mappings
        WHEN 'ReportObject'                THEN 'ReportObjects'
        WHEN 'ReportObjectAttachment'      THEN 'ReportObjectAttachments'
        WHEN 'ReportObjectParameter'       THEN 'ReportObjectParameters'
        WHEN 'ReportObjectSubscription'    THEN 'ReportObjectSubscriptions'
        WHEN 'ReportObjectTag'             THEN 'ReportObjectTags'
        WHEN 'ReportObjectTagMembership'   THEN 'ReportObjectTagMemberships'
        WHEN 'ReportObjectGroup'           THEN 'UserGroups'
        WHEN 'ReportObjectGroupMembership' THEN 'UserGroupsMembership'
        -- Same name in both schemas
        ELSE t.TABLE_NAME
    END                                                  AS PipelineB_Table,
    p.rows                                               AS SSISRowCount,
    @CaptureDate                                         AS CaptureDate
FROM   Atlas_Prd.INFORMATION_SCHEMA.TABLES t
JOIN   Atlas_Prd.sys.partitions p
       ON OBJECT_ID('Atlas_Prd.' + t.TABLE_SCHEMA + '.' + t.TABLE_NAME) = p.object_id
       AND p.index_id IN (0, 1)
WHERE  t.TABLE_SCHEMA = 'dbo'
  AND  t.TABLE_TYPE = 'BASE TABLE'
  -- Exclude system/internal tables
  AND  t.TABLE_NAME NOT LIKE 'sys%'
  AND  t.TABLE_NAME NOT LIKE '__MigrationHistory%'
  AND  t.TABLE_NAME NOT LIKE 'AspNet%';

DECLARE @PrdCount INT = @@ROWCOUNT;
PRINT '  Atlas_Prd.dbo: ' + CAST(@PrdCount AS VARCHAR(10)) + ' tables captured';


-- ────────────────────────────────────────────────────────────────────────────
-- 2. Atlas_Staging.stage.* → stage_v2.*
-- SSIS staging tables use 'stage' schema; Pipeline B uses 'stage_v2'.
-- Table names are generally the same.
-- ────────────────────────────────────────────────────────────────────────────

INSERT INTO etl.SSISBaseline (SourceDatabase, SourceSchema, SourceTable, PipelineB_Table, SSISRowCount, CaptureDate)
SELECT
    'Atlas_Staging'                                      AS SourceDatabase,
    t.TABLE_SCHEMA                                       AS SourceSchema,
    t.TABLE_NAME                                         AS SourceTable,
    'stage_v2.' + t.TABLE_NAME                           AS PipelineB_Table,
    p.rows                                               AS SSISRowCount,
    @CaptureDate                                         AS CaptureDate
FROM   INFORMATION_SCHEMA.TABLES t
JOIN   sys.partitions p
       ON OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME) = p.object_id
       AND p.index_id IN (0, 1)
WHERE  t.TABLE_SCHEMA = 'stage'
  AND  t.TABLE_TYPE = 'BASE TABLE';

DECLARE @StageCount INT = @@ROWCOUNT;
PRINT '  Atlas_Staging.stage: ' + CAST(@StageCount AS VARCHAR(10)) + ' tables captured';


-- ────────────────────────────────────────────────────────────────────────────
-- 3. Atlas_Staging.raw.* → raw_v2.*
-- SSIS raw tables use 'raw' schema; Pipeline B uses 'raw_v2'.
-- Raw table names often differ significantly (long linked-server-style names
-- in SSIS vs. short names in Pipeline B). Map by suffix matching where
-- possible; unmatched tables get NULL PipelineB_Table.
-- ────────────────────────────────────────────────────────────────────────────

INSERT INTO etl.SSISBaseline (SourceDatabase, SourceSchema, SourceTable, PipelineB_Table, SSISRowCount, CaptureDate)
SELECT
    'Atlas_Staging'                                      AS SourceDatabase,
    t.TABLE_SCHEMA                                       AS SourceSchema,
    t.TABLE_NAME                                         AS SourceTable,
    -- Try to find a matching raw_v2 table by checking if the SSIS raw table
    -- name ends with the raw_v2 table name (e.g., 'clarity_server-clarityreport-dbo-METRIC_INFO'
    -- matches raw_v2.METRIC_INFO). If no match, leave NULL.
    (SELECT TOP 1 'raw_v2.' + v2t.TABLE_NAME
     FROM   INFORMATION_SCHEMA.TABLES v2t
     WHERE  v2t.TABLE_SCHEMA = 'raw_v2'
       AND  v2t.TABLE_TYPE = 'BASE TABLE'
       AND  (   t.TABLE_NAME = v2t.TABLE_NAME                           -- exact match
             OR t.TABLE_NAME LIKE '%' + v2t.TABLE_NAME                  -- suffix match
             OR t.TABLE_NAME LIKE '%-' + v2t.TABLE_NAME                 -- hyphen-delimited suffix
            )
     ORDER BY LEN(v2t.TABLE_NAME) DESC  -- prefer longest match
    )                                                    AS PipelineB_Table,
    p.rows                                               AS SSISRowCount,
    @CaptureDate                                         AS CaptureDate
FROM   INFORMATION_SCHEMA.TABLES t
JOIN   sys.partitions p
       ON OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME) = p.object_id
       AND p.index_id IN (0, 1)
WHERE  t.TABLE_SCHEMA = 'raw'
  AND  t.TABLE_TYPE = 'BASE TABLE';

DECLARE @RawCount INT = @@ROWCOUNT;
PRINT '  Atlas_Staging.raw: ' + CAST(@RawCount AS VARCHAR(10)) + ' tables captured';


-- ────────────────────────────────────────────────────────────────────────────
-- Summary
-- ────────────────────────────────────────────────────────────────────────────

PRINT '';
PRINT 'Baseline capture complete.';
PRINT '  Total tables: ' + CAST(@PrdCount + @StageCount + @RawCount AS VARCHAR(10));
PRINT '  CaptureDate:  ' + CONVERT(VARCHAR(30), @CaptureDate, 121);
PRINT '';

-- Show mapping summary
SELECT
    SourceDatabase,
    SourceSchema,
    COUNT(*)                                             AS TotalTables,
    SUM(CASE WHEN PipelineB_Table IS NOT NULL THEN 1 ELSE 0 END) AS Mapped,
    SUM(CASE WHEN PipelineB_Table IS NULL THEN 1 ELSE 0 END)     AS Unmapped
FROM   etl.SSISBaseline
GROUP  BY SourceDatabase, SourceSchema
ORDER  BY SourceDatabase, SourceSchema;

-- Show unmapped tables (for manual review)
SELECT
    SourceDatabase + '.' + SourceSchema + '.' + SourceTable AS SSISTable,
    SSISRowCount,
    'No Pipeline B equivalent found' AS Note
FROM   etl.SSISBaseline
WHERE  PipelineB_Table IS NULL
  AND  SSISRowCount > 0
ORDER  BY SSISRowCount DESC;

GO
