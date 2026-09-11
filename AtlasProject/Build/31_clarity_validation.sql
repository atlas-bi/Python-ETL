/*******************************************************************************
 * Atlas ETL Migration — Week 4: Data Validation Queries
 * 
 * Validates RunData extraction (13 tables), PBI Activity Events,
 * Merge operations (20 tasks), and cross-system integrity.
 *
 * Run after:
 *   1. atlas_pbi_events.py (PBI extraction)
 *   2. usp_Atlas_RunData (Clarity, SlicerDicer, PBI staging + merge)
 *   3. usp_Atlas_Merge (20 staging → production operations)
 *
 * Sections:
 *   1. RunData Raw Table Row Counts (13 tables from DDL)
 *   2. RunData Row Count Parity (v2 vs SSIS production)
 *   3. PBI Activity Events Completeness
 *   4. BizKey Integrity & Deduplication
 *   5. Bridge Table Validation
 *   6. Time Dimension Consistency
 *   7. Merge Operation Verification (12 core + 7 enrichment)
 *   8. Cross-System Referential Integrity
 *   9. ETL Log Review (Today's runs)
 *
 * Author:  Larry Duren
 * Date:    February 2026
 * Version: 4.2 (Week 4 — consolidated)
 *
 * v4.2 Changes:
 *   - Section 7: DistinctUsers → DistinctUsersPast12Months on
 *     prd_v2.ReportObjects (matches corrected Phase 10 column name)
 ******************************************************************************/

USE Atlas_Staging;
GO

PRINT '================================================================';
PRINT 'Atlas ETL Migration — Week 4 Validation';
PRINT 'Run Time: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
PRINT '================================================================';
PRINT '';
GO


-- =============================================================================
-- SECTION 1: RUNDATA RAW TABLE ROW COUNTS (13 tables)
-- Verifies all tables created by 01_week4_rundata_ddl.sql are populated.
-- =============================================================================
PRINT '--- Section 1: RunData Raw Table Row Counts ---';
PRINT '';

SELECT *
FROM (
    -- Raw layer (8 tables)
    SELECT 'raw_v2.ClarityReportRunData'       AS TableName, COUNT(*) AS [RowCount] FROM raw_v2.ClarityReportRunData
    UNION ALL
    SELECT 'raw_v2.ClarityDashboardRunData',    COUNT(*) FROM raw_v2.ClarityDashboardRunData
    UNION ALL
    SELECT 'raw_v2.SlicerDicerStatsHttp',       COUNT(*) FROM raw_v2.SlicerDicerStatsHttp
    UNION ALL
    SELECT 'raw_v2.SlicerDicerStatsQuery',      COUNT(*) FROM raw_v2.SlicerDicerStatsQuery
    UNION ALL
    SELECT 'raw_v2.SlicerDicerStatsSaveLoad',   COUNT(*) FROM raw_v2.SlicerDicerStatsSaveLoad
    UNION ALL
    SELECT 'raw_v2.PbiActivityEvent',           COUNT(*) FROM raw_v2.PbiActivityEvent
    UNION ALL
    SELECT 'raw_v2.PbiActivityEventHistory',    COUNT(*) FROM raw_v2.PbiActivityEventHistory
    UNION ALL
    SELECT 'raw_v2.PbiActivityEventJoined',     COUNT(*) FROM raw_v2.PbiActivityEventJoined

    UNION ALL
    -- Stage layer (3 tables)
    SELECT 'stage_v2.ReportObjectRunDataStaging',       COUNT(*) FROM stage_v2.ReportObjectRunDataStaging
    UNION ALL
    SELECT 'stage_v2.ReportObjectRunDataParentStaging',  COUNT(*) FROM stage_v2.ReportObjectRunDataParentStaging
    UNION ALL
    SELECT 'stage_v2.ReportObjectRunDataBridgeStaging', COUNT(*) FROM stage_v2.ReportObjectRunDataBridgeStaging

    UNION ALL
    -- Production layer (2 tables — persistent)
    SELECT 'prd_v2.ReportObjectRunData',        COUNT(*) FROM prd_v2.ReportObjectRunData
    UNION ALL
    SELECT 'prd_v2.ReportObjectRunDataBridge',  COUNT(*) FROM prd_v2.ReportObjectRunDataBridge
) t
ORDER BY t.TableName;
GO


-- =============================================================================
-- SECTION 2: RUNDATA ROW COUNT PARITY (v2 vs SSIS production)
-- Compares v2 staging counts with existing SSIS-populated tables.
-- Adjust dbo.* references to match your SSIS production table names.
-- =============================================================================
PRINT '';
PRINT '--- Section 2: RunData Row Count Parity (v2 vs SSIS) ---';
PRINT '';

SELECT
    t.TableName,
    t.V2_Count,
    t.SSIS_Count,
    CASE 
        WHEN t.SSIS_Count = 0 THEN NULL
        ELSE CAST(
            ABS(t.V2_Count - t.SSIS_Count) * 100.0 / t.SSIS_Count
        AS DECIMAL(5,2))
    END AS DiffPct,
    CASE 
        WHEN t.SSIS_Count = 0 THEN 'N/A (no SSIS data)'
        WHEN ABS(t.V2_Count - t.SSIS_Count) * 100.0 / t.SSIS_Count <= 1.0 THEN 'PASS'
        WHEN ABS(t.V2_Count - t.SSIS_Count) * 100.0 / t.SSIS_Count <= 5.0 THEN 'WARNING'
        ELSE 'FAIL'
    END AS Status
FROM (
    SELECT 'RunDataStaging' AS TableName,
        (SELECT COUNT(*) FROM stage_v2.ReportObjectRunDataStaging) AS V2_Count,
        (SELECT COUNT(*) FROM stage.ReportObjectRunDataStaging) AS SSIS_Count
    UNION ALL
    SELECT 'RunDataBridgeStaging',
        (SELECT COUNT(*) FROM stage_v2.ReportObjectRunDataBridgeStaging),
        (SELECT COUNT(*) FROM stage.ReportObjectRunDataBridgeStaging)
    UNION ALL
    SELECT 'PbiActivityEventHistory',
        (SELECT COUNT(*) FROM raw_v2.PbiActivityEventHistory),
        (SELECT COUNT(*) FROM dbo.PbiActivityEventHistory)
    UNION ALL
    SELECT 'ReportObjectRunData (Prd)',
        (SELECT COUNT(*) FROM prd_v2.ReportObjectRunData),
        (SELECT COUNT(*) FROM Atlas_Prd.dbo.ReportObjectRunData)
    UNION ALL
    SELECT 'ReportObjectRunDataBridge (Prd)',
        (SELECT COUNT(*) FROM prd_v2.ReportObjectRunDataBridge),
        (SELECT COUNT(*) FROM Atlas_Prd.dbo.ReportObjectRunDataBridge)
) t;
GO


-- =============================================================================
-- SECTION 3: PBI ACTIVITY EVENTS COMPLETENESS
-- Validates the PBI extraction covers the expected 28-day window.
-- =============================================================================
PRINT '';
PRINT '--- Section 3: PBI Activity Events Completeness ---';
PRINT '';

-- Date range coverage
SELECT
    'PBI Event Window' AS Metric,
    MIN(CreationTime) AS EarliestEvent,
    MAX(CreationTime) AS LatestEvent,
    DATEDIFF(DAY, MIN(CreationTime), MAX(CreationTime)) AS DaysCovered,
    COUNT(*) AS TotalEvents,
    COUNT(DISTINCT CAST(CreationTime AS DATE)) AS DistinctDays
FROM raw_v2.PbiActivityEvent;

-- Events per month distribution
SELECT
    YEAR(CreationTime) AS EventYear,
    MONTH(CreationTime) AS EventMonth,
    COUNT(*) AS EventCount,
    COUNT(DISTINCT UserId) AS DistinctUsers
FROM raw_v2.PbiActivityEvent
WHERE CreationTime IS NOT NULL
GROUP BY YEAR(CreationTime), MONTH(CreationTime)
ORDER BY EventYear, EventMonth;

-- Top activity types
SELECT TOP 10
    Activity,
    COUNT(*) AS EventCount,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM raw_v2.PbiActivityEvent) AS DECIMAL(5,1)) AS PctOfTotal
FROM raw_v2.PbiActivityEvent
GROUP BY Activity
ORDER BY EventCount DESC;

-- History dedup check: events in raw vs history (should grow monotonically)
SELECT
    'PBI Dedup Check' AS Metric,
    (SELECT COUNT(*) FROM raw_v2.PbiActivityEvent)       AS RawEvents,
    (SELECT COUNT(*) FROM raw_v2.PbiActivityEventHistory) AS HistoryEvents,
    (SELECT COUNT(*) FROM raw_v2.PbiActivityEventJoined)  AS JoinedEvents;
GO


-- =============================================================================
-- SECTION 4: BIZKEY INTEGRITY & DEDUPLICATION
-- Validates BizKey format (pipe-delimited), uniqueness after dedup.
-- =============================================================================
PRINT '';
PRINT '--- Section 4: BizKey Integrity & Deduplication ---';
PRINT '';

-- BizKey format check: should contain || delimiters
SELECT
    'BizKey Format' AS CheckName,
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN ReportObjectBizKey LIKE '%||%' THEN 1 ELSE 0 END) AS ValidFormat,
    SUM(CASE WHEN ReportObjectBizKey NOT LIKE '%||%' THEN 1 ELSE 0 END) AS InvalidFormat,
    CASE
        WHEN SUM(CASE WHEN ReportObjectBizKey NOT LIKE '%||%' THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL — ' + CAST(SUM(CASE WHEN ReportObjectBizKey NOT LIKE '%||%' THEN 1 ELSE 0 END) AS VARCHAR(10)) + ' invalid'
    END AS Status
FROM stage_v2.ReportObjectRunDataStaging;

-- BizKey NULL check
SELECT
    'BizKey Nulls' AS CheckName,
    SUM(CASE WHEN ReportObjectBizKey IS NULL THEN 1 ELSE 0 END) AS NullBizKeys,
    CASE
        WHEN SUM(CASE WHEN ReportObjectBizKey IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'WARNING — ' + CAST(SUM(CASE WHEN ReportObjectBizKey IS NULL THEN 1 ELSE 0 END) AS VARCHAR(10)) + ' nulls'
    END AS Status
FROM stage_v2.ReportObjectRunDataStaging;

-- Deduplication check: no duplicate (BizKey + RunStartTime + User + Source)
SELECT
    'RunData Dedup' AS CheckName,
    COUNT(*) AS TotalRows,
    COUNT(DISTINCT CONCAT(ReportObjectBizKey, '|', RunStartTime, '|', RunUserName, '|', SourceSystem)) AS UniqueKeys,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT CONCAT(ReportObjectBizKey, '|', RunStartTime, '|', RunUserName, '|', SourceSystem))
            THEN 'PASS — no duplicates'
        ELSE 'FAIL — ' + CAST(COUNT(*) - COUNT(DISTINCT CONCAT(ReportObjectBizKey, '|', RunStartTime, '|', RunUserName, '|', SourceSystem)) AS VARCHAR(10)) + ' duplicates'
    END AS Status
FROM stage_v2.ReportObjectRunDataStaging;

-- BizKey segment count (should be 6 segments: Server||DB||Type||CatalogID||MasterFile||RecordID)
SELECT TOP 5
    'BizKey Sample' AS CheckName,
    ReportObjectBizKey,
    LEN(ReportObjectBizKey) - LEN(REPLACE(ReportObjectBizKey, '||', '|')) AS DelimiterCount
FROM stage_v2.ReportObjectRunDataStaging
WHERE ReportObjectBizKey IS NOT NULL;
GO


-- =============================================================================
-- SECTION 5: BRIDGE TABLE VALIDATION
-- Verifies parent-child run inheritance is working correctly.
-- =============================================================================
PRINT '';
PRINT '--- Section 5: Bridge Table Validation ---';
PRINT '';

-- Bridge staging counts by source system
SELECT
    'Bridge by Source' AS CheckName,
    SourceSystem,
    COUNT(*) AS BridgeRows
FROM stage_v2.ReportObjectRunDataBridgeStaging
GROUP BY SourceSystem;

-- Parent staging aggregation
SELECT
    'Parent Staging' AS CheckName,
    COUNT(*) AS TotalParents,
    SUM(CASE WHEN ChildCount > 0 THEN 1 ELSE 0 END) AS ParentsWithChildren,
    AVG(ChildCount) AS AvgChildrenPerParent
FROM (
    SELECT ParentReportObjectBizKey, COUNT(*) AS ChildCount
    FROM stage_v2.ReportObjectRunDataBridgeStaging
    GROUP BY ParentReportObjectBizKey
) parents;

-- Bridge references must resolve to known BizKeys in production
SELECT
    'Bridge → RunData FK' AS CheckName,
    COUNT(*) AS TotalBridges,
    SUM(CASE WHEN rd.ReportObjectBizKey IS NOT NULL THEN 1 ELSE 0 END) AS Resolved,
    SUM(CASE WHEN rd.ReportObjectBizKey IS NULL THEN 1 ELSE 0 END) AS Orphaned,
    CASE
        WHEN SUM(CASE WHEN rd.ReportObjectBizKey IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'WARNING — ' + CAST(SUM(CASE WHEN rd.ReportObjectBizKey IS NULL THEN 1 ELSE 0 END) AS VARCHAR(10)) + ' orphaned bridges'
    END AS Status
FROM prd_v2.ReportObjectRunDataBridge b
LEFT JOIN prd_v2.ReportObjectRunData rd ON b.ParentBizKey = rd.ReportObjectBizKey;
GO


-- =============================================================================
-- SECTION 6: TIME DIMENSION CONSISTENCY
-- Validates DATEPART extraction (Year, Month, Day, Hour) is populated.
-- =============================================================================
PRINT '';
PRINT '--- Section 6: Time Dimension Consistency ---';
PRINT '';

SELECT
    'Time Dimension Population' AS CheckName,
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN RunYear IS NOT NULL THEN 1 ELSE 0 END)  AS WithYear,
    SUM(CASE WHEN RunMonth IS NOT NULL THEN 1 ELSE 0 END) AS WithMonth,
    SUM(CASE WHEN RunDay IS NOT NULL THEN 1 ELSE 0 END)   AS WithDay,
    SUM(CASE WHEN RunHour IS NOT NULL THEN 1 ELSE 0 END)  AS WithHour,
    CASE
        WHEN SUM(CASE WHEN RunYear IS NULL OR RunMonth IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL — missing time dimensions'
    END AS Status
FROM stage_v2.ReportObjectRunDataStaging;

-- Year distribution (should fall within 28-day window)
SELECT
    'Year Distribution' AS CheckName,
    RunYear,
    COUNT(*) AS RunCount,
    COUNT(DISTINCT SourceSystem) AS SourceSystems
FROM stage_v2.ReportObjectRunDataStaging
WHERE RunYear IS NOT NULL
GROUP BY RunYear
ORDER BY RunYear;

-- Source system breakdown
SELECT
    'Source System Mix' AS CheckName,
    SourceSystem,
    COUNT(*) AS RunCount,
    MIN(RunStartTime) AS EarliestRun,
    MAX(RunStartTime) AS LatestRun,
    COUNT(DISTINCT RunUserName) AS DistinctUsers
FROM stage_v2.ReportObjectRunDataStaging
GROUP BY SourceSystem;
GO


-- =============================================================================
-- SECTION 7: MERGE OPERATION VERIFICATION
-- Checks that all 12 core merge targets have data, plus enrichment effects.
-- =============================================================================
PRINT '';
PRINT '--- Section 7: Merge Operation Verification ---';
PRINT '';

-- Core merge targets: production table row counts
SELECT *
FROM (
    SELECT 'prd_v2.ReportObjects'                 AS TableName, COUNT(*) AS [RowCount], 'Merge 1'  AS Operation FROM prd_v2.ReportObjects
    UNION ALL
    SELECT 'prd_v2.[User]',                         COUNT(*), 'Merge 2'   FROM prd_v2.[User]
    UNION ALL
    SELECT 'prd_v2.ReportObjectGroup',             COUNT(*), 'Merge 3'   FROM prd_v2.ReportObjectGroup
    UNION ALL
    SELECT 'prd_v2.ReportObjectGroupMembership',   COUNT(*), 'Merge 4'   FROM prd_v2.ReportObjectGroupMembership
    UNION ALL
    SELECT 'prd_v2.ReportObjectQuery',             COUNT(*), 'Merge 5'   FROM prd_v2.ReportObjectQuery
    UNION ALL
    SELECT 'prd_v2.ReportObjectHierarchy',         COUNT(*), 'Merge 5'   FROM prd_v2.ReportObjectHierarchy
    UNION ALL
    SELECT 'prd_v2.ReportObjectType',              COUNT(*), 'Merge 6'   FROM prd_v2.ReportObjectType
    UNION ALL
    SELECT 'prd_v2.ReportObjectTag',               COUNT(*), 'Merge 8'   FROM prd_v2.ReportObjectTag
    UNION ALL
    SELECT 'prd_v2.ReportObjectTagMembership',     COUNT(*), 'Merge 9'   FROM prd_v2.ReportObjectTagMembership
    UNION ALL
    SELECT 'prd_v2.ReportObjectParameter',         COUNT(*), 'Merge 10'  FROM prd_v2.ReportObjectParameter
    UNION ALL
    SELECT 'prd_v2.ReportObjectAttachment',        COUNT(*), 'Merge 11'  FROM prd_v2.ReportObjectAttachment
    UNION ALL
    SELECT 'prd_v2.ReportObjectSubscription',      COUNT(*), 'Merge 12'  FROM prd_v2.ReportObjectSubscription
    UNION ALL
    SELECT 'prd_v2.ReportObjectRunData',           COUNT(*), 'RunData'   FROM prd_v2.ReportObjectRunData
    UNION ALL
    SELECT 'prd_v2.ReportObjectRunDataBridge',     COUNT(*), 'RunData'   FROM prd_v2.ReportObjectRunDataBridge
) t
ORDER BY t.Operation, t.TableName;

-- Enrichment checks
PRINT '';
PRINT '  Enrichment Checks:';

-- EpicReleased flagging
SELECT
    'Enrich 14 - EpicReleased' AS CheckName,
    SUM(CASE WHEN EpicReleased = 'Y' THEN 1 ELSE 0 END) AS FlaggedY,
    SUM(CASE WHEN EpicReleased IS NULL OR EpicReleased <> 'Y' THEN 1 ELSE 0 END) AS NotFlagged,
    COUNT(*) AS Total
FROM prd_v2.ReportObjects;

-- Hide Reports (stale)
SELECT
    'Enrich 15 - Hidden Reports' AS CheckName,
    SUM(CASE WHEN DefaultVisibilityYN = 'N' THEN 1 ELSE 0 END) AS Hidden,
    SUM(CASE WHEN DefaultVisibilityYN = 'Y' THEN 1 ELSE 0 END) AS Visible,
    COUNT(*) AS Total
FROM prd_v2.ReportObjects;

-- Orphan status
SELECT
    'Orphan Status' AS CheckName,
    SUM(CASE WHEN OrphanedReportObjectYN = 'Y' THEN 1 ELSE 0 END) AS Orphaned,
    SUM(CASE WHEN OrphanedReportObjectYN = 'N' THEN 1 ELSE 0 END) AS Active,
    COUNT(*) AS Total
FROM prd_v2.ReportObjects;

-- Distinct user count on ReportObjects (updated by usp_Atlas_RunData Phase 10)
-- Column: DistinctUsersPast12Months (rolling 12-month window)
SELECT
    'DistinctUsersPast12Months Population' AS CheckName,
    SUM(CASE WHEN DistinctUsersPast12Months > 0 THEN 1 ELSE 0 END) AS WithUsers,
    SUM(CASE WHEN DistinctUsersPast12Months IS NULL OR DistinctUsersPast12Months = 0 THEN 1 ELSE 0 END) AS NoUsers,
    COUNT(*) AS Total
FROM prd_v2.ReportObjects;
GO


-- =============================================================================
-- SECTION 8: CROSS-SYSTEM REFERENTIAL INTEGRITY
-- Verifies dependencies between Weeks 1-4 are intact.
-- =============================================================================
PRINT '';
PRINT '--- Section 8: Cross-System Referential Integrity ---';
PRINT '';

-- RunData → ReportObjects: BizKeys in run data should exist in master catalog
SELECT
    'RunData → ReportObjects FK' AS CheckName,
    COUNT(DISTINCT rd.ReportObjectBizKey) AS RunDataBizKeys,
    SUM(CASE WHEN ro.BizKey IS NOT NULL THEN 1 ELSE 0 END) AS MatchedInCatalog,
    SUM(CASE WHEN ro.BizKey IS NULL THEN 1 ELSE 0 END) AS UnmatchedBizKeys,
    CASE
        WHEN SUM(CASE WHEN ro.BizKey IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        WHEN CAST(SUM(CASE WHEN ro.BizKey IS NULL THEN 1 ELSE 0 END) * 100.0 
             / NULLIF(COUNT(DISTINCT rd.ReportObjectBizKey), 0) AS DECIMAL(5,1)) < 5.0 THEN 'WARNING'
        ELSE 'FAIL'
    END AS Status
FROM (SELECT DISTINCT ReportObjectBizKey FROM prd_v2.ReportObjectRunData) rd
LEFT JOIN prd_v2.ReportObjects ro ON rd.ReportObjectBizKey = ro.BizKey;

-- Users in RunData should resolve to ReportObjectUser
SELECT
    'RunData Users → User Table' AS CheckName,
    COUNT(DISTINCT rds.RunUserName) AS RunDataUsers,
    SUM(CASE WHEN u.UserName IS NOT NULL THEN 1 ELSE 0 END) AS MatchedUsers,
    SUM(CASE WHEN u.UserName IS NULL THEN 1 ELSE 0 END) AS UnmatchedUsers
FROM (SELECT DISTINCT RunUserName FROM stage_v2.ReportObjectRunDataStaging WHERE RunUserName IS NOT NULL) rds
LEFT JOIN stage_v2.ReportObjectUser u ON rds.RunUserName = u.UserName;

-- Hierarchy references → ReportObjects
SELECT
    'Hierarchy → ReportObjects' AS CheckName,
    (SELECT COUNT(DISTINCT ParentBizKey) FROM prd_v2.ReportObjectHierarchy) AS UniqueParents,
    (SELECT COUNT(DISTINCT ChildBizKey) FROM prd_v2.ReportObjectHierarchy) AS UniqueChildren,
    (SELECT COUNT(*) FROM prd_v2.ReportObjectHierarchy h
     WHERE NOT EXISTS (SELECT 1 FROM prd_v2.ReportObjects ro WHERE ro.BizKey = h.ParentBizKey)) AS OrphanedParents,
    (SELECT COUNT(*) FROM prd_v2.ReportObjectHierarchy h
     WHERE NOT EXISTS (SELECT 1 FROM prd_v2.ReportObjects ro WHERE ro.BizKey = h.ChildBizKey)) AS OrphanedChildren;

GO


-- =============================================================================
-- SECTION 9: ETL LOG REVIEW (Today's Week 4 runs)
-- =============================================================================
PRINT '';
PRINT '--- Section 9: ETL Log Review (Today) ---';
PRINT '';

SELECT
    PackageName,
    StepName,
    StartTime,
    EndTime,
    DATEDIFF(SECOND, StartTime, EndTime) AS DurationSec,
    RowsAffected,
    Status,
    ErrorMessage
FROM etl.Atlas_ETL_Log
WHERE PackageName IN ('ETL-RunData', 'ETL-Merge', 'atlas_pbi_events')
  AND CAST(StartTime AS DATE) = CAST(GETDATE() AS DATE)
ORDER BY StartTime;

-- Failure summary
SELECT
    'Failures Today' AS CheckName,
    COUNT(*) AS FailedSteps,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS Status
FROM etl.Atlas_ETL_Log
WHERE PackageName IN ('ETL-RunData', 'ETL-Merge', 'atlas_pbi_events')
  AND CAST(StartTime AS DATE) = CAST(GETDATE() AS DATE)
  AND Status = 'Failure';
GO


PRINT '';
PRINT '================================================================';
PRINT 'Week 4 Validation Complete';
PRINT '================================================================';
PRINT '';
PRINT 'Expected Results:';
PRINT '  Section 1 — All 13 RunData tables populated (0 rows = investigate)';
PRINT '  Section 2 — Row counts within 5% of SSIS production';
PRINT '  Section 3 — PBI events span ~28 days with daily coverage';
PRINT '  Section 4 — 100% valid BizKey format, zero duplicates';
PRINT '  Section 5 — Bridge rows resolve to known BizKeys';
PRINT '  Section 6 — All time dimension fields populated';
PRINT '  Section 7 — All 12 merge targets populated, enrichments applied';
PRINT '  Section 8 — Cross-system FKs resolve (< 5% orphans = warning)';
PRINT '  Section 9 — Zero failures in ETL log';
GO
