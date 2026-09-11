-- ============================================================
-- Atlas Pipeline B — Pre-E2E Preflight Check
-- Run in SSMS before every E2E pipeline execution.
-- Total runtime: <30 seconds
-- ============================================================
--
-- Purpose:
--   Quick sanity check before kicking off an end-to-end pipeline run.
--   Catches common pre-flight issues that would otherwise stall or
--   fail the pipeline mid-execution:
--     • Open transactions or blocking sessions
--     • Production target tables that have grown into MERGE-stall risk
--     • Missing critical indexes after a database refresh
--     • Stale stored procedure deployments
--     • Bridge table specifically — flag if >1M rows
--
-- Usage:
--   1. Open SSMS connected to 10.247.4.56\SQL126 → Atlas_Staging
--   2. Run this script (F5)
--   3. Review all 5 result sets and the PRINT messages
--   4. If anything is flagged ⚠️ HIGH or shows blocking, investigate
--      before launching the pipeline
--
-- Maintainer notes:
--   This script is read-only — no INSERT/UPDATE/DELETE/MERGE statements.
--   All COUNT(*) queries use WITH (NOLOCK) to avoid contending with
--   any in-flight pipeline activity.
-- ============================================================

USE Atlas_Staging;
GO

PRINT '';
PRINT '================================================================';
PRINT 'ATLAS PIPELINE B — PRE-E2E PREFLIGHT CHECK';
PRINT '  Server:   ' + @@SERVERNAME;
PRINT '  Database: ' + DB_NAME();
PRINT '  Run at:   ' + CONVERT(VARCHAR(30), GETDATE(), 120);
PRINT '================================================================';


-- ============================================================
-- CHECK 1 — Open transactions / blocking sessions
-- ============================================================
-- Lists any active session in Atlas_Staging that's currently waiting,
-- blocked, or holding a long-running request. A blocking_session_id > 0
-- means the session is being blocked by another. Long wait_seconds on
-- LCK_M_* wait types indicate lock contention.
-- ============================================================

PRINT '';
PRINT '----------------------------------------------------------------';
PRINT 'CHECK 1: Open transactions / blocking';
PRINT '----------------------------------------------------------------';

SELECT session_id, status, blocking_session_id, wait_type,
       wait_time/1000 AS wait_seconds, last_request_start_time
FROM sys.dm_exec_requests
WHERE database_id = DB_ID('Atlas_Staging') AND session_id > 50;


-- ============================================================
-- CHECK 2 — Target table row counts (MERGE stall risk)
-- ============================================================
-- Flags any prd_v2 target table that's grown to a size where MERGE
-- operations may stall without proper index support. The Phase 12
-- bridge stall on the 8M-row prd_v2.ReportObjectRunDataBridge is the
-- canonical example — prevent recurrences by reviewing this output
-- before each E2E run.
--
-- Risk thresholds:
--   >1M  rows: HIGH   — verify indexes mirror production
--   >100K rows: MEDIUM — usually OK if Setup ran cleanly
--   ≤100K rows: OK     — no concern
-- ============================================================

PRINT '';
PRINT '----------------------------------------------------------------';
PRINT 'CHECK 2: Target table row counts (MERGE stall risk)';
PRINT '----------------------------------------------------------------';

SELECT TableName, RowCount,
    CASE WHEN RowCount > 1000000 THEN '⚠️ HIGH — potential MERGE stall risk'
         WHEN RowCount > 100000 THEN '🟡 MEDIUM'
         ELSE '✅ OK' END AS Risk
FROM (
    SELECT 'prd_v2.ReportObjectRunData' AS TableName, COUNT(*) AS RowCount FROM prd_v2.ReportObjectRunData WITH (NOLOCK)
    UNION ALL SELECT 'prd_v2.ReportObjectRunDataBridge', COUNT(*) FROM prd_v2.ReportObjectRunDataBridge WITH (NOLOCK)
    UNION ALL SELECT 'prd_v2.ReportObjects', COUNT(*) FROM prd_v2.ReportObjects WITH (NOLOCK)
    UNION ALL SELECT 'prd_v2.[User]', COUNT(*) FROM prd_v2.[User] WITH (NOLOCK)
    UNION ALL SELECT 'prd_v2.ReportObjectTags', COUNT(*) FROM prd_v2.ReportObjectTags WITH (NOLOCK)
    UNION ALL SELECT 'prd_v2.ReportObjectParameters', COUNT(*) FROM prd_v2.ReportObjectParameters WITH (NOLOCK)
    UNION ALL SELECT 'prd_v2.ReportObjectQuery', COUNT(*) FROM prd_v2.ReportObjectQuery WITH (NOLOCK)
    UNION ALL SELECT 'prd_v2.ReportObjectHierarchy', COUNT(*) FROM prd_v2.ReportObjectHierarchy WITH (NOLOCK)
    UNION ALL SELECT 'prd_v2.ReportObjectTagMemberships', COUNT(*) FROM prd_v2.ReportObjectTagMemberships WITH (NOLOCK)
    UNION ALL SELECT 'prd_v2.UserGroups', COUNT(*) FROM prd_v2.UserGroups WITH (NOLOCK)
    UNION ALL SELECT 'prd_v2.UserGroupsMembership', COUNT(*) FROM prd_v2.UserGroupsMembership WITH (NOLOCK)
) t
ORDER BY RowCount DESC;


-- ============================================================
-- CHECK 3 — Critical index verification
-- ============================================================
-- Lists every nonclustered index on raw_v2, stage_v2, and prd_v2
-- tables. Cross-reference against expected indexes from
-- 02_usp_Atlas_Setup.sql to confirm Setup ran successfully and
-- nothing was dropped manually after the last deployment.
--
-- Pay special attention to:
--   • IX_prd_v2_ReportObjects_BizKey
--   • IX_prd_v2_ReportObjects_EpicRecordID_MasterFile
--   • IX_prd_v2_RunDataBridge_RunId
--   • IX_prd_v2_RunDataBridge_ReportObjectId_Inherited
--   • IX_prd_v2_RunDataBridge_ReportObjectId
--   • IX_ReportObjectsStaging_Epic
--   • IX_ReportObjectsStaging_BizKey
--   • IX_HierarchyStaging_ChildBizKey
-- ============================================================

PRINT '';
PRINT '----------------------------------------------------------------';
PRINT 'CHECK 3: Critical index verification';
PRINT '----------------------------------------------------------------';

SELECT
    s.name + '.' + t.name AS TableName,
    i.name AS IndexName
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name IN ('prd_v2', 'stage_v2', 'raw_v2')
  AND i.name IS NOT NULL AND i.type > 0
ORDER BY s.name, t.name, i.name;


-- ============================================================
-- CHECK 4 — SP deployment timestamps
-- ============================================================
-- Confirms which version of each etl.* stored procedure is currently
-- deployed to the database. Compare modify_date against your most
-- recent local SP file deployment to detect:
--   • Forgotten ALTER PROCEDURE redeployments after a fix
--   • Drift between source code and live database
--
-- A common Pipeline B failure mode is editing 02_usp_Atlas_Setup.sql
-- locally and forgetting to re-execute it in SSMS. Setup recreates
-- stage_v2 tables on every run, so any manual ALTER TABLE on those
-- tables is wiped — but the SP itself only updates when re-deployed.
-- ============================================================

PRINT '';
PRINT '----------------------------------------------------------------';
PRINT 'CHECK 4: SP deployment timestamps';
PRINT '----------------------------------------------------------------';

SELECT
    SCHEMA_NAME(schema_id) + '.' + name AS SPName,
    modify_date AS LastDeployed,
    DATEDIFF(MINUTE, modify_date, GETDATE()) AS MinutesAgo
FROM sys.procedures
WHERE SCHEMA_NAME(schema_id) = 'etl'
ORDER BY name;


-- ============================================================
-- CHECK 5 — Bridge table warning
-- ============================================================
-- Standalone check for prd_v2.ReportObjectRunDataBridge — the table
-- whose 8M-row size triggered the original Phase 12 stall. If this
-- table has grown past 1M rows and the supporting indexes are
-- missing, Phase 12 will scan the heap for every staging row.
--
-- Always verify the 3 RunDataBridge indexes exist (per CHECK 3
-- output) before launching an E2E with a large bridge table.
-- ============================================================

PRINT '';
PRINT '----------------------------------------------------------------';
PRINT 'CHECK 5: Bridge table warning';
PRINT '----------------------------------------------------------------';

DECLARE @BridgeCount INT = (SELECT COUNT(*) FROM prd_v2.ReportObjectRunDataBridge WITH (NOLOCK));
DECLARE @RunDataCount INT = (SELECT COUNT(*) FROM prd_v2.ReportObjectRunData WITH (NOLOCK));
PRINT 'prd_v2.ReportObjectRunDataBridge: ' + CAST(@BridgeCount AS VARCHAR(20)) + ' rows';
PRINT 'prd_v2.ReportObjectRunData: ' + CAST(@RunDataCount AS VARCHAR(20)) + ' rows';
IF @BridgeCount > 1000000
    PRINT '⚠️ Bridge table >1M rows. Ensure IX_prd_v2_RunDataBridge_RunId and IX_prd_v2_RunDataBridge_ReportObjectId indexes exist before running E2E.';


PRINT '';
PRINT '================================================================';
PRINT 'PREFLIGHT CHECK COMPLETE';
PRINT '================================================================';
PRINT '';
GO
