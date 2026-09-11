/*
================================================================================
Atlas ETL Suite - RunData & Power BI Raw Table DDL
================================================================================
Creates 8 raw_v2 tables for RunData extraction and Power BI activity events.
These tables are populated by:
  - atlas_rundata_extractor.py  → 5 Clarity/SlicerDicer tables
  - atlas_pbi_events.py         → 3 PBI tables

Then consumed by:
  - etl.usp_Atlas_RunData (Phases 1, 3-10)

Dependencies:
  - Schema: raw_v2 (created by 02_usp_Atlas_Setup.sql)
  - Python/pyodbc via atlas_rundata_extractor.py + atlas_pbi_events.py
  - NO LINKED SERVER REQUIRED (Pipeline B)

Tables created (columns match SSIS production destination tables):
  1. raw_v2.ClarityReportRunData      (RW_RPT_RUN_DATA + RW_RPT_RUN_VU_STAT)
  2. raw_v2.ClarityDashboardRunData   (METRIC_DATA_SUMMARIES + DAILY_DATA)
  3. raw_v2.SlicerDicerStatsHttp      (slicerdicer.Stats_Http — all 11 cols)
  4. raw_v2.SlicerDicerStatsQuery     (slicerdicer.Stats_Query — 3 cols)
  5. raw_v2.SlicerDicerStatsSaveLoad  (slicerdicer.Stats_SaveLoad — 6 cols)
  6. raw_v2.PbiActivityEvent          (Power BI activity log — current)
  7. raw_v2.PbiActivityEventJoined    (PBI events joined to Atlas objects)
  8. raw_v2.PbiActivityEventHistory   (PBI events archive)

Run this script on: Atlas_Staging
Run order: After 02_usp_Atlas_Setup.sql (needs raw_v2 schema)

Version: 1.0
Last Updated: March 2026
Scripted from: Atlas_Staging server, 3/13/2026
================================================================================
*/

USE Atlas_Staging;
GO

PRINT '=== RunData & Power BI Raw Table DDL ===';
PRINT 'Start Time: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
GO

-- =============================================================================
-- SECTION 1: CLARITY RUN DATA TABLES
-- Populated by atlas_rundata_extractor.py via pyodbc from EPICCLAPRD
-- =============================================================================

-- 1. Clarity Report Run Data (RW_RPT_RUN_DATA JOIN RW_RPT_RUN_VU_STAT)
--    Mirrors raw.[clarity_server-clarityreport-dbo-rw_rpt_run_data]
IF OBJECT_ID('raw_v2.ClarityReportRunData', 'U') IS NOT NULL
    DROP TABLE raw_v2.ClarityReportRunData;
GO

CREATE TABLE [raw_v2].[ClarityReportRunData] (
    [RUN_ID]              [numeric](18,0) NOT NULL,
    [REP_SETTINGS_ID]     [numeric](18,0) NULL,
    [SERVER_NODE_NAME]    [varchar](40)   NULL,
    [RUN_INSTANT]         [datetime]      NULL,
    [RUN_USER_ID]         [varchar](18)   NULL,
    [REPORT_START_INST]   [datetime]      NULL,
    [REPORT_END_INST]     [datetime]      NULL,
    [TOTAL_EXE_TIME]      [numeric](18,0) NULL,
    [REPORT_STATUS_C]     [int]           NULL,
    [RUN_NAME]            [varchar](200)  NULL,
    [SOURCE_REPORT_ID]    [numeric](18,0) NULL,
    [REPORT_RUN_TYPE_C]   [int]           NULL,
    [REPORT_TEMPLATE_ID]  [numeric](18,0) NULL
);
GO
PRINT 'Created raw_v2.ClarityReportRunData';
GO

-- 2. Clarity Dashboard Run Data (METRIC_DATA_SUMMARIES + DAILY_DATA)
--    Mirrors raw.[clarity_server-clarity-dashboard-run-data]
IF OBJECT_ID('raw_v2.ClarityDashboardRunData', 'U') IS NOT NULL
    DROP TABLE raw_v2.ClarityDashboardRunData;
GO

CREATE TABLE [raw_v2].[ClarityDashboardRunData] (
    [RunId]               [int]           NULL,
    [SourceServer]        [nvarchar](250) NULL,
    [SourceDB]            [nvarchar](250) NULL,
    [SourceTable]         [nvarchar](250) NULL,
    [Name]                [nvarchar](MAX) NULL,
    [ReportObjectType]    [nvarchar](300) NULL,
    [EpicMasterFile]      [nvarchar](3)   NULL,
    [EpicRecordID]        [nvarchar](100) NULL,
    [RunUserId]           [nvarchar](100) NULL,
    [RunStartTime]        [datetime]      NULL
);
GO
PRINT 'Created raw_v2.ClarityDashboardRunData';
GO

-- =============================================================================
-- SECTION 2: SLICER DICER TABLES
-- Populated by atlas_rundata_extractor.py via pyodbc from EPICCDWPRD
-- =============================================================================

-- 3. SlicerDicer HTTP Request Stats
--    Mirrors raw.[slicerdicer_server-slicerdicer-stats_http]
IF OBJECT_ID('raw_v2.SlicerDicerStatsHttp', 'U') IS NOT NULL
    DROP TABLE raw_v2.SlicerDicerStatsHttp;
GO

CREATE TABLE [raw_v2].[SlicerDicerStatsHttp] (
    [RequestId]             [nvarchar](100) NULL,
    [Url]                   [nvarchar](MAX) NULL,
    [UserId]                [nvarchar](100) NULL,
    [SessionId]             [nvarchar](100) NULL,
    [Instant]               [datetime2](7)  NULL,
    [Duration]              [time](7)       NULL,
    [ClientIp]              [nvarchar](100) NULL,
    [CacheHits]             [int]           NULL,
    [CacheMisses]           [int]           NULL,
    [RequestType]           [nvarchar](100) NULL,
    [NodeName]              [nvarchar](100) NULL
);
GO
PRINT 'Created raw_v2.SlicerDicerStatsHttp';
GO

-- 4. SlicerDicer Query Stats (only 3 columns used by production)
--    Mirrors raw.[slicerdicer_server-slicerdicer-stats_query]
IF OBJECT_ID('raw_v2.SlicerDicerStatsQuery', 'U') IS NOT NULL
    DROP TABLE raw_v2.SlicerDicerStatsQuery;
GO

CREATE TABLE [raw_v2].[SlicerDicerStatsQuery] (
    [HttpRequestId]         [nvarchar](100) NULL,
    [CompiledRecordId]      [nvarchar](100) NULL,
    [ModelId]               [nvarchar](100) NULL
);
GO
PRINT 'Created raw_v2.SlicerDicerStatsQuery';
GO

-- 5. SlicerDicer Save/Load Stats
--    Mirrors raw.[slicerdicer_server-slicerdicer-stats_saveload]
IF OBJECT_ID('raw_v2.SlicerDicerStatsSaveLoad', 'U') IS NOT NULL
    DROP TABLE raw_v2.SlicerDicerStatsSaveLoad;
GO

CREATE TABLE [raw_v2].[SlicerDicerStatsSaveLoad] (
    [Instant]               [datetime2](7)  NULL,
    [Duration]              [time](7)       NULL,
    [PopulationId]          [nvarchar](20)  NULL,
    [IsLoad]                [bit]           NULL,
    [HttpRequestId]         [nvarchar](100) NULL,
    [Identifier]            [bigint]        NULL
);
GO
PRINT 'Created raw_v2.SlicerDicerStatsSaveLoad';
GO

-- =============================================================================
-- SECTION 3: POWER BI ACTIVITY EVENT TABLES
-- Populated by atlas_pbi_events.py via Power BI REST API
-- =============================================================================

-- 6. PBI Activity Events (current extraction)
IF OBJECT_ID('raw_v2.PbiActivityEvent', 'U') IS NOT NULL
    DROP TABLE raw_v2.PbiActivityEvent;
GO

CREATE TABLE [raw_v2].[PbiActivityEvent] (
    [Id]                  [varchar](200)  NULL,
    [CreationTime]        [datetime]      NULL,
    [Activity]            [varchar](200)  NULL,
    [UserId]              [varchar](200)  NULL,
    [ReportId]            [varchar](100)  NULL,
    [ReportName]          [varchar](500)  NULL,
    [WorkspaceName]       [varchar](500)  NULL,
    [WorkspaceId]         [varchar](100)  NULL,
    [AppReportId]         [varchar](100)  NULL,
    [AppName]             [varchar](500)  NULL,
    [CapacityId]          [varchar](100)  NULL,
    [CapacityName]        [varchar](500)  NULL,
    [ObjectId]            [varchar](100)  NULL,
    [DatasetId]           [varchar](100)  NULL,
    [DatasetName]         [varchar](500)  NULL,
    [ReportType]          [varchar](100)  NULL,
    [RequestId]           [varchar](200)  NULL,
    [ClientIP]            [varchar](50)   NULL,
    [UserAgent]           [varchar](500)  NULL,
    [DistributionMethod]  [varchar](100)  NULL,
    [ConsumptionMethod]   [varchar](100)  NULL,
    [UserUPN]             [varchar](255)  NULL,
    [ETL_LoadDate]        [datetime]      NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.PbiActivityEvent';
GO

-- 7. PBI Activity Events Joined (enriched with Atlas BizKey)
IF OBJECT_ID('raw_v2.PbiActivityEventJoined', 'U') IS NOT NULL
    DROP TABLE raw_v2.PbiActivityEventJoined;
GO

CREATE TABLE [raw_v2].[PbiActivityEventJoined] (
    [Id]                    [varchar](200)   NULL,
    [CreationTime]          [datetime]       NULL,
    [Activity]              [varchar](200)   NULL,
    [UserId]                [varchar](200)   NULL,
    [ReportId]              [varchar](100)   NULL,
    [ReportName]            [varchar](500)   NULL,
    [WorkspaceName]         [varchar](500)   NULL,
    [WorkspaceId]           [varchar](100)   NULL,
    [DatasetId]             [varchar](100)   NULL,
    [DatasetName]           [varchar](500)   NULL,
    [ReportType]            [varchar](100)   NULL,
    [UserUPN]               [varchar](255)   NULL,
    [ReportObjectBizKey]    [nvarchar](500)  NULL,
    [ETL_LoadDate]          [datetime]       NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.PbiActivityEventJoined';
GO

-- 8. PBI Activity Events History (archive with PK for deduplication)
IF OBJECT_ID('raw_v2.PbiActivityEventHistory', 'U') IS NOT NULL
    DROP TABLE raw_v2.PbiActivityEventHistory;
GO

CREATE TABLE [raw_v2].[PbiActivityEventHistory] (
    [Id]                  [varchar](200)  NOT NULL,
    [CreationTime]        [datetime]      NULL,
    [Activity]            [varchar](200)  NULL,
    [UserId]              [varchar](200)  NULL,
    [ReportId]            [varchar](100)  NULL,
    [ReportName]          [varchar](500)  NULL,
    [WorkspaceName]       [varchar](500)  NULL,
    [WorkspaceId]         [varchar](100)  NULL,
    [DatasetId]           [varchar](100)  NULL,
    [DatasetName]         [varchar](500)  NULL,
    [ReportType]          [varchar](100)  NULL,
    [InsertedDate]        [datetime]      NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [PK_raw_v2_PbiActivityEventHistory] PRIMARY KEY CLUSTERED ([Id] ASC)
);
GO
PRINT 'Created raw_v2.PbiActivityEventHistory';
GO

-- =============================================================================
-- VERIFICATION
-- =============================================================================

PRINT '';
PRINT '=== RunData & PBI DDL Complete ===';
PRINT 'End Time: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
PRINT '';

SELECT t.TableName, t.RowCnt
FROM (
    SELECT 'raw_v2.ClarityReportRunData'     AS TableName, COUNT(*) AS RowCnt FROM raw_v2.ClarityReportRunData
    UNION ALL
    SELECT 'raw_v2.ClarityDashboardRunData',  COUNT(*) FROM raw_v2.ClarityDashboardRunData
    UNION ALL
    SELECT 'raw_v2.SlicerDicerStatsHttp',     COUNT(*) FROM raw_v2.SlicerDicerStatsHttp
    UNION ALL
    SELECT 'raw_v2.SlicerDicerStatsQuery',    COUNT(*) FROM raw_v2.SlicerDicerStatsQuery
    UNION ALL
    SELECT 'raw_v2.SlicerDicerStatsSaveLoad', COUNT(*) FROM raw_v2.SlicerDicerStatsSaveLoad
    UNION ALL
    SELECT 'raw_v2.PbiActivityEvent',         COUNT(*) FROM raw_v2.PbiActivityEvent
    UNION ALL
    SELECT 'raw_v2.PbiActivityEventJoined',   COUNT(*) FROM raw_v2.PbiActivityEventJoined
    UNION ALL
    SELECT 'raw_v2.PbiActivityEventHistory',  COUNT(*) FROM raw_v2.PbiActivityEventHistory
) t
ORDER BY t.TableName;

PRINT 'Tables created: 8 (5 Clarity/SlicerDicer + 3 PBI)';
GO
