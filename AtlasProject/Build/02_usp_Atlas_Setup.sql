/*
================================================================================
Atlas ETL Suite - usp_Atlas_Setup (V2 Schema)
================================================================================
Migrated from: ETL-Setup SSIS Package
Purpose: Drop and recreate the 12 staging tables (clean slate for ETL run)

*** UPDATED: Uses stage_v2 schema to avoid conflicts with live SSIS process ***

Original SSIS Package Details:
  - Created: June 2021
  - Build Count: 36
  - Components: 1 SQL Task, 0 Data Flows, 1 Connection Manager
  - Complexity: Low

Run this script on: Atlas_Staging
This script REPLACES the previous 02_usp_Atlas_Setup.sql procedure definition.

Version: 2.7
Last Updated: April 2026
v2.7 2026-04-09:
  - C-4/U-1: Username NVARCHAR(MAX) → NVARCHAR(256) on
    stage_v2.ReportObjectUser. Index IX_prd_v2_User_Username added on
    prd_v2.[User].Username.
================================================================================
*/

USE Atlas_Staging;
GO

-- ══════════════════════════════════════════════════════════════════════════════
-- 1. CREATE SCHEMAS (if not exist)
-- ══════════════════════════════════════════════════════════════════════════════

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stage_v2')
BEGIN
    EXEC('CREATE SCHEMA stage_v2 AUTHORIZATION dbo');
    PRINT 'Created schema: stage_v2';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'raw_v2')
BEGIN
    EXEC('CREATE SCHEMA raw_v2 AUTHORIZATION dbo');
    PRINT 'Created schema: raw_v2';
END
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 2. CREATE/REPLACE STORED PROCEDURE
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('etl.usp_Atlas_Setup', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_Setup;
GO

CREATE PROCEDURE etl.usp_Atlas_Setup
    @ExecutionID        UNIQUEIDENTIFIER = NULL,    -- Pass from orchestrator, or NULL to auto-generate
    @UseTruncate        BIT = 0,                     -- 1 = TRUNCATE existing tables, 0 = DROP/CREATE
    @RaiseErrorOnFail   BIT = 1                      -- 1 = THROW on failure, 0 = log and continue
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @PackageName NVARCHAR(100) = 'ETL-Setup';
    DECLARE @LogID BIGINT;
    DECLARE @StepSequence INT = 0;
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @TableCount INT = 0;
    
    IF @ExecutionID IS NULL
        SET @ExecutionID = NEWID();

    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Package Start',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- ──────────────────────────────────────────────────────────────────────
        -- Table 1: ReportObjectsStaging
        -- ──────────────────────────────────────────────────────────────────────
        IF @UseTruncate = 1 AND OBJECT_ID('stage_v2.ReportObjectsStaging', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectsStaging;

            -- Section C: Stage 20 covering index — create if missing (table persists on TRUNCATE path)
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ReportObjectsStaging_Epic' AND object_id = OBJECT_ID('stage_v2.ReportObjectsStaging'))
                CREATE NONCLUSTERED INDEX IX_ReportObjectsStaging_Epic
                    ON stage_v2.ReportObjectsStaging (EpicMasterFile, EpicRecordID)
                    INCLUDE (BizKey, SourceServer);

            -- BizKey lookup index — used by PostProcessing orphan-flag EXISTS subqueries
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ReportObjectsStaging_BizKey' AND object_id = OBJECT_ID('stage_v2.ReportObjectsStaging'))
                CREATE NONCLUSTERED INDEX IX_ReportObjectsStaging_BizKey
                    ON stage_v2.ReportObjectsStaging (BizKey);
        END
        ELSE
        BEGIN
            IF OBJECT_ID('stage_v2.ReportObjectsStaging', 'U') IS NOT NULL
                DROP TABLE stage_v2.ReportObjectsStaging;
            
            CREATE TABLE stage_v2.ReportObjectsStaging (
                BizKey                  NVARCHAR(500) NULL,
                ObjectName              NVARCHAR(500) NULL,
                ObjectType              NVARCHAR(100) NULL,
                ObjectPath              NVARCHAR(1000) NULL,
                ObjectDescription       NVARCHAR(MAX) NULL,
                SourceSystem            NVARCHAR(50) NULL,
                SourceServer            NVARCHAR(200) NULL,
                CreatedDate             DATETIME NULL,
                ModifiedDate            DATETIME NULL,
                CreatedBy               NVARCHAR(200) NULL,
                ModifiedBy              NVARCHAR(200) NULL,
                IsHidden                BIT NULL,
                ObjectURL               NVARCHAR(1000) NULL,
                ParentPath              NVARCHAR(1000) NULL,
                RawDefinition           NVARCHAR(MAX) NULL,
                ExtractDate             DATETIME NOT NULL DEFAULT GETDATE(),
                EpicMasterFile          NVARCHAR(10) NULL,
                EpicRecordID            NUMERIC(18,0) NULL,
                [Availability]          NVARCHAR(MAX)   NULL,
                [EpicReportTemplateId]  NUMERIC(18,0)   NULL
            );

            -- Section C: Stage 20 covering index — joined in all 9 branches
            CREATE NONCLUSTERED INDEX IX_ReportObjectsStaging_Epic
                ON stage_v2.ReportObjectsStaging (EpicMasterFile, EpicRecordID)
                INCLUDE (BizKey, SourceServer);

            -- BizKey lookup index — used by PostProcessing orphan-flag EXISTS subqueries
            CREATE NONCLUSTERED INDEX IX_ReportObjectsStaging_BizKey
                ON stage_v2.ReportObjectsStaging (BizKey);
        END
        SET @TableCount = @TableCount + 1;
        
        -- ──────────────────────────────────────────────────────────────────────
        -- Table 2: ReportObjectUser
        -- Production-aligned staging for Atlas_Prd.dbo.User columns.
        -- Populated by usp_Atlas_LDAP Step 1. Consumed by Step 4 MERGE.
        -- Fullname_calc, Firstname_calc, ProfilePhoto, FilterValue, LastLogin,
        -- Base are app-populated and are NOT staged here.
        -- ──────────────────────────────────────────────────────────────────────
        IF @UseTruncate = 1 AND OBJECT_ID('stage_v2.ReportObjectUser', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectUser;
        END
        ELSE
        BEGIN
            IF OBJECT_ID('stage_v2.ReportObjectUser', 'U') IS NOT NULL
                DROP TABLE stage_v2.ReportObjectUser;

            CREATE TABLE stage_v2.ReportObjectUser (
                UserID          INT IDENTITY(1,1) NOT NULL,
                Username        NVARCHAR(256) NOT NULL,
                EmployeeID      NVARCHAR(MAX) NULL,
                AccountName     NVARCHAR(MAX) NULL,
                DisplayName     NVARCHAR(MAX) NULL,
                FullName        NVARCHAR(MAX) NULL,
                FirstName       NVARCHAR(MAX) NULL,
                LastName        NVARCHAR(MAX) NULL,
                Department      NVARCHAR(MAX) NULL,
                Title           NVARCHAR(MAX) NULL,
                Phone           NVARCHAR(MAX) NULL,
                Email           NVARCHAR(MAX) NULL,
                EpicId          NVARCHAR(MAX) NULL,
                LastLoadDate    DATETIME NULL,
                ExtractDate     DATETIME NOT NULL DEFAULT GETDATE(),
                CONSTRAINT PK_stage_v2_ReportObjectUser PRIMARY KEY CLUSTERED (UserID)
            );
        END
        SET @TableCount = @TableCount + 1;
        
        -- ──────────────────────────────────────────────────────────────────────
        -- Table 3: ReportObjectUserGroups
        -- ──────────────────────────────────────────────────────────────────────
        IF @UseTruncate = 1 AND OBJECT_ID('stage_v2.ReportObjectUserGroups', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectUserGroups;
        END
        ELSE
        BEGIN
            IF OBJECT_ID('stage_v2.ReportObjectUserGroups', 'U') IS NOT NULL
                DROP TABLE stage_v2.ReportObjectUserGroups;
            
            CREATE TABLE stage_v2.ReportObjectUserGroups (
                GroupId                 INT IDENTITY(1,1) NOT NULL,
                GroupName               NVARCHAR(200) NULL,
                GroupDescription        NVARCHAR(500) NULL,
                GroupType               NVARCHAR(50) NULL,
                SourceSystem            NVARCHAR(50) NULL,
                ExtractDate             DATETIME NOT NULL DEFAULT GETDATE(),
                EpicId                  NVARCHAR(MAX) NULL,
                AccountName             NVARCHAR(MAX) NULL,
                GroupEmail              NVARCHAR(MAX) NULL,
                CONSTRAINT PK_stage_v2_ReportObjectUserGroups PRIMARY KEY CLUSTERED (GroupId)
            );
        END
        SET @TableCount = @TableCount + 1;
        
        -- ──────────────────────────────────────────────────────────────────────
        -- Table 4: ReportObjectRunDataStaging
        -- ──────────────────────────────────────────────────────────────────────
        IF @UseTruncate = 1 AND OBJECT_ID('stage_v2.ReportObjectRunDataStaging', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectRunDataStaging;
        END
        ELSE
        BEGIN
            IF OBJECT_ID('stage_v2.ReportObjectRunDataStaging', 'U') IS NOT NULL
                DROP TABLE stage_v2.ReportObjectRunDataStaging;
            
            CREATE TABLE stage_v2.ReportObjectRunDataStaging (
                ReportObjectBizKey      NVARCHAR(500) NULL,
                RunUserName             NVARCHAR(200) NULL,
                RunStartTime            DATETIME NULL,
                RunYear                 INT NULL,
                RunMonth                INT NULL,
                RunDay                  INT NULL,
                RunHour                 INT NULL,
                RunDurationMs           INT NULL,
                [RowCount]              INT NULL,
                Status                  NVARCHAR(50) NULL,
                SourceSystem            NVARCHAR(50) NULL,
                ExtractDate             DATETIME NOT NULL DEFAULT GETDATE()
            );
        END
        SET @TableCount = @TableCount + 1;
        
        -- ──────────────────────────────────────────────────────────────────────
        -- Table 6: ReportObjectHierarchyStaging
        -- ──────────────────────────────────────────────────────────────────────
        IF @UseTruncate = 1 AND OBJECT_ID('stage_v2.ReportObjectHierarchyStaging', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectHierarchyStaging;

            -- ChildBizKey lookup index — used by RunData Phase 9 rollup join
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_HierarchyStaging_ChildBizKey' AND object_id = OBJECT_ID('stage_v2.ReportObjectHierarchyStaging'))
                CREATE NONCLUSTERED INDEX IX_HierarchyStaging_ChildBizKey
                    ON stage_v2.ReportObjectHierarchyStaging (ChildBizKey);
        END
        ELSE
        BEGIN
            IF OBJECT_ID('stage_v2.ReportObjectHierarchyStaging', 'U') IS NOT NULL
                DROP TABLE stage_v2.ReportObjectHierarchyStaging;

            CREATE TABLE stage_v2.ReportObjectHierarchyStaging (
                ParentBizKey            NVARCHAR(500) NULL,
                ChildBizKey             NVARCHAR(500) NULL,
                RelationshipType        NVARCHAR(50) NULL,
                SourceSystem            NVARCHAR(50) NULL,
                Line                    INT NULL,
                ExtractDate             DATETIME NOT NULL DEFAULT GETDATE()
            );

            -- ChildBizKey lookup index — used by RunData Phase 9 rollup join
            CREATE NONCLUSTERED INDEX IX_HierarchyStaging_ChildBizKey
                ON stage_v2.ReportObjectHierarchyStaging (ChildBizKey);
        END
        SET @TableCount = @TableCount + 1;
        
        -- ──────────────────────────────────────────────────────────────────────
        -- Table 5: TableReferenceStaging
        -- ──────────────────────────────────────────────────────────────────────
        IF @UseTruncate = 1 AND OBJECT_ID('stage_v2.TableReferenceStaging', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE stage_v2.TableReferenceStaging;
        END
        ELSE
        BEGIN
            IF OBJECT_ID('stage_v2.TableReferenceStaging', 'U') IS NOT NULL
                DROP TABLE stage_v2.TableReferenceStaging;
            
            CREATE TABLE stage_v2.TableReferenceStaging (
                BizKey                  NVARCHAR(500) NULL,
                ReferencedTable         NVARCHAR(500) NULL,
                ReferencedSchema        NVARCHAR(200) NULL,
                ReferencedDatabase      NVARCHAR(200) NULL,
                ReferenceType           NVARCHAR(50) NULL,
                ExtractDate             DATETIME NOT NULL DEFAULT GETDATE()
            );
        END
        SET @TableCount = @TableCount + 1;
        
        -- ──────────────────────────────────────────────────────────────────────
        -- Table 12: ReportObjectUserGroupMembers
        -- Used by usp_Atlas_LDAP Step 3 (Pipeline B) for AD group memberships
        -- ──────────────────────────────────────────────────────────────────────
        IF @UseTruncate = 1 AND OBJECT_ID('stage_v2.ReportObjectUserGroupMembers', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectUserGroupMembers;
        END
        ELSE
        BEGIN
            IF OBJECT_ID('stage_v2.ReportObjectUserGroupMembers', 'U') IS NOT NULL
                DROP TABLE stage_v2.ReportObjectUserGroupMembers;
            
            CREATE TABLE stage_v2.ReportObjectUserGroupMembers (
                GroupName               NVARCHAR(200) NULL,
                MemberName              NVARCHAR(200) NULL,
                MemberEmail             NVARCHAR(320) NULL,
                MemberType              NVARCHAR(50) NULL,
                SourceSystem            NVARCHAR(50) NULL,
                ExtractDate             DATETIME NOT NULL DEFAULT GETDATE(),
                GroupType               NVARCHAR(50) NULL,
                EpicId                  NVARCHAR(MAX) NULL
            );
        END
        SET @TableCount = @TableCount + 1;

        -- ──────────────────────────────────────────────────────────────────────
        -- Table 13: ReportObjectRunDataParentStaging
        -- Run data rolled up to parent reports via hierarchy
        -- ──────────────────────────────────────────────────────────────────────
        IF @UseTruncate = 1 AND OBJECT_ID('stage_v2.ReportObjectRunDataParentStaging', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectRunDataParentStaging;
        END
        ELSE
        BEGIN
            IF OBJECT_ID('stage_v2.ReportObjectRunDataParentStaging', 'U') IS NOT NULL
                DROP TABLE stage_v2.ReportObjectRunDataParentStaging;

            CREATE TABLE stage_v2.ReportObjectRunDataParentStaging (
                ReportObjectBizKey      NVARCHAR(500) NULL,
                RunUserName             NVARCHAR(200) NULL,
                RunStartTime            DATETIME NULL,
                RunYear                 INT NULL,
                RunMonth                INT NULL,
                RunDay                  INT NULL,
                RunHour                 INT NULL,
                RunDurationMs           INT NULL,
                SourceSystem            NVARCHAR(50) NULL,
                ExtractDate             DATETIME NOT NULL DEFAULT GETDATE()
            );
        END
        SET @TableCount = @TableCount + 1;

        -- ──────────────────────────────────────────────────────────────────────
        -- Table 14: ReportObjectRunDataBridgeStaging
        -- Individual run records linking a report object BizKey to a RunId hash.
        -- Matches SSIS stage.ReportObjectRunDataBridgeStaging layout.
        -- Populated by usp_Atlas_RunData Phase 11 (direct Inherited=0, parent Inherited=1).
        -- ──────────────────────────────────────────────────────────────────────
        IF @UseTruncate = 1 AND OBJECT_ID('stage_v2.ReportObjectRunDataBridgeStaging', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectRunDataBridgeStaging;
        END
        ELSE
        BEGIN
            IF OBJECT_ID('stage_v2.ReportObjectRunDataBridgeStaging', 'U') IS NOT NULL
                DROP TABLE stage_v2.ReportObjectRunDataBridgeStaging;

            CREATE TABLE stage_v2.ReportObjectRunDataBridgeStaging (
                ReportObjectBizKey      NVARCHAR(500) NOT NULL,
                RunId                   NVARCHAR(450) NOT NULL,
                Runs                    INT NOT NULL DEFAULT 1,
                Inherited               INT NOT NULL DEFAULT 0,
                ExtractDate             DATETIME NOT NULL DEFAULT GETDATE()
            );
        END
        SET @TableCount = @TableCount + 1;

        -- ──────────────────────────────────────────────────────────────────────
        -- Table 15: ReportObjectQueryStaging
        -- Used by Merge 5 (query text / SQL definitions)
        -- ──────────────────────────────────────────────────────────────────────
        IF @UseTruncate = 1 AND OBJECT_ID('stage_v2.ReportObjectQueryStaging', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectQueryStaging;
        END
        ELSE
        BEGIN
            IF OBJECT_ID('stage_v2.ReportObjectQueryStaging', 'U') IS NOT NULL
                DROP TABLE stage_v2.ReportObjectQueryStaging;

            CREATE TABLE stage_v2.ReportObjectQueryStaging (
                BizKey                  NVARCHAR(500) NULL,
                QueryText               NVARCHAR(MAX) NULL,
                QueryType               NVARCHAR(50) NULL,
                ExtractDate             DATETIME NOT NULL DEFAULT GETDATE()
            );
        END
        SET @TableCount = @TableCount + 1;

        -- ──────────────────────────────────────────────────────────────────────
        -- Table 16: ReportObjectTagsStaging
        -- Used by Merge 8 (distinct tag names)
        -- ──────────────────────────────────────────────────────────────────────
        IF @UseTruncate = 1 AND OBJECT_ID('stage_v2.ReportObjectTagsStaging', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectTagsStaging;
        END
        ELSE
        BEGIN
            IF OBJECT_ID('stage_v2.ReportObjectTagsStaging', 'U') IS NOT NULL
                DROP TABLE stage_v2.ReportObjectTagsStaging;

            CREATE TABLE stage_v2.ReportObjectTagsStaging (
                TagName                 NVARCHAR(200) NULL,
                ExtractDate             DATETIME NOT NULL DEFAULT GETDATE(),
                TagID                   NUMERIC(18,0) NULL
            );
        END
        SET @TableCount = @TableCount + 1;

        -- ──────────────────────────────────────────────────────────────────────
        -- Table 17: ReportObjectTagMembershipsStaging
        -- Used by Merge 9 (report-to-tag mappings)
        -- ──────────────────────────────────────────────────────────────────────
        IF @UseTruncate = 1 AND OBJECT_ID('stage_v2.ReportObjectTagMembershipsStaging', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectTagMembershipsStaging;
        END
        ELSE
        BEGIN
            IF OBJECT_ID('stage_v2.ReportObjectTagMembershipsStaging', 'U') IS NOT NULL
                DROP TABLE stage_v2.ReportObjectTagMembershipsStaging;

            CREATE TABLE stage_v2.ReportObjectTagMembershipsStaging (
                BizKey                  NVARCHAR(500) NULL,
                TagName                 NVARCHAR(200) NULL,
                ExtractDate             DATETIME NOT NULL DEFAULT GETDATE()
            );
        END
        SET @TableCount = @TableCount + 1;

        -- ──────────────────────────────────────────────────────────────────────
        -- Table 18: ReportObjectParametersStaging
        -- Used by Merge 10 (report parameters)
        -- ──────────────────────────────────────────────────────────────────────
        IF @UseTruncate = 1 AND OBJECT_ID('stage_v2.ReportObjectParametersStaging', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectParametersStaging;
        END
        ELSE
        BEGIN
            IF OBJECT_ID('stage_v2.ReportObjectParametersStaging', 'U') IS NOT NULL
                DROP TABLE stage_v2.ReportObjectParametersStaging;

            CREATE TABLE stage_v2.ReportObjectParametersStaging (
                BizKey                  NVARCHAR(500) NULL,
                ParameterName           NVARCHAR(200) NULL,
                IntraParameterLogic     NVARCHAR(MAX) NULL,
                Operator                NVARCHAR(MAX) NULL,
                DefaultValue            NVARCHAR(MAX) NULL,
                ExtractDate             DATETIME NOT NULL DEFAULT GETDATE(),
                EpicRecordID            NUMERIC(18,0) NULL,
                EpicMasterFile          NVARCHAR(3) NULL
            );
        END
        SET @TableCount = @TableCount + 1;

        -- ──────────────────────────────────────────────────────────────────────
        -- Table 21: ReportObjectGroupsMemberships
        -- Pipeline B equivalent of SSIS stage.ReportObjectGroupsMemberships.
        -- Populated by usp_Atlas_Clarity, consumed by usp_Atlas_Merge (Merge 2).
        -- ──────────────────────────────────────────────────────────────────────
        IF @UseTruncate = 1 AND OBJECT_ID('stage_v2.ReportObjectGroupsMemberships', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectGroupsMemberships;
        END
        ELSE
        BEGIN
            IF OBJECT_ID('stage_v2.ReportObjectGroupsMemberships', 'U') IS NOT NULL
                DROP TABLE stage_v2.ReportObjectGroupsMemberships;

            CREATE TABLE stage_v2.ReportObjectGroupsMemberships (
                BizKey                  NVARCHAR(500) NULL,
                GroupId                 NVARCHAR(100) NULL,
                GroupName               NVARCHAR(254) NULL,
                GroupSource             NVARCHAR(50)  NULL,
                GroupType               NVARCHAR(50)  NULL
            );
        END
        SET @TableCount = @TableCount + 1;


        -- ══════════════════════════════════════════════════════════════════════
        -- RAW_V2 INDEXES — Replicate SSIS "Add Index" task
        -- ══════════════════════════════════════════════════════════════════════
        -- raw_v2 tables are NOT dropped/recreated by Setup (they live in
        -- 05_clarity_ddl.sql / 05e). Use IF NOT EXISTS guards throughout.

        -- ──────────────────────────────────────────────────────────────────────
        -- Section A: Missing SSIS "Add Index" task indexes
        -- ──────────────────────────────────────────────────────────────────────

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_COMPONENT_SUMMARY_INFO_COMPONENT_ID' AND object_id = OBJECT_ID('raw_v2.COMPONENT_SUMMARY_INFO'))
            CREATE NONCLUSTERED INDEX IX_COMPONENT_SUMMARY_INFO_COMPONENT_ID
                ON raw_v2.COMPONENT_SUMMARY_INFO (COMPONENT_ID)
                INCLUDE (DATA_RESOURCES_ID);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_DRILL_TEXT_SQLSERVER_JOB_CONFIG_ID' AND object_id = OBJECT_ID('raw_v2.DRILL_TEXT_SQLSERVER'))
            CREATE NONCLUSTERED INDEX IX_DRILL_TEXT_SQLSERVER_JOB_CONFIG_ID
                ON raw_v2.DRILL_TEXT_SQLSERVER (JOB_CONFIGURATION_ID);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_METRIC_INFO_DEFINITION_ID' AND object_id = OBJECT_ID('raw_v2.METRIC_INFO'))
            CREATE NONCLUSTERED INDEX IX_METRIC_INFO_DEFINITION_ID
                ON raw_v2.METRIC_INFO (DEFINITION_ID);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_RESOURCE_DISPLAY_RESOURCE_ID' AND object_id = OBJECT_ID('raw_v2.RESOURCE_DISPLAY'))
            CREATE NONCLUSTERED INDEX IX_RESOURCE_DISPLAY_RESOURCE_ID
                ON raw_v2.RESOURCE_DISPLAY (RESOURCE_ID)
                INCLUDE (METRIC_DEF_ID);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_TEMPLATE_DYNAMIC_PARAM_PROMPT_ID' AND object_id = OBJECT_ID('raw_v2.TEMPLATE_DYNAMIC'))
            CREATE NONCLUSTERED INDEX IX_TEMPLATE_DYNAMIC_PARAM_PROMPT_ID
                ON raw_v2.TEMPLATE_DYNAMIC (PARAM_PROMPT_ID)
                INCLUDE (REPORT_ID);

        -- ──────────────────────────────────────────────────────────────────────
        -- Section B: Stage 20 join target indexes
        -- ──────────────────────────────────────────────────────────────────────

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CLARITY_RPT_GROUPS_REPORT_ID' AND object_id = OBJECT_ID('raw_v2.CLARITY_RPT_GROUPS'))
            CREATE NONCLUSTERED INDEX IX_CLARITY_RPT_GROUPS_REPORT_ID
                ON raw_v2.CLARITY_RPT_GROUPS (REPORT_ID)
                INCLUDE (REPORT_GROUP_C);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_OVRIDE_RPT_GROUPS_REPORT_ID' AND object_id = OBJECT_ID('raw_v2.OVRIDE_RPT_GROUPS'))
            CREATE NONCLUSTERED INDEX IX_OVRIDE_RPT_GROUPS_REPORT_ID
                ON raw_v2.OVRIDE_RPT_GROUPS (REPORT_ID)
                INCLUDE (REPORT_GROUP_C);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ClarityComponentGroups_COMPONENT_ID' AND object_id = OBJECT_ID('raw_v2.ClarityComponentGroups'))
            CREATE NONCLUSTERED INDEX IX_ClarityComponentGroups_COMPONENT_ID
                ON raw_v2.ClarityComponentGroups (COMPONENT_ID)
                INCLUDE (group_id);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_DATA_MODEL_DEFINITIONS_DATA_MODEL_ID' AND object_id = OBJECT_ID('raw_v2.DATA_MODEL_DEFINITIONS'))
            CREATE NONCLUSTERED INDEX IX_DATA_MODEL_DEFINITIONS_DATA_MODEL_ID
                ON raw_v2.DATA_MODEL_DEFINITIONS (DATA_MODEL_ID)
                INCLUDE (BASE_RECORD_ID);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ASSOC_REPORT_GROUPS_DASHBOARD_ID' AND object_id = OBJECT_ID('raw_v2.ASSOC_REPORT_GROUPS'))
            CREATE NONCLUSTERED INDEX IX_ASSOC_REPORT_GROUPS_DASHBOARD_ID
                ON raw_v2.ASSOC_REPORT_GROUPS (DASHBOARD_ID)
                INCLUDE (REPORT_GROUPS_C);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CLARITY_RPT_ASSOC_REPORT_ID' AND object_id = OBJECT_ID('raw_v2.CLARITY_RPT'))
            CREATE NONCLUSTERED INDEX IX_CLARITY_RPT_ASSOC_REPORT_ID
                ON raw_v2.CLARITY_RPT (ASSOC_REPORT_ID)
                INCLUDE (REPORT_ID, HIDE_FROM_LIBRARY_YN);

        -- v2.4: Stage 20 Branch 7 join: ri.REPORT_ID = hgrs.HGR_ID
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_REPORT_INFO_REPORT_ID_Stage20' AND object_id = OBJECT_ID('raw_v2.REPORT_INFO'))
            CREATE NONCLUSTERED INDEX IX_REPORT_INFO_REPORT_ID_Stage20
                ON raw_v2.REPORT_INFO (REPORT_ID)
                INCLUDE (REPORT_INFO_ID);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ClarityDashboardRoles_DASHBOARD_ID' AND object_id = OBJECT_ID('raw_v2.ClarityDashboardRoles'))
            CREATE NONCLUSTERED INDEX IX_ClarityDashboardRoles_DASHBOARD_ID
                ON raw_v2.ClarityDashboardRoles (dashboard_id)
                INCLUDE (user_roles_id, user_roles);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ClarityDashboardTypes_DASHBOARD_ID' AND object_id = OBJECT_ID('raw_v2.ClarityDashboardTypes'))
            CREATE NONCLUSTERED INDEX IX_ClarityDashboardTypes_DASHBOARD_ID
                ON raw_v2.ClarityDashboardTypes (dashboard_id)
                INCLUDE (user_types);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ClarityUserGroups_GroupId' AND object_id = OBJECT_ID('raw_v2.ClarityUserGroups'))
            CREATE NONCLUSTERED INDEX IX_ClarityUserGroups_GroupId
                ON raw_v2.ClarityUserGroups (GroupId)
                INCLUDE (GroupName, GroupSource);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_clarity_slicerdicer_sessions_Data_model' AND object_id = OBJECT_ID('raw_v2.clarity_slicerdicer_sessions'))
            CREATE NONCLUSTERED INDEX IX_clarity_slicerdicer_sessions_Data_model
                ON raw_v2.clarity_slicerdicer_sessions (Data_model)
                INCLUDE (Report_ID);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_DATA_MODEL_REPORT_GROUPS_DATA_MODEL_ID' AND object_id = OBJECT_ID('raw_v2.DATA_MODEL_REPORT_GROUPS'))
            CREATE NONCLUSTERED INDEX IX_DATA_MODEL_REPORT_GROUPS_DATA_MODEL_ID
                ON raw_v2.DATA_MODEL_REPORT_GROUPS (DATA_MODEL_ID)
                INCLUDE (REPORT_GROUPS_C);

        -- ──────────────────────────────────────────────────────────────────────
        -- Section C: 05d/05e-replacement indexes on narrowed raw_v2 tables
        -- These indexes were originally created by the one-time DDL scripts
        -- 05d_clarity_emp_narrow_ddl.sql and 05e_column_narrowing_ddl.sql but are
        -- absent from the 2026-04-06 DDL snapshot. Moved into Setup for
        -- idempotent recreation at every pipeline run (survives any future
        -- DROP+CREATE in 05d/05e).
        -- ──────────────────────────────────────────────────────────────────────

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CLARITY_EMP_USER_ID' AND object_id = OBJECT_ID('raw_v2.CLARITY_EMP'))
            CREATE NONCLUSTERED INDEX IX_CLARITY_EMP_USER_ID
                ON raw_v2.CLARITY_EMP (USER_ID);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_COMPONENT_INFO_COMPONENT_ID' AND object_id = OBJECT_ID('raw_v2.COMPONENT_INFO'))
            CREATE NONCLUSTERED INDEX IX_COMPONENT_INFO_COMPONENT_ID
                ON raw_v2.COMPONENT_INFO (COMPONENT_ID);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_TEMPLATE_INFO_REPORT_ID' AND object_id = OBJECT_ID('raw_v2.TEMPLATE_INFO'))
            CREATE NONCLUSTERED INDEX IX_TEMPLATE_INFO_REPORT_ID
                ON raw_v2.TEMPLATE_INFO (REPORT_ID);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_REPORT_INFO_REPORT_INFO_ID' AND object_id = OBJECT_ID('raw_v2.REPORT_INFO'))
            CREATE NONCLUSTERED INDEX IX_REPORT_INFO_REPORT_INFO_ID
                ON raw_v2.REPORT_INFO (REPORT_INFO_ID);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_REPORT_INFO_REPORT_ID' AND object_id = OBJECT_ID('raw_v2.REPORT_INFO'))
            CREATE NONCLUSTERED INDEX IX_REPORT_INFO_REPORT_ID
                ON raw_v2.REPORT_INFO (REPORT_ID);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_DASHBOARD_INFO_DASHBOARD_ID' AND object_id = OBJECT_ID('raw_v2.DASHBOARD_INFO'))
            CREATE NONCLUSTERED INDEX IX_DASHBOARD_INFO_DASHBOARD_ID
                ON raw_v2.DASHBOARD_INFO (DASHBOARD_ID);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_COMPONENT_LIST_COMPONENT_ID' AND object_id = OBJECT_ID('raw_v2.COMPONENT_LIST'))
            CREATE NONCLUSTERED INDEX IX_COMPONENT_LIST_COMPONENT_ID
                ON raw_v2.COMPONENT_LIST (COMPONENT_ID);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_COMPONENT_LIST_DASHBOARD_ID' AND object_id = OBJECT_ID('raw_v2.COMPONENT_LIST'))
            CREATE NONCLUSTERED INDEX IX_COMPONENT_LIST_DASHBOARD_ID
                ON raw_v2.COMPONENT_LIST (DASHBOARD_ID);

        -- ──────────────────────────────────────────────────────────────────────
        -- Section D: RunData Phase 7a/7b join target indexes
        -- Supports 13_usp_Atlas_RunData.sql joins that were otherwise missing
        -- index support. Only SlicerDicerStatsHttp.RequestId is indexable here —
        -- BASE_RECORD_ID and Report_ID are both NVARCHAR(MAX) in the authoritative
        -- DDL (Atlas_Staging_Table_DDL_04062026.txt lines 5172 and 4789), so
        -- they cannot be index key columns. Queries that join on these columns
        -- fall back to scan+hash, which is acceptable for the table sizes involved.
        -- The existing IX_DATA_MODEL_DEFINITIONS_DATA_MODEL_ID index already
        -- INCLUDEs BASE_RECORD_ID, giving the optimizer full data access without
        -- an extra index.
        -- ──────────────────────────────────────────────────────────────────────

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SlicerDicerStatsHttp_RequestId' AND object_id = OBJECT_ID('raw_v2.SlicerDicerStatsHttp'))
            CREATE NONCLUSTERED INDEX IX_SlicerDicerStatsHttp_RequestId
                ON raw_v2.SlicerDicerStatsHttp (RequestId);

        -- ──────────────────────────────────────────────────────────────────────
        -- Section E: prd_v2 production indexes
        -- Persistent across runs. Supports Merge 9 composite lookup and multiple
        -- BizKey-based joins across Merge 4, 5, 8, RunData Phase 12, and
        -- PostProcessing Step 3. Uses the NVARCHAR(500) BizKey column, not the
        -- NVARCHAR(MAX) ReportObjectBizKey (which is unindexable as a key).
        -- ──────────────────────────────────────────────────────────────────────

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_prd_v2_ReportObjects_EpicRecordID_MasterFile' AND object_id = OBJECT_ID('prd_v2.ReportObjects'))
            CREATE NONCLUSTERED INDEX IX_prd_v2_ReportObjects_EpicRecordID_MasterFile
                ON prd_v2.ReportObjects (EpicRecordID, EpicMasterFile)
                INCLUDE (ReportObjectID);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_prd_v2_ReportObjects_BizKey' AND object_id = OBJECT_ID('prd_v2.ReportObjects'))
            CREATE NONCLUSTERED INDEX IX_prd_v2_ReportObjects_BizKey
                ON prd_v2.ReportObjects (BizKey)
                INCLUDE (ReportObjectID, ReportObjectBizKey);

        -- ──────────────────────────────────────────────────────────────────────
        -- Section F: Medium-priority follow-up indexes (index audit followup)
        -- Addresses findings M-5, M-7, M-9, M-10, M-11 from the 2026-04-06 index
        -- audit. All persistent tables — IF NOT EXISTS guards.
        -- ──────────────────────────────────────────────────────────────────────

        -- M-5: raw_v2.clarity_intraparameter_logic ([Crit Uniq]) — used by Stage 17
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_intraparameter_logic_CritUniq' AND object_id = OBJECT_ID('raw_v2.clarity_intraparameter_logic'))
            CREATE NONCLUSTERED INDEX IX_intraparameter_logic_CritUniq
                ON raw_v2.clarity_intraparameter_logic ([Crit Uniq]);

        -- M-7: dbo.EMPtoAzureMap (Epic_AccountID) — used by LDAP and multiple Clarity
        -- stages for user resolution (COALESCE(eam.AZURE_upn, ...) pattern)
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_EMPtoAzureMap_EpicAccountID' AND object_id = OBJECT_ID('dbo.EMPtoAzureMap'))
            CREATE NONCLUSTERED INDEX IX_EMPtoAzureMap_EpicAccountID
                ON dbo.EMPtoAzureMap (Epic_AccountID)
                INCLUDE (AZURE_upn);

        -- M-9: raw_v2.ClarityUsernameLinks (user_Id) — used by LDAP Step 6
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ClarityUsernameLinks_UserId' AND object_id = OBJECT_ID('raw_v2.ClarityUsernameLinks'))
            CREATE NONCLUSTERED INDEX IX_ClarityUsernameLinks_UserId
                ON raw_v2.ClarityUsernameLinks (user_Id);

        -- M-10: raw_v2.ZC_REPORT_TYPE_HGR (REPORT_TYPE_HGR_C) — small lookup joined
        -- many times across 12_Clarity, 12b_ClarityHierarchy, and 13_RunData
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ZC_REPORT_TYPE_HGR_C' AND object_id = OBJECT_ID('raw_v2.ZC_REPORT_TYPE_HGR'))
            CREATE NONCLUSTERED INDEX IX_ZC_REPORT_TYPE_HGR_C
                ON raw_v2.ZC_REPORT_TYPE_HGR (REPORT_TYPE_HGR_C);

        -- ──────────────────────────────────────────────────────────────────────
        -- prd_v2.ReportObjectRunDataBridge — mirror production index set exactly.
        -- Production dbo.ReportObjectRunDataBridge has 3 indexes that we missed
        -- in the initial Setup, causing Phase 12 to stall on the 8M-row table:
        --   1. RunId               → Phase 12 MERGE existence check
        --   2. (ReportObjectId, Inherited) INCLUDE (RunId, Runs)
        --                          → Enrich 14 stale-report detection scan
        --   3. ReportObjectId      INCLUDE (RunId, Runs)
        --                          → PostProcessing distinct-user rollup
        -- All 3 are required for the bridge table to behave like production at
        -- our row counts.
        -- ──────────────────────────────────────────────────────────────────────
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_prd_v2_RunDataBridge_RunId' AND object_id = OBJECT_ID('prd_v2.ReportObjectRunDataBridge'))
            CREATE NONCLUSTERED INDEX IX_prd_v2_RunDataBridge_RunId
                ON prd_v2.ReportObjectRunDataBridge (RunId);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_prd_v2_RunDataBridge_ReportObjectId_Inherited' AND object_id = OBJECT_ID('prd_v2.ReportObjectRunDataBridge'))
            CREATE NONCLUSTERED INDEX IX_prd_v2_RunDataBridge_ReportObjectId_Inherited
                ON prd_v2.ReportObjectRunDataBridge (ReportObjectId, Inherited)
                INCLUDE (RunId, Runs);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_prd_v2_RunDataBridge_ReportObjectId' AND object_id = OBJECT_ID('prd_v2.ReportObjectRunDataBridge'))
            CREATE NONCLUSTERED INDEX IX_prd_v2_RunDataBridge_ReportObjectId
                ON prd_v2.ReportObjectRunDataBridge (ReportObjectId)
                INCLUDE (RunId, Runs);

        COMMIT TRANSACTION;

        EXEC etl.usp_Atlas_LogEnd 
            @LogID = @LogID,
            @RowsAffected = @TableCount,
            @Status = 'Success';
        
        PRINT 'ETL-Setup completed successfully. Tables created in stage_v2 schema: ' + CAST(@TableCount AS VARCHAR(10));
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_Atlas_LogError @LogID = @LogID;

        SET @ErrorMessage = ERROR_MESSAGE();
        
        IF @RaiseErrorOnFail = 1
        BEGIN
            THROW;
        END
        ELSE
        BEGIN
            PRINT 'ETL-Setup failed: ' + @ErrorMessage;
        END
    END CATCH
END
GO

PRINT 'Created/Updated procedure: etl.usp_Atlas_Setup (now uses stage_v2 schema)';
GO


PRINT '';
PRINT '========================================';
PRINT 'Schema Summary';
PRINT '========================================';
PRINT 'Staging tables will be created in: stage_v2.*';
PRINT 'Raw tables will be created in: raw_v2.*';
PRINT 'ETL procedures remain in: etl.*';
PRINT '';
PRINT 'This avoids conflicts with existing dbo.* and raw.* tables';
PRINT 'used by the live SSIS process.';
PRINT '========================================';
GO
