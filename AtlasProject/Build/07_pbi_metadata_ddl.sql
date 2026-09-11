/*
================================================================================
Atlas ETL Suite - Power BI Metadata Raw Table DDL
================================================================================
Creates 7 raw_v2 tables for the Power BI metadata extractor (Phase 2).
These tables are populated by:
  - atlas_pbi_metadata.py  → /v1.0/myorg/admin/{groups,reports,datasets,apps,capacities}

Then consumed by:
  - etl.usp_Atlas_RunData    (Phase 4b — joins PbiActivityEvent to PbiReport
                              for CN4 BizKey construction)
  - etl.usp_Atlas_PowerBI    (Phase 4 — stages PBI reports as ReportObjects
                              via PbiReport + PbiWorkspaceReport bridge)

Dependencies:
  - Schema: raw_v2 (created by 02_usp_Atlas_Setup.sql)
  - Python/pyodbc via atlas_pbi_metadata.py
  - PBI Admin API (Azure AD App Registration with Tenant.Read.All)
  - NO LINKED SERVER REQUIRED (Pipeline B)

Tables created (7):
  1. raw_v2.PbiWorkspace        (workspace metadata)
  2. raw_v2.PbiWorkspaceReport  (workspace→report bridge from $expand=reports)
  3. raw_v2.PbiWorkspaceUser    (workspace→user bridge from $expand=users;
                                 SamAccountName/UserPrincipalName/Domain
                                 populated later by Phase 5
                                 atlas_pbi_user_identity.py via Graph API)
  4. raw_v2.PbiReport           (KEYSTONE — flat report list, joined to
                                 PbiActivityEvent in Phase 4b for BizKey)
  5. raw_v2.PbiDataset          (dataset metadata; column names mostly
                                 verbatim from $datasetList except DatasetId
                                 + DatasetName which were renamed in Phase 2
                                 to match the *Name convention)
  6. raw_v2.PbiApp              (app list)
  7. raw_v2.PbiCapacity         (capacity list)

Provenance:
  These 7 tables were originally created on the live Atlas_Staging server
  via SSMS on 2026-04-07 during Phase 2 of the PBI metadata migration.
  This file is the authoritative repo copy of that DDL — every column name,
  type, width, and nullability was verified against sys.columns on the live
  server before being codified here.

  Two columns deviate from the original PBI_METADATA_MIGRATION_BUILD_PLAN.md
  Section 2.2 specification:
    - PbiWorkspace.Description       (added — nvarchar(max))
    - PbiReport.SensitivityLabel     (added — varchar(255))
  Both additions were applied to the live SSMS DDL during Phase 2 Part A
  and are reflected here as the authoritative schema.

  PbiDataset has no build plan entry — it was composed during Phase 2 Part A
  and applied via SSMS. This file is the first repo-checked-in copy of the
  PbiDataset DDL.

Run this script on: Atlas_Staging
Run order: After 02_usp_Atlas_Setup.sql (needs raw_v2 schema)
           Independent of 06_rundata_ddl.sql (no cross-table dependencies)

Version: 1.0
Last Updated: April 2026
Scripted from: Atlas_Staging server sys.columns, 2026-04-07 13:08
================================================================================
*/

USE Atlas_Staging;
GO

PRINT '=== Power BI Metadata Raw Table DDL ===';
PRINT 'Start Time: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
GO

-- =============================================================================
-- SECTION 1: WORKSPACE TABLES (3 tables)
-- Populated by atlas_pbi_metadata.py from /v1.0/myorg/admin/groups
-- with $expand=reports,users on the same paged call.
-- =============================================================================

-- 1. PBI Workspace (parent workspace metadata)
IF OBJECT_ID('raw_v2.PbiWorkspace', 'U') IS NOT NULL
    DROP TABLE raw_v2.PbiWorkspace;
GO

CREATE TABLE [raw_v2].[PbiWorkspace] (
    [WorkspaceId]           [varchar](100)   NOT NULL,
    [WorkspaceName]         [varchar](500)   NULL,
    [WorkspaceType]         [varchar](100)   NULL,
    [State]                 [varchar](50)    NULL,
    [IsOnDedicatedCapacity] [varchar](5)     NULL,
    [IsReadOnly]            [varchar](5)     NULL,
    [CapacityId]            [varchar](100)   NULL,
    [Description]           [nvarchar](max)  NULL,
    [ETL_LoadDate]          [datetime]       NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [PK_raw_v2_PbiWorkspace] PRIMARY KEY CLUSTERED ([WorkspaceId] ASC)
);
GO
PRINT 'Created raw_v2.PbiWorkspace';
GO

-- 2. PBI Workspace → Report bridge (one row per report in each workspace)
IF OBJECT_ID('raw_v2.PbiWorkspaceReport', 'U') IS NOT NULL
    DROP TABLE raw_v2.PbiWorkspaceReport;
GO

CREATE TABLE [raw_v2].[PbiWorkspaceReport] (
    [WorkspaceId]  [varchar](100) NOT NULL,
    [ReportId]     [varchar](100) NOT NULL,
    [ETL_LoadDate] [datetime]     NOT NULL DEFAULT GETDATE()
);
GO
CREATE INDEX [IX_raw_v2_PbiWorkspaceReport_ReportId]
    ON [raw_v2].[PbiWorkspaceReport] ([ReportId]);
GO
CREATE INDEX [IX_raw_v2_PbiWorkspaceReport_WorkspaceId]
    ON [raw_v2].[PbiWorkspaceReport] ([WorkspaceId]);
GO
PRINT 'Created raw_v2.PbiWorkspaceReport';
GO

-- 3. PBI Workspace → User bridge (one row per user in each workspace)
--    UserPrincipalName / SamAccountName / Domain are populated by
--    atlas_pbi_user_identity.py (Phase 5) via Microsoft Graph API.
IF OBJECT_ID('raw_v2.PbiWorkspaceUser', 'U') IS NOT NULL
    DROP TABLE raw_v2.PbiWorkspaceUser;
GO

CREATE TABLE [raw_v2].[PbiWorkspaceUser] (
    [WorkspaceId]          [varchar](100) NOT NULL,
    [Identifier]           [varchar](255) NULL,
    [EmailAddress]         [varchar](255) NULL,
    [DisplayName]          [varchar](500) NULL,
    [GroupUserAccessRight] [varchar](100) NULL,
    [PrincipalType]        [varchar](100) NULL,
    [GraphId]              [varchar](100) NULL,
    [UserPrincipalName]    [varchar](255) NULL,  -- populated by Phase 5
    [SamAccountName]       [varchar](100) NULL,  -- populated by Phase 5
    [Domain]               [varchar](100) NULL,  -- populated by Phase 5
    [ETL_LoadDate]         [datetime]     NOT NULL DEFAULT GETDATE()
);
GO
CREATE INDEX [IX_raw_v2_PbiWorkspaceUser_WorkspaceId]
    ON [raw_v2].[PbiWorkspaceUser] ([WorkspaceId]);
GO
CREATE INDEX [IX_raw_v2_PbiWorkspaceUser_Identifier]
    ON [raw_v2].[PbiWorkspaceUser] ([Identifier]);
GO
PRINT 'Created raw_v2.PbiWorkspaceUser';
GO

-- =============================================================================
-- SECTION 2: REPORT TABLE (KEYSTONE for Phase 4b CN4 BizKey JOIN)
-- Populated by atlas_pbi_metadata.py from /v1.0/myorg/admin/reports
-- =============================================================================

-- 4. PBI Report (flat report list — keystone for BizKey construction)
--    Phase 5 user identity columns are NULL until atlas_pbi_user_identity.py
--    runs and resolves CreatedBy/ModifiedBy GUIDs into UPN/SAM/Domain.
IF OBJECT_ID('raw_v2.PbiReport', 'U') IS NOT NULL
    DROP TABLE raw_v2.PbiReport;
GO

CREATE TABLE [raw_v2].[PbiReport] (
    [ReportId]                 [varchar](100)  NOT NULL,
    [ReportName]               [varchar](500)  NULL,
    [Description]              [nvarchar](max) NULL,
    [EmbedUrl]                 [varchar](2000) NULL,
    [WebUrl]                   [varchar](2000) NULL,
    [ReportType]               [varchar](100)  NULL,
    [SensitivityLabel]         [varchar](255)  NULL,
    [AppId]                    [varchar](100)  NULL,
    [DatasetId]                [varchar](100)  NULL,
    [WorkspaceId]              [varchar](100)  NULL,
    [WorkspaceName]            [varchar](500)  NULL,
    [WorkspaceConnection]      [varchar](500)  NULL,
    [CapacityId]               [varchar](100)  NULL,
    [CapacityName]             [varchar](500)  NULL,
    [Source]                   [varchar](200)  NULL,
    [CreatedBy]                [varchar](255)  NULL,
    [CreatedDateTime]          [datetime]      NULL,
    [ModifiedBy]               [varchar](255)  NULL,
    [ModifiedDateTime]         [datetime]      NULL,
    [CreatedByUPN]             [varchar](255)  NULL,  -- populated by Phase 5
    [CreatedBySamAccountName]  [varchar](100)  NULL,  -- populated by Phase 5
    [CreatedByDomain]          [varchar](100)  NULL,  -- populated by Phase 5
    [ModifiedByUPN]            [varchar](255)  NULL,  -- populated by Phase 5
    [ModifiedBySamAccountName] [varchar](100)  NULL,  -- populated by Phase 5
    [ModifiedByDomain]         [varchar](100)  NULL,  -- populated by Phase 5
    [DefaultVisibilityYN]      [varchar](1)    NULL,
    [ETL_LoadDate]             [datetime]      NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [PK_raw_v2_PbiReport] PRIMARY KEY CLUSTERED ([ReportId] ASC)
);
GO
CREATE INDEX [IX_raw_v2_PbiReport_WorkspaceId]
    ON [raw_v2].[PbiReport] ([WorkspaceId]);
GO
CREATE INDEX [IX_raw_v2_PbiReport_AppId]
    ON [raw_v2].[PbiReport] ([AppId]);
GO
PRINT 'Created raw_v2.PbiReport';
GO

-- =============================================================================
-- SECTION 3: DATASET TABLE
-- Populated by atlas_pbi_metadata.py from /v1.0/myorg/admin/datasets
-- =============================================================================

-- 5. PBI Dataset
--    Column names match $datasetList property names from pbi_Main_Meta.ps1
--    with two PascalCase renames:
--      id   → DatasetId   (PK)
--      name → DatasetName (matches WorkspaceName/ReportName/AppName convention)
--    All other columns retain the legacy property names. Boolean flag
--    columns are stored as varchar(5) so str(True)='True' and
--    str(False)='False' both fit. SQL Server CI_AS collation makes
--    case-only differences harmless.
IF OBJECT_ID('raw_v2.PbiDataset', 'U') IS NOT NULL
    DROP TABLE raw_v2.PbiDataset;
GO

CREATE TABLE [raw_v2].[PbiDataset] (
    [DatasetId]                        [varchar](100)  NOT NULL,
    [DatasetName]                      [varchar](500)  NULL,
    [Description]                      [nvarchar](max) NULL,
    [ContentProviderType]              [varchar](100)  NULL,
    [CreateReportEmbedUrl]             [varchar](2000) NULL,
    [CreatedDate]                      [datetime]      NULL,
    [IsEffectiveIdentityRequired]      [varchar](5)    NULL,
    [IsEffectiveIdentityRolesRequired] [varchar](5)    NULL,
    [IsOnPremGatewayRequired]          [varchar](5)    NULL,
    [IsRefreshable]                    [varchar](5)    NULL,
    [QnaEmbedUrl]                      [varchar](2000) NULL,
    [AddRowsAPIEnabled]                [varchar](5)    NULL,
    [ConfiguredBy]                     [varchar](255)  NULL,
    [SchemaMayNotBeUpToDate]           [varchar](5)    NULL,
    [SchemaRetrievalError]             [nvarchar](max) NULL,
    [SensitivityLabel]                 [varchar](255)  NULL,
    [WebUrl]                           [varchar](2000) NULL,
    [ETL_LoadDate]                     [datetime]      NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [PK_raw_v2_PbiDataset] PRIMARY KEY CLUSTERED ([DatasetId] ASC)
);
GO
PRINT 'Created raw_v2.PbiDataset';
GO

-- =============================================================================
-- SECTION 4: APP TABLE
-- Populated by atlas_pbi_metadata.py from /v1.0/myorg/admin/apps
-- =============================================================================

-- 6. PBI App
IF OBJECT_ID('raw_v2.PbiApp', 'U') IS NOT NULL
    DROP TABLE raw_v2.PbiApp;
GO

CREATE TABLE [raw_v2].[PbiApp] (
    [AppId]        [varchar](100)  NOT NULL,
    [AppName]      [varchar](500)  NULL,
    [Description]  [nvarchar](max) NULL,
    [LastUpdate]   [datetime]      NULL,
    [PublishedBy]  [varchar](255)  NULL,
    [ETL_LoadDate] [datetime]      NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [PK_raw_v2_PbiApp] PRIMARY KEY CLUSTERED ([AppId] ASC)
);
GO
PRINT 'Created raw_v2.PbiApp';
GO

-- =============================================================================
-- SECTION 5: CAPACITY TABLE
-- Populated by atlas_pbi_metadata.py from /v1.0/myorg/admin/capacities
-- =============================================================================

-- 7. PBI Capacity (small table — single non-paginated API call)
IF OBJECT_ID('raw_v2.PbiCapacity', 'U') IS NOT NULL
    DROP TABLE raw_v2.PbiCapacity;
GO

CREATE TABLE [raw_v2].[PbiCapacity] (
    [CapacityId]              [varchar](100) NOT NULL,
    [DisplayName]             [varchar](500) NULL,
    [Sku]                     [varchar](50)  NULL,
    [State]                   [varchar](50)  NULL,
    [CapacityUserAccessRight] [varchar](100) NULL,
    [Region]                  [varchar](100) NULL,
    [ETL_LoadDate]            [datetime]     NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [PK_raw_v2_PbiCapacity] PRIMARY KEY CLUSTERED ([CapacityId] ASC)
);
GO
PRINT 'Created raw_v2.PbiCapacity';
GO

-- =============================================================================
-- VERIFICATION
-- =============================================================================

PRINT '';
PRINT '=== PBI Metadata DDL Complete ===';
PRINT 'End Time: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
PRINT '';

SELECT t.TableName, t.RowCnt
FROM (
    SELECT 'raw_v2.PbiWorkspace'        AS TableName, COUNT(*) AS RowCnt FROM raw_v2.PbiWorkspace
    UNION ALL
    SELECT 'raw_v2.PbiWorkspaceReport',  COUNT(*) FROM raw_v2.PbiWorkspaceReport
    UNION ALL
    SELECT 'raw_v2.PbiWorkspaceUser',    COUNT(*) FROM raw_v2.PbiWorkspaceUser
    UNION ALL
    SELECT 'raw_v2.PbiReport',           COUNT(*) FROM raw_v2.PbiReport
    UNION ALL
    SELECT 'raw_v2.PbiDataset',          COUNT(*) FROM raw_v2.PbiDataset
    UNION ALL
    SELECT 'raw_v2.PbiApp',              COUNT(*) FROM raw_v2.PbiApp
    UNION ALL
    SELECT 'raw_v2.PbiCapacity',         COUNT(*) FROM raw_v2.PbiCapacity
) t
ORDER BY t.TableName;

PRINT 'Tables created: 7 (3 workspace + 1 report + 1 dataset + 1 app + 1 capacity)';
GO
