/*
================================================================================
Atlas ETL Suite - Production V2 Schema Setup
================================================================================
Creates prd_v2 schema in Atlas_Staging with copies of production tables.
This allows the migrated ETL to run completely in parallel with the live
SSIS process without affecting production data.

Tables created in prd_v2:
  - ReportObjects (copy from Atlas_Prd.dbo.ReportObject)
  - ReportObjectRunData (individual run records with SHA2_256 RunDataId)
  - ReportObjectRunDataBridge (run data ↔ report object bridge)
  - [User] (matches Atlas_Prd.dbo.User — populated by usp_Atlas_LDAP)
  - ReportObjectType (report type catalog)
  - ReportObjectTags (tag definitions)
  - ReportObjectHierarchy (parent-child report relationships)
  - ReportObjectQuery (report query text / SQL definitions)
  - ReportObjectParameters (report parameters)
  - ReportObjectAttachments (report attachments / linked objects)
  - ReportObjectSubscriptions (report subscriptions)
  - ReportObjectTagMemberships (report-to-tag mappings)
  - UserGroups (AD / security groups)
  - UserGroupsMembership (user-to-group memberships)
  - DoNotOrphanTypes (configuration table - created empty)
  - URLOverrides (configuration table - created empty)
  - VisibilityRules (configuration table - created empty)

Run this script on: Atlas_Staging

Version: 2.1
Last Updated: April 2026
v2.1 2026-04-09:
  - C-4/U-1: Username NVARCHAR(MAX) → NVARCHAR(256) on prd_v2.[User].
    Index IX_prd_v2_User_Username added on prd_v2.[User].Username.
================================================================================
*/

USE Atlas_Staging;
GO

-- ══════════════════════════════════════════════════════════════════════════════
-- 1. CREATE prd_v2 SCHEMA
-- ══════════════════════════════════════════════════════════════════════════════

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'prd_v2')
BEGIN
    EXEC('CREATE SCHEMA prd_v2 AUTHORIZATION dbo');
    PRINT 'Created schema: prd_v2';
END
ELSE
    PRINT 'Schema prd_v2 already exists';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 2. CREATE ReportObjects TABLE (main production table)
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.ReportObjects', 'U') IS NOT NULL
    DROP TABLE prd_v2.ReportObjects;
GO

-- Create table structure matching Atlas_Prd.dbo.ReportObject
-- Then copy data from production
SELECT *
INTO prd_v2.ReportObjects
FROM Atlas_Prd.dbo.ReportObject;

PRINT 'Created prd_v2.ReportObjects with ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows from Atlas_Prd.dbo.ReportObject';
GO

-- Add columns that PostProcessing expects (if they don't exist in source)
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'IsOrphaned')
    ALTER TABLE prd_v2.ReportObjects ADD IsOrphaned BIT NULL DEFAULT 0;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'OrphanedDate')
    ALTER TABLE prd_v2.ReportObjects ADD OrphanedDate DATETIME NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'LastSeenDate')
    ALTER TABLE prd_v2.ReportObjects ADD LastSeenDate DATETIME NULL DEFAULT GETDATE();

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'IsVisible')
    ALTER TABLE prd_v2.ReportObjects ADD IsVisible BIT NULL DEFAULT 1;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'VisibilityRuleApplied')
    ALTER TABLE prd_v2.ReportObjects ADD VisibilityRuleApplied NVARCHAR(100) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'URLOverrideApplied')
    ALTER TABLE prd_v2.ReportObjects ADD URLOverrideApplied BIT NULL DEFAULT 0;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'URLOverrideDate')
    ALTER TABLE prd_v2.ReportObjects ADD URLOverrideDate DATETIME NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'BizKey')
    ALTER TABLE prd_v2.ReportObjects ADD BizKey NVARCHAR(500) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'ObjectType')
    ALTER TABLE prd_v2.ReportObjects ADD ObjectType NVARCHAR(100) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'ObjectPath')
    ALTER TABLE prd_v2.ReportObjects ADD ObjectPath NVARCHAR(1000) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'ObjectName')
    ALTER TABLE prd_v2.ReportObjects ADD ObjectName NVARCHAR(500) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'ObjectURL')
    ALTER TABLE prd_v2.ReportObjects ADD ObjectURL NVARCHAR(1000) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'SourceSystem')
    ALTER TABLE prd_v2.ReportObjects ADD SourceSystem NVARCHAR(50) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'DistinctUserCount')
    ALTER TABLE prd_v2.ReportObjects ADD DistinctUserCount INT NULL;

-- DistinctUsersPast12Months: production column name (matches Atlas_Prd.dbo.ReportObject).
-- Updated by usp_Atlas_PostProcessing Step 5. Rolling 12-month COUNT(DISTINCT RunUserID).
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'DistinctUsersPast12Months')
    ALTER TABLE prd_v2.ReportObjects ADD DistinctUsersPast12Months INT NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'PowerBiWorkspaceName')
    ALTER TABLE prd_v2.ReportObjects ADD PowerBiWorkspaceName NVARCHAR(255) NULL;

-- Defensive: legacy columns referenced by usp_Atlas_Merge enrichment steps.
-- These should already exist via SELECT * INTO from Atlas_Prd.dbo.ReportObject,
-- but adding conditionally in case the legacy schema changes.
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'SourceServer')
    ALTER TABLE prd_v2.ReportObjects ADD SourceServer NVARCHAR(200) NOT NULL DEFAULT '';

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'SourceDB')
    ALTER TABLE prd_v2.ReportObjects ADD SourceDB NVARCHAR(200) NOT NULL DEFAULT '';

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'SourceTable')
    ALTER TABLE prd_v2.ReportObjects ADD SourceTable NVARCHAR(200) NOT NULL DEFAULT '';

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'OrphanedReportObjectYN')
    ALTER TABLE prd_v2.ReportObjects ADD OrphanedReportObjectYN CHAR(1) NULL DEFAULT 'N';

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'DefaultVisibilityYN')
    ALTER TABLE prd_v2.ReportObjects ADD DefaultVisibilityYN CHAR(1) NULL DEFAULT 'Y';

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'EpicMasterFile')
    ALTER TABLE prd_v2.ReportObjects ADD EpicMasterFile NVARCHAR(10) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'EpicRecordID')
    ALTER TABLE prd_v2.ReportObjects ADD EpicRecordID INT NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'EpicReleased')
    ALTER TABLE prd_v2.ReportObjects ADD EpicReleased CHAR(1) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'EpicRepoDescription')
    ALTER TABLE prd_v2.ReportObjects ADD EpicRepoDescription NVARCHAR(MAX) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'ReportObjectURL')
    ALTER TABLE prd_v2.ReportObjects ADD ReportObjectURL NVARCHAR(1000) NULL;

-- ReportObjectID: expected as legacy PK from SELECT * INTO. Cannot add IDENTITY
-- column via ALTER TABLE if one already exists. Only add as plain INT fallback.
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'ReportObjectID')
    ALTER TABLE prd_v2.ReportObjects ADD ReportObjectID INT NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'ReportObjectTypeID')
    ALTER TABLE prd_v2.ReportObjects ADD ReportObjectTypeID INT NULL;

-- ReportObjectBizKey: legacy column name (v2 uses BizKey). Keep both for compatibility.
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('prd_v2.ReportObjects') AND name = 'ReportObjectBizKey')
    ALTER TABLE prd_v2.ReportObjects ADD ReportObjectBizKey NVARCHAR(500) NULL;

PRINT 'Added any missing columns to prd_v2.ReportObjects';
GO

-- Codify the clustered primary key on ReportObjectID.
-- Mirrors PK_ReportObject in Atlas_Prd.dbo.ReportObject (CLUSTERED on ReportObjectID).
-- Was applied manually in SSMS on 2026-04-06 to resolve the Merge 1 heap stall;
-- codified here on 2026-04-07 so it survives a schema rebuild.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('prd_v2.ReportObjects')
      AND name = 'PK_prd_v2_ReportObjects'
)
    ALTER TABLE prd_v2.ReportObjects
        ADD CONSTRAINT PK_prd_v2_ReportObjects
            PRIMARY KEY CLUSTERED (ReportObjectID);
PRINT 'Ensured clustered PK_prd_v2_ReportObjects on prd_v2.ReportObjects(ReportObjectID)';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 3. CREATE ReportObjectRunData TABLE (run data production table)
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.ReportObjectRunData', 'U') IS NOT NULL
    DROP TABLE prd_v2.ReportObjectRunData;
GO

-- Production-compatible layout matching Atlas_Prd.dbo.ReportObjectRunData.
-- Individual run records — NOT aggregated daily summaries.
-- RunDataId is a SHA2_256 content hash (see usp_Atlas_RunData Phase 10).
CREATE TABLE prd_v2.ReportObjectRunData (
    RunId                   INT IDENTITY(1,1) NOT NULL,
    RunDataId               NVARCHAR(450) NOT NULL,
    RunUserID               INT NULL,
    RunStartTime            DATETIME NOT NULL,
    RunDurationSeconds      INT NULL,
    RunStatus               NVARCHAR(100) NULL,
    LastLoadDate            DATETIME NOT NULL DEFAULT GETDATE(),
    RunStartTime_Day        DATETIME2(7) NOT NULL,
    RunStartTime_Hour       DATETIME2(7) NOT NULL,
    RunStartTime_Month      DATETIME2(7) NOT NULL,
    RunStartTime_Year       DATETIME2(7) NOT NULL,
    CONSTRAINT PK_prd_v2_ReportObjectRunData
        PRIMARY KEY CLUSTERED (RunId),
    CONSTRAINT UQ_prd_v2_ReportObjectRunData_RunDataId
        UNIQUE NONCLUSTERED (RunDataId)
);
PRINT 'Created prd_v2.ReportObjectRunData';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 4. CREATE ReportObjectRunDataBridge TABLE (run data ↔ report object bridge)
-- Matches Atlas_Prd.dbo.ReportObjectRunDataBridge.
-- BridgeId: identity PK. RunId: SHA2_256 hash (same value as RunDataId in
-- ReportObjectRunData). ReportObjectId: resolved from BizKey at load time.
-- Inherited: 0 = direct run, 1 = inherited via parent report hierarchy.
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.ReportObjectRunDataBridge', 'U') IS NOT NULL
    DROP TABLE prd_v2.ReportObjectRunDataBridge;
GO

CREATE TABLE prd_v2.ReportObjectRunDataBridge (
    BridgeId            INT IDENTITY(1,1) NOT NULL,
    ReportObjectId      INT NOT NULL,
    RunId               NVARCHAR(450) NULL,
    Runs                INT NOT NULL,
    Inherited           INT NOT NULL,
    CONSTRAINT PK_prd_v2_ReportObjectRunDataBridge
        PRIMARY KEY CLUSTERED (BridgeId)
);
PRINT 'Created prd_v2.ReportObjectRunDataBridge';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- Composite (RunId, ReportObjectId) index supports the Phase 12 MERGE
-- matching key directly. Without it, the matcher can only seek by RunId
-- (via IX_prd_v2_RunDataBridge_RunId in 02_usp_Atlas_Setup.sql) and then
-- range-scan ReportObjectId per matched row, which scales poorly as the
-- target grows. NOTE: the other 3 bridge NCIs (RunId; ReportObjectId+Inherited;
-- ReportObjectId) live in 02_usp_Atlas_Setup.sql — this composite is the only
-- bridge index defined here.
-- ─────────────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = 'IX_prd_v2_RunDataBridge_RunId_ReportObjectId'
                 AND object_id = OBJECT_ID('prd_v2.ReportObjectRunDataBridge'))
    CREATE NONCLUSTERED INDEX IX_prd_v2_RunDataBridge_RunId_ReportObjectId
        ON prd_v2.ReportObjectRunDataBridge (RunId, ReportObjectId);
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 5. CREATE [User] TABLE (matches Atlas_Prd.dbo.User exactly)
-- Populated by usp_Atlas_LDAP Step 4 MERGE (Username = match key).
-- App-managed columns (LastLogin, ProfilePhoto, Base, Fullname_calc,
-- Firstname_calc) are NULL on ETL insert and never overwritten
-- by the ETL process. FK target for prd_v2.ReportObjectRunData.RunUserID.
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.[User]', 'U') IS NOT NULL
    DROP TABLE prd_v2.[User];
GO

CREATE TABLE prd_v2.[User] (
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
    Base            NVARCHAR(MAX) NULL,
    EpicId          NVARCHAR(MAX) NULL,
    LastLoadDate    DATETIME NULL,
    LastLogin       DATETIME NULL,
    Fullname_calc   NVARCHAR(MAX) NULL,
    Firstname_calc  NVARCHAR(MAX) NULL,
    ProfilePhoto    NVARCHAR(MAX) NULL,
    CONSTRAINT PK_prd_v2_User
        PRIMARY KEY CLUSTERED (UserID)
);
CREATE NONCLUSTERED INDEX IX_prd_v2_User_Username
    ON prd_v2.[User] (Username);
PRINT 'Created IX_prd_v2_User_Username';
PRINT 'Created prd_v2.[User]';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 6. CREATE DoNotOrphanTypes TABLE (configuration)
-- Uses ReportObjectTypeID (INT PK) to match Atlas_Prd.dbo.ReportObjectType.
-- usp_Atlas_PostProcessing Step 1 joins on ReportObjectTypeID.
-- Consolidated from 04_fix_DoNotOrphanTypes.sql (2026-03-26).
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.DoNotOrphanTypes', 'U') IS NOT NULL
    DROP TABLE prd_v2.DoNotOrphanTypes;
GO

CREATE TABLE prd_v2.DoNotOrphanTypes (
    ReportObjectTypeID  INT NOT NULL PRIMARY KEY,
    TypeName            NVARCHAR(100) NULL,  -- For reference only
    IsActive            BIT NOT NULL DEFAULT 1,
    Notes               NVARCHAR(500) NULL,
    CreatedDate         DATETIME NOT NULL DEFAULT GETDATE()
);

-- Auto-populate from Atlas_Prd if available
IF EXISTS (SELECT 1 FROM Atlas_Prd.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ReportObjectType')
BEGIN
    INSERT INTO prd_v2.DoNotOrphanTypes (ReportObjectTypeID, TypeName, Notes)
    SELECT ReportObjectTypeID, Name, 'Auto-populated from Atlas_Prd.dbo.ReportObjectType'
    FROM Atlas_Prd.dbo.ReportObjectType
    WHERE Name IN (
        'SSRS Folder',
        'SSRS Datasource',
        'SSRS Linked Report',
        'Tableau Folder',
        'SQL View',
        'SQL Stored Procedure'
    );
    PRINT 'Populated DoNotOrphanTypes from Atlas_Prd.dbo.ReportObjectType';
END
ELSE
BEGIN
    PRINT 'NOTE: Atlas_Prd.dbo.ReportObjectType not found — DoNotOrphanTypes empty.';
    PRINT 'Manually insert: INSERT INTO prd_v2.DoNotOrphanTypes (ReportObjectTypeID, TypeName) VALUES (1, ''SSRS Folder'');';
END
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 4. CREATE URLOverrides TABLE (configuration)
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.URLOverrides', 'U') IS NOT NULL
    DROP TABLE prd_v2.URLOverrides;
GO

CREATE TABLE prd_v2.URLOverrides (
    BizKey          NVARCHAR(500) NOT NULL PRIMARY KEY,
    OverrideURL     NVARCHAR(1000) NOT NULL,
    IsActive        BIT NOT NULL DEFAULT 1,
    Notes           NVARCHAR(500) NULL,
    CreatedDate     DATETIME NOT NULL DEFAULT GETDATE(),
    ModifiedDate    DATETIME NULL
);

PRINT 'Created prd_v2.URLOverrides (empty - add overrides as needed)';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 5. CREATE VisibilityRules TABLE (configuration)
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.VisibilityRules', 'U') IS NOT NULL
    DROP TABLE prd_v2.VisibilityRules;
GO

CREATE TABLE prd_v2.VisibilityRules (
    RuleID          INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    RuleName        NVARCHAR(100) NOT NULL,
    MatchType       NVARCHAR(50) NOT NULL,      -- ObjectType, PathContains, NameContains, SourceSystem
    MatchValue      NVARCHAR(500) NOT NULL,
    Action          NVARCHAR(10) NOT NULL,      -- Hide, Show
    IsActive        BIT NOT NULL DEFAULT 1,
    Priority        INT NOT NULL DEFAULT 100,
    Notes           NVARCHAR(500) NULL,
    CreatedDate     DATETIME NOT NULL DEFAULT GETDATE()
);

-- Insert sample visibility rules
INSERT INTO prd_v2.VisibilityRules (RuleName, MatchType, MatchValue, Action, Notes) VALUES
    ('Hide Test Reports', 'NameContains', '_TEST', 'Hide', 'Hide reports with _TEST in name'),
    ('Hide Dev Folder', 'PathContains', '/Development/', 'Hide', 'Hide development folder contents'),
    ('Hide Backup Reports', 'NameContains', '_BAK', 'Hide', 'Hide backup copies of reports'),
    ('Hide Draft Reports', 'NameContains', '_DRAFT', 'Hide', 'Hide draft reports');

PRINT 'Created prd_v2.VisibilityRules with sample configuration';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 9. CREATE ReportObjectType TABLE (matches Atlas_Prd.dbo.ReportObjectType)
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.ReportObjectType', 'U') IS NOT NULL
    DROP TABLE prd_v2.ReportObjectType;
GO

CREATE TABLE prd_v2.ReportObjectType (
    ReportObjectTypeID    INT IDENTITY(1,1) NOT NULL,
    Name                  NVARCHAR(MAX) NOT NULL,
    DefaultEpicMasterFile NVARCHAR(3) NULL,
    LastLoadDate          DATETIME NULL,
    ShortName             NVARCHAR(MAX) NULL,
    Visible               NVARCHAR(1) NULL,
    CONSTRAINT PK_prd_v2_ReportObjectType
        PRIMARY KEY CLUSTERED (ReportObjectTypeID)
);
PRINT 'Created prd_v2.ReportObjectType';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 10. CREATE ReportObjectTags TABLE (matches Atlas_Prd.dbo.ReportObjectTags)
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.ReportObjectTags', 'U') IS NOT NULL
    DROP TABLE prd_v2.ReportObjectTags;
GO

CREATE TABLE prd_v2.ReportObjectTags (
    TagID                 INT IDENTITY(1,1) NOT NULL,
    EpicTagID             NUMERIC(18,0) NULL,
    TagName               VARCHAR(200) NULL,
    CONSTRAINT PK_prd_v2_ReportObjectTags
        PRIMARY KEY CLUSTERED (TagID)
);
PRINT 'Created prd_v2.ReportObjectTags';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 11. CREATE ReportObjectHierarchy TABLE (matches Atlas_Prd.dbo.ReportObjectHierarchy)
-- Composite PK on (ParentReportObjectID, ChildReportObjectID) — no identity column.
-- BizKey → integer ID resolution happens at MERGE time in usp_Atlas_Merge.
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.ReportObjectHierarchy', 'U') IS NOT NULL
    DROP TABLE prd_v2.ReportObjectHierarchy;
GO

CREATE TABLE prd_v2.ReportObjectHierarchy (
    ParentReportObjectID  INT NOT NULL,
    ChildReportObjectID   INT NOT NULL,
    Line                  INT NULL,
    LastLoadDate          DATETIME NULL,
    CONSTRAINT PK_prd_v2_ReportObjectHierarchy
        PRIMARY KEY CLUSTERED (ParentReportObjectID, ChildReportObjectID)
);
PRINT 'Created prd_v2.ReportObjectHierarchy';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 12. CREATE ReportObjectQuery TABLE (matches Atlas_Prd.dbo.ReportObjectQuery)
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.ReportObjectQuery', 'U') IS NOT NULL
    DROP TABLE prd_v2.ReportObjectQuery;
GO

CREATE TABLE prd_v2.ReportObjectQuery (
    ReportObjectQueryId   INT IDENTITY(1,1) NOT NULL,
    ReportObjectId        INT NOT NULL,
    Query                 NVARCHAR(MAX) NULL,
    LastLoadDate          DATETIME NULL,
    SourceServer          NVARCHAR(MAX) NULL,
    Language              NVARCHAR(MAX) NULL,
    Name                  NVARCHAR(MAX) NULL,
    CONSTRAINT PK_prd_v2_ReportObjectQuery
        PRIMARY KEY CLUSTERED (ReportObjectQueryId)
);
PRINT 'Created prd_v2.ReportObjectQuery';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 13. CREATE ReportObjectParameters TABLE (matches Atlas_Prd.dbo.ReportObjectParameters)
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.ReportObjectParameters', 'U') IS NOT NULL
    DROP TABLE prd_v2.ReportObjectParameters;
GO

CREATE TABLE prd_v2.ReportObjectParameters (
    ReportObjectParameterId  INT IDENTITY(1,1) NOT NULL,
    ReportObjectId           INT NOT NULL,
    ParameterName            NVARCHAR(MAX) NULL,
    ParameterValue           NVARCHAR(MAX) NULL,
    IntraParameterLogic      NVARCHAR(MAX) NULL,
    Operator                 NVARCHAR(MAX) NULL,
    CONSTRAINT PK_prd_v2_ReportObjectParameters
        PRIMARY KEY CLUSTERED (ReportObjectParameterId)
);
PRINT 'Created prd_v2.ReportObjectParameters';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 14. CREATE ReportObjectAttachments TABLE (matches Atlas_Prd.dbo.ReportObjectAttachments)
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.ReportObjectAttachments', 'U') IS NOT NULL
    DROP TABLE prd_v2.ReportObjectAttachments;
GO

CREATE TABLE prd_v2.ReportObjectAttachments (
    ReportObjectAttachmentId  INT IDENTITY(1,1) NOT NULL,
    ReportObjectId            INT NOT NULL,
    Name                      NVARCHAR(MAX) NOT NULL,
    Path                      NVARCHAR(MAX) NOT NULL,
    CreationDate              DATETIME NULL,
    Source                    NVARCHAR(MAX) NULL,
    Type                      NVARCHAR(MAX) NULL,
    LastLoadDate              DATETIME NULL,
    CONSTRAINT PK_prd_v2_ReportObjectAttachments
        PRIMARY KEY CLUSTERED (ReportObjectAttachmentId)
);
PRINT 'Created prd_v2.ReportObjectAttachments';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 15. CREATE ReportObjectSubscriptions TABLE (matches Atlas_Prd.dbo.ReportObjectSubscriptions)
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.ReportObjectSubscriptions', 'U') IS NOT NULL
    DROP TABLE prd_v2.ReportObjectSubscriptions;
GO

CREATE TABLE prd_v2.ReportObjectSubscriptions (
    ReportObjectSubscriptionsId  INT IDENTITY(1,1) NOT NULL,
    ReportObjectId               INT NULL,
    UserId                       INT NULL,
    SubscriptionId               NVARCHAR(MAX) NULL,
    InactiveFlags                INT NULL,
    EmailList                    NVARCHAR(MAX) NULL,
    Description                  NVARCHAR(MAX) NULL,
    LastStatus                   NVARCHAR(MAX) NULL,
    LastRunTime                  DATETIME NULL,
    SubscriptionTo               NVARCHAR(MAX) NULL,
    LastLoadDate                 DATETIME NULL,
    CONSTRAINT PK_prd_v2_ReportObjectSubscriptions
        PRIMARY KEY CLUSTERED (ReportObjectSubscriptionsId)
);
PRINT 'Created prd_v2.ReportObjectSubscriptions';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 16. CREATE ReportObjectTagMemberships TABLE (matches Atlas_Prd.dbo.ReportObjectTagMemberships)
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.ReportObjectTagMemberships', 'U') IS NOT NULL
    DROP TABLE prd_v2.ReportObjectTagMemberships;
GO

CREATE TABLE prd_v2.ReportObjectTagMemberships (
    TagMembershipID       INT IDENTITY(1,1) NOT NULL,
    ReportObjectID        INT NOT NULL,
    TagID                 INT NOT NULL,
    Line                  INT NULL,
    CONSTRAINT PK_prd_v2_ReportObjectTagMemberships
        PRIMARY KEY CLUSTERED (TagMembershipID)
);
PRINT 'Created prd_v2.ReportObjectTagMemberships';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 17. CREATE UserGroups TABLE (matches Atlas_Prd.dbo.UserGroups)
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.UserGroups', 'U') IS NOT NULL
    DROP TABLE prd_v2.UserGroups;
GO

CREATE TABLE prd_v2.UserGroups (
    GroupId               INT IDENTITY(1,1) NOT NULL,
    AccountName           NVARCHAR(MAX) NULL,
    GroupName             NVARCHAR(MAX) NULL,
    GroupEmail            NVARCHAR(MAX) NULL,
    GroupType             NVARCHAR(MAX) NULL,
    GroupSource           NVARCHAR(MAX) NULL,
    LastLoadDate          DATETIME NULL,
    EpicId                NVARCHAR(MAX) NULL,
    CONSTRAINT PK_prd_v2_UserGroups
        PRIMARY KEY CLUSTERED (GroupId)
);
PRINT 'Created prd_v2.UserGroups';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 18. CREATE UserGroupsMembership TABLE (matches Atlas_Prd.dbo.UserGroupsMembership)
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('prd_v2.UserGroupsMembership', 'U') IS NOT NULL
    DROP TABLE prd_v2.UserGroupsMembership;
GO

CREATE TABLE prd_v2.UserGroupsMembership (
    MembershipId          INT IDENTITY(1,1) NOT NULL,
    UserId                INT NOT NULL,
    GroupId               INT NOT NULL,
    LastLoadDate          DATETIME NULL,
    CONSTRAINT PK_prd_v2_UserGroupsMembership
        PRIMARY KEY CLUSTERED (MembershipId)
);
PRINT 'Created prd_v2.UserGroupsMembership';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 19. VERIFICATION
-- ══════════════════════════════════════════════════════════════════════════════

PRINT '';
PRINT '════════════════════════════════════════════════════════════';
PRINT 'prd_v2 Schema Setup Complete';
PRINT '════════════════════════════════════════════════════════════';
PRINT '';

SELECT 'prd_v2.ReportObjects' AS TableName, COUNT(*) AS [RowCount] FROM prd_v2.ReportObjects
UNION ALL SELECT 'prd_v2.ReportObjectRunData', COUNT(*) FROM prd_v2.ReportObjectRunData
UNION ALL SELECT 'prd_v2.ReportObjectRunDataBridge', COUNT(*) FROM prd_v2.ReportObjectRunDataBridge
UNION ALL SELECT 'prd_v2.[User]', COUNT(*) FROM prd_v2.[User]
UNION ALL SELECT 'prd_v2.ReportObjectType', COUNT(*) FROM prd_v2.ReportObjectType
UNION ALL SELECT 'prd_v2.ReportObjectTags', COUNT(*) FROM prd_v2.ReportObjectTags
UNION ALL SELECT 'prd_v2.ReportObjectHierarchy', COUNT(*) FROM prd_v2.ReportObjectHierarchy
UNION ALL SELECT 'prd_v2.ReportObjectQuery', COUNT(*) FROM prd_v2.ReportObjectQuery
UNION ALL SELECT 'prd_v2.ReportObjectParameters', COUNT(*) FROM prd_v2.ReportObjectParameters
UNION ALL SELECT 'prd_v2.ReportObjectAttachments', COUNT(*) FROM prd_v2.ReportObjectAttachments
UNION ALL SELECT 'prd_v2.ReportObjectSubscriptions', COUNT(*) FROM prd_v2.ReportObjectSubscriptions
UNION ALL SELECT 'prd_v2.ReportObjectTagMemberships', COUNT(*) FROM prd_v2.ReportObjectTagMemberships
UNION ALL SELECT 'prd_v2.UserGroups', COUNT(*) FROM prd_v2.UserGroups
UNION ALL SELECT 'prd_v2.UserGroupsMembership', COUNT(*) FROM prd_v2.UserGroupsMembership
UNION ALL SELECT 'prd_v2.DoNotOrphanTypes', COUNT(*) FROM prd_v2.DoNotOrphanTypes
UNION ALL SELECT 'prd_v2.URLOverrides', COUNT(*) FROM prd_v2.URLOverrides
UNION ALL SELECT 'prd_v2.VisibilityRules', COUNT(*) FROM prd_v2.VisibilityRules;

PRINT '';
PRINT 'You can now run etl.usp_Atlas_PostProcessing with:';
PRINT '  @AtlasProdDatabase = ''Atlas_Staging''';
PRINT '  @ProdSchema = ''prd_v2''';
PRINT '';
GO
