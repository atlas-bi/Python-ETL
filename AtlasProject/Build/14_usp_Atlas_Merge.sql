/*******************************************************************************
 * Atlas ETL Migration — Week 4: usp_Atlas_Merge
 * 
 * Replaces: ETL-Merge SSIS Package (18 Execute SQL Tasks, 0 Data Flows)
 *
 * Consolidates staged data from stage_v2 tables into prd_v2 production
 * tables. This is a SQL-only package — no data flows or external processes.
 *
 * The 18 operations are organized into two phases:
 *   Phase A: Core MERGE operations (11 tasks)
 *     6.  Merge Report Types (→ prd_v2.ReportObjectType) — executes first; matches SSIS order
 *     1.  Merge Reports
 *     2.  Merge Groups (→ prd_v2.UserGroups)
 *     3.  Merge Group Memberships (→ prd_v2.UserGroupsMembership)
 *     4.  Merge Queries (→ prd_v2.ReportObjectQuery)
 *     5.  Merge Hierarchies (→ prd_v2.ReportObjectHierarchy)
 *     7.  Merge Report Tags (→ prd_v2.ReportObjectTags)
 *     8.  Merge Tag Memberships (→ prd_v2.ReportObjectTagMemberships)
 *     9.  Merge Parameters (→ prd_v2.ReportObjectParameters)
 *     [10. Merge Attachments — REMOVED v4.3: staging table dropped, always 0 rows]
 *     [11. Merge Subscriptions — REMOVED v4.3: staging table dropped, always 0 rows]
 *
 *   Phase B: Enrichment & Business Rules (7 tasks)
 *     12. Clean Users (ProperCase, Fullname standardization)
 *     13. EpicReleased (flag vendor-provided content)
 *     14. Hide Reports (stale usage threshold)
 *     15. Cubes Run Link (generate cube URLs)
 *     16. Update Repo Description (from EpicReportLibrary)
 *     17. Update Repo Images (from EpicReportLibrary)
 *     18. Update Certification Tag (dynamic SQL from GlobalSiteSettings)
 *
 * NOTE: Merge Users removed (was task 2). prd_v2.[User] is now
 * written exclusively by etl.usp_Atlas_LDAP Step 4 (Pipeline B).
 *
 * SSIS Parameters → SP Parameters:
 *   @DG_DB        → production database  (was Data_Governance_InitialCatalog)
 *   @DG_STAGE_DB  → staging database     (was DG_Staging_InitialCatalog)
 *   @ORG_AD_NAME  → AD domain name       (was Org_AD_Name)
 *
 * Dependencies:
 *   - Upstream: All extraction packages (Clarity, LDAP, DatabaseObjects,
 *     RunData) must have completed to populate stage_v2 tables.
 *   - Schemas: stage_v2, prd_v2, etl (Week 1)
 *   - Logging: etl.usp_Atlas_LogStart/LogEnd/LogError (Week 1)
 *   - Optional: EpicReportLibrary database (for tasks 17-18; skipped if
 *     unavailable)
 *
 * Execution:
 *   EXEC etl.usp_Atlas_Merge;
 *   EXEC etl.usp_Atlas_Merge @SkipEnrichment = 1;   -- core merges only
 *   EXEC etl.usp_Atlas_Merge @Debug = 1;             -- print diagnostics
 *
 * Author:  Larry Duren
 * Date:    February 2026
 * Version: 4.3 (Week 4)
 *
 * Change Log:
 *   v4.3 2026-04-03 — Removed Merge 10 (Attachments) and Merge 11
 *     (Subscriptions). Both referenced staging tables that were dropped
 *     from Atlas_Staging (ReportObjectAttachmentStaging,
 *     ReportObjectSubscriptionsStaging). Neither was ever populated;
 *     both always returned 0 rows. No downstream consumers.
 ******************************************************************************/

USE Atlas_Staging;
GO

IF OBJECT_ID('etl.usp_Atlas_Merge', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_Merge;
GO

CREATE PROCEDURE etl.usp_Atlas_Merge
    @ExecutionID        UNIQUEIDENTIFIER = NULL,
    @ORG_AD_NAME        NVARCHAR(100)    = 'MR1',
    @StagingSchema      NVARCHAR(128)    = 'stage_v2',
    @ProdSchema         NVARCHAR(128)    = 'prd_v2',
    @ProdDatabase       NVARCHAR(128)    = '',
    @SkipPrdCreation    BIT              = 0,
    @SkipEnrichment     BIT              = 0,       -- 1 = skip Phase B
    @SkipEpicRepo       BIT              = 0,       -- 1 = skip EpicReportLibrary tasks
    @HideReportMonths   INT              = 18,      -- months without usage before hiding
    @Debug              BIT              = 0,
    @RaiseErrorOnFail   BIT              = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageName    NVARCHAR(100) = N'ETL-Merge';
    DECLARE @LogID          BIGINT;
    DECLARE @StepSequence   INT = 0;
    DECLARE @StepName       NVARCHAR(200);
    DECLARE @RowCount       INT;
    DECLARE @ErrorMessage   NVARCHAR(4000);
    DECLARE @SQL            NVARCHAR(MAX);
    DECLARE @StartTime      DATETIME = GETDATE();
    DECLARE @StepStart      DATETIME;

    -- Fully-qualified schema prefixes for dynamic SQL
    DECLARE @StgPrefix      NVARCHAR(300) = QUOTENAME(@StagingSchema) + N'.';
    DECLARE @PrdPrefix      NVARCHAR(300) =
        CASE
            WHEN @ProdDatabase = N'' OR @ProdDatabase IS NULL
                THEN QUOTENAME(@ProdSchema) + N'.'
            ELSE QUOTENAME(@ProdDatabase) + N'.' + QUOTENAME(@ProdSchema) + N'.'
        END;

    -- prd_v2 (dev) uses 'ReportObjects' (plural); Atlas_Prd.dbo (prod) uses
    -- 'ReportObject' (singular). Resolve at runtime so dynamic SQL hits the
    -- right table in both environments. Same pattern as 13_usp_Atlas_RunData.
    DECLARE @PrdReportObjectTable NVARCHAR(128) =
        CASE
            WHEN @ProdDatabase = N'' OR @ProdDatabase IS NULL
                THEN N'ReportObjects'
            ELSE N'ReportObject'
        END;

    IF @ExecutionID IS NULL
        SET @ExecutionID = NEWID();

    -- Log package start
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart
        @ExecutionID  = @ExecutionID,
        @PackageName  = @PackageName,
        @StepName     = N'Package Start',
        @StepSequence = @StepSequence,
        @LogID        = @LogID OUTPUT;

    IF @Debug = 1
    BEGIN
        PRINT '=== usp_Atlas_Merge — DEBUG MODE ===';
        PRINT 'Start:            ' + CONVERT(VARCHAR(30), @StartTime, 121);
        PRINT 'Staging Schema:   ' + @StagingSchema;
        PRINT 'Prod Schema:      ' + @ProdSchema;
        PRINT 'Org AD Name:      ' + @ORG_AD_NAME;
        PRINT 'Skip Enrichment:  ' + CAST(@SkipEnrichment AS VARCHAR(1));
        PRINT 'Skip Epic Repo:   ' + CAST(@SkipEpicRepo AS VARCHAR(1));
        PRINT '';
    END;


    -- =========================================================================
    -- PHASE A: ENSURE PRODUCTION TABLES EXIST
    -- Creates prd_v2 merge targets if not already present.
    -- ReportObjects was created in Week 1 (08_create_prd_v2_schema.sql).
    -- These additional tables are needed for the 12 core merge operations.
    -- =========================================================================

    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Phase 0 - Ensure Production Tables Exist';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID  = @ExecutionID,
        @PackageName  = @PackageName,
        @StepName     = @StepName,
        @StepSequence = @StepSequence,
        @LogID        = @LogID OUTPUT;

    BEGIN TRY
        IF @SkipPrdCreation = 0
        BEGIN
        IF @Debug = 0
        BEGIN
            -- OBJECT_ID resolves names in the SP's home database (Atlas_Staging),
            -- so a schema-only argument can't see Atlas_Prd tables in cutover —
            -- it returns NULL and the CREATE TABLE fires against an existing
            -- table (error 2714). Build a three-part prefix that matches the
            -- target database when @ProdDatabase is set.
            DECLARE @ObjPrefix NVARCHAR(300) =
                CASE
                    WHEN @ProdDatabase = N'' OR @ProdDatabase IS NULL
                        THEN @ProdSchema + N'.'
                    ELSE @ProdDatabase + N'.' + @ProdSchema + N'.'
                END;

            -- NOTE: [User] is managed by etl.usp_Atlas_LDAP Step 7 (Pipeline B).
            --       No fallback CREATE here; DDL is in 03_create_prd_v2_schema.sql.

            -- ReportObjectType (production-aligned)
            IF OBJECT_ID(@ObjPrefix + 'ReportObjectType', 'U') IS NULL
            BEGIN
                SET @SQL = N'CREATE TABLE ' + @PrdPrefix + N'ReportObjectType (
                    ReportObjectTypeID    INT IDENTITY(1,1) NOT NULL,
                    Name                  NVARCHAR(MAX) NOT NULL,
                    DefaultEpicMasterFile NVARCHAR(3) NULL,
                    LastLoadDate          DATETIME NULL,
                    ShortName             NVARCHAR(MAX) NULL,
                    Visible               NVARCHAR(1) NULL,
                    CONSTRAINT PK_' + @ProdSchema + N'_ReportObjectType PRIMARY KEY CLUSTERED (ReportObjectTypeID)
                );';
                EXEC sp_executesql @SQL;
            END;

            -- UserGroups (was ReportObjectGroup; production-aligned)
            IF OBJECT_ID(@ObjPrefix + 'UserGroups', 'U') IS NULL
            BEGIN
                SET @SQL = N'CREATE TABLE ' + @PrdPrefix + N'UserGroups (
                    GroupId               INT IDENTITY(1,1) NOT NULL,
                    AccountName           NVARCHAR(MAX) NULL,
                    GroupName             NVARCHAR(MAX) NULL,
                    GroupEmail            NVARCHAR(MAX) NULL,
                    GroupType             NVARCHAR(MAX) NULL,
                    GroupSource           NVARCHAR(MAX) NULL,
                    LastLoadDate          DATETIME NULL,
                    EpicId                NVARCHAR(MAX) NULL,
                    CONSTRAINT PK_' + @ProdSchema + N'_UserGroups PRIMARY KEY CLUSTERED (GroupId)
                );';
                EXEC sp_executesql @SQL;
            END;

            -- UserGroupsMembership (was ReportObjectGroupMembership; production-aligned)
            IF OBJECT_ID(@ObjPrefix + 'UserGroupsMembership', 'U') IS NULL
            BEGIN
                SET @SQL = N'CREATE TABLE ' + @PrdPrefix + N'UserGroupsMembership (
                    MembershipId          INT IDENTITY(1,1) NOT NULL,
                    UserId                INT NOT NULL,
                    GroupId               INT NOT NULL,
                    LastLoadDate          DATETIME NULL,
                    CONSTRAINT PK_' + @ProdSchema + N'_UserGroupsMembership PRIMARY KEY CLUSTERED (MembershipId)
                );';
                EXEC sp_executesql @SQL;
            END;

            -- ReportObjectQuery (production-aligned)
            IF OBJECT_ID(@ObjPrefix + 'ReportObjectQuery', 'U') IS NULL
            BEGIN
                SET @SQL = N'CREATE TABLE ' + @PrdPrefix + N'ReportObjectQuery (
                    ReportObjectQueryId   INT IDENTITY(1,1) NOT NULL,
                    ReportObjectId        INT NOT NULL,
                    Query                 NVARCHAR(MAX) NULL,
                    LastLoadDate          DATETIME NULL,
                    SourceServer          NVARCHAR(MAX) NULL,
                    Language              NVARCHAR(MAX) NULL,
                    Name                  NVARCHAR(MAX) NULL,
                    CONSTRAINT PK_' + @ProdSchema + N'_ReportObjectQuery PRIMARY KEY CLUSTERED (ReportObjectQueryId)
                );';
                EXEC sp_executesql @SQL;
            END;

            -- ReportObjectHierarchy (production-aligned; composite PK, no identity)
            IF OBJECT_ID(@ObjPrefix + 'ReportObjectHierarchy', 'U') IS NULL
            BEGIN
                SET @SQL = N'CREATE TABLE ' + @PrdPrefix + N'ReportObjectHierarchy (
                    ParentReportObjectID  INT NOT NULL,
                    ChildReportObjectID   INT NOT NULL,
                    Line                  INT NULL,
                    LastLoadDate          DATETIME NULL,
                    CONSTRAINT PK_' + @ProdSchema + N'_ReportObjectHierarchy PRIMARY KEY CLUSTERED (ParentReportObjectID, ChildReportObjectID)
                );';
                EXEC sp_executesql @SQL;
            END;

            -- ReportObjectTags (was ReportObjectTag; production-aligned)
            IF OBJECT_ID(@ObjPrefix + 'ReportObjectTags', 'U') IS NULL
            BEGIN
                SET @SQL = N'CREATE TABLE ' + @PrdPrefix + N'ReportObjectTags (
                    TagID                 INT IDENTITY(1,1) NOT NULL,
                    EpicTagID             NUMERIC(18,0) NULL,
                    TagName               VARCHAR(200) NULL,
                    CONSTRAINT PK_' + @ProdSchema + N'_ReportObjectTags PRIMARY KEY CLUSTERED (TagID)
                );';
                EXEC sp_executesql @SQL;
            END;

            -- ReportObjectTagMemberships (was ReportObjectTagMembership; production-aligned)
            IF OBJECT_ID(@ObjPrefix + 'ReportObjectTagMemberships', 'U') IS NULL
            BEGIN
                SET @SQL = N'CREATE TABLE ' + @PrdPrefix + N'ReportObjectTagMemberships (
                    TagMembershipID       INT IDENTITY(1,1) NOT NULL,
                    ReportObjectID        INT NOT NULL,
                    TagID                 INT NOT NULL,
                    Line                  INT NULL,
                    CONSTRAINT PK_' + @ProdSchema + N'_ReportObjectTagMemberships PRIMARY KEY CLUSTERED (TagMembershipID)
                );';
                EXEC sp_executesql @SQL;
            END;

            -- ReportObjectParameters (was ReportObjectParameter; production-aligned)
            IF OBJECT_ID(@ObjPrefix + 'ReportObjectParameters', 'U') IS NULL
            BEGIN
                SET @SQL = N'CREATE TABLE ' + @PrdPrefix + N'ReportObjectParameters (
                    ReportObjectParameterId  INT IDENTITY(1,1) NOT NULL,
                    ReportObjectId           INT NOT NULL,
                    ParameterName            NVARCHAR(MAX) NULL,
                    ParameterValue           NVARCHAR(MAX) NULL,
                    IntraParameterLogic      NVARCHAR(MAX) NULL,
                    Operator                 NVARCHAR(MAX) NULL,
                    CONSTRAINT PK_' + @ProdSchema + N'_ReportObjectParameters PRIMARY KEY CLUSTERED (ReportObjectParameterId)
                );';
                EXEC sp_executesql @SQL;
            END;

            -- ReportObjectAttachments (was ReportObjectAttachment; production-aligned)
            IF OBJECT_ID(@ObjPrefix + 'ReportObjectAttachments', 'U') IS NULL
            BEGIN
                SET @SQL = N'CREATE TABLE ' + @PrdPrefix + N'ReportObjectAttachments (
                    ReportObjectAttachmentId  INT IDENTITY(1,1) NOT NULL,
                    ReportObjectId            INT NOT NULL,
                    Name                      NVARCHAR(MAX) NOT NULL,
                    Path                      NVARCHAR(MAX) NOT NULL,
                    CreationDate              DATETIME NULL,
                    Source                    NVARCHAR(MAX) NULL,
                    Type                      NVARCHAR(MAX) NULL,
                    LastLoadDate              DATETIME NULL,
                    CONSTRAINT PK_' + @ProdSchema + N'_ReportObjectAttachments PRIMARY KEY CLUSTERED (ReportObjectAttachmentId)
                );';
                EXEC sp_executesql @SQL;
            END;

            -- ReportObjectSubscriptions (was ReportObjectSubscription; production-aligned)
            IF OBJECT_ID(@ObjPrefix + 'ReportObjectSubscriptions', 'U') IS NULL
            BEGIN
                SET @SQL = N'CREATE TABLE ' + @PrdPrefix + N'ReportObjectSubscriptions (
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
                    CONSTRAINT PK_' + @ProdSchema + N'_ReportObjectSubscriptions PRIMARY KEY CLUSTERED (ReportObjectSubscriptionsId)
                );';
                EXEC sp_executesql @SQL;
            END;

            -- ReportObjectImages (app schema equivalent — not renamed)
            IF OBJECT_ID(@ObjPrefix + 'ReportObjectImages', 'U') IS NULL
            BEGIN
                SET @SQL = N'CREATE TABLE ' + @PrdPrefix + N'ReportObjectImages (
                    ImageID             INT IDENTITY(1,1) NOT NULL,
                    ReportObjectID      INT NULL,
                    BizKey              NVARCHAR(500) NULL,
                    ImageData           VARBINARY(MAX) NULL,
                    ImageName           NVARCHAR(500) NULL,
                    SourceSystem        NVARCHAR(50) NULL,
                    ETL_LoadDate        DATETIME NOT NULL DEFAULT GETDATE(),
                    CONSTRAINT PK_' + @ProdSchema + N'_ReportObjectImages PRIMARY KEY CLUSTERED (ImageID)
                );';
                EXEC sp_executesql @SQL;
            END;
        END;
        END; -- @SkipPrdCreation

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = 11,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': 11 production tables verified/created';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();

        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
            PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =========================================================================
    -- =========================================================================
    --                     PHASE A: CORE MERGE OPERATIONS
    -- =========================================================================
    -- =========================================================================


    -- =====================================================================
    -- MERGE 0: Deduplicate staging tables (defensive)
    -- Root cause: COMPONENT_DESC fan-out in usp_Atlas_Clarity Step 8
    -- produces duplicate BizKeys in ReportObjectsStaging. This step
    -- ensures all staging tables are clean before any MERGE runs.
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Merge 0 - Dedup Staging Tables';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID  = @ExecutionID,
        @PackageName  = @PackageName,
        @StepName     = @StepName,
        @StepSequence = @StepSequence,
        @LogID        = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            -- ReportObjectsStaging: ~176K dups from IDB/IDM fan-out
            DELETE DUP FROM (
                SELECT ROW_NUMBER() OVER (PARTITION BY BizKey ORDER BY ExtractDate DESC) AS RowNum
                FROM stage_v2.ReportObjectsStaging
            ) DUP WHERE RowNum > 1;
            SET @RowCount = @@ROWCOUNT;

            -- Defensive dedup for other staging tables.
            -- Partition key is (BizKey, QueryText) to match SSIS Merge
            -- Queries.sql, which partitions on (bizkey, query). QueryType
            -- has only 2 distinct values ('SQL', 'M-Code'), so partitioning
            -- on it would collapse every group of same-BizKey queries sharing
            -- a type into a single row — dropping legitimate multi-QueryText-
            -- per-report variants that SSIS would preserve. Diagnostic on
            -- current data confirmed zero row-count impact (no such
            -- multi-variant reports exist in the BILH snapshot), but the
            -- prior key was semantically wrong and would cause silent loss
            -- as soon as the underlying data acquires any legitimate
            -- multi-variant reports.
            DELETE DUP FROM (
                SELECT ROW_NUMBER() OVER (PARTITION BY BizKey, QueryText ORDER BY ExtractDate DESC) AS RowNum
                FROM stage_v2.ReportObjectQueryStaging
            ) DUP WHERE RowNum > 1;
            SET @RowCount = @RowCount + @@ROWCOUNT;

            DELETE DUP FROM (
                SELECT ROW_NUMBER() OVER (PARTITION BY ParentBizKey, ChildBizKey ORDER BY ExtractDate DESC) AS RowNum
                FROM stage_v2.ReportObjectHierarchyStaging
            ) DUP WHERE RowNum > 1;
            SET @RowCount = @RowCount + @@ROWCOUNT;

            DELETE DUP FROM (
                SELECT ROW_NUMBER() OVER (
                    PARTITION BY EpicRecordID, EpicMasterFile, ParameterName
                    ORDER BY ExtractDate DESC
                ) AS RowNum
                FROM stage_v2.ReportObjectParametersStaging
            ) DUP WHERE RowNum > 1;
            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- ReportObjectUserGroups: Merge 2 uses 6-key compound match
            DELETE DUP FROM (
                SELECT ROW_NUMBER() OVER (
                    PARTITION BY GroupName, SourceSystem, GroupType, EpicId, AccountName, GroupEmail
                    ORDER BY ExtractDate DESC
                ) AS RowNum
                FROM stage_v2.ReportObjectUserGroups
            ) DUP WHERE RowNum > 1;
            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- ReportObjectTagsStaging: Merge 7 uses SELECT DISTINCT (safe)
            -- ReportObjectTagMembershipsStaging: Merge 8 is INSERT-based
            -- No dedup needed for Types (Merge 6 uses SELECT DISTINCT)
        END
        ELSE
        BEGIN
            SET @RowCount = 0;
            PRINT @StepName + ': DEBUG — skipped';
        END;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';

        IF @Debug = 1 OR @RowCount > 0
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' duplicates removed';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- MERGE 0b: Create Indexes for JOIN Performance
    -- (SSIS equivalent: "Add Index" task — first operation in package)
    -- Staging index is DROP/CREATE (table TRUNCATEd each run).
    -- prd_v2 indexes are IF NOT EXISTS (persistent across runs).
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Merge 0b - Create Indexes';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID  = @ExecutionID,
        @PackageName  = @PackageName,
        @StepName     = @StepName,
        @StepSequence = @StepSequence,
        @LogID        = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            -- 1. Staging: membership resolution (recreated each run after TRUNCATE)
            IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_stage_v2_UGMembers_Resolution'
                       AND object_id = OBJECT_ID('stage_v2.ReportObjectUserGroupMembers'))
                DROP INDEX IX_stage_v2_UGMembers_Resolution ON stage_v2.ReportObjectUserGroupMembers;

            CREATE INDEX IX_stage_v2_UGMembers_Resolution
                ON stage_v2.ReportObjectUserGroupMembers (MemberName, GroupName)
                INCLUDE (SourceSystem, GroupType, EpicId);

            -- Support ROW_NUMBER() OVER (PARTITION BY BizKey) dedup in Merge 1 #report_temp build.
            -- stage_v2.ReportObjectsStaging is truncated each run — DROP+CREATE pattern.
            -- Note: 02_usp_Atlas_Setup.sql also creates a plain IX_ReportObjectsStaging_BizKey
            -- (no INCLUDE). This block replaces it with the wider covering index that Merge 1
            -- needs for the #report_temp build (avoids key lookups for ExtractDate sort and
            -- the columns the SELECT projects out).
            IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ReportObjectsStaging_BizKey'
                       AND object_id = OBJECT_ID('stage_v2.ReportObjectsStaging'))
                DROP INDEX IX_ReportObjectsStaging_BizKey ON stage_v2.ReportObjectsStaging;

            CREATE INDEX IX_ReportObjectsStaging_BizKey
                ON stage_v2.ReportObjectsStaging (BizKey)
                INCLUDE (ExtractDate, ObjectType, SourceServer, CreatedBy, ModifiedBy);

            -- prd_v2 indexes removed: Username and GroupName columns are NVARCHAR(MAX)
            -- which cannot be indexed. Matches SSIS production behavior (no indexes on
            -- these columns in Atlas_Prd.dbo.UserGroups or [User]).

            SET @RowCount = 2;  -- 2 indexes created
        END
        ELSE
        BEGIN
            SET @RowCount = 0;
            PRINT @StepName + ': DEBUG — skipped';
        END;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' indexes';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- MERGE 6: Report Types
    -- (SSIS Task 14: Merge Report Types)
    -- Executes before Merge 1 so prd_v2.ReportObjectType is populated
    -- when Merge 1 resolves ReportObjectTypeID. Matches SSIS execution
    -- order: Merge_Report_Types ran before Merge_Reports in ETL-Merge.dtsx.
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Merge 6 - Report Types';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID, @PackageName = @PackageName,
        @StepName = @StepName, @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            -- Extract distinct types from staged reports; TypeName → Name
            SET @SQL = N'
            MERGE ' + @PrdPrefix + N'ReportObjectType AS tgt
            USING (
                SELECT ObjectType AS TypeName,
                       MAX(EpicMasterFile) AS DefaultEpicMasterFile
                FROM ' + @StgPrefix + N'ReportObjectsStaging
                WHERE ObjectType IS NOT NULL
                GROUP BY ObjectType
            ) AS src
                ON tgt.Name = src.TypeName
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.DefaultEpicMasterFile = COALESCE(src.DefaultEpicMasterFile, tgt.DefaultEpicMasterFile),
                    tgt.LastLoadDate = GETDATE()
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (Name, DefaultEpicMasterFile, LastLoadDate)
                VALUES (src.TypeName, src.DefaultEpicMasterFile, GETDATE())
            ;';

            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- MERGE 0c: Merge Users (must run BEFORE Merge 1)
    -- (SSIS Task 17: Merge Users — restored for Pipeline B)
    --
    -- Merge 1 inserts AuthorUserID / LastModifiedByUserID into ReportObject(s)
    -- with FKs to dbo.[User](UserID) (FK__ReportObj__Autho__35682A19,
    -- FK__ReportObj__LastM__365C4E52). Pipeline B originally relied on the
    -- legacy SSIS "Merge Users" task to populate dbo.[User] before reports
    -- merged. That task was dropped when Pipeline B redirected user data to
    -- prd_v2.ReportObjectUser only — leaving Atlas_Prd.dbo.[User] unwritten
    -- and Merge 1 failing with FK error 547 in production.
    --
    -- Source: stage_v2.ReportObjectUser (populated by usp_Atlas_LDAP).
    -- Match key: LOWER(Username) = LOWER(Username) (matches SSIS).
    -- Columns synced: Username, EmployeeID, AccountName, DisplayName,
    --   FullName, FirstName, LastName, Department, Title, Phone, Email,
    --   EpicId, LastLoadDate.
    -- Columns skipped: Base, Fullname_calc, Firstname_calc (not present in
    --   stage_v2.ReportObjectUser — left to whatever the target row holds).
    -- Delete policy: WHEN NOT MATCHED BY SOURCE → no-op (matches SSIS;
    --   historical users are retained even after they leave the directory).
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Merge 0c - Merge Users';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID  = @ExecutionID,
        @PackageName  = @PackageName,
        @StepName     = @StepName,
        @StepSequence = @StepSequence,
        @LogID        = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            SET @SQL = N'
            DROP TABLE IF EXISTS #user_temp;

            -- Dedup by Username, blank → NULL, take MAX of each attribute.
            SELECT
                Username,
                MAX(CASE WHEN EmployeeID  = '''' THEN NULL ELSE EmployeeID  END) AS EmployeeID,
                MAX(CASE WHEN AccountName = '''' THEN NULL ELSE AccountName END) AS AccountName,
                MAX(CASE WHEN ISNULL(DisplayName, '''') = '''' THEN FullName ELSE DisplayName END) AS DisplayName,
                MAX(CASE WHEN FullName    = '''' THEN NULL ELSE FullName    END) AS FullName,
                MAX(CASE WHEN FirstName   = '''' THEN NULL ELSE FirstName   END) AS FirstName,
                MAX(CASE WHEN LastName    = '''' THEN NULL ELSE LastName    END) AS LastName,
                MAX(CASE WHEN Department  = '''' THEN NULL ELSE Department  END) AS Department,
                MAX(CASE WHEN Title       = '''' THEN NULL ELSE Title       END) AS Title,
                MAX(CASE WHEN Phone       = '''' THEN NULL ELSE Phone       END) AS Phone,
                MAX(CASE WHEN Email       = '''' THEN NULL ELSE Email       END) AS Email,
                MAX(CASE WHEN EpicId      = '''' THEN NULL ELSE EpicId      END) AS EpicId
            INTO #user_temp
            FROM ' + @StgPrefix + N'ReportObjectUser
            WHERE Username IS NOT NULL AND Username <> ''''
            GROUP BY Username;

            MERGE ' + @PrdPrefix + N'[User] AS tgt
            USING #user_temp AS src
                ON LOWER(tgt.Username) = LOWER(src.Username)
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.EmployeeID   = src.EmployeeID,
                    tgt.AccountName  = src.AccountName,
                    tgt.DisplayName  = src.DisplayName,
                    tgt.FullName     = src.FullName,
                    tgt.FirstName    = src.FirstName,
                    tgt.LastName     = src.LastName,
                    tgt.Department   = src.Department,
                    tgt.Title        = src.Title,
                    tgt.Phone        = src.Phone,
                    tgt.Email        = src.Email,
                    tgt.EpicId       = src.EpicId,
                    tgt.LastLoadDate = GETDATE()
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    Username, EmployeeID, AccountName, DisplayName, FullName,
                    FirstName, LastName, Department, Title, Phone, Email,
                    EpicId, LastLoadDate
                )
                VALUES (
                    src.Username, src.EmployeeID, src.AccountName, src.DisplayName, src.FullName,
                    src.FirstName, src.LastName, src.Department, src.Title, src.Phone, src.Email,
                    src.EpicId, GETDATE()
                );

            DROP TABLE IF EXISTS #user_temp;
            ';

            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        -- Hard dependency for Merge 1 — always raise so cutover doesn't
        -- proceed with an unpopulated dbo.[User].
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- MERGE 1: Reports (master report catalog)
    -- (SSIS Task 15: Merge Reports)
    -- #report_temp -> dedup -> MERGE into prd_v2.ReportObjects
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Merge 1 - Reports';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID  = @ExecutionID,
        @PackageName  = @PackageName,
        @StepName     = @StepName,
        @StepSequence = @StepSequence,
        @LogID        = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            -- ────────────────────────────────────────────────────────────────
            -- STEP 1: Pre-create #user_lookup in the SP scope (visible to
            -- subsequent sp_executesql calls), then INSERT INTO it via
            -- dynamic SQL that routes [User] through @PrdPrefix. Splitting
            -- temp-table creation from the dynamic SQL avoids parser issues
            -- that surfaced in the previous one-big-batch approach
            -- (SHA f0bf735 — error 102 'Incorrect syntax near ,' in prod).
            --
            -- Collapses the 186K-row [User] NVARCHAR(MAX) Username column
            -- into an indexed NVARCHAR(200) temp table. MaxUsernameLen
            -- confirmed 96 chars in live data — NVARCHAR(200) is safe.
            -- ────────────────────────────────────────────────────────────────
            IF OBJECT_ID('tempdb..#user_lookup') IS NOT NULL DROP TABLE #user_lookup;

            CREATE TABLE #user_lookup (
                UserID    INT             NULL,
                Username  NVARCHAR(200)   NULL
            );

            SET @SQL = N'
            INSERT INTO #user_lookup (UserID, Username)
            SELECT MIN(u.UserID),
                   CAST(u.Username AS NVARCHAR(200))
            FROM ' + @PrdPrefix + N'[User] u
            WHERE u.Username IS NOT NULL
            GROUP BY CAST(u.Username AS NVARCHAR(200));';
            EXEC sp_executesql @SQL;

            CREATE CLUSTERED INDEX IX_ul_Username ON #user_lookup (Username);

            -- ────────────────────────────────────────────────────────────────
            -- STEP 2: Pre-create #report_temp, then INSERT INTO via dynamic
            -- SQL that routes ReportObjectType through @PrdPrefix.
            -- Pre-resolves user IDs, type IDs, SourceDB substring,
            -- Availability default, and EpicMasterFile width truncation so
            -- the MERGE is a thin pass over an already-shaped source.
            -- ────────────────────────────────────────────────────────────────
            IF OBJECT_ID('tempdb..#report_temp') IS NOT NULL DROP TABLE #report_temp;

            CREATE TABLE #report_temp (
                BizKey                NVARCHAR(500)   NULL,
                ObjectName            NVARCHAR(500)   NULL,
                ObjectType            NVARCHAR(100)   NULL,
                ObjectPath            NVARCHAR(1000)  NULL,
                ObjectDescription     NVARCHAR(MAX)   NULL,
                SourceSystem          NVARCHAR(50)    NULL,
                SourceServer          NVARCHAR(200)   NULL,
                ModifiedDate          DATETIME        NULL,
                IsHidden              BIT             NULL,
                ObjectURL             NVARCHAR(1000)  NULL,
                EpicMasterFile        NVARCHAR(3)     NULL,
                EpicRecordID          NUMERIC(18, 0)  NULL,
                EpicReportTemplateId  NUMERIC(18, 0)  NULL,
                Availability          NVARCHAR(MAX)   NULL,
                SourceDB              NVARCHAR(500)   NULL,
                ReportObjectTypeID    INT             NULL,
                AuthorUserID          INT             NULL,
                LastModifiedByUserID  INT             NULL
            );

            SET @SQL = N'
            INSERT INTO #report_temp (
                BizKey, ObjectName, ObjectType, ObjectPath, ObjectDescription,
                SourceSystem, SourceServer, ModifiedDate, IsHidden, ObjectURL,
                EpicMasterFile, EpicRecordID, EpicReportTemplateId,
                Availability, SourceDB, ReportObjectTypeID,
                AuthorUserID, LastModifiedByUserID
            )
            SELECT
                src_inner.BizKey,
                src_inner.ObjectName,
                src_inner.ObjectType,
                src_inner.ObjectPath,
                src_inner.ObjectDescription,
                src_inner.SourceSystem,
                src_inner.SourceServer,
                src_inner.ModifiedDate,
                src_inner.IsHidden,
                src_inner.ObjectURL,
                LEFT(src_inner.EpicMasterFile, 3),
                src_inner.EpicRecordID,
                src_inner.EpicReportTemplateId,
                ISNULL(src_inner.Availability, ''Public''),
                ISNULL(
                    SUBSTRING(src_inner.BizKey,
                        CHARINDEX(''||'', src_inner.BizKey) + 2,
                        CHARINDEX(''||'', src_inner.BizKey,
                            CHARINDEX(''||'', src_inner.BizKey) + 2)
                            - CHARINDEX(''||'', src_inner.BizKey) - 2),
                    ''''),
                rt.ReportObjectTypeID,
                ul_a.UserID,
                ul_m.UserID
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY BizKey ORDER BY ExtractDate DESC) AS RowNum
                FROM stage_v2.ReportObjectsStaging
            ) src_inner
            LEFT JOIN ' + @PrdPrefix + N'ReportObjectType rt
                ON rt.Name = src_inner.ObjectType
            LEFT JOIN #user_lookup ul_a
                ON ul_a.Username = src_inner.CreatedBy
            LEFT JOIN #user_lookup ul_m
                ON ul_m.Username = src_inner.ModifiedBy
            WHERE src_inner.RowNum = 1;';
            EXEC sp_executesql @SQL;

            CREATE CLUSTERED INDEX IX_rt_BizKey ON #report_temp (BizKey);

            -- ────────────────────────────────────────────────────────────────
            -- STEP 3: MERGE into production. Dynamic SQL retained for
            -- @PrdPrefix and @PrdReportObjectTable substitution.
            --
            -- DEVIATION 1: MERGE ON uses tgt.BizKey (NVARCHAR(500), backed by
            -- IX_prd_v2_ReportObjects_BizKey) instead of tgt.ReportObjectBizKey
            -- (NVARCHAR(MAX), unindexable). Same pattern as commits c93d223
            -- (Phase 12) and e428d35 (Merges 4/5/8). Both columns hold the
            -- same value because the UPDATE/INSERT below writes both.
            -- DEVIATION 2: AuthorUserID/LastModifiedByUserID resolved via the
            -- pre-materialized #user_lookup instead of correlated subqueries.
            -- DEVIATION 3: Availability defaulted to 'Public' in #report_temp.
            -- DEVIATION 4: LEFT(EpicMasterFile, 3) — staging is NVARCHAR(10),
            -- target is NVARCHAR(3); confirmed max len = 3 in live data.
            -- DEVIATION 5: WHEN NOT MATCHED BY SOURCE flips
            -- OrphanedReportObjectYN to 'Y' for target rows whose BizKey is
            -- no longer present in the staging set.
            -- ────────────────────────────────────────────────────────────────
            SET @SQL = N'
            MERGE ' + @PrdPrefix + @PrdReportObjectTable + N' AS tgt
            USING #report_temp AS src
                ON tgt.BizKey = src.BizKey
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ReportObjectBizKey      = src.BizKey,
                    tgt.BizKey                  = src.BizKey,
                    tgt.Name                    = src.ObjectName,
                    tgt.ObjectName              = src.ObjectName,
                    tgt.ObjectType              = src.ObjectType,
                    tgt.Description             = src.ObjectDescription,
                    tgt.ReportObjectTypeID      = src.ReportObjectTypeID,
                    tgt.AuthorUserID            = src.AuthorUserID,
                    tgt.LastModifiedByUserID    = src.LastModifiedByUserID,
                    tgt.LastModifiedDate        = src.ModifiedDate,
                    tgt.ReportObjectURL         = src.ObjectURL,
                    tgt.ObjectURL               = src.ObjectURL,
                    tgt.EpicMasterFile          = src.EpicMasterFile,
                    tgt.EpicRecordID            = src.EpicRecordID,
                    tgt.EpicReportTemplateId    = src.EpicReportTemplateId,
                    tgt.DefaultVisibilityYN     = CASE WHEN src.IsHidden = 1 THEN ''N'' ELSE ''Y'' END,
                    tgt.Availability            = src.Availability,
                    tgt.SourceSystem            = src.SourceSystem,
                    tgt.SourceServer            = ISNULL(src.SourceServer, ''''),
                    tgt.SourceDB                = src.SourceDB,
                    tgt.ReportServerPath        = src.ObjectPath,
                    tgt.ObjectPath              = src.ObjectPath,
                    tgt.LastLoadDate            = GETDATE(),
                    tgt.LastSeenDate            = GETDATE(),
                    tgt.OrphanedReportObjectYN  = ''N''
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    ReportObjectBizKey, BizKey, Name, ObjectName, ObjectType,
                    Description, ReportObjectTypeID, AuthorUserID, LastModifiedByUserID,
                    LastModifiedDate, ReportObjectURL, ObjectURL, EpicMasterFile, EpicRecordID,
                    EpicReportTemplateId, DefaultVisibilityYN, Availability, SourceSystem,
                    SourceServer, SourceDB, SourceTable, ReportServerPath, ObjectPath,
                    LastLoadDate, OrphanedReportObjectYN
                )
                VALUES (
                    src.BizKey, src.BizKey, src.ObjectName, src.ObjectName, src.ObjectType,
                    src.ObjectDescription, src.ReportObjectTypeID, src.AuthorUserID,
                    src.LastModifiedByUserID, src.ModifiedDate, src.ObjectURL, src.ObjectURL,
                    src.EpicMasterFile, src.EpicRecordID, src.EpicReportTemplateId,
                    CASE WHEN src.IsHidden = 1 THEN ''N'' ELSE ''Y'' END,
                    src.Availability, src.SourceSystem, ISNULL(src.SourceServer, ''''),
                    src.SourceDB, '''', src.ObjectPath, src.ObjectPath, GETDATE(), ''N''
                )
            WHEN NOT MATCHED BY SOURCE THEN
                UPDATE SET
                    tgt.OrphanedReportObjectYN  = ''Y'',
                    tgt.LastLoadDate            = GETDATE();

            SET @merge_count = @@ROWCOUNT;
            ';

            EXEC sp_executesql @SQL, N'@merge_count INT OUTPUT', @merge_count = @RowCount OUTPUT;

            -- STEP 4: Cleanup
            DROP TABLE IF EXISTS #report_temp;
            DROP TABLE IF EXISTS #user_lookup;
        END
        ELSE
        BEGIN
            SET @SQL = N'SELECT @cnt = COUNT(*) FROM ' + @StgPrefix + N'ReportObjectsStaging;';
            EXEC sp_executesql @SQL, N'@cnt INT OUTPUT', @cnt = @RowCount OUTPUT;
        END;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- MERGE 2: Groups
    -- (SSIS Task 7: Merge Groups)
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Merge 2 - Groups';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID, @PackageName = @PackageName,
        @StepName = @StepName, @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            -- Build #group_temp from UNION of two sources (matches SSIS "Merge Groups" pattern).
            -- Source 1: stage_v2.ReportObjectUserGroups (Azure AD + Clarity groups from LDAP SP)
            -- Source 2: stage_v2.ReportObjectGroupsMemberships (report-to-group mappings from Clarity SP)
            --   LEFT JOIN to existing prd_v2.UserGroups retrieves AccountName/GroupEmail for known groups.
            SET @SQL = N'
            DROP TABLE IF EXISTS #group_temp;

            SELECT * INTO #group_temp FROM (
                SELECT DISTINCT
                    g.AccountName,
                    g.GroupName,
                    g.GroupEmail,
                    g.GroupType,
                    g.SourceSystem          AS GroupSource,
                    g.EpicId
                FROM ' + @StgPrefix + N'ReportObjectUserGroups g

                UNION

                SELECT DISTINCT
                    ug.AccountName,
                    m.GroupName,
                    ug.GroupEmail,
                    m.GroupType,
                    m.GroupSource,
                    CASE WHEN m.GroupSource = ''Clarity''
                         THEN CAST(m.GroupId AS NVARCHAR(MAX))
                         ELSE NULL
                    END                     AS EpicId
                FROM ' + @StgPrefix + N'ReportObjectGroupsMemberships m
                LEFT OUTER JOIN ' + @PrdPrefix + N'UserGroups ug
                    ON TRY_CAST(m.GroupId AS INT) = ug.GroupId
                WHERE m.GroupName != ''''

                UNION

                -- Source 3: Distinct Clarity group signatures from membership staging
                -- Defensive catch-all: ensures every group referenced by a membership
                -- exists in the catalog, even if missed by Sources 1-2.
                SELECT DISTINCT
                    NULL                    AS AccountName,
                    m.GroupName,
                    NULL                    AS GroupEmail,
                    m.GroupType,
                    m.SourceSystem          AS GroupSource,
                    m.EpicId
                FROM ' + @StgPrefix + N'ReportObjectUserGroupMembers m
                WHERE m.GroupName IS NOT NULL
                  AND m.SourceSystem = ''Clarity''
            ) AS t;

            MERGE ' + @PrdPrefix + N'UserGroups AS tgt
            USING #group_temp AS src
                ON  ISNULL(tgt.GroupName, ''asdf'')    = ISNULL(src.GroupName, ''asdf'')
                AND ISNULL(tgt.GroupEmail, ''asdf'')   = ISNULL(src.GroupEmail, ''asdf'')
                AND ISNULL(tgt.AccountName, ''asdf'')  = ISNULL(src.AccountName, ''asdf'')
                AND ISNULL(tgt.GroupType, ''asdf'')    = ISNULL(src.GroupType, ''asdf'')
                AND ISNULL(tgt.GroupSource, ''asdf'')  = ISNULL(src.GroupSource, ''asdf'')
                AND ISNULL(tgt.EpicId, ''asdf'')       = ISNULL(src.EpicId, ''asdf'')
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.AccountName      = COALESCE(src.AccountName, tgt.AccountName),
                    tgt.GroupEmail        = COALESCE(src.GroupEmail, tgt.GroupEmail),
                    tgt.GroupType         = src.GroupType,
                    tgt.GroupSource       = src.GroupSource,
                    tgt.EpicId            = src.EpicId,
                    tgt.LastLoadDate      = GETDATE()
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (AccountName, GroupName, GroupEmail, GroupType, GroupSource, EpicId, LastLoadDate)
                VALUES (src.AccountName, src.GroupName, src.GroupEmail, src.GroupType, src.GroupSource, src.EpicId, GETDATE())
            WHEN NOT MATCHED BY SOURCE
                AND NOT EXISTS (SELECT 1 FROM ' + @PrdPrefix + N'UserGroupsMembership m WHERE tgt.GroupId = m.GroupId)
                AND NOT EXISTS (SELECT 1 FROM dbo.ReportGroupsMemberships m WHERE tgt.GroupId = m.GroupId)
                THEN DELETE
            ;

            DROP TABLE IF EXISTS #group_temp;
            ';

            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- MERGE 3: Report Groups (Group Memberships)
    -- (SSIS Task 10: Merge Report Groups)
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Merge 3 - Report Group Memberships';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID, @PackageName = @PackageName,
        @StepName = @StepName, @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            SET @SQL = N'
            -- Full refresh: delete + insert for membership data
            DELETE FROM ' + @PrdPrefix + N'UserGroupsMembership;

            -- Pass 1: Compound key match (exact, preferred)
            -- Resolve UserName → UserId via [User], GroupName → GroupId via UserGroups
            INSERT INTO ' + @PrdPrefix + N'UserGroupsMembership (
                UserId, GroupId, LastLoadDate
            )
            SELECT DISTINCT
                u.UserID,
                g.GroupId,
                GETDATE()
            FROM ' + @StgPrefix + N'ReportObjectUserGroupMembers m
            INNER JOIN ' + @PrdPrefix + N'[User] u
                ON u.Username = m.MemberName
            INNER JOIN ' + @PrdPrefix + N'UserGroups g
                ON  g.GroupName = m.GroupName
                AND ISNULL(g.GroupSource, ''asdf'') = ISNULL(m.SourceSystem, ''asdf'')
                AND ISNULL(g.GroupType, ''asdf'')   = ISNULL(m.GroupType, ''asdf'')
                AND ISNULL(g.EpicId, ''asdf'')      = ISNULL(m.EpicId, ''asdf'')
            WHERE m.MemberName IS NOT NULL
              AND m.GroupName IS NOT NULL;
            ';

            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- MERGE 4: Queries (report query text / SQL definitions)
    -- (SSIS Task 9: Merge Queries)
    -- Includes ROW_NUMBER deduplication per business key.
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Merge 4 - Queries';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID, @PackageName = @PackageName,
        @StepName = @StepName, @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            SET @SQL = N'
            -- Full refresh with dedup; BizKey → ReportObjectId via JOIN
            DELETE FROM ' + @PrdPrefix + N'ReportObjectQuery;

            INSERT INTO ' + @PrdPrefix + N'ReportObjectQuery (
                ReportObjectId, Query, Language, LastLoadDate
            )
            SELECT
                ro.ReportObjectID,
                deduped.QueryText,
                deduped.QueryType AS Language,
                GETDATE()
            FROM (
                SELECT
                    rqs.BizKey,
                    rqs.QueryText,
                    rqs.QueryType,
                    ROW_NUMBER() OVER (
                        PARTITION BY rqs.BizKey, rqs.QueryText
                        ORDER BY rqs.ExtractDate DESC
                    ) AS RowNum
                FROM ' + @StgPrefix + N'ReportObjectQueryStaging rqs
                WHERE rqs.BizKey IS NOT NULL
            ) deduped
            INNER JOIN ' + @PrdPrefix + @PrdReportObjectTable + N' ro
                ON ro.BizKey = deduped.BizKey
            WHERE deduped.RowNum = 1;
            ';

            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- MERGE 5: Hierarchies (parent-child report relationships)
    -- (SSIS Task 8: Merge Hierarchies)
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Merge 5 - Hierarchies';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID, @PackageName = @PackageName,
        @StepName = @StepName, @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            SET @SQL = N'
            -- BizKey → integer ID resolution via JOIN to ReportObjects
            DELETE FROM ' + @PrdPrefix + N'ReportObjectHierarchy;

            INSERT INTO ' + @PrdPrefix + N'ReportObjectHierarchy (
                ParentReportObjectID, ChildReportObjectID, Line, LastLoadDate
            )
            SELECT
                ro_p.ReportObjectID,
                ro_c.ReportObjectID,
                ROW_NUMBER() OVER (
                    PARTITION BY ro_p.ReportObjectID
                    ORDER BY ro_c.ReportObjectID
                ) AS Line,
                GETDATE()
            FROM ' + @StgPrefix + N'ReportObjectHierarchyStaging h
            INNER JOIN ' + @PrdPrefix + @PrdReportObjectTable + N' ro_p
                ON ro_p.BizKey = h.ParentBizKey
            INNER JOIN ' + @PrdPrefix + @PrdReportObjectTable + N' ro_c
                ON ro_c.BizKey = h.ChildBizKey
            WHERE h.ParentBizKey IS NOT NULL
              AND h.ChildBizKey IS NOT NULL;
            ';

            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- MERGE 7: Report Tags
    -- (SSIS Task 13: Merge Report Tags)
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Merge 7 - Report Tags';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID, @PackageName = @PackageName,
        @StepName = @StepName, @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            SET @SQL = N'
            MERGE ' + @PrdPrefix + N'ReportObjectTags AS tgt
            USING (
                SELECT DISTINCT TagID, TagName
                FROM ' + @StgPrefix + N'ReportObjectTagsStaging
                WHERE TagName IS NOT NULL
            ) AS src
                ON tgt.TagName = src.TagName AND tgt.EpicTagID = src.TagID
            WHEN MATCHED AND tgt.EpicTagID != src.TagID THEN
                UPDATE SET tgt.EpicTagID = src.TagID
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (EpicTagID, TagName) VALUES (src.TagID, src.TagName)
            ;';

            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- MERGE 8: Report Tag Memberships
    -- (SSIS Task 12: Merge Report Tag Memberships)
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Merge 8 - Report Tag Memberships';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID, @PackageName = @PackageName,
        @StepName = @StepName, @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            SET @SQL = N'
            -- BizKey → ReportObjectID, TagName → TagID via JOINs
            DELETE FROM ' + @PrdPrefix + N'ReportObjectTagMemberships;

            INSERT INTO ' + @PrdPrefix + N'ReportObjectTagMemberships (
                ReportObjectID, TagID
            )
            SELECT DISTINCT
                ro.ReportObjectID,
                t.TagID
            FROM ' + @StgPrefix + N'ReportObjectTagMembershipsStaging stg
            INNER JOIN ' + @PrdPrefix + @PrdReportObjectTable + N' ro
                ON ro.BizKey = stg.BizKey
            INNER JOIN ' + @PrdPrefix + N'ReportObjectTags t
                ON t.TagName = stg.TagName
            WHERE stg.BizKey IS NOT NULL AND stg.TagName IS NOT NULL;
            ';

            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- MERGE 9: Report Parameters
    -- (SSIS Task 11: Merge Report Parameters)
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Merge 9 - Report Parameters';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID, @PackageName = @PackageName,
        @StepName = @StepName, @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            SET @SQL = N'
            DELETE FROM ' + @PrdPrefix + N'ReportObjectParameters;

            INSERT INTO ' + @PrdPrefix + N'ReportObjectParameters (
                ReportObjectId, ParameterName, ParameterValue,
                IntraParameterLogic, Operator
            )
            SELECT DISTINCT
                ro.ReportObjectID,
                p.ParameterName,
                p.DefaultValue,
                p.IntraparameterLogic,
                p.Operator
            FROM ' + @StgPrefix + N'ReportObjectParametersStaging p
            INNER JOIN ' + @PrdPrefix + @PrdReportObjectTable + N' ro
                ON ro.EpicRecordID = p.EpicRecordID
                AND ro.EpicMasterFile = p.EpicMasterFile
            WHERE p.EpicRecordID IS NOT NULL
              AND p.EpicMasterFile IS NOT NULL
              AND ro.EpicRecordID IS NOT NULL;
            ';

            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =========================================================================
    -- CHECK: Skip Enrichment?
    -- =========================================================================
    IF @SkipEnrichment = 1
    BEGIN
        IF @Debug = 1
            PRINT '[SKIP] Enrichment phase disabled (@SkipEnrichment = 1)';

        GOTO END_OF_ENRICHMENT;
    END;


    -- =========================================================================
    -- =========================================================================
    --           PHASE B: ENRICHMENT & BUSINESS RULES (7 tasks)
    -- =========================================================================
    -- =========================================================================


    -- =====================================================================
    -- ENRICH 12: Clean Users (standardize names)
    -- (SSIS Task 2: Clean Users)
    -- Applies ProperCase and domain-strip formatting to user display names.
    -- NOTE: Updates DisplayName and Username columns (production-aligned).
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Enrich 12 - Clean Users';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID, @PackageName = @PackageName,
        @StepName = @StepName, @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            -- ProperCase: 'JOHN SMITH' → 'John Smith'
            -- Strip domain prefix: 'MR1\jsmith' → 'jsmith'
            SET @SQL = N'
            UPDATE u
            SET u.DisplayName =
                UPPER(LEFT(u.DisplayName, 1))
                + LOWER(SUBSTRING(u.DisplayName, 2, LEN(u.DisplayName)))
            FROM ' + @PrdPrefix + N'[User] u
            WHERE u.DisplayName IS NOT NULL
              AND u.DisplayName = UPPER(u.DisplayName)
              AND LEN(u.DisplayName) > 1;

            -- Strip domain prefix from Username
            UPDATE u
            SET u.Username = SUBSTRING(u.Username, CHARINDEX(''\'', u.Username) + 1, LEN(u.Username))
            FROM ' + @PrdPrefix + N'[User] u
            WHERE u.Username LIKE ''%\%'';
            ';

            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- ENRICH 13: EpicReleased (flag vendor content)
    -- (SSIS Task 4: EpicReleased)
    -- Reports with EpicMasterFile + low RecordID = Epic-provided content.
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Enrich 13 - EpicReleased Flags';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID, @PackageName = @PackageName,
        @StepName = @StepName, @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            SET @SQL = N'
            UPDATE r
            SET r.EpicReleased = ''Y''
            FROM ' + @PrdPrefix + @PrdReportObjectTable + N' r
            WHERE (
                (r.EpicMasterFile = ''HGR'' AND r.EpicRecordID < 100000)
                OR (r.EpicMasterFile = ''IDM'' AND r.EpicRecordID < 100000)
                OR (r.EpicMasterFile = ''IDB'' AND r.EpicRecordID < 100000)
                OR (r.EpicMasterFile = ''IDK'' AND r.EpicRecordID < 1000000)
                OR (r.EpicMasterFile = ''IDN'' AND r.EpicRecordID < 1000000)
            )
            AND r.RepositoryDescription IS NOT NULL
            AND (r.EpicReleased IS NULL OR r.EpicReleased <> ''Y'');
            ';

            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- ENRICH 14: Hide Reports (stale usage threshold)
    -- (SSIS Task 5: Hide Reports)
    -- Two-pass logic matching SSIS ETL-Merge/SQL/05_Hide_Reports.sql:
    --   Pass 1: Hide reports with no run data in @HideReportMonths,
    --           excluding TypeID 23 and 42.
    --   Pass 2: Hide children whose ALL parents are hidden.
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Enrich 14 - Hide Stale Reports';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID, @PackageName = @PackageName,
        @StepName = @StepName, @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            -- Pass 1: Hide stale reports with no recent run data
            -- REQUIRES VERIFICATION: SSIS also had a DoNotPurge check via
            -- app.ReportObject_doc and a visible ReportObjectType filter.
            -- These reference app schema tables that may not exist in prd_v2.
            -- If prd_v2.ReportObject_doc exists, add:
            --   AND NOT EXISTS (SELECT 1 FROM [prd_v2].ReportObject_doc doc
            --       WHERE doc.ReportObjectID = r.ReportObjectID AND doc.DoNotPurge = 1)
            SET @SQL = N'
            UPDATE r
            SET r.DefaultVisibilityYN = ''N''
            FROM ' + @PrdPrefix + @PrdReportObjectTable + N' r
            WHERE r.DefaultVisibilityYN = ''Y''
              AND r.OrphanedReportObjectYN = ''N''
              AND r.ReportObjectTypeID <> 23
              AND r.ReportObjectTypeID <> 42
              AND (
                  SELECT COUNT(1)
                  FROM ' + @PrdPrefix + N'ReportObjectRunData d
                  INNER JOIN ' + @PrdPrefix + N'ReportObjectRunDataBridge b
                      ON b.RunId = d.RunDataId
                  WHERE b.ReportObjectID = r.ReportObjectID
                    AND d.RunStartTime > DATEADD(MONTH, -' + CAST(@HideReportMonths AS NVARCHAR(10)) + N', GETDATE())
              ) = 0;
            ';

            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;

            -- Pass 2: Hide children whose ALL parents are hidden
            SET @SQL = N'
            UPDATE r
            SET r.DefaultVisibilityYN = ''N''
            FROM ' + @PrdPrefix + @PrdReportObjectTable + N' r
            WHERE r.DefaultVisibilityYN = ''Y''
              AND EXISTS (
                  SELECT 1 FROM ' + @PrdPrefix + N'ReportObjectHierarchy h
                  WHERE h.ChildReportObjectID = r.ReportObjectID
                    AND h.ParentReportObjectID IS NOT NULL
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM ' + @PrdPrefix + N'ReportObjectHierarchy hcheck
                  INNER JOIN ' + @PrdPrefix + @PrdReportObjectTable + N' rpcheck
                      ON hcheck.ParentReportObjectID = rpcheck.ReportObjectID
                  WHERE hcheck.ChildReportObjectID = r.ReportObjectID
                    AND rpcheck.DefaultVisibilityYN = ''Y''
              );
            ';

            EXEC sp_executesql @SQL;
            SET @RowCount = @RowCount + @@ROWCOUNT;
        END
        ELSE SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- ENRICH 15: Cubes Run Link (generate URLs for cube objects)
    -- (SSIS Task 3: Cubes Run Link)
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Enrich 15 - Cubes Run Link';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID, @PackageName = @PackageName,
        @StepName = @StepName, @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            -- Simplified: removed unnecessary JOIN to ReportObjectType
            SET @SQL = N'
            UPDATE r
            SET r.ReportObjectURL = CONCAT(''/data/File?handler=Cube&id='', r.ReportObjectID)
            FROM ' + @PrdPrefix + @PrdReportObjectTable + N' r
            WHERE r.ObjectType = ''SSAS Cube''
              AND r.OrphanedReportObjectYN = ''N'';
            ';

            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- ENRICH 16: Update Repo Description (from EpicReportLibrary)
    -- (SSIS Task 19: Update Repo Description)
    -- Joins with EpicReportLibrary for official Epic documentation.
    -- Skipped if @SkipEpicRepo = 1.
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Enrich 16 - Repo Description';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID, @PackageName = @PackageName,
        @StepName = @StepName, @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        IF @SkipEpicRepo = 0 AND @Debug = 0
        BEGIN
            -- Attempt to update from EpicReportLibrary if accessible.
            -- DB_ID() check prevents compile-time validation of 3-part name
            -- when the database doesn't exist on this server.
            IF DB_ID('EpicReportLibrary') IS NOT NULL
            BEGIN
                SET @SQL = N'
                UPDATE ro
                SET ro.EpicRepoDescription = erl.[Description]
                FROM ' + @PrdPrefix + @PrdReportObjectTable + N' ro
                INNER JOIN EpicReportLibrary.dbo.ReportDocumentation erl
                    ON ro.EpicMasterFile = erl.MasterFile
                    AND ro.EpicRecordID  = erl.RecordID
                WHERE erl.[Description] IS NOT NULL
                  AND (ro.EpicRepoDescription IS NULL
                       OR ro.EpicRepoDescription <> erl.[Description]);
                ';
                EXEC sp_executesql @SQL;
                SET @RowCount = @@ROWCOUNT;
            END
            ELSE
            BEGIN
                PRINT 'EpicReportLibrary not available — skipping description enrichment';
                SET @RowCount = 0;
            END;
        END
        ELSE
        BEGIN
            IF @Debug = 1 AND @SkipEpicRepo = 1
                PRINT '  [SKIP] EpicReportLibrary disabled (@SkipEpicRepo = 1)';
            SET @RowCount = 0;
        END;

        DECLARE @Enrich16Status NVARCHAR(20) = CASE
            WHEN @SkipEpicRepo = 1 OR DB_ID('EpicReportLibrary') IS NULL THEN N'Warning'
            ELSE N'Success' END;
        DECLARE @Enrich16Message NVARCHAR(4000) = CASE
            WHEN @SkipEpicRepo = 1
                THEN N'Skipped — @SkipEpicRepo = 1'
            WHEN DB_ID(N'EpicReportLibrary') IS NULL
                THEN N'EpicReportLibrary database not present on this server'
            ELSE NULL
        END;
        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = @Enrich16Status,
            @Message      = @Enrich16Message;
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows (' + @Enrich16Status + ')';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- ENRICH 17: Update Repo Images (from EpicReportLibrary)
    -- (SSIS Task 20: Update Repo Images)
    -- Refreshes report screenshots from EpicReportLibrary.
    -- Skipped if @SkipEpicRepo = 1.
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Enrich 17 - Repo Images';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID, @PackageName = @PackageName,
        @StepName = @StepName, @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        -- NOTE: When EpicReportLibrary is re-enabled, this block targets
        -- prd_v2.ReportObjectImages in dev but app.ReportObjectImages_doc
        -- in Atlas_Prd.dbo. @PrdReportObjectTable pattern will need
        -- extension to cover this schema/name difference at that time.
        IF @SkipEpicRepo = 0 AND @Debug = 0
        BEGIN
            IF DB_ID('EpicReportLibrary') IS NOT NULL
            BEGIN
                SET @SQL = N'
                -- Delete existing repo images and refresh
                DELETE FROM ' + @PrdPrefix + N'ReportObjectImages
                WHERE SourceSystem = ''EpicReportLibrary'';

                INSERT INTO ' + @PrdPrefix + N'ReportObjectImages (
                    ReportObjectID, BizKey, ImageData, ImageName, SourceSystem
                )
                SELECT
                    ro.ReportObjectID,
                    ro.BizKey,
                    erl.ImageContent,
                    erl.ImageName,
                    ''EpicReportLibrary''
                FROM ' + @PrdPrefix + @PrdReportObjectTable + N' ro
                INNER JOIN EpicReportLibrary.dbo.ReportImages erl
                    ON ro.EpicMasterFile = erl.MasterFile
                    AND ro.EpicRecordID  = erl.RecordID
                WHERE erl.ImageContent IS NOT NULL;
                ';
                EXEC sp_executesql @SQL;
                SET @RowCount = @@ROWCOUNT;
            END
            ELSE
            BEGIN
                PRINT 'EpicReportLibrary not available — skipping image enrichment';
                SET @RowCount = 0;
            END;
        END
        ELSE SET @RowCount = 0;

        DECLARE @Enrich17Status NVARCHAR(20) = CASE
            WHEN @SkipEpicRepo = 1 OR DB_ID('EpicReportLibrary') IS NULL THEN N'Warning'
            ELSE N'Success' END;
        DECLARE @Enrich17Message NVARCHAR(4000) = CASE
            WHEN @SkipEpicRepo = 1
                THEN N'Skipped — @SkipEpicRepo = 1'
            WHEN DB_ID(N'EpicReportLibrary') IS NULL
                THEN N'EpicReportLibrary database not present on this server'
            ELSE NULL
        END;
        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = @Enrich17Status,
            @Message      = @Enrich17Message;
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows (' + @Enrich17Status + ')';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =====================================================================
    -- ENRICH 18: Update Certification Tag (dynamic SQL from config)
    -- (SSIS Task 18: Update Certification Tag)
    -- Executes dynamic tagging logic stored in app.GlobalSiteSettings.
    -- =====================================================================
    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Enrich 18 - Certification Tag';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID, @PackageName = @PackageName,
        @StepName = @StepName, @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            -- Attempt to read dynamic tagging SQL from GlobalSiteSettings
            DECLARE @TagSQL NVARCHAR(MAX) = NULL;

            BEGIN TRY
                -- GlobalSiteSettings lives in the [app] schema (both Atlas_Prd
                -- and Atlas_Staging). @PrdPrefix targets [dbo]/prd_v2, so build
                -- a one-off prefix for this read. In dev we point at
                -- [Atlas_Staging].[dbo] per spec — the table doesn't exist
                -- there, the inner CATCH below absorbs the failure (same
                -- effective behavior as before this fix).
                DECLARE @SettingsPrefix NVARCHAR(300) =
                    CASE
                        WHEN @ProdDatabase = N'' OR @ProdDatabase IS NULL
                            THEN N'[Atlas_Staging].[dbo]'
                        ELSE QUOTENAME(@ProdDatabase) + N'.[app]'
                    END;

                SET @SQL = N'
                SELECT @out = [Value]
                FROM ' + @SettingsPrefix + N'.GlobalSiteSettings
                WHERE [Name] = ''report_tag_etl'';';

                EXEC sp_executesql @SQL, N'@out NVARCHAR(MAX) OUTPUT', @out = @TagSQL OUTPUT;
            END TRY
            BEGIN CATCH
                -- GlobalSiteSettings table may not exist in v2 yet
                SET @TagSQL = NULL;
            END CATCH;

            IF @TagSQL IS NOT NULL AND LEN(@TagSQL) > 0
            BEGIN
                EXEC sp_executesql @TagSQL;
                SET @RowCount = @@ROWCOUNT;
            END
            ELSE
            BEGIN
                SET @RowCount = 0;
                IF @Debug = 1
                    PRINT '  [SKIP] No certification tag SQL found in GlobalSiteSettings';
            END;
        END
        ELSE SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1 PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    END_OF_ENRICHMENT:


    PHASE_COMPLETE:

    EXEC etl.usp_Atlas_LogEnd
        @LogID        = @LogID,
        @RowsAffected = @StepSequence,
        @Status       = N'Success';

    IF @Debug = 1
    BEGIN
        PRINT '';
        PRINT '=== usp_Atlas_Merge Complete ===';
        PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR(10)) + 's';
        PRINT 'Steps Executed: ' + CAST(@StepSequence AS VARCHAR(10));
    END;

    PRINT '';
    PRINT 'ETL-Merge completed. Steps: ' + CAST(@StepSequence AS VARCHAR(10))
          + ', Duration: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR(10)) + 's';

    RETURN 0;

END;
GO

PRINT 'Created procedure: etl.usp_Atlas_Merge (18 operations, stage_v2 → prd_v2)';
GO
