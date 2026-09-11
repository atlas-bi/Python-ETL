/*******************************************************************************
 * Atlas ETL Migration — Week 4: usp_Atlas_RunData (Pipeline B)
 * 
 * Replaces: ETL-RunData SSIS Package (15 SQL Tasks, 5 Data Flow Tasks,
 *           1 Execute Process Task, 27 Precedence Constraints)
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  PIPELINE B AMENDMENT                                                   │
 * │  Phase 2 (5 linked server extractions) moved to Python:                │
 * │      atlas_rundata_extractor.py                                        │
 * │  This SP handles Phases 1, 3-10 (truncate, index, transform, merge).  │
 * │  The orchestrator calls:                                                │
 * │      1. atlas_pbi_events.py           → raw_v2.PbiActivityEvent       │
 * │      2. atlas_rundata_extractor.py    → raw_v2.Clarity* + SlicerDicer*│
 * │      3. usp_Atlas_RunData (this SP)   → Phase 1,3-10                  │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * Dependencies:
 *   - DDL: 01_week4_rundata_ddl.sql
 *   - raw_v2.* tables populated by atlas_rundata_extractor.py + atlas_pbi_events.py
 *   - Logging: etl.usp_Atlas_LogStart, etl.usp_Atlas_LogEnd, etl.usp_Atlas_LogError
 *   - Staging: stage_v2.ReportObjectHierarchyStaging (Week 1)
 *   - NO LINKED SERVERS REQUIRED (Pipeline B)
 *
 * Execution:
 *   EXEC etl.usp_Atlas_RunData;
 *   EXEC etl.usp_Atlas_RunData @SkipPBI = 1;
 *   EXEC etl.usp_Atlas_RunData @ExtractOnly = 1;  -- validates raw data only
 *   EXEC etl.usp_Atlas_RunData @Debug = 1;
 *
 * Author:  Larry Duren
 * Date:    March 2026
 * Version: 5.0 (Pipeline B — append-only RunData)
 *
 * Change Log:
 *   v5.0 2026-05-02 — Phase 10 and Phase 12 reverted from MERGE
 *        back to INSERT, with WHERE NOT EXISTS clauses added to
 *        prevent duplicate inserts into now append-only production
 *        tables. v4.9's MERGE pattern was failing through pyodbc's
 *        sp_executesql path with misleading "Incorrect syntax near
 *        'RunStart'" parser errors. Root cause undetermined; v4.7's
 *        INSERT pattern is restored. Phase 1 v4.9 behavior preserved
 *        (staging-only truncate, production tables append-only).
 ******************************************************************************/

USE Atlas_Staging;
GO

IF OBJECT_ID('etl.usp_Atlas_RunData', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_RunData;
GO

CREATE PROCEDURE etl.usp_Atlas_RunData
    @ExecutionID        UNIQUEIDENTIFIER = NULL,
    @StagingSchema      NVARCHAR(128)    = 'stage_v2',
    @ProdSchema         NVARCHAR(128)    = 'prd_v2',
    @ProdDatabase       NVARCHAR(128)    = '',
    @ORG_AD_NAME        NVARCHAR(100)    = 'MR1',
    @SkipPBI            BIT              = 0,
    @ExtractOnly        BIT              = 0,
    @Debug              BIT              = 0,
    @RaiseErrorOnFail   BIT              = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- Fully-qualified schema prefixes for dynamic SQL
    DECLARE @PrdPrefix NVARCHAR(300) =
        CASE
            WHEN @ProdDatabase = N'' OR @ProdDatabase IS NULL
                THEN QUOTENAME(@ProdSchema) + N'.'
            ELSE QUOTENAME(@ProdDatabase) + N'.' + QUOTENAME(@ProdSchema) + N'.'
        END;
    DECLARE @StgPrefix NVARCHAR(300) = QUOTENAME(@StagingSchema) + N'.';

    -- prd_v2 (dev) uses 'ReportObjects' (plural); Atlas_Prd.dbo (prod) uses
    -- 'ReportObject' (singular). Pick the right name based on @ProdDatabase
    -- so Phase 12's bridge JOIN resolves in both environments. The BizKey
    -- column is named 'BizKey' on both (NVARCHAR(500) in prod, added by
    -- cn3_bizkey_ddl.sql) — no column-name divergence.
    DECLARE @PrdReportObjectTable NVARCHAR(128) =
        CASE
            WHEN @ProdDatabase = N'' OR @ProdDatabase IS NULL
                THEN N'ReportObjects'
            ELSE N'ReportObject'
        END;

    DECLARE @PackageName    NVARCHAR(100) = N'ETL-RunData';
    DECLARE @StepName       NVARCHAR(200);
    DECLARE @StepStart      DATETIME;
    DECLARE @LogID          BIGINT;
    DECLARE @StepSequence   INT = 0;
    DECLARE @RowCount       INT;
    DECLARE @TotalRows      INT = 0;
    DECLARE @ErrorMessage   NVARCHAR(4000);
    DECLARE @SQL            NVARCHAR(MAX);

    IF @ExecutionID IS NULL
        SET @ExecutionID = NEWID();

    IF @Debug = 1
    BEGIN
        PRINT '=== usp_Atlas_RunData — Pipeline B (No Linked Server) ===';
        PRINT 'NOTE: Raw extraction handled by atlas_rundata_extractor.py';
        PRINT 'NOTE: PBI events handled by atlas_pbi_events.py';
        PRINT '';
    END;


    -- =========================================================================
    -- PHASE 1: TRUNCATE STAGING TABLES ONLY
    -- Production tables dbo.ReportObjectRunData and
    -- dbo.ReportObjectRunDataBridge are NOT truncated.
    -- Data accumulates across runs (MERGE pattern in Phases 10/12).
    -- Rolling window enforced upstream by atlas_rundata_extractor.py
    -- via config.rundata_lookback_days (default 30 days).
    --
    -- Rolling window enforced by atlas_rundata_extractor.py.
    -- Default: config.rundata_lookback_days = 30 days (Clarity
    -- and SlicerDicer). PBI Activity Events: pbi_lookback_days
    -- = 28 days (Microsoft Graph API limit).
    -- The SP itself has no date filters — it ingests all rows
    -- present in raw_v2 and stage_v2 tables.
    -- =========================================================================

    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Phase 1 - Truncate Staging Tables (Pipeline B)';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName    = @StepName,
        @StepSequence = @StepSequence,
        @LogID       = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            -- PBI joined table (rebuilt each run)
            TRUNCATE TABLE raw_v2.PbiActivityEventJoined;

            -- Staging tables
            TRUNCATE TABLE stage_v2.ReportObjectRunDataStaging;
            TRUNCATE TABLE stage_v2.ReportObjectRunDataParentStaging;
            TRUNCATE TABLE stage_v2.ReportObjectRunDataBridgeStaging;

            -- Production tables (dbo.ReportObjectRunData,
            -- dbo.ReportObjectRunDataBridge) are append-only and NEVER
            -- truncated here. Phases 10 and 12 use MERGE to accumulate
            -- run history across pipeline executions. The FK drop/recreate
            -- pattern that previously wrapped the parent TRUNCATE was
            -- removed in v4.9 (2026-04-30) along with the TRUNCATEs.

            -- TRUNCATE does not expose a meaningful @@ROWCOUNT; log operation
            -- count (4) instead. Post-truncate row count is 0 by definition.
            SET @RowCount = 0;
        END;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = 4,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': 4 staging tables truncated — production RunData tables are append-only (never truncated)';

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
    -- PHASE 2: REMOVED — Pipeline B Amendment
    -- Raw extraction (Clarity + SlicerDicer) handled by:
    --   atlas_rundata_extractor.py
    -- Validate that raw data exists.
    -- =========================================================================

    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Phase 2 - Validate Raw Data (Pipeline B)';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName    = @StepName,
        @StepSequence = @StepSequence,
        @LogID       = @LogID OUTPUT;

    BEGIN TRY
        DECLARE @ClarityRunCount INT, @SDCount INT, @PBICount INT;
        
        SELECT @ClarityRunCount = COUNT(*) FROM raw_v2.ClarityReportRunData;
        SELECT @SDCount = COUNT(*) FROM raw_v2.SlicerDicerStatsHttp;
        SELECT @PBICount = CASE WHEN @SkipPBI = 0 
                                THEN (SELECT COUNT(*) FROM raw_v2.PbiActivityEvent) 
                                ELSE 0 END;
        
        SET @TotalRows = @ClarityRunCount + @SDCount + @PBICount;

        IF @Debug = 1
        BEGIN
            PRINT 'Raw data validation (Pipeline B):';
            PRINT '  raw_v2.ClarityReportRunData:  ' + CAST(@ClarityRunCount AS VARCHAR(20)) + ' rows';
            PRINT '  raw_v2.SlicerDicerStatsHttp:  ' + CAST(@SDCount AS VARCHAR(20)) + ' rows';
            PRINT '  raw_v2.PbiActivityEvent:       ' + CAST(@PBICount AS VARCHAR(20)) + ' rows';
        END;
        
        IF @ClarityRunCount = 0 AND @SDCount = 0
            PRINT 'WARNING: Both Clarity and SlicerDicer raw tables are empty. Run atlas_rundata_extractor.py first.';

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @TotalRows,
            @Status       = N'Success';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();

        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
            PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- If ExtractOnly mode, stop here (Pipeline B: validates raw data presence)
    IF @ExtractOnly = 1
    BEGIN
        PRINT 'ExtractOnly mode: raw data validation complete. Stopping.';
        RETURN 0;
    END;


    -- =========================================================================
    -- PHASE 3: INDEX RAW TABLES
    -- =========================================================================

    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Phase 3 - Create Raw Table Indexes';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName    = @StepName,
        @StepSequence = @StepSequence,
        @LogID       = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            -- Clarity indexes
            -- v2.1: Column names fixed to match actual raw_v2.ClarityReportRunData schema
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ClarityRunData_ReportInfoID' AND object_id = OBJECT_ID('raw_v2.ClarityReportRunData'))
                CREATE NONCLUSTERED INDEX IX_ClarityRunData_ReportInfoID
                    ON raw_v2.ClarityReportRunData (SOURCE_REPORT_ID);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ClarityRunData_UserID' AND object_id = OBJECT_ID('raw_v2.ClarityReportRunData'))
                CREATE NONCLUSTERED INDEX IX_ClarityRunData_UserID
                    ON raw_v2.ClarityReportRunData (RUN_USER_ID);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ClarityRunData_RunStart' AND object_id = OBJECT_ID('raw_v2.ClarityReportRunData'))
                CREATE NONCLUSTERED INDEX IX_ClarityRunData_RunStart
                    ON raw_v2.ClarityReportRunData (REPORT_START_INST);

            -- SlicerDicer indexes
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SDHttp_SessionId' AND object_id = OBJECT_ID('raw_v2.SlicerDicerStatsHttp'))
                CREATE NONCLUSTERED INDEX IX_SDHttp_SessionId
                    ON raw_v2.SlicerDicerStatsHttp (SessionId);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SDQuery_HttpRequestId' AND object_id = OBJECT_ID('raw_v2.SlicerDicerStatsQuery'))
                CREATE NONCLUSTERED INDEX IX_SDQuery_HttpRequestId
                    ON raw_v2.SlicerDicerStatsQuery (HttpRequestId);

            -- PBI indexes
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_PbiEvent_ReportId' AND object_id = OBJECT_ID('raw_v2.PbiActivityEvent'))
                CREATE NONCLUSTERED INDEX IX_PbiEvent_ReportId
                    ON raw_v2.PbiActivityEvent (ReportId);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_PbiEvent_CreationTime' AND object_id = OBJECT_ID('raw_v2.PbiActivityEvent'))
                CREATE NONCLUSTERED INDEX IX_PbiEvent_CreationTime
                    ON raw_v2.PbiActivityEvent (CreationTime);
        END;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = 7,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': 7 indexes created/verified';

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
    -- PHASES 4-10: UNCHANGED FROM PIPELINE A
    -- All remaining phases operate on LOCAL raw_v2/stage_v2/prd_v2 tables.
    -- No linked server access was used in these phases.
    -- Phases: PBI Processing → Staging Transforms → Stage Indexing →
    --         Bridge Logic → Deduplication → Production MERGE → User Counts
    --
    -- [The complete Phase 4-10 SQL from 02_usp_Atlas_RunData.sql lines 611-1626
    --  is included verbatim below.]
    -- =========================================================================

    -- =========================================================================
    -- PHASE 4: PBI PROCESSING
    -- =========================================================================

    IF @SkipPBI = 0
    BEGIN
        -- Step 4a: Deduplicate PBI events into history
        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Phase 4a - Deduplicate PBI Events to History';
        SET @StepStart = GETDATE();

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID = @ExecutionID,
            @PackageName = @PackageName,
            @StepName    = @StepName,
            @StepSequence = @StepSequence,
            @LogID       = @LogID OUTPUT;

        BEGIN TRY
            IF @Debug = 0
            BEGIN
                INSERT INTO raw_v2.PbiActivityEventHistory (
                    Id, CreationTime, Activity, UserId,
                    ReportId, ReportName, WorkspaceName, WorkspaceId,
                    DatasetId, DatasetName, ReportType
                )
                SELECT
                    e.Id, e.CreationTime, e.Activity, e.UserId,
                    e.ReportId, e.ReportName, e.WorkspaceName, e.WorkspaceId,
                    e.DatasetId, e.DatasetName, e.ReportType
                FROM raw_v2.PbiActivityEvent e
                WHERE NOT EXISTS (
                    SELECT 1 FROM raw_v2.PbiActivityEventHistory h
                    WHERE h.Id = e.Id
                );

                SET @RowCount = @@ROWCOUNT;
            END
            ELSE
                SET @RowCount = 0;

            EXEC etl.usp_Atlas_LogEnd
                @LogID        = @LogID,
                @RowsAffected = @RowCount,
                @Status       = N'Success';

            IF @Debug = 1
                PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' new events';

        END TRY
        BEGIN CATCH
            EXEC etl.usp_Atlas_LogError @LogID = @LogID;
            SET @ErrorMessage = ERROR_MESSAGE();
            IF @RaiseErrorOnFail = 1
                THROW;
            ELSE
                PRINT @StepName + ' failed: ' + @ErrorMessage;
        END CATCH;

        -- Step 4b: Join PBI events with report metadata (CN4 Option B BizKey)
        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Phase 4b - Join PBI Events with Report Metadata';
        SET @StepStart = GETDATE();

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID = @ExecutionID,
            @PackageName = @PackageName,
            @StepName    = @StepName,
            @StepSequence = @StepSequence,
            @LogID       = @LogID OUTPUT;

        BEGIN TRY
            IF @Debug = 0
            BEGIN
                -- ─────────────────────────────────────────────────────────────
                -- Phase 4b rewrite (PBI migration Phase 3, CN4 Option B):
                --
                -- BizKey format (per CN4, locked 2026-04-07):
                --     PowerBI || <WorkspaceId> || <ReportType> || <ReportId> || ||
                --
                -- WorkspaceId is resolved via raw_v2.PbiWorkspaceReport — the
                -- workspace→report bridge populated by atlas_pbi_metadata.py
                -- from the /admin/groups $expand=reports nested payload. The
                -- /admin/reports endpoint does NOT return workspaceId directly
                -- on individual report objects, so raw_v2.PbiReport.WorkspaceId
                -- is NULL on every row. The bridge JOIN is required to resolve
                -- WorkspaceId for the BizKey. Confirmed by smoke test 2026-04-07
                -- (3,567 reports across 106 workspaces, 901 workspace-report
                -- bridge rows).
                --
                -- Join key to PbiReport: COALESCE(e.AppReportId, e.ReportId)
                -- = rpt.ReportId. App-published reports use AppReportId as the
                -- workspace-copy identifier; standalone workspace reports use
                -- the direct ReportId. Matches SSIS File 14's pattern.
                --
                -- Activity filter: e.Activity LIKE 'ViewReport%' per client
                -- decision 2026-04-07 (matches SSIS File 14, narrower than the
                -- pre-Phase-3 6-value IN clause which also included
                -- ExportReport/PrintReport/ShareReport/GetSnapshots/ViewDashboard).
                -- Also filters e.UserId IS NOT NULL — rows with no resolvable
                -- user produce unusable RunData downstream.
                --
                -- BizKey will not resolve at Phase 12 (final RunData merge
                -- into prd_v2.ReportObjectRunDataBridge at line ~1341) until
                -- Phase 4 of the PBI migration stages PBI reports into
                -- prd_v2.ReportObjects with matching CN4 BizKeys. This is the
                -- expected intermediate state between Phase 3 and Phase 4.
                -- ─────────────────────────────────────────────────────────────
                INSERT INTO raw_v2.PbiActivityEventJoined (
                    Id, CreationTime, Activity, UserId,
                    ReportId, ReportName, WorkspaceName, WorkspaceId,
                    DatasetId, DatasetName, ReportType,
                    UserUPN,
                    ReportObjectBizKey
                )
                SELECT
                    e.Id,
                    e.CreationTime,
                    e.Activity,
                    e.UserId,
                    e.ReportId,
                    COALESCE(rpt.ReportName, e.ReportName)          AS ReportName,
                    COALESCE(rpt.WorkspaceName, e.WorkspaceName)    AS WorkspaceName,
                    COALESCE(wr.WorkspaceId, e.WorkspaceId)         AS WorkspaceId,
                    e.DatasetId,
                    e.DatasetName,
                    COALESCE(rpt.ReportType, e.ReportType)          AS ReportType,
                    -- UserUPN: NULL until Phase 5 enrichment runs (atlas_pbi_user_identity.py)
                    e.UserUPN                                       AS UserUPN,
                    CONCAT(
                        N'PowerBI',                     '||',
                        ISNULL(wr.WorkspaceId,   N''),  '||',
                        ISNULL(rpt.ReportType,   N''),  '||',
                        ISNULL(rpt.ReportId,     N''),  '||||'
                    )                                                AS ReportObjectBizKey
                FROM raw_v2.PbiActivityEvent e
                LEFT JOIN raw_v2.PbiReport rpt
                    ON COALESCE(e.AppReportId, e.ReportId) = rpt.ReportId
                LEFT JOIN raw_v2.PbiWorkspaceReport wr
                    ON rpt.ReportId = wr.ReportId
                WHERE e.Activity LIKE 'ViewReport%'
                  AND e.UserId IS NOT NULL;

                SET @RowCount = @@ROWCOUNT;
            END
            ELSE
                SET @RowCount = 0;

            EXEC etl.usp_Atlas_LogEnd
                @LogID        = @LogID,
                @RowsAffected = @RowCount,
                @Status       = N'Success';

            IF @Debug = 1
                PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' joined events';

        END TRY
        BEGIN CATCH
            EXEC etl.usp_Atlas_LogError @LogID = @LogID;
            SET @ErrorMessage = ERROR_MESSAGE();
            IF @RaiseErrorOnFail = 1
                THROW;
            ELSE
                PRINT @StepName + ' failed: ' + @ErrorMessage;
        END CATCH;

    END; -- @SkipPBI = 0


    -- =========================================================================
    -- PHASE 4c: BUILD USER RESOLUTION LOOKUP (Performance Optimization)
    -- Pre-computes Epic USER_ID → UserName mapping so Phases 5-6 can use
    -- a simple equi-join instead of a non-SARGable LIKE '%\' + ID pattern.
    -- =========================================================================

    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Phase 4c - Build User Resolution Lookup';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName    = @StepName,
        @StepSequence = @StepSequence,
        @LogID       = @LogID OUTPUT;

    BEGIN TRY
        -- Create temp lookup outside IF/ELSE to avoid error 2714 at compile time
        CREATE TABLE #UserLookup (
            EpicUserId NVARCHAR(50) NOT NULL PRIMARY KEY,
            ResolvedUserName NVARCHAR(200)
        );

        IF @Debug = 0
        BEGIN
            -- First pass: EpicId is the numeric Clarity USER_ID; Username is the
            -- resolved SamAccountName. Maps run data RUN_USER_ID → Username.
            -- v4.4: EpicId replaces UserName (SamAccountName was never numeric);
            --       Username replaces UserPrincipalName (column removed in 0696c9e).
            INSERT INTO #UserLookup (EpicUserId, ResolvedUserName)
            SELECT EpicUserId, ResolvedUserName
            FROM (
                SELECT
                    u.EpicId AS EpicUserId,
                    u.Username AS ResolvedUserName,
                    ROW_NUMBER() OVER (
                        PARTITION BY u.EpicId
                        ORDER BY
                            -- Prefer UPN (contains @) over domain\id format
                            CASE WHEN u.Username LIKE '%@%' THEN 0 ELSE 1 END,
                            u.Username
                    ) AS rn
                FROM stage_v2.ReportObjectUser u
                WHERE u.EpicId IS NOT NULL
                  AND u.Username IS NOT NULL
                  AND ISNUMERIC(u.EpicId) = 1
            ) ranked
            WHERE rn = 1;

            -- Second pass: domain\id pattern — extract the ID after the backslash
            -- e.g. Username = 'MR1\12345' → EpicUserId = '12345'
            INSERT INTO #UserLookup (EpicUserId, ResolvedUserName)
            SELECT
                RIGHT(u.Username, LEN(u.Username) - CHARINDEX('\', u.Username)) AS EpicUserId,
                u.Username AS ResolvedUserName
            FROM stage_v2.ReportObjectUser u
            WHERE u.Username LIKE '%\%'
              AND u.Username IS NOT NULL
              AND RIGHT(u.Username, LEN(u.Username) - CHARINDEX('\', u.Username)) NOT IN (
                  SELECT EpicUserId FROM #UserLookup
              );

            SET @RowCount = (SELECT COUNT(*) FROM #UserLookup);
        END
        ELSE
        BEGIN
            SET @RowCount = 0;
            PRINT @StepName + ': DEBUG — temp table created (0 rows)';
        END

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1 OR @RowCount > 0
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' user mappings';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =========================================================================
    -- PHASE 5: STAGING TRANSFORMS — BizKey construction + user resolution
    -- Transforms raw_v2 run data into stage_v2.ReportObjectRunDataStaging
    -- =========================================================================

    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Phase 5 - Stage Clarity Report Run Data';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName    = @StepName,
        @StepSequence = @StepSequence,
        @LogID       = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            INSERT INTO stage_v2.ReportObjectRunDataStaging (
                ReportObjectBizKey, RunUserName, RunStartTime,
                RunYear, RunMonth, RunDay, RunHour,
                RunDurationMs, [RowCount], Status,
                SourceSystem, ExtractDate
            )
            -- v4.6: BizKey now matches SSIS ETL-RunData HRX pattern exactly.
            -- JOIN ZC_REPORT_TYPE_HGR for granular type labels instead of CASE on int.
            -- BizKey: server||db||Type||||HRX||REPORT_INFO_ID
            SELECT
                N'clarity_server' + N'||' + N'clarityreport' + N'||'
                    + CASE
                        WHEN zt.NAME = N'Settings'  THEN N'Application Report'
                        WHEN zt.NAME = N'Workbench'  THEN N'Reporting Workbench Report'
                        WHEN zt.NAME IS NULL         THEN N'Other HRX Report'
                        ELSE zt.NAME + N' Report'
                      END
                    + N'||' + N''
                    + N'||' + N'HRX'
                    + N'||' + CAST(r.SOURCE_REPORT_ID AS NVARCHAR(50))
                AS ReportObjectBizKey,
                ISNULL(ul.ResolvedUserName,
                       @ORG_AD_NAME + N'\' + CAST(r.RUN_USER_ID AS NVARCHAR(50)))
                AS RunUserName,
                r.REPORT_START_INST AS RunStartTime,
                YEAR(r.REPORT_START_INST)  AS RunYear,
                MONTH(r.REPORT_START_INST) AS RunMonth,
                DAY(r.REPORT_START_INST)   AS RunDay,
                DATEPART(HOUR, r.REPORT_START_INST) AS RunHour,
                DATEDIFF(MILLISECOND, r.REPORT_START_INST,
                         ISNULL(r.REPORT_END_INST, r.REPORT_START_INST)) AS RunDurationMs,
                NULL AS [RowCount],
                CASE r.REPORT_STATUS_C
                    WHEN 1 THEN N'Success'
                    WHEN 2 THEN N'Stopped'
                    WHEN 3 THEN N'Cached'
                    WHEN 4 THEN N'Error'
                    ELSE NULL
                END AS Status,
                N'Clarity' AS SourceSystem,
                GETDATE() AS ExtractDate
            FROM raw_v2.ClarityReportRunData r
            LEFT JOIN raw_v2.REPORT_INFO ri
                ON r.SOURCE_REPORT_ID = ri.REPORT_INFO_ID
            LEFT JOIN raw_v2.ZC_REPORT_TYPE_HGR zt
                ON zt.REPORT_TYPE_HGR_C = ri.RECORD_TYPE_C
            LEFT JOIN #UserLookup ul
                ON CAST(r.RUN_USER_ID AS NVARCHAR(50)) = ul.EpicUserId
            WHERE r.SOURCE_REPORT_ID IS NOT NULL;

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

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


    -- =========================================================================
    -- PHASE 5b: STAGE HGR TEMPLATE PARENT RUNS
    -- Template-level runs (SOURCE_REPORT_ID IS NULL) → ParentStaging.
    -- Matches SSIS ETL-RunData HGR pattern: BizKey uses TEMPLATE_INFO.REPORT_ID
    -- and ZC_REPORT_TYPE_HGR for type labels. Only runs without a child HRX.
    -- =========================================================================

    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Phase 5b - Stage HGR Template Parent Runs';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName    = @StepName,
        @StepSequence = @StepSequence,
        @LogID       = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            INSERT INTO stage_v2.ReportObjectRunDataParentStaging (
                ReportObjectBizKey, RunUserName, RunStartTime,
                RunYear, RunMonth, RunDay, RunHour,
                RunDurationMs, SourceSystem, ExtractDate
            )
            SELECT
                N'clarity_server' + N'||' + N'clarityreport' + N'||'
                    + CASE
                        WHEN zt.NAME = N'Settings'  THEN N'Application Report Template'
                        WHEN zt.NAME = N'Workbench'  THEN N'Reporting Workbench Template'
                        WHEN zt.NAME IS NULL         THEN N'Other HGR Template'
                        ELSE zt.NAME + N' Template'
                      END
                    + N'||' + N''
                    + N'||' + N'HGR'
                    + N'||' + CAST(ti.REPORT_ID AS NVARCHAR(50))
                AS ReportObjectBizKey,
                ISNULL(ul.ResolvedUserName,
                       @ORG_AD_NAME + N'\' + CAST(r.RUN_USER_ID AS NVARCHAR(50)))
                AS RunUserName,
                r.REPORT_START_INST AS RunStartTime,
                YEAR(r.REPORT_START_INST)  AS RunYear,
                MONTH(r.REPORT_START_INST) AS RunMonth,
                DAY(r.REPORT_START_INST)   AS RunDay,
                DATEPART(HOUR, r.REPORT_START_INST) AS RunHour,
                DATEDIFF(MILLISECOND, r.REPORT_START_INST,
                         ISNULL(r.REPORT_END_INST, r.REPORT_START_INST)) AS RunDurationMs,
                N'Clarity' AS SourceSystem,
                GETDATE() AS ExtractDate
            FROM raw_v2.ClarityReportRunData r
            INNER JOIN raw_v2.TEMPLATE_INFO ti
                ON r.REPORT_TEMPLATE_ID = ti.REPORT_ID
            LEFT JOIN raw_v2.ZC_REPORT_TYPE_HGR zt
                ON zt.REPORT_TYPE_HGR_C = ti.REPORT_TYPE_HGR_C
            LEFT JOIN #UserLookup ul
                ON CAST(r.RUN_USER_ID AS NVARCHAR(50)) = ul.EpicUserId
            WHERE r.SOURCE_REPORT_ID IS NULL;

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

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


    -- =========================================================================
    -- PHASE 6: STAGE DASHBOARD RUN DATA
    -- =========================================================================

    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Phase 6 - Stage Clarity Dashboard Run Data';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName    = @StepName,
        @StepSequence = @StepSequence,
        @LogID       = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            INSERT INTO stage_v2.ReportObjectRunDataStaging (
                ReportObjectBizKey, RunUserName, RunStartTime,
                RunYear, RunMonth, RunDay, RunHour,
                RunDurationMs, [RowCount], Status,
                SourceSystem, ExtractDate
            )
            -- v4.6: BizKey matches SSIS dashboard pattern — uses pre-populated fields
            -- from raw_v2.ClarityDashboardRunData (populated by Python extractor).
            -- BizKey: server||db||Type||||MasterFile||RecordID
            SELECT
                N'clarity_server' + N'||' + N'clarityreport' + N'||'
                    + ISNULL(d.ReportObjectType, N'Source Radar Dashboard')
                    + N'||' + N''
                    + N'||' + ISNULL(d.EpicMasterFile, N'IDM')
                    + N'||' + ISNULL(CAST(d.EpicRecordID AS NVARCHAR(50)), N''),
                -- v2.2: Equi-join on pre-computed #UserLookup (was non-SARGable LIKE)
                ISNULL(ul.ResolvedUserName,
                       @ORG_AD_NAME + N'\' + CAST(d.RunUserId AS NVARCHAR(50))),
                d.RunStartTime,
                YEAR(d.RunStartTime),
                MONTH(d.RunStartTime),
                DAY(d.RunStartTime),
                DATEPART(HOUR, d.RunStartTime),
                NULL,  -- no duration for dashboard access
                NULL,
                N'Success',
                N'Clarity',
                GETDATE()
            FROM raw_v2.ClarityDashboardRunData d
            LEFT JOIN #UserLookup ul
                ON CAST(d.RunUserId AS NVARCHAR(50)) = ul.EpicUserId
            WHERE d.EpicRecordID IS NOT NULL;

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

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


    -- =========================================================================
    -- PHASE 7: STAGE PBI RUN DATA (from joined events)
    -- =========================================================================

    IF @SkipPBI = 0
    BEGIN
        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Phase 7 - Stage PBI Run Data';
        SET @StepStart = GETDATE();

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID = @ExecutionID,
            @PackageName = @PackageName,
            @StepName    = @StepName,
            @StepSequence = @StepSequence,
            @LogID       = @LogID OUTPUT;

        BEGIN TRY
            IF @Debug = 0
            BEGIN
                INSERT INTO stage_v2.ReportObjectRunDataStaging (
                    ReportObjectBizKey, RunUserName, RunStartTime,
                    RunYear, RunMonth, RunDay, RunHour,
                    RunDurationMs, [RowCount], Status,
                    SourceSystem, ExtractDate
                )
                SELECT
                    p.ReportObjectBizKey,
                    -- Option B (client decision 2026-04-07): prefer resolved UPN
                    -- from atlas_pbi_user_identity.py Graph enrichment; fall back
                    -- to UserId (GUID or legacy UPN) if enrichment hasn't run or
                    -- the user was deleted from AD (404 in Graph batch response).
                    -- UPN max 255 chars; target RunUserName is nvarchar(200).
                    -- UPNs in BILH tenant confirmed < 200 chars in practice —
                    -- silent truncation risk is theoretical, not observed.
                    ISNULL(p.UserUPN, p.UserId)    AS RunUserName,
                    p.CreationTime,
                    YEAR(p.CreationTime),
                    MONTH(p.CreationTime),
                    DAY(p.CreationTime),
                    DATEPART(HOUR, p.CreationTime),
                    NULL,
                    NULL,
                    N'Success',
                    N'PowerBI',
                    GETDATE()
                FROM raw_v2.PbiActivityEventJoined p
                WHERE p.ReportObjectBizKey IS NOT NULL;

                SET @RowCount = @@ROWCOUNT;
            END
            ELSE
                SET @RowCount = 0;

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
    END;


    -- =========================================================================
    -- PHASE 7a: STAGE SLICERDICER FDM MODEL RUNS
    -- (SSIS: ETL-RunData/SQL/22_SlicerDicer_Stage_-_SlicerDicer_Runs.sql)
    -- Joins Http → Query → DATA_MODEL_DEFINITIONS to build FDM BizKeys.
    -- Dedup: only keep sessions with counter > min_counter (i.e., sessions
    -- that ran a model query beyond the initial load).
    -- =========================================================================

    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Phase 7a - Stage SlicerDicer FDM Model Runs';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName    = @StepName,
        @StepSequence = @StepSequence,
        @LogID       = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            ;WITH session_data AS (
                SELECT
                    h.SessionId,
                    h.UserId,
                    MAX(h.Instant)    AS Instant,
                    q.ModelId,
                    d.BASE_RECORD_ID,
                    d.DATA_MODEL_ID,
                    COUNT(1)          AS counter
                FROM raw_v2.SlicerDicerStatsHttp h
                INNER JOIN raw_v2.SlicerDicerStatsQuery q
                    ON q.HttpRequestId = h.RequestId
                INNER JOIN raw_v2.DATA_MODEL_DEFINITIONS d
                    ON d.BASE_RECORD_ID = q.ModelId
                WHERE h.UserId IS NOT NULL
                  AND q.ModelId IS NOT NULL
                  AND h.RequestType = N'Query'
                GROUP BY h.SessionId, h.UserId, q.ModelId,
                         d.BASE_RECORD_ID, d.DATA_MODEL_ID
            ),
            min_counters AS (
                SELECT SessionId, MIN(counter) AS MinCounter
                FROM session_data
                GROUP BY SessionId
            )
            INSERT INTO stage_v2.ReportObjectRunDataStaging (
                ReportObjectBizKey, RunUserName, RunStartTime,
                RunYear, RunMonth, RunDay, RunHour,
                RunDurationMs, [RowCount], Status,
                SourceSystem, ExtractDate
            )
            SELECT
                N'slicerdicer_server' + N'||' + N'slicerdicer' + N'||'
                    + N'SlicerDicer Model' + N'||' + N'' + N'||'
                    + N'FDM' + N'||'
                    + CAST(sd.DATA_MODEL_ID AS NVARCHAR(50))
                AS ReportObjectBizKey,
                ISNULL(ul.ResolvedUserName,
                       @ORG_AD_NAME + N'\' + CAST(sd.UserId AS NVARCHAR(50)))
                AS RunUserName,
                CAST(sd.Instant AS DATETIME) AS RunStartTime,
                YEAR(CAST(sd.Instant AS DATETIME))           AS RunYear,
                MONTH(CAST(sd.Instant AS DATETIME))          AS RunMonth,
                DAY(CAST(sd.Instant AS DATETIME))            AS RunDay,
                DATEPART(HOUR, CAST(sd.Instant AS DATETIME)) AS RunHour,
                NULL AS RunDurationMs,
                NULL AS [RowCount],
                N'Success' AS Status,
                N'SlicerDicer' AS SourceSystem,
                GETDATE() AS ExtractDate
            FROM session_data sd
            INNER JOIN min_counters mc
                ON sd.SessionId = mc.SessionId
                AND sd.counter > mc.MinCounter
            LEFT JOIN #UserLookup ul
                ON sd.UserId = ul.EpicUserId;

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

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


    -- =========================================================================
    -- PHASE 7b: STAGE SLICERDICER SESSION RUNS
    -- (SSIS: ETL-RunData/SQL/24+25 SlicerDicer Session runs)
    -- Joins SaveLoad → Http for user resolution, validates session exists
    -- in clarity_slicerdicer_sessions. BizKey uses clarity_server prefix
    -- to match usp_Atlas_Clarity Step 19 SlicerDicer session records.
    -- =========================================================================

    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Phase 7b - Stage SlicerDicer Session Runs';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName    = @StepName,
        @StepSequence = @StepSequence,
        @LogID       = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            INSERT INTO stage_v2.ReportObjectRunDataStaging (
                ReportObjectBizKey, RunUserName, RunStartTime,
                RunYear, RunMonth, RunDay, RunHour,
                RunDurationMs, [RowCount], Status,
                SourceSystem, ExtractDate
            )
            SELECT
                N'clarity_server' + N'||' + N'clarityreport' + N'||'
                    + N'SlicerDicer Session' + N'||' + N'' + N'||'
                    + N'HRX' + N'||'
                    + CAST(c.Report_ID AS NVARCHAR(50))
                AS ReportObjectBizKey,
                ISNULL(ul.ResolvedUserName,
                       @ORG_AD_NAME + N'\' + CAST(h.UserId AS NVARCHAR(50)))
                AS RunUserName,
                CAST(s.Instant AS DATETIME) AS RunStartTime,
                YEAR(CAST(s.Instant AS DATETIME))           AS RunYear,
                MONTH(CAST(s.Instant AS DATETIME))          AS RunMonth,
                DAY(CAST(s.Instant AS DATETIME))            AS RunDay,
                DATEPART(HOUR, CAST(s.Instant AS DATETIME)) AS RunHour,
                NULL AS RunDurationMs,
                NULL AS [RowCount],
                N'Success' AS Status,
                N'SlicerDicer' AS SourceSystem,
                GETDATE() AS ExtractDate
            FROM raw_v2.SlicerDicerStatsSaveLoad s
            INNER JOIN raw_v2.SlicerDicerStatsHttp h
                ON s.HttpRequestId = h.RequestId
            INNER JOIN raw_v2.clarity_slicerdicer_sessions c
                ON c.Report_ID = s.PopulationId
            LEFT JOIN #UserLookup ul
                ON h.UserId = ul.EpicUserId;

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

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


    -- =========================================================================
    -- PHASE 8: INDEX STAGING TABLES
    -- =========================================================================

    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Phase 8 - Index Staging Run Data';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName    = @StepName,
        @StepSequence = @StepSequence,
        @LogID       = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_RunDataStaging_BizKey'
                           AND object_id = OBJECT_ID('stage_v2.ReportObjectRunDataStaging'))
                CREATE NONCLUSTERED INDEX IX_RunDataStaging_BizKey
                    ON stage_v2.ReportObjectRunDataStaging (ReportObjectBizKey);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_RunDataStaging_User'
                           AND object_id = OBJECT_ID('stage_v2.ReportObjectRunDataStaging'))
                CREATE NONCLUSTERED INDEX IX_RunDataStaging_User
                    ON stage_v2.ReportObjectRunDataStaging (RunUserName);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_RunDataStaging_Time'
                           AND object_id = OBJECT_ID('stage_v2.ReportObjectRunDataStaging'))
                CREATE NONCLUSTERED INDEX IX_RunDataStaging_Time
                    ON stage_v2.ReportObjectRunDataStaging (RunStartTime);
        END;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = 3, @Status = N'Success';
        IF @Debug = 1
            PRINT @StepName + ': 3 indexes created/verified';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =========================================================================
    -- PHASE 9: ROLL UP RUN DATA TO PARENT REPORTS
    -- Propagates child run records to parent reports via hierarchy.
    -- No aggregation — individual records preserved for Phase 10 MERGE.
    -- =========================================================================

    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Phase 9 - Roll Up Run Data to Parent Reports';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName    = @StepName,
        @StepSequence = @StepSequence,
        @LogID       = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            -- Roll up child run data to parent reports via hierarchy
            INSERT INTO stage_v2.ReportObjectRunDataParentStaging (
                ReportObjectBizKey, RunUserName, RunStartTime,
                RunYear, RunMonth, RunDay, RunHour,
                RunDurationMs, SourceSystem, ExtractDate
            )
            SELECT
                h.ParentBizKey AS ReportObjectBizKey,
                rd.RunUserName,
                rd.RunStartTime,
                rd.RunYear, rd.RunMonth, rd.RunDay, rd.RunHour,
                rd.RunDurationMs,
                rd.SourceSystem,
                GETDATE()
            FROM stage_v2.ReportObjectRunDataStaging rd
            INNER JOIN stage_v2.ReportObjectHierarchyStaging h
                ON rd.ReportObjectBizKey = h.ChildBizKey
            WHERE h.ParentBizKey IS NOT NULL
              AND h.ParentBizKey <> rd.ReportObjectBizKey;

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

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


    -- =========================================================================
    -- PHASE 10: PRODUCTION INSERT — INDIVIDUAL RUN RECORDS
    -- Source: UNION ALL of direct (ReportObjectRunDataStaging) and parent-
    -- rolled-up (ReportObjectRunDataParentStaging) individual run records.
    -- Derived columns match SSIS ETL-RunData.dtsx "Populate Atlas Runs" task:
    --   RunDataId          SHA2_256 hash of user + start time + duration + status
    --   RunUserID          resolved via prd_v2.[User].Username lookup
    --   RunDurationSeconds RunDurationMs / 1000
    --   RunStartTime_*     DATEADD/DATEDIFF truncations to year/month/day/hour
    --
    --   v5.0: Reverted from MERGE to INSERT with WHERE NOT EXISTS guard.
    --   v4.9's MERGE failed through pyodbc's sp_executesql path with
    --   misleading "Incorrect syntax near 'RunStart'" errors.
    --   WHERE NOT EXISTS on RunDataId prevents duplicate inserts into the
    --   now append-only production table. ROW_NUMBER dedup CTE preserved.
    -- =========================================================================

    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Phase 10 - Insert Individual Run Records to Production';
    SET @StepStart = GETDATE();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName    = @StepName,
        @StepSequence = @StepSequence,
        @LogID       = @LogID OUTPUT;

    BEGIN TRY
        IF @Debug = 0
        BEGIN
            -- INSERT with WHERE NOT EXISTS guard replaces the v4.9 MERGE.
            -- v4.9's MERGE failed through pyodbc's sp_executesql path with
            -- "Incorrect syntax near 'RunStart'" errors; root cause undetermined.
            -- WHERE NOT EXISTS on RunDataId prevents duplicate inserts into the
            -- append-only production table. ROW_NUMBER dedup CTE unchanged.
            -- ParentStaging has no Status column — NULL passed for parent rows.
            SET @SQL = N'
            INSERT INTO ' + @PrdPrefix + N'ReportObjectRunData (
                RunDataId, RunUserID, RunStartTime, RunDurationSeconds,
                RunStatus, LastLoadDate,
                RunStartTime_Day, RunStartTime_Hour,
                RunStartTime_Month, RunStartTime_Year
            )
            SELECT
                src.RunDataId, src.RunUserID, src.RunStartTime, src.RunDurationSeconds,
                src.Status, GETDATE(),
                src.RunStartTime_Day, src.RunStartTime_Hour,
                src.RunStartTime_Month, src.RunStartTime_Year
            FROM (
                SELECT
                    RunUserName, RunStartTime, RunDurationSeconds, Status,
                    RunDataId, RunUserID,
                    RunStartTime_Year, RunStartTime_Month, RunStartTime_Day, RunStartTime_Hour
                FROM (
                    SELECT
                        combined.RunUserName,
                        combined.RunStartTime,
                        ISNULL(combined.RunDurationMs / 1000, 0)    AS RunDurationSeconds,
                        ISNULL(combined.Status, '''')               AS Status,
                        CONVERT(NVARCHAR(256),
                            HASHBYTES(''SHA2_256'',
                                combined.RunUserName
                                + CONVERT(NVARCHAR, combined.RunStartTime, 109)
                                + CAST(ISNULL(CAST(combined.RunDurationMs / 1000 AS INT), 0) AS NVARCHAR)
                                + ISNULL(combined.Status, '''')
                            ), 2
                        )                                           AS RunDataId,
                        (SELECT MIN(u.UserID)
                         FROM ' + @PrdPrefix + N'[User] u
                         WHERE u.UserName = combined.RunUserName
                           AND u.UserName != ''''
                           AND u.UserName IS NOT NULL
                           AND u.UserName != '' ''
                        )                                          AS RunUserID,
                        CAST(DATEADD(yy, DATEDIFF(yy, 0, combined.RunStartTime), 0) AS DATETIME2(7)) AS RunStartTime_Year,
                        CAST(DATEADD(m,  DATEDIFF(m,  0, combined.RunStartTime), 0) AS DATETIME2(7)) AS RunStartTime_Month,
                        CAST(DATEADD(dd, DATEDIFF(dd, 0, combined.RunStartTime), 0) AS DATETIME2(7)) AS RunStartTime_Day,
                        CAST(DATEADD(hh, DATEDIFF(hh, 0, combined.RunStartTime), 0) AS DATETIME2(7)) AS RunStartTime_Hour,
                        ROW_NUMBER() OVER (PARTITION BY
                            CONVERT(NVARCHAR(256),
                                HASHBYTES(''SHA2_256'',
                                    combined.RunUserName
                                    + CONVERT(NVARCHAR, combined.RunStartTime, 109)
                                    + CAST(ISNULL(CAST(combined.RunDurationMs / 1000 AS INT), 0) AS NVARCHAR)
                                    + ISNULL(combined.Status, '''')
                                ), 2
                            )
                            ORDER BY combined.RunStartTime
                        ) AS rn
                    FROM (
                        SELECT
                            RunUserName, RunStartTime, RunDurationMs, Status
                        FROM stage_v2.ReportObjectRunDataStaging
                        UNION ALL
                        SELECT
                            RunUserName, RunStartTime, RunDurationMs, NULL AS Status
                        FROM stage_v2.ReportObjectRunDataParentStaging
                    ) combined
                    WHERE combined.RunStartTime IS NOT NULL
                ) deduped
                WHERE rn = 1
            ) AS src
            WHERE NOT EXISTS (
                SELECT 1 FROM ' + @PrdPrefix + N'ReportObjectRunData prd
                WHERE prd.RunDataId = src.RunDataId
            );';
            EXEC sp_executesql @SQL;

            SET @RowCount = @@ROWCOUNT;

            -- DistinctUsersPast12Months is computed in usp_Atlas_PostProcessing
            -- Step 5 (post-Insert, post-Bridge population). Join path:
            -- ReportObjectRunDataBridge → ReportObjectRunData → RunUserID.
        END
        ELSE
            SET @RowCount = 0;

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


    -- =========================================================================
    -- PHASE 11: POPULATE BRIDGE STAGING
    -- Computes the SHA2_256 RunId hash (same formula as Phase 10 RunDataId)
    -- and writes one row per run per report object into bridge staging.
    -- Direct runs (from ReportObjectRunDataStaging): Inherited = 0.
    -- Parent-inherited runs (from ReportObjectRunDataParentStaging): Inherited = 1.
    -- ParentStaging has no Status column — '' substituted to match hash formula.
    -- =========================================================================

    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Phase 11 - Populate Bridge Staging';
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
            INSERT INTO stage_v2.ReportObjectRunDataBridgeStaging (
                ReportObjectBizKey, RunId, Runs, Inherited, ExtractDate
            )
            -- Direct runs — Inherited = 0
            SELECT
                ReportObjectBizKey,
                CONVERT(NVARCHAR(450),
                    HASHBYTES('SHA2_256',
                        RunUserName
                        + CONVERT(NVARCHAR, RunStartTime, 109)
                        + CAST(ISNULL(CAST(RunDurationMs / 1000 AS INT), 0) AS NVARCHAR)
                        + ISNULL(Status, '')
                    ), 2
                ) AS RunId,
                1 AS Runs,
                0 AS Inherited,
                GETDATE()
            FROM stage_v2.ReportObjectRunDataStaging
            WHERE RunStartTime IS NOT NULL
            UNION ALL
            -- Inherited runs via parent hierarchy — Inherited = 1
            -- ParentStaging has no Status column; '' matches ISNULL(Status,'') in hash
            SELECT
                ReportObjectBizKey,
                CONVERT(NVARCHAR(450),
                    HASHBYTES('SHA2_256',
                        RunUserName
                        + CONVERT(NVARCHAR, RunStartTime, 109)
                        + CAST(ISNULL(CAST(RunDurationMs / 1000 AS INT), 0) AS NVARCHAR)
                        + ''
                    ), 2
                ) AS RunId,
                1 AS Runs,
                1 AS Inherited,
                GETDATE()
            FROM stage_v2.ReportObjectRunDataParentStaging
            WHERE RunStartTime IS NOT NULL;

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

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


    -- =========================================================================
    -- PHASE 11b: INDEX BRIDGE STAGING
    --   stage_v2.ReportObjectRunDataBridgeStaging is populated in Phase 11 as
    --   a ~9M-row heap with no indexes. The Phase 12 source SELECT then
    --   full-scans it and hash-joins to prd_v2.ReportObjects. Adding NCIs
    --   after Phase 11 lets the Phase 12 plan use streaming joins instead.
    --   IX_BridgeStaging_BizKey supports the JOIN to prd_v2.ReportObjects on
    --   ReportObjectBizKey. IX_BridgeStaging_RunId provides ordered access
    --   on the RunId column for downstream operations.
    --
    -- DROP-then-CREATE (not IF NOT EXISTS): Phase 1 TRUNCATE removes rows but
    -- leaves indexes intact. If we left prior-run indexes in place, Phase 11
    -- would insert 9M rows into an already-indexed heap, adding per-row
    -- maintenance overhead with zero benefit (Phase 11 doesn't read the
    -- indexes). Dropping before Phase 11 would split this step in two;
    -- instead we drop-and-recreate here, after Phase 11 has populated the
    -- (unindexed) heap, so Phase 11 runs fast AND Phase 12 gets fresh indexes.
    -- =========================================================================

    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Phase 11b - Index BridgeStaging';
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
            -- Drop prior-run indexes so Phase 11 always inserts into a clean
            -- heap, then rebuild against the fully-populated table.
            IF EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name = 'IX_BridgeStaging_BizKey'
                         AND object_id = OBJECT_ID('stage_v2.ReportObjectRunDataBridgeStaging'))
                DROP INDEX IX_BridgeStaging_BizKey
                    ON stage_v2.ReportObjectRunDataBridgeStaging;

            IF EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name = 'IX_BridgeStaging_RunId'
                         AND object_id = OBJECT_ID('stage_v2.ReportObjectRunDataBridgeStaging'))
                DROP INDEX IX_BridgeStaging_RunId
                    ON stage_v2.ReportObjectRunDataBridgeStaging;

            CREATE NONCLUSTERED INDEX IX_BridgeStaging_BizKey
                ON stage_v2.ReportObjectRunDataBridgeStaging (ReportObjectBizKey);

            CREATE NONCLUSTERED INDEX IX_BridgeStaging_RunId
                ON stage_v2.ReportObjectRunDataBridgeStaging (RunId);

            SET @RowCount = 2;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID, @RowsAffected = @RowCount, @Status = N'Success';
        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' indexes created/verified';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        IF @RaiseErrorOnFail = 1 THROW;
        ELSE PRINT @StepName + ' failed: ' + @ErrorMessage;
    END CATCH;


    -- =========================================================================
    -- PHASE 12: BRIDGE INSERT — ReportObjectRunDataBridge
    -- Resolves BizKey → ReportObjectID via prd_v2.ReportObjects JOIN and
    -- inserts one row per unique (RunId, ReportObjectId) pair.
    -- ReportObjectID IS NOT NULL guard excludes new prd_v2 rows not yet seeded
    -- with an integer ID from Atlas_Prd.
    --
    --   v5.0: Reverted from MERGE to INSERT with WHERE NOT EXISTS guard.
    --   v4.9's MERGE failed through pyodbc's sp_executesql path.
    --   SELECT DISTINCT in source preserves prior semantics — one row per
    --   unique (RunId, ReportObjectId) pair, Runs = 1. Pipeline B does not
    --   pre-aggregate bridge staging; SSIS does via GROUP BY count(1).
    --   WHERE NOT EXISTS prevents cross-run duplicates on (ReportObjectId, RunId).
    -- =========================================================================

    SET @StepSequence = @StepSequence + 1;
    SET @StepName = N'Phase 12 - Insert Run Data Bridge to Production';
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
            -- INSERT with WHERE NOT EXISTS replaces the v4.9 MERGE.
            -- No DB-level unique constraint on (ReportObjectId, RunId) —
            -- SELECT DISTINCT within this run plus WHERE NOT EXISTS prevents
            -- cross-run duplicates without the MERGE overhead.
            SET @SQL = N'
            INSERT INTO ' + @PrdPrefix + N'ReportObjectRunDataBridge (
                ReportObjectId, RunId, Runs, Inherited
            )
            SELECT DISTINCT
                ro.ReportObjectID,
                b.RunId,
                b.Runs,
                b.Inherited
            FROM stage_v2.ReportObjectRunDataBridgeStaging b
            INNER JOIN ' + @PrdPrefix + @PrdReportObjectTable + N' ro
                ON b.ReportObjectBizKey = ro.BizKey
            WHERE b.RunId IS NOT NULL
              AND ro.ReportObjectID IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM ' + @PrdPrefix + N'ReportObjectRunDataBridge prd
                  WHERE prd.ReportObjectId = ro.ReportObjectID
                    AND prd.RunId = b.RunId
              );';
            EXEC sp_executesql @SQL;

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

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


    -- =========================================================================
    -- PROCEDURE COMPLETE
    -- =========================================================================

    PRINT '';
    PRINT '=== usp_Atlas_RunData Complete (Pipeline B) ===';

    RETURN 0;

END;
GO

PRINT 'Created etl.usp_Atlas_RunData (Pipeline B — no linked server extraction)';
GO
