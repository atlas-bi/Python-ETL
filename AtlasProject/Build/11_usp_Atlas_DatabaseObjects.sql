/*******************************************************************************
 * Atlas ETL Migration — usp_Atlas_DatabaseObjects (Pipeline B)
 *
 * Stages database object metadata from raw_v2.DatabaseObjects (populated by
 * atlas_db_objects_extractor.py) into stage_v2.ReportObjectsStaging and
 * stage_v2.ReportObjectQueryStaging.
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  PIPELINE B — No linked servers required                               │
 * │  The orchestrator calls:                                                │
 * │      1. atlas_db_objects_extractor.py → raw_v2.DatabaseObjects (Python)│
 * │      2. usp_Atlas_DatabaseObjects     → stage_v2.* (this SP)          │
 * │      3. atlas_query_hierarchy.py      → stage_v2.TableRef* (Python)   │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * Source:  raw_v2.DatabaseObjects (11 columns)
 * Targets: stage_v2.ReportObjectsStaging     (APPEND — usp_Atlas_Clarity also writes)
 *          stage_v2.ReportObjectQueryStaging  (APPEND — usp_Atlas_Clarity also writes)
 *
 * NOTE: stage_v2.DatabaseObjectsStaging is orphaned (no downstream consumers).
 *       This SP does NOT populate it.
 *
 * BizKey delimiter: DOUBLE PIPE ||
 *   Pattern: SourceServer||SourceDB||ReportObjectType||ObjectID||||
 *
 * Dependencies:
 *   - raw_v2.DatabaseObjects populated by atlas_db_objects_extractor.py
 *   - Schemas: raw_v2, stage_v2 (created by 02_usp_Atlas_Setup.sql)
 *   - Logging: etl.usp_Atlas_LogStart, etl.usp_Atlas_LogEnd, etl.usp_Atlas_LogError
 *   - NO LINKED SERVER REQUIRED (Pipeline B)
 *
 * Execution:
 *   EXEC etl.usp_Atlas_DatabaseObjects;
 *   EXEC etl.usp_Atlas_DatabaseObjects @Debug = 1;
 *   EXEC etl.usp_Atlas_DatabaseObjects @RaiseErrorOnFail = 0;
 *
 * Change Log:
 *   v2.0  2026-03-25  Pipeline B refactor — reads from raw_v2.DatabaseObjects,
 *                     drops DatabaseObjectsStaging (orphaned), adds
 *                     ReportObjectQueryStaging, fixes BizKey to || delimiter,
 *                     adds @Debug validation mode, SET XACT_ABORT ON
 *
 * Author:  Larry Duren
 * Version: 2.0
 ******************************************************************************/

USE Atlas_Staging;
GO

IF OBJECT_ID('etl.usp_Atlas_DatabaseObjects', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_DatabaseObjects;
GO

CREATE PROCEDURE etl.usp_Atlas_DatabaseObjects
    @ExecutionID        UNIQUEIDENTIFIER = NULL,
    @RaiseErrorOnFail   BIT = 1,
    @Debug              BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- =========================================================================
    -- Variables
    -- =========================================================================
    DECLARE @PackageName    NVARCHAR(100) = N'usp_Atlas_DatabaseObjects';
    DECLARE @StepName       NVARCHAR(200);
    DECLARE @LogID          BIGINT;
    DECLARE @StepSequence   INT = 0;
    DECLARE @RowCount       INT;
    DECLARE @StartTime      DATETIME = GETDATE();
    DECLARE @ErrorMessage   NVARCHAR(4000);

    IF @ExecutionID IS NULL
        SET @ExecutionID = NEWID();

    BEGIN TRY

        IF @Debug = 1
        BEGIN
            PRINT '=== usp_Atlas_DatabaseObjects — Pipeline B ===';
            PRINT 'ExecutionID: ' + CAST(@ExecutionID AS VARCHAR(36));
            PRINT 'Start: ' + CONVERT(VARCHAR(30), @StartTime, 121);
            PRINT '';
        END;

        -- =====================================================================
        -- STEP 1: Pre-check — Validate raw_v2.DatabaseObjects has rows
        -- =====================================================================
        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Step 1: Validate raw_v2.DatabaseObjects';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        DECLARE @RawCount INT;
        SELECT @RawCount = COUNT(*) FROM raw_v2.DatabaseObjects;

        IF @RawCount = 0
        BEGIN
            SET @ErrorMessage = N'raw_v2.DatabaseObjects is empty. Run atlas_db_objects_extractor.py first.';
            EXEC etl.usp_Atlas_LogEnd
                @LogID        = @LogID,
                @RowsAffected = 0,
                @Status       = N'Failed';

            IF @RaiseErrorOnFail = 1
                RAISERROR(@ErrorMessage, 16, 1);

            PRINT @ErrorMessage;
            RETURN 1;
        END;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RawCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RawCount AS VARCHAR(20)) + ' rows in raw_v2.DatabaseObjects';

        -- =====================================================================
        -- STEP 2: Stage to ReportObjectsStaging (APPEND — do NOT truncate)
        -- =====================================================================
        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Step 2: DatabaseObjects → ReportObjectsStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 1
        BEGIN
            -- Validate column references without inserting
            SELECT TOP 0
                ro.SourceServer + N'||' + ro.SourceDB + N'||' + ro.ReportObjectType
                    + N'||' + CAST(ro.ObjectID AS NVARCHAR(30)) + N'||||'
                                                            AS BizKey,
                ro.Name                                     AS ObjectName,
                ro.ReportObjectType                         AS ObjectType,
                ro.SourceDB + N'.' + ro.SourceSchema + N'.' + ro.Name
                                                            AS ObjectPath,
                ro.[Description]                            AS ObjectDescription,
                N'DatabaseObjects'                          AS SourceSystem,
                ro.SourceServer                             AS SourceServer,
                CAST(NULL AS DATETIME)                      AS CreatedDate,
                ro.LastModifiedDate                         AS ModifiedDate,
                CAST(NULL AS NVARCHAR(200))                 AS CreatedBy,
                CAST(NULL AS NVARCHAR(200))                 AS ModifiedBy,
                CASE WHEN ro.DefaultVisibilityYN = N'N' THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END
                                                            AS IsHidden,
                CAST(NULL AS NVARCHAR(1000))                AS ObjectURL,
                CAST(NULL AS NVARCHAR(1000))                AS ParentPath,
                ro.Query                                    AS RawDefinition,
                ro.ExtractDate                              AS ExtractDate
            FROM raw_v2.DatabaseObjects ro;

            SET @RowCount = 0;
            PRINT @StepName + ': DEBUG — column validation passed (0 rows inserted)';
        END
        ELSE
        BEGIN
            INSERT INTO stage_v2.ReportObjectsStaging (
                BizKey, ObjectName, ObjectType, ObjectPath, ObjectDescription,
                SourceSystem, SourceServer, CreatedDate, ModifiedDate,
                CreatedBy, ModifiedBy, IsHidden, ObjectURL, ParentPath,
                RawDefinition, ExtractDate
            )
            SELECT
                ro.SourceServer + N'||' + ro.SourceDB + N'||' + ro.ReportObjectType
                    + N'||' + CAST(ro.ObjectID AS NVARCHAR(30)) + N'||||'
                                                            AS BizKey,
                ro.Name                                     AS ObjectName,
                ro.ReportObjectType                         AS ObjectType,
                ro.SourceDB + N'.' + ro.SourceSchema + N'.' + ro.Name
                                                            AS ObjectPath,
                ro.[Description]                            AS ObjectDescription,
                N'DatabaseObjects'                          AS SourceSystem,
                ro.SourceServer                             AS SourceServer,
                NULL                                        AS CreatedDate,
                ro.LastModifiedDate                         AS ModifiedDate,
                NULL                                        AS CreatedBy,
                NULL                                        AS ModifiedBy,
                CASE WHEN ro.DefaultVisibilityYN = N'N' THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END
                                                            AS IsHidden,
                NULL                                        AS ObjectURL,
                NULL                                        AS ParentPath,
                ro.Query                                    AS RawDefinition,
                ro.ExtractDate                              AS ExtractDate
            FROM raw_v2.DatabaseObjects ro;

            SET @RowCount = @@ROWCOUNT;
        END;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

        -- =====================================================================
        -- STEP 3: Stage to ReportObjectQueryStaging (WHERE Query IS NOT NULL)
        -- =====================================================================
        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Step 3: DatabaseObjects → ReportObjectQueryStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 1
        BEGIN
            -- Validate column references without inserting
            SELECT TOP 0
                ro.SourceServer + N'||' + ro.SourceDB + N'||' + ro.ReportObjectType
                    + N'||' + CAST(ro.ObjectID AS NVARCHAR(30)) + N'||||'
                                                            AS BizKey,
                ro.Query                                    AS QueryText,
                N'SQL'                                      AS QueryType,
                ro.ExtractDate                              AS ExtractDate
            FROM raw_v2.DatabaseObjects ro
            WHERE ro.Query IS NOT NULL;

            SET @RowCount = 0;
            PRINT @StepName + ': DEBUG — column validation passed (0 rows inserted)';
        END
        ELSE
        BEGIN
            INSERT INTO stage_v2.ReportObjectQueryStaging (
                BizKey, QueryText, QueryType, ExtractDate
            )
            SELECT
                ro.SourceServer + N'||' + ro.SourceDB + N'||' + ro.ReportObjectType
                    + N'||' + CAST(ro.ObjectID AS NVARCHAR(30)) + N'||||'
                                                            AS BizKey,
                ro.Query                                    AS QueryText,
                N'SQL'                                      AS QueryType,
                ro.ExtractDate                              AS ExtractDate
            FROM raw_v2.DatabaseObjects ro
            WHERE ro.Query IS NOT NULL;

            SET @RowCount = @@ROWCOUNT;
        END;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';

        -- =====================================================================
        -- COMPLETE
        -- =====================================================================
        IF @Debug = 1
        BEGIN
            PRINT '';
            PRINT '=== usp_Atlas_DatabaseObjects complete ===';
            PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR(10)) + 's';
        END;

    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        IF @LogID IS NOT NULL
            EXEC etl.usp_Atlas_LogError @LogID = @LogID;

        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
            PRINT 'usp_Atlas_DatabaseObjects FAILED: ' + @ErrorMessage;
    END CATCH;

END;
GO

PRINT 'Created procedure: etl.usp_Atlas_DatabaseObjects (v2.0 — Pipeline B)';
GO
