/*
================================================================================
Atlas ETL Suite - usp_Atlas_PostProcessing (V2 - Fixed Type Conversion)
================================================================================
Updated to use CASE statements in visibility rules to prevent type conversion
errors when MatchType and MatchValue have different data type expectations.

Run this script on: Atlas_Staging

Version: 2.1.0
Last Updated: April 2026
================================================================================
*/

USE Atlas_Staging;
GO

IF OBJECT_ID('etl.usp_Atlas_PostProcessing', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_PostProcessing;
GO

CREATE PROCEDURE etl.usp_Atlas_PostProcessing
    @ExecutionID            UNIQUEIDENTIFIER = NULL,
    @StagingSchema          NVARCHAR(128) = 'stage_v2',
    @ProdSchema             NVARCHAR(128) = 'prd_v2',
    @ProdDatabase           NVARCHAR(128) = '',
    @RaiseErrorOnFail       BIT = 1,
    @Debug                  BIT = 0
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
    -- 'ReportObject' (singular). Same resolver pattern as 13_usp_Atlas_RunData
    -- and 14_usp_Atlas_Merge.
    DECLARE @PrdReportObjectTable NVARCHAR(128) =
        CASE
            WHEN @ProdDatabase = N'' OR @ProdDatabase IS NULL
                THEN N'ReportObjects'
            ELSE N'ReportObject'
        END;

    DECLARE @PackageName NVARCHAR(100) = 'ETL-PostProcessing';
    DECLARE @LogID BIGINT;
    DECLARE @StepSequence INT = 0;
    DECLARE @RowCount INT;
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    IF @ExecutionID IS NULL
        SET @ExecutionID = NEWID();
    
    IF @Debug = 1
    BEGIN
        PRINT '=== usp_Atlas_PostProcessing — DEBUG MODE ===';
        PRINT 'ExecutionID: ' + CAST(@ExecutionID AS VARCHAR(36));
        PRINT 'DML statements will be skipped; logging and validation only.';
        PRINT '';
    END

    PRINT 'Using staging schema: ' + @StagingSchema;
    PRINT 'Using production schema: ' + @ProdSchema;
    PRINT '';
    
    
    -- ══════════════════════════════════════════════════════════════════════════
    -- STEP 1: Update Orphan Flags
    -- ══════════════════════════════════════════════════════════════════════════
    
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 1 - Update Orphan Flags',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        SET @SQL = N'
        UPDATE ro
        SET ro.OrphanedReportObjectYN = ''Y''
        FROM ' + @PrdPrefix + @PrdReportObjectTable + N' ro
        WHERE (ro.OrphanedReportObjectYN = ''N'' OR ro.OrphanedReportObjectYN IS NULL)
          AND ro.LastSeenDate < DATEADD(DAY, -7, GETDATE())
          AND ro.ReportObjectTypeID NOT IN (
              SELECT dot.ReportObjectTypeID 
              FROM ' + @PrdPrefix + N'DoNotOrphanTypes dot
              WHERE dot.IsActive = 1
          )
          AND NOT EXISTS (
              SELECT 1 
              FROM ' + @StgPrefix + N'ReportObjectsStaging ros
              WHERE ros.BizKey = ro.BizKey
          );
        ';
        
        IF @Debug = 0
        BEGIN
            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            SET @RowCount = 0;
            PRINT 'Step 1: DEBUG — skipped orphan flag UPDATE (0 rows)';
        END

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';

        PRINT 'Step 1 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' reports marked as orphaned';
        
    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        
        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
            PRINT 'Step 1 failed: ' + @ErrorMessage;
    END CATCH


    -- ══════════════════════════════════════════════════════════════════════════
    -- STEP 1b: Reverse Orphan Flags for Protected Types
    -- Mirrors SSIS ETL-PostProcessing Task 3:
    -- Update Orphaned PowerBI Reports
    -- Source: Atlas_Combined_SQL.sql
    --   ETL-PostProcessing/SQL/03_Update_Orphaned_PowerBI_Reports.sql
    -- Gap identified 2026-04-30 — was missing from Pipeline B.
    --
    -- Step 1's NOT IN (DoNotOrphanTypes) clause prevents protected types
    -- from being newly orphaned this run, but Merge 1's WHEN NOT MATCHED
    -- BY SOURCE flips OrphanedReportObjectYN='Y' on every prd row missing
    -- from the staging set, regardless of TypeID. This step is the SSIS
    -- safety net that resets the flag for any TypeID currently in the
    -- DoNotOrphanTypes config table. The cross-database reference to
    -- Atlas_Staging.dbo.DoNotOrphanTypes is hardcoded to mirror SSIS.
    -- ══════════════════════════════════════════════════════════════════════════

    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 1b - Reverse Orphan Flags for Protected Types',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        SET @SQL = N'
        UPDATE ro
        SET ro.OrphanedReportObjectYN = ''N''
        FROM ' + @PrdPrefix + @PrdReportObjectTable + N' ro
        WHERE OrphanedReportObjectYN = ''Y''
          AND ReportObjectTypeID IN (
              SELECT ReportObjectTypeID
              FROM Atlas_Staging.dbo.DoNotOrphanTypes
          );
        ';

        IF @Debug = 0
        BEGIN
            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            SET @RowCount = 0;
            PRINT 'Step 1b: DEBUG — skipped reverse orphan flag UPDATE (0 rows)';
        END

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';

        PRINT 'Step 1b completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' protected-type reports un-orphaned';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();

        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
            PRINT 'Step 1b failed: ' + @ErrorMessage;
    END CATCH


    -- ══════════════════════════════════════════════════════════════════════════
    -- STEP 2: Reverse Orphan Flags for Rediscovered Reports
    -- ══════════════════════════════════════════════════════════════════════════
    
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 2 - Reverse Orphan Flags',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        SET @SQL = N'
        UPDATE ro
        SET ro.OrphanedReportObjectYN = ''N'',
            ro.LastSeenDate = GETDATE()
        FROM ' + @PrdPrefix + @PrdReportObjectTable + N' ro
        WHERE ro.OrphanedReportObjectYN = ''Y''
          AND EXISTS (
              SELECT 1 
              FROM ' + @StgPrefix + N'ReportObjectsStaging ros
              WHERE ros.BizKey = ro.BizKey
          );
        ';
        
        IF @Debug = 0
        BEGIN
            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            SET @RowCount = 0;
            PRINT 'Step 2: DEBUG — skipped reverse orphan UPDATE (0 rows)';
        END

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';

        PRINT 'Step 2 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' reports un-orphaned';
        
    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        
        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
            PRINT 'Step 2 failed: ' + @ErrorMessage;
    END CATCH
    
    
    -- ══════════════════════════════════════════════════════════════════════════
    -- STEP 3: Apply URL Overrides
    -- ══════════════════════════════════════════════════════════════════════════
    
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 3 - Apply URL Overrides',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        SET @SQL = N'
        UPDATE ro
        SET ro.ReportObjectURL = uo.OverrideURL
        FROM ' + @PrdPrefix + @PrdReportObjectTable + N' ro
        INNER JOIN ' + @PrdPrefix + N'URLOverrides uo
            ON ro.BizKey = uo.BizKey
        WHERE uo.IsActive = 1
          AND (ro.ReportObjectURL <> uo.OverrideURL OR ro.ReportObjectURL IS NULL);
        ';
        
        IF @Debug = 0
        BEGIN
            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            SET @RowCount = 0;
            PRINT 'Step 3: DEBUG — skipped URL override UPDATE (0 rows)';
        END

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';

        PRINT 'Step 3 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' URL overrides applied';
        
    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        
        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
            PRINT 'Step 3 failed: ' + @ErrorMessage;
    END CATCH
    
    
    -- ══════════════════════════════════════════════════════════════════════════
    -- STEP 4: Update Visibility (DefaultVisibilityYN) - Hide
    -- Uses CASE to prevent type conversion errors
    -- ══════════════════════════════════════════════════════════════════════════
    
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 4 - Update Visibility Rules',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        SET @SQL = N'
        UPDATE ro
        SET ro.DefaultVisibilityYN = ''N''
        FROM ' + @PrdPrefix + @PrdReportObjectTable + N' ro
        INNER JOIN ' + @PrdPrefix + N'VisibilityRules vr
            ON vr.IsActive = 1
            AND vr.Action = ''Hide''
            AND (
                CASE 
                    WHEN vr.MatchType = ''ReportObjectTypeID'' AND TRY_CAST(vr.MatchValue AS INT) IS NOT NULL
                        THEN CASE WHEN ro.ReportObjectTypeID = CAST(vr.MatchValue AS INT) THEN 1 ELSE 0 END
                    WHEN vr.MatchType = ''PathContains''
                        THEN CASE WHEN ro.ObjectPath LIKE ''%'' + vr.MatchValue + ''%'' THEN 1 ELSE 0 END
                    WHEN vr.MatchType = ''NameContains''
                        THEN CASE WHEN ro.ObjectName LIKE ''%'' + vr.MatchValue + ''%'' THEN 1 ELSE 0 END
                    WHEN vr.MatchType = ''SourceSystem''
                        THEN CASE WHEN ro.SourceSystem = vr.MatchValue THEN 1 ELSE 0 END
                    ELSE 0
                END = 1
            )
        WHERE (ro.DefaultVisibilityYN = ''Y'' OR ro.DefaultVisibilityYN IS NULL);
        ';
        
        IF @Debug = 0
        BEGIN
            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

        SET @SQL = N'
        UPDATE ro
        SET ro.DefaultVisibilityYN = ''Y''
        FROM ' + @PrdPrefix + @PrdReportObjectTable + N' ro
        INNER JOIN ' + @PrdPrefix + N'VisibilityRules vr
            ON vr.IsActive = 1
            AND vr.Action = ''Show''
            AND (
                CASE 
                    WHEN vr.MatchType = ''ReportObjectTypeID'' AND TRY_CAST(vr.MatchValue AS INT) IS NOT NULL
                        THEN CASE WHEN ro.ReportObjectTypeID = CAST(vr.MatchValue AS INT) THEN 1 ELSE 0 END
                    WHEN vr.MatchType = ''PathContains''
                        THEN CASE WHEN ro.ObjectPath LIKE ''%'' + vr.MatchValue + ''%'' THEN 1 ELSE 0 END
                    WHEN vr.MatchType = ''NameContains''
                        THEN CASE WHEN ro.ObjectName LIKE ''%'' + vr.MatchValue + ''%'' THEN 1 ELSE 0 END
                    WHEN vr.MatchType = ''SourceSystem''
                        THEN CASE WHEN ro.SourceSystem = vr.MatchValue THEN 1 ELSE 0 END
                    ELSE 0
                END = 1
            )
        WHERE ro.DefaultVisibilityYN = ''N'';
        ';
        
        IF @Debug = 0
        BEGIN
            EXEC sp_executesql @SQL;
            SET @RowCount = @RowCount + @@ROWCOUNT;
        END

        IF @Debug = 1
            PRINT 'Step 4: DEBUG — skipped visibility rule UPDATEs (0 rows)';

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';

        PRINT 'Step 4 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' visibility rules applied';
        
    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        
        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
            PRINT 'Step 4 failed: ' + @ErrorMessage;
    END CATCH
    
    
    -- ══════════════════════════════════════════════════════════════════════════
    -- STEP 5: Update DistinctUsersPast12Months
    -- Rolling 12-month COUNT(DISTINCT RunUserID) per ReportObject.
    -- Join path: ReportObjectRunDataBridge → ReportObjectRunData → RunUserID
    -- Replaces the removed Phase 10 update from usp_Atlas_RunData (2026-03-30).
    -- ══════════════════════════════════════════════════════════════════════════

    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 5 - DistinctUsersPast12Months',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        SET @SQL = N'
        UPDATE ro
        SET ro.DistinctUsersPast12Months = counts.UserCount
        FROM ' + @PrdPrefix + @PrdReportObjectTable + N' ro
        INNER JOIN (
            SELECT
                b.ReportObjectId,
                COUNT(DISTINCT rd.RunUserID) AS UserCount
            FROM ' + @PrdPrefix + N'ReportObjectRunDataBridge b
            INNER JOIN ' + @PrdPrefix + N'ReportObjectRunData rd
                ON b.RunId = rd.RunDataId
            WHERE rd.RunUserID IS NOT NULL
              AND rd.RunStartTime >= DATEADD(MONTH, -12, GETDATE())
            GROUP BY b.ReportObjectId
        ) counts
            ON ro.ReportObjectID = counts.ReportObjectId;
        ';

        IF @Debug = 0
        BEGIN
            EXEC sp_executesql @SQL;
            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            SET @RowCount = 0;
            PRINT 'Step 5: DEBUG — skipped DistinctUsersPast12Months UPDATE (0 rows)';
        END

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';

        PRINT 'Step 5 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' report objects updated with user counts';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();

        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
            PRINT 'Step 5 failed: ' + @ErrorMessage;
    END CATCH


    PRINT '';
    PRINT 'ETL-PostProcessing completed.';
    
END
GO

PRINT 'Created procedure: etl.usp_Atlas_PostProcessing (fixed type conversion)';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- Re-populate VisibilityRules with sample data (now safe to use)
-- ══════════════════════════════════════════════════════════════════════════════

TRUNCATE TABLE prd_v2.VisibilityRules;

INSERT INTO prd_v2.VisibilityRules (RuleName, MatchType, MatchValue, Action, Notes) VALUES
    ('Hide Test Reports', 'NameContains', '_TEST', 'Hide', 'Hide reports with _TEST in name'),
    ('Hide Dev Folder', 'PathContains', '/Development/', 'Hide', 'Hide development folder contents'),
    ('Hide Backup Reports', 'NameContains', '_BAK', 'Hide', 'Hide backup copies of reports'),
    ('Hide Draft Reports', 'NameContains', '_DRAFT', 'Hide', 'Hide draft reports');

PRINT 'Re-populated prd_v2.VisibilityRules with sample data';
GO
