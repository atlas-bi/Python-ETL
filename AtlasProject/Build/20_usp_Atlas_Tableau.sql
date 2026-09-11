/*
================================================================================
Atlas ETL Suite - usp_Atlas_Tableau (V2 Schema)
================================================================================
Migrated from: ETL-Tableau SSIS Package
Purpose: Transform pre-loaded Tableau raw data into Atlas staging tables

*** UPDATED: Uses stage_v2 and raw_v2 schemas ***

Original SSIS Package Details:
  - Created: June 9, 2021
  - Build Count: 19
  - Components: 0 SQL Tasks, 1 Data Flow (Stage), 1 Connection Manager
  - Complexity: Low (simplest package in the suite)

OPTIONAL: Set enable_tableau=False in atlas_config.py to skip this package.

Run this script on: Atlas_Staging

Version: 2.0.1
Last Updated: February 2026
================================================================================
*/

USE Atlas_Staging;
GO

IF OBJECT_ID('etl.usp_Atlas_Tableau', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_Tableau;
GO

CREATE PROCEDURE etl.usp_Atlas_Tableau
    @ExecutionID            UNIQUEIDENTIFIER = NULL,
    @TableauServer          NVARCHAR(200) = 'eptblp01',
    @RaiseErrorOnFail       BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @PackageName NVARCHAR(100) = 'ETL-Tableau';
    DECLARE @LogID BIGINT;
    DECLARE @StepSequence INT = 0;
    DECLARE @RowCount INT;
    DECLARE @TotalRows INT = 0;
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    IF @ExecutionID IS NULL
        SET @ExecutionID = NEWID();
    
    
    -- ══════════════════════════════════════════════════════════════════════════
    -- PREREQUISITE CHECK: Verify raw tables exist and have data
    -- ══════════════════════════════════════════════════════════════════════════
    
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 0 - Verify Raw Tables',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        DECLARE @RawTableCount INT;
        
        SELECT @RawTableCount = COUNT(*)
        FROM (
            SELECT 1 AS t WHERE OBJECT_ID('raw_v2.tableau_reports', 'U') IS NOT NULL
            UNION ALL
            SELECT 1 WHERE OBJECT_ID('raw_v2.tableau_users', 'U') IS NOT NULL
            UNION ALL
            SELECT 1 WHERE OBJECT_ID('raw_v2.tableau_groups', 'U') IS NOT NULL
            UNION ALL
            SELECT 1 WHERE OBJECT_ID('raw_v2.tableau_hierarchy', 'U') IS NOT NULL
        ) x;
        
        IF @RawTableCount < 4
        BEGIN
            RAISERROR('ETL-Tableau requires pre-loaded raw_v2 tables. Missing tables detected. Ensure Tableau REST API extraction has run.', 16, 1);
        END
        
        EXEC etl.usp_Atlas_LogEnd 
            @LogID = @LogID,
            @RowsAffected = @RawTableCount,
            @Status = 'Success';
            
    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        
        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
        BEGIN
            PRINT 'Prerequisite check failed: ' + @ErrorMessage;
            RETURN;
        END
    END CATCH
    
    
    -- ══════════════════════════════════════════════════════════════════════════
    -- STEP 1: Stage Tableau Reports → ReportObjectsStaging
    -- ══════════════════════════════════════════════════════════════════════════
    
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 1 - Stage Tableau Reports',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        INSERT INTO stage_v2.ReportObjectsStaging (
            BizKey,
            ObjectName,
            ObjectType,
            ObjectPath,
            ObjectDescription,
            SourceSystem,
            SourceServer,
            CreatedDate,
            ModifiedDate,
            CreatedBy,
            ModifiedBy,
            IsHidden,
            ObjectURL,
            ParentPath,
            ExtractDate
        )
        SELECT 
            'Tableau|' + @TableauServer + '|' + tr.content_type + '|' + CAST(tr.luid AS NVARCHAR(100)) AS BizKey,
            tr.name AS ObjectName,
            CASE tr.content_type
                WHEN 'workbook' THEN 'Tableau Workbook'
                WHEN 'view' THEN 'Tableau View'
                WHEN 'datasource' THEN 'Tableau Data Source'
                WHEN 'flow' THEN 'Tableau Prep Flow'
                ELSE 'Tableau ' + ISNULL(tr.content_type, 'Unknown')
            END AS ObjectType,
            tr.project_path + '/' + tr.name AS ObjectPath,
            tr.description AS ObjectDescription,
            'Tableau' AS SourceSystem,
            @TableauServer AS SourceServer,
            tr.created_at AS CreatedDate,
            tr.updated_at AS ModifiedDate,
            tr.owner_name AS CreatedBy,
            tr.owner_name AS ModifiedBy,
            0 AS IsHidden,
            'https://' + @TableauServer + '/#/' + 
                CASE tr.content_type
                    WHEN 'workbook' THEN 'workbooks/' + CAST(tr.repository_id AS NVARCHAR(50))
                    WHEN 'view' THEN 'views/' + CAST(tr.repository_id AS NVARCHAR(50))
                    WHEN 'datasource' THEN 'datasources/' + CAST(tr.repository_id AS NVARCHAR(50))
                    ELSE tr.content_type + '/' + CAST(tr.repository_id AS NVARCHAR(50))
                END AS ObjectURL,
            tr.project_path AS ParentPath,
            GETDATE() AS ExtractDate
        FROM raw_v2.tableau_reports tr
        WHERE tr.name IS NOT NULL;
        
        SET @RowCount = @@ROWCOUNT;
        SET @TotalRows = @TotalRows + @RowCount;
        
        EXEC etl.usp_Atlas_LogEnd 
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';
        
        PRINT 'Step 1 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' Tableau reports staged';
        
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
    -- STEP 2: Stage Tableau Users → ReportObjectUser
    -- ══════════════════════════════════════════════════════════════════════════
    
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 2 - Stage Tableau Users',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        INSERT INTO stage_v2.ReportObjectUser (
            UserName,
            UserDisplayName,
            UserEmail,
            UserPrincipalName,
            IsActive,
            SourceSystem,
            ExtractDate
        )
        SELECT DISTINCT
            tu.username AS UserName,
            tu.full_name AS UserDisplayName,
            tu.email AS UserEmail,
            tu.email AS UserPrincipalName,
            CASE WHEN tu.site_role <> 'Unlicensed' THEN 1 ELSE 0 END AS IsActive,
            'Tableau' AS SourceSystem,
            GETDATE() AS ExtractDate
        FROM raw_v2.tableau_users tu
        WHERE tu.username IS NOT NULL
          AND tu.username <> '';
        
        SET @RowCount = @@ROWCOUNT;
        SET @TotalRows = @TotalRows + @RowCount;
        
        EXEC etl.usp_Atlas_LogEnd 
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';
        
        PRINT 'Step 2 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' Tableau users staged';
        
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
    -- STEP 3: Stage Tableau Groups → ReportObjectUserGroups
    -- ══════════════════════════════════════════════════════════════════════════
    
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 3 - Stage Tableau Groups',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        INSERT INTO stage_v2.ReportObjectUserGroups (
            GroupName,
            GroupDescription,
            GroupType,
            SourceSystem,
            ExtractDate
        )
        SELECT DISTINCT
            tg.name AS GroupName,
            tg.name AS GroupDescription,
            CASE 
                WHEN tg.domain_name = 'local' THEN 'Local'
                ELSE 'Domain'
            END AS GroupType,
            'Tableau' AS SourceSystem,
            GETDATE() AS ExtractDate
        FROM raw_v2.tableau_groups tg
        WHERE tg.name IS NOT NULL;
        
        SET @RowCount = @@ROWCOUNT;
        SET @TotalRows = @TotalRows + @RowCount;
        
        EXEC etl.usp_Atlas_LogEnd 
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';
        
        PRINT 'Step 3 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' Tableau groups staged';
        
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
    -- STEP 4: Stage Tableau Hierarchy → ReportObjectHierarchyStaging
    -- ══════════════════════════════════════════════════════════════════════════
    
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 4 - Stage Tableau Hierarchy',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        INSERT INTO stage_v2.ReportObjectHierarchyStaging (
            ParentBizKey,
            ChildBizKey,
            RelationshipType,
            SourceSystem,
            ExtractDate
        )
        SELECT 
            'Tableau|' + @TableauServer + '|workbook|' + CAST(th.workbook_luid AS NVARCHAR(100)) AS ParentBizKey,
            'Tableau|' + @TableauServer + '|view|' + CAST(th.view_luid AS NVARCHAR(100)) AS ChildBizKey,
            'Contains' AS RelationshipType,
            'Tableau' AS SourceSystem,
            GETDATE() AS ExtractDate
        FROM raw_v2.tableau_hierarchy th
        WHERE th.workbook_luid IS NOT NULL
          AND th.view_luid IS NOT NULL;
        
        SET @RowCount = @@ROWCOUNT;
        SET @TotalRows = @TotalRows + @RowCount;
        
        EXEC etl.usp_Atlas_LogEnd 
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';
        
        PRINT 'Step 4 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' hierarchy relationships staged';
        
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
    -- STEP 5: Stage Tableau Data Sources → ReportObjectDataSourcesStaging
    -- ══════════════════════════════════════════════════════════════════════════
    
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 5 - Stage Tableau Data Sources',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        INSERT INTO stage_v2.ReportObjectDataSourcesStaging (
            BizKey,
            DataSourceName,
            DataSourceType,
            ConnectionString,
            ServerName,
            DatabaseName,
            SourceSystem,
            ExtractDate
        )
        SELECT 
            'Tableau|' + @TableauServer + '|datasource|' + CAST(tr.luid AS NVARCHAR(100)) AS BizKey,
            tr.name AS DataSourceName,
            'Tableau Published Data Source' AS DataSourceType,
            NULL AS ConnectionString,
            @TableauServer AS ServerName,
            NULL AS DatabaseName,
            'Tableau' AS SourceSystem,
            GETDATE() AS ExtractDate
        FROM raw_v2.tableau_reports tr
        WHERE tr.content_type = 'datasource';
        
        SET @RowCount = @@ROWCOUNT;
        SET @TotalRows = @TotalRows + @RowCount;
        
        EXEC etl.usp_Atlas_LogEnd 
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';
        
        PRINT 'Step 5 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' data sources staged';
        
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
    PRINT 'ETL-Tableau completed. Total rows staged: ' + CAST(@TotalRows AS VARCHAR(10));
    
END
GO

PRINT 'Created procedure: etl.usp_Atlas_Tableau (uses stage_v2/raw_v2 schemas)';
GO
