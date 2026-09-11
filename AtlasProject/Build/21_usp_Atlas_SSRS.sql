/*
================================================================================
Atlas ETL Suite - usp_Atlas_SSRS (V2 Schema)
================================================================================
Migrated from: ETL-SSRS1 and ETL-SSRS2 SSIS Packages (consolidated)
Purpose: Extract SSRS ReportServer catalog data

*** UPDATED: Uses stage_v2 and raw_v2 schemas ***

OPTIONAL: Set enable_ssrs=False in atlas_config.py to skip this package.

Run this script on: Atlas_Staging

Version: 2.0.1
Last Updated: February 2026
================================================================================
*/

USE Atlas_Staging;
GO

IF OBJECT_ID('etl.usp_Atlas_SSRS', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_SSRS;
GO

CREATE PROCEDURE etl.usp_Atlas_SSRS
    @ExecutionID            UNIQUEIDENTIFIER = NULL,
    @SSRSServer             NVARCHAR(200),
    @SSRSDatabase           NVARCHAR(128) = 'ReportServer',
    @TablePrefix            NVARCHAR(50) = 'SSRS_',
    @BaseURL                NVARCHAR(500) = NULL,
    @HBIUsersGUID           NVARCHAR(100) = NULL,
    @RaiseErrorOnFail       BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @PackageName NVARCHAR(100) = 'ETL-SSRS';
    DECLARE @LogID BIGINT;
    DECLARE @StepSequence INT = 0;
    DECLARE @RowCount INT;
    DECLARE @TotalRows INT = 0;
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @SQL NVARCHAR(MAX);
    
    IF @ExecutionID IS NULL
        SET @ExecutionID = NEWID();
    
    IF @BaseURL IS NULL
        SET @BaseURL = 'http://' + @SSRSServer + '/Reports';
    
    DECLARE @CatalogTable NVARCHAR(200) = 'raw_v2.' + @TablePrefix + 'Catalog';
    DECLARE @PolicyUserRoleTable NVARCHAR(200) = 'raw_v2.' + @TablePrefix + 'PolicyUserRole';
    DECLARE @SubscriptionsTable NVARCHAR(200) = 'raw_v2.' + @TablePrefix + 'Subscriptions';
    DECLARE @UsersTable NVARCHAR(200) = 'raw_v2.' + @TablePrefix + 'Users';
    
    
    -- ══════════════════════════════════════════════════════════════════════════
    -- STEP 1: Create Raw Staging Tables in raw_v2 schema
    -- ══════════════════════════════════════════════════════════════════════════
    
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 1 - Create Raw Tables for ' + @SSRSServer,
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        -- Drop and recreate Catalog table
        SET @SQL = 'IF OBJECT_ID(''' + @CatalogTable + ''', ''U'') IS NOT NULL DROP TABLE ' + @CatalogTable;
        EXEC sp_executesql @SQL;
        
        SET @SQL = '
        CREATE TABLE ' + @CatalogTable + ' (
            ItemID              UNIQUEIDENTIFIER NULL,
            Path                NVARCHAR(425) NULL,
            Name                NVARCHAR(425) NULL,
            ParentID            UNIQUEIDENTIFIER NULL,
            Type                INT NULL,
            Content             VARBINARY(MAX) NULL,
            Description         NVARCHAR(512) NULL,
            Hidden              BIT NULL,
            CreatedByID         UNIQUEIDENTIFIER NULL,
            CreationDate        DATETIME NULL,
            ModifiedByID        UNIQUEIDENTIFIER NULL,
            ModifiedDate        DATETIME NULL,
            MimeType            NVARCHAR(260) NULL,
            PolicyID            UNIQUEIDENTIFIER NULL,
            ExtractDate         DATETIME NOT NULL DEFAULT GETDATE()
        )';
        EXEC sp_executesql @SQL;
        
        SET @SQL = 'IF OBJECT_ID(''' + @PolicyUserRoleTable + ''', ''U'') IS NOT NULL DROP TABLE ' + @PolicyUserRoleTable;
        EXEC sp_executesql @SQL;
        
        SET @SQL = '
        CREATE TABLE ' + @PolicyUserRoleTable + ' (
            ID                  UNIQUEIDENTIFIER NOT NULL,
            RoleID              UNIQUEIDENTIFIER NOT NULL,
            UserID              UNIQUEIDENTIFIER NOT NULL,
            PolicyID            UNIQUEIDENTIFIER NOT NULL,
            ExtractDate         DATETIME NOT NULL DEFAULT GETDATE()
        )';
        EXEC sp_executesql @SQL;
        
        SET @SQL = 'IF OBJECT_ID(''' + @SubscriptionsTable + ''', ''U'') IS NOT NULL DROP TABLE ' + @SubscriptionsTable;
        EXEC sp_executesql @SQL;
        
        SET @SQL = '
        CREATE TABLE ' + @SubscriptionsTable + ' (
            SubscriptionID      UNIQUEIDENTIFIER NOT NULL,
            OwnerID             UNIQUEIDENTIFIER NOT NULL,
            Report_OID          UNIQUEIDENTIFIER NOT NULL,
            ExtensionSettings   NVARCHAR(MAX) NULL,
            ModifiedByID        UNIQUEIDENTIFIER NOT NULL,
            ModifiedDate        DATETIME NOT NULL,
            Description         NVARCHAR(512) NULL,
            LastStatus          NVARCHAR(260) NULL,
            DeliveryExtension   NVARCHAR(260) NULL,
            ExtractDate         DATETIME NOT NULL DEFAULT GETDATE(),
            CONSTRAINT PK_' + REPLACE(@TablePrefix, '_', '') + '_Subscriptions PRIMARY KEY (SubscriptionID)
        )';
        EXEC sp_executesql @SQL;
        
        SET @SQL = 'IF OBJECT_ID(''' + @UsersTable + ''', ''U'') IS NOT NULL DROP TABLE ' + @UsersTable;
        EXEC sp_executesql @SQL;
        
        SET @SQL = '
        CREATE TABLE ' + @UsersTable + ' (
            UserID              UNIQUEIDENTIFIER NULL,
            Sid                 VARBINARY(85) NULL,
            UserType            INT NULL,
            AuthType            INT NULL,
            UserName            NVARCHAR(260) NULL,
            ExtractDate         DATETIME NOT NULL DEFAULT GETDATE()
        )';
        EXEC sp_executesql @SQL;
        
        EXEC etl.usp_Atlas_LogEnd 
            @LogID = @LogID,
            @RowsAffected = 4,
            @Status = 'Success';
        
        PRINT 'Step 1 completed: Raw tables created in raw_v2 schema';
        
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
    -- STEP 2: Load Raw Data from ReportServer
    -- ══════════════════════════════════════════════════════════════════════════
    
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 2 - Load Raw from ' + @SSRSServer,
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        SET @SQL = '
        INSERT INTO ' + @CatalogTable + ' (
            ItemID, Path, Name, ParentID, Type, Content, Description,
            Hidden, CreatedByID, CreationDate, ModifiedByID, ModifiedDate,
            MimeType, PolicyID
        )
        SELECT 
            ItemID, Path, Name, ParentID, Type, Content, Description,
            Hidden, CreatedByID, CreationDate, ModifiedByID, ModifiedDate,
            MimeType, PolicyID
        FROM ' + QUOTENAME(@SSRSServer) + '.' + QUOTENAME(@SSRSDatabase) + '.dbo.Catalog';
        EXEC sp_executesql @SQL;
        SET @RowCount = @@ROWCOUNT;
        SET @TotalRows = @TotalRows + @RowCount;
        PRINT '  Catalog: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        
        SET @SQL = '
        INSERT INTO ' + @PolicyUserRoleTable + ' (ID, RoleID, UserID, PolicyID)
        SELECT ID, RoleID, UserID, PolicyID
        FROM ' + QUOTENAME(@SSRSServer) + '.' + QUOTENAME(@SSRSDatabase) + '.dbo.PolicyUserRole';
        EXEC sp_executesql @SQL;
        SET @RowCount = @@ROWCOUNT;
        SET @TotalRows = @TotalRows + @RowCount;
        PRINT '  PolicyUserRole: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        
        SET @SQL = '
        INSERT INTO ' + @SubscriptionsTable + ' (
            SubscriptionID, OwnerID, Report_OID, ExtensionSettings,
            ModifiedByID, ModifiedDate, Description, LastStatus, DeliveryExtension
        )
        SELECT 
            SubscriptionID, OwnerID, Report_OID, CAST(ExtensionSettings AS NVARCHAR(MAX)),
            ModifiedByID, ModifiedDate, Description, LastStatus, DeliveryExtension
        FROM ' + QUOTENAME(@SSRSServer) + '.' + QUOTENAME(@SSRSDatabase) + '.dbo.Subscriptions';
        EXEC sp_executesql @SQL;
        SET @RowCount = @@ROWCOUNT;
        SET @TotalRows = @TotalRows + @RowCount;
        PRINT '  Subscriptions: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        
        SET @SQL = '
        INSERT INTO ' + @UsersTable + ' (UserID, Sid, UserType, AuthType, UserName)
        SELECT UserID, Sid, UserType, AuthType, UserName
        FROM ' + QUOTENAME(@SSRSServer) + '.' + QUOTENAME(@SSRSDatabase) + '.dbo.Users';
        EXEC sp_executesql @SQL;
        SET @RowCount = @@ROWCOUNT;
        SET @TotalRows = @TotalRows + @RowCount;
        PRINT '  Users: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        
        EXEC etl.usp_Atlas_LogEnd 
            @LogID = @LogID,
            @RowsAffected = @TotalRows,
            @Status = 'Success';
        
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
    -- STEP 3: Create Index on PolicyID
    -- ══════════════════════════════════════════════════════════════════════════
    
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 3 - Create PolicyID Index',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        SET @SQL = '
        CREATE NONCLUSTERED INDEX IX_' + REPLACE(@TablePrefix, '_', '') + '_Catalog_PolicyID 
        ON ' + @CatalogTable + ' (PolicyID) 
        INCLUDE (ItemID, Name, Type)';
        EXEC sp_executesql @SQL;
        
        EXEC etl.usp_Atlas_LogEnd 
            @LogID = @LogID,
            @RowsAffected = 1,
            @Status = 'Success';
        
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
    -- STEP 4: Stage Reports → stage_v2.ReportObjectsStaging
    -- ══════════════════════════════════════════════════════════════════════════
    
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 4 - Stage Reports',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        SET @SQL = '
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
            IsHidden,
            ObjectURL,
            ParentPath,
            ExtractDate
        )
        SELECT 
            ''SSRS|'' + ''' + @SSRSServer + ''' + ''|'' + CAST(c.ItemID AS NVARCHAR(100)) AS BizKey,
            c.Name AS ObjectName,
            CASE c.Type
                WHEN 1 THEN ''SSRS Folder''
                WHEN 2 THEN ''SSRS Report''
                WHEN 3 THEN ''SSRS File''
                WHEN 4 THEN ''SSRS Linked Report''
                WHEN 5 THEN ''SSRS Datasource''
                WHEN 6 THEN ''SSRS Model''
                WHEN 8 THEN ''SSRS Shared Dataset''
                WHEN 9 THEN ''SSRS Report Part''
                WHEN 11 THEN ''SSRS KPI''
                WHEN 13 THEN ''SSRS Power BI Report''
                ELSE ''SSRS Unknown''
            END AS ObjectType,
            c.Path AS ObjectPath,
            c.Description AS ObjectDescription,
            ''SSRS'' AS SourceSystem,
            ''' + @SSRSServer + ''' AS SourceServer,
            c.CreationDate AS CreatedDate,
            c.ModifiedDate AS ModifiedDate,
            ISNULL(c.Hidden, 0) AS IsHidden,
            CASE 
                WHEN c.Type IN (2, 4) THEN ''' + @BaseURL + '/report'' + REPLACE(c.Path, '' '', ''%20'')
                WHEN c.Type = 1 THEN ''' + @BaseURL + '/browse'' + REPLACE(c.Path, '' '', ''%20'')
                ELSE ''' + @BaseURL + '/manage/catalogitem/properties'' + REPLACE(c.Path, '' '', ''%20'')
            END AS ObjectURL,
            CASE 
                WHEN CHARINDEX(''/'', REVERSE(c.Path)) > 0 
                THEN LEFT(c.Path, LEN(c.Path) - CHARINDEX(''/'', REVERSE(c.Path)))
                ELSE ''''
            END AS ParentPath,
            GETDATE() AS ExtractDate
        FROM ' + @CatalogTable + ' c
        WHERE c.Name IS NOT NULL';
        
        EXEC sp_executesql @SQL;
        SET @RowCount = @@ROWCOUNT;
        
        EXEC etl.usp_Atlas_LogEnd 
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';
        
        PRINT 'Step 4 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' reports staged';
        
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
    -- STEP 5: Stage Users → stage_v2.ReportObjectUser
    -- ══════════════════════════════════════════════════════════════════════════
    
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 5 - Stage Users',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        SET @SQL = '
        INSERT INTO stage_v2.ReportObjectUser (
            UserName,
            UserDisplayName,
            IsActive,
            SourceSystem,
            ExtractDate
        )
        SELECT DISTINCT
            u.UserName AS UserName,
            u.UserName AS UserDisplayName,
            1 AS IsActive,
            ''SSRS'' AS SourceSystem,
            GETDATE() AS ExtractDate
        FROM ' + @UsersTable + ' u
        WHERE u.UserName IS NOT NULL
          AND u.UserName <> ''''';
        
        EXEC sp_executesql @SQL;
        SET @RowCount = @@ROWCOUNT;
        
        EXEC etl.usp_Atlas_LogEnd 
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';
        
        PRINT 'Step 5 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' users staged';
        
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
    PRINT 'ETL-SSRS completed for ' + @SSRSServer;
    
END
GO

PRINT 'Created procedure: etl.usp_Atlas_SSRS (uses stage_v2/raw_v2 schemas)';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- WRAPPER: Run for all configured SSRS servers
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('etl.usp_Atlas_SSRS_All', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_SSRS_All;
GO

CREATE PROCEDURE etl.usp_Atlas_SSRS_All
    @ExecutionID            UNIQUEIDENTIFIER = NULL,
    @RaiseErrorOnFail       BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @ExecutionID IS NULL
        SET @ExecutionID = NEWID();
    
    EXEC etl.usp_Atlas_SSRS
        @ExecutionID = @ExecutionID,
        @SSRSServer = 'ssrs_database',
        @SSRSDatabase = 'ReportServer',
        @TablePrefix = 'SSRS1_',
        @BaseURL = 'http://ssrs_database/Reports',
        @HBIUsersGUID = 'F1B753AC-1229-400F-B272-C74373674483',
        @RaiseErrorOnFail = @RaiseErrorOnFail;

    EXEC etl.usp_Atlas_SSRS
        @ExecutionID = @ExecutionID,
        @SSRSServer = 'clarity_server',
        @SSRSDatabase = 'ReportServer',
        @TablePrefix = 'SSRS2_',
        @BaseURL = 'http://clarity_server/Reports',
        @HBIUsersGUID = '2AEFA3AF-6E5A-43B9-94F2-3D610E31F5D1',
        @RaiseErrorOnFail = @RaiseErrorOnFail;
    
    PRINT 'All SSRS servers processed.';
END
GO

PRINT 'Created procedure: etl.usp_Atlas_SSRS_All';
GO
