/*
================================================================================
Atlas ETL Suite - ETL Logging Infrastructure
================================================================================
Creates the etl schema, Atlas_ETL_Log table, and 4 logging stored procedures.
This must run BEFORE any other Atlas ETL stored procedure, as every SP in the
pipeline depends on these logging components.

Objects created:
  - Schema:    etl (if not exists)
  - Table:     etl.Atlas_ETL_Log (with computed DurationSeconds, audit columns)
  - Procedure: etl.usp_Atlas_LogStart
  - Procedure: etl.usp_Atlas_LogEnd
  - Procedure: etl.usp_Atlas_LogError
  - Procedure: etl.usp_Atlas_LogErrorManual

Run this script on: Atlas_Staging
Run order: After database exists, before 10_usp_Atlas_Setup.sql

Version: 1.1
Last Updated: March 2026
v1.1 Changes:
  - Table DDL now matches production server exactly (scripted from server)
  - Added computed column DurationSeconds
  - Added audit columns: ServerName, DatabaseName, UserName (with defaults)
  - Added CHECK constraint on Status column
  - datetime2(3) precision matches production
================================================================================
*/

USE Atlas_Staging;
GO

-- ══════════════════════════════════════════════════════════════════════════════
-- 1. CREATE etl SCHEMA (if not exists)
-- ══════════════════════════════════════════════════════════════════════════════

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
BEGIN
    EXEC('CREATE SCHEMA etl AUTHORIZATION dbo');
    PRINT 'Created schema: etl';
END
ELSE
    PRINT 'Schema etl already exists';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 2. CREATE Atlas_ETL_Log TABLE
--    Scripted from production server: Atlas_Staging.[etl].[Atlas_ETL_Log]
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('etl.Atlas_ETL_Log', 'U') IS NULL
BEGIN
    CREATE TABLE [etl].[Atlas_ETL_Log] (
        [LogID]             [bigint] IDENTITY(1,1) NOT NULL,
        [ExecutionID]       [uniqueidentifier] NOT NULL,
        [PackageName]       [nvarchar](100) NOT NULL,
        [StepName]          [nvarchar](200) NOT NULL,
        [StepSequence]      [int] NULL,
        [StartTime]         [datetime2](3) NOT NULL,
        [EndTime]           [datetime2](3) NULL,
        [DurationSeconds]   AS (DATEDIFF(SECOND, [StartTime], [EndTime])),
        [RowsAffected]      [int] NULL,
        [Status]            [nvarchar](20) NOT NULL,
        [ErrorNumber]       [int] NULL,
        [ErrorSeverity]     [int] NULL,
        [ErrorState]        [int] NULL,
        [ErrorMessage]      [nvarchar](4000) NULL,
        [ErrorProcedure]    [nvarchar](200) NULL,
        [ErrorLine]         [int] NULL,
        [ServerName]        [nvarchar](128) NOT NULL,
        [DatabaseName]      [nvarchar](128) NOT NULL,
        [UserName]          [nvarchar](128) NOT NULL,
        CONSTRAINT [PK_Atlas_ETL_Log] PRIMARY KEY CLUSTERED ([LogID] ASC)
    );

    -- Defaults
    ALTER TABLE [etl].[Atlas_ETL_Log] ADD DEFAULT (SYSDATETIME()) FOR [StartTime];
    ALTER TABLE [etl].[Atlas_ETL_Log] ADD DEFAULT ('Running') FOR [Status];
    ALTER TABLE [etl].[Atlas_ETL_Log] ADD DEFAULT (@@SERVERNAME) FOR [ServerName];
    ALTER TABLE [etl].[Atlas_ETL_Log] ADD DEFAULT (DB_NAME()) FOR [DatabaseName];
    ALTER TABLE [etl].[Atlas_ETL_Log] ADD DEFAULT (SUSER_SNAME()) FOR [UserName];

    -- Status check constraint
    ALTER TABLE [etl].[Atlas_ETL_Log] WITH CHECK
        ADD CONSTRAINT [CK_Atlas_ETL_Log_Status]
        CHECK ([Status] IN ('Running', 'Success', 'Warning', 'Failure'));

    -- Indexes for common query patterns
    CREATE NONCLUSTERED INDEX [IX_Atlas_ETL_Log_StartTime]
        ON [etl].[Atlas_ETL_Log] ([StartTime] DESC)
        INCLUDE ([PackageName], [Status]);

    CREATE NONCLUSTERED INDEX [IX_Atlas_ETL_Log_ExecutionID]
        ON [etl].[Atlas_ETL_Log] ([ExecutionID])
        INCLUDE ([PackageName], [StepName], [Status]);

    PRINT 'Created table: etl.Atlas_ETL_Log';
END
ELSE
    PRINT 'Table etl.Atlas_ETL_Log already exists';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 3. CREATE usp_Atlas_LogStart
--    Inserts a new log row with Status='Running' and returns the LogID.
--    ServerName, DatabaseName, UserName auto-populate from defaults.
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('etl.usp_Atlas_LogStart', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_LogStart;
GO

CREATE PROCEDURE [etl].[usp_Atlas_LogStart]
    @ExecutionID    UNIQUEIDENTIFIER,
    @PackageName    NVARCHAR(100),
    @StepName       NVARCHAR(200),
    @StepSequence   INT = NULL,
    @LogID          BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO etl.Atlas_ETL_Log (
        ExecutionID,
        PackageName,
        StepName,
        StepSequence,
        Status
    )
    VALUES (
        @ExecutionID,
        @PackageName,
        @StepName,
        @StepSequence,
        'Running'
    );

    SET @LogID = SCOPE_IDENTITY();
END
GO

PRINT 'Created procedure: etl.usp_Atlas_LogStart';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 4. CREATE usp_Atlas_LogEnd
--    Updates an existing log row with completion status.
--    DurationSeconds auto-computes from StartTime -> EndTime.
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('etl.usp_Atlas_LogEnd', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_LogEnd;
GO

CREATE PROCEDURE [etl].[usp_Atlas_LogEnd]
    @LogID          BIGINT,
    @RowsAffected   INT = NULL,
    @Status         NVARCHAR(20) = 'Success',
    @Message        NVARCHAR(4000) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE etl.Atlas_ETL_Log
    SET EndTime = SYSDATETIME(),
        RowsAffected = @RowsAffected,
        Status = @Status,
        ErrorMessage = CASE WHEN @Status = 'Warning' THEN @Message ELSE NULL END
    WHERE LogID = @LogID;
END
GO

PRINT 'Created procedure: etl.usp_Atlas_LogEnd';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 5. CREATE usp_Atlas_LogError
--    Captures SQL Server error context (must be called inside CATCH block).
--    DurationSeconds auto-computes from StartTime -> EndTime.
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('etl.usp_Atlas_LogError', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_LogError;
GO

CREATE PROCEDURE [etl].[usp_Atlas_LogError]
    @LogID          BIGINT,
    @RowsAffected   INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE etl.Atlas_ETL_Log
    SET EndTime = SYSDATETIME(),
        RowsAffected = @RowsAffected,
        Status = 'Failure',
        ErrorNumber = ERROR_NUMBER(),
        ErrorSeverity = ERROR_SEVERITY(),
        ErrorState = ERROR_STATE(),
        ErrorMessage = ERROR_MESSAGE(),
        ErrorProcedure = ERROR_PROCEDURE(),
        ErrorLine = ERROR_LINE()
    WHERE LogID = @LogID;
END
GO

PRINT 'Created procedure: etl.usp_Atlas_LogError';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- 6. CREATE usp_Atlas_LogErrorManual
--    Logs errors from Python scripts or other contexts where ERROR_*()
--    functions are not available.
-- ══════════════════════════════════════════════════════════════════════════════

IF OBJECT_ID('etl.usp_Atlas_LogErrorManual', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_LogErrorManual;
GO

CREATE PROCEDURE [etl].[usp_Atlas_LogErrorManual]
    @LogID          BIGINT,
    @ErrorMessage   NVARCHAR(4000),
    @ErrorNumber    INT = NULL,
    @RowsAffected   INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE etl.Atlas_ETL_Log
    SET EndTime = SYSDATETIME(),
        RowsAffected = @RowsAffected,
        Status = 'Failure',
        ErrorNumber = @ErrorNumber,
        ErrorMessage = @ErrorMessage
    WHERE LogID = @LogID;
END
GO

PRINT 'Created procedure: etl.usp_Atlas_LogErrorManual';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ══════════════════════════════════════════════════════════════════════════════

PRINT '';
PRINT '════════════════════════════════════════════════════════════';
PRINT 'ETL Logging Infrastructure Complete';
PRINT '════════════════════════════════════════════════════════════';

SELECT 'etl.Atlas_ETL_Log' AS Object, 'Table' AS Type,
       (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'Atlas_ETL_Log') AS ColumnCount
UNION ALL
SELECT 'etl.usp_Atlas_LogStart', 'Procedure', NULL
UNION ALL
SELECT 'etl.usp_Atlas_LogEnd', 'Procedure', NULL
UNION ALL
SELECT 'etl.usp_Atlas_LogError', 'Procedure', NULL
UNION ALL
SELECT 'etl.usp_Atlas_LogErrorManual', 'Procedure', NULL;
GO
