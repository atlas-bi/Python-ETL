/*******************************************************************************
 * Atlas ETL Migration — Narrowed CLARITY_EMP Table DDL
 *
 * Performance fix: replaces the 173-column raw_v2.CLARITY_EMP with a
 * 5-column narrowed version.  The full extraction via fast_executemany
 * takes ~25 min at ~84 rows/sec due to ~4 KB row width.  Narrowing to
 * the 5 consumed columns drops row width to ~700 bytes.
 *
 * Column justification (verified against 12_usp_Atlas_Clarity.sql v7.0):
 *
 *   USER_ID           — JOIN key.  Used in Steps 7, 8, 12, 16, 17
 *                        (aliases: puser, author, modder, d).
 *   NAME              — Display / fallback name.  Used in Steps 7, 8,
 *                        12, 16, 17.
 *   SYSTEM_LOGIN      — Azure UPN resolution via EMPtoAzureMap.
 *                        Used in Steps 7, 8, 12, 16, 17.
 *   USER_STATUS_C     — Safety margin.  NOT referenced in the SP;
 *                        appears only in Extract 47 source_query
 *                        (runs on Clarity server).  Negligible cost.
 *   EMP_RECORD_TYPE_C — Safety margin.  Same as USER_STATUS_C.
 *
 * stage_v2.ReportObjectUser has no EpicId column — its schema uses
 * UserID, UserName, UserDisplayName, UserEmail, UserPrincipalName,
 * Department, Title, ManagerName, IsActive, SourceSystem, ExtractDate.
 * No additional CLARITY_EMP columns are needed for the user staging path.
 *
 * Column types match the SSIS ExternalMetadata from ETL-Clarity.dtsx.
 *
 * Dependencies:
 *   - Schema: raw_v2 (created by 02_usp_Atlas_Setup.sql)
 *   - Replaces: 05_clarity_ddl.sql CLARITY_EMP definition
 *
 * Consumed by:
 *   - etl.usp_Atlas_Clarity (Steps 7, 8, 12, 16, 17)
 *   - atlas_clarity_extractor.py (Extract 47 destination)
 *
 * Run order: After 02_usp_Atlas_Setup.sql, before 12_usp_Atlas_Clarity.sql
 *
 * Author:  Larry Duren
 * Date:    March 2026
 * Version: 1.0
 ******************************************************************************/

USE Atlas_Staging;
GO

PRINT '=== Narrowed CLARITY_EMP Table DDL ===';
PRINT 'Start Time: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
GO

-- =============================================================================
-- raw_v2.CLARITY_EMP — Narrowed to 5 consumed columns
-- Source: dbo.CLARITY_EMP on EPICCLAPRD (Extract 47)
-- Previously 173 columns (~4 KB/row); now 5 columns (~700 bytes/row)
-- Used by: usp_Atlas_Clarity Steps 7, 8, 12, 16, 17
-- =============================================================================
IF OBJECT_ID('raw_v2.CLARITY_EMP', 'U') IS NOT NULL
BEGIN
    DECLARE @OldColCount INT;
    SELECT @OldColCount = COUNT(*)
    FROM   INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_SCHEMA = 'raw_v2'
      AND  TABLE_NAME   = 'CLARITY_EMP';

    PRINT 'Dropping existing raw_v2.CLARITY_EMP (' + CAST(@OldColCount AS VARCHAR(10)) + ' columns)';
    DROP TABLE raw_v2.CLARITY_EMP;
END;
GO

CREATE TABLE raw_v2.CLARITY_EMP (
    USER_ID             VARCHAR(18)     NULL,
    NAME                VARCHAR(160)    NULL,
    SYSTEM_LOGIN        VARCHAR(254)    NULL,
    USER_STATUS_C       INT             NULL,
    EMP_RECORD_TYPE_C   INT             NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.CLARITY_EMP (narrowed: 5 data columns + ETL_LoadDate)';
GO

CREATE NONCLUSTERED INDEX IX_CLARITY_EMP_USER_ID
    ON raw_v2.CLARITY_EMP (USER_ID);
GO
PRINT 'Created index IX_CLARITY_EMP_USER_ID';
GO

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '--- Column verification ---';
SELECT TABLE_SCHEMA,
       TABLE_NAME,
       COLUMN_NAME,
       DATA_TYPE,
       CHARACTER_MAXIMUM_LENGTH,
       IS_NULLABLE,
       COLUMN_DEFAULT
FROM   INFORMATION_SCHEMA.COLUMNS
WHERE  TABLE_SCHEMA = 'raw_v2'
  AND  TABLE_NAME   = 'CLARITY_EMP'
ORDER BY ORDINAL_POSITION;
GO

PRINT '';
PRINT '=== Narrowed CLARITY_EMP DDL Complete ===';
PRINT 'End Time: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
PRINT 'Expected columns: 6 (5 data + ETL_LoadDate)';
GO
