/*
================================================================================
Atlas ETL Suite - usp_Atlas_ValidateAzureADMapping (V2 Schema)
================================================================================
Purpose: DISABLED — formerly validated Azure AD user mapping by cross-referencing
         raw_v2.ClarityEmployees against stage_v2.ReportObjectUser.

         raw_v2.ClarityEmployees was removed from the codebase (2026-04-03).
         usp_Atlas_LDAP uses raw_v2.CLARITY_EMP, not ClarityEmployees.
         This SP is retained as a stub for future re-implementation against
         CLARITY_EMP if cross-system user validation is needed.

Dependencies:
  - stage_v2.ReportObjectUser (populated by usp_Atlas_LDAP Step 1)
  - etl.usp_Atlas_LogStart, etl.usp_Atlas_LogEnd, etl.usp_Atlas_LogError

Run this script on: Atlas_Staging

Version: 3.1.0
Last Updated: April 2026

Change Log:
  v3.1.0 2026-04-03 — Disabled: removed all raw_v2.ClarityEmployees references.
         Table confirmed dropped from Atlas_Staging. SP body replaced with stub.
================================================================================
*/

USE Atlas_Staging;
GO

IF OBJECT_ID('etl.usp_Atlas_ValidateAzureADMapping', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_ValidateAzureADMapping;
GO

CREATE PROCEDURE etl.usp_Atlas_ValidateAzureADMapping
    @ExecutionID        UNIQUEIDENTIFIER = NULL,
    @DetailLevel        VARCHAR(10) = 'SUMMARY',    -- 'SUMMARY' or 'FULL'
    @SampleSize         INT = 500,                   -- Max rows in detail output
    @RaiseErrorOnFail   BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- ══════════════════════════════════════════════════════════════════════════
    -- DISABLED — raw_v2.ClarityEmployees removed from codebase (2026-04-03).
    -- usp_Atlas_LDAP uses raw_v2.CLARITY_EMP, not ClarityEmployees.
    -- This SP is retained as a stub. Re-implement against CLARITY_EMP if
    -- cross-system user validation is needed in the future.
    -- ══════════════════════════════════════════════════════════════════════════

    DECLARE @PackageName NVARCHAR(100) = 'ETL-Clarity-Validate';
    DECLARE @LogID BIGINT;

    IF @ExecutionID IS NULL
        SET @ExecutionID = NEWID();

    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Disabled — ClarityEmployees table removed',
        @StepSequence = 1,
        @LogID = @LogID OUTPUT;

    PRINT '=== Azure AD User Mapping Validation ===';
    PRINT 'DISABLED: raw_v2.ClarityEmployees has been removed from the codebase.';
    PRINT 'usp_Atlas_LDAP now uses raw_v2.CLARITY_EMP directly.';
    PRINT 'Re-implement this SP against CLARITY_EMP if validation is needed.';

    EXEC etl.usp_Atlas_LogEnd
        @LogID = @LogID,
        @RowsAffected = 0,
        @Status = 'Success';

END
GO

PRINT 'Created/Updated procedure: etl.usp_Atlas_ValidateAzureADMapping';
GO
