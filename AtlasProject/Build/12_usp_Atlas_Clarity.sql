/*******************************************************************************
 * Atlas ETL Migration — usp_Atlas_Clarity (Pipeline B)
 * 
 * Replaces: ETL-Clarity SSIS Package (8 SQL Tasks, 2 Data Flow Tasks)
 * 
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  PIPELINE B AMENDMENT                                                   │
 * │  Phase 1 (8 linked server extractions) has been moved to Python:        │
 * │      atlas_clarity_extractor.py                                         │
 * │  This SP now handles Phase 2 (staging transforms) ONLY.                │
 * │  The orchestrator calls:                                                │
 * │      1. atlas_clarity_extractor.py  → raw_v2.*  (Python)               │
 * │      2. usp_Atlas_Clarity           → stage_v2.* (this SP)             │
 * │      3. atlas_csv_loader.py         → raw_v2.CSV_* (Python)            │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * This procedure transforms and consolidates raw_v2 data (populated by
 * atlas_clarity_extractor.py) into stage_v2 tables with denormalized
 * lookups (department names, security class names, usage counts).
 *
 * Dependencies:
 *   - raw_v2 tables populated by atlas_clarity_extractor.py
 *   - Schema: raw_v2, stage_v2 (created by 01_clarity_ddl.sql)
 *   - Logging: etl.usp_Atlas_LogStart, etl.usp_Atlas_LogEnd (05_create_etl_logging.sql)
 *   - NO LINKED SERVER REQUIRED (Pipeline B)
 *
 * Execution:
 *   EXEC etl.usp_Atlas_Clarity;
 *   EXEC etl.usp_Atlas_Clarity @Debug = 1;
 *
 * Author:  Larry Duren
 * Date:    March 2026
 * Version: 7.6 (Pipeline B — Phase 7: ALL staging transforms complete)
 *
 * v7.6 Changes:
 *   - Removed Stage 18 (Hierarchies) — moved to usp_Atlas_ClarityHierarchy
 *     which runs post-CSV-loader. CSV-dependent branches were executing
 *     against empty tables. (C2)
 ******************************************************************************/

USE Atlas_Staging;
GO

IF OBJECT_ID('etl.usp_Atlas_Clarity', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_Clarity;
GO

CREATE PROCEDURE etl.usp_Atlas_Clarity
    @ExecutionID    UNIQUEIDENTIFIER = NULL,
    @Debug          BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- =========================================================================
    -- Variables
    -- =========================================================================
    DECLARE @PackageName    NVARCHAR(100) = N'ETL-Clarity';
    DECLARE @StepName       NVARCHAR(200);
    DECLARE @LogID          BIGINT;              -- v3.2: Changed from INT to BIGINT (matches LogStart OUTPUT)
    DECLARE @StepSequence   INT = 0;             -- v3.2: Added step counter
    DECLARE @RowCount       INT;
    DECLARE @StartTime      DATETIME = GETDATE();
    DECLARE @ErrorMessage   NVARCHAR(4000);
    DECLARE @ErrorSeverity  INT;
    DECLARE @ErrorState     INT;

    IF @ExecutionID IS NULL
        SET @ExecutionID = NEWID();
    BEGIN TRY

        -- Log procedure start
        SET @StepSequence = @StepSequence + 1;
        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = N'Procedure Start (Pipeline B — Staging Only)',
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 1
        BEGIN
            PRINT '=== usp_Atlas_Clarity — Pipeline B (Staging Transforms Only) ===';
            PRINT 'NOTE: Raw extraction is handled by atlas_clarity_extractor.py';
            PRINT 'ExecutionID: ' + CAST(@ExecutionID AS VARCHAR(36));
            PRINT 'Start: ' + CONVERT(VARCHAR(30), @StartTime, 121);
            PRINT '';
        END;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = 0,
            @Status       = N'Success';

        -- =====================================================================
        -- Validate raw_v2 data availability
        -- =====================================================================
        
        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Validate raw_v2 data availability';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;
        
        DECLARE @RawEmpCount INT, @RawReportInfoCount INT;
        SELECT @RawEmpCount = COUNT(*) FROM raw_v2.CLARITY_EMP;
        SELECT @RawReportInfoCount = COUNT(*) FROM raw_v2.REPORT_INFO;

        IF @RawEmpCount = 0 AND @RawReportInfoCount = 0
        BEGIN
            SET @ErrorMessage = N'Core raw_v2 tables (CLARITY_EMP, REPORT_INFO) are empty. Run atlas_clarity_extractor.py first.';
            EXEC etl.usp_Atlas_LogEnd
                @LogID        = @LogID,
                @RowsAffected = 0,
                @Status       = N'Warning',
                @Message      = @ErrorMessage;
            RAISERROR(@ErrorMessage, 16, 1);
        END;

        IF @Debug = 1
        BEGIN
            PRINT 'raw_v2.CLARITY_EMP: ' + CAST(@RawEmpCount AS VARCHAR(20)) + ' rows';
            PRINT 'raw_v2.REPORT_INFO: ' + CAST(@RawReportInfoCount AS VARCHAR(20)) + ' rows';
            PRINT '';
        END;

        SET @RowCount = @RawEmpCount + @RawReportInfoCount;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        -- =====================================================================
        -- PHASE 2: STAGING TRANSFORMS
        -- =====================================================================

        -- =====================================================================
        -- STEP 4: Stage LPP — Reporting Workbench Extensions
        -- =====================================================================
        -- Source: ETL-Clarity SSIS "Clarity LPP" Execute SQL Task
        -- raw_v2 tables: CLARITY_LPP, LPP_COMMENTS
        -- Target: stage_v2.ReportObjectsStaging (INSERT — do NOT truncate)
        -- BizKey: clarity_server||clarityreport||Reporting Workbench Extension||||||LPP||{LPP_ID}
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage 4: LPP (Workbench Extensions) → ReportObjectsStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            INSERT INTO stage_v2.ReportObjectsStaging (
                BizKey,
                ObjectName,
                ObjectType,
                ObjectDescription,
                SourceSystem,
                SourceServer,
                IsHidden,
                ExtractDate,
                EpicMasterFile,
                EpicRecordID,
                [Availability],
                [EpicReportTemplateId]
            )
            SELECT
                -- BizKey: SourceServer||SourceDB||ReportObjectType||NULL||EpicMasterFile||EpicRecordID
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    N'Reporting Workbench Extension', N'||',
                    N'',               N'||',
                    N'LPP',            N'||',
                    lpp.LPP_ID
                )                                               AS BizKey,
                CONVERT(NVARCHAR(MAX), lpp.LPP_NAME)            AS ObjectName,
                N'Reporting Workbench Extension'                AS ObjectType,
                -- Description: concatenate LPP_COMMENTS lines via STRING_AGG / XML PATH
                LEFT(LTRIM(STUFF((
                    SELECT N'' + CHAR(10) + d.COMMENTS
                    FROM raw_v2.LPP_COMMENTS d
                    WHERE d.LPP_ID = lpp.LPP_ID
                    ORDER BY d.LPP_ID, d.LINE
                    FOR XML PATH('')
                ), 1, 1, '')), 3995)                            AS ObjectDescription,
                N'Clarity'                                      AS SourceSystem,
                N'clarity_server'                               AS SourceServer,
                CASE
                    WHEN ISNULL(lpp.RECORD_STATE_C, 0) <> 4     -- not hidden
                        THEN 0
                    ELSE 1
                END                                             AS IsHidden,
                GETDATE()                                       AS ExtractDate,
                N'LPP'                                          AS EpicMasterFile,
                lpp.LPP_ID                                      AS EpicRecordID,
                N'Public'                                       AS [Availability],
                NULL                                            AS [EpicReportTemplateId]
            FROM raw_v2.CLARITY_LPP lpp
            WHERE ISNULL(RECORD_STATE_C, 0) NOT IN (2, 6, 3, 1);

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';


        -- =====================================================================
        -- STEP 5: Stage IDN — Metric Definitions
        -- =====================================================================
        -- Source: ETL-Clarity SSIS "Clarity IDN (Metric Definitions)" Execute SQL Task
        -- raw_v2 tables: METRIC_INFO, METRIC_DESC
        -- Target: stage_v2.ReportObjectsStaging (INSERT — do NOT truncate)
        -- BizKey: clarity_server||clarityreport||Radar Metric||||||IDN||{DEFINITION_ID}
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage 5: IDN (Metric Definitions) → ReportObjectsStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            INSERT INTO stage_v2.ReportObjectsStaging (
                BizKey,
                ObjectName,
                ObjectType,
                ObjectDescription,
                SourceSystem,
                SourceServer,
                ModifiedDate,
                IsHidden,
                ExtractDate,
                EpicMasterFile,
                EpicRecordID,
                [Availability],
                [EpicReportTemplateId]
            )
            SELECT
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    N'Radar Metric',   N'||',
                    N'',               N'||',
                    N'IDN',            N'||',
                    idn.DEFINITION_ID
                )                                               AS BizKey,
                CONVERT(NVARCHAR(MAX), idn.METRIC_NAME)         AS ObjectName,
                N'Radar Metric'                                 AS ObjectType,
                LTRIM(STUFF((
                    SELECT N' ' + md.RECORD_DESC
                    FROM raw_v2.METRIC_DESC md
                    WHERE md.DEFINITION_ID = idn.DEFINITION_ID
                    ORDER BY md.DEFINITION_ID, md.LINE
                    FOR XML PATH('')
                ), 1, 1, ''))                                   AS ObjectDescription,
                N'Clarity'                                      AS SourceSystem,
                N'clarity_server'                               AS SourceServer,
                idn.INST_OF_UPDATE_DTTM                         AS ModifiedDate,
                -- DefaultVisibilityYN = 'N' in SSIS; IsHidden = 1
                CASE
                    WHEN idn.ACTIVE_YN = 'Y'
                        AND (idn.RECORD_STATUS_C IS NULL OR idn.RECORD_STATUS_C = 0)
                        THEN 0
                    ELSE 1
                END                                             AS IsHidden,
                GETDATE()                                       AS ExtractDate,
                N'IDN'                                          AS EpicMasterFile,
                idn.DEFINITION_ID                               AS EpicRecordID,
                N'Public'                                       AS [Availability],
                NULL                                            AS [EpicReportTemplateId]
            FROM raw_v2.METRIC_INFO idn;

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';


        -- =====================================================================
        -- STEP 6: Stage FDM — SlicerDicer Models
        -- =====================================================================
        -- Source: ETL-Clarity SSIS "Clarity FDM (SlicerDicer)" Execute SQL Task
        -- raw_v2 tables: DATA_MODEL_DEFINITIONS, DATA_MODEL_DESCRIPTION
        -- Target: stage_v2.ReportObjectsStaging (INSERT — do NOT truncate)
        -- BizKey: slicerdicer_server||slicerdicer||SlicerDicer Model||||||FDM||{DATA_MODEL_ID}
        -- Note: Self-join to check COMPILED RECORD inactive status
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage 6: FDM (SlicerDicer Models) → ReportObjectsStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            INSERT INTO stage_v2.ReportObjectsStaging (
                BizKey,
                ObjectName,
                ObjectType,
                ObjectDescription,
                SourceSystem,
                SourceServer,
                IsHidden,
                ExtractDate,
                EpicMasterFile,
                EpicRecordID,
                [Availability],
                [EpicReportTemplateId]
            )
            SELECT DISTINCT
                CONCAT(
                    N'slicerdicer_server', N'||',
                    N'slicerdicer',        N'||',
                    N'SlicerDicer Model',  N'||',
                    N'',                   N'||',
                    N'FDM',                N'||',
                    b.DATA_MODEL_ID
                )                                               AS BizKey,
                CONVERT(NVARCHAR(MAX), b.RECORD_NAME)           AS ObjectName,
                N'SlicerDicer Model'                            AS ObjectType,
                CONVERT(NVARCHAR(MAX), (
                    SELECT STUFF((
                        SELECT ' ' + t.DATA_MODEL_DESC
                        FROM raw_v2.DATA_MODEL_DESCRIPTION t
                        WHERE t.DATA_MODEL_ID = b.DATA_MODEL_ID
                        FOR XML PATH('')
                    ), 1, 1, '')
                ))                                              AS ObjectDescription,
                N'Clarity'                                      AS SourceSystem,
                N'slicerdicer_server'                           AS SourceServer,
                CASE
                    WHEN b.RECORD_NAME LIKE '%COMPILED RECORD%' THEN 1
                    WHEN b.RECORD_NAME LIKE '%OVERRIDE RECORD%' THEN 1
                    WHEN ISNULL(c.INACTIVE_YN, 'N') = 'N'       THEN 0
                    ELSE 1
                END                                             AS IsHidden,
                GETDATE()                                       AS ExtractDate,
                N'FDM'                                          AS EpicMasterFile,
                b.DATA_MODEL_ID                                 AS EpicRecordID,
                N'Public'                                       AS [Availability],
                NULL                                            AS [EpicReportTemplateId]
            FROM raw_v2.DATA_MODEL_DEFINITIONS b
            LEFT OUTER JOIN raw_v2.DATA_MODEL_DEFINITIONS c
                ON c.BASE_RECORD_ID = CAST(b.DATA_MODEL_ID AS NVARCHAR(MAX))
                AND c.RECORD_NAME LIKE '%[COMPILED RECORD]%'
            WHERE b.RECORD_NAME IS NOT NULL;

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';


        -- =====================================================================
        -- STEP 7: Stage FDS — SlicerDicer Filters
        -- =====================================================================
        -- Source: ETL-Clarity SSIS "Clarity FDS (SlicerDicer Filters)" Execute SQL Task
        -- raw_v2 tables: FILTER_DEFINITIONS, clarity_fds (CSV-sourced)
        -- Target: stage_v2.ReportObjectsStaging (INSERT — do NOT truncate)
        -- BizKey: clarity_server||clarityreport||SlicerDicer Filter||||||FDS||{FILTER_ID}
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage 7: FDS (SlicerDicer Filters) → ReportObjectsStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            INSERT INTO stage_v2.ReportObjectsStaging (
                BizKey,
                ObjectName,
                ObjectType,
                ObjectDescription,
                RawDefinition,
                SourceSystem,
                SourceServer,
                ModifiedDate,
                ParentPath,
                IsHidden,
                ExtractDate,
                EpicMasterFile,
                EpicRecordID,
                [Availability],
                [EpicReportTemplateId]
            )
            SELECT DISTINCT
                CONCAT(
                    N'clarity_server',     N'||',
                    N'clarityreport',      N'||',
                    N'SlicerDicer Filter', N'||',
                    N'',                   N'||',
                    N'FDS',                N'||',
                    fds.FILTER_ID
                )                                               AS BizKey,
                CONVERT(NVARCHAR(MAX), fds.FILTER_NAME)         AS ObjectName,
                N'SlicerDicer Filter'                           AS ObjectType,
                CONVERT(NVARCHAR(MAX), fdse.[Filter Description]) AS ObjectDescription,
                -- DetailedDescription → RawDefinition (closest stage_v2 column)
                CASE WHEN fdse.[Filter ID] IS NOT NULL THEN
                    CONCAT(
                        CASE WHEN fdse.[Filter Display Name] IS NOT NULL
                            THEN CONCAT(N'Display Name: ', fdse.[Filter Display Name], CHAR(10), CHAR(10))
                            ELSE NULL END,
                        CASE WHEN fdse.[Filter Data Type] IS NOT NULL
                            THEN CONCAT(N'Data Type: ', fdse.[Filter Data Type], CHAR(10), CHAR(10))
                            ELSE NULL END,
                        CASE WHEN fdse.[Filter Category] IS NOT NULL
                            THEN CONCAT(N'Filter Category: ', fdse.[Filter Category], CHAR(10), CHAR(10))
                            ELSE NULL END,
                        CASE WHEN fdse.[Filter Information Table Name] IS NOT NULL
                            THEN CONCAT(N'Filter Information Table Name: ', fdse.[Filter Information Table Name], CHAR(10))
                            ELSE NULL END,
                        CASE WHEN fdse.[Filter Information Data Expression] IS NOT NULL
                            THEN CONCAT(N'Filter Information Data Expression: ', fdse.[Filter Information Data Expression], CHAR(10), CHAR(10))
                            ELSE NULL END,
                        CASE WHEN fdse.[Is Filter Column Only?] IS NOT NULL
                            THEN CONCAT(N'Is Column Only?: ', fdse.[Is Filter Column Only?])
                            ELSE NULL END
                    )
                ELSE NULL END                                   AS RawDefinition,
                N'Clarity'                                      AS SourceSystem,
                N'clarity_server'                               AS SourceServer,
                COALESCE(fds.INSTANT_OF_UPDATE_DTTM, fds.RECORD_CREATION_DT) AS ModifiedDate,
                -- ParentMasterFile + ParentRecordID → ParentPath
                CASE
                    WHEN fds.BASE_RECORD_ID IS NOT NULL
                        THEN CONCAT(N'FDS||', fds.BASE_RECORD_ID)
                    ELSE NULL
                END                                             AS ParentPath,
                CASE
                    WHEN fds.FILTER_INACTIVE_YN = 'Y' THEN 1
                    ELSE 0
                END                                             AS IsHidden,
                GETDATE()                                       AS ExtractDate,
                N'FDS'                                          AS EpicMasterFile,
                fds.FILTER_ID                                   AS EpicRecordID,
                N'Public'                                       AS [Availability],
                NULL                                            AS [EpicReportTemplateId]
            FROM raw_v2.FILTER_DEFINITIONS fds
            LEFT OUTER JOIN raw_v2.clarity_fds fdse
                ON fds.FILTER_ID = TRY_CAST(fdse.[Filter ID] AS NUMERIC(18,0));

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';


        -- =====================================================================
        -- STEP 8: Stage IDB — Components
        -- =====================================================================
        -- Source: ETL-Clarity SSIS "Clarity IDB (Components)" Execute SQL Task
        -- raw_v2 tables: COMPONENT_INFO, COMPONENT_DESC, ZC_RECORD_TYPE_24, CLARITY_EMP
        -- Also: dbo.EMPtoAzureMap (local, populated by usp_Atlas_LDAP)
        -- Target: stage_v2.ReportObjectsStaging (INSERT — do NOT truncate)
        -- BizKey: clarity_server||clarityreport||{ReportObjectType}||||IDB||{COMPONENT_ID}
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage 8: IDB (Components) → ReportObjectsStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            INSERT INTO stage_v2.ReportObjectsStaging (
                BizKey,
                ObjectName,
                ObjectType,
                ObjectDescription,
                SourceSystem,
                SourceServer,
                CreatedBy,
                ModifiedDate,
                IsHidden,
                ExtractDate,
                EpicMasterFile,
                EpicRecordID,
                [Availability],
                [EpicReportTemplateId]
            )
            SELECT
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    CONCAT(COALESCE(idbt.NAME, N'Other'), N' Radar Dashboard Component'), N'||',
                    N'',               N'||',
                    N'IDB',            N'||',
                    idb.COMPONENT_ID
                )                                               AS BizKey,
                CONVERT(NVARCHAR(MAX), idb.COMPONENT_NAME)      AS ObjectName,
                CONCAT(COALESCE(idbt.NAME, N'Other'), N' Radar Dashboard Component')
                                                                AS ObjectType,
                CONVERT(NVARCHAR(MAX), idbd.RECORD_DESC)        AS ObjectDescription,
                N'Clarity'                                      AS SourceSystem,
                N'clarity_server'                               AS SourceServer,
                -- Author: prefer Azure UPN, then SYSTEM_LOGIN, then NAME
                CASE
                    WHEN eam.AZURE_upn IS NOT NULL THEN eam.AZURE_upn
                    WHEN puser.SYSTEM_LOGIN IS NULL THEN puser.NAME
                    ELSE puser.SYSTEM_LOGIN
                END                                             AS CreatedBy,
                idb.INSTANT_OF_UPD_DTTM                         AS ModifiedDate,
                CASE
                    WHEN (idb.RECORD_STATUS_C IS NULL OR idb.RECORD_STATUS_C = 0)
                        AND idb.RECORD_TYPE_C <> 3              -- Not Personal
                        AND idb.READY_FOR_USE_YN = 'Y'
                        THEN 0
                    ELSE 1
                END                                             AS IsHidden,
                GETDATE()                                       AS ExtractDate,
                N'IDB'                                          AS EpicMasterFile,
                idb.COMPONENT_ID                                AS EpicRecordID,
                N'Public'                                       AS [Availability],
                NULL                                            AS [EpicReportTemplateId]
            FROM raw_v2.COMPONENT_INFO idb
            -- COMPONENT_DESC has no LINE column (verified against Clarity source and SSIS).
            -- Aggregate to one row per COMPONENT_ID to prevent duplicate BizKeys.
            LEFT OUTER JOIN (
                SELECT COMPONENT_ID, MIN(RECORD_DESC) AS RECORD_DESC
                FROM raw_v2.COMPONENT_DESC
                GROUP BY COMPONENT_ID
            ) idbd
                ON idbd.COMPONENT_ID = idb.COMPONENT_ID
            LEFT OUTER JOIN raw_v2.ZC_RECORD_TYPE_24 idbt
                ON idbt.RECORD_TYPE_24_C = idb.RECORD_TYPE_C
            LEFT OUTER JOIN raw_v2.CLARITY_EMP puser
                ON puser.USER_ID = idb.USER_ID
            LEFT OUTER JOIN dbo.EMPtoAzureMap eam
                ON puser.SYSTEM_LOGIN = eam.Epic_AccountID
                AND eam.AZURE_upn NOT LIKE '%;%';

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';


        -- =====================================================================
        -- STEP 9: Stage IDM — Dashboards
        -- =====================================================================
        -- Source: ETL-Clarity SSIS "Clarity IDM (Dashboards)" Execute SQL Task
        -- raw_v2 tables: DASHBOARD_INFO, DASHBOARD_DESC, ZC_RECORD_TYPE_24, CLARITY_EMP
        -- Also: dbo.EMPtoAzureMap (local, populated by usp_Atlas_LDAP)
        -- Target: stage_v2.ReportObjectsStaging (INSERT — do NOT truncate)
        -- BizKey: clarity_server||clarityreport||{ReportObjectType}||||IDM||{DASHBOARD_ID}
        -- Note: Self-join to DASHBOARD_INFO for override records
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage 9: IDM (Dashboards) → ReportObjectsStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            INSERT INTO stage_v2.ReportObjectsStaging (
                BizKey,
                ObjectName,
                ObjectType,
                ObjectDescription,
                SourceSystem,
                SourceServer,
                CreatedBy,
                ModifiedDate,
                IsHidden,
                ExtractDate,
                EpicMasterFile,
                EpicRecordID,
                [Availability],
                [EpicReportTemplateId]
            )
            SELECT
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    CONCAT(COALESCE(idmt.NAME, N'Other'), N' Radar Dashboard'), N'||',
                    N'',               N'||',
                    N'IDM',            N'||',
                    idm.DASHBOARD_ID
                )                                               AS BizKey,
                CONVERT(NVARCHAR(MAX), idm.DASHBOARD_NAME)      AS ObjectName,
                CONCAT(COALESCE(idmt.NAME, N'Other'), N' Radar Dashboard')
                                                                AS ObjectType,
                -- Description: concatenate DASHBOARD_DESC lines via XML PATH
                LTRIM(STUFF((
                    SELECT N' ' + d.RECORD_DESC
                    FROM raw_v2.DASHBOARD_DESC d
                    WHERE d.DASHBOARD_ID = idm.DASHBOARD_ID
                    ORDER BY d.DASHBOARD_ID, d.LINE
                    FOR XML PATH('')
                ), 1, 1, ''))                                   AS ObjectDescription,
                N'Clarity'                                      AS SourceSystem,
                N'clarity_server'                               AS SourceServer,
                -- Author: prefer Azure UPN, then SYSTEM_LOGIN, then NAME
                CASE
                    WHEN eam.AZURE_upn IS NOT NULL THEN eam.AZURE_upn
                    WHEN puser.SYSTEM_LOGIN IS NULL THEN puser.NAME
                    ELSE puser.SYSTEM_LOGIN
                END                                             AS CreatedBy,
                idm.INSTANT_OF_UPD_DTTM                         AS ModifiedDate,
                CASE
                    WHEN idm.RECORD_TYPE_C <> 3                 -- Not Personal
                        AND COALESCE(idm_override.READY_FOR_USE_YN, idm.READY_FOR_USE_YN) = 'Y'
                        AND COALESCE(idm_override.ENABLED_YN, idm.ENABLED_YN) = 'Y'
                        AND idm.RECORD_STATUS_C IS NULL         -- Not hidden or deleted
                        THEN 0
                    ELSE 1
                END                                             AS IsHidden,
                GETDATE()                                       AS ExtractDate,
                N'IDM'                                          AS EpicMasterFile,
                idm.DASHBOARD_ID                                AS EpicRecordID,
                N'Public'                                       AS [Availability],
                NULL                                            AS [EpicReportTemplateId]
            FROM raw_v2.DASHBOARD_INFO idm
            LEFT OUTER JOIN raw_v2.ZC_RECORD_TYPE_24 idmt
                ON idmt.RECORD_TYPE_24_C = idm.RECORD_TYPE_C
            LEFT OUTER JOIN raw_v2.CLARITY_EMP puser
                ON puser.USER_ID = idm.USER_ID
            LEFT OUTER JOIN dbo.EMPtoAzureMap eam
                ON puser.SYSTEM_LOGIN = eam.Epic_AccountID
                AND eam.AZURE_upn NOT LIKE '%;%'
            -- Self-join for override records (OVRIDE_STATUS_C = 1)
            LEFT OUTER JOIN (
                SELECT
                    OVRIDE_PARENT_DB_ID,
                    MIN(READY_FOR_USE_YN)   AS READY_FOR_USE_YN,
                    MIN(ENABLED_YN)         AS ENABLED_YN
                FROM raw_v2.DASHBOARD_INFO
                WHERE OVRIDE_STATUS_C = 1
                GROUP BY OVRIDE_PARENT_DB_ID
            ) idm_override
                ON idm_override.OVRIDE_PARENT_DB_ID = idm.DASHBOARD_ID;

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';


        -- =====================================================================
        -- STEP 10: Stage IDK — Dashboard Resources
        -- =====================================================================
        -- Source: ETL-Clarity SSIS "Clarity IDK (Dashboard Resources)" Execute SQL Task
        -- raw_v2 tables: RESOURCE_DISPLAY, COMPONENT_SUMMARY_INFO, COMPONENT_INFO
        -- Target: stage_v2.ReportObjectsStaging (INSERT — do NOT truncate)
        -- BizKey: clarity_server||clarityreport||Radar Dashboard Resource||||IDK||{RESOURCE_ID}
        -- Note: DDL uses RESOURCE_ID (SSIS raw used RECORD_ID — renamed in raw_v2)
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage 10: IDK (Dashboard Resources) → ReportObjectsStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            INSERT INTO stage_v2.ReportObjectsStaging (
                BizKey,
                ObjectName,
                ObjectType,
                SourceSystem,
                SourceServer,
                ModifiedDate,
                IsHidden,
                ExtractDate,
                EpicMasterFile,
                EpicRecordID,
                [Availability],
                [EpicReportTemplateId]
            )
            SELECT DISTINCT
                CONCAT(
                    N'clarity_server',            N'||',
                    N'clarityreport',             N'||',
                    N'Radar Dashboard Resource',  N'||',
                    N'',                          N'||',
                    N'IDK',                       N'||',
                    idk.RESOURCE_ID
                )                                               AS BizKey,
                CONVERT(NVARCHAR(MAX), idk.RECORD_NAME)         AS ObjectName,
                N'Radar Dashboard Resource'                     AS ObjectType,
                N'Clarity'                                      AS SourceSystem,
                N'clarity_server'                               AS SourceServer,
                idk.INSTANT_OF_UPDATE_DTTM                      AS ModifiedDate,
                -- Visibility: resource is public only if it's linked to a valid component
                CASE
                    WHEN visible_resources.RESOURCE_ID IS NOT NULL THEN 0
                    ELSE 1
                END                                             AS IsHidden,
                GETDATE()                                       AS ExtractDate,
                N'IDK'                                          AS EpicMasterFile,
                idk.RESOURCE_ID                                 AS EpicRecordID,
                N'Public'                                       AS [Availability],
                NULL                                            AS [EpicReportTemplateId]
            FROM raw_v2.RESOURCE_DISPLAY idk
            LEFT OUTER JOIN (
                -- Resources that live on a valid, public component
                SELECT DISTINCT idk2.RESOURCE_ID
                FROM raw_v2.RESOURCE_DISPLAY idk2
                INNER JOIN raw_v2.COMPONENT_SUMMARY_INFO r
                    ON r.DATA_RESOURCES_ID = idk2.RESOURCE_ID
                INNER JOIN (
                    SELECT COMPONENT_ID
                    FROM raw_v2.COMPONENT_INFO
                    WHERE (RECORD_STATUS_C IS NULL OR RECORD_STATUS_C = 0)
                        AND RECORD_TYPE_C <> 3       -- Not Personal
                        AND READY_FOR_USE_YN = 'Y'
                ) valid_components
                    ON valid_components.COMPONENT_ID = r.COMPONENT_ID
            ) visible_resources
                ON visible_resources.RESOURCE_ID = idk.RESOURCE_ID;

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';


        -- =====================================================================
        -- STEP 11: Stage HGR — Templates
        -- =====================================================================
        -- Source: ETL-Clarity SSIS "Clarity HGR (Templates)" Execute SQL Task
        -- raw_v2 tables: TEMPLATE_INFO, ZC_REPORT_TYPE_HGR, TEMPLATE_DYNAMIC,
        --   TEMPLATE_DESCRIPTION, PROMPT_INFO, QUERY_DYNAMIC, CLARITY_RPT,
        --   CLARITY_RPT_QUEUES
        -- Target: stage_v2.ReportObjectsStaging (INSERT — do NOT truncate)
        -- BizKey: clarity_server||clarityreport||{ReportObjectType}||||HGR||{REPORT_ID}
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage 11: HGR (Templates) → ReportObjectsStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            INSERT INTO stage_v2.ReportObjectsStaging (
                BizKey,
                ObjectName,
                ObjectType,
                ObjectDescription,
                RawDefinition,
                SourceSystem,
                SourceServer,
                IsHidden,
                ExtractDate,
                EpicMasterFile,
                EpicRecordID,
                [Availability],
                [EpicReportTemplateId]
            )
            SELECT DISTINCT
                -- BizKey: SourceServer||SourceDB||ReportObjectType||||EpicMasterFile||EpicRecordID
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    CASE
                        WHEN rec_type.NAME = 'Settings'  THEN N'Application Report Template'
                        WHEN rec_type.NAME = 'Workbench'  THEN N'Reporting Workbench Template'
                        WHEN rec_type.NAME IS NULL         THEN N'Other HGR Template'
                        ELSE CONCAT(rec_type.NAME, N' Template')
                    END,               N'||',
                    N'',               N'||',
                    N'HGR',            N'||',
                    hgr.REPORT_ID
                )                                               AS BizKey,
                CONVERT(NVARCHAR(MAX), hgr.REPORT_NAME)         AS ObjectName,
                CASE
                    WHEN rec_type.NAME = 'Settings'  THEN N'Application Report Template'
                    WHEN rec_type.NAME = 'Workbench'  THEN N'Reporting Workbench Template'
                    WHEN rec_type.NAME IS NULL         THEN N'Other HGR Template'
                    ELSE CONCAT(rec_type.NAME, N' Template')
                END                                             AS ObjectType,
                -- Description from TEMPLATE_DESCRIPTION (or TEMPLATE_DYNAMIC.DESCRIPTION as fallback)
                COALESCE(
                    LTRIM(STUFF((
                        SELECT N' ' + td_desc.SEARCH_SOURCE_DESC
                        FROM raw_v2.TEMPLATE_DESCRIPTION td_desc
                        WHERE td_desc.REPORT_ID = hgr.REPORT_ID
                        ORDER BY td_desc.REPORT_ID, td_desc.LINE
                        FOR XML PATH('')
                    ), 1, 1, '')),
                    CONVERT(NVARCHAR(MAX), td.DESCRIPTION)
                )                                               AS ObjectDescription,
                -- DetailedDescription: Workbench-specific fields
                CASE WHEN rec_type.NAME = 'Workbench' THEN
                    CONCAT(
                        N'Max Records to Search: ',
                        FORMAT(ISNULL(td.MAX_NUM_SEARCH, 1000000), 'N0'),
                        CHAR(10),
                        N'Max Records to Return: ',
                        FORMAT(ISNULL(td.MAX_NUM_RETURN, 50000), 'N0'),
                        CHAR(10),
                        CASE
                            WHEN rq.report_queues IS NOT NULL
                                THEN CONCAT(N'Report Queues (HGR Level): ', CHAR(10), rq.report_queues)
                            ELSE CONCAT(N'Report Queue (System Default): ', CHAR(10), N'1. REPORT')
                        END,
                        CHAR(10), CHAR(10),
                        N'Context: ', CHAR(10), qd.CONTEXT, N' ',
                        CASE
                            WHEN qd.SELECT_TYPE_C = 1 THEN N'Records'
                            WHEN qd.SELECT_TYPE_C = 2 THEN N'Contacts'
                            WHEN qd.SELECT_TYPE_C = 3 THEN N'Defined by Search'
                            ELSE NULL
                        END
                    )
                ELSE NULL END                                   AS RawDefinition,
                N'Clarity'                                      AS SourceSystem,
                N'clarity_server'                               AS SourceServer,
                CASE
                    WHEN (hgr.STATUS_C = 0 OR hgr.STATUS_C IS NULL) THEN 0
                    ELSE 1
                END                                             AS IsHidden,
                GETDATE()                                       AS ExtractDate,
                N'HGR'                                          AS EpicMasterFile,
                hgr.REPORT_ID                                   AS EpicRecordID,
                N'Public'                                       AS [Availability],
                NULL                                            AS [EpicReportTemplateId]
            FROM raw_v2.TEMPLATE_INFO hgr
            LEFT OUTER JOIN raw_v2.ZC_REPORT_TYPE_HGR rec_type
                ON rec_type.REPORT_TYPE_HGR_C = hgr.REPORT_TYPE_HGR_C
            -- Latest contact from TEMPLATE_DYNAMIC (max CONTACT_NUM per REPORT_ID)
            LEFT OUTER JOIN (
                SELECT REPORT_ID, MAX(CONTACT_NUM) AS CONTACT_NUM
                FROM raw_v2.TEMPLATE_DYNAMIC
                GROUP BY REPORT_ID
            ) tdc ON tdc.REPORT_ID = hgr.REPORT_ID
            LEFT OUTER JOIN raw_v2.TEMPLATE_DYNAMIC td
                ON td.REPORT_ID = tdc.REPORT_ID
                AND td.CONTACT_NUM = tdc.CONTACT_NUM
            -- PROMPT_INFO for query context
            LEFT OUTER JOIN raw_v2.PROMPT_INFO hgp
                ON td.PARAM_PROMPT_ID = hgp.PARAMETER_PROMPT_ID
            -- Latest QUERY_DYNAMIC row (ROW_NUMBER by contact_date_real DESC)
            LEFT OUTER JOIN (
                SELECT TEMPLATE_ID, CONTEXT, SELECT_TYPE_C,
                    ROW_NUMBER() OVER (PARTITION BY TEMPLATE_ID ORDER BY CONTACT_DATE_REAL DESC) AS Rnk
                FROM raw_v2.QUERY_DYNAMIC
            ) qd ON hgp.QUERY_TEMPLATE_ID = qd.TEMPLATE_ID AND qd.Rnk = 1
            -- CLARITY_RPT for report queues
            LEFT OUTER JOIN raw_v2.CLARITY_RPT rpt
                ON hgr.REPORT_ID = rpt.ASSOC_REPORT_ID
                AND rpt.RECORD_STATUS_C IS NULL
            -- Report queues concatenation
            LEFT OUTER JOIN (
                SELECT DISTINCT q2.REPORT_ID,
                    STUFF((
                        SELECT CHAR(10) + CAST(q1.LINE AS NVARCHAR) + N'. ' + q1.Q_LIST_DESC
                        FROM raw_v2.CLARITY_RPT_QUEUES q1
                        WHERE q1.REPORT_ID = q2.REPORT_ID
                        ORDER BY q1.LINE
                        FOR XML PATH('')
                    ), 1, 1, '') AS report_queues
                FROM raw_v2.CLARITY_RPT_QUEUES q2
            ) rq ON rpt.REPORT_ID = rq.REPORT_ID;

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';


        -- =====================================================================
        -- STEP 12: Stage HRX — Reports (most complex transform)
        -- =====================================================================
        -- Source: ETL-Clarity SSIS "Stage HRXs" Execute SQL Task
        -- raw_v2 tables: REPORT_INFO, ZC_REPORT_TYPE_HGR, CLARITY_EMP,
        --   TEMPLATE_INFO, TEMPLATE_INFO_2, TEMPLATE_DYNAMIC, OVRIDE_RPT_GROUPS,
        --   REPORT_DESC, CLARITY_RPT_GROUPS, CLARITY_RPT, CLARITY_RPT_QUEUES,
        --   REPORT_QUEUES
        -- Also: dbo.EMPtoAzureMap (local, for author/modifier resolution)
        -- Target: stage_v2.ReportObjectsStaging (INSERT — do NOT truncate)
        -- BizKey: clarity_server||clarityreport||{ReportObjectType}||||HRX||{REPORT_INFO_ID}
        -- Uses temp tables: #HGRs (template context), #desc (description)
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage 12: HRX (Reports) → ReportObjectsStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            -- ── Temp table #HGRs: template context for active, non-IT/admin groups ──
            IF OBJECT_ID('tempdb..#HGRs') IS NOT NULL DROP TABLE #HGRs;

            SELECT DISTINCT
                ti.REPORT_ID        AS HGR_ID,
                ti.REPORT_NAME      AS HGR_NAME,
                rpt.REPORT_NAME     AS RPT_NAME,
                rpt.REPORT_ID       AS RPT_ID,
                ti.REPORT_TYPE_HGR_C,
                td.MAX_NUM_SEARCH,
                td.MAX_NUM_RETURN,
                rq.report_queues
            INTO #HGRs
            FROM raw_v2.CLARITY_RPT_GROUPS crg
            LEFT OUTER JOIN raw_v2.CLARITY_RPT rpt
                ON crg.REPORT_ID = rpt.REPORT_ID
            LEFT OUTER JOIN raw_v2.TEMPLATE_INFO ti
                ON rpt.ASSOC_REPORT_ID = ti.REPORT_ID
            LEFT OUTER JOIN raw_v2.TEMPLATE_DYNAMIC td
                ON ti.REPORT_ID = td.REPORT_ID
            LEFT OUTER JOIN (
                SELECT DISTINCT q2.REPORT_ID,
                    STUFF((
                        SELECT CHAR(10) + CAST(q1.LINE AS NVARCHAR) + N'. ' + q1.Q_LIST_DESC
                        FROM raw_v2.CLARITY_RPT_QUEUES q1
                        WHERE q1.REPORT_ID = q2.REPORT_ID
                        ORDER BY q1.LINE
                        FOR XML PATH('')
                    ), 1, 1, '') AS report_queues
                FROM raw_v2.CLARITY_RPT_QUEUES q2
            ) rq ON crg.REPORT_ID = rq.REPORT_ID
            WHERE crg.REPORT_GROUP_C != '200011'    -- IT Staff
                AND crg.REPORT_GROUP_C != '160182'  -- Reporting Administration
                AND (rpt.HIDE_FROM_LIBRARY_YN != 'Y' OR rpt.HIDE_FROM_LIBRARY_YN IS NULL)
                AND ti.REPORT_TYPE_HGR_C NOT IN (3, 7, 8) -- Epic-Crystal, E Webi, External Crystal
                AND (ti.STATUS_C IS NULL OR ti.STATUS_C = 0);

            -- ── Temp table #desc: pre-concatenated report descriptions ──
            IF OBJECT_ID('tempdb..#desc') IS NOT NULL DROP TABLE #desc;

            SELECT DISTINCT hrx.REPORT_INFO_ID,
                STUFF((
                    SELECT N' ' + d.REPORT_DESCRIPTION
                    FROM raw_v2.REPORT_DESC d
                    WHERE d.REPORT_INFO_ID = hrx.REPORT_INFO_ID
                    ORDER BY d.REPORT_INFO_ID, d.LINE
                    FOR XML PATH('')
                ), 1, 1, '') AS report_description
            INTO #desc
            FROM raw_v2.REPORT_DESC hrx;

            -- ── Main HRX INSERT ──
            INSERT INTO stage_v2.ReportObjectsStaging (
                BizKey,
                ObjectName,
                ObjectType,
                ObjectDescription,
                RawDefinition,
                SourceSystem,
                SourceServer,
                CreatedBy,
                ModifiedBy,
                ModifiedDate,
                ObjectURL,
                IsHidden,
                ExtractDate,
                EpicMasterFile,
                EpicRecordID,
                [Availability],
                [EpicReportTemplateId]
            )
            SELECT DISTINCT
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    CASE
                        WHEN rec_type.NAME = N'Settings'  THEN N'Application Report'
                        WHEN rec_type.NAME = N'Workbench'  THEN N'Reporting Workbench Report'
                        WHEN rec_type.NAME IS NULL          THEN N'Other HRX Report'
                        ELSE rec_type.NAME + N' Report'
                    END,               N'||',
                    N'',               N'||',
                    N'HRX',            N'||',
                    hrx.REPORT_INFO_ID
                )                                               AS BizKey,
                CONVERT(NVARCHAR(MAX), hrx.REPORT_INFO_NAME)    AS ObjectName,
                CASE
                    WHEN rec_type.NAME = N'Settings'  THEN N'Application Report'
                    WHEN rec_type.NAME = N'Workbench'  THEN N'Reporting Workbench Report'
                    WHEN rec_type.NAME IS NULL          THEN N'Other HRX Report'
                    ELSE rec_type.NAME + N' Report'
                END                                             AS ObjectType,
                -- Description from pre-concatenated #desc
                rd.report_description                           AS ObjectDescription,
                -- DetailedDescription: Workbench overrides for search/return limits + queues
                CONVERT(NVARCHAR(MAX), CONCAT(
                    CASE WHEN rec_type.NAME = N'Workbench' THEN CONCAT(
                        N'Max Records to Search: ',
                        FORMAT(COALESCE(hrx.OVRIDE_SEARCH_RECS, hgr.MAX_NUM_SEARCH, 1000000), 'N0'),
                        CHAR(10),
                        N'Max Records to Return: ',
                        FORMAT(COALESCE(hrx.OVRIDE_FIND_RECS, hgr.MAX_NUM_RETURN, 50000), 'N0')
                    ) ELSE NULL END,
                    CASE
                        WHEN rec_type.NAME = N'Workbench' AND rq.report_queues IS NOT NULL
                            THEN CONCAT(CHAR(10), N'Report Queues (HRX Overridden): ', CHAR(10), rq.report_queues)
                        WHEN rec_type.NAME = N'Workbench' AND rq.report_queues IS NULL AND hgr.report_queues IS NOT NULL
                            THEN CONCAT(CHAR(10), N'Report Queues (HGR Default): ', CHAR(10), hgr.report_queues)
                        WHEN rec_type.NAME = N'Workbench' AND rq.report_queues IS NULL AND hgr.report_queues IS NULL
                            THEN CONCAT(CHAR(10), N'Report Queue (System Default): ', CHAR(10), N'1. REPORT')
                        ELSE NULL
                    END
                ))                                              AS RawDefinition,
                N'Clarity'                                      AS SourceSystem,
                N'clarity_server'                               AS SourceServer,
                -- Author resolution: Azure UPN → SYSTEM_LOGIN → NAME
                COALESCE(autheam.AZURE_upn, author.SYSTEM_LOGIN, author.NAME) AS CreatedBy,
                -- Modifier resolution: same chain
                COALESCE(modeam.AZURE_upn, modder.SYSTEM_LOGIN, modder.NAME)  AS ModifiedBy,
                hrx.INST_OF_LAST_MOD_DTTM                      AS ModifiedDate,
                -- ReportObjectURL: crystal_filename from TEMPLATE_INFO_2
                CONCAT(N'', hrg_2.CRYSTAL_FILENAME)             AS ObjectURL,
                -- Visibility: not private, has override group or parent template
                CASE
                    WHEN (hrx.PRIVATE_OR_PUBLIC_C <> 1 OR hrx.PRIVATE_OR_PUBLIC_C IS NULL)
                        AND (org.REPORT_GROUP_C IS NOT NULL OR hgr.HGR_ID IS NOT NULL)
                        THEN 0
                    ELSE 1
                END                                             AS IsHidden,
                GETDATE()                                       AS ExtractDate,
                N'HRX'                                          AS EpicMasterFile,
                hrx.REPORT_INFO_ID                              AS EpicRecordID,
                CASE
                    WHEN (hrx.PRIVATE_OR_PUBLIC_C <> 1
                          OR hrx.PRIVATE_OR_PUBLIC_C IS NULL)
                         AND rec_type.NAME != N'SlicerDicer Session'
                    THEN N'Public'
                    ELSE N'Private'
                END                                             AS [Availability],
                hrx.REPORT_ID                                   AS [EpicReportTemplateId]
            FROM raw_v2.REPORT_INFO hrx
            LEFT OUTER JOIN raw_v2.ZC_REPORT_TYPE_HGR rec_type
                ON hrx.RECORD_TYPE_C = rec_type.REPORT_TYPE_HGR_C
            -- Author
            LEFT OUTER JOIN raw_v2.CLARITY_EMP author
                ON author.USER_ID = hrx.CREATED_BY_USER_ID
            LEFT OUTER JOIN dbo.EMPtoAzureMap autheam
                ON author.SYSTEM_LOGIN = autheam.Epic_AccountID
                AND autheam.AZURE_upn NOT LIKE '%;%'
            -- Modifier
            LEFT OUTER JOIN raw_v2.CLARITY_EMP modder
                ON modder.USER_ID = hrx.LAST_MOD_BY_USER_ID
            LEFT OUTER JOIN dbo.EMPtoAzureMap modeam
                ON modder.SYSTEM_LOGIN = modeam.Epic_AccountID
                AND modeam.AZURE_upn NOT LIKE '%;%'
            -- Parent template
            LEFT OUTER JOIN raw_v2.TEMPLATE_INFO hrg
                ON hrg.REPORT_ID = hrx.REPORT_ID
            LEFT OUTER JOIN raw_v2.TEMPLATE_INFO_2 hrg_2
                ON hrg_2.REPORT_ID = hrg.REPORT_ID
            -- Override report groups (excluding IT Staff and Reporting Administration)
            LEFT OUTER JOIN raw_v2.OVRIDE_RPT_GROUPS org
                ON hrx.REPORT_INFO_ID = org.REPORT_ID
                AND org.REPORT_GROUP_C != '200011'   -- IT Staff
                AND org.REPORT_GROUP_C != '160182'   -- Reporting Administration
            -- Template context from #HGRs temp table
            LEFT OUTER JOIN #HGRs hgr
                ON hrx.REPORT_ID = hgr.HGR_ID
            -- HRX-level report queues (from REPORT_QUEUES, not CLARITY_RPT_QUEUES)
            LEFT OUTER JOIN (
                SELECT DISTINCT q2.REPORT_INFO_ID,
                    STUFF((
                        SELECT CHAR(10) + CAST(q1.LINE AS NVARCHAR) + N'. ' + q1.Q_LIST_DESC
                        FROM raw_v2.REPORT_QUEUES q1
                        WHERE q1.REPORT_INFO_ID = q2.REPORT_INFO_ID
                        ORDER BY q1.LINE
                        FOR XML PATH('')
                    ), 1, 1, '') AS report_queues
                FROM raw_v2.REPORT_QUEUES q2
            ) rq ON hrx.REPORT_INFO_ID = rq.REPORT_INFO_ID
            -- Pre-concatenated description from #desc temp table
            LEFT OUTER JOIN #desc rd
                ON hrx.REPORT_INFO_ID = rd.REPORT_INFO_ID
            WHERE hrx.TEMP_REPORT_C = 0;  -- Active only

            SET @RowCount = @@ROWCOUNT;

            -- Clean up temp tables
            IF OBJECT_ID('tempdb..#HGRs') IS NOT NULL DROP TABLE #HGRs;
            IF OBJECT_ID('tempdb..#desc') IS NOT NULL DROP TABLE #desc;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';


        -- =====================================================================
        -- STEP 13: Stage E3N (Code Templates) + PAF (Workbench Columns)
        -- =====================================================================
        -- Source: ETL-Clarity SSIS "Code Template" and "Clarity PAF" Execute SQL Tasks
        -- raw_v2 tables: code_template, E3N_Export, clarity_paf, CLARITY_LPP
        -- Target: stage_v2.ReportObjectsStaging (INSERT — do NOT truncate)
        -- These are CSV-sourced tables used as hierarchy endpoints.
        -- BizKey: clarity_server||clarityreport||Code Template||||E3N||{ID}
        --         clarity_server||clarityreport||Reporting Workbench Column||||PAF||{Column ID}
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage 13: E3N/PAF (Code Templates + Workbench Columns) → ReportObjectsStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            -- ── E3N: Code Templates ──
            INSERT INTO stage_v2.ReportObjectsStaging (
                BizKey,
                ObjectName,
                ObjectType,
                ObjectDescription,
                RawDefinition,
                SourceSystem,
                SourceServer,
                IsHidden,
                ExtractDate,
                EpicMasterFile,
                EpicRecordID,
                [Availability],
                [EpicReportTemplateId]
            )
            SELECT
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    N'Code Template',  N'||',
                    N'',               N'||',
                    N'E3N',            N'||',
                    REPLACE(ct.[Code Template ID], NCHAR(10), '')
                )                                               AS BizKey,
                CONVERT(NVARCHAR(MAX), ct.[Code Template Name]) AS ObjectName,
                N'Code Template'                                AS ObjectType,
                -- Description: prefer E3N_Export description, fallback to CSV description
                CONVERT(NVARCHAR(MAX), CONCAT(
                    CASE
                        WHEN e3n.[DEFINITION ID] IS NOT NULL THEN e3n.[RECORD DESCRIPTION]
                        ELSE ct.[Code Template Description]
                    END,
                    CHAR(10), CHAR(10),
                    N'Template Type: ', ct.[Code Template Template Type],
                    CHAR(10), CHAR(10),
                    N'INI: ', ct.[Code Template INI],
                    CHAR(10), CHAR(10),
                    CASE
                        WHEN ct.[Code Template Programming Point Definition Item] != ''
                            THEN N'Item: ' + ct.[Code Template Programming Point Definition Item]
                        ELSE N''
                    END
                ))                                              AS ObjectDescription,
                -- DetailedDescription: parameter IDs
                CASE
                    WHEN ct.[Code Template Parameter ID] IS NOT NULL
                        AND ct.[Code Template Parameter ID] != ''
                    THEN CONCAT(
                        N'Parameters: ', CHAR(10),
                        CASE
                            WHEN ct.[Code Template ID] IN (
                                SELECT DISTINCT [Code Template ID]
                                FROM raw_v2.code_template
                                WHERE LEN([Code Template Parameter ID]) = 1500
                            ) AND e3n.[PARAMETER ID RECORD NAME] IS NOT NULL
                                THEN e3n.[PARAMETER ID RECORD NAME]
                            ELSE ct.[Code Template Parameter ID]
                        END
                    )
                    ELSE NULL
                END                                             AS RawDefinition,
                N'Clarity'                                      AS SourceSystem,
                N'clarity_server'                               AS SourceServer,
                0                                               AS IsHidden,
                GETDATE()                                       AS ExtractDate,
                N'E3N'                                          AS EpicMasterFile,
                TRY_CAST(REPLACE(ct.[Code Template ID], NCHAR(10), N'') AS NUMERIC(18,0)) AS EpicRecordID,
                N'Public'                                       AS [Availability],
                NULL                                            AS [EpicReportTemplateId]
            FROM raw_v2.code_template ct
            LEFT OUTER JOIN raw_v2.E3N_Export e3n
                ON ct.[Code Template ID] = e3n.[DEFINITION ID]
            WHERE TRY_CAST(REPLACE(ct.[Code Template ID], NCHAR(10), '') AS NUMERIC) IS NOT NULL;

            DECLARE @E3NCount INT = @@ROWCOUNT;

            -- ── PAF: Workbench Columns ──
            INSERT INTO stage_v2.ReportObjectsStaging (
                BizKey,
                ObjectName,
                ObjectType,
                ObjectDescription,
                RawDefinition,
                SourceSystem,
                SourceServer,
                IsHidden,
                ExtractDate,
                EpicMasterFile,
                EpicRecordID,
                [Availability],
                [EpicReportTemplateId]
            )
            SELECT
                CONCAT(
                    N'clarity_server',              N'||',
                    N'clarityreport',               N'||',
                    N'Reporting Workbench Column',  N'||',
                    N'',                            N'||',
                    N'PAF',                         N'||',
                    REPLACE(paf.[Column ID], NCHAR(10), '')
                )                                               AS BizKey,
                CONVERT(NVARCHAR(MAX), paf.Name)                AS ObjectName,
                N'Reporting Workbench Column'                   AS ObjectType,
                CASE
                    WHEN paf.Description != '' THEN paf.Description
                    ELSE NULL
                END                                             AS ObjectDescription,
                -- DetailedDescription: associated apps, INI, item, extension info
                CONVERT(NVARCHAR(MAX), CONCAT(
                    CASE WHEN paf.AssocApps != ''
                        THEN CONCAT(N'Associated Applications: ', paf.AssocApps, CHAR(10), CHAR(10))
                        ELSE NULL END,
                    CASE WHEN paf.ColumnINI != ''
                        THEN CONCAT(N'Column INI: ', paf.ColumnINI)
                        ELSE NULL END,
                    CHAR(10), CHAR(10),
                    CASE WHEN paf.ColumnItem != ''
                        THEN CONCAT(N'Column Item: ', paf.ColumnItem)
                        ELSE N'' END,
                    CHAR(10), CHAR(10),
                    CASE WHEN paf.Extension != ''
                        THEN CONCAT(N'Extension: ', lpp.LPP_NAME, N' [', paf.Extension, N']', CHAR(10))
                        ELSE NULL END,
                    CASE WHEN paf.ExtensionParameter != ''
                        THEN CONCAT(N'Extension Parameter: ', paf.ExtensionParameter)
                        ELSE NULL END
                ))                                              AS RawDefinition,
                N'Clarity'                                      AS SourceSystem,
                N'clarity_server'                               AS SourceServer,
                0                                               AS IsHidden,
                GETDATE()                                       AS ExtractDate,
                N'PAF'                                          AS EpicMasterFile,
                TRY_CAST(REPLACE(paf.[Column ID], NCHAR(10), N'') AS NUMERIC(18,0)) AS EpicRecordID,
                N'Public'                                       AS [Availability],
                NULL                                            AS [EpicReportTemplateId]
            FROM raw_v2.clarity_paf paf
            LEFT OUTER JOIN raw_v2.CLARITY_LPP lpp
                ON paf.Extension = CAST(lpp.LPP_ID AS VARCHAR)
            WHERE TRY_CAST(REPLACE(paf.[Column ID], NCHAR(10), '') AS NUMERIC) IS NOT NULL;

            SET @RowCount = @E3NCount + @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';


        -- =====================================================================
        -- STEP 14: Stage Tags → ReportObjectTagsStaging
        -- =====================================================================
        -- Source: ETL-Clarity SSIS "Clarity Tag Info" Execute SQL Task
        -- raw_v2 tables: TAG_INFO
        -- Target: stage_v2.ReportObjectTagsStaging (TRUNCATE + INSERT — Clarity-owned)
        -- Note: raw_v2.TAG_INFO does not include RECORD_STATUS_C (not extracted).
        --   SSIS filters WHERE ISNULL(RECORD_STATUS_C,1)=1 but we insert all tags.
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage 14: Tags → ReportObjectTagsStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectTagsStaging;

            INSERT INTO stage_v2.ReportObjectTagsStaging (
                TagID,
                TagName,
                ExtractDate
            )
            SELECT DISTINCT
                ti.TAG_ID       AS TagID,
                ti.TAG_NAME     AS TagName,
                GETDATE()       AS ExtractDate
            FROM raw_v2.TAG_INFO ti
            WHERE ti.TAG_NAME IS NOT NULL;

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';


        -- =====================================================================
        -- STEP 15: Stage TagMemberships → ReportObjectTagMembershipsStaging
        -- =====================================================================
        -- Source: ETL-Clarity SSIS tag membership queries (4 UNION ALL branches)
        --   Clarity HRX ReportTagMembership, Clarity HGR ReportTagMembership,
        --   Clarity IDB ReportTagMembership, Clarity IDM ReportTagMembership
        -- raw_v2 tables: REPORT_TAGS, TEMPLATE_TAGS, COMPONENT_TAGS, DASHBOARD_TAGS,
        --   TAG_INFO, REPORT_INFO, TEMPLATE_INFO, COMPONENT_INFO, DASHBOARD_INFO,
        --   ZC_REPORT_TYPE_HGR, ZC_RECORD_TYPE_24
        -- Target: stage_v2.ReportObjectTagMembershipsStaging (TRUNCATE + INSERT)
        -- BizKey must match the parent object's BizKey in ReportObjectsStaging
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage 15: TagMemberships → ReportObjectTagMembershipsStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectTagMembershipsStaging;

            -- Branch A: HRX (Report) tag memberships
            INSERT INTO stage_v2.ReportObjectTagMembershipsStaging (
                BizKey,
                TagName,
                ExtractDate
            )
            SELECT DISTINCT
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    CASE
                        WHEN rec_type.NAME = N'Settings'  THEN N'Application Report'
                        WHEN rec_type.NAME = N'Workbench'  THEN N'Reporting Workbench Report'
                        WHEN rec_type.NAME IS NULL          THEN N'Other HRX Report'
                        ELSE rec_type.NAME + N' Report'
                    END,               N'||',
                    N'',               N'||',
                    N'HRX',            N'||',
                    hrx.REPORT_INFO_ID
                )                       AS BizKey,
                ti.tag_name             AS TagName,
                GETDATE()               AS ExtractDate
            FROM raw_v2.REPORT_TAGS tags
            INNER JOIN raw_v2.REPORT_INFO hrx
                ON hrx.REPORT_INFO_ID = tags.report_id
            INNER JOIN raw_v2.ZC_REPORT_TYPE_HGR rec_type
                ON hrx.RECORD_TYPE_C = rec_type.REPORT_TYPE_HGR_C
            INNER JOIN raw_v2.TAG_INFO ti
                ON ti.tag_id = tags.tag_id;

            -- Branch B: HGR (Template) tag memberships
            INSERT INTO stage_v2.ReportObjectTagMembershipsStaging (
                BizKey,
                TagName,
                ExtractDate
            )
            SELECT DISTINCT
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    CASE
                        WHEN rec_type.NAME = 'Settings'  THEN N'Application Report Template'
                        WHEN rec_type.NAME = 'Workbench'  THEN N'Reporting Workbench Template'
                        WHEN rec_type.NAME IS NULL         THEN N'Other HGR Template'
                        ELSE CONCAT(rec_type.NAME, N' Template')
                    END,               N'||',
                    N'',               N'||',
                    N'HGR',            N'||',
                    hgr.REPORT_ID
                )                       AS BizKey,
                ti.tag_name             AS TagName,
                GETDATE()               AS ExtractDate
            FROM raw_v2.TEMPLATE_TAGS tags
            INNER JOIN raw_v2.TEMPLATE_INFO hgr
                ON hgr.REPORT_ID = tags.report_id
            LEFT OUTER JOIN raw_v2.ZC_REPORT_TYPE_HGR rec_type
                ON rec_type.REPORT_TYPE_HGR_C = hgr.REPORT_TYPE_HGR_C
            INNER JOIN raw_v2.TAG_INFO ti
                ON ti.tag_id = tags.tag_id;

            -- Branch C: IDB (Component) tag memberships
            INSERT INTO stage_v2.ReportObjectTagMembershipsStaging (
                BizKey,
                TagName,
                ExtractDate
            )
            SELECT DISTINCT
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    CONCAT(COALESCE(idbt.NAME, N'Other'), N' Radar Dashboard Component'), N'||',
                    N'',               N'||',
                    N'IDB',            N'||',
                    idb.COMPONENT_ID
                )                       AS BizKey,
                ti.tag_name             AS TagName,
                GETDATE()               AS ExtractDate
            FROM raw_v2.COMPONENT_TAGS tags
            INNER JOIN raw_v2.COMPONENT_INFO idb
                ON idb.COMPONENT_ID = tags.report_id
            LEFT OUTER JOIN raw_v2.ZC_RECORD_TYPE_24 idbt
                ON idbt.RECORD_TYPE_24_C = idb.RECORD_TYPE_C
            INNER JOIN raw_v2.TAG_INFO ti
                ON ti.tag_id = tags.tag_id;

            -- Branch D: IDM (Dashboard) tag memberships
            INSERT INTO stage_v2.ReportObjectTagMembershipsStaging (
                BizKey,
                TagName,
                ExtractDate
            )
            SELECT DISTINCT
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    COALESCE(idmt.NAME, N'Other') + N' Radar Dashboard', N'||',
                    N'',               N'||',
                    N'IDM',            N'||',
                    idm.DASHBOARD_ID
                )                       AS BizKey,
                ti.tag_name             AS TagName,
                GETDATE()               AS ExtractDate
            FROM raw_v2.DASHBOARD_TAGS tags
            INNER JOIN raw_v2.DASHBOARD_INFO idm
                ON idm.DASHBOARD_ID = tags.report_id
            LEFT OUTER JOIN raw_v2.ZC_RECORD_TYPE_24 idmt
                ON idmt.RECORD_TYPE_24_C = idm.RECORD_TYPE_C
            INNER JOIN raw_v2.TAG_INFO ti
                ON ti.tag_id = tags.tag_id;

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';


        -- =====================================================================
        -- STEP 16: Stage Queries → ReportObjectQueryStaging
        -- =====================================================================
        -- 5 query sources (matching SSIS ETL-Clarity "Files and Queries" task):
        --   1. Cogito SQL Queries (QUERY_DYNAMIC → DRILL_TEXT_SQLSERVER CTE)
        --   2. Radar Metric Queries (clarity_metric_query + METRIC_INFO)
        --   3. Cogito Drill SQL / HGR Template Drill (DRILL_TEXT_SQLSERVER agg)
        --   4. E3N Code Template M-Code (code_template CSV)
        --   5. LPP M_CODE / Reporting Workbench Extensions (CLARITY_LPP)
        --
        -- raw_v2 tables: DRILL_TEXT_SQLSERVER, QUERY_DYNAMIC, PROMPT_INFO,
        --   TEMPLATE_DYNAMIC, METRIC_INFO, clarity_metric_query, code_template,
        --   CLARITY_LPP
        -- Target: stage_v2.ReportObjectQueryStaging (TRUNCATE + 5 INSERTs)
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage 16: Queries → ReportObjectQueryStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectQueryStaging;

            -- Reset accumulator: Source 1 used to own the first SET @RowCount
            -- in this step; after its removal, Source 2 is the first INSERT
            -- and needs a clean starting point for the running total.
            SET @RowCount = 0;

            -- A prior "Source 1" added a second Cogito SQL branch that
            -- aggregated DRILL_TEXT_SQLSERVER with ORDER BY sql1.LINE inside
            -- the STUFF/FOR XML concatenation. SSIS's Cogito SQL Queries
            -- component does NOT use ORDER BY — it concatenates in heap/
            -- physical order. Source 3 below is the faithful SSIS translation
            -- and is the only Cogito SQL branch we need. On the current BILH
            -- snapshot the two variants produced the same BizKeys and were
            -- collapsed by Merge 0, but keeping both was wrong by SSIS
            -- fidelity standards and diverged from the SSIS reference.

            -- Source 2: Radar Metric Queries (from clarity_metric_query + METRIC_INFO)
            -- SSIS: "Clarity Base Queries" component
            -- BizKey: clarity_server||clarityreport||Radar Metric||||IDN||{DEFINITION_ID}
            INSERT INTO stage_v2.ReportObjectQueryStaging (
                BizKey,
                QueryText,
                QueryType,
                ExtractDate
            )
            SELECT
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    N'Radar Metric',   N'||',
                    N'',               N'||',
                    N'IDN',            N'||',
                    idn.DEFINITION_ID
                )                                               AS BizKey,
                q.[query]                                       AS QueryText,
                N'SQL'                                          AS QueryType,
                GETDATE()                                       AS ExtractDate
            FROM raw_v2.METRIC_INFO idn
            INNER JOIN raw_v2.clarity_metric_query q
                ON q.[idn id] = CAST(idn.DEFINITION_ID AS NVARCHAR(MAX));

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- Source 3: Cogito SQL Queries — HGR Template Drill SQL
            -- SSIS counterpart: Package\Files and Queries\Cogito SQL Queries
            --   (byte-equivalent to AtlasProject/Reference/ETL-Clarity/SQL/
            --    Cogito SQL Queries.sql).
            -- This is the ONLY Cogito SQL branch — the prior "Source 1" that
            -- added ORDER BY sql1.LINE inside the FOR XML aggregation was
            -- removed because SSIS does not use ORDER BY here and we need
            -- to preserve heap-order
            -- concatenation to match SSIS QueryText output exactly.
            -- BizKey: clarity_server||clarityreport||Reporting Workbench Template||||HGR||{REPORT_ID}
            ;WITH DrillText AS (
                SELECT
                    sql2.JOB_CONFIGURATION_ID,
                    STUFF((
                        SELECT CHAR(10) + sql1.DRILL_TEXT_SQLSERVER
                        FROM raw_v2.DRILL_TEXT_SQLSERVER sql1
                        WHERE sql1.JOB_CONFIGURATION_ID = sql2.JOB_CONFIGURATION_ID
                        FOR XML PATH(''), TYPE
                    ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS Query
                FROM raw_v2.DRILL_TEXT_SQLSERVER sql2
                GROUP BY sql2.JOB_CONFIGURATION_ID
            )
            INSERT INTO stage_v2.ReportObjectQueryStaging (
                BizKey,
                QueryText,
                QueryType,
                ExtractDate
            )
            SELECT
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    N'Reporting Workbench Template', N'||',
                    N'',               N'||',
                    N'HGR',            N'||',
                    template.REPORT_ID
                )                                               AS BizKey,
                dt.Query                                        AS QueryText,
                N'SQL'                                          AS QueryType,
                GETDATE()                                       AS ExtractDate
            FROM raw_v2.QUERY_DYNAMIC d
            INNER JOIN DrillText dt
                ON dt.JOB_CONFIGURATION_ID = d.JOB_CONFIG_ID
            INNER JOIN raw_v2.PROMPT_INFO p
                ON p.QUERY_TEMPLATE_ID = d.TEMPLATE_ID
            INNER JOIN raw_v2.TEMPLATE_DYNAMIC template
                ON template.PARAM_PROMPT_ID = p.PARAMETER_PROMPT_ID;

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- Source 4: E3N Code Template M-Code
            -- SSIS: "E3N M Code" component
            -- BizKey: clarity_server||clarityreport||Code Template||||E3N||{Code Template ID}
            INSERT INTO stage_v2.ReportObjectQueryStaging (
                BizKey,
                QueryText,
                QueryType,
                ExtractDate
            )
            SELECT
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    N'Code Template',  N'||',
                    N'',               N'||',
                    N'E3N',            N'||',
                    ct.[Code Template ID]
                )                                               AS BizKey,
                CONVERT(NVARCHAR(MAX), CONCAT('/* M-Code */', CHAR(10), ct.[Code Template M Code]))
                                                                AS QueryText,
                N'M-Code'                                       AS QueryType,
                GETDATE()                                       AS ExtractDate
            FROM raw_v2.code_template ct;

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- Source 5: LPP M_CODE / Reporting Workbench Extensions
            -- SSIS: "LPP M_CODE" component
            -- BizKey: clarity_server||clarityreport||Reporting Workbench Extension||||LPP||{LPP_ID}
            INSERT INTO stage_v2.ReportObjectQueryStaging (
                BizKey,
                QueryText,
                QueryType,
                ExtractDate
            )
            SELECT
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    N'Reporting Workbench Extension', N'||',
                    N'',               N'||',
                    N'LPP',            N'||',
                    lpp.LPP_ID
                )                                               AS BizKey,
                CONVERT(NVARCHAR(MAX), CONCAT('/* M-Code */', CHAR(10), lpp.M_CODE))
                                                                AS QueryText,
                N'M-Code'                                       AS QueryType,
                GETDATE()                                       AS ExtractDate
            FROM raw_v2.CLARITY_LPP lpp;

            SET @RowCount = @RowCount + @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';


        -- =====================================================================
        -- STEP 17: Stage Parameters → ReportObjectParametersStaging
        -- =====================================================================
        -- Source: ETL-Clarity SSIS "HRX Parameters", "HGR Parameters",
        --   "Cogito SQL Parameters" Execute SQL Tasks
        -- raw_v2 tables: SEARCH_EXPRESSION, PROMPT_PARAMETERS, REPORT_INFO,
        --   ZC_REPORT_TYPE_HGR, clarity_intraparameter_logic (CSV),
        --   TEMPLATE_INFO, TEMPLATE_DYNAMIC, QUERY_DYNAMIC, PROMPT_INFO,
        --   DRILL_TEXT_SQLSERVER
        -- Target: stage_v2.ReportObjectParametersStaging (TRUNCATE + INSERT)
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage 17: Parameters → ReportObjectParametersStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectParametersStaging;

            -- Source 1: HRX Parameters (report-level criteria/search expressions)
            -- Faithful translation of SSIS File 108 (Atlas_Combined_SQL.sql lines 2286-2313).
            -- BizKey is intentionally omitted from this INSERT — Merge 9 now joins on
            -- (EpicRecordID, EpicMasterFile) per commit a80cd57, so the BizKey column on
            -- stage_v2.ReportObjectParametersStaging is left NULL (column is nullable).
            -- Including BizKey in the SELECT DISTINCT projection was distorting dedup
            -- and suppressing 45,504 rows vs the SSIS source query.
            -- [Crit Uniq] cast widened from INT to NUMERIC(18,0) to match SSIS implicit
            -- cast behavior — SSIS had no INT-overflow truncation on the IPL LEFT JOIN.
            INSERT INTO stage_v2.ReportObjectParametersStaging (
                ParameterName,
                DefaultValue,
                EpicRecordID,
                EpicMasterFile,
                Operator,
                IntraParameterLogic,
                ExtractDate
            )
            SELECT DISTINCT
                CAST(p.CAPTION       AS NVARCHAR(200))           AS ParameterName,
                CAST(s.EXPRSN_VALUE  AS NVARCHAR(MAX))           AS DefaultValue,
                s.REPORT_INFO_ID                                 AS EpicRecordID,
                N'HRX'                                           AS EpicMasterFile,
                CAST(s.OPERATOR      AS NVARCHAR(MAX))           AS Operator,
                CAST(ipl.[Intraparam Logic] AS NVARCHAR(MAX))    AS IntraParameterLogic,
                GETDATE()                                        AS ExtractDate
            FROM raw_v2.SEARCH_EXPRESSION s
            INNER JOIN raw_v2.PROMPT_PARAMETERS p
                ON p.PARAM_UNIQ = s.PARAMETER_UNIQ
            INNER JOIN raw_v2.REPORT_INFO hrx
                ON s.REPORT_INFO_ID = hrx.REPORT_INFO_ID
            INNER JOIN raw_v2.ZC_REPORT_TYPE_HGR rec_type
                ON hrx.RECORD_TYPE_C = rec_type.REPORT_TYPE_HGR_C
            LEFT OUTER JOIN raw_v2.clarity_intraparameter_logic ipl
                ON s.PARAMETER_UNIQ = TRY_CAST(ipl.[Crit Uniq] AS NUMERIC(18,0))
                AND s.REPORT_INFO_ID = TRY_CAST(ipl.[HRX ID]    AS NUMERIC(18,0))
            WHERE hrx.TEMP_REPORT_C = 0
                AND hrx.REPORT_ID IS NOT NULL
                AND (hrx.PRIVATE_OR_PUBLIC_C <> 1 OR hrx.PRIVATE_OR_PUBLIC_C IS NULL)
                AND p.PARAMETER_PROMPT_ID = hrx.REPORT_ID;

            DECLARE @HRXParamCount INT = @@ROWCOUNT;

            -- Source 2: HGR Parameters (template-level parameter definitions)
            INSERT INTO stage_v2.ReportObjectParametersStaging (
                BizKey,
                ParameterName,
                ExtractDate,
                EpicRecordID,
                EpicMasterFile
            )
            SELECT
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    CASE
                        WHEN rec_type.NAME = 'Settings'  THEN N'Application Report Template'
                        WHEN rec_type.NAME = 'Workbench'  THEN N'Reporting Workbench Template'
                        WHEN rec_type.NAME IS NULL         THEN N'Other HGR Template'
                        ELSE CONCAT(rec_type.NAME, N' Template')
                    END,               N'||',
                    N'',               N'||',
                    N'HGR',            N'||',
                    p.PARAMETER_PROMPT_ID
                )                                               AS BizKey,
                CAST(p.CAPTION AS NVARCHAR(MAX))                AS ParameterName,
                GETDATE()                                       AS ExtractDate,
                CAST(p.PARAMETER_PROMPT_ID AS NUMERIC(18,0))        AS EpicRecordID,
                N'HGR'                                              AS EpicMasterFile
            FROM raw_v2.PROMPT_PARAMETERS p
            INNER JOIN raw_v2.TEMPLATE_INFO hgr
                ON hgr.REPORT_ID = p.PARAMETER_PROMPT_ID
            LEFT OUTER JOIN raw_v2.ZC_REPORT_TYPE_HGR rec_type
                ON rec_type.REPORT_TYPE_HGR_C = hgr.REPORT_TYPE_HGR_C;

            DECLARE @HGRParamCount INT = @@ROWCOUNT;

            -- Source 3: Cogito SQL Parameters (UNPIVOT date/time fields)
            INSERT INTO stage_v2.ReportObjectParametersStaging (
                BizKey,
                ParameterName,
                DefaultValue,
                ExtractDate,
                EpicRecordID,
                EpicMasterFile
            )
            SELECT DISTINCT
                CONCAT(
                    N'clarity_server', N'||',
                    N'clarityreport',  N'||',
                    N'Reporting Workbench Template', N'||',
                    N'',               N'||',
                    N'HGR',            N'||',
                    template.REPORT_ID
                )                                               AS BizKey,
                unpvt.Param_Name                                AS ParameterName,
                CAST(unpvt.Param_Value AS NVARCHAR(MAX))        AS DefaultValue,
                GETDATE()                                       AS ExtractDate,
                CAST(template.REPORT_ID AS NUMERIC(18,0))           AS EpicRecordID,
                N'HGR'                                              AS EpicMasterFile
            FROM raw_v2.QUERY_DYNAMIC d
            INNER JOIN raw_v2.DRILL_TEXT_SQLSERVER t
                ON t.JOB_CONFIGURATION_ID = d.JOB_CONFIG_ID
            INNER JOIN raw_v2.PROMPT_INFO p
                ON p.QUERY_TEMPLATE_ID = d.TEMPLATE_ID
            INNER JOIN raw_v2.TEMPLATE_DYNAMIC template
                ON template.PARAM_PROMPT_ID = p.PARAMETER_PROMPT_ID
            INNER JOIN (
                SELECT TEMPLATE_ID, Param_Value, Param_Name
                FROM (
                    SELECT TEMPLATE_ID,
                        CAST(START_DATE AS NVARCHAR(254)) AS START_DATE,
                        CAST(START_TIME AS NVARCHAR(254)) AS START_TIME,
                        CAST(END_DATE AS NVARCHAR(254))   AS END_DATE,
                        CAST(END_TIME AS NVARCHAR(254))   AS END_TIME
                    FROM raw_v2.QUERY_DYNAMIC
                ) AS src
                UNPIVOT (Param_Value FOR Param_Name IN (START_DATE, START_TIME, END_DATE, END_TIME)) AS unpvt
            ) unpvt ON unpvt.TEMPLATE_ID = d.TEMPLATE_ID;

            SET @RowCount = @HRXParamCount + @HGRParamCount + @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';


        -- =====================================================================
        -- STEP 18: (REMOVED — moved to usp_Atlas_ClarityHierarchy)
        -- Hierarchy staging now runs post-CSV-loader via 12b_usp_Atlas_ClarityHierarchy.sql.
        -- CSV-dependent branches (H6-H10) were executing against empty tables.
        -- =====================================================================


        -- =====================================================================
        -- STEP 19: Stage SlicerDicer Sessions → ReportObjectsStaging
        -- =====================================================================
        -- Source: SSIS ETL-Clarity 160_DataFlow_SlicerDicer_Sessions.sql
        -- SlicerDicer sessions from raw_v2.clarity_slicerdicer_sessions
        -- joined to raw_v2.clarity_slicerdicer_public_sessions for
        -- visibility (public vs private).
        --
        -- BizKey format: clarity_server||clarityreport||SlicerDicer Session||||HRX||{Report_ID}
        -- (matches Step 18 H12 hierarchy references)
        --
        -- Without this step, SlicerDicer sessions never reach
        -- stage_v2.ReportObjectsStaging, Merge 1 cannot create prd_v2
        -- records, and Step 20 Branch 8 always produces 0 rows.
        --
        -- Target: stage_v2.ReportObjectsStaging (INSERT — no truncate)
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage 19: SlicerDicer Sessions → ReportObjectsStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            INSERT INTO stage_v2.ReportObjectsStaging (
                BizKey, ObjectName, ObjectType, ObjectPath,
                ObjectDescription, SourceSystem, SourceServer,
                CreatedDate, ModifiedDate, CreatedBy, ModifiedBy,
                IsHidden, ObjectURL, EpicMasterFile, EpicRecordID,
                Availability, EpicReportTemplateId
            )
            SELECT DISTINCT
                CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                       N'SlicerDicer Session', N'||', N'', N'||',
                       N'HRX', N'||', ses.Report_ID)    AS BizKey,
                ses.Name                                 AS ObjectName,
                N'SlicerDicer Session'                   AS ObjectType,
                NULL                                     AS ObjectPath,
                CONVERT(NVARCHAR(MAX), ses.Description)  AS ObjectDescription,
                N'Clarity'                               AS SourceSystem,
                N'clarity_server'                        AS SourceServer,
                TRY_CAST(ses.Created AS DATETIME)        AS CreatedDate,
                TRY_CAST(ses.Last_modified_date AS DATETIME) AS ModifiedDate,
                COALESCE(eam.AZURE_upn, author.SYSTEM_LOGIN, author.NAME) AS CreatedBy,
                COALESCE(eam.AZURE_upn, author.SYSTEM_LOGIN, author.NAME) AS ModifiedBy,
                CASE WHEN ps.Session_ID IS NOT NULL
                     THEN 0 ELSE 1 END                  AS IsHidden,
                NULL                                     AS ObjectURL,
                N'HRX'                                   AS EpicMasterFile,
                TRY_CAST(ses.Report_ID AS NUMERIC(18,0)) AS EpicRecordID,
                CASE WHEN ps.Session_ID IS NOT NULL
                     THEN N'Public' ELSE N'Private' END  AS Availability,
                NULL                                     AS EpicReportTemplateId
            FROM raw_v2.clarity_slicerdicer_sessions ses
            LEFT OUTER JOIN raw_v2.clarity_slicerdicer_public_sessions ps
                ON ses.Report_ID = ps.Session_ID
            LEFT OUTER JOIN raw_v2.CLARITY_EMP author
                ON author.USER_ID = ses.Created_by
            LEFT OUTER JOIN dbo.EMPtoAzureMap eam
                ON author.SYSTEM_LOGIN = eam.Epic_AccountID
                AND eam.AZURE_upn NOT LIKE '%;%';

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';


        -- =====================================================================
        -- STEP 20: Stage Clarity Report Group Memberships
        --          → ReportObjectGroupsMemberships
        -- =====================================================================
        -- Source: SSIS ETL-Clarity "Stage Clarity Report Group Membership"
        -- 9-part UNION mapping report objects to security groups:
        --   1. Dashboard User Roles (IDM) — with OVRIDE_PARENT_DB_ID fallback
        --   2. Component RW Access (IDB)
        --   3. Dashboard User Types (IDM) — with OVRIDE_PARENT_DB_ID fallback
        --   4. Data Model Groups (FDM) — with BASE_RECORD_ID fallback
        --   5. Template Groups (HGR)
        --   6. Override RPT Groups (HRX)
        --   7. HRX groups inherited from parent HGR template
        --   8. SlicerDicer Public Session groups via FDM
        --   9. Dashboard ASSOC_REPORT_GROUPS
        -- JOINs to stage_v2.ReportObjectsStaging via BizKey suffix matching
        -- (BizKey ends with ||EpicMasterFile||EpicRecordID).
        --
        -- Fixes applied (fix/clarity-step19):
        --   F5: Branches 1,3 — ISNULL(di.OVRIDE_PARENT_DB_ID, dashboard_id)
        --   F6: Branch 4 — ISNULL(TRY_CAST(DMD.BASE_RECORD_ID), DMD.DATA_MODEL_ID)
        --   F2: Branch 7 — HRX inherits HGR template groups (via #HGRs CTE)
        --   F3: Branch 8 — SlicerDicer public sessions inherit FDM groups
        --   F4: Branch 9 — Dashboard ASSOC_REPORT_GROUPS
        --   B8: Branch 8 — fix cast direction: CAST(ss.EpicRecordID AS NVARCHAR)
        --   B9: Branch 9 — replace ClarityUserGroups with ZC_ALLOWABLE_GRPS
        -- Target: stage_v2.ReportObjectGroupsMemberships (TRUNCATE + INSERT)
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage 20: Clarity Report Group Memberships → ReportObjectGroupsMemberships';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectGroupsMemberships;

            -- ──────────────────────────────────────────────────────────────────
            -- Performance: pre-materialize three temp tables shared across the
            -- 9-branch UNION below. Eliminates repeated scans of large source
            -- tables and gives the optimizer a tight clustered seek target.
            -- ──────────────────────────────────────────────────────────────────

            -- #ROS: filtered snapshot of ReportObjectsStaging joined by every branch
            IF OBJECT_ID('tempdb..#ROS') IS NOT NULL DROP TABLE #ROS;

            SELECT BizKey, EpicMasterFile, EpicRecordID
            INTO #ROS
            FROM stage_v2.ReportObjectsStaging
            WHERE EpicMasterFile IN (N'IDM', N'IDB', N'FDM', N'HGR', N'HRX')
              AND EpicRecordID IS NOT NULL;

            CREATE CLUSTERED INDEX IX_ROS ON #ROS (EpicMasterFile, EpicRecordID);

            -- #Step20HGRs: replicates SSIS #HGRs temp table used by Branch 7
            -- (HRX reports inheriting groups from their parent HGR template)
            IF OBJECT_ID('tempdb..#Step20HGRs') IS NOT NULL DROP TABLE #Step20HGRs;

            SELECT DISTINCT
                ti.REPORT_ID        AS HGR_ID,
                ag.ALLOWABLE_GRPS_C AS REPORT_GROUP_C,
                ag.NAME             AS REPORT_GROUP_NAME
            INTO #Step20HGRs
            FROM raw_v2.CLARITY_RPT_GROUPS crg
            LEFT OUTER JOIN raw_v2.CLARITY_RPT rpt
                ON crg.REPORT_ID = rpt.REPORT_ID
            LEFT OUTER JOIN raw_v2.TEMPLATE_INFO ti
                ON rpt.ASSOC_REPORT_ID = ti.REPORT_ID
            LEFT OUTER JOIN raw_v2.ZC_ALLOWABLE_GRPS ag
                ON crg.REPORT_GROUP_C = ag.ALLOWABLE_GRPS_C
            WHERE (rpt.HIDE_FROM_LIBRARY_YN != 'Y' OR rpt.HIDE_FROM_LIBRARY_YN IS NULL)
              AND ti.REPORT_TYPE_HGR_C NOT IN (3, 7, 8)
              AND (ti.STATUS_C IS NULL OR ti.STATUS_C = 0);

            CREATE CLUSTERED INDEX IX_Step20HGRs ON #Step20HGRs (HGR_ID);

            -- #ClarityUG: distinct group dimension shared by Branches 2 and 3
            IF OBJECT_ID('tempdb..#ClarityUG') IS NOT NULL DROP TABLE #ClarityUG;

            SELECT DISTINCT GroupName, GroupId
            INTO #ClarityUG
            FROM raw_v2.ClarityUserGroups;

            CREATE CLUSTERED INDEX IX_ClarityUG ON #ClarityUG (GroupId, GroupName);

            -- ──────────────────────────────────────────────────────────────────
            -- 9-branch UNION. Inner DISTINCTs removed — outer UNION deduplicates.
            -- All branches join #ROS instead of stage_v2.ReportObjectsStaging.
            -- ──────────────────────────────────────────────────────────────────
            INSERT INTO stage_v2.ReportObjectGroupsMemberships (
                BizKey, GroupId, GroupName, GroupSource, GroupType
            )
            SELECT * FROM (
                -- 1. Dashboard User Roles (IDM)
                SELECT
                    s.BizKey,
                    d.user_roles_id                             AS GroupId,
                    d.user_roles                                AS GroupName,
                    N'Clarity'                                  AS GroupSource,
                    N'Epic User Role'                           AS GroupType
                FROM raw_v2.ClarityDashboardRoles d
                LEFT JOIN raw_v2.DASHBOARD_INFO di
                    ON d.dashboard_id = di.DASHBOARD_ID
                    AND di.OVRIDE_STATUS_C = 2
                INNER JOIN #ROS s
                    ON ISNULL(di.OVRIDE_PARENT_DB_ID,
                              TRY_CAST(d.dashboard_id AS NUMERIC(18,0))) = s.EpicRecordID
                    AND s.EpicMasterFile = N'IDM'

                UNION

                -- 2. Component RW Access (IDB)
                SELECT
                    s.BizKey,
                    d.group_id                                  AS GroupId,
                    g.GroupName,
                    N'Clarity'                                  AS GroupSource,
                    N'Epic Reporting Workbench Access'           AS GroupType
                FROM raw_v2.ClarityComponentGroups d
                INNER JOIN #ClarityUG g
                    ON d.group_id = g.GroupId
                INNER JOIN #ROS s
                    ON TRY_CAST(d.COMPONENT_ID AS NUMERIC(18,0)) = s.EpicRecordID AND s.EpicMasterFile = N'IDB'

                UNION

                -- 3. Dashboard User Types (IDM)
                SELECT
                    s.BizKey,
                    d.user_types                                AS GroupId,
                    g.GroupName,
                    N'Clarity'                                  AS GroupSource,
                    N'Epic User Type'                           AS GroupType
                FROM raw_v2.ClarityDashboardTypes d
                INNER JOIN #ClarityUG g
                    ON d.user_types = g.GroupId
                LEFT JOIN raw_v2.DASHBOARD_INFO di
                    ON d.dashboard_id = di.DASHBOARD_ID
                    AND di.OVRIDE_STATUS_C = 2
                INNER JOIN #ROS s
                    ON ISNULL(di.OVRIDE_PARENT_DB_ID,
                              TRY_CAST(d.dashboard_id AS NUMERIC(18,0))) = s.EpicRecordID
                    AND s.EpicMasterFile = N'IDM'

                UNION

                -- 4. Data Model Groups (FDM)
                SELECT
                    s.BizKey,
                    CAST(zc.ALLOWABLE_GRPS_C AS NVARCHAR(MAX))  AS GroupId,
                    CAST(zc.NAME AS NVARCHAR(MAX))              AS GroupName,
                    N'Clarity'                                  AS GroupSource,
                    N'Epic Reporting Workbench Access'           AS GroupType
                FROM raw_v2.DATA_MODEL_DEFINITIONS DMD
                INNER JOIN raw_v2.DATA_MODEL_REPORT_GROUPS DMRG
                    ON DMD.DATA_MODEL_ID = DMRG.DATA_MODEL_ID
                INNER JOIN raw_v2.ZC_ALLOWABLE_GRPS zc
                    ON DMRG.REPORT_GROUPS_C = zc.ALLOWABLE_GRPS_C
                INNER JOIN #ROS s
                    ON ISNULL(
                        TRY_CAST(DMD.BASE_RECORD_ID AS NUMERIC(18,0)),
                        DMD.DATA_MODEL_ID
                    ) = s.EpicRecordID AND s.EpicMasterFile = N'FDM'

                UNION

                -- 5. Template Groups (HGR)
                SELECT
                    s.BizKey,
                    CAST(g.REPORT_GROUP_C AS NVARCHAR(MAX))     AS GroupId,
                    CAST(z.NAME AS NVARCHAR(MAX))               AS GroupName,
                    N'Clarity'                                  AS GroupSource,
                    N'Epic Reporting Workbench Access'           AS GroupType
                FROM raw_v2.TEMPLATE_INFO t
                INNER JOIN raw_v2.CLARITY_RPT r
                    ON t.REPORT_ID = r.ASSOC_REPORT_ID
                INNER JOIN raw_v2.CLARITY_RPT_GROUPS g
                    ON r.REPORT_ID = g.REPORT_ID
                INNER JOIN #ROS s
                    ON t.REPORT_ID = s.EpicRecordID AND s.EpicMasterFile = N'HGR'
                INNER JOIN raw_v2.ZC_ALLOWABLE_GRPS z
                    ON CAST(g.REPORT_GROUP_C AS VARCHAR(66)) = CAST(z.ALLOWABLE_GRPS_C AS VARCHAR(66))

                UNION

                -- 6. Override RPT Groups (HRX)
                SELECT
                    s.BizKey,
                    CAST(g.REPORT_GROUP_C AS NVARCHAR(MAX))     AS GroupId,
                    CAST(z.NAME AS NVARCHAR(MAX))               AS GroupName,
                    N'Clarity'                                  AS GroupSource,
                    N'Epic Reporting Workbench Access'           AS GroupType
                FROM raw_v2.OVRIDE_RPT_GROUPS g
                INNER JOIN #ROS s
                    ON g.REPORT_ID = s.EpicRecordID AND s.EpicMasterFile = N'HRX'
                INNER JOIN raw_v2.ZC_ALLOWABLE_GRPS z
                    ON CAST(g.REPORT_GROUP_C AS VARCHAR(66)) = CAST(z.ALLOWABLE_GRPS_C AS VARCHAR(66))

                UNION

                -- 7. HRX groups inherited from parent HGR template
                -- HRX reports that have no override groups (OVRIDE_RPT_GROUPS)
                -- inherit group assignments from their parent HGR template via
                -- the pre-materialized #Step20HGRs temp table.
                SELECT
                    s.BizKey,
                    CAST(hgrs.REPORT_GROUP_C AS NVARCHAR(MAX))  AS GroupId,
                    CAST(hgrs.REPORT_GROUP_NAME AS NVARCHAR(MAX)) AS GroupName,
                    N'Clarity'                                  AS GroupSource,
                    N'Epic Reporting Workbench Access'           AS GroupType
                FROM raw_v2.REPORT_INFO ri
                INNER JOIN #ROS s
                    ON ri.REPORT_INFO_ID = s.EpicRecordID AND s.EpicMasterFile = N'HRX'
                INNER JOIN #Step20HGRs hgrs
                    ON ri.REPORT_ID = hgrs.HGR_ID
                LEFT JOIN raw_v2.OVRIDE_RPT_GROUPS org
                    ON ri.REPORT_INFO_ID = org.REPORT_ID
                WHERE org.REPORT_GROUP_C IS NULL

                UNION

                -- 8. SlicerDicer Public Session groups via FDM
                -- FDM data model groups propagated to associated SlicerDicer
                -- public sessions (HRX-type reports). Cast direction fixed:
                -- CSV-sourced string columns cast to NUMERIC so the #ROS
                -- clustered index seek on EpicRecordID is preserved.
                SELECT
                    ss.BizKey,
                    CAST(zc.ALLOWABLE_GRPS_C AS NVARCHAR(MAX))  AS GroupId,
                    CAST(zc.NAME AS NVARCHAR(MAX))              AS GroupName,
                    N'Clarity'                                  AS GroupSource,
                    N'Epic Reporting Workbench Access'           AS GroupType
                FROM raw_v2.DATA_MODEL_DEFINITIONS DMD
                INNER JOIN raw_v2.DATA_MODEL_REPORT_GROUPS DMRG
                    ON DMD.DATA_MODEL_ID = DMRG.DATA_MODEL_ID
                INNER JOIN raw_v2.ZC_ALLOWABLE_GRPS zc
                    ON DMRG.REPORT_GROUPS_C = zc.ALLOWABLE_GRPS_C
                INNER JOIN #ROS s
                    ON ISNULL(
                        TRY_CAST(DMD.BASE_RECORD_ID AS NUMERIC(18,0)),
                        DMD.DATA_MODEL_ID
                    ) = s.EpicRecordID AND s.EpicMasterFile = N'FDM'
                INNER JOIN raw_v2.clarity_slicerdicer_sessions sds
                    ON TRY_CAST(sds.Data_model AS NUMERIC(18,0)) = s.EpicRecordID
                INNER JOIN raw_v2.clarity_slicerdicer_public_sessions ps
                    ON sds.Report_ID = ps.Session_ID
                INNER JOIN #ROS ss
                    ON TRY_CAST(sds.Report_ID AS NUMERIC(18,0)) = ss.EpicRecordID
                    AND ss.EpicMasterFile = N'HRX'

                UNION

                -- 9. Dashboard ASSOC_REPORT_GROUPS
                -- Additional dashboard group assignments from the
                -- ASSOC_REPORT_GROUPS table (with OVRIDE_PARENT_DB_ID fallback).
                SELECT
                    s.BizKey,
                    CAST(d.REPORT_GROUPS_C AS NVARCHAR(MAX))    AS GroupId,
                    CAST(zc.NAME AS NVARCHAR(MAX))              AS GroupName,
                    N'Clarity'                                  AS GroupSource,
                    N'Epic Reporting Workbench Access'           AS GroupType
                FROM raw_v2.ASSOC_REPORT_GROUPS d
                INNER JOIN raw_v2.ZC_ALLOWABLE_GRPS zc
                    ON d.REPORT_GROUPS_C = zc.ALLOWABLE_GRPS_C
                LEFT OUTER JOIN raw_v2.DASHBOARD_INFO di
                    ON d.DASHBOARD_ID = di.DASHBOARD_ID
                    AND di.OVRIDE_STATUS_C = 2
                INNER JOIN #ROS s
                    ON ISNULL(di.OVRIDE_PARENT_DB_ID,
                              TRY_CAST(d.DASHBOARD_ID AS NUMERIC(18,0))) = s.EpicRecordID
                    AND s.EpicMasterFile = N'IDM'
            ) AS t;

            SET @RowCount = @@ROWCOUNT;

            -- ──────────────────────────────────────────────────────────────────
            -- Post-INSERT covering index: benefits Merge 3 (Group Memberships)
            -- which joins on BizKey against this staging table.
            -- ──────────────────────────────────────────────────────────────────
            CREATE NONCLUSTERED INDEX IX_stage_v2_GroupsMemberships_BizKey
                ON stage_v2.ReportObjectGroupsMemberships (BizKey)
                INCLUDE (GroupId, GroupName, GroupSource, GroupType);

            -- Cleanup temp tables
            IF OBJECT_ID('tempdb..#ROS') IS NOT NULL DROP TABLE #ROS;
            IF OBJECT_ID('tempdb..#Step20HGRs') IS NOT NULL DROP TABLE #Step20HGRs;
            IF OBJECT_ID('tempdb..#ClarityUG') IS NOT NULL DROP TABLE #ClarityUG;
        END
        ELSE
            SET @RowCount = 0;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
            PRINT @StepName + ': ' + CAST(@RowCount AS VARCHAR(20)) + ' rows';


        -- =====================================================================
        -- PROCEDURE COMPLETE
        -- =====================================================================
        SET @StepSequence = @StepSequence + 1;
        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = N'Procedure End',
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = 0,
            @Status       = N'Success';

        IF @Debug = 1
        BEGIN
            PRINT '';
            PRINT '=== usp_Atlas_Clarity Complete (Pipeline B) ===';
            PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR(10)) + 's';
        END;

        RETURN 0;

    END TRY
    BEGIN CATCH
        SET @ErrorMessage  = ERROR_MESSAGE();
        SET @ErrorSeverity = ERROR_SEVERITY();
        SET @ErrorState    = ERROR_STATE();

        -- v3.2: Use usp_Atlas_LogError (captures ERROR_*() context automatically)
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;

        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
        RETURN 1;

    END CATCH;
END;
GO

PRINT 'Created etl.usp_Atlas_Clarity (Pipeline B v7.0 — ALL staging transforms complete)';
GO
