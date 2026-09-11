/*******************************************************************************
 * Atlas ETL Migration — usp_Atlas_ClarityHierarchy (Pipeline B)
 *
 * Purpose:
 *   Clarity hierarchy staging — populates stage_v2.ReportObjectHierarchyStaging
 *   with 15 branches (H1 + H1b share the HGR-HRX relationship type;
 *   H12 + H12b handle the two FDM-HRX join patterns).
 *
 *   Extracted from usp_Atlas_Clarity Stage 18 because several branches depend
 *   on CSV-loaded tables (clarity_hrx_column_mapping, clarity_paf, fds_fdm_map,
 *   code_template, E3N_Export) that are populated by atlas_csv_loader.py.
 *   When hierarchy staging lived inside usp_Atlas_Clarity, it executed BEFORE
 *   atlas_csv_loader, so CSV-dependent branches (H6, H7, H8, H9, H10) always
 *   ran against empty tables and produced 0 rows.
 *
 * Execution Order:
 *   atlas_clarity_extractor.py  -> raw_v2.*           (Clarity extraction)
 *   usp_Atlas_Clarity           -> stage_v2.*         (staging transforms)
 *   atlas_csv_loader.py         -> raw_v2.CSV_*       (CSV flat-file loads)
 *   >>> usp_Atlas_ClarityHierarchy -> stage_v2.ReportObjectHierarchyStaging <<<
 *   usp_Atlas_LDAP              -> stage_v2.User/Groups
 *
 * New branches vs usp_Atlas_Clarity Stage 18:
 *   H1b  — HGR->HRX unfiltered (SSIS file 135, no WHERE clause)
 *   H12b — FDM->HRX display name join (SSIS file 110, CONCAT pattern)
 *
 * Fix:
 *   @@ROWCOUNT is now accumulated after every INSERT (was only captured
 *   after H12 in the original, logging 0 if H12 inserted 0 rows).
 *
 * Dependencies:
 *   - raw_v2 tables populated by atlas_clarity_extractor.py
 *   - raw_v2 CSV tables populated by atlas_csv_loader.py
 *   - Schema: raw_v2, stage_v2 (created by 02_usp_Atlas_Setup.sql)
 *   - Logging: etl.usp_Atlas_LogStart, etl.usp_Atlas_LogEnd
 *
 * Execution:
 *   EXEC etl.usp_Atlas_ClarityHierarchy;
 *   EXEC etl.usp_Atlas_ClarityHierarchy @Debug = 1;
 *
 * Author:  Larry Duren
 * Date:    April 2026
 * Version: 1.1 — 2026-04-07: H7 PAF-LPP ChildBizKey now uses SSIS file 130
 *                            SUBSTRING extraction (numeric LPP ID) instead of
 *                            file 155 verbatim Extension (which never resolved
 *                            in Merge 5). Path A fix per C-6 source verification.
 *                            Cosmetic: H1 comment correction, branch count 14→15.
 *          1.0 — Initial extraction from usp_Atlas_Clarity Stage 18.
 ******************************************************************************/

USE Atlas_Staging;
GO

IF OBJECT_ID('etl.usp_Atlas_ClarityHierarchy', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_ClarityHierarchy;
GO

CREATE PROCEDURE etl.usp_Atlas_ClarityHierarchy
    @ExecutionID      UNIQUEIDENTIFIER = NULL,
    @Debug            INT = 0,
    @RaiseErrorOnFail BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageName    NVARCHAR(100) = N'ETL-Clarity-Hierarchy';
    DECLARE @StepName       NVARCHAR(200);
    DECLARE @LogID          BIGINT;
    DECLARE @StepSequence   INT = 0;
    DECLARE @RowCount       INT;
    DECLARE @StartTime      DATETIME = GETDATE();
    DECLARE @ErrorMessage   NVARCHAR(4000);
    DECLARE @ErrorSeverity  INT;
    DECLARE @ErrorState     INT;

    IF @ExecutionID IS NULL
        SET @ExecutionID = NEWID();

    BEGIN TRY

        -- =====================================================================
        -- STEP 1: Hierarchy Staging (15 branches)
        -- =====================================================================
        -- Source: ETL-Clarity SSIS hierarchy data flows + 2 new branches
        -- Target: stage_v2.ReportObjectHierarchyStaging (TRUNCATE + INSERT)
        -- Columns: ParentBizKey, ChildBizKey, RelationshipType, SourceSystem
        -- All BizKeys use the same pipe-delimited pattern from usp_Atlas_Clarity
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage Hierarchies → ReportObjectHierarchyStaging (15 branches)';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            TRUNCATE TABLE stage_v2.ReportObjectHierarchyStaging;

            SET @RowCount = 0;

            -- ── H1: HGR → HRX (Template → Report) ──
            -- Source: SSIS file 114 "Clarity HGR-HRX Hierarchy"
            -- NOTE: SSIS file 114 applied three WHERE filters (TEMP_REPORT_C = 0,
            -- REPORT_ID IS NOT NULL, private_or_public_c <> 1). This branch does NOT
            -- apply those filters. At the DISTINCT staging level this is harmless:
            -- H1b (unfiltered) already produces the file-135 superset, and Merge 5
            -- deduplicates on the composite PK. H1 + H1b together faithfully replicate
            -- the SSIS output. [C-6 verification 2026-04-07]
            INSERT INTO stage_v2.ReportObjectHierarchyStaging (
                ParentBizKey, ChildBizKey, RelationshipType, SourceSystem, ExtractDate
            )
            SELECT DISTINCT
                CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                    CASE
                        WHEN rec_type.NAME = 'Settings' THEN N'Application Report Template'
                        WHEN rec_type.NAME = 'Workbench' THEN N'Reporting Workbench Template'
                        WHEN rec_type.NAME IS NULL THEN N'Other HGR Template'
                        ELSE CONCAT(rec_type.NAME, N' Template')
                    END, N'||', N'', N'||', N'HGR', N'||', hrx.REPORT_ID
                ) AS ParentBizKey,
                CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                    CASE
                        WHEN rec_type.NAME = N'Settings' THEN N'Application Report'
                        WHEN rec_type.NAME = N'Workbench' THEN N'Reporting Workbench Report'
                        WHEN rec_type.NAME IS NULL THEN N'Other HRX Report'
                        ELSE rec_type.NAME + N' Report'
                    END, N'||', N'', N'||', N'HRX', N'||', hrx.REPORT_INFO_ID
                ) AS ChildBizKey,
                N'HGR-HRX' AS RelationshipType,
                N'Clarity'  AS SourceSystem,
                GETDATE()   AS ExtractDate
            FROM raw_v2.REPORT_INFO hrx
            INNER JOIN raw_v2.ZC_REPORT_TYPE_HGR rec_type
                ON hrx.RECORD_TYPE_C = rec_type.REPORT_TYPE_HGR_C;

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- ── H1b: HGR → HRX (Template → Report) — UNFILTERED ──
            -- Source: SSIS file 135 "Clarity HGR-HRX Hierarchy" (Stage Reports 1)
            -- DCR COMMENT: "we want all child report objects included in HGR
            -- hierarchy to properly calc rundata"
            -- No WHERE clause filters — includes all REPORT_INFO rows that
            -- have a matching rec_type (INNER JOIN still applies).
            INSERT INTO stage_v2.ReportObjectHierarchyStaging (
                ParentBizKey, ChildBizKey, RelationshipType, SourceSystem, ExtractDate
            )
            SELECT DISTINCT
                CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                    CASE
                        WHEN rec_type.NAME = 'Settings' THEN N'Application Report Template'
                        WHEN rec_type.NAME = 'Workbench' THEN N'Reporting Workbench Template'
                        WHEN rec_type.NAME IS NULL THEN N'Other HGR Template'
                        ELSE CONCAT(rec_type.NAME, N' Template')
                    END, N'||', N'', N'||', N'HGR', N'||', hrx.REPORT_ID
                ) AS ParentBizKey,
                CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                    CASE
                        WHEN rec_type.NAME = N'Settings' THEN N'Application Report'
                        WHEN rec_type.NAME = N'Workbench' THEN N'Reporting Workbench Report'
                        WHEN rec_type.NAME IS NULL THEN N'Other HRX Report'
                        ELSE rec_type.NAME + N' Report'
                    END, N'||', N'', N'||', N'HRX', N'||', hrx.REPORT_INFO_ID
                ) AS ChildBizKey,
                N'HGR-HRX' AS RelationshipType,
                N'Clarity'  AS SourceSystem,
                GETDATE()   AS ExtractDate
            FROM raw_v2.REPORT_INFO hrx
            INNER JOIN raw_v2.ZC_REPORT_TYPE_HGR rec_type
                ON hrx.RECORD_TYPE_C = rec_type.REPORT_TYPE_HGR_C;

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- ── H2: IDB → IDK (Component → Dashboard Resource) ──
            -- Source: "Clarity IDB-IDK Hierarchy"
            INSERT INTO stage_v2.ReportObjectHierarchyStaging (
                ParentBizKey, ChildBizKey, RelationshipType, SourceSystem, ExtractDate
            )
            SELECT DISTINCT
                CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                    CONCAT(COALESCE(idbt.NAME, N'Other'), N' Radar Dashboard Component'),
                    N'||', N'', N'||', N'IDB', N'||', idb.COMPONENT_ID
                ) AS ParentBizKey,
                CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                    N'Radar Dashboard Resource', N'||', N'', N'||',
                    N'IDK', N'||', idk.RESOURCE_ID
                ) AS ChildBizKey,
                N'IDB-IDK' AS RelationshipType,
                N'Clarity'  AS SourceSystem,
                GETDATE()   AS ExtractDate
            FROM raw_v2.COMPONENT_INFO idb
            LEFT OUTER JOIN raw_v2.ZC_RECORD_TYPE_24 idbt
                ON idbt.RECORD_TYPE_24_C = idb.RECORD_TYPE_C
            INNER JOIN raw_v2.COMPONENT_SUMMARY_INFO idbr
                ON idbr.COMPONENT_ID = idb.COMPONENT_ID
            INNER JOIN raw_v2.RESOURCE_DISPLAY idk
                ON idk.RESOURCE_ID = idbr.DATA_RESOURCES_ID
            WHERE idbr.DATA_RESOURCES_ID IS NOT NULL;

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- ── H3: IDK → IDN (Dashboard Resource → Metric) ──
            -- Source: "Clarity IDK-IDN Hierarchy"
            INSERT INTO stage_v2.ReportObjectHierarchyStaging (
                ParentBizKey, ChildBizKey, RelationshipType, SourceSystem, ExtractDate
            )
            SELECT DISTINCT
                CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                    N'Radar Dashboard Resource', N'||', N'', N'||',
                    N'IDK', N'||', idk.RESOURCE_ID
                ) AS ParentBizKey,
                CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                    N'Radar Metric', N'||', N'', N'||',
                    N'IDN', N'||', idn.DEFINITION_ID
                ) AS ChildBizKey,
                N'IDK-IDN' AS RelationshipType,
                N'Clarity'  AS SourceSystem,
                GETDATE()   AS ExtractDate
            FROM raw_v2.RESOURCE_DISPLAY idk
            INNER JOIN raw_v2.METRIC_INFO idn
                ON idk.METRIC_DEF_ID = idn.DEFINITION_ID
            WHERE idn.DEFINITION_ID IS NOT NULL;

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- ── H4a: IDB → HRX (Component → Report via REPORT_ID) ──
            -- Source: "Clarity IDB-HRX Hierarchy" (first UNION)
            INSERT INTO stage_v2.ReportObjectHierarchyStaging (
                ParentBizKey, ChildBizKey, RelationshipType, SourceSystem, ExtractDate
            )
            SELECT DISTINCT
                CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                    N'Source Radar Dashboard Component', N'||', N'', N'||',
                    N'IDB', N'||', CINFO.COMPONENT_ID
                ) AS ParentBizKey,
                CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                    CASE
                        WHEN rec_type.NAME = N'Settings' THEN N'Application Report'
                        WHEN rec_type.NAME = N'Workbench' THEN N'Reporting Workbench Report'
                        WHEN rec_type.NAME IS NULL THEN N'Other HRX Report'
                        ELSE rec_type.NAME + N' Report'
                    END, N'||', N'', N'||', N'HRX', N'||', CINFO.REPORT_ID
                ) AS ChildBizKey,
                N'IDB-HRX' AS RelationshipType,
                N'Clarity'  AS SourceSystem,
                GETDATE()   AS ExtractDate
            FROM raw_v2.COMPONENT_INFO CINFO
            INNER JOIN raw_v2.REPORT_INFO hrx
                ON hrx.REPORT_INFO_ID = CINFO.REPORT_ID
            LEFT OUTER JOIN raw_v2.ZC_REPORT_TYPE_HGR rec_type
                ON hrx.RECORD_TYPE_C = rec_type.REPORT_TYPE_HGR_C
            WHERE CINFO.REPORT_ID IS NOT NULL;

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- ── H4b: IDB → HRX (Component → SlicerDicer Session) ──
            -- Source: "Clarity IDB-HRX Hierarchy" (second UNION)
            INSERT INTO stage_v2.ReportObjectHierarchyStaging (
                ParentBizKey, ChildBizKey, RelationshipType, SourceSystem, ExtractDate
            )
            SELECT DISTINCT
                CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                    N'Source Radar Dashboard Component', N'||', N'', N'||',
                    N'IDB', N'||', CINFO.COMPONENT_ID
                ) AS ParentBizKey,
                CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                    N'SlicerDicer Session', N'||', N'', N'||',
                    N'HRX', N'||', CINFO.SLICERDICER_REPORT_INFO_ID
                ) AS ChildBizKey,
                N'IDB-HRX' AS RelationshipType,
                N'Clarity'  AS SourceSystem,
                GETDATE()   AS ExtractDate
            FROM raw_v2.COMPONENT_INFO CINFO
            INNER JOIN raw_v2.clarity_slicerdicer_sessions sd
                ON sd.Report_ID = CAST(CINFO.SLICERDICER_REPORT_INFO_ID AS NVARCHAR(MAX))
            WHERE CINFO.SLICERDICER_REPORT_INFO_ID IS NOT NULL;

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- ── H5: IDM → IDB (Dashboard → Component) ──
            -- Source: "Clarity IDM-IDB Hierarchy"
            INSERT INTO stage_v2.ReportObjectHierarchyStaging (
                ParentBizKey, ChildBizKey, RelationshipType, SourceSystem, ExtractDate
            )
            SELECT ParentBizKey, ChildBizKey,
                N'IDM-IDB' AS RelationshipType,
                N'Clarity'  AS SourceSystem,
                GETDATE()   AS ExtractDate
            FROM (
                SELECT DISTINCT
                    CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                        COALESCE(idmt.NAME, N'Other') + N' Radar Dashboard',
                        N'||', N'', N'||', N'IDM', N'||', idm.DASHBOARD_ID
                    ) AS ParentBizKey,
                    CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                        CONCAT(COALESCE(idbt.NAME, N'Other'), N' Radar Dashboard Component'),
                        N'||', N'', N'||', N'IDB', N'||', idb.COMPONENT_ID
                    ) AS ChildBizKey,
                    idbl.REGION, idbl.LINE,
                    ROW_NUMBER() OVER (
                        PARTITION BY idm.DASHBOARD_ID, idb.COMPONENT_ID
                        ORDER BY idbl.REGION ASC, idbl.LINE ASC
                    ) AS IDB_LINE
                FROM raw_v2.DASHBOARD_INFO idm
                LEFT OUTER JOIN raw_v2.ZC_RECORD_TYPE_24 idmt
                    ON idmt.RECORD_TYPE_24_C = idm.RECORD_TYPE_C
                INNER JOIN raw_v2.COMPONENT_LIST idbl
                    ON idbl.DASHBOARD_ID = idm.DASHBOARD_ID
                INNER JOIN raw_v2.COMPONENT_INFO idb
                    ON idb.COMPONENT_ID = idbl.COMPONENT_ID
                LEFT OUTER JOIN raw_v2.ZC_RECORD_TYPE_24 idbt
                    ON idbt.RECORD_TYPE_24_C = idb.RECORD_TYPE_C
                WHERE idbl.COMPONENT_ID IS NOT NULL
            ) db_comps
            WHERE IDB_LINE = 1;

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- ── H6: HRX → PAF (Report → Workbench Column) ──
            -- Source: "Clarity HRX-PAF Hierarchy"
            -- Uses clarity_hrx_column_mapping CSV (cross-apply STRING_SPLIT)
            INSERT INTO stage_v2.ReportObjectHierarchyStaging (
                ParentBizKey, ChildBizKey, RelationshipType, SourceSystem, ExtractDate
            )
            SELECT ParentBizKey, ChildBizKey,
                N'HRX-PAF' AS RelationshipType,
                N'Clarity'  AS SourceSystem,
                GETDATE()   AS ExtractDate
            FROM (
                SELECT DISTINCT
                    CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                        CASE
                            WHEN rec_type.NAME = N'Settings' THEN N'Application Report'
                            WHEN rec_type.NAME = N'Workbench' THEN N'Reporting Workbench Report'
                            WHEN rec_type.NAME IS NULL THEN N'Other HRX Report'
                            ELSE rec_type.NAME + N' Report'
                        END, N'||', N'', N'||', N'HRX', N'||', t.HRXID
                    ) AS ParentBizKey,
                    CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                        N'Reporting Workbench Column', N'||', N'', N'||',
                        N'PAF', N'||', REPLACE(t.PAFID, NCHAR(13), '')
                    ) AS ChildBizKey
                FROM (
                    SELECT ch.ReportID AS HRXID, cs.value AS PAFID
                    FROM raw_v2.clarity_hrx_column_mapping ch
                    CROSS APPLY STRING_SPLIT(REPLACE(ch.HRXSelectedFields, CHAR(10), ','), ',') cs
                ) t
                INNER JOIN raw_v2.clarity_paf cp
                    ON REPLACE(t.PAFID, NCHAR(13), '') = REPLACE(cp.[Column ID], NCHAR(10), '')
                INNER JOIN raw_v2.REPORT_INFO hrx
                    ON t.HRXID = CAST(hrx.REPORT_INFO_ID AS NVARCHAR(MAX))
                LEFT OUTER JOIN raw_v2.ZC_REPORT_TYPE_HGR rec_type
                    ON hrx.RECORD_TYPE_C = rec_type.REPORT_TYPE_HGR_C
                WHERE ISNULL(t.PAFID, '') <> ''
            ) sub
            WHERE ParentBizKey IS NOT NULL AND ChildBizKey IS NOT NULL;

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- ── H7: PAF → LPP (Workbench Column → Extension) ──
            -- Source: SSIS Stage Reports 2 "Clarity PAF-LPP Hierarchy" (verbatim variant)
            --   raw_v2.clarity_paf.Extension contains the raw numeric LPP ID
            --   (e.g., '100343', '66956') OR the literal 'nan' string from
            --   atlas_csv_loader.py when the source CSV had a missing value.
            --   There is NO bracket format in the BILH data, so the prior
            --   SUBSTRING extraction (C-6, 2026-04-07) produced empty LPP IDs
            --   for all 77,749 rows and zero resolved in Merge 5.
            --   Use Extension verbatim per SSIS SR2 variant; filter out 'nan'
            --   and other non-numeric values via TRY_CAST(... AS BIGINT) — the
            --   strict integer guard avoids ISNUMERIC false positives ('$',
            --   '+', '1e3', etc.) and matches the LPP_ID integer PK type used
            --   when LPP ReportObjects are built in 12_usp_Atlas_Clarity.
            INSERT INTO stage_v2.ReportObjectHierarchyStaging (
                ParentBizKey, ChildBizKey, RelationshipType, SourceSystem, ExtractDate
            )
            SELECT ParentBizKey, ChildBizKey,
                N'PAF-LPP' AS RelationshipType,
                N'Clarity'  AS SourceSystem,
                GETDATE()   AS ExtractDate
            FROM (
                SELECT DISTINCT
                    CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                        N'Reporting Workbench Column', N'||', N'', N'||',
                        N'PAF', N'||', paf.[Column ID]
                    ) AS ParentBizKey,
                    CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                        N'Reporting Workbench Extension', N'||', N'', N'||',
                        N'LPP', N'||',
                        -- SSIS file 130: SUBSTRING extracts numeric LPP ID from bracketed
                        -- Extension format, e.g., 'Display Name [12345]' → '12345'.
                        -- File 155 (verbatim Extension) is intentionally NOT used here:
                        -- it produces BizKeys that never resolve in Merge 5. Path A fix
                        -- per C-6 source verification 2026-04-07.
                        -- CASE guard added 2026-04-07: Extension values without brackets
                        -- (no '[' or ']') would produce a negative SUBSTRING length and
                        -- abort the SP. Guard returns NULL for those rows; CONCAT emits
                        -- empty segment. Rows with valid [numeric] format extract correctly.
                        CASE
                            WHEN CHARINDEX('[', paf.Extension) > 0
                             AND CHARINDEX(']', paf.Extension)
                                 > CHARINDEX('[', paf.Extension)
                            THEN SUBSTRING(
                                paf.Extension,
                                CHARINDEX('[', paf.Extension) + 1,
                                CHARINDEX(']', paf.Extension)
                                    - CHARINDEX('[', paf.Extension) - 1
                            )
                            ELSE NULL
                        END
                    ) AS ChildBizKey
                FROM raw_v2.clarity_paf paf
                WHERE ISNULL(paf.Extension, '') <> ''
                  AND TRY_CAST(paf.Extension AS BIGINT) IS NOT NULL
            ) sub
            WHERE ParentBizKey IS NOT NULL AND ChildBizKey IS NOT NULL;

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- ── H8: IDB → E3N (Component → Code Template) ──
            -- Source: "IDB - E3N Hierarchy"
            INSERT INTO stage_v2.ReportObjectHierarchyStaging (
                ParentBizKey, ChildBizKey, RelationshipType, SourceSystem, ExtractDate
            )
            SELECT DISTINCT
                CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                    N'Source Radar Dashboard Component', N'||', N'', N'||',
                    N'IDB', N'||', CINFO.COMPONENT_ID
                ) AS ParentBizKey,
                CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                    N'Code Template', N'||', N'', N'||',
                    N'E3N', N'||', CINFO.CODE_TEMPLATE_ID
                ) AS ChildBizKey,
                N'IDB-E3N' AS RelationshipType,
                N'Clarity'  AS SourceSystem,
                GETDATE()   AS ExtractDate
            FROM raw_v2.COMPONENT_INFO CINFO
            INNER JOIN raw_v2.code_template ct
                ON CAST(CINFO.CODE_TEMPLATE_ID AS NVARCHAR(200)) = ct.[Code Template ID]
            WHERE CINFO.CODE_TEMPLATE_ID IS NOT NULL;

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- ── H9: LPP → E3N (Extension → Code Template) ──
            -- Source: "LPP - E3N Hierarchy"
            INSERT INTO stage_v2.ReportObjectHierarchyStaging (
                ParentBizKey, ChildBizKey, RelationshipType, SourceSystem, ExtractDate
            )
            SELECT ParentBizKey, ChildBizKey,
                N'LPP-E3N' AS RelationshipType,
                N'Clarity'  AS SourceSystem,
                GETDATE()   AS ExtractDate
            FROM (
                SELECT DISTINCT
                    CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                        N'Reporting Workbench Extension', N'||', N'', N'||',
                        N'LPP', N'||', CAST(lpp.LPP_ID AS NVARCHAR)
                    ) AS ParentBizKey,
                    CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                        N'Code Template', N'||', N'', N'||',
                        N'E3N', N'||', CAST(lpp.TEMPLATE_ID AS NVARCHAR)
                    ) AS ChildBizKey
                FROM raw_v2.CLARITY_LPP lpp
                INNER JOIN raw_v2.code_template ct
                    ON CAST(lpp.TEMPLATE_ID AS NVARCHAR) = ct.[Code Template ID]
                WHERE ISNULL(CAST(lpp.TEMPLATE_ID AS NVARCHAR), '') <> ''
            ) sub
            WHERE ParentBizKey IS NOT NULL AND ChildBizKey IS NOT NULL;

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- ── H10: FDM → FDS (SlicerDicer Model → Filter) ──
            -- Source: "FDS - FDM Hierarchy" (note: SSIS names parent as FDM)
            INSERT INTO stage_v2.ReportObjectHierarchyStaging (
                ParentBizKey, ChildBizKey, RelationshipType, SourceSystem, ExtractDate
            )
            SELECT ParentBizKey, ChildBizKey,
                N'FDM-FDS' AS RelationshipType,
                N'Clarity'  AS SourceSystem,
                GETDATE()   AS ExtractDate
            FROM (
                SELECT DISTINCT
                    CONCAT(N'slicerdicer_server', N'||', N'slicerdicer', N'||',
                        N'SlicerDicer Model', N'||', N'', N'||',
                        N'FDM', N'||', s.[FDM ID]
                    ) AS ParentBizKey,
                    CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                        N'SlicerDicer Filter', N'||', N'', N'||',
                        N'FDS', N'||', s.[FDS ID]
                    ) AS ChildBizKey
                FROM raw_v2.fds_fdm_map s
                WHERE s.[FDM ID] <> ''
            ) sub
            WHERE ParentBizKey IS NOT NULL AND ChildBizKey IS NOT NULL;

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- ── H11: HGR → LPP (Template → Extension via Setup Data) ──
            -- Source: "HGR - LPP Setup Data Extensions"
            INSERT INTO stage_v2.ReportObjectHierarchyStaging (
                ParentBizKey, ChildBizKey, RelationshipType, SourceSystem, ExtractDate
            )
            SELECT ParentBizKey, ChildBizKey,
                N'HGR-LPP' AS RelationshipType,
                N'Clarity'  AS SourceSystem,
                GETDATE()   AS ExtractDate
            FROM (
                SELECT DISTINCT
                    CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                        CASE
                            WHEN rec_type.NAME = 'Settings' THEN N'Application Report Template'
                            WHEN rec_type.NAME = 'Workbench' THEN N'Reporting Workbench Template'
                            WHEN rec_type.NAME IS NULL THEN N'Other HGR Template'
                            ELSE CONCAT(rec_type.NAME, N' Template')
                        END, N'||', N'', N'||', N'HGR', N'||', ti.REPORT_ID
                    ) AS ParentBizKey,
                    CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                        N'Reporting Workbench Extension', N'||', N'', N'||',
                        N'LPP', N'||', td.SETUP_DATA_PP_ID
                    ) AS ChildBizKey
                FROM raw_v2.TEMPLATE_DYNAMIC td
                INNER JOIN raw_v2.TEMPLATE_INFO ti
                    ON td.REPORT_ID = ti.REPORT_ID
                INNER JOIN raw_v2.ZC_REPORT_TYPE_HGR rec_type
                    ON ti.REPORT_TYPE_HGR_C = rec_type.REPORT_TYPE_HGR_C
                WHERE td.SETUP_DATA_PP_ID IS NOT NULL
            ) sub
            WHERE ParentBizKey IS NOT NULL AND ChildBizKey IS NOT NULL;

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- ── H12: FDM → HRX (SlicerDicer Model → Session) ──
            -- Source: SSIS file 145 "Clarity FDM-HRX Hierarchy" (direct ID match)
            --   INNER JOIN to DATA_MODEL_DEFINITIONS (was LEFT OUTER). Sessions
            --   whose Data_model didn't match any DMD row previously emitted
            --   phantom ParentBizKeys ending '||FDM||' (CONCAT converts NULL
            --   DATA_MODEL_ID to empty string; the outer IS NOT NULL guard does
            --   not catch it because the resulting string is non-NULL). Phantoms
            --   inflated staging and never resolved in Merge 5. INNER JOIN drops
            --   unmatched sessions before they reach the SELECT list.
            INSERT INTO stage_v2.ReportObjectHierarchyStaging (
                ParentBizKey, ChildBizKey, RelationshipType, SourceSystem, ExtractDate
            )
            SELECT ParentBizKey, ChildBizKey,
                N'FDM-HRX' AS RelationshipType,
                N'Clarity'  AS SourceSystem,
                GETDATE()   AS ExtractDate
            FROM (
                SELECT DISTINCT
                    CONCAT(N'slicerdicer_server', N'||', N'slicerdicer', N'||',
                        N'SlicerDicer Model', N'||', N'', N'||',
                        N'FDM', N'||', d.DATA_MODEL_ID
                    ) AS ParentBizKey,
                    CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                        N'SlicerDicer Session', N'||', N'', N'||',
                        N'HRX', N'||', s.Report_ID
                    ) AS ChildBizKey
                FROM raw_v2.clarity_slicerdicer_sessions s
                INNER JOIN raw_v2.DATA_MODEL_DEFINITIONS d
                    ON TRY_CAST(s.Data_model AS NUMERIC(18,0)) = d.DATA_MODEL_ID
                LEFT OUTER JOIN raw_v2.clarity_slicerdicer_public_sessions ps
                    ON s.Report_ID = ps.Session_ID
                WHERE s.Data_model <> ''
            ) sub
            WHERE ParentBizKey IS NOT NULL AND ChildBizKey IS NOT NULL;

            SET @RowCount = @RowCount + @@ROWCOUNT;

            -- ── H12b: FDM → HRX (SlicerDicer Model → Session) — display name join ──
            -- Source: SSIS file 110 "Clarity FDM-HRX Hierarchy" (Stage Reports, pass 1)
            -- Uses CONCAT(RECORD_NAME, ' [', DATA_MODEL_ID, ']') join pattern
            -- This is the older join that matches Data_model strings like
            -- "Model Name [123]" instead of raw DATA_MODEL_ID.
            --   INNER JOIN to DATA_MODEL_DEFINITIONS (was LEFT OUTER) — see H12
            --   comment for full rationale. H12 and H12b together previously
            --   produced 21,020 phantom FDM-HRX rows.
            INSERT INTO stage_v2.ReportObjectHierarchyStaging (
                ParentBizKey, ChildBizKey, RelationshipType, SourceSystem, ExtractDate
            )
            SELECT ParentBizKey, ChildBizKey,
                N'FDM-HRX' AS RelationshipType,
                N'Clarity'  AS SourceSystem,
                GETDATE()   AS ExtractDate
            FROM (
                SELECT DISTINCT
                    CONCAT(N'slicerdicer_server', N'||', N'slicerdicer', N'||',
                        N'SlicerDicer Model', N'||', N'', N'||',
                        N'FDM', N'||', d.DATA_MODEL_ID
                    ) AS ParentBizKey,
                    CONCAT(N'clarity_server', N'||', N'clarityreport', N'||',
                        N'SlicerDicer Session', N'||', N'', N'||',
                        N'HRX', N'||', s.Report_ID
                    ) AS ChildBizKey
                FROM raw_v2.clarity_slicerdicer_sessions s
                INNER JOIN raw_v2.DATA_MODEL_DEFINITIONS d
                    ON s.Data_model = CONCAT(d.RECORD_NAME, N' [',
                                             CAST(d.DATA_MODEL_ID AS NVARCHAR(MAX)), N']')
                LEFT OUTER JOIN raw_v2.clarity_slicerdicer_public_sessions ps
                    ON s.Report_ID = ps.Session_ID
                WHERE s.Data_model <> ''
            ) sub
            WHERE ParentBizKey IS NOT NULL AND ChildBizKey IS NOT NULL;

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


        IF @Debug = 1
        BEGIN
            PRINT '';
            PRINT '=== usp_Atlas_ClarityHierarchy Complete ===';
            PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR(10)) + 's';
        END;

        RETURN 0;

    END TRY
    BEGIN CATCH
        SET @ErrorMessage  = ERROR_MESSAGE();
        SET @ErrorSeverity = ERROR_SEVERITY();
        SET @ErrorState    = ERROR_STATE();

        EXEC etl.usp_Atlas_LogError @LogID = @LogID;

        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
        RETURN 1;

    END CATCH;
END;
GO

PRINT 'Created etl.usp_Atlas_ClarityHierarchy (v1.1 — 15 hierarchy branches post-CSV-loader)';
GO
