/*******************************************************************************
 * Atlas ETL Migration — usp_Atlas_PowerBI (Pipeline B — Phase 4)
 *
 * Purpose:
 *   Stages PBI reports from raw_v2.PbiReport into stage_v2.ReportObjectsStaging
 *   so they land in prd_v2.ReportObjects via Merge 1. Resolves WorkspaceId for
 *   the CN4 Option B BizKey via the raw_v2.PbiWorkspaceReport bridge.
 *
 *   Smoke test 2026-04-07: 3,567 reports across 106 workspaces, 901
 *   workspace-report bridge rows. ~75% of reports have no bridge entry
 *   and will produce an empty WorkspaceId in BizKey segment 2
 *   (PowerBI||||<type>||<id>||||) — consistent with Phase 3 which uses
 *   the same ISNULL(wr.WorkspaceId, N'') handling. Not a defect; orphaned/
 *   standalone/app-only reports share a BizKey shape with empty segment 2.
 *
 *   The CN4 BizKey MUST be byte-identical to the Phase 3 BizKey at
 *   13_usp_Atlas_RunData.sql:420-425 for Phase 12's resolve-to-prd_v2 join
 *   to succeed. Any deviation (whitespace, segment order, ISNULL handling,
 *   segment literals) will cause Merge 1 and Phase 12 to resolve different
 *   row sets.
 *
 * Targeted DELETE (not TRUNCATE):
 *   stage_v2.ReportObjectsStaging is shared with usp_Atlas_Clarity,
 *   usp_Atlas_DatabaseObjects, and (when enabled) usp_Atlas_Tableau /
 *   usp_Atlas_SSRS. Each source uses a distinct SourceSystem value:
 *       N'Clarity', N'DatabaseObjects', N'Tableau', N'SSRS', N'PowerBI'
 *   This SP deletes only WHERE SourceSystem = N'PowerBI' to preserve
 *   rows from other sources. The delete is idempotent — on a fresh
 *   pipeline run it's a no-op because Setup (02_usp_Atlas_Setup.sql:133)
 *   has already TRUNCATEd the full table. Manual re-runs of 12c alone
 *   use the DELETE to wipe stale PBI rows.
 *
 * Dependencies:
 *   - raw_v2.PbiReport populated by atlas_pbi_metadata.py
 *   - raw_v2.PbiWorkspaceReport populated by atlas_pbi_metadata.py
 *   - Must run AFTER atlas_pbi_metadata.py (for raw_v2 tables)
 *   - Must run BEFORE usp_Atlas_Merge (Merge 1 consumes the staging rows)
 *
 * Column mapping notes (stage_v2.ReportObjectsStaging schema verified
 * 2026-04-07 against 02_usp_Atlas_Setup.sql:151-172 and
 * Atlas_Staging_Table_DDL_04062026.txt:6404-6425):
 *   - EpicRecordID: NULL for every PBI row. PbiReport.ReportId is a GUID
 *     (varchar(100)) and staging EpicRecordID is NUMERIC(18,0).
 *   - IsHidden: 0 for every PBI row. PbiReport.DefaultVisibilityYN is
 *     NULL on every row (legacy script does not populate). Merge 1 at
 *     14_usp_Atlas_Merge.sql:711 translates IsHidden=0 →
 *     prd_v2.ReportObjects.DefaultVisibilityYN = 'Y'.
 *   - CreatedBy / ModifiedBy: populated from rpt.CreatedByUPN and
 *     rpt.ModifiedByUPN, which are NULL until Phase 5 runs
 *     atlas_pbi_user_identity.py. Until then, PBI ReportObjects have
 *     NULL Author/LastModifiedBy in prd_v2 — acceptable intermediate state.
 *   - SourceServer: fixed literal N'PowerBI'. There is no per-row
 *     equivalent of Clarity's N'clarity_server' → CapacityName would be
 *     the nearest analog but CapacityName is NULL on every PbiReport row.
 *   - ObjectPath, ParentPath, RawDefinition, EpicReportTemplateId: not
 *     populated (no meaningful PbiReport source).
 *
 * Execution:
 *   EXEC etl.usp_Atlas_PowerBI;
 *   EXEC etl.usp_Atlas_PowerBI @Debug = 1;
 *
 * Author:  Larry Duren
 * Date:    April 2026
 * Version: 1.0 — 2026-04-07 (initial Phase 4 implementation)
 ******************************************************************************/

USE Atlas_Staging;
GO

IF OBJECT_ID('etl.usp_Atlas_PowerBI', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_PowerBI;
GO

CREATE PROCEDURE etl.usp_Atlas_PowerBI
    @ExecutionID      UNIQUEIDENTIFIER = NULL,
    @Debug            INT = 0,
    @RaiseErrorOnFail BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageName    NVARCHAR(100) = N'ETL-PowerBI';
    DECLARE @StepName       NVARCHAR(200);
    DECLARE @LogID          BIGINT;
    DECLARE @StepSequence   INT = 0;
    DECLARE @RowCount       INT;
    DECLARE @DeletedCount   INT;
    DECLARE @StartTime      DATETIME = GETDATE();
    DECLARE @ErrorMessage   NVARCHAR(4000);
    DECLARE @ErrorSeverity  INT;
    DECLARE @ErrorState     INT;

    IF @ExecutionID IS NULL
        SET @ExecutionID = NEWID();

    BEGIN TRY

        -- =====================================================================
        -- STEP 1: Stage PBI Reports → ReportObjectsStaging
        -- =====================================================================
        -- Source: raw_v2.PbiReport LEFT JOIN raw_v2.PbiWorkspaceReport
        -- Target: stage_v2.ReportObjectsStaging
        -- BizKey: CN4 Option B — PowerBI||<WorkspaceId>||<ReportType>||<ReportId>||||
        -- =====================================================================

        SET @StepSequence = @StepSequence + 1;
        SET @StepName = N'Stage PBI Reports → ReportObjectsStaging';

        EXEC etl.usp_Atlas_LogStart
            @ExecutionID  = @ExecutionID,
            @PackageName  = @PackageName,
            @StepName     = @StepName,
            @StepSequence = @StepSequence,
            @LogID        = @LogID OUTPUT;

        IF @Debug = 0
        BEGIN
            -- ─────────────────────────────────────────────────────────────
            -- Targeted wipe of prior PBI rows. Idempotent:
            --   - On a fresh pipeline run, 02_usp_Atlas_Setup has already
            --     TRUNCATEd the full staging table → this DELETE is a no-op.
            --   - On a manual re-run of 12c alone, this DELETE wipes the
            --     previous 12c run's PBI rows so the re-insert is clean.
            -- Preserves rows from other sources (Clarity, DatabaseObjects,
            -- Tableau, SSRS) which use distinct SourceSystem values.
            -- ─────────────────────────────────────────────────────────────
            DELETE FROM stage_v2.ReportObjectsStaging
            WHERE SourceSystem = N'PowerBI';

            SET @DeletedCount = @@ROWCOUNT;

            -- ─────────────────────────────────────────────────────────────
            -- Stage PBI reports with CN4 Option B BizKey.
            --
            -- WorkspaceId resolved via raw_v2.PbiWorkspaceReport bridge.
            -- PbiReport.WorkspaceId is NULL on every row because the
            -- /admin/reports endpoint does not return workspaceId directly.
            -- The bridge (populated from /admin/groups $expand=reports
            -- nested payload) is the only source of record. ~75% of reports
            -- have no bridge entry and will get empty segment 2 — consistent
            -- with Phase 3 which uses the same ISNULL(wr.WorkspaceId, N'')
            -- handling.
            --
            -- BizKey CONCAT must be byte-identical to Phase 3 at
            -- 13_usp_Atlas_RunData.sql:420-425. Verify character-for-character
            -- before any future modification.
            -- ─────────────────────────────────────────────────────────────
            INSERT INTO stage_v2.ReportObjectsStaging (
                BizKey,
                ObjectName,
                ObjectType,
                ObjectDescription,
                SourceSystem,
                SourceServer,
                CreatedDate,
                ModifiedDate,
                CreatedBy,
                ModifiedBy,
                IsHidden,
                ObjectURL,
                EpicMasterFile,
                EpicRecordID,
                [Availability],
                [EpicReportTemplateId]
            )
            SELECT
                CONCAT(
                    N'PowerBI',                     '||',
                    ISNULL(wr.WorkspaceId,   N''),  '||',
                    ISNULL(rpt.ReportType,   N''),  '||',
                    ISNULL(rpt.ReportId,     N''),  '||||'
                )                                           AS BizKey,
                rpt.ReportName                              AS ObjectName,
                rpt.ReportType                              AS ObjectType,
                rpt.Description                             AS ObjectDescription,
                N'PowerBI'                                  AS SourceSystem,
                N'PowerBI'                                  AS SourceServer,
                rpt.CreatedDateTime                         AS CreatedDate,
                rpt.ModifiedDateTime                        AS ModifiedDate,
                rpt.CreatedByUPN                            AS CreatedBy,    -- NULL until Phase 5
                rpt.ModifiedByUPN                           AS ModifiedBy,   -- NULL until Phase 5
                CAST(0 AS BIT)                              AS IsHidden,     -- all PBI rows visible; Merge 1 → DefaultVisibilityYN='Y'
                rpt.WebUrl                                  AS ObjectURL,
                N'PBI'                                      AS EpicMasterFile,
                NULL                                        AS EpicRecordID, -- GUIDs don't fit NUMERIC(18,0)
                N'Public'                                   AS [Availability],
                NULL                                        AS [EpicReportTemplateId]
            FROM raw_v2.PbiReport rpt
            LEFT JOIN raw_v2.PbiWorkspaceReport wr
                ON rpt.ReportId = wr.ReportId
            WHERE rpt.ReportId IS NOT NULL;

            SET @RowCount = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            SET @DeletedCount = 0;
            SET @RowCount = 0;
        END

        EXEC etl.usp_Atlas_LogEnd
            @LogID        = @LogID,
            @RowsAffected = @RowCount,
            @Status       = N'Success';

        IF @Debug = 1
        BEGIN
            PRINT @StepName + ': deleted ' + CAST(@DeletedCount AS VARCHAR(20))
                + ' prior PBI rows, inserted ' + CAST(@RowCount AS VARCHAR(20)) + ' new rows';
        END;

        IF @Debug = 1
        BEGIN
            PRINT '';
            PRINT '=== usp_Atlas_PowerBI Complete ===';
            PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR(10)) + 's';
        END;

        RETURN 0;

    END TRY
    BEGIN CATCH
        SET @ErrorMessage  = ERROR_MESSAGE();
        SET @ErrorSeverity = ERROR_SEVERITY();
        SET @ErrorState    = ERROR_STATE();

        EXEC etl.usp_Atlas_LogError @LogID = @LogID;

        IF @RaiseErrorOnFail = 1
            RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

        RETURN 1;

    END CATCH;
END;
GO

PRINT 'Created etl.usp_Atlas_PowerBI (v1.0 — stage PBI reports as ReportObjects, CN4 Option B BizKey)';
GO
