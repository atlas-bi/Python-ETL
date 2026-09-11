/*******************************************************************************
 * Atlas ETL Migration — Column-Narrowed Raw Table DDL (11 tables)
 *
 * Companion to 05d_clarity_emp_narrow_ddl.sql (CLARITY_EMP narrowing).
 * Drops and recreates 11 raw_v2 tables with only the columns consumed by
 * usp_Atlas_Clarity v7.0. Column types match SSIS ExternalMetadata from
 * ETL-Clarity.dtsx.
 *
 * Summary of all 12 narrowed tables (including CLARITY_EMP from 05d):
 *
 *   Table                    Was   Now   SP Steps
 *   ─────────────────────    ───   ───   ──────────────────
 *   CLARITY_EMP              173     5   7, 8, 12, 16, 17      (see 05d)
 *   METRIC_INFO              104     5   5
 *   COMPONENT_INFO            64    10   8, 10, 18, 20
 *   TEMPLATE_INFO             59     4   11, 12, 15, 20
 *   TEMPLATE_DYNAMIC          44     7   11, 18, 19
 *   RESOURCE_DISPLAY          40     4   10, 20
 *   REPORT_INFO               39    11   12, 18, 19, 20
 *   FILTER_DEFINITIONS        35     6   7
 *   DATA_MODEL_DEFINITIONS    28     4   6, 20
 *   QUERY_DYNAMIC             27     9   11, 18, 19
 *   CLARITY_RPT               26     5   11, 12
 *   DASHBOARD_INFO            23    10   9, 20
 *   COMPONENT_LIST            18     4   20
 *
 * Verified via sys.sql_expression_dependencies: sole consumer is
 * etl.usp_Atlas_Clarity.
 *
 * Dependencies:
 *   - Schema: raw_v2 (created by 02_usp_Atlas_Setup.sql)
 *
 * Consumed by:
 *   - etl.usp_Atlas_Clarity (Steps 5-20)
 *   - atlas_clarity_extractor.py (extraction destination)
 *
 * Run order: After 02_usp_Atlas_Setup.sql, before 12_usp_Atlas_Clarity.sql
 *
 * Author:  Larry Duren
 * Date:    March 2026
 * Version: 1.0
 ******************************************************************************/

USE Atlas_Staging;
GO

PRINT '=== Column-Narrowed Raw Table DDL (11 tables) ===';
PRINT 'Start Time: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
GO

-- =============================================================================
-- 1. raw_v2.METRIC_INFO (104 → 5 columns)
-- Source: dbo.METRIC_INFO on EPICCLAPRD (Extract 17)
-- Used by: usp_Atlas_Clarity Step 5 (IDN staging) via alias idn
-- =============================================================================
IF OBJECT_ID('raw_v2.METRIC_INFO', 'U') IS NOT NULL
BEGIN
    DECLARE @MetricCols INT;
    SELECT @MetricCols = COUNT(*)
    FROM   INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_SCHEMA = 'raw_v2' AND TABLE_NAME = 'METRIC_INFO';
    PRINT 'Dropping existing raw_v2.METRIC_INFO (' + CAST(@MetricCols AS VARCHAR(10)) + ' columns)';
    DROP TABLE raw_v2.METRIC_INFO;
END;
GO

CREATE TABLE raw_v2.METRIC_INFO (
    DEFINITION_ID        NUMERIC(18,0)  NULL,
    METRIC_NAME          NVARCHAR(254)  NULL,
    INST_OF_UPDATE_DTTM  DATETIME       NULL,
    ACTIVE_YN            VARCHAR(1)     NULL,
    RECORD_STATUS_C      INT            NULL,
    ETL_LoadDate         DATETIME       NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.METRIC_INFO (narrowed: 5 data columns + ETL_LoadDate)';
GO

-- =============================================================================
-- 2. raw_v2.COMPONENT_INFO (64 → 10 columns)
-- Source: dbo.COMPONENT_INFO on EPICCLAPRD (Extract 6)
-- Used by: usp_Atlas_Clarity Steps 8, 10, 18, 20 via aliases idb, CINFO
-- =============================================================================
IF OBJECT_ID('raw_v2.COMPONENT_INFO', 'U') IS NOT NULL
BEGIN
    DECLARE @CompInfoCols INT;
    SELECT @CompInfoCols = COUNT(*)
    FROM   INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_SCHEMA = 'raw_v2' AND TABLE_NAME = 'COMPONENT_INFO';
    PRINT 'Dropping existing raw_v2.COMPONENT_INFO (' + CAST(@CompInfoCols AS VARCHAR(10)) + ' columns)';
    DROP TABLE raw_v2.COMPONENT_INFO;
END;
GO

CREATE TABLE raw_v2.COMPONENT_INFO (
    COMPONENT_ID                NUMERIC(18,0)  NULL,
    COMPONENT_NAME              NVARCHAR(254)  NULL,
    INSTANT_OF_UPD_DTTM         DATETIME       NULL,
    READY_FOR_USE_YN            VARCHAR(1)     NULL,
    RECORD_STATUS_C             INT            NULL,
    RECORD_TYPE_C               INT            NULL,
    USER_ID                     VARCHAR(18)    NULL,
    CODE_TEMPLATE_ID            NUMERIC(18,0)  NULL,
    REPORT_ID                   NUMERIC(18,0)  NULL,
    SLICERDICER_REPORT_INFO_ID  NUMERIC(18,0)  NULL,
    ETL_LoadDate                DATETIME       NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.COMPONENT_INFO (narrowed: 10 data columns + ETL_LoadDate)';
GO

CREATE NONCLUSTERED INDEX IX_COMPONENT_INFO_COMPONENT_ID
    ON raw_v2.COMPONENT_INFO (COMPONENT_ID);
GO
PRINT 'Created index IX_COMPONENT_INFO_COMPONENT_ID';
GO

-- =============================================================================
-- 3. raw_v2.TEMPLATE_INFO (59 → 4 columns)
-- Source: dbo.TEMPLATE_INFO on EPICCLAPRD (Extract 26)
-- Used by: usp_Atlas_Clarity Steps 11, 12, 15, 20 via aliases hgr, ti, hrg
-- =============================================================================
IF OBJECT_ID('raw_v2.TEMPLATE_INFO', 'U') IS NOT NULL
BEGIN
    DECLARE @TemplInfoCols INT;
    SELECT @TemplInfoCols = COUNT(*)
    FROM   INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_SCHEMA = 'raw_v2' AND TABLE_NAME = 'TEMPLATE_INFO';
    PRINT 'Dropping existing raw_v2.TEMPLATE_INFO (' + CAST(@TemplInfoCols AS VARCHAR(10)) + ' columns)';
    DROP TABLE raw_v2.TEMPLATE_INFO;
END;
GO

CREATE TABLE raw_v2.TEMPLATE_INFO (
    REPORT_ID          NUMERIC(18,0)  NULL,
    REPORT_NAME        NVARCHAR(254)  NULL,
    REPORT_TYPE_HGR_C  INT            NULL,
    STATUS_C           INT            NULL,
    ETL_LoadDate       DATETIME       NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.TEMPLATE_INFO (narrowed: 4 data columns + ETL_LoadDate)';
GO

CREATE NONCLUSTERED INDEX IX_TEMPLATE_INFO_REPORT_ID
    ON raw_v2.TEMPLATE_INFO (REPORT_ID);
GO
PRINT 'Created index IX_TEMPLATE_INFO_REPORT_ID';
GO

-- =============================================================================
-- 4. raw_v2.TEMPLATE_DYNAMIC (44 → 7 columns)
-- Source: dbo.TEMPLATE_DYNAMIC on EPICCLAPRD (Extract 25)
-- Used by: usp_Atlas_Clarity Steps 11, 18, 19 via aliases td, template
-- =============================================================================
IF OBJECT_ID('raw_v2.TEMPLATE_DYNAMIC', 'U') IS NOT NULL
BEGIN
    DECLARE @TemplDynCols INT;
    SELECT @TemplDynCols = COUNT(*)
    FROM   INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_SCHEMA = 'raw_v2' AND TABLE_NAME = 'TEMPLATE_DYNAMIC';
    PRINT 'Dropping existing raw_v2.TEMPLATE_DYNAMIC (' + CAST(@TemplDynCols AS VARCHAR(10)) + ' columns)';
    DROP TABLE raw_v2.TEMPLATE_DYNAMIC;
END;
GO

CREATE TABLE raw_v2.TEMPLATE_DYNAMIC (
    REPORT_ID          NUMERIC(18,0)  NULL,
    MAX_NUM_SEARCH     INT            NULL,
    MAX_NUM_RETURN     INT            NULL,
    DESCRIPTION        NVARCHAR(MAX)  NULL,
    CONTACT_NUM        NUMERIC(18,0)  NULL,
    PARAM_PROMPT_ID    NUMERIC(18,0)  NULL,
    SETUP_DATA_PP_ID   NUMERIC(18,0)  NULL,
    ETL_LoadDate       DATETIME       NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.TEMPLATE_DYNAMIC (narrowed: 7 data columns + ETL_LoadDate)';
GO

-- =============================================================================
-- 5. raw_v2.RESOURCE_DISPLAY (40 → 4 columns)
-- Source: dbo.RESOURCE_DISPLAY on EPICCLAPRD (Extract 28)
-- Used by: usp_Atlas_Clarity Steps 10, 20 via aliases idk, idk2
-- =============================================================================
IF OBJECT_ID('raw_v2.RESOURCE_DISPLAY', 'U') IS NOT NULL
BEGIN
    DECLARE @ResDspCols INT;
    SELECT @ResDspCols = COUNT(*)
    FROM   INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_SCHEMA = 'raw_v2' AND TABLE_NAME = 'RESOURCE_DISPLAY';
    PRINT 'Dropping existing raw_v2.RESOURCE_DISPLAY (' + CAST(@ResDspCols AS VARCHAR(10)) + ' columns)';
    DROP TABLE raw_v2.RESOURCE_DISPLAY;
END;
GO

CREATE TABLE raw_v2.RESOURCE_DISPLAY (
    RESOURCE_ID            NUMERIC(18,0)  NULL,
    RECORD_NAME            NVARCHAR(254)  NULL,
    METRIC_DEF_ID          NUMERIC(18,0)  NULL,
    INSTANT_OF_UPDATE_DTTM DATETIME       NULL,
    ETL_LoadDate           DATETIME       NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.RESOURCE_DISPLAY (narrowed: 4 data columns + ETL_LoadDate)';
GO

-- =============================================================================
-- 6. raw_v2.REPORT_INFO (39 → 11 columns)
-- Source: dbo.REPORT_INFO on EPICCLAPRD (Extract 22)
-- Used by: usp_Atlas_Clarity Steps 12, 18, 19, 20 via alias hrx
-- =============================================================================
IF OBJECT_ID('raw_v2.REPORT_INFO', 'U') IS NOT NULL
BEGIN
    DECLARE @RptInfoCols INT;
    SELECT @RptInfoCols = COUNT(*)
    FROM   INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_SCHEMA = 'raw_v2' AND TABLE_NAME = 'REPORT_INFO';
    PRINT 'Dropping existing raw_v2.REPORT_INFO (' + CAST(@RptInfoCols AS VARCHAR(10)) + ' columns)';
    DROP TABLE raw_v2.REPORT_INFO;
END;
GO

CREATE TABLE raw_v2.REPORT_INFO (
    REPORT_INFO_ID        NUMERIC(18,0)  NULL,
    REPORT_INFO_NAME      NVARCHAR(254)  NULL,
    RECORD_TYPE_C         INT            NULL,
    PRIVATE_OR_PUBLIC_C   INT            NULL,
    TEMP_REPORT_C         INT            NULL,
    REPORT_ID             NUMERIC(18,0)  NULL,
    CREATED_BY_USER_ID    VARCHAR(18)    NULL,
    LAST_MOD_BY_USER_ID   VARCHAR(18)    NULL,
    INST_OF_LAST_MOD_DTTM DATETIME       NULL,
    OVRIDE_SEARCH_RECS    INT            NULL,
    OVRIDE_FIND_RECS      INT            NULL,
    ETL_LoadDate          DATETIME       NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.REPORT_INFO (narrowed: 11 data columns + ETL_LoadDate)';
GO

CREATE NONCLUSTERED INDEX IX_REPORT_INFO_REPORT_INFO_ID
    ON raw_v2.REPORT_INFO (REPORT_INFO_ID);
GO
PRINT 'Created index IX_REPORT_INFO_REPORT_INFO_ID';
GO

CREATE NONCLUSTERED INDEX IX_REPORT_INFO_REPORT_ID
    ON raw_v2.REPORT_INFO (REPORT_ID);
GO
PRINT 'Created index IX_REPORT_INFO_REPORT_ID';
GO

-- =============================================================================
-- 7. raw_v2.FILTER_DEFINITIONS (35 → 6 columns)
-- Source: dbo.FILTER_DEFINITIONS on EPICCLAPRD (Extract 13)
-- Used by: usp_Atlas_Clarity Step 7 (FDS staging) via alias fds
-- =============================================================================
IF OBJECT_ID('raw_v2.FILTER_DEFINITIONS', 'U') IS NOT NULL
BEGIN
    DECLARE @FiltDefCols INT;
    SELECT @FiltDefCols = COUNT(*)
    FROM   INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_SCHEMA = 'raw_v2' AND TABLE_NAME = 'FILTER_DEFINITIONS';
    PRINT 'Dropping existing raw_v2.FILTER_DEFINITIONS (' + CAST(@FiltDefCols AS VARCHAR(10)) + ' columns)';
    DROP TABLE raw_v2.FILTER_DEFINITIONS;
END;
GO

CREATE TABLE raw_v2.FILTER_DEFINITIONS (
    FILTER_ID              NUMERIC(18,0)  NULL,
    FILTER_NAME            NVARCHAR(254)  NULL,
    BASE_RECORD_ID         NUMERIC(18,0)  NULL,
    FILTER_INACTIVE_YN     VARCHAR(1)     NULL,
    INSTANT_OF_UPDATE_DTTM DATETIME       NULL,
    RECORD_CREATION_DT     DATETIME       NULL,
    ETL_LoadDate           DATETIME       NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.FILTER_DEFINITIONS (narrowed: 6 data columns + ETL_LoadDate)';
GO

-- =============================================================================
-- 8. raw_v2.DATA_MODEL_DEFINITIONS (28 → 4 columns)
-- Source: dbo.DATA_MODEL_DEFINITIONS on EPICCLAPRD (Extract 29)
-- Used by: usp_Atlas_Clarity Steps 6, 20 via aliases b, c, d
-- =============================================================================
IF OBJECT_ID('raw_v2.DATA_MODEL_DEFINITIONS', 'U') IS NOT NULL
BEGIN
    DECLARE @DataModelCols INT;
    SELECT @DataModelCols = COUNT(*)
    FROM   INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_SCHEMA = 'raw_v2' AND TABLE_NAME = 'DATA_MODEL_DEFINITIONS';
    PRINT 'Dropping existing raw_v2.DATA_MODEL_DEFINITIONS (' + CAST(@DataModelCols AS VARCHAR(10)) + ' columns)';
    DROP TABLE raw_v2.DATA_MODEL_DEFINITIONS;
END;
GO

CREATE TABLE raw_v2.DATA_MODEL_DEFINITIONS (
    DATA_MODEL_ID   NUMERIC(18,0)  NULL,
    RECORD_NAME     NVARCHAR(254)  NULL,
    BASE_RECORD_ID  NVARCHAR(254)  NULL,
    INACTIVE_YN     VARCHAR(1)     NULL,
    ETL_LoadDate    DATETIME       NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.DATA_MODEL_DEFINITIONS (narrowed: 4 data columns + ETL_LoadDate)';
GO

-- =============================================================================
-- 9. raw_v2.QUERY_DYNAMIC (27 → 9 columns)
-- Source: dbo.QUERY_DYNAMIC on EPICCLAPRD (Extract 20)
-- Used by: usp_Atlas_Clarity Steps 11, 18, 19 via alias d, qd, UNPIVOT
-- =============================================================================
IF OBJECT_ID('raw_v2.QUERY_DYNAMIC', 'U') IS NOT NULL
BEGIN
    DECLARE @QueryDynCols INT;
    SELECT @QueryDynCols = COUNT(*)
    FROM   INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_SCHEMA = 'raw_v2' AND TABLE_NAME = 'QUERY_DYNAMIC';
    PRINT 'Dropping existing raw_v2.QUERY_DYNAMIC (' + CAST(@QueryDynCols AS VARCHAR(10)) + ' columns)';
    DROP TABLE raw_v2.QUERY_DYNAMIC;
END;
GO

CREATE TABLE raw_v2.QUERY_DYNAMIC (
    TEMPLATE_ID        NUMERIC(18,0)  NULL,
    JOB_CONFIG_ID      NUMERIC(18,0)  NULL,
    CONTEXT            VARCHAR(254)   NULL,
    SELECT_TYPE_C      INT            NULL,
    CONTACT_DATE_REAL  FLOAT          NULL,
    START_DATE         NVARCHAR(254)  NULL,
    START_TIME         NVARCHAR(254)  NULL,
    END_DATE           NVARCHAR(254)  NULL,
    END_TIME           NVARCHAR(254)  NULL,
    ETL_LoadDate       DATETIME       NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.QUERY_DYNAMIC (narrowed: 9 data columns + ETL_LoadDate)';
GO

-- =============================================================================
-- 10. raw_v2.CLARITY_RPT (26 → 5 columns)
-- Source: dbo.CLARITY_RPT on EPICCLAPRD (Extract 2)
-- Used by: usp_Atlas_Clarity Steps 11, 12 via alias rpt
-- =============================================================================
IF OBJECT_ID('raw_v2.CLARITY_RPT', 'U') IS NOT NULL
BEGIN
    DECLARE @ClarRptCols INT;
    SELECT @ClarRptCols = COUNT(*)
    FROM   INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_SCHEMA = 'raw_v2' AND TABLE_NAME = 'CLARITY_RPT';
    PRINT 'Dropping existing raw_v2.CLARITY_RPT (' + CAST(@ClarRptCols AS VARCHAR(10)) + ' columns)';
    DROP TABLE raw_v2.CLARITY_RPT;
END;
GO

CREATE TABLE raw_v2.CLARITY_RPT (
    REPORT_ID            NUMERIC(18,0)  NULL,
    REPORT_NAME          NVARCHAR(254)  NULL,
    ASSOC_REPORT_ID      NUMERIC(18,0)  NULL,
    HIDE_FROM_LIBRARY_YN VARCHAR(1)     NULL,
    RECORD_STATUS_C      INT            NULL,
    ETL_LoadDate         DATETIME       NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.CLARITY_RPT (narrowed: 5 data columns + ETL_LoadDate)';
GO

-- =============================================================================
-- 11. raw_v2.DASHBOARD_INFO (23 → 10 columns)
-- Source: dbo.DASHBOARD_INFO on EPICCLAPRD (Extract 10)
-- Used by: usp_Atlas_Clarity Step 9 (IDM staging), Step 20 via aliases idm
-- =============================================================================
IF OBJECT_ID('raw_v2.DASHBOARD_INFO', 'U') IS NOT NULL
BEGIN
    DECLARE @DashInfoCols INT;
    SELECT @DashInfoCols = COUNT(*)
    FROM   INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_SCHEMA = 'raw_v2' AND TABLE_NAME = 'DASHBOARD_INFO';
    PRINT 'Dropping existing raw_v2.DASHBOARD_INFO (' + CAST(@DashInfoCols AS VARCHAR(10)) + ' columns)';
    DROP TABLE raw_v2.DASHBOARD_INFO;
END;
GO

CREATE TABLE raw_v2.DASHBOARD_INFO (
    DASHBOARD_ID          NUMERIC(18,0)  NULL,
    DASHBOARD_NAME        NVARCHAR(254)  NULL,
    ENABLED_YN            VARCHAR(1)     NULL,
    INSTANT_OF_UPD_DTTM   DATETIME       NULL,
    READY_FOR_USE_YN      VARCHAR(1)     NULL,
    RECORD_STATUS_C       INT            NULL,
    RECORD_TYPE_C         INT            NULL,
    USER_ID               VARCHAR(18)    NULL,
    OVRIDE_STATUS_C       INT            NULL,
    OVRIDE_PARENT_DB_ID   NUMERIC(18,0)  NULL,
    ETL_LoadDate          DATETIME       NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.DASHBOARD_INFO (narrowed: 10 data columns + ETL_LoadDate)';
GO

CREATE NONCLUSTERED INDEX IX_DASHBOARD_INFO_DASHBOARD_ID
    ON raw_v2.DASHBOARD_INFO (DASHBOARD_ID);
GO
PRINT 'Created index IX_DASHBOARD_INFO_DASHBOARD_ID';
GO

-- =============================================================================
-- 12. raw_v2.COMPONENT_LIST (18 → 4 columns)
-- Source: dbo.COMPONENT_LIST on EPICCLAPRD (Extract 7)
-- Used by: usp_Atlas_Clarity Step 20 (hierarchy) via alias idbl
-- =============================================================================
IF OBJECT_ID('raw_v2.COMPONENT_LIST', 'U') IS NOT NULL
BEGIN
    DECLARE @CompListCols INT;
    SELECT @CompListCols = COUNT(*)
    FROM   INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_SCHEMA = 'raw_v2' AND TABLE_NAME = 'COMPONENT_LIST';
    PRINT 'Dropping existing raw_v2.COMPONENT_LIST (' + CAST(@CompListCols AS VARCHAR(10)) + ' columns)';
    DROP TABLE raw_v2.COMPONENT_LIST;
END;
GO

CREATE TABLE raw_v2.COMPONENT_LIST (
    COMPONENT_ID   NUMERIC(18,0)  NULL,
    DASHBOARD_ID   NUMERIC(18,0)  NULL,
    LINE           INT            NULL,
    REGION         INT            NULL,
    ETL_LoadDate   DATETIME       NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.COMPONENT_LIST (narrowed: 4 data columns + ETL_LoadDate)';
GO

CREATE NONCLUSTERED INDEX IX_COMPONENT_LIST_COMPONENT_ID
    ON raw_v2.COMPONENT_LIST (COMPONENT_ID);
GO
PRINT 'Created index IX_COMPONENT_LIST_COMPONENT_ID';
GO

CREATE NONCLUSTERED INDEX IX_COMPONENT_LIST_DASHBOARD_ID
    ON raw_v2.COMPONENT_LIST (DASHBOARD_ID);
GO
PRINT 'Created index IX_COMPONENT_LIST_DASHBOARD_ID';
GO

-- =============================================================================
-- Verification: column counts for all 11 tables
-- =============================================================================
PRINT '';
PRINT '--- Column count verification ---';

SELECT TABLE_NAME, COUNT(*) AS ColumnCount
FROM   INFORMATION_SCHEMA.COLUMNS
WHERE  TABLE_SCHEMA = 'raw_v2'
  AND  TABLE_NAME IN (
       'METRIC_INFO', 'COMPONENT_INFO', 'TEMPLATE_INFO', 'TEMPLATE_DYNAMIC',
       'RESOURCE_DISPLAY', 'REPORT_INFO', 'FILTER_DEFINITIONS',
       'DATA_MODEL_DEFINITIONS', 'QUERY_DYNAMIC', 'CLARITY_RPT',
       'DASHBOARD_INFO', 'COMPONENT_LIST'
       )
GROUP BY TABLE_NAME
ORDER BY TABLE_NAME;
GO

PRINT '';
PRINT '=== Column-Narrowed DDL Complete ===';
PRINT 'End Time: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
PRINT 'Tables created: 11 (+ indexes: 7)';
GO
