/*******************************************************************************
 * Atlas ETL Migration — Clarity Schema Verification
 *
 * Run this against EPICCLAPRD.Clarity to verify that all tables and columns
 * referenced by atlas_clarity_extractor.py actually exist in Epic Clarity.
 *
 * Usage: sqlcmd -S "EPICCLAPRD.BILH.ITSYSTEMS.ORG" -d Clarity -i 32_clarity_schema_verify.sql
 *    Or: Run via linked server from Atlas_Staging:
 *        SELECT * FROM OPENQUERY([EPICCLAPRD], 'SELECT ...')
 *
 * Author:  Larry Duren
 * Date:    March 2026
 ******************************************************************************/

-- ============================================================================
-- STEP 1: Verify all referenced tables exist
-- ============================================================================
PRINT '=== Verifying Clarity Tables Exist ===';
PRINT '';

SELECT 'TABLE CHECK' AS CheckType,
       t.expected_table,
       CASE WHEN ist.TABLE_NAME IS NOT NULL THEN 'EXISTS' ELSE '** MISSING **' END AS Status
FROM (VALUES
    ('CLARITY_EMP'), ('CLARITY_RPT'), ('CLARITY_RPT_GROUPS'), ('CLARITY_RPT_QUEUES'),
    ('COMPONENT_DESC'), ('COMPONENT_INFO'), ('COMPONENT_LIST'), ('COMPONENT_SUMMARY_INFO'),
    ('DASHBOARD_DESC'), ('DASHBOARD_INFO'), ('DRILL_TEXT_SQLSERVER'),
    ('EMP_BASIC_INFO'), ('FILTER_DEFINITIONS'),
    ('CLARITY_LPP'), ('LPP_COMMENTS'),
    ('METRIC_DESC'), ('METRIC_INFO'),
    ('OVRIDE_RPT_GROUPS'), ('PROMPT_INFO'), ('QUERY_DYNAMIC'),
    ('REPORT_DESC'), ('REPORT_INFO'), ('REPORT_QUEUES'),
    ('TEMPLATE_DESCRIPTION'), ('TEMPLATE_DYNAMIC'), ('TEMPLATE_INFO'), ('TEMPLATE_INFO_2'),
    ('RESOURCE_DISPLAY'),
    ('DATA_MODEL_DEFINITIONS'), ('DATA_MODEL_DESCRIPTION'), ('DATA_MODEL_REPORT_GROUPS'),
    ('ASSOC_REPORT_GROUPS'),
    ('ZC_ALLOWABLE_GRPS'), ('ZC_RECORD_TYPE_24'), ('ZC_REPORT_TYPE_HGR'),
    ('TAG_INFO'), ('COMPONENT_GROUPS'),
    ('ASSOC_USER_ROLES'), ('USER_ROLE'), ('ASSOC_USER_TYPES'),
    ('TEMPLATE_TAGS'), ('ADDL_REPORT_TAGS'), ('COMPONENT_TAGS'), ('DASHBOARD_TAGS'),
    ('PROMPT_PARAMETERS'), ('SEARCH_EXPRESSION'), ('ZC_COMPARE_OPERATO'),
    ('EMP_LOGIN_HX'),
    ('CLARITY_ECL'), ('CLARITY_EMP_2'), ('RW_SEC_RPTGRPS_BASE'), ('RW_SEC_RPTGRPS_ADDL'),
    ('CLARITY_EMP_ROLE'), ('CACHED_USER_TYPE'), ('ZC_USER_TYPES'), ('BI_SEC_POINTS')
) AS t(expected_table)
LEFT JOIN INFORMATION_SCHEMA.TABLES ist
    ON ist.TABLE_NAME = t.expected_table
    AND ist.TABLE_SCHEMA = 'dbo'
ORDER BY t.expected_table;

-- ============================================================================
-- STEP 2: Spot-check key columns on critical tables
-- ============================================================================
PRINT '';
PRINT '=== Spot-checking Key Columns ===';
PRINT '';

-- CLARITY_EMP: verify a sample of the 173 columns
SELECT 'COLUMN CHECK: CLARITY_EMP' AS CheckType,
       c.expected_column,
       CASE WHEN isc.COLUMN_NAME IS NOT NULL THEN 'EXISTS' ELSE '** MISSING **' END AS Status
FROM (VALUES
    ('USER_ID'), ('NAME'), ('PROV_ID'), ('SYSTEM_LOGIN'), ('USER_STATUS_C'),
    ('EMP_RECORD_TYPE_C'), ('LNK_SEC_TEMPLT_ID'), ('LAST_ACCS_DATETIME'),
    ('MC_DEPARTMENT_ID'), ('DEACTIVATE_DAYS'), ('CAD_OTH_DEP_ECL_ID')
) AS c(expected_column)
LEFT JOIN INFORMATION_SCHEMA.COLUMNS isc
    ON isc.TABLE_NAME = 'CLARITY_EMP'
    AND isc.TABLE_SCHEMA = 'dbo'
    AND isc.COLUMN_NAME = c.expected_column;

-- METRIC_INFO: verify a sample of the 104 columns
SELECT 'COLUMN CHECK: METRIC_INFO' AS CheckType,
       c.expected_column,
       CASE WHEN isc.COLUMN_NAME IS NOT NULL THEN 'EXISTS' ELSE '** MISSING **' END AS Status
FROM (VALUES
    ('DEFINITION_ID'), ('METRIC_NAME'), ('ACTIVE_YN'), ('JOB_CONFIGURATION_ID'),
    ('AUTO_SQL_FACT_TABLE_NAME'), ('SQL_SOURCE_DATABASE_C')
) AS c(expected_column)
LEFT JOIN INFORMATION_SCHEMA.COLUMNS isc
    ON isc.TABLE_NAME = 'METRIC_INFO'
    AND isc.TABLE_SCHEMA = 'dbo'
    AND isc.COLUMN_NAME = c.expected_column;

-- REPORT_INFO: verify key columns
SELECT 'COLUMN CHECK: REPORT_INFO' AS CheckType,
       c.expected_column,
       CASE WHEN isc.COLUMN_NAME IS NOT NULL THEN 'EXISTS' ELSE '** MISSING **' END AS Status
FROM (VALUES
    ('REPORT_INFO_ID'), ('REPORT_INFO_NAME'), ('TEMP_REPORT_C'),
    ('CREATED_BY_USER_ID'), ('INST_OF_LAST_MOD_DTTM'), ('HIDE_FROM_LIBRARY_YN')
) AS c(expected_column)
LEFT JOIN INFORMATION_SCHEMA.COLUMNS isc
    ON isc.TABLE_NAME = 'REPORT_INFO'
    AND isc.TABLE_SCHEMA = 'dbo'
    AND isc.COLUMN_NAME = c.expected_column;

-- COMPONENT_INFO: verify key columns
SELECT 'COLUMN CHECK: COMPONENT_INFO' AS CheckType,
       c.expected_column,
       CASE WHEN isc.COLUMN_NAME IS NOT NULL THEN 'EXISTS' ELSE '** MISSING **' END AS Status
FROM (VALUES
    ('COMPONENT_ID'), ('COMPONENT_NAME'), ('READY_FOR_USE_YN'),
    ('SLICERDICER_REPORT_INFO_ID'), ('ACTIVITY')
) AS c(expected_column)
LEFT JOIN INFORMATION_SCHEMA.COLUMNS isc
    ON isc.TABLE_NAME = 'COMPONENT_INFO'
    AND isc.TABLE_SCHEMA = 'dbo'
    AND isc.COLUMN_NAME = c.expected_column;

-- ============================================================================
-- STEP 3: Count columns per table (compare with expected)
-- ============================================================================
PRINT '';
PRINT '=== Column Counts Per Table ===';
PRINT '';

SELECT
    t.expected_table,
    t.expected_count,
    ISNULL(actual.col_count, 0) AS actual_count,
    CASE WHEN ISNULL(actual.col_count, 0) >= t.expected_count THEN 'OK'
         WHEN ISNULL(actual.col_count, 0) = 0 THEN '** TABLE MISSING **'
         ELSE '** FEWER COLUMNS THAN EXPECTED **'
    END AS Status
FROM (VALUES
    ('CLARITY_EMP', 173), ('CLARITY_RPT', 25), ('COMPONENT_INFO', 65),
    ('DASHBOARD_INFO', 23), ('METRIC_INFO', 104), ('REPORT_INFO', 39),
    ('TEMPLATE_INFO', 59), ('TEMPLATE_DYNAMIC', 44), ('FILTER_DEFINITIONS', 35),
    ('RESOURCE_DISPLAY', 37), ('DATA_MODEL_DEFINITIONS', 29),
    ('COMPONENT_SUMMARY_INFO', 83), ('QUERY_DYNAMIC', 26),
    ('PROMPT_PARAMETERS', 11), ('EMP_BASIC_INFO', 16)
) AS t(expected_table, expected_count)
LEFT JOIN (
    SELECT TABLE_NAME, COUNT(*) AS col_count
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
    GROUP BY TABLE_NAME
) AS actual ON actual.TABLE_NAME = t.expected_table
ORDER BY t.expected_table;

PRINT '';
PRINT '=== Schema Verification Complete ===';
PRINT 'Any rows showing ** MISSING ** need investigation before running the extractor.';
