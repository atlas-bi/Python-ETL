/*******************************************************************************
 * Atlas ETL Migration — Clarity Friendly-Named Table DDL
 *
 * Creates pre-transformed raw_v2 tables for Clarity extractions.
 * These tables are populated by atlas_clarity_extractor.py with clean,
 * pre-joined subsets of the full Clarity source tables.
 *
 * Dependencies:
 *   - Schema: raw_v2 (created by 02_usp_Atlas_Setup.sql)
 *
 * Run order: After 02_usp_Atlas_Setup.sql, before 12_usp_Atlas_Clarity.sql
 *
 * Author:  Larry Duren
 * Date:    March 2026
 * Version: 1.3
 *
 * Change Log:
 *   v1.3 2026-04-09 — Removed ClarityDepartments (no Merge consumers;
 *        Phase 2 friendly tables fully retired).
 ******************************************************************************/

USE Atlas_Staging;
GO

PRINT '=== Clarity Friendly-Named Raw Table DDL ===';
PRINT 'Start Time: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
GO

-- ============================================================================
-- 3. Metric Query definitions extracted from Clarity (EPICCLAPRD)
-- Source: SSIS ETL-Clarity "Clarity SQL" → raw.[clarity-metric-query]
-- ============================================================================
DROP TABLE IF EXISTS raw_v2.clarity_metric_query;
CREATE TABLE raw_v2.clarity_metric_query (
    [idn id] NVARCHAR(MAX) NULL,
    [name]   NVARCHAR(MAX) NULL,
    [query]  NVARCHAR(MAX) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
GO
PRINT 'Created raw_v2.clarity_metric_query';
GO

PRINT '';
PRINT '=== Clarity Friendly-Named DDL Complete ===';
PRINT 'End Time: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
PRINT 'Tables created: 1';
GO
