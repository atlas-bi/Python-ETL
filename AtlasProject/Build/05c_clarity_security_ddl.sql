/*******************************************************************************
 * Atlas ETL Migration — Clarity Security Group Table DDL
 *
 * Creates 9 raw_v2 tables for Epic security group staging.
 * These tables are populated by atlas_clarity_extractor.py and consumed by
 * the expanded usp_Atlas_Clarity (Option A staging transforms).
 *
 * Dependencies:
 *   - Schema: raw_v2 (created by 02_usp_Atlas_Setup.sql)
 *
 * Consumed by:
 *   - etl.usp_Atlas_Clarity (Epic group staging transforms)
 *
 * Run order: After 02_usp_Atlas_Setup.sql, before 12_usp_Atlas_Clarity.sql
 *
 * Author:  Larry Duren
 * Date:    March 2026
 * Version: 1.0
 ******************************************************************************/

USE Atlas_Staging;
GO

PRINT '=== Clarity Security Group Table DDL ===';
PRINT 'Start Time: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
GO

-- =============================================================================
-- 1. raw_v2.CLARITY_ECL — Security class definitions
-- Source: dbo.CLARITY_ECL on EPICCLAPRD
-- Used for: RW Security classes, Analytics Security classes
-- =============================================================================
IF OBJECT_ID('raw_v2.CLARITY_ECL', 'U') IS NOT NULL
    DROP TABLE raw_v2.CLARITY_ECL;
GO

CREATE TABLE raw_v2.CLARITY_ECL (
    ECL_ID              NUMERIC(18,0)   NULL,
    CLASSIFCTN_NAME     NVARCHAR(254)   NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.CLARITY_ECL';
GO

-- =============================================================================
-- 2. raw_v2.CLARITY_EMP_2 — Extended employee security fields
-- Source: dbo.CLARITY_EMP_2 on EPICCLAPRD (security columns only)
-- Used for: RW Security class assignment, Analytics ECL, Linkable Templates
-- =============================================================================
IF OBJECT_ID('raw_v2.CLARITY_EMP_2', 'U') IS NOT NULL
    DROP TABLE raw_v2.CLARITY_EMP_2;
GO

-- NOTE: LNK_SEC_TEMPLT_ID lives on CLARITY_EMP, not CLARITY_EMP_2.
-- The "Applied Linkable Templates" group staging query must JOIN
-- raw_v2.CLARITY_EMP (Extract 1) for that column.
CREATE TABLE raw_v2.CLARITY_EMP_2 (
    USER_ID             VARCHAR(18)     NULL,
    RW_CLASS_ID         NUMERIC(18,0)   NULL,
    ANALYTICS_ECL_ID    NUMERIC(18,0)   NULL,
    TEMPLT_DSPLY_TITLE  NVARCHAR(254)   NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.CLARITY_EMP_2';
GO

-- =============================================================================
-- 3. raw_v2.RW_SEC_RPTGRPS_BASE — Base RW access group assignments
-- Source: dbo.RW_SEC_RPTGRPS_BASE on EPICCLAPRD
-- Used for: Global Reporting Workbench access groups
-- =============================================================================
IF OBJECT_ID('raw_v2.RW_SEC_RPTGRPS_BASE', 'U') IS NOT NULL
    DROP TABLE raw_v2.RW_SEC_RPTGRPS_BASE;
GO

CREATE TABLE raw_v2.RW_SEC_RPTGRPS_BASE (
    USER_ID             VARCHAR(18)     NULL,
    CLTY_RPT_GRP_C      NUMERIC(18,0)   NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.RW_SEC_RPTGRPS_BASE';
GO

-- =============================================================================
-- 4. raw_v2.RW_SEC_RPTGRPS_ADDL — Additional/override RW access groups
-- Source: dbo.RW_SEC_RPTGRPS_ADDL on EPICCLAPRD
-- Used for: Override Reporting Workbench access groups
-- =============================================================================
IF OBJECT_ID('raw_v2.RW_SEC_RPTGRPS_ADDL', 'U') IS NOT NULL
    DROP TABLE raw_v2.RW_SEC_RPTGRPS_ADDL;
GO

CREATE TABLE raw_v2.RW_SEC_RPTGRPS_ADDL (
    USER_ID             VARCHAR(18)     NULL,
    RPT_GRP_C           NUMERIC(18,0)   NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.RW_SEC_RPTGRPS_ADDL';
GO

-- =============================================================================
-- 5. raw_v2.CLARITY_EMP_ROLE — Employee role assignments
-- Source: dbo.CLARITY_EMP_ROLE on EPICCLAPRD
-- Used for: User role group staging
-- =============================================================================
IF OBJECT_ID('raw_v2.CLARITY_EMP_ROLE', 'U') IS NOT NULL
    DROP TABLE raw_v2.CLARITY_EMP_ROLE;
GO

CREATE TABLE raw_v2.CLARITY_EMP_ROLE (
    USER_ID             VARCHAR(18)     NULL,
    DEFAULT_USER_ROLE   VARCHAR(192)    NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.CLARITY_EMP_ROLE';
GO

-- =============================================================================
-- 6. raw_v2.USER_ROLE — Role descriptor lookup
-- Source: dbo.USER_ROLE on EPICCLAPRD
-- Used for: Role name resolution
-- =============================================================================
IF OBJECT_ID('raw_v2.USER_ROLE', 'U') IS NOT NULL
    DROP TABLE raw_v2.USER_ROLE;
GO

CREATE TABLE raw_v2.USER_ROLE (
    USER_ROLE_ID        NUMERIC(18,0)   NULL,
    USER_ROLE_DESCRIPTOR NVARCHAR(254)  NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.USER_ROLE';
GO

-- =============================================================================
-- 7. raw_v2.CACHED_USER_TYPE — User type assignments
-- Source: dbo.CACHED_USER_TYPE on EPICCLAPRD
-- Used for: User type group staging
-- =============================================================================
IF OBJECT_ID('raw_v2.CACHED_USER_TYPE', 'U') IS NOT NULL
    DROP TABLE raw_v2.CACHED_USER_TYPE;
GO

CREATE TABLE raw_v2.CACHED_USER_TYPE (
    USER_ID             VARCHAR(18)     NULL,
    CACHED_USER_TYPE_C  NUMERIC(18,0)   NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.CACHED_USER_TYPE';
GO

-- =============================================================================
-- 8. raw_v2.ZC_USER_TYPES — User type category lookup
-- Source: dbo.ZC_USER_TYPES on EPICCLAPRD
-- Used for: User type name resolution
-- =============================================================================
IF OBJECT_ID('raw_v2.ZC_USER_TYPES', 'U') IS NOT NULL
    DROP TABLE raw_v2.ZC_USER_TYPES;
GO

CREATE TABLE raw_v2.ZC_USER_TYPES (
    USER_TYPES_C        NUMERIC(18,0)   NULL,
    NAME                NVARCHAR(254)   NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.ZC_USER_TYPES';
GO

-- =============================================================================
-- 9. raw_v2.BI_SEC_POINTS — Analytics BI security points
-- Source: dbo.BI_SEC_POINTS on EPICCLAPRD
-- Used for: Analytics security class SD access flag
-- =============================================================================
IF OBJECT_ID('raw_v2.BI_SEC_POINTS', 'U') IS NOT NULL
    DROP TABLE raw_v2.BI_SEC_POINTS;
GO

CREATE TABLE raw_v2.BI_SEC_POINTS (
    ECL_ID              NUMERIC(18,0)   NULL,
    BI_SEC_POINTS_C     INT             NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.BI_SEC_POINTS';
GO

PRINT '';
PRINT '=== Clarity Security Group DDL Complete ===';
PRINT 'End Time: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
PRINT 'Tables created: 9';
GO
