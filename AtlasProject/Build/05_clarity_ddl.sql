/*******************************************************************************
 * Atlas ETL Migration — Week 3: Clarity Extraction DDL
 *
 * Creates raw_v2 and stage_v2 tables for the 47 Clarity SQL extractions
 * and 8 CSV flat-file loads. Run once before executing usp_Atlas_Clarity.
 *
 * Dependencies:
 *   - Schemas: raw_v2, stage_v2 (created in Week 1 usp_Atlas_Setup)
 *   - Linked Server: [EPICCLAPRD] pointing to Clarity database
 *
 * Author:  Larry Duren
 * Date:    February 2026
 * Version: 3.0 (Week 3)
 ******************************************************************************/

USE Atlas_Staging;
GO

PRINT '=== Week 3: Clarity Extraction DDL ===';
PRINT 'Start Time: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
GO

-- =============================================================================
-- SECTION 1: RAW TABLES (Clarity SQL Extractions)
-- These land data directly from linked server queries against Epic Clarity.
-- =============================================================================

-- -------------------------------------------------------------------------
-- Group A: Core Clarity Tables (from Create Clarity tables.sql)
-- -------------------------------------------------------------------------

-- A1. TEMPLATE_INFO_2
DROP TABLE IF EXISTS raw_v2.TEMPLATE_INFO_2;
GO
CREATE TABLE raw_v2.TEMPLATE_INFO_2 (
    [REPORT_ID] [numeric](18, 0) NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [CRYSTAL_FILENAME] [varchar](254) NULL,
    [OUTPUT_FORMAT_C] [int] NULL,
    [ENTERPRISE_FOLDER] [varchar](50) NULL,
    [CONTEXT_ID] [numeric](18, 0) NULL,
    [REASON_NO_CONTEXT_C] [int] NULL
);
GO
PRINT 'Created raw_v2.TEMPLATE_INFO_2';
GO

-- A2. CLARITY_EMP
DROP TABLE IF EXISTS raw_v2.CLARITY_EMP;
GO
CREATE TABLE raw_v2.CLARITY_EMP (
    [USER_ID] [varchar](18) NULL,
    [NAME] [varchar](160) NULL,
    [PROV_ID] [varchar](18) NULL,
    [EPIC_EMP_ID] [varchar](18) NULL,
    [MC_DEPARTMENT_ID] [numeric](18, 0) NULL,
    [CR_USER_NAME] [varchar](254) NULL,
    [PB_DEF_CLS_NM] [varchar](40) NULL,
    [CONF_SEC_CLS_NM] [varchar](40) NULL,
    [DFLT_SEC_CLASS_C] [varchar](18) NULL,
    [EPR_SEC_CLASS_C] [varchar](200) NULL,
    [MR_CLASS_C] [varchar](18) NULL,
    [USER_CONFIG_ID] [varchar](18) NULL,
    [MC_DEF_SEC_LEVEL_C] [varchar](18) NULL,
    [RFL_DEF_CLS_C] [varchar](18) NULL,
    [IB_SEC_CLASS_ID] [varchar](18) NULL,
    [SHARED_SEC_CL_ID] [varchar](18) NULL,
    [CUST_SVC_DEF_CLS] [varchar](18) NULL,
    [DEL_STATUS_C] [int] NULL,
    [USER_NAME_EXT] [varchar](160) NULL,
    [USER_STATUS_C] [int] NULL,
    [ADDRESS] [varchar](254) NULL,
    [CITY] [varchar](60) NULL,
    [STATE_PROVINCE] [varchar](50) NULL,
    [ZIP_CODE] [varchar](50) NULL,
    [PHONE] [varchar](50) NULL,
    [LAST_PW_UPDATE] [datetime] NULL,
    [SQL_ECL_ID] [varchar](18) NULL,
    [ES_AUTH_ALLSA_YN] [varchar](1) NULL,
    [CAD0_OTH_DEP_C] [int] NULL,
    [CAD0_DEPARTMENT_ID] [numeric](18, 0) NULL,
    [CAD1_OTH_DEP_C] [int] NULL,
    [CAD1_DEPARTMENT_ID] [numeric](18, 0) NULL,
    [ES_DSKTP_ACSS_YN] [varchar](1) NULL,
    [ES_RPT_SEC_PNT_C] [int] NULL,
    [CT_DFLT_CLS_C] [varchar](18) NULL,
    [CT_DSKTP_ACSS_YN] [varchar](1) NULL,
    [AR_DFLT_FACLTY_C] [int] NULL,
    [AR_DF_SERV_AREA_ID] [numeric](18, 0) NULL,
    [AR_DFLT_LOC_ID] [numeric](18, 0) NULL,
    [AR_DEPARTMENT_ID] [numeric](18, 0) NULL,
    [DFLT_ECL_ID] [varchar](18) NULL,
    [MR_RESTR_ACCS_YN] [varchar](1) NULL,
    [ENBL_RSLT_REV_YN] [varchar](1) NULL,
    [PRF_LST_PX_C] [int] NULL,
    [PRF_LST_COMDX_C] [int] NULL,
    [PRF_LST_MEDS_C] [int] NULL,
    [PRF_LST_RFV_C] [int] NULL,
    [MPI_SEC_CLS_C] [varchar](18) NULL,
    [MAIL_SYSTEM_C] [int] NULL,
    [LGIN_DEPARTMENT_ID] [numeric](18, 0) NULL,
    [EL_ACCS_C] [int] NULL,
    [EW_USER_CLS_C] [varchar](18) NULL,
    [CR_DFLT_ECL_ID] [varchar](18) NULL,
    [APCLM_DEF_ECL_ID] [varchar](18) NULL,
    [CASE_DEF_ECL_ID] [varchar](18) NULL,
    [AP_DEF_ECL_ID] [varchar](18) NULL,
    [CAPRR_DEF_ECL_ID] [varchar](18) NULL,
    [CAPPAY_DEF_ECL_ID] [varchar](18) NULL,
    [LAST_ACCS_DATETIME] [datetime] NULL,
    [DFLT_LOC_YN] [varchar](1) NULL,
    [RESTR_ACCS_REV_YN] [varchar](1) NULL,
    [LAST_USER_ID] [varchar](18) NULL,
    [MILLIMAN_USA_UNAME] [varchar](255) NULL,
    [MR_LOGON_DEPT_ID] [numeric](18, 0) NULL,
    [CAD_PRV_OTH_DPT_YN] [varchar](254) NULL,
    [CAD_GUI_BKDROP_YN] [varchar](254) NULL,
    [CAD_GUI_FRM_SZE_C] [int] NULL,
    [IS_DFLT_DEPT_YN] [varchar](254) NULL,
    [DISPLAY_ERR_RPT_C] [varchar](254) NULL,
    [IS_COLLECTOR_YN] [varchar](254) NULL,
    [USER_ALIAS] [varchar](254) NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [SYSTEM_LOGIN] [varchar](254) NULL,
    [RPT_GRP_ONE] [varchar](254) NULL,
    [RPT_GRP_TWO] [varchar](254) NULL,
    [RPT_GRP_THREE] [varchar](254) NULL,
    [LAB_DEFAULT_ECL_ID] [varchar](18) NULL,
    [LAB_WORKBENCH_YN] [varchar](254) NULL,
    [SUPERVISOR_YN] [varchar](1) NULL,
    [LICENSE_USRTYPE_C] [int] NULL,
    [EW_ACCS_C] [int] NULL,
    [EL_ACCS_PP_GRP_YN] [varchar](1) NULL,
    [EL_NOTFY_EMAIL_YN] [varchar](1) NULL,
    [EL_DAYS_BTN_EMAIL] [int] NULL,
    [EL_LAST_EMAIL_DT] [datetime] NULL,
    [EL_GRP_NOTIFY_YN] [varchar](1) NULL,
    [EL_USR_AFFECT_YN] [varchar](1) NULL,
    [EL_TRMS_ACPT_INST] [datetime] NULL,
    [EL_ACCS_PROG_PNT] [varchar](254) NULL,
    [WEB_EXT_IDENTIFIER] [varchar](254) NULL,
    [ME_PORTAL_DEF_ID] [varchar](184) NULL,
    [ST_PASTE_C] [int] NULL,
    [ME_ADMIN_FLAG_YN] [varchar](1) NULL,
    [ME_ACCESS_C] [int] NULL,
    [FORCE_PWD_CHANGE_YN] [varchar](1) NULL,
    [OR_SYSTEM_CLASS_ID] [varchar](18) NULL,
    [RX_SEC_CLASS_ID] [varchar](18) NULL,
    [OR_DEF_LOC_SECUR_ID] [varchar](18) NULL,
    [INP_EMR_SEC_CLS_ID] [varchar](18) NULL,
    [DFLT_ACCT_WQ_ID] [numeric](18, 0) NULL,
    [SP_FAC_YN] [varchar](1) NULL,
    [SP_AFF_YN] [varchar](1) NULL,
    [SP_OUT_YN] [varchar](1) NULL,
    [HB_DFLT_LGN_DEP_ID] [numeric](18, 0) NULL,
    [CDT_DFLT_ECL_ID] [varchar](18) NULL,
    [ROI_DFLT_ECL_ID] [varchar](18) NULL,
    [NTCM_DFLT_ECL_ID] [varchar](18) NULL,
    [HH_DFLT_ECL_ID] [varchar](18) NULL,
    [ER_DFLT_ECL_ID] [varchar](18) NULL,
    [LAB_DFLT_TST_ECL_ID] [varchar](18) NULL,
    [PEAR_DFLT_ECL_ID] [varchar](18) NULL,
    [OB_DFLT_ECL_ID] [varchar](18) NULL,
    [CHSYNC_DFLT_ECL_ID] [varchar](18) NULL,
    [CDA_DFLT_ECL_ID] [varchar](18) NULL,
    [CTM_DFLT_ECL_ID] [varchar](18) NULL,
    [CE_DFLT_ECL_ID] [varchar](18) NULL,
    [HNDHLD_DFLT_ECL_ID] [varchar](18) NULL,
    [PRM_BIL_DFLT_ECL_ID] [varchar](18) NULL,
    [CONF_DFLT_ECL_ID] [varchar](18) NULL,
    [MR_INIT_PRAC_C] [varchar](66) NULL,
    [ADT_DFLT_ECL_ID] [varchar](18) NULL,
    [EDI_DFLT_ECL_ID] [varchar](18) NULL,
    [HB_DFLT_ECL_ID] [varchar](18) NULL,
    [RIS_DFLT_ECL_ID] [varchar](18) NULL,
    [CARD_DFLT_ECL_ID] [varchar](18) NULL,
    [EMFI_DFLT_ECL_ID] [varchar](18) NULL,
    [DC_DFLT_ECL_ID] [varchar](18) NULL,
    [LOGIN_BLOCKED_C] [int] NULL,
    [EMP_RECORD_TYPE_C] [int] NULL,
    [LNK_SEC_TEMPLT_ID] [varchar](18) NULL,
    [LAB_BIL_DFLT_ECL_ID] [varchar](18) NULL,
    [LOGIN_BLOCKED_C_CMT] [varchar](254) NULL,
    [PPL_TYPE_C] [int] NULL,
    [CM_DFLT_SPT_USER_ID] [varchar](18) NULL,
    [CM_DFLT_SPT_POOL_ID] [numeric](18, 0) NULL,
    [CM_DFLT_SPT_FREETXT] [varchar](254) NULL,
    [MR_ADMIN_VIEW_ONLY] [int] NULL,
    [SYS_OVERV_YN] [varchar](1) NULL,
    [FOCUS_DEPT_FIELD_YN] [varchar](1) NULL,
    [DFLT_SCHED_VIEW_C] [int] NULL,
    [OR_DEF_CASE_LOC_ID] [numeric](18, 0) NULL,
    [OR_DEF_CASE_SVC_C] [varchar](66) NULL,
    [OR_DEF_CASE_SRGN_ID] [varchar](18) NULL,
    [USR_LOG_OR_SCRPT_ID] [varchar](18) NULL,
    [RFL_REP_SECPT_C] [int] NULL,
    [LAB_LAST_DRAWTYPE_C] [int] NULL,
    [SHOW_UNREL_RES_YN] [varchar](1) NULL,
    [SHOW_ERR_RPT_C] [int] NULL,
    [PTSRCH_DEF_FAC_YN] [varchar](1) NULL,
    [IS_SUP_PROV_REQ_C] [int] NULL,
    [FAC_FILTER_TYPE_C] [int] NULL,
    [LAB_RESCREEN_FACTOR] [numeric](6, 2) NULL,
    [IGNORE_LIGHT_M_YN] [varchar](1) NULL,
    [HIM_REP_SEC_PT_C] [int] NULL,
    [HIM_ADMIN_ACCESS_YN] [varchar](1) NULL,
    [PREF_LIST_SET_ORX_C] [int] NULL,
    [DFLT_PROB_PRI_C] [int] NULL,
    [PROB_PRI_OFF_PREF_C] [int] NULL,
    [PROB_PRI_ON_PREF_C] [int] NULL,
    [PROB_LST_PREF_L_ID] [numeric](18, 0) NULL,
    [USR_SORT_PROB_YN] [varchar](1) NULL,
    [LAST_PATIENT_LIST] [varchar](254) NULL,
    [IP_UNIQUE_ID] [varchar](192) NULL,
    [IPEMR_DEF_RESTR_YN] [varchar](1) NULL,
    [IP_DEF_RES_ACC_YN] [varchar](1) NULL,
    [IP_PATLST_DEFLST_ID] [varchar](18) NULL,
    [PTSRCH_SHOW_LST_YN] [varchar](1) NULL,
    [DSB_FRM_SEC_ID] [varchar](18) NULL,
    [EFF_FROM_DATE] [datetime] NULL,
    [EFF_TO_DATE] [datetime] NULL,
    [DEACTIVATE_DAYS] [int] NULL,
    [CAD_OTH_DEP_ECL_ID] [varchar](18) NULL
);
GO
PRINT 'Created raw_v2.CLARITY_EMP';
GO

-- A3. CLARITY_RPT
DROP TABLE IF EXISTS raw_v2.CLARITY_RPT;
GO
CREATE TABLE raw_v2.CLARITY_RPT (
    [REPORT_ID] [numeric](18, 0) NOT NULL,
    [REPORT_NAME] [varchar](200) NULL,
    [REPORT_DESC_ONE] [varchar](255) NULL,
    [REPORT_DESC_TWO] [varchar](255) NULL,
    [REPORT_TYPE] [varchar](60) NULL,
    [REPORT_SELECT_TYPE] [varchar](20) NULL,
    [INPUT_FILE_NAME] [varchar](254) NULL,
    [INFO_FOLDER] [varchar](254) NULL,
    [SELECTION_STRING] [varchar](60) NULL,
    [REPORT_OUTPUT_FMT] [varchar](40) NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [ASSOC_REPORT_ID] [numeric](18, 0) NULL,
    [REPORT_TYPE_C] [varchar](66) NULL,
    [REPORT_OVRD_DB_CONN_ID] [numeric](18, 0) NULL,
    [RECORD_STATUS_C] [int] NULL,
    [REPORT_CLASS_C] [varchar](66) NULL,
    [OVR_SUB_INACT_DAYS] [int] NULL,
    [OVR_OUTPUT_FORMAT_C] [int] NULL,
    [OVR_FREQ_C] [varchar](66) NULL,
    [OVR_PRIORITY_C] [int] NULL,
    [OVR_DAYS_KEEP_INST] [int] NULL,
    [OVR_PUBLISHED_FOLDER] [varchar](50) NULL,
    [OVR_USE_RPT_LOGIN_YN] [varchar](1) NULL,
    [OVR_HRS_KEEP_RSLT] [int] NULL,
    [HIDE_FROM_LIBRARY_YN] [varchar](1) NULL
);
GO
PRINT 'Created raw_v2.CLARITY_RPT';
GO

-- A4. CLARITY_RPT_GROUPS
DROP TABLE IF EXISTS raw_v2.CLARITY_RPT_GROUPS;
GO
CREATE TABLE raw_v2.CLARITY_RPT_GROUPS (
    [REPORT_ID] [numeric](18, 0) NOT NULL,
    [LINE] [int] NOT NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [REPORT_GROUP_C] [varchar](66) NULL
);
GO
PRINT 'Created raw_v2.CLARITY_RPT_GROUPS';
GO

-- A5. COMPONENT_DESC
DROP TABLE IF EXISTS raw_v2.COMPONENT_DESC;
GO
CREATE TABLE raw_v2.COMPONENT_DESC (
    [COMPONENT_ID] [numeric](18, 0) NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [RECORD_DESC] [varchar](1024) NULL
);
GO
PRINT 'Created raw_v2.COMPONENT_DESC';
GO

-- A6. COMPONENT_INFO
DROP TABLE IF EXISTS raw_v2.COMPONENT_INFO;
GO
CREATE TABLE raw_v2.COMPONENT_INFO (
    [COMPONENT_ID] [numeric](18, 0) NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [COMPONENT_NAME] [varchar](200) NULL,
    [RECORD_STATUS_C] [int] NULL,
    [DISPLAY_FORMAT_C] [int] NULL,
    [DATA_SOURCE_C] [int] NULL,
    [RECORD_TYPE_C] [int] NULL,
    [PARENT_COMPON_ID] [numeric](18, 0) NULL,
    [PARENT_COMPON_UNIQ] [varchar](50) NULL,
    [OWNING_APPL_ID] [numeric](18, 0) NULL,
    [READY_FOR_USE_YN] [varchar](1) NULL,
    [AVAIL_TO_USER_YN] [varchar](1) NULL,
    [USER_ID] [varchar](18) NULL,
    [DASHBOARD_ID] [numeric](18, 0) NULL,
    [REFRESH_RATE] [int] NULL,
    [SHOW_REFR_BTTN_YN] [varchar](1) NULL,
    [RPT_TEMPLATE_C] [varchar](66) NULL,
    [SHOW_RPT_COMP_TM_YN] [varchar](1) NULL,
    [VIEW_REPORT_YN] [varchar](1) NULL,
    [RUN_REPORT_YN] [varchar](1) NULL,
    [EXP_RSLT_YN] [varchar](1) NULL,
    [ENABLE_DRILLDOWN_YN] [varchar](1) NULL,
    [REPORT_ID] [numeric](18, 0) NULL,
    [EXTENSION_ID] [numeric](18, 0) NULL,
    [INIT_PARAM] [varchar](254) NULL,
    [SHOW_DATA_TIME_YN] [varchar](1) NULL,
    [CODE_TEMPLATE_ID] [numeric](18, 0) NULL,
    [EMBD_CONTENT_HEIGHT] [int] NULL,
    [EMBD_SRC_URL] [varchar](2048) NULL,
    [MULT_DATA_RES_C] [int] NULL,
    [PERIOD_INTERVAL_C] [int] NULL,
    [NUM_PERIODS] [int] NULL,
    [SUMMARY_LEVEL_C] [int] NULL,
    [DYN_SUM_LOCS_C] [int] NULL,
    [METRIC_TYPE_C] [int] NULL,
    [SHOW_YTD_YN] [varchar](1) NULL,
    [SHOW_QTR_TO_DT_YN] [varchar](1) NULL,
    [SHOW_MO_TO_DT_YN] [varchar](1) NULL,
    [SHOW_WEEK_TO_DT_YN] [varchar](1) NULL,
    [DISPLAY_TITLE] [varchar](254) NULL,
    [COMPON_COLOR_C] [int] NULL,
    [LAUNCH_ACTIVITY] [varchar](254) NULL,
    [SHOW_UPDATE_TIME_YN] [varchar](1) NULL,
    [HEADER_ICON] [varchar](254) NULL,
    [ACTIVITY_TOOLTIP] [varchar](254) NULL,
    [RECORD_CREATION_DT] [datetime] NULL,
    [INSTANT_OF_UPD_DTTM] [datetime] NULL,
    [SHOW_EXT_BENCHMRK_YN] [varchar](1) NULL,
    [SUMMARY_INDEX] [int] NULL,
    [PERIODS_IN_SPARKLN] [int] NULL,
    [INCLUDE_ZERO_IN_GRAPH_YN] [varchar](1) NULL,
    [ENABLE_BKGLOADING_YN] [varchar](1) NULL,
    [SHOW_TODAY_YN] [varchar](1) NULL,
    [SHOW_FISC_YTD_YN] [varchar](1) NULL,
    [RPT_TEMPLATE_ID] [numeric](18, 0) NULL,
    [OVERRIDE_GT_LABEL] [varchar](254) NULL,
    [OVERRIDE_GT_ROWS_YN] [varchar](1) NULL,
    [EXCL_XTD_SPARKLN_YN] [varchar](1) NULL,
    [MODEL_ID] [numeric](18, 0) NULL,
    [EXCL_FUT_DATA_YN] [varchar](1) NULL,
    [SLICERDICER_REPORT_INFO_ID] [numeric](18, 0) NULL,
    [MAX_HEIGHT] [int] NULL,
    [ACTIVITY] [varchar](140) NULL
);
GO
PRINT 'Created raw_v2.COMPONENT_INFO';
GO

-- A7. COMPONENT_LIST
DROP TABLE IF EXISTS raw_v2.COMPONENT_LIST;
GO
CREATE TABLE raw_v2.COMPONENT_LIST (
    [DASHBOARD_ID] [numeric](18, 0) NOT NULL,
    [LINE] [int] NOT NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [REGION] [int] NULL,
    [COMPONENT_UNIQ_ID] [varchar](50) NULL,
    [COMPONENT_ID] [numeric](18, 0) NULL,
    [TITLE_OVERRIDE] [varchar](254) NULL,
    [WIDTH] [numeric](18, 2) NULL,
    [MAX_HEIGHT] [int] NULL,
    [CAN_REMOVE_YN] [varchar](1) NULL,
    [CAN_EDIT_YN] [varchar](1) NULL,
    [CAN_COLLAPSE_YN] [varchar](1) NULL,
    [START_COLLAPSED_YN] [varchar](1) NULL,
    [COMPONENT_STATUS_C] [int] NULL,
    [OVERRIDE_COLOR_C] [int] NULL,
    [SOURCE_REGION] [int] NULL,
    [SOURCE_INDEX] [int] NULL
);
GO
PRINT 'Created raw_v2.COMPONENT_LIST';
GO

-- A8. DASHBOARD_DESC
DROP TABLE IF EXISTS raw_v2.DASHBOARD_DESC;
GO
CREATE TABLE raw_v2.DASHBOARD_DESC (
    [DASHBOARD_ID] [numeric](18, 0) NULL,
    [LINE] [int] NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [RECORD_DESC] [varchar](1000) NULL
);
GO
PRINT 'Created raw_v2.DASHBOARD_DESC';
GO

-- A9. DASHBOARD_INFO
DROP TABLE IF EXISTS raw_v2.DASHBOARD_INFO;
GO
CREATE TABLE raw_v2.DASHBOARD_INFO (
    [DASHBOARD_ID] [numeric](18, 0) NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [DASHBOARD_NAME] [varchar](200) NULL,
    [RECORD_STATUS_C] [int] NULL,
    [RECORD_TYPE_C] [int] NULL,
    [PARENT_DSHBD_ID] [numeric](18, 0) NULL,
    [OWNING_APPL_ID] [numeric](18, 0) NULL,
    [READY_FOR_USE_YN] [varchar](1) NULL,
    [ENABLED_YN] [varchar](1) NULL,
    [USER_ID] [varchar](18) NULL,
    [LAYOUT_ID] [numeric](18, 0) NULL,
    [CAN_ADD_COMPON_YN] [varchar](1) NULL,
    [NO_GRP_COMPON_YN] [varchar](1) NULL,
    [PRIM_PERSONLZTN_YN] [varchar](1) NULL,
    [DISPLAY_TITLE] [varchar](254) NULL,
    [RECORD_CREATION_DT] [datetime] NULL,
    [INSTANT_OF_UPD_DTTM] [datetime] NULL,
    [GROUP_COMPONENTS_YN] [varchar](1) NULL,
    [OVRIDE_STATUS_C] [int] NULL,
    [OVRIDE_CONTEXT] [varchar](62) NULL,
    [OVRIDE_PARENT_DB_ID] [numeric](18, 0) NULL,
    [SOURCE_LAYOUT_ID] [numeric](18, 0) NULL
);
GO
PRINT 'Created raw_v2.DASHBOARD_INFO';
GO

-- A11. FILTER_DEFINITIONS
DROP TABLE IF EXISTS raw_v2.FILTER_DEFINITIONS;
GO
CREATE TABLE raw_v2.FILTER_DEFINITIONS (
    [FILTER_ID] [numeric](18, 0) NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [FILTER_NAME] [varchar](200) NULL,
    [RECORD_STATUS_C] [int] NULL,
    [OVERRIDE_REC_STS_C] [int] NULL,
    [OVERRIDE_CONTEXT_C] [int] NULL,
    [BASE_RECORD_ID] [numeric](18, 0) NULL,
    [OVERRIDE_CMPL_INST_DTTM] [datetime] NULL,
    [FILTER_CAT_C] [int] NULL,
    [FILTER_DATA_TYPE_C] [int] NULL,
    [FILTER_LIST_YN] [varchar](1) NULL,
    [FILTER_LOWER_BOUND] [numeric](18, 2) NULL,
    [FILTER_UPPER_BOUND] [numeric](18, 2) NULL,
    [GRANULARITY] [numeric](18, 2) NULL,
    [OVERTIME_YN] [varchar](1) NULL,
    [COPY_FORWARD_YN] [varchar](1) NULL,
    [SENTENCE_PREFIX] [varchar](254) NULL,
    [SENTENCE_PREFIX_NEG] [varchar](254) NULL,
    [SENTENCE_PREFIX_PAST] [varchar](254) NULL,
    [SENTENCE_PREFIX_PAST_NEG] [varchar](254) NULL,
    [SENTENCE_PREFIX_PROG] [varchar](254) NULL,
    [OPTION_LIST_TABLE] [varchar](254) NULL,
    [OPTION_LIST_COLUMN] [varchar](254) NULL,
    [TABLE_PARAM_TYPE] [varchar](254) NULL,
    [WAREHOUSE_TABLE_NAME_C] [int] NULL,
    [RECORD_CREATION_DT] [datetime] NULL,
    [INSTANT_OF_UPDATE_DTTM] [datetime] NULL,
    [UNIT] [varchar](100) NULL,
    [FILTER_INACTIVE_YN] [varchar](1) NULL,
    [METRIC_ID] [varchar](18) NULL,
    [FILTER_NUMBER] [numeric](18, 0) NULL,
    [NOADD_YN] [varchar](1) NULL,
    [FLO_MEAS_ID] [varchar](18) NULL,
    [LOOKUP_TYPE_NAME] [varchar](254) NULL
);
GO
PRINT 'Created raw_v2.FILTER_DEFINITIONS';
GO

-- A12. METRIC_DESC
DROP TABLE IF EXISTS raw_v2.METRIC_DESC;
GO
CREATE TABLE raw_v2.METRIC_DESC (
    [DEFINITION_ID] [numeric](18, 0) NULL,
    [LINE] [int] NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [RECORD_DESC] [varchar](254) NULL
);
GO
PRINT 'Created raw_v2.METRIC_DESC';
GO

-- A13. METRIC_INFO
DROP TABLE IF EXISTS raw_v2.METRIC_INFO;
GO
CREATE TABLE raw_v2.METRIC_INFO (
    [DEFINITION_ID] [numeric](18, 0) NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [METRIC_NAME] [varchar](200) NULL,
    [RECORD_STATUS_C] [int] NULL,
    [OVERRIDE_REC_STS_C] [int] NULL,
    [OVERRIDE_CONTEXT_C] [varchar](62) NULL,
    [PARENT_RECORD_ID] [numeric](18, 0) NULL,
    [OVERRIDE_COMP_DTTM] [datetime] NULL,
    [DISPLAY_TITLE] [varchar](200) NULL,
    [RESULT_TYPE_C] [int] NULL,
    [UNIT_TYPE_C] [int] NULL,
    [COLL_MTHD_C] [int] NULL,
    [CONFIG_TYPE_C] [int] NULL,
    [RECORD_TYPE_C] [int] NULL,
    [OWNING_APPL_ID] [numeric](18, 0) NULL,
    [LOWER_VALUE_GOOD_C] [int] NULL,
    [ACTIVE_YN] [varchar](1) NULL,
    [EXT_AGG_SUPPORT_YN] [varchar](1) NULL,
    [EXT_BNCH_FOR_DEF_ID] [numeric](18, 0) NULL,
    [RECORD_CREATION_DT] [datetime] NULL,
    [INST_OF_UPDATE_DTTM] [datetime] NULL,
    [QM_ID] [numeric](18, 0) NULL,
    [COPY_FROM_DEF_ID] [numeric](18, 0) NULL,
    [DATA_NOT_EXTRCT_YN] [varchar](1) NULL,
    [ADT_EXCL_RULE_ID] [varchar](18) NULL,
    [ADT_EXCL_EXT_ID] [numeric](18, 0) NULL,
    [ADT_EVAL_EXT_ID] [numeric](18, 0) NULL,
    [CSF_FACT_TABLE] [varchar](192) NULL,
    [CSF_AGG_PROC_ID] [numeric](18, 0) NULL,
    [MIN_AGE] [int] NULL,
    [MAX_AGE] [int] NULL,
    [IP_READMIT_INDEX_DX_ID] [varchar](18) NULL,
    [IP_READMIT_PLAN_PROC_ID] [varchar](18) NULL,
    [IP_READMIT_UNPLAN_DX_ID] [varchar](18) NULL,
    [IP_READMIT_SNGL_DAY_YN] [varchar](1) NULL,
    [ADT_DEP_CALC_MTHD_C] [int] NULL,
    [ADT_USR_CALC_MTHD_C] [int] NULL,
    [FACILITY_EXCL_YN] [varchar](1) NULL,
    [MPM_EVAL_RULE_ID] [varchar](18) NULL,
    [MPM_MEAS_GROUP_C] [int] NULL,
    [SCALE_FACTOR] [numeric](14, 6) NULL,
    [DEF_LOAD_EXT_ID] [numeric](18, 0) NULL,
    [ROLLUP_MTHD_C] [int] NULL,
    [EXT_AGG_CONTEXT_C] [int] NULL,
    [QM_SUB_MEASURE_ID] [varchar](256) NULL,
    [QM_VERSION] [int] NULL,
    [OM_SUM_TYPE_C] [int] NULL,
    [NEAR_DUE_THRESH] [int] NULL,
    [PAST_DUE_THRESH] [int] NULL,
    [LEVEL_CONFIG_LPP_ID] [numeric](18, 0) NULL,
    [BENCHMARK_SOURCE_C] [int] NULL,
    [RX_EXCL_EXT_ID] [numeric](18, 0) NULL,
    [RX_EVAL_EXT_ID] [numeric](18, 0) NULL,
    [RX_EVAL_ACT_C] [int] NULL,
    [IP_READMIT_PLAN_OR_PROC_ID] [varchar](18) NULL,
    [IP_READMIT_PLAN_ICD_PROC_ID] [varchar](18) NULL,
    [RIS_NQMBC_QM_C] [varchar](66) NULL,
    [RIS_NQMBC_RANGE_LOW] [int] NULL,
    [RIS_NQMBC_RANGE_HIGH] [int] NULL,
    [FEATURE_TRACKING_DEFINITION_ID] [numeric](18, 0) NULL,
    [NUMERATOR_LOGIC] [varchar](100) NULL,
    [DENOMINATOR_LOGIC] [varchar](100) NULL,
    [IP_METRIC_WINDOW_DAYS] [int] NULL,
    [IP_READMIT_ALWAYS_PLAN_DX_ID] [varchar](18) NULL,
    [EXT_WINDOW_ALLOW_OVERRIDES_YN] [varchar](1) NULL,
    [LOOKBACK_WINDOW] [varchar](10) NULL,
    [AUTO_SQL_TABLE_TYPE_C] [int] NULL,
    [AUTO_SQL_RUN_GROUP_ID] [numeric](18, 0) NULL,
    [AUTO_SQL_FACT_TABLE_NAME] [varchar](128) NULL,
    [AUTO_SQL_FILTER_EXPRESSION] [varchar](508) NULL,
    [AUTO_SQL_NUMER_AGGN_FUNCTION_C] [int] NULL,
    [AUTO_SQL_NUMER_EXPRESSION] [varchar](508) NULL,
    [AUTO_SQL_DENOM_AGGN_FUNCTION_C] [int] NULL,
    [AUTO_SQL_DENOM_EXPRESSION] [varchar](508) NULL,
    [AUTO_SQL_DATE_EXPRESSION] [varchar](508) NULL,
    [GOAL_ENABLE_YN] [varchar](1) NULL,
    [GOAL] [numeric](18, 2) NULL,
    [HIDE_DEPT_SCORE_YN] [varchar](1) NULL,
    [METRIC_GROUPING_C] [int] NULL,
    [BACKGROUND_ONLY_YN] [varchar](1) NULL,
    [SCALE_MIN] [numeric](18, 2) NULL,
    [SCALE_MAX] [numeric](18, 2) NULL,
    [TEMPLATE_C] [int] NULL,
    [HP_EVAL_RULE_ID] [varchar](18) NULL,
    [EXCLUSIONS_DEFINITION_ID] [numeric](18, 0) NULL,
    [JOB_CONFIGURATION_ID] [numeric](18, 0) NULL,
    [ADT_CLS_CALC_MTHD_C] [int] NULL,
    [BACKFILL_WINDOW] [varchar](10) NULL,
    [IP_MORTALITY_INDEX_DX_ID] [varchar](18) NULL,
    [IP_MORTALITY_METRIC_TYPE_C] [int] NULL,
    [NUM_UNIT_TYPE_C] [int] NULL,
    [DEN_UNIT_TYPE_C] [int] NULL,
    [REG_FAC_ROLLUP_TYPE_C] [int] NULL,
    [REG_TIME_ACT_TOT_C] [int] NULL,
    [IP_MIN_INDEX_LENGTH_OF_STAY] [int] NULL,
    [ES_INCL_OR_EXCL_PROV_C] [int] NULL,
    [ES_CNT_ADDL_ENC_TYPE_YN] [varchar](1) NULL,
    [IP_METRIC_EXCLUSION_DX_ID] [varchar](18) NULL,
    [READMIT_ALGORITHM_METHOD_C] [int] NULL,
    [IP_PRIMARY_DX_GROUPER_ID] [varchar](18) NULL,
    [IP_SECONDARY_DX_GROUPER_ID] [varchar](18) NULL,
    [IP_SECONDARY_DX_POA_YN] [varchar](1) NULL,
    [SQL_SOURCE_DATABASE_C] [int] NULL
);
GO
PRINT 'Created raw_v2.METRIC_INFO';
GO

-- A14. OVRIDE_RPT_GROUPS
DROP TABLE IF EXISTS raw_v2.OVRIDE_RPT_GROUPS;
GO
CREATE TABLE raw_v2.OVRIDE_RPT_GROUPS (
    [REPORT_ID] [numeric](18, 0) NOT NULL,
    [LINE] [int] NOT NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [REPORT_GROUP_C] [varchar](66) NULL
);
GO
PRINT 'Created raw_v2.OVRIDE_RPT_GROUPS';
GO

-- A15. REPORT_DESC
DROP TABLE IF EXISTS raw_v2.REPORT_DESC;
GO
CREATE TABLE raw_v2.REPORT_DESC (
    [REPORT_INFO_ID] [numeric](18, 0) NULL,
    [LINE] [int] NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [REPORT_DESCRIPTION] [varchar](3500) NULL
);
GO
PRINT 'Created raw_v2.REPORT_DESC';
GO

-- A16. REPORT_INFO
DROP TABLE IF EXISTS raw_v2.REPORT_INFO;
GO
CREATE TABLE raw_v2.REPORT_INFO (
    [REPORT_INFO_ID] [numeric](18, 0) NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [REPORT_INFO_NAME] [varchar](200) NULL,
    [RPT_TYPE_C] [varchar](66) NULL,
    [PRIVATE_OR_PUBLIC_C] [int] NULL,
    [TEMP_REPORT_C] [int] NULL,
    [CURRENT_DEPT_YN] [varchar](1) NULL,
    [ANCHORED_COLUMNS] [int] NULL,
    [RECORD_TYPE_C] [int] NULL,
    [PARENT_TEMPLATE_ID] [numeric](18, 0) NULL,
    [LOGIC] [varchar](254) NULL,
    [END_DATE_STRING] [varchar](12) NULL,
    [START_DATE_STRING] [varchar](12) NULL,
    [END_TIME_STRING] [varchar](20) NULL,
    [START_TIME_STRING] [varchar](20) NULL,
    [REPORT_ID] [numeric](18, 0) NULL,
    [OWNED_BY_USER_ID] [varchar](18) NULL,
    [LAST_MOD_BY_USER_ID] [varchar](18) NULL,
    [LAST_RUN_BY_USER_ID] [varchar](18) NULL,
    [LOAD_ALL_YN] [varchar](1) NULL,
    [OVRIDE_OUTPUT_FMT_C] [int] NULL,
    [OVRIDE_RUN_FREQ_C] [varchar](66) NULL,
    [OVRIDE_BOE_FLDR] [varchar](50) NULL,
    [OVRIDE_CACHE_EXP] [int] NULL,
    [OVRIDE_REQ_PRI_C] [int] NULL,
    [OVRIDE_DAYS_KEEP] [int] NULL,
    [OVRIDE_DAYS_BEFORE_SUB_INACT] [int] NULL,
    [RECORD_STATE_C] [int] NULL,
    [CREATED_BY_USER_ID] [varchar](18) NULL,
    [INST_OF_CREATION_DTTM] [datetime] NULL,
    [INST_OF_LAST_MOD_DTTM] [datetime] NULL,
    [BI_FIX_FLAG_C] [int] NULL,
    [BI_LAST_BATCH_GUID] [varchar](36) NULL,
    [OVRIDE_SEARCH_RECS] [int] NULL,
    [OVRIDE_FIND_RECS] [int] NULL,
    [HAS_ABS_DATES_YN] [varchar](1) NULL,
    [HAS_STYLE_OVERRIDES_YN] [varchar](1) NULL,
    [HIDE_FROM_LIBRARY_YN] [varchar](1) NULL
);
GO
PRINT 'Created raw_v2.REPORT_INFO';
GO

-- A17. DATA_MODEL_DEFINITIONS
DROP TABLE IF EXISTS raw_v2.DATA_MODEL_DEFINITIONS;
GO
CREATE TABLE raw_v2.DATA_MODEL_DEFINITIONS (
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [DATA_MODEL_ID] [numeric](18, 0) NULL,
    [RECORD_NAME] [varchar](200) NULL,
    [RECORD_STATUS_C] [int] NULL,
    [OVERRIDE_REC_STAT_C] [int] NULL,
    [OVERRIDE_CONTEXT_C] [int] NULL,
    [BASE_RECORD_ID] [nvarchar](max) NULL,
    [OVERRIDE_COMP_INST_DTTM] [datetime] NULL,
    [INACTIVE_YN] [varchar](1) NULL,
    [ROOT_WAREHOUSE_TAB] [varchar](100) NULL,
    [ROOT_WAREHOUSE_COL] [varchar](508) NULL,
    [ADDL_WHERE_CLAUSE] [varchar](508) NULL,
    [SERV_AREA_FILTER_ID] [numeric](18, 0) NULL,
    [SERVICE_AREA_TAB] [varchar](100) NULL,
    [SERVICE_AREA_ID_COL] [varchar](100) NULL,
    [SERV_AREA_USERID_COL] [varchar](100) NULL,
    [SERV_AREA_REQUIRED_YN] [varchar](1) NULL,
    [LINEAGE_TABLE] [varchar](200) NULL,
    [LINEAGE_COLUMN] [varchar](200) NULL,
    [LINEAGE_INI] [varchar](3) NULL,
    [EPIC_LINEAGE_TYPE_C] [int] NULL,
    [DEFAULT_DATE_RANGE] [varchar](50) NULL,
    [LINK_DISPLAY_NAME] [varchar](254) NULL,
    [HUB_TABLE] [varchar](200) NULL,
    [ONLY_REPORT_GROUP_YN] [varchar](1) NULL,
    [INCL_UNSPECIFIED_SA_ID_YN] [varchar](1) NULL,
    [SUPPORTS_FUT_DATE_YN] [varchar](1) NULL
);
GO
PRINT 'Created raw_v2.DATA_MODEL_DEFINITIONS';
GO

-- A18. DATA_MODEL_DESCRIPTION
DROP TABLE IF EXISTS raw_v2.DATA_MODEL_DESCRIPTION;
GO
CREATE TABLE raw_v2.DATA_MODEL_DESCRIPTION (
    [DATA_MODEL_ID] [numeric](18, 0) NULL,
    [LINE] [int] NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [DATA_MODEL_DESC] [varchar](508) NULL
);
GO
PRINT 'Created raw_v2.DATA_MODEL_DESCRIPTION';
GO

-- -------------------------------------------------------------------------
-- Group B: Lookup, Security & Tags (from Create Clarity tables 2.sql)
-- -------------------------------------------------------------------------

-- B1. COMPONENT_SUMMARY_INFO
DROP TABLE IF EXISTS raw_v2.COMPONENT_SUMMARY_INFO;
GO
CREATE TABLE raw_v2.COMPONENT_SUMMARY_INFO (
    [COMPONENT_ID] [numeric](18, 0) NOT NULL,
    [LINE] [int] NOT NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [DATA_RESOURCES_ID] [numeric](18, 0) NULL
);
GO
PRINT 'Created raw_v2.COMPONENT_SUMMARY_INFO';
GO

-- ZC_RECORD_TYPE_24
DROP TABLE IF EXISTS raw_v2.ZC_RECORD_TYPE_24;
GO
CREATE TABLE raw_v2.ZC_RECORD_TYPE_24 (
    [RECORD_TYPE_24_C] [int] NULL,
    [NAME] [varchar](254) NULL,
    [TITLE] [varchar](254) NULL,
    [ABBR] [varchar](254) NULL,
    [INTERNAL_ID] [int] NULL
);
GO
PRINT 'Created raw_v2.ZC_RECORD_TYPE_24';
GO

-- ZC_REPORT_TYPE_HGR
DROP TABLE IF EXISTS raw_v2.ZC_REPORT_TYPE_HGR;
GO
CREATE TABLE raw_v2.ZC_REPORT_TYPE_HGR (
    [REPORT_TYPE_HGR_C] [int] NULL,
    [NAME] [varchar](254) NULL,
    [TITLE] [varchar](254) NULL,
    [ABBR] [varchar](254) NULL,
    [INTERNAL_ID] [int] NULL
);
GO
PRINT 'Created raw_v2.ZC_REPORT_TYPE_HGR';
GO

-- B4. ClarityUsernameLinks (no IDENTITY, no PK)
DROP TABLE IF EXISTS raw_v2.ClarityUsernameLinks;
GO
CREATE TABLE raw_v2.ClarityUsernameLinks (
    [user_Id] [nvarchar](255) NULL,
    [Name] [nvarchar](255) NULL,
    [domain_name] [nvarchar](255) NULL
);
GO
PRINT 'Created raw_v2.ClarityUsernameLinks';
GO

-- B5. ClarityUserGroups
DROP TABLE IF EXISTS raw_v2.ClarityUserGroups;
GO
CREATE TABLE raw_v2.ClarityUserGroups (
    [USER_ID] [nvarchar](18) NULL,
    [GroupName] [nvarchar](200) NULL,
    [GroupSource] [nvarchar](100) NULL,
    [GroupId] [nvarchar](50) NULL
);
GO
PRINT 'Created raw_v2.ClarityUserGroups';
GO

-- B6. ClarityDashboardRoles
DROP TABLE IF EXISTS raw_v2.ClarityDashboardRoles;
GO
CREATE TABLE raw_v2.ClarityDashboardRoles (
    [dashboard_id] [numeric](18,0) NULL,
    [user_roles] [nvarchar](max) NULL,
    [user_roles_id] [numeric](18,0) NULL
);
GO
PRINT 'Created raw_v2.ClarityDashboardRoles';
GO

-- B7. ClarityComponentGroups
DROP TABLE IF EXISTS raw_v2.ClarityComponentGroups;
GO
CREATE TABLE raw_v2.ClarityComponentGroups (
    [COMPONENT_ID] [nvarchar](42) NULL,
    [group_id] [nvarchar](67) NULL
);
GO
PRINT 'Created raw_v2.ClarityComponentGroups';
GO

-- B8. ClarityDashboardTypes
DROP TABLE IF EXISTS raw_v2.ClarityDashboardTypes;
GO
CREATE TABLE raw_v2.ClarityDashboardTypes (
    [dashboard_id] [numeric](18,0) NULL,
    [user_types] [nvarchar](66) NULL
);
GO
PRINT 'Created raw_v2.ClarityDashboardTypes';
GO

-- B9. CLARITY_LPP
DROP TABLE IF EXISTS raw_v2.CLARITY_LPP;
GO
CREATE TABLE raw_v2.CLARITY_LPP (
    [LPP_ID] numeric(18, 0) NULL,
    [LPP_NAME] varchar(254) NULL,
    [LPP_TYPE_C] int NULL,
    [M_CODE] varchar(4000) NULL,
    [COMMENTS] varchar(3000) NULL,
    [CM_PHY_OWNER_ID] varchar(25) NULL,
    [CM_LOG_OWNER_ID] varchar(25) NULL,
    [RECORD_STATE_C] int NULL,
    [TEMPLATE_ID] numeric(18, 0) NULL
);
GO
PRINT 'Created raw_v2.CLARITY_LPP';
GO

-- B10. LPP_COMMENTS
DROP TABLE IF EXISTS raw_v2.LPP_COMMENTS;
GO
CREATE TABLE raw_v2.LPP_COMMENTS (
    [LPP_ID] numeric(18, 0) NULL,
    [LINE] int NULL,
    [COMMENTS] varchar(max) NULL
);
GO
PRINT 'Created raw_v2.LPP_COMMENTS';
GO

-- B11. TAG_INFO
DROP TABLE IF EXISTS raw_v2.TAG_INFO;
GO
CREATE TABLE raw_v2.TAG_INFO (
    [tag_id] [numeric](18, 0) NULL,
    [tag_name] [varchar](200) NULL
);
GO
PRINT 'Created raw_v2.TAG_INFO';
GO

-- B12. REPORT_TAGS (source: ADDL_REPORT_TAGS in Clarity)
DROP TABLE IF EXISTS raw_v2.REPORT_TAGS;
GO
CREATE TABLE raw_v2.REPORT_TAGS (
    [report_id] [numeric](18, 0) NULL,
    [line] [int] NULL,
    [tag_id] [numeric](18, 0) NULL
);
GO
PRINT 'Created raw_v2.REPORT_TAGS';
GO

-- B13. COMPONENT_TAGS
DROP TABLE IF EXISTS raw_v2.COMPONENT_TAGS;
GO
CREATE TABLE raw_v2.COMPONENT_TAGS (
    [report_id] [numeric](18, 0) NULL,
    [line] [int] NULL,
    [tag_id] [numeric](18, 0) NULL
);
GO
PRINT 'Created raw_v2.COMPONENT_TAGS';
GO

-- B14. DASHBOARD_TAGS
DROP TABLE IF EXISTS raw_v2.DASHBOARD_TAGS;
GO
CREATE TABLE raw_v2.DASHBOARD_TAGS (
    [report_id] [numeric](18, 0) NULL,
    [line] [int] NULL,
    [tag_id] [numeric](18, 0) NULL
);
GO
PRINT 'Created raw_v2.DASHBOARD_TAGS';
GO

-- B15. TEMPLATE_TAGS
DROP TABLE IF EXISTS raw_v2.TEMPLATE_TAGS;
GO
CREATE TABLE raw_v2.TEMPLATE_TAGS (
    [report_id] [numeric](18, 0) NULL,
    [line] [int] NULL,
    [tag_id] [numeric](18, 0) NULL
);
GO
PRINT 'Created raw_v2.TEMPLATE_TAGS';
GO

-- B16. ASSOC_REPORT_GROUPS
DROP TABLE IF EXISTS raw_v2.ASSOC_REPORT_GROUPS;
GO
CREATE TABLE raw_v2.ASSOC_REPORT_GROUPS (
    [DASHBOARD_ID] [numeric](18, 0) NOT NULL,
    [LINE] [int] NOT NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [REPORT_GROUPS_C] [varchar](66) NULL
);
GO
PRINT 'Created raw_v2.ASSOC_REPORT_GROUPS';
GO

-- -------------------------------------------------------------------------
-- Group C: Templates, Parameters & Queues (from Create Clarity tables 3.sql)
-- -------------------------------------------------------------------------

-- C1. TEMPLATE_DESCRIPTION
DROP TABLE IF EXISTS raw_v2.TEMPLATE_DESCRIPTION;
GO
CREATE TABLE raw_v2.TEMPLATE_DESCRIPTION (
    [REPORT_ID] [numeric](18, 0) NULL,
    [CONTACT_DATE_REAL] [float] NULL,
    [LINE] [int] NULL,
    [CONTACT_DATE] [datetime] NULL,
    [SEARCH_SOURCE_DESC] [varchar](3500) NULL
);
GO
PRINT 'Created raw_v2.TEMPLATE_DESCRIPTION';
GO

-- C2. TEMPLATE_DYNAMIC
DROP TABLE IF EXISTS raw_v2.TEMPLATE_DYNAMIC;
GO
CREATE TABLE raw_v2.TEMPLATE_DYNAMIC (
    [REPORT_ID] [numeric](18, 0) NULL,
    [CONTACT_DATE_REAL] [float] NULL,
    [CONTACT_DATE] [datetime] NULL,
    [CONTACT_NUM] [varchar](254) NULL,
    [CM_CT_OWNER_ID] [varchar](25) NULL,
    [PARAMETER_VIEWER_ID] [numeric](18, 0) NULL,
    [INFO_REC_NAME_ID] [numeric](18, 0) NULL,
    [SUBSET_COLUMNID_ID] [numeric](18, 0) NULL,
    [SUBSET_COLUMNDAT_ID] [numeric](18, 0) NULL,
    [SUBSET_INI] [varchar](5) NULL,
    [BEFORE_ACT_PACK_YN] [varchar](1) NULL,
    [DESCRIPTION] [varchar](254) NULL,
    [PARAM_PROMPT_ID] [numeric](18, 0) NULL,
    [TEMPLATE_ID] [numeric](18, 0) NULL,
    [CACHE_EXP_IN] [int] NULL,
    [VIEW_EXP_RESULTS_YN] [varchar](1) NULL,
    [SEARCH_SOURCE_NAME] [varchar](254) NULL,
    [PRINT_CLASS_C] [varchar](66) NULL,
    [HELP_MODULE] [varchar](254) NULL,
    [HELP_FILE_C] [int] NULL,
    [SEARCH_HELP_ID] [varchar](254) NULL,
    [SEARCH_HLP_MODULE_C] [int] NULL,
    [VIEWER_HELP_ID] [varchar](254) NULL,
    [VIEWER_HLP_MODULE_C] [int] NULL,
    [REPORT_UI_C] [int] NULL,
    [SELECT_CRIT_YN] [varchar](1) NULL,
    [ALLOW_ADD_CRIT_YN] [varchar](1) NULL,
    [VIEWER_REPORT_ID] [varchar](18) NULL,
    [MATCH_LINE_ONLY_YN] [varchar](1) NULL,
    [ENABLE_CACHING_YN] [varchar](1) NULL,
    [EN_SEARCH_SUMM_YN] [varchar](1) NULL,
    [STATIC_COL_DATA_C] [int] NULL,
    [SHOW_EXPORT_YN] [varchar](1) NULL,
    [WRAP_TEXT_C] [int] NULL,
    [REP_LAYOUT_C] [varchar](66) NULL,
    [PRINT_VIEW_TAG_ID] [numeric](18, 0) NULL,
    [SPEC_VIEW_EXPEX_ID] [numeric](18, 0) NULL,
    [SETUP_DATA_PP_ID] [numeric](18, 0) NULL,
    [H_SUMM_TEMPLATE_ID] [numeric](18, 0) NULL,
    [ENABLE_PREV_YN] [varchar](1) NULL,
    [MAX_NUM_SEARCH] [int] NULL,
    [MAX_NUM_RETURN] [int] NULL,
    [PLAIN_TXT_PRN_CLS_C] [varchar](66) NULL,
    [VWR_RPT_BTG_VIEW_C] [varchar](66) NULL
);
GO
PRINT 'Created raw_v2.TEMPLATE_DYNAMIC';
GO

-- C3. TEMPLATE_INFO
DROP TABLE IF EXISTS raw_v2.TEMPLATE_INFO;
GO
CREATE TABLE raw_v2.TEMPLATE_INFO (
    [REPORT_ID] [numeric](18, 0) NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [REPORT_NAME] [varchar](254) NULL,
    [STATUS_C] [int] NULL,
    [SRC_TEMPL_ID] [numeric](18, 0) NULL,
    [UPD_VERSION_C] [numeric](8, 3) NULL,
    [REPORT_TYPE_C] [varchar](66) NULL,
    [REP_PAGE_PROG_ID] [varchar](254) NULL,
    [SHOW_DISPTAB_YN] [varchar](1) NULL,
    [DEF_REPSTG_YN] [varchar](1) NULL,
    [ENABLE_PRTBTN_YN] [varchar](1) NULL,
    [SHOW_RS_YN] [varchar](1) NULL,
    [SHOW_PRTTAB_YN] [varchar](1) NULL,
    [DEF_PRTDLG_YN] [varchar](1) NULL,
    [SUP_PRTLAYOUT_YN] [varchar](1) NULL,
    [AC_RW_SHARE_REP_YN] [varchar](1) NULL,
    [SHOW_SUMTAB_YN] [varchar](1) NULL,
    [SUP_RTFPRT_YN] [varchar](1) NULL,
    [CODE_BUILD_REP] [varchar](254) NULL,
    [PRINT_CODE] [varchar](254) NULL,
    [SUP_COLICONS_YN] [varchar](1) NULL,
    [SPT_ROWFONT_YN] [varchar](1) NULL,
    [ROW_OVERRIDE_YN] [varchar](1) NULL,
    [SUP_FILTERLPP_YN] [varchar](1) NULL,
    [SUP_REPBUILDR_YN] [varchar](1) NULL,
    [APP_TYPE_ID] [numeric](18, 0) NULL,
    [SUP_VCPROL_YN] [varchar](1) NULL,
    [REP_PAGE_PARAM] [varchar](254) NULL,
    [REP_PAGE_CAPTION] [varchar](254) NULL,
    [REPORT_TYPE_HGR_C] [int] NULL,
    [RUN_CAPTION] [varchar](254) NULL,
    [PUBLIC_ONLY_MODE_YN] [varchar](1) NULL,
    [SPRTS_HRCHY_YN] [varchar](1) NULL,
    [SPRTS_VIEWSCREENS_YN] [varchar](1) NULL,
    [RPT_SETUP_PROGID_ID] [numeric](18, 0) NULL,
    [USE_REPORT_LOGIN_YN] [varchar](1) NULL,
    [REP_PAGE_PROG_ID2_ID] [numeric](18, 0) NULL,
    [POST_RUN_LPP_ID] [numeric](18, 0) NULL,
    [SHOW_TLBRTAB_YN] [varchar](1) NULL,
    [SPT_WRPOVRD_YN] [varchar](1) NULL,
    [MESSAGE_AREA_TEXT] [varchar](4000) NULL,
    [REPORT_FREQ_C] [varchar](66) NULL,
    [REQPRIORITY_C] [int] NULL,
    [DAYS_KEEP_INSTANCE] [int] NULL,
    [WEBI_REPORT_CUID] [varchar](50) NULL,
    [WEBI_REPORT_NAME] [varchar](300) NULL,
    [ACTIVITY_DESCRIPTOR] [varchar](100) NULL,
    [WEB_ENABLED_YN] [varchar](1) NULL,
    [CRITERIA_VIEW] [varchar](254) NULL,
    [DEF_COLORS_FONTS] [varchar](254) NULL,
    [ANCHORED_COLUMNS] [int] NULL,
    [VERIFIED_CHECKSUM] [int] NULL,
    [TPL_CHECKSUM] [int] NULL,
    [ENABLE_EMFI_YN] [varchar](1) NULL,
    [CRYSTAL_DATAMODEL_C] [int] NULL,
    [SHOW_OVERRIDE_TAB_YN] [varchar](1) NULL,
    [NEED_RSLT_UPDATE_YN] [varchar](1) NULL,
    [EXTBI_VW_BEHAVIOR_C] [int] NULL
);
GO
PRINT 'Created raw_v2.TEMPLATE_INFO';
GO

-- C4. RESOURCE_DISPLAY
DROP TABLE IF EXISTS raw_v2.RESOURCE_DISPLAY;
GO
CREATE TABLE raw_v2.RESOURCE_DISPLAY (
    [RESOURCE_ID] [numeric](18, 0) NOT NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [RECORD_NAME] [varchar](250) NULL,
    [RECORD_STATUS_C] [int] NULL,
    [DISPLAY_TITLE] [varchar](200) NULL,
    [Y_AXIS_TITLE] [varchar](200) NULL,
    [OVERRIDE_REC_STS_C] [int] NULL,
    [OVERRIDE_CONTEXT] [varchar](62) NULL,
    [PARENT_RECORD_ID] [numeric](18, 0) NULL,
    [OVERRIDE_COMP_INST_DTTM] [datetime] NULL,
    [RECORD_TYPE_C] [int] NULL,
    [PARENT_REC_ID] [numeric](18, 0) NULL,
    [OWN_APP_ID] [numeric](18, 0) NULL,
    [FEATURE_TRK_DEF_ID] [numeric](18, 0) NULL,
    [METRIC_LOAD_EXT_ID] [numeric](18, 0) NULL,
    [DRILLDOWN_EXT_ID] [numeric](18, 0) NULL,
    [USE_THRESHOLDS_DD_YN] [varchar](1) NULL,
    [SHOW_ALL_TIERS_YN] [varchar](1) NULL,
    [RPT_TMP_SRC_ID] [numeric](18, 0) NULL,
    [REPORT_DATA_SRC_ID] [numeric](18, 0) NULL,
    [MODEL_DATA_SRC_ID] [numeric](18, 0) NULL,
    [REPORT_SUMMARY_IDX] [int] NULL,
    [SUMMARY_COLUMN_ID] [numeric](18, 0) NULL,
    [SUMMARY_FUNCTION_C] [int] NULL,
    [SUM_FUNC_NUM_AND_YN] [varchar](1) NULL,
    [SUM_FUNC_DEN_AND_YN] [varchar](1) NULL,
    [METRIC_DEF_ID] [numeric](18, 0) NULL,
    [METRIC_RESULT_PIECE_C] [int] NULL,
    [GOAL] [numeric](18, 2) NULL,
    [MUST_EXCEED_GOAL_YN] [varchar](1) NULL,
    [COL_DFLT_DISP_FMT_C] [int] NULL,
    [COL_NUM_DECIMALS] [int] NULL,
    [RND_MTHD_RESRC_C] [int] NULL,
    [DISAB_PEER_GROUP_YN] [varchar](1) NULL,
    [DISPLAY_OVRIDE_C] [int] NULL,
    [GROUP_HEADER_ID] [numeric](18, 0) NULL,
    [KEY_RES_GRP_ID] [numeric](18, 0) NULL,
    [RECORD_CREATION_DT] [datetime] NULL,
    [INSTANT_OF_UPDATE_DTTM] [datetime] NULL
);
GO
PRINT 'Created raw_v2.RESOURCE_DISPLAY';
GO

-- C5. DATA_MODEL_REPORT_GROUPS
DROP TABLE IF EXISTS raw_v2.DATA_MODEL_REPORT_GROUPS;
GO
CREATE TABLE raw_v2.DATA_MODEL_REPORT_GROUPS (
    [DATA_MODEL_ID] [numeric](18,0) NULL,
    [LINE] [int] NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [REPORT_GROUPS_C] [varchar](66) NULL
);
GO
PRINT 'Created raw_v2.DATA_MODEL_REPORT_GROUPS';
GO

-- C6. ZC_ALLOWABLE_GRPS
DROP TABLE IF EXISTS raw_v2.ZC_ALLOWABLE_GRPS;
GO
CREATE TABLE raw_v2.ZC_ALLOWABLE_GRPS (
    [ALLOWABLE_GRPS_C] [varchar](66) NULL,
    [NAME] [varchar](254) NULL,
    [TITLE] [varchar](254) NULL,
    [ABBR] [varchar](254) NULL,
    [INTERNAL_ID] [varchar](66) NULL
);
GO
PRINT 'Created raw_v2.ZC_ALLOWABLE_GRPS';
GO

-- C7. PROMPT_PARAMETERS
DROP TABLE IF EXISTS raw_v2.PROMPT_PARAMETERS;
GO
CREATE TABLE raw_v2.PROMPT_PARAMETERS (
    [PARAMETER_PROMPT_ID] [numeric](18, 0) NOT NULL,
    [LINE] [int] NOT NULL,
    [CONTACT_DATE] [datetime] NULL,
    [PARAMETER_NAME] [varchar](254) NULL,
    [PARAM_UNIQ] [int] NULL,
    [CAPTION] [varchar](254) NULL,
    [HELP_TEXT] [varchar](1024) NULL,
    [VISIBLE_YN] [varchar](1) NULL,
    [REQUIRED_YN] [varchar](1) NULL,
    [ENABLE_YN] [varchar](1) NULL,
    [DEFAULT_YN] [varchar](1) NULL
);
GO
PRINT 'Created raw_v2.PROMPT_PARAMETERS';
GO

-- C8. SEARCH_EXPRESSION
DROP TABLE IF EXISTS raw_v2.SEARCH_EXPRESSION;
GO
CREATE TABLE raw_v2.SEARCH_EXPRESSION (
    [REPORT_INFO_ID] [numeric](18, 0) NOT NULL,
    [PARAMETER_LINE] [int] NULL,
    [PARAMETER_UNIQ] [int] NULL,
    [EXPRSN_VALUE] [varchar](254) NULL,
    [LINE] [int] NOT NULL,
    [OPERATOR] [varchar](max) NULL
);
GO
PRINT 'Created raw_v2.SEARCH_EXPRESSION';
GO

-- C9. QUERY_DYNAMIC
DROP TABLE IF EXISTS raw_v2.QUERY_DYNAMIC;
GO
CREATE TABLE raw_v2.QUERY_DYNAMIC (
    [TEMPLATE_ID] [numeric](18, 0) NOT NULL,
    [CONTACT_DATE_REAL] [float] NOT NULL,
    [CONTACT_DATE] [datetime] NULL,
    [CONTACT_NUM] [int] NULL,
    [CONTEXT] [varchar](254) NULL,
    [REPORT_TYPE_C] [int] NULL,
    [SELECT_TYPE_C] [int] NULL,
    [START_DATE] [varchar](254) NULL,
    [START_TIME] [varchar](254) NULL,
    [END_DATE] [varchar](254) NULL,
    [END_TIME] [varchar](254) NULL,
    [DESCRIPTION] [varchar](254) NULL,
    [CUSTOM_LOGIC_C] [int] NULL,
    [LOGIC] [varchar](254) NULL,
    [INCLUDE_DEL_C] [int] NULL,
    [RECORD_SEL_PPT_ID] [numeric](18, 0) NULL,
    [CONTACT_SELECTPP_ID] [numeric](18, 0) NULL,
    [VAL_PP_ID] [numeric](18, 0) NULL,
    [ROUTINE_NAME] [varchar](254) NULL,
    [ROUT_NAME_PPT_ID] [numeric](18, 0) NULL,
    [RW_SQL_QUERY] [varchar](254) NULL,
    [SQL_QUERY_TYPE_C] [int] NULL,
    [LAST_UNIQ_USED] [int] NULL,
    [CM_CT_OWNER_ID] [varchar](25) NULL,
    [USE_DEPT_YN] [varchar](1) NULL,
    [RW_USERINPUT_ID] [numeric](18, 0) NULL,
    [JOB_CONFIG_ID] [numeric](18, 0) NULL
);
GO
PRINT 'Created raw_v2.QUERY_DYNAMIC';
GO

-- C10. PROMPT_INFO
DROP TABLE IF EXISTS raw_v2.PROMPT_INFO;
GO
CREATE TABLE raw_v2.PROMPT_INFO (
    [PARAMETER_PROMPT_ID] [numeric](18, 0) NOT NULL,
    [CONTACT_DATE_REAL] [float] NOT NULL,
    [CONTACT_DATE] [datetime] NULL,
    [CONTACT_NUM] [varchar](254) NULL,
    [CM_CT_OWNER_ID] [varchar](25) NULL,
    [QUERY_TEMPLATE_ID] [numeric](18, 0) NULL,
    [INTERPRM_LOGIC_YN] [varchar](1) NULL,
    [DATE_RANGE_OPTION_C] [int] NULL,
    [DURATION_LIMIT] [int] NULL
);
GO
PRINT 'Created raw_v2.PROMPT_INFO';
GO

-- C11. DRILL_TEXT_SQLSERVER
DROP TABLE IF EXISTS raw_v2.DRILL_TEXT_SQLSERVER;
GO
CREATE TABLE raw_v2.DRILL_TEXT_SQLSERVER (
    [JOB_CONFIGURATION_ID] [numeric](18, 0) NOT NULL,
    [LINE] [int] NOT NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [DRILL_TEXT_SQLSERVER] [varchar](4000) NULL
);
GO
PRINT 'Created raw_v2.DRILL_TEXT_SQLSERVER';
GO

-- C12. REPORT_QUEUES
DROP TABLE IF EXISTS raw_v2.REPORT_QUEUES;
GO
CREATE TABLE raw_v2.REPORT_QUEUES (
    [REPORT_INFO_ID] [numeric](18, 0) NOT NULL,
    [LINE] [int] NOT NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [Q_LIST_DESC] [varchar](4000) NULL
);
GO
PRINT 'Created raw_v2.REPORT_QUEUES';
GO

-- C13. CLARITY_RPT_QUEUES
DROP TABLE IF EXISTS raw_v2.CLARITY_RPT_QUEUES;
GO
CREATE TABLE raw_v2.CLARITY_RPT_QUEUES (
    [REPORT_ID] [numeric](18, 0) NOT NULL,
    [LINE] [int] NOT NULL,
    [CM_PHY_OWNER_ID] [varchar](25) NULL,
    [CM_LOG_OWNER_ID] [varchar](25) NULL,
    [Q_LIST_DESC] [varchar](4000) NULL
);
GO
PRINT 'Created raw_v2.CLARITY_RPT_QUEUES';
GO

-- =============================================================================
-- SECTION 2: RAW TABLES (CSV Flat-File Loads)
-- These land data from CSV files on the network share.
-- Python atlas_csv_loader.py populates these via bulk insert.
-- =============================================================================

-- CSV 1. PAF / Columns Extract (Atlas_-_Columns_Extract.csv)
-- Source: SSIS "Clarity Flat Files" DFT → raw.[clarity-paf]
IF OBJECT_ID('raw_v2.clarity_paf', 'U') IS NOT NULL
    DROP TABLE raw_v2.clarity_paf;
GO

-- 2026-03-25: Widened columns to resolve CSV loader truncation errors.
CREATE TABLE raw_v2.clarity_paf (
    [Column ID]         NVARCHAR(4000)  NULL,
    [Name]              NVARCHAR(4000)  NULL,
    [Description]       NVARCHAR(4000)  NULL,
    [AssocApps]         NVARCHAR(4000)  NULL,
    [DataType]          NVARCHAR(4000)  NULL,
    [ColumnINI]         NVARCHAR(4000)  NULL,
    [ColumnItem]        NVARCHAR(4000)  NULL,
    [Extension]         NVARCHAR(4000)  NULL,
    [PAFFieldType]      NVARCHAR(4000)  NULL,
    [ExtensionParameter] NVARCHAR(4000) NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.clarity_paf';
GO

-- CSV 2. HRX Column Mapping (Atlas_-_Report_Column_Mapping_Extract.csv)
-- Source: SSIS "Clarity Flat Files" DFT → raw.[clarity-hrx-column-mapping]
IF OBJECT_ID('raw_v2.clarity_hrx_column_mapping', 'U') IS NOT NULL
    DROP TABLE raw_v2.clarity_hrx_column_mapping;
GO

CREATE TABLE raw_v2.clarity_hrx_column_mapping (
    [ReportID]          NVARCHAR(MAX)   NULL,
    [Name]              NVARCHAR(MAX)   NULL,
    [ReportTemplate]    NVARCHAR(MAX)   NULL,
    [ReportType]        NVARCHAR(MAX)   NULL,
    [OwnedBy]           NVARCHAR(MAX)   NULL,
    [Availability]      NVARCHAR(MAX)   NULL,
    [TemplateType]      NVARCHAR(MAX)   NULL,
    [Created]           NVARCHAR(MAX)   NULL,
    [HRXSelectedFields] NVARCHAR(MAX)   NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.clarity_hrx_column_mapping';
GO

-- CSV 3. SlicerDicer Sessions (Atlas_-_SlicerDicer_Sessions_Extract.csv)
-- Source: SSIS "Clarity Flat Files" DFT → raw.[clarity-slicerdicer-sessions]
IF OBJECT_ID('raw_v2.clarity_slicerdicer_sessions', 'U') IS NOT NULL
    DROP TABLE raw_v2.clarity_slicerdicer_sessions;
GO

CREATE TABLE raw_v2.clarity_slicerdicer_sessions (
    [Report_ID]         NVARCHAR(MAX)   NULL,
    [Name]              NVARCHAR(MAX)   NULL,
    [Report_type]       NVARCHAR(MAX)   NULL,
    [Description]       NVARCHAR(MAX)   NULL,
    [Record_type]       NVARCHAR(MAX)   NULL,
    [Created_by]        NVARCHAR(MAX)   NULL,
    [Created]           NVARCHAR(MAX)   NULL,
    [Last_modified_by]  NVARCHAR(MAX)   NULL,
    [Last_modified_date] NVARCHAR(MAX)  NULL,
    [Display_title]     NVARCHAR(MAX)   NULL,
    [Data_model]        NVARCHAR(50)    NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.clarity_slicerdicer_sessions';
GO

-- CSV 4. SlicerDicer Public Sessions (Atlas_-_SlicerDicer_Public_Sessions_Extract.csv)
-- Source: SSIS "Clarity Flat Files" DFT → raw.[clarity-slicerdicer-public-sessions]
IF OBJECT_ID('raw_v2.clarity_slicerdicer_public_sessions', 'U') IS NOT NULL
    DROP TABLE raw_v2.clarity_slicerdicer_public_sessions;
GO

CREATE TABLE raw_v2.clarity_slicerdicer_public_sessions (
    [Session_ID]        NVARCHAR(MAX)   NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.clarity_slicerdicer_public_sessions';
GO

-- CSV 5. Code Template Extract (Atlas_-_Code_Template_Extract.csv)
-- Source: SSIS "Clarity Flat Files" DFT → raw.code_template
IF OBJECT_ID('raw_v2.code_template', 'U') IS NOT NULL
    DROP TABLE raw_v2.code_template;
GO

CREATE TABLE raw_v2.code_template (
    [Code Template ID]                              NVARCHAR(200)   NULL,
    [Code Template Name]                            NVARCHAR(4000)  NULL,
    [Code Template Description]                     NVARCHAR(4000)  NULL,
    [Code Template Template Type]                   NVARCHAR(200)   NULL,
    [Code Template INI]                             NVARCHAR(200)   NULL,
    [Code Template Programming Point Definition Item] NVARCHAR(200) NULL,
    [Code Template M Code]                          NVARCHAR(500)   NULL,
    [Code Template Parameter ID]                    NVARCHAR(4000)  NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.code_template';
GO

-- CSV 6. E3N Export (CodeTemplates.csv)
-- Source: SSIS "Clarity Flat Files" DFT → raw.[E3N_Export]
IF OBJECT_ID('raw_v2.E3N_Export', 'U') IS NOT NULL
    DROP TABLE raw_v2.E3N_Export;
GO

-- 2026-03-25: Widened columns to resolve CSV loader truncation errors (Errors 1-2).
-- nvarchar(MAX) columns require fast_executemany=False in atlas_csv_loader.py.
CREATE TABLE raw_v2.E3N_Export (
    [DEFINITION ID]             NVARCHAR(4000)  NULL,
    [DEFINITION NAME]           NVARCHAR(MAX)   NULL,
    [RECORD DESCRIPTION]        NVARCHAR(MAX)   NULL,
    [PARAMETER ID]              NVARCHAR(4000)  NULL,
    [PARAMETER ID RECORD NAME]  NVARCHAR(MAX)   NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.E3N_Export';
GO

-- CSV 7. FDS / SlicerDicer Filters (Atlas_-_SlicerDicer_Filters_Extract.csv)
-- Source: SSIS "Clarity Flat Files" DFT → raw.[clarity-fds]
IF OBJECT_ID('raw_v2.clarity_fds', 'U') IS NOT NULL
    DROP TABLE raw_v2.clarity_fds;
GO

CREATE TABLE raw_v2.clarity_fds (
    [Filter ID]                         NVARCHAR(50)    NULL,
    [Filter Name]                       NVARCHAR(500)   NULL,
    [Filter Description]                NVARCHAR(4000)  NULL,
    [Record Type]                       NVARCHAR(200)   NULL,
    [Filter Category]                   NVARCHAR(200)   NULL,
    [Filter Data Type]                  NVARCHAR(200)   NULL,
    [Filter Display Name]               NVARCHAR(500)   NULL,
    [Is Filter Inactive?]               NVARCHAR(50)    NULL,
    [Is Filter Sensitive?]              NVARCHAR(50)    NULL,
    [Is Filter Column Only?]            NVARCHAR(50)    NULL,
    [Overall Review Status]             NVARCHAR(500)   NULL,
    [Last Review Date]                  NVARCHAR(200)   NULL,
    [Reviewers]                         NVARCHAR(4000)  NULL,
    [Filter Information Table Name]     NVARCHAR(500)   NULL,
    [Filter Information Data Expression] NVARCHAR(4000) NULL,
    [Supported Summary Level]           NVARCHAR(500)   NULL,
    [Filter Data Source]                NVARCHAR(500)   NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.clarity_fds';
GO

-- CSV 8. FDS-FDM Map (Atlas_-_SlicerDicer_Filter_Data_Model_Map.csv)
-- Source: SSIS "Clarity Flat Files" DFT → raw.[fds-fdm-map]
IF OBJECT_ID('raw_v2.fds_fdm_map', 'U') IS NOT NULL
    DROP TABLE raw_v2.fds_fdm_map;
GO

CREATE TABLE raw_v2.fds_fdm_map (
    [FDS ID]            VARCHAR(50)     NULL,
    [FDM ID]            VARCHAR(50)     NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.fds_fdm_map';
GO

-- CSV 9. HRX Parameter Logic (BILH_HRX_Parameter_Logic.csv)
-- Source: SSIS "Clarity Flat Files" DFT → raw.[clarity-intraparameter-logic]
IF OBJECT_ID('raw_v2.clarity_intraparameter_logic', 'U') IS NOT NULL
    DROP TABLE raw_v2.clarity_intraparameter_logic;
GO

CREATE TABLE raw_v2.clarity_intraparameter_logic (
    [HRX ID]            VARCHAR(50)     NULL,
    [Crit Uniq]         VARCHAR(50)     NULL,
    [Intraparam Logic]  VARCHAR(4000)   NULL,
    ETL_LoadDate        DATETIME        NOT NULL DEFAULT GETDATE()
);
GO
PRINT 'Created raw_v2.clarity_intraparameter_logic';
GO

PRINT '';
PRINT '=== Week 3 Clarity DDL Complete ===';
PRINT 'End Time: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
PRINT 'Raw tables created:     54 (46 SQL extract + 8 CSV)';
GO
