"""
Atlas ETL Migration — Pipeline B: atlas_clarity_extractor.py

Replaces: usp_Atlas_Clarity Phase 1 (8 linked server extractions)
Amendment: Pipeline B eliminates the linked server dependency by using
           direct Python/pyodbc connections to Epic Clarity.

This script connects directly to EPICCLAPRD.BILH.ITSYSTEMS.ORG via pyodbc,
executes the same 8 extraction queries that were previously run through
[EPICCLAPRD].Clarity.dbo.* linked server syntax, and bulk-inserts results
into raw_v2.* tables in Atlas_Staging.

After this script completes, usp_Atlas_Clarity_pipelineB runs Phase 2
(staging transforms) using only LOCAL raw_v2.* tables — no linked server.

Pipeline position:
    atlas_clarity_extractor.py  →  usp_Atlas_Clarity (Phase 2 staging only)
                                →  atlas_csv_loader.py (CSV flat files)

Dependencies:
    - Python 3.11+ with pyodbc
    - Network connectivity to EPICCLAPRD (port 1433)
    - Network connectivity to Atlas_Staging (10.247.4.56\\SQL126)
    - atlas_config.py for connection strings
    - raw_v2 schema tables (created by 01_week3_clarity_ddl.sql)

Usage:
    python atlas_clarity_extractor.py                    # Extract all 8
    python atlas_clarity_extractor.py --extract employees # Single extract
    python atlas_clarity_extractor.py --dry-run           # Count only
    python atlas_clarity_extractor.py --verbose           # Debug logging

Author:  Larry Duren
Date:    March 2026
Version: 3.1 (Pipeline B Amendment)
"""

import os
import sys
import argparse
import logging
import uuid
from datetime import datetime
from typing import Optional

try:
    import pyodbc
except ImportError as e:
    print(f"ERROR: Required package not installed: {e}")
    print("Install with: pip install pyodbc")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
try:
    from atlas_config import config
except ImportError:
    # Fallback for standalone execution / testing
    class _FallbackConfig:
        epic_clarity_server = os.getenv('EPIC_CLARITY_SERVER', 'EPICCLAPRD.BILH.ITSYSTEMS.ORG')
        epic_clarity_database = 'Clarity'
        # CN2 (2026-04-07): service account credentials for Epic Clarity.
        # When empty, the builder falls through to Trusted_Connection=yes
        # (dev workstation / standalone mode). When populated via env var,
        # UID/PWD is used (production mode). Mirrors atlas_config.py
        # epic_clarity_sql_user / epic_clarity_sql_password behavior.
        epic_clarity_sql_user = os.getenv('EPIC_CLARITY_SQL_USER', '')
        epic_clarity_sql_password = os.getenv('EPIC_CLARITY_SQL_PWD', '')
        atlas_staging_server = os.getenv('ATLAS_STAGING_SERVER', r'10.247.4.56\SQL126')
        atlas_staging_database = os.getenv('ATLAS_STAGING_DB', 'Atlas_Staging')
        query_timeout_seconds = int(os.getenv('ATLAS_QUERY_TIMEOUT', '3600'))
        connection_timeout_seconds = 30
        bulk_insert_batch_size = 10000
        log_level = 'INFO'
        def get_epic_clarity_connection_string(self):
            if self.epic_clarity_sql_user:
                auth = (f"UID={self.epic_clarity_sql_user};"
                        f"PWD={self.epic_clarity_sql_password};")
            else:
                auth = "Trusted_Connection=yes;"
            return (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.epic_clarity_server};"
                f"DATABASE={self.epic_clarity_database};"
                f"{auth}"
                f"TrustServerCertificate=yes;"
                f"Connection Timeout={self.connection_timeout_seconds};"
            )
        def get_atlas_staging_connection_string(self):
            return (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.atlas_staging_server};DATABASE={self.atlas_staging_database};"
                f"Trusted_Connection=yes;TrustServerCertificate=yes;"
                f"Connection Timeout={self.connection_timeout_seconds};"
            )
    config = _FallbackConfig()


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, config.log_level.upper(), logging.INFO),
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,
)
logger = logging.getLogger('atlas_clarity_extractor')

PACKAGE_NAME = 'ETL-Clarity-Extract'


# ══════════════════════════════════════════════════════════════════════════════
# ETL LOGGING (matches atlas_csv_loader.py / atlas_query_hierarchy.py pattern)
# ══════════════════════════════════════════════════════════════════════════════

def get_staging_connection() -> pyodbc.Connection:
    """Connect to Atlas Staging database."""
    return pyodbc.connect(
        config.get_atlas_staging_connection_string(),
        timeout=config.connection_timeout_seconds,
    )


def get_clarity_connection() -> pyodbc.Connection:
    """Connect directly to Epic Clarity database (Pipeline B)."""
    return pyodbc.connect(
        config.get_epic_clarity_connection_string(),
        timeout=config.connection_timeout_seconds,
    )


def log_start(conn: pyodbc.Connection, exec_id: str, step: str, seq: int) -> int:
    """Call etl.usp_Atlas_LogStart and return LogID."""
    cursor = conn.cursor()
    cursor.execute(
        "DECLARE @LogID INT; "
        "EXEC etl.usp_Atlas_LogStart "
        "  @ExecutionID=?, @PackageName=?, @StepName=?, @StepSequence=?, @LogID=@LogID OUTPUT; "
        "SELECT @LogID;",
        exec_id, PACKAGE_NAME, step, seq,
    )
    row = cursor.fetchone()
    log_id = row[0] if row else 0
    conn.commit()
    return log_id


def log_end(conn: pyodbc.Connection, log_id: int, rows: int,
            status: str = 'Success', error_msg: str = None):
    """Call etl.usp_Atlas_LogEnd."""
    cursor = conn.cursor()
    if error_msg:
        cursor.execute(
            "EXEC etl.usp_Atlas_LogEnd @LogID=?, @RowsAffected=?, @Status=?, @Message=?",
            log_id, rows, status, error_msg,
        )
    else:
        cursor.execute(
            "EXEC etl.usp_Atlas_LogEnd @LogID=?, @RowsAffected=?, @Status=?",
            log_id, rows, status,
        )
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# EXTRACTION DEFINITIONS
# Each entry maps an extract name to its source query (run on Clarity)
# and target table (in Atlas_Staging raw_v2).
# ══════════════════════════════════════════════════════════════════════════════

EXTRACTIONS = {
    # ── Group A: Direct table extractions ─────────────────────────────────────
    "clarity_emp": {
        "step_name": "Extract 1: CLARITY_EMP",
        "target_table": "raw_v2.CLARITY_EMP",
        # ── Column-narrowed: 173 → 6 columns (2026-04-03) ──────────────
        # Only 3 columns consumed by usp_Atlas_Clarity v7.0 Steps 7/8/12/16/17:
        #   USER_ID (JOIN key), NAME (display), SYSTEM_LOGIN (Azure UPN resolution)
        # Safety margin: USER_STATUS_C, EMP_RECORD_TYPE_C (for validation/future use)
        # LNK_SEC_TEMPLT_ID: required for Stage 21 Branch 6 (Applied Linkable Templates)
        # Performance: ~25 min → <1 min estimated (row width ~4KB → ~700 bytes)
        # stage_v2.ReportObjectUser has no EpicId — no additional columns needed
        "target_columns": [
            "USER_ID", "NAME", "SYSTEM_LOGIN", "USER_STATUS_C",
            "EMP_RECORD_TYPE_C", "LNK_SEC_TEMPLT_ID",
        ],
        "source_query": """
            SELECT
                USER_ID, NAME, SYSTEM_LOGIN, USER_STATUS_C,
                EMP_RECORD_TYPE_C, LNK_SEC_TEMPLT_ID
            FROM dbo.CLARITY_EMP WITH (NOLOCK)
        """,
    },
    "clarity_rpt": {
        "step_name": "Extract 2: CLARITY_RPT",
        "target_table": "raw_v2.CLARITY_RPT",
        # ── Column-narrowed: 26 → 5 columns (2026-03-24) ──────────────
        "target_columns": [
            "REPORT_ID", "REPORT_NAME", "ASSOC_REPORT_ID",
            "HIDE_FROM_LIBRARY_YN", "RECORD_STATUS_C",
        ],
        "source_query": """
            SELECT
                REPORT_ID, REPORT_NAME, ASSOC_REPORT_ID,
                HIDE_FROM_LIBRARY_YN, RECORD_STATUS_C
            FROM dbo.CLARITY_RPT WITH (NOLOCK)
        """,
    },
    "clarity_rpt_groups": {
        "step_name": "Extract 3: CLARITY_RPT_GROUPS",
        "target_table": "raw_v2.CLARITY_RPT_GROUPS",
        "target_columns": [
            "REPORT_ID", "LINE", "CM_PHY_OWNER_ID", "CM_LOG_OWNER_ID", "REPORT_GROUP_C",
        ],
        "source_query": """
            SELECT
                REPORT_ID, LINE, CM_PHY_OWNER_ID, CM_LOG_OWNER_ID, REPORT_GROUP_C
            FROM dbo.CLARITY_RPT_GROUPS WITH (NOLOCK)
        """,
    },
    "clarity_rpt_queues": {
        "step_name": "Extract 4: CLARITY_RPT_QUEUES",
        "target_table": "raw_v2.CLARITY_RPT_QUEUES",
        "target_columns": [
            "REPORT_ID", "LINE", "CM_PHY_OWNER_ID", "CM_LOG_OWNER_ID", "Q_LIST_DESC",
        ],
        "source_query": """
            SELECT
                REPORT_ID, LINE, CM_PHY_OWNER_ID, CM_LOG_OWNER_ID, Q_LIST_DESC
            FROM dbo.CLARITY_RPT_QUEUES WITH (NOLOCK)
        """,
    },
    "component_desc": {
        "step_name": "Extract 5: COMPONENT_DESC",
        "target_table": "raw_v2.COMPONENT_DESC",
        "target_columns": [
            "COMPONENT_ID", "CM_PHY_OWNER_ID", "CM_LOG_OWNER_ID", "RECORD_DESC",
        ],
        "source_query": """
            SELECT
                COMPONENT_ID, CM_PHY_OWNER_ID, CM_LOG_OWNER_ID, RECORD_DESC
            FROM dbo.COMPONENT_DESC WITH (NOLOCK)
        """,
    },
    "component_info": {
        "step_name": "Extract 6: COMPONENT_INFO",
        "target_table": "raw_v2.COMPONENT_INFO",
        # ── Column-narrowed: 64 → 10 columns (2026-03-24) ──────────────
        "target_columns": [
            "COMPONENT_ID", "COMPONENT_NAME", "INSTANT_OF_UPD_DTTM",
            "READY_FOR_USE_YN", "RECORD_STATUS_C", "RECORD_TYPE_C",
            "USER_ID", "CODE_TEMPLATE_ID", "REPORT_ID",
            "SLICERDICER_REPORT_INFO_ID",
        ],
        "source_query": """
            SELECT
                COMPONENT_ID, COMPONENT_NAME, INSTANT_OF_UPD_DTTM,
                READY_FOR_USE_YN, RECORD_STATUS_C, RECORD_TYPE_C,
                USER_ID, CODE_TEMPLATE_ID, REPORT_ID,
                SLICERDICER_REPORT_INFO_ID
            FROM dbo.COMPONENT_INFO WITH (NOLOCK)
        """,
    },
    "component_list": {
        "step_name": "Extract 7: COMPONENT_LIST",
        "target_table": "raw_v2.COMPONENT_LIST",
        # ── Column-narrowed: 18 → 4 columns (2026-03-24) ──────────────
        "target_columns": [
            "COMPONENT_ID", "DASHBOARD_ID", "LINE", "REGION",
        ],
        "source_query": """
            SELECT
                COMPONENT_ID, DASHBOARD_ID, LINE, REGION
            FROM dbo.COMPONENT_LIST WITH (NOLOCK)
        """,
    },
    "component_summary_info": {
        "step_name": "Extract 8: COMPONENT_SUMMARY_INFO",
        "target_table": "raw_v2.COMPONENT_SUMMARY_INFO",
        "target_columns": [
            "COMPONENT_ID", "LINE", "CM_PHY_OWNER_ID", "CM_LOG_OWNER_ID",
            "DATA_RESOURCES_ID",
        ],
        "source_query": """
            SELECT
                COMPONENT_ID, LINE, CM_PHY_OWNER_ID, CM_LOG_OWNER_ID,
                DATA_RESOURCES_ID
            FROM dbo.COMPONENT_SUMMARY_INFO WITH (NOLOCK)
        """,
    },
    "dashboard_desc": {
        "step_name": "Extract 9: DASHBOARD_DESC",
        "target_table": "raw_v2.DASHBOARD_DESC",
        "target_columns": [
            "DASHBOARD_ID", "LINE", "CM_PHY_OWNER_ID", "CM_LOG_OWNER_ID", "RECORD_DESC",
        ],
        "source_query": """
            SELECT
                DASHBOARD_ID, LINE, CM_PHY_OWNER_ID, CM_LOG_OWNER_ID, RECORD_DESC
            FROM dbo.DASHBOARD_DESC WITH (NOLOCK)
        """,
    },
    "dashboard_info": {
        "step_name": "Extract 10: DASHBOARD_INFO",
        "target_table": "raw_v2.DASHBOARD_INFO",
        # ── Column-narrowed: 23 → 10 columns (2026-03-24) ──────────────
        "target_columns": [
            "DASHBOARD_ID", "DASHBOARD_NAME", "ENABLED_YN", "INSTANT_OF_UPD_DTTM",
            "READY_FOR_USE_YN", "RECORD_STATUS_C", "RECORD_TYPE_C", "USER_ID",
            "OVRIDE_STATUS_C", "OVRIDE_PARENT_DB_ID",
        ],
        "source_query": """
            SELECT
                DASHBOARD_ID, DASHBOARD_NAME, ENABLED_YN, INSTANT_OF_UPD_DTTM,
                READY_FOR_USE_YN, RECORD_STATUS_C, RECORD_TYPE_C, USER_ID,
                OVRIDE_STATUS_C, OVRIDE_PARENT_DB_ID
            FROM dbo.DASHBOARD_INFO WITH (NOLOCK)
            WHERE ISNULL(RECORD_TYPE_C, 1) <> 3
        """,
    },
    "drill_text_sqlserver": {
        "step_name": "Extract 11: DRILL_TEXT_SQLSERVER",
        "target_table": "raw_v2.DRILL_TEXT_SQLSERVER",
        "target_columns": [
            "JOB_CONFIGURATION_ID", "LINE", "CM_PHY_OWNER_ID", "CM_LOG_OWNER_ID",
            "DRILL_TEXT_SQLSERVER",
        ],
        "source_query": """
            SELECT
                JOB_CONFIGURATION_ID, LINE, CM_PHY_OWNER_ID, CM_LOG_OWNER_ID,
                DRILL_TEXT_SQLSERVER
            FROM dbo.DRILL_TEXT_SQLSERVER WITH (NOLOCK)
        """,
    },
    "filter_definitions": {
        "step_name": "Extract 13: FILTER_DEFINITIONS",
        "target_table": "raw_v2.FILTER_DEFINITIONS",
        # ── Column-narrowed: 35 → 6 columns (2026-03-24) ──────────────
        "target_columns": [
            "FILTER_ID", "FILTER_NAME", "BASE_RECORD_ID", "FILTER_INACTIVE_YN",
            "INSTANT_OF_UPDATE_DTTM", "RECORD_CREATION_DT",
        ],
        "source_query": """
            SELECT
                FILTER_ID, FILTER_NAME, BASE_RECORD_ID, FILTER_INACTIVE_YN,
                INSTANT_OF_UPDATE_DTTM, RECORD_CREATION_DT
            FROM dbo.FILTER_DEFINITIONS WITH (NOLOCK)
        """,
    },
    "clarity_lpp": {
        "step_name": "Extract 14: CLARITY_LPP",
        "target_table": "raw_v2.CLARITY_LPP",
        "target_columns": [
            "LPP_ID", "LPP_NAME", "LPP_TYPE_C", "M_CODE", "COMMENTS",
            "CM_PHY_OWNER_ID", "CM_LOG_OWNER_ID", "RECORD_STATE_C", "TEMPLATE_ID",
        ],
        "source_query": """
            SELECT
                LPP_ID, LPP_NAME, LPP_TYPE_C, M_CODE, COMMENTS,
                CM_PHY_OWNER_ID, CM_LOG_OWNER_ID, RECORD_STATE_C, TEMPLATE_ID
            FROM dbo.CLARITY_LPP WITH (NOLOCK)
        """,
    },
    "lpp_comments": {
        "step_name": "Extract 15: LPP_COMMENTS",
        "target_table": "raw_v2.LPP_COMMENTS",
        "target_columns": [
            "LPP_ID", "LINE", "COMMENTS",
        ],
        "source_query": """
            SELECT
                LPP_ID, LINE, COMMENTS
            FROM dbo.LPP_COMMENTS WITH (NOLOCK)
        """,
    },
    "metric_desc": {
        "step_name": "Extract 16: METRIC_DESC",
        "target_table": "raw_v2.METRIC_DESC",
        "target_columns": [
            "DEFINITION_ID", "LINE", "CM_PHY_OWNER_ID", "CM_LOG_OWNER_ID", "RECORD_DESC",
        ],
        "source_query": """
            SELECT
                DEFINITION_ID, LINE, CM_PHY_OWNER_ID, CM_LOG_OWNER_ID, RECORD_DESC
            FROM dbo.METRIC_DESC WITH (NOLOCK)
        """,
    },
    "metric_info": {
        "step_name": "Extract 17: METRIC_INFO",
        "target_table": "raw_v2.METRIC_INFO",
        # ── Column-narrowed: 104 → 5 columns (2026-03-24) ──────────────
        "target_columns": [
            "DEFINITION_ID", "METRIC_NAME", "INST_OF_UPDATE_DTTM",
            "ACTIVE_YN", "RECORD_STATUS_C",
        ],
        "source_query": """
            SELECT
                DEFINITION_ID, METRIC_NAME, INST_OF_UPDATE_DTTM,
                ACTIVE_YN, RECORD_STATUS_C
            FROM dbo.METRIC_INFO WITH (NOLOCK)
        """,
    },
    "ovride_rpt_groups": {
        "step_name": "Extract 18: OVRIDE_RPT_GROUPS",
        "target_table": "raw_v2.OVRIDE_RPT_GROUPS",
        "target_columns": [
            "REPORT_ID", "LINE", "CM_PHY_OWNER_ID", "CM_LOG_OWNER_ID", "REPORT_GROUP_C",
        ],
        "source_query": """
            SELECT
                REPORT_ID, LINE, CM_PHY_OWNER_ID, CM_LOG_OWNER_ID, REPORT_GROUP_C
            FROM dbo.OVRIDE_RPT_GROUPS WITH (NOLOCK)
        """,
    },
    "prompt_info": {
        "step_name": "Extract 19: PROMPT_INFO",
        "target_table": "raw_v2.PROMPT_INFO",
        "target_columns": [
            "PARAMETER_PROMPT_ID", "CONTACT_DATE_REAL", "CONTACT_DATE",
            "CONTACT_NUM", "CM_CT_OWNER_ID", "QUERY_TEMPLATE_ID",
            "INTERPRM_LOGIC_YN", "DATE_RANGE_OPTION_C", "DURATION_LIMIT",
        ],
        "source_query": """
            SELECT
                PARAMETER_PROMPT_ID, CONTACT_DATE_REAL, CONTACT_DATE,
                CONTACT_NUM, CM_CT_OWNER_ID, QUERY_TEMPLATE_ID,
                INTERPRM_LOGIC_YN, DATE_RANGE_OPTION_C, DURATION_LIMIT
            FROM dbo.PROMPT_INFO WITH (NOLOCK)
        """,
    },
    "query_dynamic": {
        "step_name": "Extract 20: QUERY_DYNAMIC",
        "target_table": "raw_v2.QUERY_DYNAMIC",
        # ── Column-narrowed: 27 → 9 columns (2026-03-24) ──────────────
        "target_columns": [
            "TEMPLATE_ID", "JOB_CONFIG_ID", "CONTEXT", "SELECT_TYPE_C",
            "CONTACT_DATE_REAL", "START_DATE", "START_TIME", "END_DATE", "END_TIME",
        ],
        "source_query": """
            SELECT
                TEMPLATE_ID, JOB_CONFIG_ID, CONTEXT, SELECT_TYPE_C,
                CONTACT_DATE_REAL, START_DATE, START_TIME, END_DATE, END_TIME
            FROM dbo.QUERY_DYNAMIC WITH (NOLOCK)
        """,
    },
    "report_desc": {
        "step_name": "Extract 21: REPORT_DESC",
        "target_table": "raw_v2.REPORT_DESC",
        "target_columns": [
            "REPORT_INFO_ID", "LINE", "CM_PHY_OWNER_ID", "CM_LOG_OWNER_ID",
            "REPORT_DESCRIPTION",
        ],
        "source_query": """
            SELECT
                REPORT_INFO_ID, LINE, CM_PHY_OWNER_ID, CM_LOG_OWNER_ID,
                REPORT_DESCRIPTION
            FROM dbo.REPORT_DESC WITH (NOLOCK)
        """,
    },
    "report_info": {
        "step_name": "Extract 22: REPORT_INFO",
        "target_table": "raw_v2.REPORT_INFO",
        # ── Column-narrowed: 39 → 11 columns (2026-03-24) ──────────────
        "target_columns": [
            "REPORT_INFO_ID", "REPORT_INFO_NAME", "RECORD_TYPE_C",
            "PRIVATE_OR_PUBLIC_C", "TEMP_REPORT_C", "REPORT_ID",
            "CREATED_BY_USER_ID", "LAST_MOD_BY_USER_ID",
            "INST_OF_LAST_MOD_DTTM", "OVRIDE_SEARCH_RECS", "OVRIDE_FIND_RECS",
        ],
        "source_query": """
            SELECT
                REPORT_INFO_ID, REPORT_INFO_NAME, RECORD_TYPE_C,
                PRIVATE_OR_PUBLIC_C, TEMP_REPORT_C, REPORT_ID,
                CREATED_BY_USER_ID, LAST_MOD_BY_USER_ID,
                INST_OF_LAST_MOD_DTTM, OVRIDE_SEARCH_RECS, OVRIDE_FIND_RECS
            FROM dbo.REPORT_INFO WITH (NOLOCK)
        """,
    },
    "report_queues": {
        "step_name": "Extract 23: REPORT_QUEUES",
        "target_table": "raw_v2.REPORT_QUEUES",
        "target_columns": [
            "REPORT_INFO_ID", "LINE", "CM_PHY_OWNER_ID", "CM_LOG_OWNER_ID", "Q_LIST_DESC",
        ],
        "source_query": """
            SELECT
                REPORT_INFO_ID, LINE, CM_PHY_OWNER_ID, CM_LOG_OWNER_ID, Q_LIST_DESC
            FROM dbo.REPORT_QUEUES WITH (NOLOCK)
        """,
    },
    "template_description": {
        "step_name": "Extract 24: TEMPLATE_DESCRIPTION",
        "target_table": "raw_v2.TEMPLATE_DESCRIPTION",
        "target_columns": [
            "REPORT_ID", "CONTACT_DATE_REAL", "LINE", "CONTACT_DATE",
            "SEARCH_SOURCE_DESC",
        ],
        "source_query": """
            SELECT
                REPORT_ID, CONTACT_DATE_REAL, LINE, CONTACT_DATE,
                SEARCH_SOURCE_DESC
            FROM dbo.TEMPLATE_DESCRIPTION WITH (NOLOCK)
        """,
    },
    "template_dynamic": {
        "step_name": "Extract 25: TEMPLATE_DYNAMIC",
        "target_table": "raw_v2.TEMPLATE_DYNAMIC",
        # ── Column-narrowed: 44 → 7 columns (2026-03-24) ──────────────
        "target_columns": [
            "REPORT_ID", "MAX_NUM_SEARCH", "MAX_NUM_RETURN", "DESCRIPTION",
            "CONTACT_NUM", "PARAM_PROMPT_ID", "SETUP_DATA_PP_ID",
        ],
        "source_query": """
            SELECT
                REPORT_ID, MAX_NUM_SEARCH, MAX_NUM_RETURN, DESCRIPTION,
                CONTACT_NUM, PARAM_PROMPT_ID, SETUP_DATA_PP_ID
            FROM dbo.TEMPLATE_DYNAMIC WITH (NOLOCK)
        """,
    },
    "template_info": {
        "step_name": "Extract 26: TEMPLATE_INFO",
        "target_table": "raw_v2.TEMPLATE_INFO",
        # ── Column-narrowed: 59 → 4 columns (2026-03-24) ──────────────
        "target_columns": [
            "REPORT_ID", "REPORT_NAME", "REPORT_TYPE_HGR_C", "STATUS_C",
        ],
        "source_query": """
            SELECT
                REPORT_ID, REPORT_NAME, REPORT_TYPE_HGR_C, STATUS_C
            FROM dbo.TEMPLATE_INFO WITH (NOLOCK)
        """,
    },
    "template_info_2": {
        "step_name": "Extract 27: TEMPLATE_INFO_2",
        "target_table": "raw_v2.TEMPLATE_INFO_2",
        "target_columns": [
            "REPORT_ID", "CM_PHY_OWNER_ID", "CM_LOG_OWNER_ID",
            "CRYSTAL_FILENAME", "OUTPUT_FORMAT_C", "ENTERPRISE_FOLDER",
            "CONTEXT_ID", "REASON_NO_CONTEXT_C",
        ],
        "source_query": """
            SELECT
                REPORT_ID, CM_PHY_OWNER_ID, CM_LOG_OWNER_ID,
                CRYSTAL_FILENAME, OUTPUT_FORMAT_C, ENTERPRISE_FOLDER,
                CONTEXT_ID, REASON_NO_CONTEXT_C
            FROM dbo.TEMPLATE_INFO_2 WITH (NOLOCK)
        """,
    },
    "resource_display": {
        "step_name": "Extract 28: RESOURCE_DISPLAY",
        "target_table": "raw_v2.RESOURCE_DISPLAY",
        # ── Column-narrowed: 40 → 4 columns (2026-03-24) ──────────────
        "target_columns": [
            "RESOURCE_ID", "RECORD_NAME", "METRIC_DEF_ID", "INSTANT_OF_UPDATE_DTTM",
        ],
        "source_query": """
            SELECT
                RESOURCE_ID, RECORD_NAME, METRIC_DEF_ID, INSTANT_OF_UPDATE_DTTM
            FROM dbo.RESOURCE_DISPLAY WITH (NOLOCK)
        """,
    },
    "data_model_definitions": {
        "step_name": "Extract 29: DATA_MODEL_DEFINITIONS",
        "target_table": "raw_v2.DATA_MODEL_DEFINITIONS",
        # ── Column-narrowed: 28 → 4 columns (2026-03-24) ──────────────
        "target_columns": [
            "DATA_MODEL_ID", "RECORD_NAME", "BASE_RECORD_ID", "INACTIVE_YN",
        ],
        "source_query": """
            SELECT
                DATA_MODEL_ID, RECORD_NAME, LEFT(BASE_RECORD_ID, 254) AS BASE_RECORD_ID, INACTIVE_YN
            FROM dbo.DATA_MODEL_DEFINITIONS WITH (NOLOCK)
        """,
    },
    "data_model_description": {
        "step_name": "Extract 30: DATA_MODEL_DESCRIPTION",
        "target_table": "raw_v2.DATA_MODEL_DESCRIPTION",
        "target_columns": [
            "DATA_MODEL_ID", "LINE", "CM_PHY_OWNER_ID", "CM_LOG_OWNER_ID",
            "DATA_MODEL_DESC",
        ],
        "source_query": """
            SELECT
                DATA_MODEL_ID, LINE, CM_PHY_OWNER_ID, CM_LOG_OWNER_ID,
                DATA_MODEL_DESC
            FROM dbo.DATA_MODEL_DESCRIPTION WITH (NOLOCK)
        """,
    },
    "data_model_report_groups": {
        "step_name": "Extract 31: DATA_MODEL_REPORT_GROUPS",
        "target_table": "raw_v2.DATA_MODEL_REPORT_GROUPS",
        "target_columns": [
            "DATA_MODEL_ID", "LINE", "CM_PHY_OWNER_ID", "CM_LOG_OWNER_ID",
            "REPORT_GROUPS_C",
        ],
        "source_query": """
            SELECT
                CAST(DATA_MODEL_ID AS NUMERIC(18,0)) AS DATA_MODEL_ID, LINE, CM_PHY_OWNER_ID, CM_LOG_OWNER_ID,
                REPORT_GROUPS_C
            FROM dbo.DATA_MODEL_REPORT_GROUPS WITH (NOLOCK)
        """,
    },
    "assoc_report_groups": {
        "step_name": "Extract 32: ASSOC_REPORT_GROUPS",
        "target_table": "raw_v2.ASSOC_REPORT_GROUPS",
        "target_columns": [
            "DASHBOARD_ID", "LINE", "CM_PHY_OWNER_ID", "CM_LOG_OWNER_ID",
            "REPORT_GROUPS_C",
        ],
        "source_query": """
            SELECT
                DASHBOARD_ID, LINE, CM_PHY_OWNER_ID, CM_LOG_OWNER_ID,
                REPORT_GROUPS_C
            FROM dbo.ASSOC_REPORT_GROUPS WITH (NOLOCK)
        """,
    },
    "zc_allowable_grps": {
        "step_name": "Extract 33: ZC_ALLOWABLE_GRPS",
        "target_table": "raw_v2.ZC_ALLOWABLE_GRPS",
        "target_columns": [
            "ALLOWABLE_GRPS_C", "NAME", "TITLE", "ABBR", "INTERNAL_ID",
        ],
        "source_query": """
            SELECT
                ALLOWABLE_GRPS_C, NAME, TITLE, ABBR, INTERNAL_ID
            FROM dbo.ZC_ALLOWABLE_GRPS WITH (NOLOCK)
        """,
    },
    "zc_record_type_24": {
        "step_name": "Extract 34: ZC_RECORD_TYPE_24",
        "target_table": "raw_v2.ZC_RECORD_TYPE_24",
        "target_columns": [
            "RECORD_TYPE_24_C", "NAME", "TITLE", "ABBR", "INTERNAL_ID",
        ],
        "source_query": """
            SELECT
                RECORD_TYPE_24_C, NAME, TITLE, ABBR, INTERNAL_ID
            FROM dbo.ZC_RECORD_TYPE_24 WITH (NOLOCK)
        """,
    },
    "zc_report_type_hgr": {
        "step_name": "Extract 35: ZC_REPORT_TYPE_HGR",
        "target_table": "raw_v2.ZC_REPORT_TYPE_HGR",
        "target_columns": [
            "REPORT_TYPE_HGR_C", "NAME", "TITLE", "ABBR", "INTERNAL_ID",
        ],
        "source_query": """
            SELECT
                REPORT_TYPE_HGR_C, NAME, TITLE, ABBR, INTERNAL_ID
            FROM dbo.ZC_REPORT_TYPE_HGR WITH (NOLOCK)
        """,
    },
    # ── Group B: Filtered/joined extractions ──────────────────────────────────
    "tag_info": {
        "step_name": "Extract 36: TAG_INFO",
        "target_table": "raw_v2.TAG_INFO",
        "target_columns": [
            "TAG_ID", "TAG_NAME",
        ],
        "source_query": """
            SELECT
                TAG_ID, TAG_NAME
            FROM dbo.TAG_INFO WITH (NOLOCK)
            WHERE ISNULL(RECORD_STATUS_C, 1) = 1
        """,
    },
    "component_groups": {
        "step_name": "Extract 37: COMPONENT_GROUPS",
        "target_table": "raw_v2.ClarityComponentGroups",
        "target_columns": [
            "COMPONENT_ID", "group_id",
        ],
        "source_query": """
            SELECT
                CONCAT(N'', COMPONENT_ID) AS COMPONENT_ID,
                CONCAT(N'', groups_c) AS group_id
            FROM dbo.COMPONENT_GROUPS WITH (NOLOCK)
        """,
    },
    "dashboard_roles": {
        "step_name": "Extract 38: DASHBOARD_ROLES",
        "target_table": "raw_v2.ClarityDashboardRoles",
        "target_columns": [
            "dashboard_id", "user_roles", "user_roles_id",
        ],
        "source_query": """
            SELECT
                TRY_CAST(r.dashboard_id AS NUMERIC(18,0)) AS dashboard_id,
                CONCAT(N'', r.User_roles) AS user_roles,
                TRY_CAST(u.User_role_id AS NUMERIC(18,0)) AS user_roles_id
            FROM dbo.ASSOC_USER_ROLES r WITH (NOLOCK)
            LEFT OUTER JOIN dbo.USER_ROLE u WITH (NOLOCK)
                ON r.USER_ROLES = u.USER_ROLE_DESCRIPTOR
        """,
    },
    "dashboard_types": {
        "step_name": "Extract 39: DASHBOARD_TYPES",
        "target_table": "raw_v2.ClarityDashboardTypes",
        "target_columns": [
            "dashboard_id", "user_types",
        ],
        "source_query": """
            SELECT
                TRY_CAST(dashboard_id AS NUMERIC(18,0)) AS dashboard_id,
                CAST(User_Types_c AS NVARCHAR(66)) AS user_types
            FROM dbo.ASSOC_USER_TYPES WITH (NOLOCK)
        """,
    },
    "template_tags": {
        "step_name": "Extract 40: TEMPLATE_TAGS",
        "target_table": "raw_v2.TEMPLATE_TAGS",
        "target_columns": [
            "report_id", "line", "tag_id",
        ],
        "source_query": """
            SELECT
                REPORT_ID AS report_id,
                LINE AS line,
                TAGS_RPT_TMPL_ID AS tag_id
            FROM dbo.TEMPLATE_TAGS WITH (NOLOCK)
        """,
    },
    "report_tags": {
        "step_name": "Extract 41: ADDL_REPORT_TAGS",
        "target_table": "raw_v2.REPORT_TAGS",
        "target_columns": [
            "report_id", "line", "tag_id",
        ],
        "source_query": """
            SELECT
                REPORT_ID AS report_id,
                LINE AS line,
                ADDL_RPT_TAG_ID AS tag_id
            FROM dbo.ADDL_REPORT_TAGS WITH (NOLOCK)
        """,
    },
    "component_tags": {
        "step_name": "Extract 42: COMPONENT_TAGS",
        "target_table": "raw_v2.COMPONENT_TAGS",
        "target_columns": [
            "report_id", "line", "tag_id",
        ],
        "source_query": """
            SELECT
                COMPONENT_ID AS report_id,
                LINE AS line,
                TAG_ID AS tag_id
            FROM dbo.COMPONENT_TAGS WITH (NOLOCK)
        """,
    },
    "dashboard_tags": {
        "step_name": "Extract 43: DASHBOARD_TAGS",
        "target_table": "raw_v2.DASHBOARD_TAGS",
        "target_columns": [
            "report_id", "line", "tag_id",
        ],
        "source_query": """
            SELECT
                DASHBOARD_ID AS report_id,
                LINE AS line,
                TAG_ID AS tag_id
            FROM dbo.DASHBOARD_TAGS WITH (NOLOCK)
        """,
    },
    "prompt_parameters": {
        "step_name": "Extract 44: PROMPT_PARAMETERS",
        "target_table": "raw_v2.PROMPT_PARAMETERS",
        "target_columns": [
            "PARAMETER_PROMPT_ID", "LINE", "CONTACT_DATE", "PARAMETER_NAME",
            "PARAM_UNIQ", "CAPTION", "HELP_TEXT", "VISIBLE_YN", "REQUIRED_YN",
            "ENABLE_YN", "DEFAULT_YN",
        ],
        "source_query": """
            SELECT
                PARAMETER_PROMPT_ID, LINE, CONTACT_DATE, PARAMETER_NAME,
                PARAM_UNIQ, CAPTION, HELP_TEXT, VISIBLE_YN, REQUIRED_YN,
                ENABLE_YN, DEFAULT_YN
            FROM dbo.PROMPT_PARAMETERS WITH (NOLOCK)
        """,
    },
    "search_expression": {
        "step_name": "Extract 45: SEARCH_EXPRESSION",
        "target_table": "raw_v2.SEARCH_EXPRESSION",
        "target_columns": [
            "REPORT_INFO_ID", "PARAMETER_LINE", "PARAMETER_UNIQ",
            "EXPRSN_VALUE", "LINE", "OPERATOR",
        ],
        "source_query": """
            SELECT
                se.REPORT_INFO_ID, se.PARAMETER_LINE, se.PARAMETER_UNIQ,
                se.EXPRSN_VALUE, se.LINE,
                oc.ABBR AS OPERATOR
            FROM dbo.SEARCH_EXPRESSION se WITH (NOLOCK)
            LEFT OUTER JOIN dbo.ZC_COMPARE_OPERATO oc WITH (NOLOCK)
                ON se.EXPRSN_OPERATOR_C = oc.COMPARE_OPERATO_C
        """,
    },
    # ── Group C: Complex queries ──────────────────────────────────────────────
    "clarity_user_groups": {
        "step_name": "Extract 47: ClarityUserGroups",
        "target_table": "raw_v2.ClarityUserGroups",
        "target_columns": [
            "USER_ID", "GroupName", "GroupSource", "GroupId",
        ],
        "source_query": """
            -- reporting workbench security
            SELECT
                CAST(SECREC.USER_ID AS NVARCHAR(18)) AS USER_ID,
                CAST(CLARITY_ECL.CLASSIFCTN_NAME AS NVARCHAR(200)) AS GroupName,
                N'Epic Reporting Workbench Security' AS GroupSource,
                CAST(CLARITY_ECL.ECL_ID AS NVARCHAR(50)) AS GroupId
            FROM dbo.CLARITY_ECL WITH (NOLOCK)
            LEFT JOIN dbo.CLARITY_EMP_2 SECREC WITH (NOLOCK)
                ON CLARITY_ECL.ECL_ID = SECREC.RW_CLASS_ID
            LEFT JOIN dbo.CLARITY_EMP WITH (NOLOCK)
                ON SECREC.USER_ID = CLARITY_EMP.USER_ID
            WHERE 1=1
                AND CLARITY_EMP.USER_STATUS_C = '1'
                AND CLARITY_EMP.EMP_RECORD_TYPE_C <> '5'

            UNION ALL

            -- global reporting workbench access
            SELECT
                CAST(r.USER_ID AS NVARCHAR(18)) AS USER_ID,
                CAST(b.[name] AS NVARCHAR(200)) AS GroupName,
                N'Epic Reporting Workbench Access',
                CAST(CLTY_RPT_GRP_C AS NVARCHAR(50)) AS GroupId
            FROM dbo.RW_SEC_RPTGRPS_BASE r WITH (NOLOCK)
            LEFT JOIN dbo.ZC_ALLOWABLE_GRPS b WITH (NOLOCK)
                ON r.CLTY_RPT_GRP_C = b.ALLOWABLE_GRPS_C
            LEFT JOIN dbo.CLARITY_EMP WITH (NOLOCK)
                ON CLARITY_EMP.USER_ID = r.USER_ID
            WHERE 1=1
                AND CLARITY_EMP.USER_STATUS_C = '1'
                AND CLARITY_EMP.EMP_RECORD_TYPE_C <> '5'

            UNION ALL

            -- reporting workbench access override groups
            SELECT
                CAST(r.USER_ID AS NVARCHAR(18)) AS USER_ID,
                CAST(b.[name] AS NVARCHAR(200)) AS GroupName,
                N'Epic Reporting Workbench Access',
                CAST(RPT_GRP_C AS NVARCHAR(50)) AS GroupId
            FROM dbo.RW_SEC_RPTGRPS_ADDL r WITH (NOLOCK)
            LEFT JOIN dbo.ZC_ALLOWABLE_GRPS b WITH (NOLOCK)
                ON r.RPT_GRP_C = b.ALLOWABLE_GRPS_C
            LEFT JOIN dbo.CLARITY_EMP WITH (NOLOCK)
                ON CLARITY_EMP.USER_ID = r.USER_ID
            WHERE 1=1
                AND CLARITY_EMP.USER_STATUS_C = '1'
                AND CLARITY_EMP.EMP_RECORD_TYPE_C <> '5'

            UNION ALL

            -- user roles set by template
            SELECT
                CAST(user_id AS NVARCHAR(18)) AS USER_ID,
                CAST(default_user_role AS NVARCHAR(200)) AS GroupName,
                'Epic User Role',
                CAST(user_role_id AS NVARCHAR(50)) AS GroupId
            FROM dbo.CLARITY_EMP_ROLE e WITH (NOLOCK)
            LEFT OUTER JOIN dbo.USER_ROLE r WITH (NOLOCK)
                ON e.DEFAULT_USER_ROLE = r.USER_ROLE_DESCRIPTOR

            UNION ALL

            -- user types
            SELECT
                CAST(t.USER_ID AS NVARCHAR(18)) AS USER_ID,
                CAST(z.[name] AS NVARCHAR(200)) AS GroupName,
                N'Epic User Type',
                CAST(CACHED_USER_TYPE_C AS NVARCHAR(50)) AS GroupId
            FROM dbo.CACHED_USER_TYPE t WITH (NOLOCK)
            LEFT JOIN dbo.CLARITY_EMP WITH (NOLOCK)
                ON t.USER_ID = CLARITY_EMP.USER_ID
            LEFT JOIN dbo.ZC_USER_TYPES z WITH (NOLOCK)
                ON z.USER_TYPES_C = t.CACHED_USER_TYPE_C
            WHERE CLARITY_EMP.USER_STATUS_C = '1'
                AND CLARITY_EMP.EMP_RECORD_TYPE_C <> '5'

            UNION ALL

            -- applied linkable template
            SELECT DISTINCT
                CAST(emp.USER_ID AS NVARCHAR(18)) AS USER_ID,
                CAST(CASE WHEN temp2.TEMPLT_DSPLY_TITLE IS NULL
                    THEN temp.NAME
                    ELSE temp2.TEMPLT_DSPLY_TITLE END AS NVARCHAR(200)) AS GroupName,
                'Epic Applied Linkable Template',
                CAST(temp.USER_ID AS NVARCHAR(50)) AS GroupId
            FROM dbo.CLARITY_EMP emp WITH (NOLOCK)
            INNER JOIN dbo.CLARITY_EMP_2 temp2 WITH (NOLOCK)
                ON emp.LNK_SEC_TEMPLT_ID = temp2.USER_ID
            LEFT OUTER JOIN dbo.CLARITY_EMP temp WITH (NOLOCK)
                ON emp.LNK_SEC_TEMPLT_ID = temp.USER_ID
            WHERE emp.EMP_RECORD_TYPE_C = 1

            UNION ALL

            -- analytics security class with SD access specified
            SELECT DISTINCT
                CAST(emp2.USER_ID AS NVARCHAR(18)) AS USER_ID,
                CAST(CONCAT(ecl.CLASSIFCTN_NAME,
                    CASE WHEN sp.BI_SEC_POINTS_C IS NOT NULL THEN ' (SD)'
                        ELSE ' (No SD)'
                    END) AS NVARCHAR(200)) AS GroupName,
                'Epic Analytics Security Class',
                CAST(ecl.ECL_ID AS NVARCHAR(50)) AS GroupId
            FROM dbo.CLARITY_EMP_2 emp2 WITH (NOLOCK)
            INNER JOIN dbo.CLARITY_ECL ecl WITH (NOLOCK)
                ON emp2.ANALYTICS_ECL_ID = ecl.ECL_ID
            INNER JOIN dbo.CLARITY_EMP emp WITH (NOLOCK)
                ON emp2.USER_ID = emp.USER_ID
            LEFT OUTER JOIN dbo.BI_SEC_POINTS sp WITH (NOLOCK)
                ON ecl.ECL_ID = sp.ECL_ID
                    AND sp.BI_SEC_POINTS_C = 9
            WHERE emp.EMP_RECORD_TYPE_C = 1
                AND emp.USER_STATUS_C = 1
        """,
    },

    # ── Group D: Epic security group source tables ─────────────────────────
    # These 9 tables support the expanded usp_Atlas_Clarity staging transforms
    # for Epic security groups (Option A — "Load Clarity Groups" SSIS equivalent).

    "clarity_ecl": {
        "step_name": "Extract 48: CLARITY_ECL",
        "target_table": "raw_v2.CLARITY_ECL",
        "target_columns": [
            "ECL_ID", "CLASSIFCTN_NAME",
        ],
        "source_query": """
            SELECT
                ECL_ID,
                CLASSIFCTN_NAME
            FROM dbo.CLARITY_ECL WITH (NOLOCK)
        """,
    },
    "clarity_emp_2": {
        "step_name": "Extract 49: CLARITY_EMP_2",
        "target_table": "raw_v2.CLARITY_EMP_2",
        "target_columns": [
            "USER_ID", "RW_CLASS_ID", "ANALYTICS_ECL_ID",
            "TEMPLT_DSPLY_TITLE",
        ],
        # NOTE: LNK_SEC_TEMPLT_ID lives on CLARITY_EMP, not CLARITY_EMP_2.
        # The "Applied Linkable Templates" group staging query must JOIN
        # CLARITY_EMP (Extract 1) for that column.
        "source_query": """
            SELECT
                USER_ID,
                RW_CLASS_ID,
                ANALYTICS_ECL_ID,
                TEMPLT_DSPLY_TITLE
            FROM dbo.CLARITY_EMP_2 WITH (NOLOCK)
        """,
    },
    "rw_sec_rptgrps_base": {
        "step_name": "Extract 50: RW_SEC_RPTGRPS_BASE",
        "target_table": "raw_v2.RW_SEC_RPTGRPS_BASE",
        "target_columns": [
            "USER_ID", "CLTY_RPT_GRP_C",
        ],
        "source_query": """
            SELECT
                USER_ID,
                CLTY_RPT_GRP_C
            FROM dbo.RW_SEC_RPTGRPS_BASE WITH (NOLOCK)
        """,
    },
    "rw_sec_rptgrps_addl": {
        "step_name": "Extract 51: RW_SEC_RPTGRPS_ADDL",
        "target_table": "raw_v2.RW_SEC_RPTGRPS_ADDL",
        "target_columns": [
            "USER_ID", "RPT_GRP_C",
        ],
        "source_query": """
            SELECT
                USER_ID,
                RPT_GRP_C
            FROM dbo.RW_SEC_RPTGRPS_ADDL WITH (NOLOCK)
        """,
    },
    "clarity_emp_role": {
        "step_name": "Extract 52: CLARITY_EMP_ROLE",
        "target_table": "raw_v2.CLARITY_EMP_ROLE",
        "target_columns": [
            "USER_ID", "DEFAULT_USER_ROLE",
        ],
        "source_query": """
            SELECT
                USER_ID, DEFAULT_USER_ROLE
            FROM dbo.CLARITY_EMP_ROLE WITH (NOLOCK)
        """,
    },
    "user_role": {
        "step_name": "Extract 53: USER_ROLE",
        "target_table": "raw_v2.USER_ROLE",
        "target_columns": [
            "USER_ROLE_ID", "USER_ROLE_DESCRIPTOR",
        ],
        "source_query": """
            SELECT
                USER_ROLE_ID,
                USER_ROLE_DESCRIPTOR
            FROM dbo.USER_ROLE WITH (NOLOCK)
        """,
    },
    "cached_user_type": {
        "step_name": "Extract 54: CACHED_USER_TYPE",
        "target_table": "raw_v2.CACHED_USER_TYPE",
        "target_columns": [
            "USER_ID", "CACHED_USER_TYPE_C",
        ],
        "source_query": """
            SELECT
                USER_ID,
                CACHED_USER_TYPE_C
            FROM dbo.CACHED_USER_TYPE WITH (NOLOCK)
        """,
    },
    "zc_user_types": {
        "step_name": "Extract 55: ZC_USER_TYPES",
        "target_table": "raw_v2.ZC_USER_TYPES",
        "target_columns": [
            "USER_TYPES_C", "NAME",
        ],
        "source_query": """
            SELECT
                USER_TYPES_C,
                NAME
            FROM dbo.ZC_USER_TYPES WITH (NOLOCK)
        """,
    },
    "bi_sec_points": {
        "step_name": "Extract 56: BI_SEC_POINTS",
        "target_table": "raw_v2.BI_SEC_POINTS",
        "target_columns": [
            "ECL_ID", "BI_SEC_POINTS_C",
        ],
        "source_query": """
            SELECT
                ECL_ID,
                BI_SEC_POINTS_C
            FROM dbo.BI_SEC_POINTS WITH (NOLOCK)
        """,
    },

    # ── Group E: Friendly-named pre-transformed tables ─────────────────────
    # These 5 tables are consumed by usp_Atlas_Clarity stages 1-3 and
    # usp_Atlas_LDAP. Column names are derived from the SP's SELECT statements
    # (12_usp_Atlas_Clarity.sql). Source column mappings are verified against:
    #   - raw_v2 DDL in 05_clarity_ddl.sql (CLARITY_EMP 173 cols, TEMPLATE_INFO 60 cols)
    #   - SSIS package XML (Reference/ETL-Clarity/Clarity-ETL.xml column metadata)
    #   - RunData extractor (atlas_rundata_extractor.py RW_RPT_RUN_DATA query)
    #   - Extract 48 DDL (CLARITY_ECL columns)
    #
    # UNRESOLVABLE columns (not on any source table in SSIS XML, DDL, or repo):
    #   - ClarityEmployees: REMOVED 2026-04-03 (table dropped, extraction deleted).
    #   - ClarityReportTemplates: REPORT_CATEGORY_C, REPORT_CATEGORY,
    #     OWNER_USER_ID, CREATED_DTTM, LAST_MODIFIED_DTTM, DESCRIPTION —
    #     not on TEMPLATE_INFO per SSIS XML and DDL. Extracted as NULL.
    #     Description is likely built via XML PATH from TEMPLATE_DESCRIPTION
    #     (see HGR staging query). Owner/dates may be on a related table.
    #   - ClarityDepartments: CLARITY_DEP table is not referenced in SSIS
    #     XML or any DDL in this repo. Table name and columns are assumed
    #     from Epic standard naming. Marked UNRESOLVABLE.

    # ── Group D: Metric Query extraction ─────────────────────────────────────
    # Source: SSIS ETL-Clarity "Clarity SQL" → raw.[clarity-metric-query]
    # Two-part UNION: (1) JOB_CONFIG_TEXT_SQLSERVER-based queries, (2) auto-SQL
    # generated metric queries. All 3 output columns are nvarchar(max).
    # fast_executemany MUST be disabled — pyodbc mishandles nvarchar(max) bulk.
    "clarity_metric_query": {
        "step_name": "Extract 46: clarity_metric_query",
        "target_table": "raw_v2.clarity_metric_query",
        "target_columns": [
            "idn id", "name", "query",
        ],
        "fast_executemany": False,
        "source_query": """
            select CONCAT(N'',definition_id) [idn id], CONCAT(N'',DISPLAY_TITLE) [name]
            , CONCAT(N'',(SELECT char(10) + isnull(TEXT_SQLSERVER ,char(10))
                      FROM  dbo.JOB_CONFIG_TEXT_SQLSERVER sqls
                      WHERE sqls.JOB_CONFIGURATION_ID = mi.JOB_CONFIGURATION_ID
                      ORDER BY line
                      FOR XML PATH(''), type).value('(./text())[1]','nvarchar(max)')) [query]
            from metric_info mi
            where mi.JOB_CONFIGURATION_ID is not null

            union

            select DEFINITION_ID, Display_title, (select replace(replace(Concat(
              N'/*',char(10),
              'IDN ID: ',mi.DEFINITION_ID,char(10),
              'IDN Name: ',mi.METRIC_NAME,Char(10),
              'IDN Display Title: ',mi.Display_title,char(10),
              'Description: ',char(10),
              (select stuff((select ''+ record_desc from metric_desc where definition_id = mi.definition_id for xml path('')),1,0,'')),char(10),'*/',CHAR(10),
              (select stuff((select char(10) + concat(N'declare @',property_name, ' as nvarchar(max) = ;') from metric_info_prop_defs where DEFINITION_ID = mi.DEFINITION_ID for xml path('')),1,1,'')),char(10),char(10),
              'select',CHAR(10),
              char(9),'GROUPING_ID(',(select substring(stuff((select ',' + auto_sql_Target_expression from metric_sum_level where definition_id = mi.DEFINITION_ID and sup_sum_lvls_c != 0  order by sup_sum_lvls_c for xml path('')),1,1,''),2,999999)),') "GROUPING_ID"',char(10),
              (select stuff((select char(10) +  char(9)+', ' + auto_sql_Target_expression + ' "TARGET_' + cast(sup_sum_lvls_c as nvarchar) + '"' from metric_sum_level where definition_id = mi.DEFINITION_ID and sup_sum_lvls_c != 0  order by sup_sum_lvls_c for xml path('')),1,1,'')),char(10),
              char(9),', ',mi.Auto_sql_date_expression, ' "INTERVAL_START_DT"',char(10),
              nmf.Title,
              dnf.Title,
              'from',CHAR(10),
              char(9),'[',db.NAME,']..', mi.AUTO_SQL_FACT_TABLE_NAME,' fact',CHAR(10),
              (select stuff((
                select char(10) +
                  concat(N'',char(9),'left outer join [',db.NAME,']..',
                    mad.dimension_table_name,' ',
                    mad.dimension_table_alias, ' ',
                    stuff((select
                        '' + concat(
                        ' on fact.',
                        madl.fact_table_column_name, ' = ',
                        mad.DIMENSION_TABLE_ALIAS,
                        '.',
                        madl.dimension_table_column_name)
                      from METRIC_AUTO_DIMENSION_LNK madl where mad.DEFINITION_ID = madl.DEFINITION_ID and mad.DIMENSION_KEY = madl.DIMENSION_KEY for xml path('')),1,1,'')
                    ) from METRIC_AUTO_DIMENSION mad where mad.DEFINITION_ID = mi.DEFINITION_ID order by dimension_table_alias for xml path('') ),1,1,'') ), CHAR(10),
                    case when FACILITY_EXCL_YN = 'Y' then char(9) + 'INNER JOIN [Clarity]..D_METRIC_DEPT_INCLUSIONS gen_exclusions ON gen_exclusions.DEFINITION_ID = '+ cast(mi.DEFINITION_ID as nvarchar)+char(10)+char(9)+char(9)+
                'AND ' + (select top 1 isnull(auto_sql_Target_expression,'fact.DEPARTMENT_ID') from metric_sum_level where definition_id = mi.DEFINITION_ID and auto_sql_target_expression = 'fact.DISCH_DEPT_ID') + ' = gen_exclusions.DEPARTMENT_ID'+char(10)+char(9)+char(9)+
                'AND gen_exclusions.INCLUDED_DATE <= '+mi.Auto_sql_date_expression+char(10) else '' end,
              'where',CHAR(10),
              case when mi.AUTO_SQL_FILTER_EXPRESSION is not null then concat(N'',char(9),'(',mi.AUTO_SQL_FILTER_EXPRESSION,')',CHAR(10),'and ') else '' end,
              char(9),mi.Auto_sql_date_expression, ' is not null',char(10),
              'GROUP BY',char(10),
              char(9),'GROUPING SETS (',char(10),char(9),
              (select substring(stuff((select ',' + auto_sql_Target_expression from metric_sum_level where definition_id = mi.DEFINITION_ID and sup_sum_lvls_c != 0  order by sup_sum_lvls_c for xml path('')),1,1,''),2,999999)),char(10),
              char(9),')',char(10),
              char(9),', ',mi.Auto_sql_date_expression
              ),'''{{','@'),'}}''','')
              from metric_info mi
              left outer join ZC_CRYSTAL_DATAMODEL db on mi.SQL_SOURCE_DATABASE_C = db.CRYSTAL_DATAMODEL_C
              left outer join ZC_AUTO_SQL_AGGN_FUNC nmf on mi.AUTO_SQL_NUMER_AGGN_FUNCTION_C = nmf.AUTO_SQL_AGGN_FUNC_C
              left outer join ZC_AUTO_SQL_AGGN_FUNC dnf on mi.AUTO_SQL_DENOM_AGGN_FUNCTION_C = dnf.AUTO_SQL_AGGN_FUNC_C
              where 1=1
              and mi.DEFINITION_ID = mis.DEFINITION_ID ) [query]
            from metric_info mis
            where mis.JOB_CONFIGURATION_ID is null
            and AUTO_SQL_FACT_TABLE_NAME is not null
            and COLL_MTHD_C in (99999,99990)
        """,
    },
    # ── Username / domain name links (feeds usp_Atlas_LDAP Steps 5-6) ────────
    # Source: SSIS ETL-Clarity "Load Clarity Users" component
    # Joins clarity_emp to emp_login_hx to resolve the most-used OS login
    # per user (partitioned by user_id, ranked by login frequency + recency).
    # ~121K rows. Bounded nvarchar(255) columns — fast_executemany OK.
    "clarity_username_links": {
        "step_name": "Extract 62: ClarityUsernameLinks",
        "target_table": "raw_v2.ClarityUsernameLinks",
        "target_columns": [
            "user_Id", "Name", "domain_name",
        ],
        "source_query": """
            SELECT
                CONCAT(N'', t.user_id)                                    AS user_Id,
                CONCAT(N'', t.Name)                                        AS Name,
                CONCAT(N'', LOWER(ISNULL(t.system_login, t.os_login)))    AS domain_name
            FROM (
                SELECT
                    e.user_id,
                    t.os_login,
                    cnt AS logins,
                    ROW_NUMBER() OVER (PARTITION BY e.user_id ORDER BY rownum DESC) AS rownum,
                    e.Name,
                    e.user_status_c,
                    e.system_login
                FROM clarity_emp e
                LEFT OUTER JOIN (
                    SELECT
                        user_id,
                        os_login,
                        COUNT(1) cnt,
                        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(1) DESC) AS rownum
                    FROM (
                        SELECT
                            user_id,
                            os_login,
                            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_instant_dttm DESC) AS rownum
                        FROM emp_login_hx
                        WHERE os_login LIKE '%-%'
                    ) AS t
                    WHERE rownum < 100
                    GROUP BY user_id, os_login
                ) AS t
                    ON t.user_id = e.user_Id
                    AND (   os_login LIKE '%' + COALESCE(NULLIF(
                                SUBSTRING(SUBSTRING(e.Name, 0, CHARINDEX(',', e.name)),
                                    CHARINDEX('-', SUBSTRING(e.Name, 0, CHARINDEX(',', e.name))) + 1,
                                    LEN(SUBSTRING(e.Name, 0, CHARINDEX(',', e.name))) + 1
                                        - ISNULL(CHARINDEX('-', SUBSTRING(e.Name, 0, CHARINDEX(',', e.name))), 0)
                                ), ''), e.name) + '%'
                        OR  os_login LIKE '%' + COALESCE(NULLIF(
                                COALESCE(NULLIF(SUBSTRING(e.Name, 0, CHARINDEX('-', e.name)), ''),
                                    SUBSTRING(e.Name, 0, CHARINDEX(',', e.name))
                                ), ''), e.name) + '%'
                    )
            ) AS t
            WHERE ISNULL(rownum, 1) = 1
        """,
    },
}

# PHASE2 friendly table extractions removed 2026-04-09 — tables had
# no consumers in Merge or the Atlas app. raw_v2.clarity_metric_query
# retained separately (deferred — needs clarity_metric_query CSV).


# ══════════════════════════════════════════════════════════════════════════════
# CORE EXTRACTION LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def run_extraction(extract_key: str, extract_def: dict, exec_id: str,
                   step_seq: int, dry_run: bool = False) -> int:
    """
    Execute a single extraction: read from Clarity, write to Atlas_Staging.
    
    Returns row count inserted.
    """
    step_name = extract_def['step_name']
    target_table = extract_def['target_table']
    target_cols = extract_def['target_columns']
    source_query = extract_def['source_query']
    
    logger.info(f"  {step_name}")
    start = datetime.now()
    
    # Log start
    staging_conn = get_staging_connection()
    log_id = log_start(staging_conn, exec_id, step_name, step_seq)
    
    try:
        # ── Read from Clarity ─────────────────────────────────────────────
        clarity_conn = get_clarity_connection()
        clarity_cursor = clarity_conn.cursor()
        clarity_cursor.execute(source_query)
        
        if dry_run:
            row_count = 0
            for _ in clarity_cursor:
                row_count += 1
            logger.info(f"    [DRY RUN] {row_count:,} rows available")
            clarity_cursor.close()
            clarity_conn.close()
            log_end(staging_conn, log_id, row_count, 'Success')
            staging_conn.close()
            return row_count
        
        # ── Fetch raw pyodbc rows (bypass pandas to preserve native types) ─
        rows = clarity_cursor.fetchall()
        clarity_cursor.close()
        clarity_conn.close()

        row_count = len(rows)
        logger.info(f"    Fetched {row_count:,} rows from Clarity ({(datetime.now()-start).total_seconds():.1f}s)")

        if row_count == 0:
            log_end(staging_conn, log_id, 0, 'Success')
            staging_conn.close()
            return 0

        # Convert pyodbc Row objects to plain lists (NULLs are already None)
        data = [list(row) for row in rows]

        # ── Truncate target table ─────────────────────────────────────────
        staging_cursor = staging_conn.cursor()
        staging_cursor.execute(f"TRUNCATE TABLE {target_table}")
        staging_conn.commit()

        # ── Bulk insert into Atlas_Staging ────────────────────────────────
        placeholders = ', '.join(['?'] * len(target_cols))
        # Bracket column names to handle spaces (e.g., [idn id] in clarity_metric_query)
        col_list = ', '.join(f'[{c}]' for c in target_cols)
        insert_sql = f"INSERT INTO {target_table} ({col_list}) VALUES ({placeholders})"

        # Query column metadata to set explicit types for numeric/decimal columns.
        # Prevents fast_executemany from inferring INT for NUMERIC(18,0) columns,
        # which causes "Numeric value out of range" (SQLSTATE 22003) on large values.
        schema_name, table_name = target_table.split('.', 1)
        meta_cursor = staging_conn.cursor()
        meta_cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, schema_name, table_name)
        col_meta = {row.COLUMN_NAME: row for row in meta_cursor.fetchall()}
        meta_cursor.close()

        # Disable fast_executemany for tables with all nvarchar(max) columns
        # (e.g., clarity_metric_query). pyodbc mishandles max-length types in bulk.
        use_fast = extract_def.get('fast_executemany', True)
        staging_cursor.fast_executemany = use_fast

        input_sizes = []
        for col in target_cols:
            meta = col_meta.get(col)
            if meta and meta.DATA_TYPE in ('numeric', 'decimal'):
                input_sizes.append((pyodbc.SQL_DECIMAL, meta.NUMERIC_PRECISION, meta.NUMERIC_SCALE))
            else:
                input_sizes.append(None)
        staging_cursor.setinputsizes(input_sizes)

        # Insert in batches
        batch_size = config.bulk_insert_batch_size
        for batch_start in range(0, row_count, batch_size):
            batch = data[batch_start:batch_start + batch_size]
            staging_cursor.executemany(insert_sql, batch)
        
        staging_conn.commit()
        staging_cursor.close()
        
        elapsed = (datetime.now() - start).total_seconds()
        logger.info(f"    Inserted {row_count:,} rows into {target_table} ({elapsed:.1f}s)")
        
        log_end(staging_conn, log_id, row_count, 'Success')
        staging_conn.close()
        return row_count
    
    except Exception as e:
        error_msg = str(e)
        logger.error(f"    FAILED: {step_name} — {error_msg}")
        try:
            clarity_conn.close()
        except Exception:
            pass
        try:
            log_end(staging_conn, log_id, 0, 'Failure', error_msg)
            staging_conn.close()
        except Exception:
            pass
        raise


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def run_all_extractions(extract_filter: Optional[list] = None,
                        dry_run: bool = False) -> bool:
    """
    Execute all (or selected) Clarity extractions.

    Args:
        extract_filter: If set, only run these specific extraction keys.
        dry_run: If True, count rows only without inserting.

    Returns True on success, False on failure.
    """
    exec_id = os.getenv('ATLAS_EXECUTION_ID', str(uuid.uuid4()))
    step_seq = 0
    total_rows = 0
    start_time = datetime.now()
    
    logger.info("=" * 60)
    logger.info("ATLAS ETL — Clarity Extractor (Pipeline B)")
    logger.info(f"Execution ID: {exec_id}")
    logger.info(f"Source:  {config.epic_clarity_server}/{config.epic_clarity_database}")
    logger.info(f"Target:  {config.atlas_staging_server}/{config.atlas_staging_database}")
    logger.info(f"Mode:    {'DRY RUN' if dry_run else 'LIVE'}")
    logger.info("=" * 60)
    
    # Test Clarity connectivity first
    logger.info("Testing Clarity connection...")
    try:
        test_conn = get_clarity_connection()
        cursor = test_conn.cursor()
        cursor.execute("SELECT @@SERVERNAME, DB_NAME()")
        row = cursor.fetchone()
        logger.info(f"  Connected: {row[0]} / {row[1]}")
        cursor.close()
        test_conn.close()
    except Exception as e:
        logger.error(f"  Clarity connection failed: {e}")
        return False

    # Test Atlas Staging connectivity
    logger.info("Testing Atlas Staging connection...")
    try:
        test_conn = get_staging_connection()
        cursor = test_conn.cursor()
        cursor.execute("SELECT @@SERVERNAME, DB_NAME()")
        row = cursor.fetchone()
        logger.info(f"  Connected: {row[0]} / {row[1]}")
        cursor.close()
        test_conn.close()
    except Exception as e:
        logger.error(f"  Atlas Staging connection failed: {e}")
        logger.error("  Verify ATLAS_SQL_USER and ATLAS_SQL_PWD environment variables are set.")
        logger.error("  Note: In PowerShell, double quotes interpret $ as variables — "
                      "use single quotes for passwords containing $.")
        sys.exit(1)

    # Run extractions
    extractions_to_run = dict(EXTRACTIONS)
    if extract_filter:
        invalid_keys = [k for k in extract_filter if k not in EXTRACTIONS]
        for k in invalid_keys:
            logger.error(f"Unknown extraction: '{k}'. "
                         f"Valid keys: {', '.join(EXTRACTIONS.keys())}")
        valid_keys = [k for k in extract_filter if k in EXTRACTIONS]
        if not valid_keys:
            return False
        extractions_to_run = {k: EXTRACTIONS[k] for k in valid_keys}
    
    failed = []
    for key, defn in extractions_to_run.items():
        step_seq += 1
        try:
            rows = run_extraction(key, defn, exec_id, step_seq, dry_run)
            total_rows += rows
        except Exception as e:
            logger.error(f"  Extraction '{key}' failed: {e}")
            failed.append(key)

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"Clarity extraction complete: {total_rows:,} total rows ({elapsed:.1f}s)")
    if failed:
        logger.error(f"FAILED extractions: {', '.join(failed)}")
    logger.info("=" * 60)
    
    return len(failed) == 0


def main():
    parser = argparse.ArgumentParser(description='Atlas Clarity Extractor (Pipeline B)')
    parser.add_argument('--extract', action='append', default=None,
                        help=f"Run specific extraction(s). Repeat for multiple: "
                             f"--extract key1 --extract key2. "
                             f"Options: {', '.join(EXTRACTIONS.keys())}")
    parser.add_argument('--dry-run', action='store_true',
                        help='Count rows only, no insert')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable debug logging')
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    success = run_all_extractions(
        extract_filter=args.extract,
        dry_run=args.dry_run,
    )
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
