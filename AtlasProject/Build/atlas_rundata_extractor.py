"""
Atlas ETL Migration — Pipeline B: atlas_rundata_extractor.py

Replaces: usp_Atlas_RunData Phase 2 (5 linked server extractions)
Amendment: Pipeline B eliminates linked server dependencies by using
           direct Python/pyodbc connections to Clarity and Caboodle.

This script extracts run/usage data from two Epic source systems:
  1. Clarity  — RW_RPT_RUN_DATA+RW_RPT_RUN_VU_STAT, Dashboard via
               METRIC_DATA_SUMMARIES+DAILY_DATA (2 queries, 2-week window)
  2. Caboodle — SlicerDicer Stats_Http (11 cols), Stats_Query (3 cols),
               Stats_SaveLoad (6 cols) (3 queries, 2-week window)

Results are bulk-inserted into raw_v2.* tables in Atlas_Staging.
After this script completes, usp_Atlas_RunData_pipelineB resumes at
Phase 3 (indexing) through Phase 10 (production merge).

Pipeline position:
    atlas_pbi_events.py         →  (populates raw_v2.PbiActivityEvent)
    atlas_rundata_extractor.py  →  (populates raw_v2.Clarity* + raw_v2.SlicerDicer*)
    usp_Atlas_RunData           →  Phase 1 truncate, Phase 3-10 transform/merge

Dependencies:
    - Python 3.11+ with pyodbc, pandas
    - Network connectivity to EPICCLAPRD (Clarity) and EPICCDWPRD (Caboodle)
    - Network connectivity to Atlas_Staging (10.247.4.56\\SQL126)
    - atlas_config.py for connection strings
    - raw_v2 tables from 01_week4_rundata_ddl.sql

Usage:
    python atlas_rundata_extractor.py                       # All 5 extractions
    python atlas_rundata_extractor.py --source clarity      # Clarity only
    python atlas_rundata_extractor.py --source slicerdicer  # SlicerDicer only
    python atlas_rundata_extractor.py --days 7              # 7-day window
    python atlas_rundata_extractor.py --dry-run             # Count only
    python atlas_rundata_extractor.py --verbose             # Debug logging

Author:  Larry Duren
Date:    March 2026
Version: 4.3 (Pipeline B Amendment)

Change Log:
    v4.3 2026-04-30 — Refactored run_extraction() to use fetchmany()
           streaming with per-batch commits. Eliminates MemoryError
           on large extractions (confirmed root cause of Phase 2a/2c
           failures during 365-day backfill attempt). Peak memory is
           now O(fetch_batch_size) regardless of total result set size.
           Also: improved exception logging to include type(e).__name__
           and repr(e) fallback so no-message exceptions (MemoryError
           etc.) produce meaningful @Message values in etl.Atlas_ETL_Log.
"""

import os
import sys
import argparse
import logging
import uuid
from datetime import datetime, timedelta
from typing import Optional, List

try:
    import pyodbc
    import pandas as pd
except ImportError as e:
    print(f"ERROR: Required package not installed: {e}")
    print("Install with: pip install pyodbc pandas")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
try:
    from atlas_config import config
except ImportError:
    class _FallbackConfig:
        epic_clarity_server = os.getenv('EPIC_CLARITY_SERVER', 'EPICCLAPRD.BILH.ITSYSTEMS.ORG')
        epic_clarity_database = 'Clarity'
        epic_caboodle_server = os.getenv('EPIC_CABOODLE_SERVER', 'EPICCDWPRD.BILH.ITSYSTEMS.ORG')
        epic_caboodle_database = os.getenv('EPIC_CABOODLE_DB', 'CDW_SlicerDicer')
        # CN2 (2026-04-07): service account credentials for Epic sources.
        # When empty, builders fall through to Trusted_Connection=yes (dev /
        # standalone mode). When populated via env var, UID/PWD is used
        # (production mode). Mirrors atlas_config.py behavior.
        epic_clarity_sql_user = os.getenv('EPIC_CLARITY_SQL_USER', '')
        epic_clarity_sql_password = os.getenv('EPIC_CLARITY_SQL_PWD', '')
        epic_caboodle_sql_user = os.getenv('EPIC_CABOODLE_SQL_USER', '')
        epic_caboodle_sql_password = os.getenv('EPIC_CABOODLE_SQL_PWD', '')
        atlas_staging_server = os.getenv('ATLAS_STAGING_SERVER', r'10.247.4.56\SQL126')
        atlas_staging_database = os.getenv('ATLAS_STAGING_DB', 'Atlas_Staging')
        query_timeout_seconds = int(os.getenv('ATLAS_QUERY_TIMEOUT', '3600'))
        connection_timeout_seconds = 30
        bulk_insert_batch_size = 10000
        fetch_batch_size = 50000
        log_level = 'INFO'
        rundata_lookback_days = 30
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
        def get_epic_caboodle_connection_string(self):
            if self.epic_caboodle_sql_user:
                auth = (f"UID={self.epic_caboodle_sql_user};"
                        f"PWD={self.epic_caboodle_sql_password};")
            else:
                auth = "Trusted_Connection=yes;"
            return (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.epic_caboodle_server};"
                f"DATABASE={self.epic_caboodle_database};"
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
logger = logging.getLogger('atlas_rundata_extractor')

PACKAGE_NAME = 'ETL-RunData-Extract'


# ══════════════════════════════════════════════════════════════════════════════
# ETL LOGGING
# ══════════════════════════════════════════════════════════════════════════════

def get_staging_connection() -> pyodbc.Connection:
    return pyodbc.connect(
        config.get_atlas_staging_connection_string(),
        timeout=config.connection_timeout_seconds,
    )


def get_clarity_connection() -> pyodbc.Connection:
    return pyodbc.connect(
        config.get_epic_clarity_connection_string(),
        timeout=config.connection_timeout_seconds,
    )


def get_caboodle_connection() -> pyodbc.Connection:
    return pyodbc.connect(
        config.get_epic_caboodle_connection_string(),
        timeout=config.connection_timeout_seconds,
    )


def log_start(conn, exec_id, step, seq):
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


def log_end(conn, log_id, rows, status='Success', error_msg=None):
    cursor = conn.cursor()
    # Distinguish "no message" (None — caller did not supply one) from "empty
    # message" (str(e) returned ''). The former takes the no-@Message branch;
    # the latter must still write the empty string to @Message so a Failure
    # row never has a silently-NULL ErrorMessage.
    if error_msg is not None:
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
# ══════════════════════════════════════════════════════════════════════════════

def build_extractions(lookback_days: int) -> dict:
    """
    Build extraction definitions with parameterized cutoff date.
    
    The cutoff date calculation uses Python datetime instead of SQL DATEADD
    since we now execute against the source server directly.
    """
    # The cutoff is passed as a SQL parameter to keep server-side filtering
    return {
        # ── Clarity: Report Run Data (RW_RPT_RUN_DATA.sql) ────────────────
        # Source: RW_RPT_RUN_DATA LEFT JOIN RW_RPT_RUN_VU_STAT
        # Produces one row per run+viewer combo with GROUP BY dedup
        # Landing table mirrors raw.[clarity_server-clarityreport-dbo-rw_rpt_run_data]
        "clarity_report_runs": {
            "step_name": "Phase 2a - Extract Clarity Report Run Data",
            "source": "clarity",
            "target_table": "raw_v2.ClarityReportRunData",
            "target_columns": [
                "RUN_ID", "REP_SETTINGS_ID", "SERVER_NODE_NAME",
                "RUN_INSTANT", "RUN_USER_ID",
                "REPORT_START_INST", "REPORT_END_INST",
                "TOTAL_EXE_TIME", "REPORT_STATUS_C",
                "RUN_NAME", "SOURCE_REPORT_ID",
                "REPORT_RUN_TYPE_C", "REPORT_TEMPLATE_ID",
            ],
            "source_query": """
                SELECT
                    ROW_NUMBER() OVER (ORDER BY rrrd.RUN_ID + ISNULL(rrrvs.LINE, 0)) AS RUN_ID,
                    rrrd.REP_SETTINGS_ID,
                    rrrd.SERVER_NODE_NAME,
                    COALESCE(rrrd.REPORT_START_INST, rrrd.RUN_INSTANT) AS RUN_INSTANT,
                    COALESCE(rrrvs.VIEW_USER_ID, rrrd.RUN_USER_ID) AS RUN_USER_ID,
                    rrrd.REPORT_START_INST,
                    rrrd.REPORT_END_INST,
                    rrrd.TOTAL_EXE_TIME,
                    rrrd.REPORT_STATUS_C,
                    rrrd.RUN_NAME,
                    rrrd.SOURCE_REPORT_ID,
                    rrrd.REPORT_RUN_TYPE_C,
                    rrrd.REPORT_TEMPLATE_ID
                FROM dbo.RW_RPT_RUN_DATA rrrd WITH (NOLOCK)
                LEFT OUTER JOIN dbo.RW_RPT_RUN_VU_STAT rrrvs WITH (NOLOCK)
                    ON rrrd.RUN_ID = rrrvs.RUN_ID
                WHERE COALESCE(rrrd.REPORT_START_INST, rrrd.RUN_INSTANT) >= DATEADD(DAY, ?, GETDATE())
                GROUP BY
                    rrrd.RUN_ID + ISNULL(rrrvs.LINE, 0),
                    rrrd.REP_SETTINGS_ID,
                    rrrd.SERVER_NODE_NAME,
                    rrrvs.VIEW_INSTANT,
                    rrrvs.VIEW_USER_ID,
                    rrrd.RUN_USER_ID,
                    rrrd.REPORT_START_INST,
                    rrrd.RUN_INSTANT,
                    rrrd.REPORT_END_INST,
                    rrrd.TOTAL_EXE_TIME,
                    rrrd.REPORT_STATUS_C,
                    rrrd.RUN_NAME,
                    rrrd.SOURCE_REPORT_ID,
                    rrrd.REPORT_RUN_TYPE_C,
                    rrrd.REPORT_TEMPLATE_ID
            """,
            "query_params": [-lookback_days],
        },
        # ── Clarity: Dashboard Run Data (Dashboard Run Data.sql) ───────────
        # Source: METRIC_DATA_SUMMARIES + DAILY_DATA + DASHBOARD_INFO
        #         + ZC_RECORD_TYPE_24 + numbers CTE (replaces master..spt_values)
        # Filters: DEFINITION_ID=33500, COMPLD_SUM_LEVEL='5^61',
        #          TIER2_NUM_TGT_TRANS IS NOT NULL, TIER1_TARGET_TRANS IS NOT NULL
        # Landing table mirrors raw.[clarity_server-clarity-dashboard-run-data]
        "clarity_dashboard_runs": {
            "step_name": "Phase 2b - Extract Clarity Dashboard Run Data",
            "source": "clarity",
            "target_table": "raw_v2.ClarityDashboardRunData",
            "target_columns": [
                "RunId", "SourceServer", "SourceDB", "SourceTable",
                "Name", "ReportObjectType", "EpicMasterFile",
                "EpicRecordID", "RunUserId", "RunStartTime",
            ],
            "source_query": """
                ;WITH nums AS (
                    SELECT TOP 1000
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS NUM
                    FROM sys.all_objects a
                    CROSS JOIN sys.all_objects b
                ),
                numnum AS (
                    SELECT n1.NUM
                    FROM nums n1
                    LEFT JOIN nums n2 ON n1.NUM >= n2.NUM
                )
                SELECT
                    ROW_NUMBER() OVER (ORDER BY numnum.NUM) AS RunId,
                    N'clarity_server' AS SourceServer,
                    N'clarityreport' AS SourceDB,
                    N'dashboard_info' AS SourceTable,
                    CONVERT(NVARCHAR(MAX), idm.DASHBOARD_NAME) AS Name,
                    COALESCE(idmt.NAME, N'Other') + N' Radar Dashboard' AS ReportObjectType,
                    N'IDM' AS EpicMasterFile,
                    CONCAT(N'', idm.DASHBOARD_ID) AS EpicRecordID,
                    CONCAT(N'', sfi.TIER1_TARGET_TRANS) AS RunUserId,
                    dly.DAY_DT AS RunStartTime
                FROM
                    dbo.METRIC_DATA_SUMMARIES sfi WITH (NOLOCK)
                    LEFT JOIN dbo.DAILY_DATA dly WITH (NOLOCK)
                        ON sfi.SUM_FACTS_ID = dly.SUM_FACTS_ID
                    LEFT JOIN dbo.DASHBOARD_INFO idm WITH (NOLOCK)
                        ON idm.DASHBOARD_ID = sfi.TIER2_NUM_TGT_TRANS
                    LEFT JOIN dbo.ZC_RECORD_TYPE_24 idmt WITH (NOLOCK)
                        ON idmt.RECORD_TYPE_24_C = idm.RECORD_TYPE_C
                    LEFT JOIN numnum
                        ON numnum.NUM = dly.VALUE_DAY
                WHERE sfi.DEFINITION_ID = 33500
                    AND sfi.COMPLD_SUM_LEVEL = '5^61'
                    AND dly.DAY_DT >= DATEADD(DAY, ?, GETDATE())
                    AND sfi.TIER2_NUM_TGT_TRANS IS NOT NULL
                    AND sfi.TIER1_TARGET_TRANS IS NOT NULL
            """,
            "query_params": [-lookback_days],
        },
        # ── SlicerDicer: HTTP Request Stats (SlicerDicer Stats_Http.sql) ──
        # Source: slicerdicer.stats_http (all 11 columns)
        # Note: Instant converted from UTC to local via SYSDATETIMEOFFSET()
        # Landing table mirrors raw.[slicerdicer_server-slicerdicer-stats_http]
        "slicerdicer_http": {
            "step_name": "Phase 2c - Extract SlicerDicer HTTP Stats",
            "source": "caboodle",
            "target_table": "raw_v2.SlicerDicerStatsHttp",
            "target_columns": [
                "RequestId", "Url", "UserId", "SessionId",
                "Instant", "Duration", "ClientIp",
                "CacheHits", "CacheMisses", "RequestType", "NodeName",
            ],
            "source_query": """
                SELECT
                    RequestId,
                    Url,
                    UserId,
                    SessionId,
                    DATEADD(MINUTE, DATEPART(TZ, SYSDATETIMEOFFSET()), Instant) AS Instant,
                    Duration,
                    ClientIp,
                    CacheHits,
                    CacheMisses,
                    RequestType,
                    NodeName
                FROM slicerdicer.Stats_Http WITH (NOLOCK)
                WHERE Instant > DATEADD(DAY, ?, GETDATE())
            """,
            "query_params": [-lookback_days],
        },
        # ── SlicerDicer: Query Stats (SlicerDicer Stats_Query.sql) ────────
        # Source: slicerdicer.stats_query (only 3 columns, filtered)
        # Production only selects HttpRequestId, CompiledRecordId, ModelId
        # Landing table mirrors raw.[slicerdicer_server-slicerdicer-stats_query]
        "slicerdicer_query": {
            "step_name": "Phase 2d - Extract SlicerDicer Query Stats",
            "source": "caboodle",
            "target_table": "raw_v2.SlicerDicerStatsQuery",
            "target_columns": [
                "HttpRequestId", "CompiledRecordId", "ModelId",
            ],
            "source_query": """
                SELECT
                    HttpRequestId,
                    CompiledRecordId,
                    ModelId
                FROM slicerdicer.Stats_Query WITH (NOLOCK)
                WHERE ModelId IS NOT NULL
                  AND HttpRequestId IN (
                      SELECT RequestId FROM slicerdicer.Stats_Http
                      WHERE Instant > DATEADD(DAY, ?, GETDATE())
                  )
            """,
            "query_params": [-lookback_days],
        },
        # ── SlicerDicer: Save/Load Stats (SlicerDicer Stats_SaveLoad.sql) ─
        # Source: slicerdicer.stats_saveload (6 columns)
        # Note: Instant converted from UTC to local via SYSDATETIMEOFFSET()
        # Landing table mirrors raw.[slicerdicer_server-slicerdicer-stats_saveload]
        "slicerdicer_saveload": {
            "step_name": "Phase 2e - Extract SlicerDicer Save/Load Stats",
            "source": "caboodle",
            "target_table": "raw_v2.SlicerDicerStatsSaveLoad",
            "target_columns": [
                "Instant", "Duration", "PopulationId",
                "IsLoad", "HttpRequestId", "Identifier",
            ],
            "source_query": """
                SELECT
                    DATEADD(MINUTE, DATEPART(TZ, SYSDATETIMEOFFSET()), Instant) AS Instant,
                    Duration,
                    PopulationId,
                    IsLoad,
                    HttpRequestId,
                    Identifier
                FROM slicerdicer.Stats_SaveLoad WITH (NOLOCK)
                WHERE Instant > DATEADD(DAY, ?, GETDATE())
            """,
            "query_params": [-lookback_days],
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# CORE EXTRACTION LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def run_extraction(key: str, defn: dict, exec_id: str, step_seq: int,
                   dry_run: bool = False) -> int:
    """Execute a single extraction. Returns row count."""
    step_name = defn['step_name']
    source_type = defn['source']
    target_table = defn['target_table']
    target_cols = defn['target_columns']
    source_query = defn['source_query']
    query_params = defn.get('query_params', [])
    
    logger.info(f"  {step_name}")
    start = datetime.now()
    
    staging_conn = get_staging_connection()
    log_id = log_start(staging_conn, exec_id, step_name, step_seq)
    
    try:
        # ── Connect to source ─────────────────────────────────────────────
        if source_type == 'clarity':
            source_conn = get_clarity_connection()
        elif source_type == 'caboodle':
            source_conn = get_caboodle_connection()
        else:
            raise ValueError(f"Unknown source type: {source_type}")
        
        source_conn.timeout = config.query_timeout_seconds
        source_cursor = source_conn.cursor()
        source_cursor.execute(source_query, *query_params)
        
        if dry_run:
            row_count = sum(1 for _ in source_cursor)
            logger.info(f"    [DRY RUN] {row_count:,} rows available")
            source_cursor.close()
            source_conn.close()
            log_end(staging_conn, log_id, row_count)
            staging_conn.close()
            return row_count
        
        # ── Streaming fetch via fetchmany() + per-batch commit ──────────
        # Peak memory is O(fetch_batch_size), independent of total result
        # set size. Replaces the prior fetchall() + materialized-list pattern
        # that produced MemoryError on the 365-day Phase 2a/2c backfill
        # (~63M rows, ~12GB peak memory).
        fetch_batch_size = config.fetch_batch_size
        source_cursor.arraysize = fetch_batch_size  # pyodbc prefetch hint

        # ── Pre-build INSERT SQL and column-type hints BEFORE the loop ──
        # setinputsizes must be set once on the staging cursor before any
        # executemany() calls. The column metadata query runs before the
        # first batch is fetched.
        placeholders = ', '.join(['?'] * len(target_cols))
        insert_sql = f"INSERT INTO {target_table} ({', '.join(target_cols)}) VALUES ({placeholders})"

        # Query column metadata to set explicit types for numeric/decimal columns.
        # Prevents fast_executemany from inferring INT for NUMERIC(18,0) columns,
        # which causes "Numeric value out of range" (SQLSTATE 22003) on large values.
        schema_name, table_name_only = target_table.split('.', 1)
        meta_cursor = staging_conn.cursor()
        meta_cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, schema_name, table_name_only)
        col_meta = {row.COLUMN_NAME: row for row in meta_cursor.fetchall()}
        meta_cursor.close()
        input_sizes = []
        for col in target_cols:
            meta = col_meta.get(col)
            if meta and meta.DATA_TYPE in ('numeric', 'decimal'):
                input_sizes.append((pyodbc.SQL_DECIMAL, meta.NUMERIC_PRECISION, meta.NUMERIC_SCALE))
            else:
                input_sizes.append(None)

        staging_cursor = staging_conn.cursor()
        staging_cursor.fast_executemany = True
        staging_cursor.setinputsizes(input_sizes)

        # ── Fetch first batch BEFORE truncating ─────────────────────────
        # If the source returns 0 rows, do NOT truncate — preserve the
        # prior run's data and log a Warning. Same intent as the previous
        # zero-row guard at commit 487480a.
        first_batch = source_cursor.fetchmany(fetch_batch_size)

        if not first_batch:
            logger.warning(
                f"    WARNING: {step_name} returned 0 rows — "
                f"source may be empty, retention boundary exceeded, "
                f"or query was silently terminated by server."
            )
            source_cursor.close()
            source_conn.close()
            staging_cursor.close()
            log_end(staging_conn, log_id, 0, 'Warning')
            staging_conn.close()
            return 0

        # ── Got rows: truncate target then stream-insert ────────────────
        staging_cursor.execute(f"TRUNCATE TABLE {target_table}")
        staging_conn.commit()

        row_count = 0
        batch = first_batch
        batch_num = 0
        while batch:
            batch_num += 1
            batch_data = [list(row) for row in batch]
            staging_cursor.executemany(insert_sql, batch_data)
            staging_conn.commit()  # commit per batch — bounds tlog growth
            row_count += len(batch_data)

            if batch_num == 1 or batch_num % 10 == 0:
                logger.info(
                    f"    Streamed batch {batch_num}: "
                    f"+{len(batch_data):,} rows (cumulative {row_count:,})"
                )

            batch = source_cursor.fetchmany(fetch_batch_size)

        source_cursor.close()
        source_conn.close()
        staging_cursor.close()

        elapsed = (datetime.now() - start).total_seconds()
        logger.info(
            f"    Inserted {row_count:,} rows into {target_table} "
            f"({batch_num} batches, {elapsed:.1f}s)"
        )

        log_end(staging_conn, log_id, row_count)
        staging_conn.close()
        return row_count

    except Exception as e:
        # str(e) is empty for some no-message exceptions (notably MemoryError
        # and certain ODBC driver errors). Fall back to repr(e) so the
        # @Message column always carries something diagnostic, and prefix
        # the python logger output with type(e).__name__ so the exception
        # class is always visible even when the message is uninformative.
        error_msg = str(e) if str(e) else repr(e)
        logger.error(
            f"    FAILED: {step_name} — "
            f"{type(e).__name__}: {error_msg}"
        )
        try:
            source_conn.close()
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

def run_all_extractions(source_filter: Optional[str] = None,
                        lookback_days: Optional[int] = None,
                        dry_run: bool = False) -> bool:
    """Run all (or filtered) RunData extractions."""
    exec_id = os.getenv('ATLAS_EXECUTION_ID', str(uuid.uuid4()))
    step_seq = 0
    total_rows = 0
    start_time = datetime.now()
    
    days = lookback_days or config.rundata_lookback_days
    extractions = build_extractions(days)
    
    # Filter by source system if requested
    if source_filter:
        source_filter = source_filter.lower()
        extractions = {k: v for k, v in extractions.items()
                       if v['source'] == source_filter
                       or k.startswith(source_filter)}
        if not extractions:
            logger.error(f"No extractions match filter: '{source_filter}'. "
                         f"Use 'clarity' or 'slicerdicer'.")
            return False
    
    logger.info("=" * 60)
    logger.info("ATLAS ETL — RunData Extractor (Pipeline B)")
    logger.info(f"Execution ID:   {exec_id}")
    logger.info(f"Clarity source: {config.epic_clarity_server}")
    logger.info(f"Caboodle source:{config.epic_caboodle_server}/{config.epic_caboodle_database}")
    logger.info(f"Target:         {config.atlas_staging_server}/{config.atlas_staging_database}")
    logger.info(f"Lookback:       {days} days")
    logger.info(f"Mode:           {'DRY RUN' if dry_run else 'LIVE'}")
    logger.info("=" * 60)
    
    # Test connectivity
    for label, get_conn in [("Clarity", get_clarity_connection),
                             ("Caboodle", get_caboodle_connection)]:
        # Only test connections we'll actually use
        sources_needed = set(v['source'] for v in extractions.values())
        if label.lower() not in sources_needed and label.lower()[:5] not in str(sources_needed):
            continue
        try:
            logger.info(f"Testing {label} connection...")
            c = get_conn()
            cur = c.cursor()
            cur.execute("SELECT @@SERVERNAME, DB_NAME()")
            row = cur.fetchone()
            logger.info(f"  Connected: {row[0]} / {row[1]}")
            cur.close()
            c.close()
        except Exception as e:
            logger.error(f"  {label} connection failed: {e}")
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
    failed = []
    for key, defn in extractions.items():
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
    logger.info(f"RunData extraction complete: {total_rows:,} total rows ({elapsed:.1f}s)")
    if failed:
        logger.error(f"FAILED: {', '.join(failed)}")
    logger.info("=" * 60)
    
    return len(failed) == 0


def main():
    parser = argparse.ArgumentParser(description='Atlas RunData Extractor (Pipeline B)')
    parser.add_argument('--source', type=str, default=None,
                        help="Filter by source: 'clarity' or 'slicerdicer'")
    parser.add_argument('--days', type=int, default=None,
                        help=f"Lookback days (default: {config.rundata_lookback_days})")
    parser.add_argument('--dry-run', action='store_true', help='Count only')
    parser.add_argument('--verbose', action='store_true', help='Debug logging')
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    success = run_all_extractions(
        source_filter=args.source,
        lookback_days=args.days,
        dry_run=args.dry_run,
    )
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
