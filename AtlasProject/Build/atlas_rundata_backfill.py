"""
Atlas ETL — atlas_rundata_backfill.py (Pipeline B)
====================================================

Purpose:
    One-time historical backfill for the five RunData extractions.
    Steps through history in 30-day chunks, oldest first, APPENDING
    to raw_v2 tables (no TRUNCATE). After completion, run
    usp_Atlas_RunData and usp_Atlas_Merge manually to load the
    backfilled data into production.

Usage:
    python atlas_rundata_backfill.py
    python atlas_rundata_backfill.py --days 180
    python atlas_rundata_backfill.py --days 180 --chunk-days 30
    python atlas_rundata_backfill.py --dry-run
    python atlas_rundata_backfill.py --verbose

Behavior:
    - Iterates 5 extractions × N chunks where N = ceil(days / chunk_days)
    - Each chunk uses an explicit [chunk_start, chunk_end) date range
      with both bounds passed as Python datetime parameters (no
      server-side DATEADD; chunks abut without gaps or overlaps)
    - APPEND-only: target raw_v2 tables are NEVER truncated
    - Streaming fetchmany() + per-batch commit (mirrors the
      run_extraction() pattern in atlas_rundata_extractor.py from
      commit 7a69a7e — bounded peak memory regardless of chunk size)
    - Single ExecutionID for the entire run; PackageName='ETL-RunData-Backfill'
      so dashboard reports can isolate backfill log rows from daily runs
    - On a chunk failure, logs the failure and continues to the next
      chunk — does NOT abort the whole backfill

Safety:
    Idempotent re-run is NOT guaranteed. If the backfill is interrupted
    mid-chunk, the partial chunk's rows remain in the target. Re-running
    over the same date range produces duplicate rows. To safely re-run,
    truncate the affected raw_v2 tables manually before re-execution.

After completion:
    Run usp_Atlas_RunData and usp_Atlas_Merge manually so the backfilled
    raw data flows through staging into production. The RunData SP's
    Phase 1 truncates only the staging tables (per commit 3efff6f) — the
    raw_v2 tables filled by this script are NOT touched.

Author:  Larry Duren
Date:    April 2026
Version: 1.0 (one-time historical backfill)
"""

# ═══════════════════════════════════════════════════════════════════════════════
# WARNING: This script appends to raw_v2 tables without truncating.
# Running it multiple times over the same date range will produce duplicate
# rows. Truncate the target raw_v2 tables manually before re-running if needed.
# ═══════════════════════════════════════════════════════════════════════════════

import argparse
import logging
import sys
import uuid
from datetime import datetime, time, timedelta
from typing import Optional, Tuple

try:
    import pyodbc
except ImportError as e:
    print(f"ERROR: Required package not installed: {e}")
    print("Install with: pip install pyodbc")
    sys.exit(1)

from atlas_config import config
from atlas_rundata_extractor import (
    get_staging_connection,
    get_clarity_connection,
    get_caboodle_connection,
    log_end,
)


# ──────────────────────────────────────────────────────────────────────────────
# Logging — same format as atlas_rundata_extractor.py for consistency
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, config.log_level.upper(), logging.INFO),
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,
)
logger = logging.getLogger('atlas_rundata_backfill')


PACKAGE_NAME = 'ETL-RunData-Backfill'


# ══════════════════════════════════════════════════════════════════════════════
# LOGGING HELPER (backfill-specific PACKAGE_NAME)
# ══════════════════════════════════════════════════════════════════════════════

def log_start(conn, exec_id, step, seq):
    """Mirror of atlas_rundata_extractor.log_start, with PACKAGE_NAME bound to
    'ETL-RunData-Backfill' so backfill log rows are distinguishable from
    normal daily-run rows in etl.Atlas_ETL_Log."""
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


# ══════════════════════════════════════════════════════════════════════════════
# BACKFILL EXTRACTION DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════
#
# Each definition is the daily extractor's source_query (atlas_rundata_extractor.py,
# build_extractions()) with its single open-ended lower-bound predicate
# replaced by an explicit [start, end) range:
#
#     >= DATEADD(DAY, ?, GETDATE())   →   >= ? AND <date_col> < ?
#     >  DATEADD(DAY, ?, GETDATE())   →   >= ? AND <date_col> < ?
#
# Inclusive lower / exclusive upper makes abutting chunks safe (no double-count
# of the boundary row). All other parts of each query — joins, GROUP BY, column
# aliases, NOLOCK hints, timezone normalization — are preserved verbatim.
# ══════════════════════════════════════════════════════════════════════════════

def build_backfill_extractions() -> list:
    """Ordered list of 5 extraction definitions adapted for chunked backfill.

    Returns a LIST (not a dict) to enforce execution sequence:
        2a → 2b → 2c → 2d → 2e
    """
    return [
        # ── Phase 2a: Clarity Report Run Data ─────────────────────────────────
        {
            "step_prefix":    "Phase 2a - Backfill Clarity Report Run Data",
            "source":         "clarity",
            "target_table":   "raw_v2.ClarityReportRunData",
            "target_columns": [
                "RUN_ID", "REP_SETTINGS_ID", "SERVER_NODE_NAME",
                "RUN_INSTANT", "RUN_USER_ID",
                "REPORT_START_INST", "REPORT_END_INST",
                "TOTAL_EXE_TIME", "REPORT_STATUS_C",
                "RUN_NAME", "SOURCE_REPORT_ID",
                "REPORT_RUN_TYPE_C", "REPORT_TEMPLATE_ID",
            ],
            "date_column":    "COALESCE(rrrd.REPORT_START_INST, rrrd.RUN_INSTANT)",
            "source_query":   """
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
                WHERE COALESCE(rrrd.REPORT_START_INST, rrrd.RUN_INSTANT) >= ?
                  AND COALESCE(rrrd.REPORT_START_INST, rrrd.RUN_INSTANT) <  ?
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
        },
        # ── Phase 2b: Clarity Dashboard Run Data ──────────────────────────────
        {
            "step_prefix":    "Phase 2b - Backfill Clarity Dashboard Run Data",
            "source":         "clarity",
            "target_table":   "raw_v2.ClarityDashboardRunData",
            "target_columns": [
                "RunId", "SourceServer", "SourceDB", "SourceTable",
                "Name", "ReportObjectType", "EpicMasterFile",
                "EpicRecordID", "RunUserId", "RunStartTime",
            ],
            "date_column":    "dly.DAY_DT",
            "source_query":   """
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
                    AND dly.DAY_DT >= ?
                    AND dly.DAY_DT <  ?
                    AND sfi.TIER2_NUM_TGT_TRANS IS NOT NULL
                    AND sfi.TIER1_TARGET_TRANS IS NOT NULL
            """,
        },
        # ── Phase 2c: SlicerDicer HTTP Stats ──────────────────────────────────
        {
            "step_prefix":    "Phase 2c - Backfill SlicerDicer HTTP Stats",
            "source":         "caboodle",
            "target_table":   "raw_v2.SlicerDicerStatsHttp",
            "target_columns": [
                "RequestId", "Url", "UserId", "SessionId",
                "Instant", "Duration", "ClientIp",
                "CacheHits", "CacheMisses", "RequestType", "NodeName",
            ],
            "date_column":    "Instant",
            "source_query":   """
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
                WHERE Instant >= ?
                  AND Instant <  ?
            """,
        },
        # ── Phase 2d: SlicerDicer Query Stats ─────────────────────────────────
        # Date filter applies inside the IN-subquery against Stats_Http.Instant.
        {
            "step_prefix":    "Phase 2d - Backfill SlicerDicer Query Stats",
            "source":         "caboodle",
            "target_table":   "raw_v2.SlicerDicerStatsQuery",
            "target_columns": [
                "HttpRequestId", "CompiledRecordId", "ModelId",
            ],
            "date_column":    "Stats_Http.Instant (via subquery)",
            "source_query":   """
                SELECT
                    HttpRequestId,
                    CompiledRecordId,
                    ModelId
                FROM slicerdicer.Stats_Query WITH (NOLOCK)
                WHERE ModelId IS NOT NULL
                  AND HttpRequestId IN (
                      SELECT RequestId FROM slicerdicer.Stats_Http
                      WHERE Instant >= ?
                        AND Instant <  ?
                  )
            """,
        },
        # ── Phase 2e: SlicerDicer Save/Load Stats ─────────────────────────────
        {
            "step_prefix":    "Phase 2e - Backfill SlicerDicer Save/Load Stats",
            "source":         "caboodle",
            "target_table":   "raw_v2.SlicerDicerStatsSaveLoad",
            "target_columns": [
                "Instant", "Duration", "PopulationId",
                "IsLoad", "HttpRequestId", "Identifier",
            ],
            "date_column":    "Instant",
            "source_query":   """
                SELECT
                    DATEADD(MINUTE, DATEPART(TZ, SYSDATETIMEOFFSET()), Instant) AS Instant,
                    Duration,
                    PopulationId,
                    IsLoad,
                    HttpRequestId,
                    Identifier
                FROM slicerdicer.Stats_SaveLoad WITH (NOLOCK)
                WHERE Instant >= ?
                  AND Instant <  ?
            """,
        },
    ]


# ══════════════════════════════════════════════════════════════════════════════
# CHUNK EXECUTION (streaming, append-only)
# ══════════════════════════════════════════════════════════════════════════════

def run_backfill_chunk(defn: dict,
                       chunk_start: datetime, chunk_end: datetime,
                       exec_id: str, step_seq: int,
                       staging_conn, source_conn,
                       dry_run: bool = False) -> Tuple[int, str]:
    """Stream a single date-chunk extraction into the target raw_v2 table.

    Mirrors atlas_rundata_extractor.run_extraction() (commit 7a69a7e) with
    these differences:
        - NO TRUNCATE — appends to whatever is already in the target
        - query_params = [chunk_start, chunk_end] (Python datetimes)
        - step_name embeds the chunk's date range
        - On exception: logs Failure, returns (0, 'Failure') WITHOUT
          re-raising, so the outer loop can continue with the next chunk

    Connections are caller-owned: this function does not open or close
    staging_conn or source_conn.

    Returns (row_count, status) where status ∈ {'Success', 'Warning', 'Failure'}.
    """
    step_name = (
        f"{defn['step_prefix']} "
        f"{chunk_start.date()}..{chunk_end.date()}"
    )
    source_query = defn['source_query']
    target_table = defn['target_table']
    target_cols  = defn['target_columns']
    query_params = [chunk_start, chunk_end]

    logger.info(f"  {step_name}")
    start = datetime.now()

    log_id = log_start(staging_conn, exec_id, step_name, step_seq)

    try:
        source_conn.timeout = config.query_timeout_seconds
        source_cursor = source_conn.cursor()
        source_cursor.execute(source_query, *query_params)

        if dry_run:
            row_count = sum(1 for _ in source_cursor)
            logger.info(f"    [DRY RUN] {row_count:,} rows available")
            source_cursor.close()
            log_end(staging_conn, log_id, row_count)
            return row_count, 'Success'

        # ── Streaming fetch via fetchmany() + per-batch commit ──────────
        fetch_batch_size = config.fetch_batch_size
        source_cursor.arraysize = fetch_batch_size

        # ── Pre-build INSERT SQL and column-type hints BEFORE the loop ──
        placeholders = ', '.join(['?'] * len(target_cols))
        insert_sql = (
            f"INSERT INTO {target_table} ({', '.join(target_cols)}) "
            f"VALUES ({placeholders})"
        )

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
                input_sizes.append(
                    (pyodbc.SQL_DECIMAL, meta.NUMERIC_PRECISION, meta.NUMERIC_SCALE)
                )
            else:
                input_sizes.append(None)

        staging_cursor = staging_conn.cursor()
        staging_cursor.fast_executemany = True
        staging_cursor.setinputsizes(input_sizes)

        # ── Fetch first batch (no truncate — append-only) ──────────────
        first_batch = source_cursor.fetchmany(fetch_batch_size)

        if not first_batch:
            logger.warning(
                f"    WARNING: {step_name} returned 0 rows — "
                f"chunk may be outside source retention window or "
                f"genuinely empty."
            )
            source_cursor.close()
            staging_cursor.close()
            log_end(staging_conn, log_id, 0, 'Warning')
            return 0, 'Warning'

        # ── Stream remaining batches, append to target ──────────────────
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
        staging_cursor.close()

        elapsed = (datetime.now() - start).total_seconds()
        logger.info(
            f"    Inserted {row_count:,} rows into {target_table} "
            f"({batch_num} batches, {elapsed:.1f}s)"
        )

        log_end(staging_conn, log_id, row_count)
        return row_count, 'Success'

    except Exception as e:
        # str(e) is empty for some no-message exceptions (notably MemoryError
        # and certain ODBC driver errors). Fall back to repr(e) so the
        # @Message column always carries something diagnostic.
        error_msg = str(e) if str(e) else repr(e)
        logger.error(
            f"    FAILED: {step_name} — "
            f"{type(e).__name__}: {error_msg}"
        )
        # Clear any aborted transaction state on shared connections so the
        # next chunk can proceed. Both rollbacks are best-effort.
        try:
            staging_conn.rollback()
        except Exception:
            pass
        try:
            source_conn.rollback()
        except Exception:
            pass
        try:
            log_end(staging_conn, log_id, 0, 'Failure', error_msg)
        except Exception:
            pass
        # Per spec: do not re-raise. Return Failure status and let the
        # outer loop continue with the next chunk.
        return 0, 'Failure'


# ══════════════════════════════════════════════════════════════════════════════
# BACKFILL ORCHESTRATION
# ══════════════════════════════════════════════════════════════════════════════

def _generate_chunks(end_date: datetime, total_days: int,
                     chunk_days: int) -> list:
    """Build a list of [chunk_start, chunk_end) datetime tuples, oldest first.

    Last chunk's end is capped at end_date (no future dates). Abutting chunks
    share their boundary instant: chunk N's chunk_end equals chunk N+1's
    chunk_start. Because the queries use [start, end) (>= and <), no row is
    counted in two chunks.
    """
    start_date = end_date - timedelta(days=total_days)
    chunks = []
    cursor = start_date
    while cursor < end_date:
        chunk_end = min(cursor + timedelta(days=chunk_days), end_date)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end
    return chunks


def run_backfill(total_days: int = 180,
                 chunk_days: int = 30,
                 dry_run: bool = False) -> bool:
    """Run all 5 extractions over the past ``total_days`` in chunk_days slices.

    Returns True if every chunk completed Success or Warning; False if any
    chunk logged Failure.
    """
    exec_id = str(uuid.uuid4())

    # End date = midnight today (date only, time = 00:00:00). Eliminates
    # within-second drift between chunks.
    today = datetime.now().date()
    end_date = datetime.combine(today, time.min)

    chunks = _generate_chunks(end_date, total_days, chunk_days)
    extractions = build_backfill_extractions()

    logger.info("=" * 60)
    logger.info("ATLAS ETL — RunData Backfill (Pipeline B)")
    logger.info(f"Execution ID:   {exec_id}")
    logger.info(f"Date range:     {chunks[0][0].date()} to {end_date.date()}")
    logger.info(f"Total days:     {total_days}")
    logger.info(f"Chunk size:     {chunk_days} days")
    logger.info(f"Chunk count:    {len(chunks)} per extraction × "
                f"{len(extractions)} extractions = "
                f"{len(chunks) * len(extractions)} total")
    logger.info(f"Mode:           {'DRY RUN' if dry_run else 'LIVE (APPEND)'}")
    logger.info("=" * 60)

    if not dry_run:
        logger.warning(
            "APPEND MODE: this run inserts into raw_v2 tables without "
            "truncating. Re-running over an overlapping date range will "
            "produce duplicate rows. To safely re-run, manually truncate "
            "the target raw_v2 tables first."
        )

    # ── Open shared connections (one each, reused across all chunks) ──
    logger.info("Opening connections...")
    try:
        staging_conn  = get_staging_connection()
        clarity_conn  = get_clarity_connection()
        caboodle_conn = get_caboodle_connection()
    except Exception as e:
        logger.error(f"Failed to open connections: {type(e).__name__}: {e}")
        return False

    try:
        # Quick connectivity probe before any work.
        for label, c in (("Atlas Staging", staging_conn),
                         ("Clarity",       clarity_conn),
                         ("Caboodle",      caboodle_conn)):
            cur = c.cursor()
            cur.execute("SELECT @@SERVERNAME, DB_NAME()")
            row = cur.fetchone()
            logger.info(f"  {label} connected: {row[0]} / {row[1]}")
            cur.close()

        # Tracking
        step_seq = 0
        per_extraction_rows = {e['step_prefix']: 0 for e in extractions}
        per_extraction_status = {
            e['step_prefix']: {'Success': 0, 'Warning': 0, 'Failure': 0}
            for e in extractions
        }
        chunks_total = len(chunks) * len(extractions)
        chunks_done = 0
        backfill_start = datetime.now()

        # Outer loop: extraction. Inner loop: chunks (oldest first). This
        # ordering keeps each target table's chunks sequential — useful for
        # monitoring per-table progress and for resuming if a single
        # extraction's source connection turns flaky.
        for defn in extractions:
            source_conn = (clarity_conn if defn['source'] == 'clarity'
                           else caboodle_conn)
            logger.info("")
            logger.info(f"── {defn['step_prefix']} → {defn['target_table']} ──")
            for chunk_start, chunk_end in chunks:
                step_seq += 1
                chunks_done += 1
                logger.info(
                    f"[{chunks_done}/{chunks_total}] "
                    f"chunk {chunk_start.date()}..{chunk_end.date()}"
                )
                rows, status = run_backfill_chunk(
                    defn, chunk_start, chunk_end, exec_id, step_seq,
                    staging_conn=staging_conn,
                    source_conn=source_conn,
                    dry_run=dry_run,
                )
                per_extraction_rows[defn['step_prefix']] += rows
                per_extraction_status[defn['step_prefix']][status] += 1

        elapsed = (datetime.now() - backfill_start).total_seconds()

        # ── Summary ──────────────────────────────────────────────────
        total_failures = sum(
            s['Failure'] for s in per_extraction_status.values()
        )
        total_warnings = sum(
            s['Warning'] for s in per_extraction_status.values()
        )
        total_successes = sum(
            s['Success'] for s in per_extraction_status.values()
        )

        logger.info("")
        logger.info("=" * 60)
        logger.info("BACKFILL COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Execution ID:           {exec_id}")
        logger.info(f"Chunks attempted:       {chunks_done}")
        logger.info(f"  Success:              {total_successes}")
        logger.info(f"  Warning (zero rows):  {total_warnings}")
        logger.info(f"  Failure:              {total_failures}")
        logger.info(f"Elapsed:                "
                    f"{elapsed:.1f}s ({elapsed / 60:.1f} min)")
        logger.info("")
        logger.info("Per-extraction summary:")
        for defn in extractions:
            prefix = defn['step_prefix']
            stats = per_extraction_status[prefix]
            logger.info(
                f"  {prefix}: "
                f"{per_extraction_rows[prefix]:,} rows "
                f"(S={stats['Success']} "
                f"W={stats['Warning']} "
                f"F={stats['Failure']})"
            )
        logger.info("=" * 60)

        if total_failures > 0:
            logger.error(
                f"{total_failures} chunk(s) FAILED — review etl.Atlas_ETL_Log "
                f"for ExecutionID = '{exec_id}' to identify which date ranges "
                f"need manual retry."
            )
            return False

        logger.info(
            "Next step: run usp_Atlas_RunData and usp_Atlas_Merge manually "
            "to load backfilled data into production."
        )
        return True

    finally:
        # Close all three shared connections regardless of outcome.
        for conn in (staging_conn, clarity_conn, caboodle_conn):
            try:
                conn.close()
            except Exception:
                pass


# ══════════════════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='Atlas RunData Historical Backfill (Pipeline B)',
    )
    parser.add_argument(
        '--days', type=int, default=180,
        help='Total historical days to backfill (default: 180).',
    )
    parser.add_argument(
        '--chunk-days', type=int, default=30,
        help='Size of each date chunk in days (default: 30).',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Count rows only — do not insert into target tables.',
    )
    parser.add_argument(
        '--verbose', action='store_true',
        help='Debug logging.',
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    success = run_backfill(
        total_days=args.days,
        chunk_days=args.chunk_days,
        dry_run=args.dry_run,
    )
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
