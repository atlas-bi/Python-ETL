"""
Atlas ETL Migration — Pipeline B: atlas_db_objects_extractor.py

Replaces: usp_Atlas_DatabaseObjects Step 2 (linked server extractions)
Amendment: Pipeline B eliminates the linked server dependency by using
           direct Python/pyodbc connections to the Clarity host.

This script connects directly to each database on the Clarity server
(Clarity, RW_PUB, EDM, CogitoTools) and extracts sys.all_objects metadata.
Results are bulk-inserted into raw_v2.DatabaseObjects in Atlas_Staging.

After this script completes, usp_Atlas_DatabaseObjects_pipelineB runs
Steps 3-4 (staging transforms) using only LOCAL raw_v2 tables.

Pipeline position:
    atlas_db_objects_extractor.py  →  usp_Atlas_DatabaseObjects (staging only)
                                  →  atlas_query_hierarchy.py

Dependencies:
    - Python 3.11+ with pyodbc, pandas
    - Network connectivity to EPICCLAPRD (port 1433)
    - Network connectivity to Atlas_Staging (10.247.4.56\\SQL126)
    - atlas_config.py for connection strings and database list
    - raw_v2 schema (created by usp_Atlas_Setup in Week 1)

Usage:
    python atlas_db_objects_extractor.py                    # All databases
    python atlas_db_objects_extractor.py --db Clarity       # Single database
    python atlas_db_objects_extractor.py --dry-run          # Count only
    python atlas_db_objects_extractor.py --verbose          # Debug logging

Author:  Larry Duren
Date:    March 2026
Version: 2.1 (Pipeline B Amendment)
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
        # CN2 (2026-04-07): service account credentials for Epic Clarity.
        # The Clarity service account is used for every database on the
        # Clarity host (Clarity / RW_PUB / EDM / CogitoTools). When empty,
        # the builder falls through to Trusted_Connection=yes. When
        # populated via env var, UID/PWD is used.
        #
        # Pre-existing issue flagged for a future commit (out of scope for CN2):
        # this fallback stub defines get_epic_clarity_db_connection_string()
        # while the live call site in extract_database() (line 203) invokes
        # config.get_epic_clarity_connection_string_for_db(database). The
        # names diverge and the fallback stub method would raise AttributeError
        # if atlas_config ever failed to import. Not fixed here — CN2 is
        # scope-locked to the SQL Auth switch.
        epic_clarity_sql_user = os.getenv('EPIC_CLARITY_SQL_USER', '')
        epic_clarity_sql_password = os.getenv('EPIC_CLARITY_SQL_PWD', '')
        atlas_staging_server = os.getenv('ATLAS_STAGING_SERVER', r'10.247.4.56\SQL126')
        atlas_staging_database = os.getenv('ATLAS_STAGING_DB', 'Atlas_Staging')
        query_timeout_seconds = int(os.getenv('ATLAS_QUERY_TIMEOUT', '3600'))
        connection_timeout_seconds = 30
        bulk_insert_batch_size = 10000
        log_level = 'INFO'
        # SVC_REPORTHUB_CLARITY SQL credentials (credentials.json) have
        # access to EDM and CogitoTools on EPICCLAPRD.
        database_objects_sources = ('Clarity', 'RW_PUB', 'EDM', 'CogitoTools')
        def get_epic_clarity_db_connection_string(self, database):
            if self.epic_clarity_sql_user:
                auth = (f"UID={self.epic_clarity_sql_user};"
                        f"PWD={self.epic_clarity_sql_password};")
            else:
                auth = "Trusted_Connection=yes;"
            return (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.epic_clarity_server};"
                f"DATABASE={database};"
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
logger = logging.getLogger('atlas_db_objects_extractor')

PACKAGE_NAME = 'ETL-DatabaseObjects-Extract'


# ══════════════════════════════════════════════════════════════════════════════
# ETL LOGGING
# ══════════════════════════════════════════════════════════════════════════════

def get_staging_connection() -> pyodbc.Connection:
    return pyodbc.connect(
        config.get_atlas_staging_connection_string(),
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
# EXTRACTION QUERY
# This is the same query that was previously run via linked server dynamic SQL
# in usp_Atlas_DatabaseObjects Step 2.
# ══════════════════════════════════════════════════════════════════════════════

OBJECTS_QUERY = """
SELECT 
    o.name              AS Name,
    m.definition        AS Query,
    CASE o.type
        WHEN 'P'  THEN 'SQL Stored Procedure'
        WHEN 'V'  THEN 'SQL View'
        WHEN 'FN' THEN 'SQL Function'
        WHEN 'TF' THEN 'SQL Table-Valued Function'
        WHEN 'IF' THEN 'SQL Inline Table-Valued Function'
        ELSE 'SQL ' + o.type_desc
    END                 AS ReportObjectType,
    s.name              AS SourceSchema,
    o.object_id         AS ObjectID,
    o.modify_date       AS LastModifiedDate,
    CAST(ep.value AS NVARCHAR(MAX)) AS [Description]
FROM sys.all_objects o
LEFT OUTER JOIN sys.schemas s
    ON o.schema_id = s.schema_id
LEFT JOIN sys.sql_modules m 
    ON o.object_id = m.object_id
LEFT JOIN sys.extended_properties ep 
    ON o.object_id = ep.major_id 
    AND ep.minor_id = 0 
    AND ep.name = 'MS_Description'
WHERE o.type IN ('P', 'V', 'FN')
  AND o.is_ms_shipped = 0
ORDER BY s.name, o.name
"""

TARGET_COLUMNS = [
    "SourceServer", "SourceDB", "SourceSchema", "Name", "Query",
    "ReportObjectType", "ObjectID", "LastModifiedDate",
    "DefaultVisibilityYN", "Description",
]


# ══════════════════════════════════════════════════════════════════════════════
# CORE EXTRACTION LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def extract_database(database: str, exec_id: str, step_seq: int,
                     dry_run: bool = False) -> int:
    """
    Extract sys.all_objects from a single database on the Clarity server.
    
    Returns row count inserted.
    """
    step_name = f"Extract from {database}"
    logger.info(f"  {step_name}")
    start = datetime.now()
    
    staging_conn = get_staging_connection()
    log_id = log_start(staging_conn, exec_id, step_name, step_seq)
    
    try:
        # ── Connect directly to the target database on Clarity server ─────
        db_conn_str = config.get_epic_clarity_connection_string_for_db(database)
        db_conn = pyodbc.connect(db_conn_str, timeout=config.connection_timeout_seconds)
        cursor = db_conn.cursor()
        cursor.execute(OBJECTS_QUERY)
        
        if dry_run:
            row_count = sum(1 for _ in cursor)
            logger.info(f"    [DRY RUN] {row_count:,} objects in {database}")
            cursor.close()
            db_conn.close()
            log_end(staging_conn, log_id, row_count)
            staging_conn.close()
            return row_count
        
        # ── Fetch results ─────────────────────────────────────────────────
        rows = cursor.fetchall()
        cursor.close()
        db_conn.close()
        
        row_count = len(rows)
        logger.info(f"    Fetched {row_count:,} objects from {database}")
        
        if row_count == 0:
            log_end(staging_conn, log_id, 0)
            staging_conn.close()
            return 0
        
        # ── Build records with SourceServer / SourceDB columns ────────────
        records = []
        for r in rows:
            records.append((
                config.epic_clarity_server,  # SourceServer
                database,                     # SourceDB
                r.SourceSchema,               # SourceSchema
                r.Name,                       # Name
                r.Query,                      # Query
                r.ReportObjectType,           # ReportObjectType
                r.ObjectID,                   # ObjectID
                r.LastModifiedDate,           # LastModifiedDate
                'N',                          # DefaultVisibilityYN
                r.Description,                # Description
            ))
        
        # ── Bulk insert into raw_v2.DatabaseObjects ───────────────────────
        staging_cursor = staging_conn.cursor()
        staging_cursor.fast_executemany = True

        # Query column metadata to set explicit types for numeric/decimal columns.
        # Prevents fast_executemany from inferring INT for NUMERIC(18,0) columns,
        # which causes "Numeric value out of range" (SQLSTATE 22003) on large values.
        meta_cursor = staging_conn.cursor()
        meta_cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'raw_v2' AND TABLE_NAME = 'DatabaseObjects'
            ORDER BY ORDINAL_POSITION
        """)
        col_meta = {row.COLUMN_NAME: row for row in meta_cursor.fetchall()}
        meta_cursor.close()
        input_sizes = []
        for col in TARGET_COLUMNS:
            meta = col_meta.get(col)
            if meta and meta.DATA_TYPE in ('numeric', 'decimal'):
                input_sizes.append((pyodbc.SQL_DECIMAL, meta.NUMERIC_PRECISION, meta.NUMERIC_SCALE))
            else:
                input_sizes.append(None)
        staging_cursor.setinputsizes(input_sizes)

        placeholders = ', '.join(['?'] * len(TARGET_COLUMNS))
        insert_sql = (
            f"INSERT INTO raw_v2.DatabaseObjects "
            f"({', '.join(TARGET_COLUMNS)}) VALUES ({placeholders})"
        )

        batch_size = config.bulk_insert_batch_size
        for i in range(0, len(records), batch_size):
            staging_cursor.executemany(insert_sql, records[i:i + batch_size])
        
        staging_conn.commit()
        staging_cursor.close()
        
        elapsed = (datetime.now() - start).total_seconds()
        logger.info(f"    Inserted {row_count:,} objects ({elapsed:.1f}s)")
        
        log_end(staging_conn, log_id, row_count)
        staging_conn.close()
        return row_count
    
    except Exception as e:
        error_msg = str(e)
        logger.error(f"    FAILED: {step_name} — {error_msg}")
        try:
            log_end(staging_conn, log_id, 0, 'Failure', error_msg)
            staging_conn.close()
        except Exception:
            pass
        raise


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def run_all_extractions(db_filter: Optional[str] = None,
                        dry_run: bool = False) -> bool:
    """
    Extract database objects from all (or one) databases on Clarity server.
    """
    exec_id = os.getenv('ATLAS_EXECUTION_ID', str(uuid.uuid4()))
    step_seq = 0
    total_rows = 0
    start_time = datetime.now()
    
    databases = list(config.database_objects_sources)
    if db_filter:
        if db_filter not in databases:
            logger.error(f"Unknown database: '{db_filter}'. Valid: {', '.join(databases)}")
            return False
        databases = [db_filter]
    
    logger.info("=" * 60)
    logger.info("ATLAS ETL — Database Objects Extractor (Pipeline B)")
    logger.info(f"Execution ID: {exec_id}")
    logger.info(f"Source server: {config.epic_clarity_server}")
    logger.info(f"Databases:     {', '.join(databases)}")
    logger.info(f"Mode:          {'DRY RUN' if dry_run else 'LIVE'}")
    logger.info("=" * 60)

    # Test Clarity connectivity first
    logger.info("Testing Clarity connection...")
    try:
        test_conn_str = config.get_epic_clarity_connection_string_for_db(databases[0])
        test_conn = pyodbc.connect(test_conn_str, timeout=config.connection_timeout_seconds)
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

    # Step 1: Drop and recreate raw table (matches original SP behavior)
    if not dry_run:
        step_seq += 1
        logger.info("  Step 1: Recreate raw_v2.DatabaseObjects")
        staging_conn = get_staging_connection()
        log_id = log_start(staging_conn, exec_id, 'Step 1 - Recreate Raw Table', step_seq)
        try:
            cursor = staging_conn.cursor()
            cursor.execute("""
                IF OBJECT_ID('raw_v2.DatabaseObjects', 'U') IS NOT NULL
                    DROP TABLE raw_v2.DatabaseObjects;
                
                CREATE TABLE raw_v2.DatabaseObjects (
                    SourceServer        NVARCHAR(200) NOT NULL,
                    SourceDB            NVARCHAR(128) NOT NULL,
                    SourceSchema        NVARCHAR(128) NULL,
                    Name                NVARCHAR(128) NOT NULL,
                    Query               NVARCHAR(MAX) NULL,
                    ReportObjectType    NVARCHAR(50) NOT NULL,
                    ObjectID            INT NOT NULL,
                    LastModifiedDate    DATETIME NULL,
                    DefaultVisibilityYN NVARCHAR(1) NOT NULL DEFAULT 'Y',
                    [Description]       NVARCHAR(MAX) NULL,
                    ExtractDate         DATETIME NOT NULL DEFAULT GETDATE()
                );
            """)
            staging_conn.commit()
            cursor.close()
            log_end(staging_conn, log_id, 1)
            staging_conn.close()
            logger.info("    raw_v2.DatabaseObjects recreated")
        except Exception as e:
            logger.error(f"    Failed to recreate table: {e}")
            log_end(staging_conn, log_id, 0, 'Failure', str(e))
            staging_conn.close()
            return False
    
    # Step 2: Extract from each database
    failed = []
    for db_name in databases:
        step_seq += 1
        try:
            rows = extract_database(db_name, exec_id, step_seq, dry_run)
            total_rows += rows
        except Exception as e:
            logger.error(f"  Extraction '{db_name}' failed: {e}")
            failed.append(db_name)

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"Database objects extraction complete: {total_rows:,} total objects ({elapsed:.1f}s)")
    if failed:
        logger.error(f"FAILED databases: {', '.join(failed)}")
    logger.info("=" * 60)
    
    return len(failed) == 0


def main():
    parser = argparse.ArgumentParser(description='Atlas DB Objects Extractor (Pipeline B)')
    parser.add_argument('--db', type=str, default=None,
                        help=f"Extract single DB. Options: {', '.join(config.database_objects_sources)}")
    parser.add_argument('--dry-run', action='store_true', help='Count only')
    parser.add_argument('--verbose', action='store_true', help='Debug logging')
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    success = run_all_extractions(db_filter=args.db, dry_run=args.dry_run)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
