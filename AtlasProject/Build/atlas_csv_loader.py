"""
Atlas ETL Suite - CSV Loader (V2 Schema)
=========================================
Migrated from: ETL-Clarity SSIS Data Flow Tasks (2 DFTs handling 9 CSV files)

Loads 9 CSV flat files from the network share into raw_v2 tables in
Atlas_Staging. Each file is read with pandas, validated, and bulk-inserted
via pyodbc fast_executemany.

This script runs AFTER usp_Atlas_Clarity completes the SQL extractions.
The orchestrator calls this script between the Clarity SP and DatabaseObjects.

*** UPDATED: Uses atlas_config for connection/paths, SP-based ETL logging ***

Usage:
    python atlas_csv_loader.py                    # Load all 9 CSVs
    python atlas_csv_loader.py --file paf         # Load single file by key
    python atlas_csv_loader.py --dry-run          # Validate only, no insert
    python atlas_csv_loader.py --limit 1000       # Load first N rows per file
    python atlas_csv_loader.py --verbose          # Show batch-level debug logging

Version: 3.2.1
Last Updated: March 2026
"""

import argparse
import logging
import sys
import uuid
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

try:
    import pyodbc
    import pandas as pd
except ImportError as e:
    print(f"ERROR: Required package not installed: {e}")
    print("Install with: pip install pyodbc pandas")
    sys.exit(1)

try:
    from atlas_config import config
except ImportError:
    print("ERROR: atlas_config.py not found. Ensure it's in the same directory.")
    sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('atlas_csv_loader')


# ══════════════════════════════════════════════════════════════════════════════
# CSV FILE MANIFEST
# Maps logical names → (filename, target table, expected columns)
# ══════════════════════════════════════════════════════════════════════════════

CSV_MANIFEST = {
    # ── Files WITH a header row (ColumnNamesInFirstDataRow="True") ────────
    "paf": {
        "filename": "Atlas_-_Columns_Extract.csv",
        "target_table": "raw_v2.clarity_paf",
        # expected_columns = DB destination column names (after rename)
        "expected_columns": [
            "Column ID", "Name", "Description", "AssocApps",
            "DataType", "ColumnINI", "ColumnItem", "Extension",
            "PAFFieldType", "ExtensionParameter"
        ],
        # CSV headers differ from DB columns; SSIS renames on insert
        # Note: actual CSV header is "Assoc. Apps" (dot+space), not "Assoc  Apps"
        "column_renames": {
            "Assoc. Apps": "AssocApps",
            "Data Type": "DataType",
            "INI": "ColumnINI",
            "Item": "ColumnItem",
            "Field Type": "PAFFieldType",
            "Column Extension Parameter": "ExtensionParameter",
        },
        "encoding": "cp1252",
        "delimiter": ",",
        "has_header": True,           # header=True + DataRowsToSkip=1
        "skiprows": 1,                # SSIS skips 1 row after header
    },
    "hrx_column_mapping": {
        "filename": "Atlas_-_Report_Column_Mapping_Extract.csv",
        "target_table": "raw_v2.clarity_hrx_column_mapping",
        # expected_columns = DB destination column names (after rename+drop)
        "expected_columns": [
            "ReportID", "Name", "ReportTemplate", "ReportType",
            "OwnedBy", "Availability", "TemplateType", "Created",
            "HRXSelectedFields"
        ],
        # File has NO header row — SSIS defines 10 positional columns
        # Column order from SSIS connection manager:
        #   Report ID, HRX CID, Name, Report Template, Report Type,
        #   Owned By, Availability, Template Type, Created, HRX Selected Fields
        "column_renames": {
            "Report ID": "ReportID",
            "Report Template": "ReportTemplate",
            "Report Type": "ReportType",
            "Owned By": "OwnedBy",
            "Template Type": "TemplateType",
            "HRX Selected Fields": "HRXSelectedFields",
        },
        "drop_columns": ["HRX CID"],
        "encoding": "cp1252",
        "delimiter": ",",
        "has_header": False,          # no header row in actual file
        "csv_columns": [
            "Report ID", "HRX CID", "Name", "Report Template",
            "Report Type", "Owned By", "Availability",
            "Template Type", "Created", "HRX Selected Fields"
        ],
    },
    "slicerdicer_sessions": {
        "filename": "Atlas_-_SlicerDicer_Sessions_Extract.csv",
        "target_table": "raw_v2.clarity_slicerdicer_sessions",
        # expected_columns = DB destination column names (after rename)
        "expected_columns": [
            "Report_ID", "Name", "Report_type", "Description",
            "Record_type", "Created_by", "Created",
            "Last_modified_by", "Last_modified_date",
            "Display_title", "Data_model"
        ],
        # CSV headers differ from DB columns; SSIS renames on insert
        # CSV order: Report ID, Name, Report Type, Created By, Created,
        #   Record Type, Last Edit User, Report Last Modified Instant,
        #   Display Name, Data Model, Report Template
        "column_renames": {
            "Report ID": "Report_ID",
            "Report Type": "Report_type",
            "Created By": "Created_by",
            "Record Type": "Record_type",
            "Last Edit User": "Last_modified_by",
            "Report Last Modified Instant": "Last_modified_date",
            "Display Name": "Display_title",
            "Data Model": "Data_model",
            "Report Template": "Description",   # SSIS maps Report Template → Description
        },
        "encoding": "cp1252",
        "delimiter": ",",
        "has_header": True,           # header=True + DataRowsToSkip=1
        "skiprows": 1,                # SSIS skips 1 row after header
    },
    "code_template": {
        "filename": "Atlas_-_Code_Template_Extract.csv",
        "target_table": "raw_v2.code_template",
        "expected_columns": [
            "Code Template ID", "Code Template Name",
            "Code Template Description", "Code Template Template Type",
            "Code Template INI",
            "Code Template Programming Point Definition Item",
            "Code Template M Code", "Code Template Parameter ID"
        ],
        "encoding": "cp1252",
        "delimiter": ",",
        "has_header": True,
    },
    "e3n_export": {
        "filename": "CodeTemplates.csv",
        "target_table": "raw_v2.E3N_Export",
        "expected_columns": [
            "DEFINITION ID", "DEFINITION NAME", "RECORD DESCRIPTION",
            "PARAMETER ID", "PARAMETER ID RECORD NAME"
        ],
        "encoding": "cp1252",
        "delimiter": ",",
        "has_header": True,
        # nvarchar(MAX) + fast_executemany caps buffer at 8000 bytes; 467 rows, no perf impact
        "fast_executemany": False,
    },
    "fds": {
        "filename": "Atlas_-_SlicerDicer_Filters_Extract.csv",
        "target_table": "raw_v2.clarity_fds",
        "expected_columns": [
            "Filter ID", "Filter Name", "Filter Description",
            "Record Type", "Filter Category", "Filter Data Type",
            "Filter Display Name", "Is Filter Inactive?",
            "Is Filter Sensitive?", "Is Filter Column Only?",
            "Overall Review Status", "Last Review Date", "Reviewers",
            "Filter Information Table Name",
            "Filter Information Data Expression",
            "Supported Summary Level", "Filter Data Source"
        ],
        "encoding": "cp1252",
        "delimiter": ",",
        "has_header": True,
    },
    # ── Files WITHOUT a header row ────────────────────────────────────────
    "slicerdicer_public_sessions": {
        "filename": "Atlas_-_SlicerDicer_Public_Sessions_Extract.csv",
        "target_table": "raw_v2.clarity_slicerdicer_public_sessions",
        "expected_columns": [
            "Session_ID"
        ],
        "encoding": "cp1252",
        "delimiter": ",",            # single-column, newline-delimited
        "has_header": False,
        "skiprows": 1,                # SSIS DataRowsToSkip=1
    },
    "fds_fdm_map": {
        "filename": "Atlas_-_SlicerDicer_Filter_Data_Model_Map.csv",
        "target_table": "raw_v2.fds_fdm_map",
        "expected_columns": [
            "FDS ID", "FDM ID"
        ],
        "encoding": "cp1252",
        "delimiter": ",",
        "has_header": False,
    },
    "hrx_parameter_logic": {
        "filename": "BILH_HRX_Parameter_Logic.csv",
        "target_table": "raw_v2.clarity_intraparameter_logic",
        "expected_columns": [
            "HRX ID", "Crit Uniq", "Intraparam Logic"
        ],
        "encoding": "cp1252",
        "delimiter": "|",            # pipe-delimited per SSIS _x007C_
        "has_header": False,
    },
}


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE & ETL LOGGING (matches atlas_query_hierarchy.py pattern)
# ══════════════════════════════════════════════════════════════════════════════

PACKAGE_NAME = 'ETL-Clarity-CSV'


def get_connection() -> pyodbc.Connection:
    """Create a connection to Atlas Staging database."""
    conn_str = config.get_atlas_staging_connection_string()
    return pyodbc.connect(conn_str, timeout=config.connection_timeout_seconds)


def log_start(conn: pyodbc.Connection, exec_id: str, step: str, seq: int) -> int:
    """Call etl.usp_Atlas_LogStart and return LogID."""
    cursor = conn.cursor()
    cursor.execute("""
        DECLARE @LogID BIGINT;
        EXEC etl.usp_Atlas_LogStart
            @ExecutionID = ?,
            @PackageName = ?,
            @StepName = ?,
            @StepSequence = ?,
            @LogID = @LogID OUTPUT;
        SELECT @LogID;
    """, exec_id, PACKAGE_NAME, step, seq)

    log_id = cursor.fetchone()[0]
    conn.commit()
    return log_id


def log_end(conn: pyodbc.Connection, log_id: int, rows: int, status: str = 'Success'):
    """Call etl.usp_Atlas_LogEnd."""
    cursor = conn.cursor()
    cursor.execute("""
        EXEC etl.usp_Atlas_LogEnd
            @LogID = ?,
            @RowsAffected = ?,
            @Status = ?
    """, log_id, rows, status)
    conn.commit()


def log_error(conn: pyodbc.Connection, log_id: int, error_msg: str):
    """Call etl.usp_Atlas_LogErrorManual."""
    cursor = conn.cursor()
    cursor.execute("""
        EXEC etl.usp_Atlas_LogErrorManual
            @LogID = ?,
            @ErrorMessage = ?
    """, log_id, error_msg[:4000])
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# CSV VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def validate_csv(
    df: pd.DataFrame,
    expected_columns: List[str],
) -> Tuple[bool, List[str]]:
    """
    Validate a loaded DataFrame against expected schema.

    Returns:
        (is_valid, list_of_warnings)
    """
    warnings = []

    if df.empty:
        return False, ["File is empty (0 rows)"]

    # Check column names (case-insensitive match)
    actual_cols = [c.strip() for c in df.columns.tolist()]
    expected_lower = [c.lower() for c in expected_columns]
    actual_lower = [c.lower() for c in actual_cols]

    missing = set(expected_lower) - set(actual_lower)
    extra = set(actual_lower) - set(expected_lower)

    if missing:
        warnings.append(f"Missing expected columns: {missing}")
    if extra:
        warnings.append(f"Extra columns found (will be ignored): {extra}")

    # Warn on high null percentage
    for col in expected_columns:
        matching_col = None
        for ac in actual_cols:
            if ac.lower() == col.lower():
                matching_col = ac
                break
        if matching_col and matching_col in df.columns:
            null_pct = df[matching_col].isna().mean() * 100
            if null_pct > 50:
                warnings.append(f"Column '{col}' is {null_pct:.1f}% null")

    is_valid = len(missing) == 0
    return is_valid, warnings


# ══════════════════════════════════════════════════════════════════════════════
# BULK INSERT
# ══════════════════════════════════════════════════════════════════════════════

def bulk_insert_df(
    conn: pyodbc.Connection,
    df: pd.DataFrame,
    target_table: str,
    expected_columns: List[str],
    batch_size: int = 5000,
    fast_executemany: bool = True,
) -> int:
    """
    Truncate target table and bulk insert DataFrame rows.
    Uses fast_executemany for performance.

    Returns:
        Number of rows inserted.
    """
    cursor = conn.cursor()

    # Truncate
    logger.debug(f"Truncating {target_table}")
    cursor.execute(f"TRUNCATE TABLE {target_table}")
    conn.commit()

    # Map DataFrame columns to expected columns (case-insensitive)
    actual_cols = df.columns.tolist()
    col_map = {}
    for exp_col in expected_columns:
        for act_col in actual_cols:
            if act_col.lower().strip() == exp_col.lower():
                col_map[exp_col] = act_col
                break

    # Select only mapped columns, in expected order
    mapped_cols = [col_map[c] for c in expected_columns if c in col_map]
    insert_df = df[mapped_cols].copy()

    # Clean string data
    for col in insert_df.select_dtypes(include=["object"]).columns:
        insert_df[col] = insert_df[col].where(insert_df[col].notna(), None)
        insert_df[col] = insert_df[col].apply(
            lambda x: str(x).strip() if x is not None else None
        )

    # Build parameterized INSERT (bracket column names for spaces/special chars)
    target_cols = [c for c in expected_columns if c in col_map]
    placeholders = ", ".join(["?"] * len(target_cols))
    col_list = ", ".join([f"[{c}]" for c in target_cols])
    insert_sql = f"INSERT INTO {target_table} ({col_list}) VALUES ({placeholders})"

    # Execute in batches with fast_executemany
    cursor.fast_executemany = fast_executemany
    total_rows = 0

    for start in range(0, len(insert_df), batch_size):
        batch = insert_df.iloc[start : start + batch_size]
        rows = [tuple(row) for row in batch.itertuples(index=False, name=None)]
        cursor.executemany(insert_sql, rows)
        conn.commit()
        total_rows += len(rows)
        logger.debug(f"  Inserted batch: rows {start + 1} to {start + len(rows)}")

    return total_rows


# ══════════════════════════════════════════════════════════════════════════════
# LOAD SINGLE CSV
# ══════════════════════════════════════════════════════════════════════════════

def load_csv_file(
    conn: pyodbc.Connection,
    exec_id: str,
    step_seq: int,
    key: str,
    manifest_entry: dict,
    csv_base_path: str,
    dry_run: bool = False,
    limit: Optional[int] = None,
) -> Tuple[bool, int]:
    """
    Load a single CSV file into its target raw_v2 table.

    Returns:
        (success, rows_loaded)
    """
    filename = manifest_entry["filename"]
    target_table = manifest_entry["target_table"]
    expected_columns = manifest_entry["expected_columns"]
    file_encoding = manifest_entry.get("encoding", "cp1252")
    file_delimiter = manifest_entry.get("delimiter", ",")
    has_header = manifest_entry.get("has_header", True)
    skiprows = manifest_entry.get("skiprows", None)
    file_path = os.path.join(csv_base_path, filename)
    step_name = f"Load CSV: {key} ({filename})"

    log_id = log_start(conn, exec_id, step_name, step_seq)
    logger.info(f"Loading: {filename} → {target_table}")
    logger.info(f"  Settings: encoding={file_encoding}, delimiter={repr(file_delimiter)}, header={has_header}")

    # Check file exists
    if not os.path.isfile(file_path):
        msg = f"File not found: {file_path}"
        logger.error(f"  {msg}")
        log_error(conn, log_id, msg)
        log_end(conn, log_id, 0, 'Failure')
        return False, 0

    # Read CSV with per-file settings; fall back to latin-1 on decode errors
    try:
        read_kwargs = {
            "filepath_or_buffer": file_path,
            "sep": file_delimiter,
            "low_memory": False,
            "dtype": str,
            "na_values": ["", "NULL", "null", "N/A", "n/a"],
            "nrows": limit,
        }

        # Header handling
        # csv_columns overrides expected_columns for headerless files that
        # have extra/renamed columns (e.g. HRX Column Mapping has 10 CSV
        # columns but only 9 map to the destination after drop/rename)
        csv_columns = manifest_entry.get("csv_columns", None)
        if has_header:
            read_kwargs["header"] = 0
        else:
            read_kwargs["header"] = None
            read_kwargs["names"] = csv_columns if csv_columns else expected_columns

        # Skip rows (SSIS DataRowsToSkip — rows after header to discard)
        if skiprows and has_header:
            # skip row(s) between header and data
            read_kwargs["skiprows"] = list(range(1, skiprows + 1))
        elif skiprows and not has_header:
            # no header: skip first N raw rows
            read_kwargs["skiprows"] = skiprows

        # Try primary encoding, fall back to latin-1
        try:
            read_kwargs["encoding"] = file_encoding
            df = pd.read_csv(**read_kwargs)
        except (UnicodeDecodeError, UnicodeError):
            logger.warning(f"  {file_encoding} decode failed, falling back to latin-1")
            read_kwargs["encoding"] = "latin-1"
            df = pd.read_csv(**read_kwargs)

        logger.info(f"  Read {len(df):,} rows, {len(df.columns)} columns from {filename}")

        # Apply column renames (CSV header → DB column name, matching SSIS)
        column_renames = manifest_entry.get("column_renames", {})
        if column_renames:
            df = df.rename(columns=column_renames)
            logger.info(f"  Renamed {len(column_renames)} columns per SSIS mapping")

        # Drop columns not mapped to destination (e.g. HRX CID)
        drop_columns = manifest_entry.get("drop_columns", [])
        if drop_columns:
            existing_drops = [c for c in drop_columns if c in df.columns]
            if existing_drops:
                df = df.drop(columns=existing_drops)
                logger.info(f"  Dropped unmapped columns: {existing_drops}")

    except Exception as e:
        msg = f"CSV read error: {e}"
        logger.error(f"  {msg}")
        log_error(conn, log_id, msg)
        log_end(conn, log_id, 0, 'Failure')
        return False, 0

    # Validate
    is_valid, warnings = validate_csv(df, expected_columns)
    for w in warnings:
        logger.warning(f"  Validation: {w}")

    if not is_valid:
        msg = "Validation failed: missing required columns"
        logger.error(f"  {msg}")
        log_error(conn, log_id, msg)
        log_end(conn, log_id, 0, 'Failure')
        return False, 0

    if dry_run:
        logger.info(f"  DRY RUN: Would insert {len(df):,} rows into {target_table}")
        log_end(conn, log_id, len(df), 'Success')
        return True, len(df)

    # Bulk insert
    try:
        fast_exec = manifest_entry.get("fast_executemany", True)
        rows_loaded = bulk_insert_df(conn, df, target_table, expected_columns, fast_executemany=fast_exec)
        logger.info(f"  Loaded {rows_loaded:,} rows")
        log_end(conn, log_id, rows_loaded, 'Success')
        return True, rows_loaded
    except Exception as e:
        msg = f"Insert error: {e}"
        logger.error(f"  {msg}")
        log_error(conn, log_id, msg)
        log_end(conn, log_id, 0, 'Failure')
        return False, 0


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Atlas ETL — CSV Loader (Week 3: Clarity flat files)"
    )
    parser.add_argument(
        "--file", "-f",
        choices=list(CSV_MANIFEST.keys()),
        help="Load a single CSV file by key (default: load all)",
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Validate files only; do not insert data",
    )
    parser.add_argument(
        "--limit",
        type=int, default=None,
        help="Limit rows read per CSV (for testing)",
    )
    parser.add_argument(
        "--csv-path",
        default=config.csv_network_share,
        help=f"Override CSV base path (default from config: {config.csv_network_share})",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test database connection and exit",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose/debug logging (shows batch progress, truncate ops)",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger('atlas_csv_loader').setLevel(logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)

    # Use orchestrator's execution ID if available, else generate new
    exec_id = os.getenv('ATLAS_EXECUTION_ID', str(uuid.uuid4()))

    logger.info("=" * 60)
    logger.info("Atlas CSV Loader — Starting")
    logger.info(f"Execution ID: {exec_id}")
    logger.info(f"CSV Path:     {args.csv_path}")
    logger.info(f"Target DB:    {config.atlas_staging_server} / {config.atlas_staging_database}")
    logger.info(f"Dry Run:      {args.dry_run}")
    logger.info("=" * 60)

    try:
        conn = get_connection()
        logger.info("Database connection established")
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        sys.exit(1)

    if args.test:
        cursor = conn.cursor()
        cursor.execute("SELECT @@SERVERNAME, DB_NAME(), SYSTEM_USER")
        row = cursor.fetchone()
        logger.info(f"  Server: {row[0]}, Database: {row[1]}, User: {row[2]}")
        conn.close()
        logger.info("Connection test successful!")
        sys.exit(0)

    # Determine which files to process
    if args.file:
        files_to_load = {args.file: CSV_MANIFEST[args.file]}
    else:
        files_to_load = CSV_MANIFEST

    # Process each file
    total_rows = 0
    failures = 0
    step_seq = 0

    for key, entry in files_to_load.items():
        step_seq += 1
        success, rows = load_csv_file(
            conn, exec_id, step_seq, key, entry,
            args.csv_path,
            dry_run=args.dry_run,
            limit=args.limit,
        )
        total_rows += rows
        if not success:
            failures += 1

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info(f"  Files processed: {len(files_to_load)}")
    logger.info(f"  Successful:      {len(files_to_load) - failures}")
    logger.info(f"  Failed:          {failures}")
    logger.info(f"  Total rows:      {total_rows:,}")
    logger.info("=" * 60)

    conn.close()

    if failures > 0:
        logger.error(f"{failures} file(s) failed to load. Check logs above.")
        sys.exit(1)

    logger.info("Atlas CSV Loader — Complete")
    sys.exit(0)


if __name__ == "__main__":
    main()
