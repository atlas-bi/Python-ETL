"""
Atlas ETL Migration — Week 4: atlas_pbi_events.py

Replaces: pbi_Activity_Events.ps1 (legacy PowerShell script)
          SSIS Task 7: PowerShell PBI Activity Events (Execute Process Task)

Extracts Power BI Activity Events from the Power BI REST Admin API and
loads them into raw_v2.PbiActivityEvent in Atlas_Staging. Uses a 28-day
rolling lookback window (configurable via atlas_config.py).

The Power BI Admin API returns activity events one day at a time, with
pagination via continuationUri. This script iterates day-by-day across
the lookback window, authenticating via Azure AD service principal
(client_credentials grant).

This script runs BEFORE usp_Atlas_RunData. The orchestrator
(atlas_orchestrator.py) calls this script first, then invokes the
stored procedure which handles deduplication, staging, and merge.

Pipeline position:
    atlas_pbi_events.py  ->  usp_Atlas_RunData (Phase 4a dedup -> 4b join -> 5c stage)

Dependencies:
    - Python 3.11+ with pyodbc, pandas (urllib.request for HTTP — no requests lib)
    - Azure AD App Registration with Power BI Admin API permissions:
        * Tenant.Read.All  (Application permission)
        * Or equivalent admin consent for Activity Events
    - atlas_config.py for credentials and connection strings
    - Atlas_Staging database with raw_v2.PbiActivityEvent table
      (created by 01_week4_rundata_ddl.sql)

Usage:
    python atlas_pbi_events.py                      # Full 28-day extraction
    python atlas_pbi_events.py --days 7             # Last 7 days only
    python atlas_pbi_events.py --start 2026-01-01   # Custom start date
    python atlas_pbi_events.py --dry-run            # Auth test + count only
    python atlas_pbi_events.py --verbose            # Debug logging

Author:  Larry Duren
Date:    February 2026
Version: 4.3 (production cleanup — urllib, SP logging, diagnostic removal)

v4.3 Changes:
    - Replaced requests library with urllib.request (requests re-encodes %27)
    - Switched ETL logging from raw INSERT to usp_Atlas_LogStart/LogEnd SPs
    - Removed all troubleshooting/diagnostic logging artifacts
"""

import os
import sys
import argparse
import json
import logging
import time
import uuid
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta, timezone
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
    from atlas_config import config as _cfg
    ATLAS_STAGING_SERVER   = _cfg.atlas_staging_server
    ATLAS_STAGING_DATABASE = _cfg.atlas_staging_database
    QUERY_TIMEOUT          = _cfg.query_timeout_seconds
    PBI_TENANT_ID          = _cfg.pbi_tenant_id
    PBI_CLIENT_ID          = _cfg.pbi_client_id
    PBI_CLIENT_SECRET      = _cfg.pbi_client_secret
    PBI_LOOKBACK_DAYS      = _cfg.pbi_lookback_days
except ImportError:
    ATLAS_STAGING_SERVER   = os.getenv("ATLAS_STAGING_SERVER", r"10.247.4.56\SQL126")
    ATLAS_STAGING_DATABASE = os.getenv("ATLAS_STAGING_DB", "Atlas_Staging")
    QUERY_TIMEOUT          = int(os.getenv("ATLAS_QUERY_TIMEOUT", "3600"))
    PBI_TENANT_ID          = os.getenv("PBI_TENANT_ID", "your-tenant-id")
    PBI_CLIENT_ID          = os.getenv("PBI_CLIENT_ID", "your-client-id")
    PBI_CLIENT_SECRET      = os.getenv("PBI_CLIENT_SECRET", "")
    PBI_LOOKBACK_DAYS      = int(os.getenv("PBI_LOOKBACK_DAYS", "30"))

# API endpoints
AZURE_TOKEN_URL    = "https://login.microsoftonline.com/{tenant}/oauth2/token"
PBI_ACTIVITY_URL   = "https://api.powerbi.com/v1.0/myorg/admin/activityevents"

# Target table (v2 schema isolation)
TARGET_TABLE = "raw_v2.PbiActivityEvent"

# Columns in target table (must match 01_week4_rundata_ddl.sql)
TARGET_COLUMNS = [
    "Id", "CreationTime", "Activity", "UserId",
    "ReportId", "ReportName", "WorkspaceName", "WorkspaceId",
    "DatasetId", "DatasetName", "ReportType", "RequestId",
    "ClientIP", "UserAgent", "DistributionMethod", "ConsumptionMethod",
    # Added for downstream BizKey JOIN
    "AppName", "AppReportId", "CapacityId", "CapacityName", "ObjectId",
]


# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------
def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure structured logging (matches atlas_csv_loader.py pattern)."""
    log_level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger("atlas_pbi_events")
    logger.setLevel(log_level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


# ---------------------------------------------------------------------------
# Database Connection
# ---------------------------------------------------------------------------
def get_connection() -> pyodbc.Connection:
    """Create pyodbc connection to Atlas_Staging."""
    try:
        conn_str = _cfg.get_atlas_staging_connection_string()
    except NameError:
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={ATLAS_STAGING_SERVER};"
            f"DATABASE={ATLAS_STAGING_DATABASE};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=30;"
        )
    conn = pyodbc.connect(conn_str)
    conn.timeout = QUERY_TIMEOUT
    return conn


# ---------------------------------------------------------------------------
# ETL Logging (uses usp_Atlas_LogStart / LogEnd SPs)
# ---------------------------------------------------------------------------
PACKAGE_NAME = "atlas_pbi_events"


def log_start(conn: pyodbc.Connection, exec_id: str, step: str, seq: int) -> int:
    """Call etl.usp_Atlas_LogStart and return LogID."""
    try:
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
    except Exception as e:
        print(f"WARNING: ETL LogStart failed: {e}")
        return 0


def log_end(conn: pyodbc.Connection, log_id: int, rows: int,
            status: str = "Success", error_msg: str = None):
    """Call etl.usp_Atlas_LogEnd."""
    try:
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
    except Exception as e:
        print(f"WARNING: ETL LogEnd failed: {e}")


# ---------------------------------------------------------------------------
# Azure AD Authentication
# ---------------------------------------------------------------------------
def get_access_token(logger: logging.Logger) -> Optional[str]:
    """
    Acquire an Azure AD access token via client_credentials grant.

    Uses the v1.0 OAuth endpoint with resource parameter, matching the
    legacy PowerShell implementation. Uses urllib.request (not requests).

    Returns:
        Bearer token string, or None on failure.
    """
    token_url = AZURE_TOKEN_URL.format(tenant=PBI_TENANT_ID)

    payload = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": PBI_CLIENT_ID,
        "client_secret": PBI_CLIENT_SECRET,
        "resource": "https://analysis.windows.net/powerbi/api",
    }).encode("utf-8")

    logger.debug(f"Requesting token from: {token_url}")
    logger.debug(f"Client ID: {PBI_CLIENT_ID[:8]}...")

    try:
        req = urllib.request.Request(token_url, data=payload, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            token_data = json.loads(resp.read().decode("utf-8"))
            access_token = token_data.get("access_token")

            if not access_token:
                logger.error("Token response missing 'access_token' field")
                logger.debug(f"Response: {json.dumps(token_data, indent=2)}")
                return None

            logger.info("Azure AD authentication successful")
            return access_token

    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        logger.error(f"Token request failed (HTTP {e.code})")
        try:
            error_detail = json.loads(body)
            logger.error(f"  Error: {error_detail.get('error', 'unknown')}")
            logger.error(f"  Description: {error_detail.get('error_description', 'none')}")
        except (ValueError, json.JSONDecodeError):
            logger.error(f"  Response: {body[:500]}")
        return None

    except urllib.error.URLError as e:
        logger.error(f"Token request failed: {e.reason}")
        return None


# ---------------------------------------------------------------------------
# Power BI Activity Events API
# ---------------------------------------------------------------------------
def fetch_events_for_day(
    access_token: str,
    target_date: datetime,
    logger: logging.Logger,
) -> Optional[list[dict]]:
    """
    Fetch all Power BI activity events for a single UTC day.

    Uses urllib.request with %27-encoded single quotes around datetime
    values (OData format).

    Args:
        access_token: Bearer token from Azure AD.
        target_date: The UTC date to fetch events for.
        logger: Logger instance.

    Returns:
        List of event dictionaries for the day, or None on HTTP 400
        (used by caller to detect retention boundary).
    """
    # Build UTC day boundaries
    start_dt = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = start_dt + timedelta(days=1) - timedelta(seconds=1)

    # %27 = single quote, Z = UTC
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # URL with %27-encoded single quotes (OData datetime format)
    initial_url = (
        f"{PBI_ACTIVITY_URL}"
        f"?startDateTime=%27{start_str}%27&endDateTime=%27{end_str}%27"
    )

    all_events = []
    page_count = 0
    url = initial_url
    network_retries = 0
    max_network_retries = 3
    network_retry_delay = 10

    while url:
        try:
            req = urllib.request.Request(url, method="GET")
            req.add_header("Authorization", f"Bearer {access_token}")

            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                events = data.get("activityEventEntities", [])
                all_events.extend(events)
                page_count += 1
                network_retries = 0  # Reset on success

                # Follow continuationUri until null
                continuation = data.get("continuationUri")
                if continuation and continuation != url:
                    url = continuation
                else:
                    url = None

        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")
            if e.code == 429:
                retry_after = int(e.headers.get("Retry-After", 60))
                logger.warning(f"  Rate limited for {target_date.date()}, "
                               f"retrying in {retry_after}s")
                time.sleep(retry_after)
                continue
            elif e.code == 400:
                return None  # Caller uses None to detect retention boundary
            else:
                logger.error(f"  HTTP {e.code} for {target_date.date()}: {err_body[:500]}")
                break

        except (urllib.error.URLError, ConnectionResetError, TimeoutError) as e:
            network_retries += 1
            reason = getattr(e, "reason", e)
            if network_retries <= max_network_retries:
                logger.warning(f"  Network error for {target_date.date()} "
                               f"(attempt {network_retries}/{max_network_retries}): {reason}")
                time.sleep(network_retry_delay)
                continue
            else:
                logger.error(f"  Network error for {target_date.date()} "
                             f"after {max_network_retries} retries: {reason}")
                break

    return all_events


def extract_events(
    access_token: str,
    start_date: datetime,
    end_date: datetime,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Extract Power BI activity events across a date range.

    Iterates day-by-day (API constraint) and collects all events into
    a single DataFrame.

    Args:
        access_token: Bearer token.
        start_date: First date to extract (inclusive).
        end_date: Last date to extract (inclusive).
        logger: Logger instance.

    Returns:
        DataFrame with all extracted events.
    """
    all_events = []
    current_date = start_date
    days_processed = 0
    total_days = (end_date - start_date).days + 1
    found_first_success = False

    logger.info(f"Extracting events: {start_date.date()} -> {end_date.date()} ({total_days} days)")

    while current_date <= end_date:
        days_processed += 1
        day_events = fetch_events_for_day(access_token, current_date, logger)

        if day_events is None:
            # HTTP 400 — check if we've seen any successful day yet
            if not found_first_success:
                logger.warning(f"  {current_date.date()}: day outside API retention window — skipping")
                current_date += timedelta(days=1)
                continue
            else:
                logger.error(f"  HTTP 400 for {current_date.date()} after successful days — aborting")
                break

        found_first_success = True

        if day_events:
            all_events.extend(day_events)
            logger.debug(f"  {current_date.date()}: {len(day_events)} events "
                         f"(day {days_processed}/{total_days})")
        else:
            logger.debug(f"  {current_date.date()}: 0 events")

        # Progress logging every 30 days
        if days_processed % 30 == 0:
            logger.info(f"  Progress: {days_processed}/{total_days} days, "
                        f"{len(all_events):,} events so far")

        current_date += timedelta(days=1)

    logger.info(f"Extraction complete: {len(all_events):,} events across {days_processed} days")

    if not all_events:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    # Convert to DataFrame
    df = pd.DataFrame(all_events)

    return df


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------
def normalize_events(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Normalize raw API response DataFrame to match target table schema.

    The PBI Activity Events API returns varying fields depending on
    the activity type. This function maps API fields -> target columns,
    filling missing columns with None.

    Args:
        df: Raw DataFrame from API extraction.
        logger: Logger instance.

    Returns:
        Normalized DataFrame with exactly TARGET_COLUMNS.
    """
    if df.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    # API field -> target column mapping (API uses camelCase)
    field_map = {
        "Id": "Id",
        "CreationTime": "CreationTime",
        "Activity": "Activity",
        "UserId": "UserId",
        "ReportId": "ReportId",
        "ReportName": "ReportName",
        "WorkSpaceName": "WorkspaceName",   # Note: API uses capital S
        "WorkspaceName": "WorkspaceName",   # Also accept this variant
        "WorkspaceId": "WorkspaceId",
        "DatasetId": "DatasetId",
        "DatasetName": "DatasetName",
        "ReportType": "ReportType",
        "RequestId": "RequestId",
        "ClientIP": "ClientIP",
        "UserAgent": "UserAgent",
        "DistributionMethod": "DistributionMethod",
        "ConsumptionMethod": "ConsumptionMethod",
        # Metadata fields the downstream BizKey JOIN needs.
        # Alphabetized within block.
        "AppName": "AppName",
        "AppReportId": "AppReportId",
        "CapacityId": "CapacityId",
        "CapacityName": "CapacityName",
        "ObjectId": "ObjectId",
    }

    # Rename columns that exist in the DataFrame
    rename_map = {}
    for api_field, target_col in field_map.items():
        if api_field in df.columns and api_field != target_col:
            rename_map[api_field] = target_col

    if rename_map:
        df = df.rename(columns=rename_map)

    # Ensure all target columns exist, fill missing with None
    for col in TARGET_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Select only target columns in the correct order
    df = df[TARGET_COLUMNS].copy()

    # Convert CreationTime to datetime
    if "CreationTime" in df.columns:
        df["CreationTime"] = pd.to_datetime(df["CreationTime"], errors="coerce")

    # Truncate long strings to match SQL column widths
    string_limits = {
        "Id": 200,
        "Activity": 200,
        "UserId": 200,
        "ReportId": 100,
        "ReportName": 500,
        "WorkspaceName": 500,
        "WorkspaceId": 100,
        "DatasetId": 100,
        "DatasetName": 500,
        "ReportType": 100,
        "RequestId": 200,
        "ClientIP": 50,
        "UserAgent": 500,
        "DistributionMethod": 100,
        "ConsumptionMethod": 100,
        # Metadata fields for downstream BizKey JOIN
        "AppName": 500,
        "AppReportId": 100,
        "CapacityId": 100,
        "CapacityName": 500,
        "ObjectId": 100,
    }
    for col, max_len in string_limits.items():
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: str(x)[:max_len] if pd.notna(x) and x is not None else None
            )

    logger.info(f"Normalized {len(df):,} events to {len(TARGET_COLUMNS)} target columns")

    # Log activity type distribution
    if not df.empty and "Activity" in df.columns:
        top_activities = df["Activity"].value_counts().head(5)
        for activity, count in top_activities.items():
            logger.debug(f"  {activity}: {count:,}")

    return df


def load_events_to_db(
    conn: pyodbc.Connection,
    df: pd.DataFrame,
    logger: logging.Logger,
    batch_size: int = 5000,
) -> int:
    """
    Bulk insert events into raw_v2.PbiActivityEvent.

    Truncates the target table first (raw tables are refreshed each run).
    Uses fast_executemany for performance.

    Args:
        conn: Database connection.
        df: Normalized DataFrame.
        logger: Logger instance.
        batch_size: Rows per batch for bulk insert.

    Returns:
        Total rows inserted.
    """
    if df.empty:
        logger.info("No events to load — skipping insert")
        return 0

    cursor = conn.cursor()

    # Truncate target table (raw tables are dropped/recreated each run)
    logger.info(f"Truncating {TARGET_TABLE}")
    cursor.execute(f"TRUNCATE TABLE {TARGET_TABLE}")
    conn.commit()

    # Build parameterized INSERT
    col_list = ", ".join(TARGET_COLUMNS)
    placeholders = ", ".join(["?"] * len(TARGET_COLUMNS))
    insert_sql = f"INSERT INTO {TARGET_TABLE} ({col_list}) VALUES ({placeholders})"

    # Clean NaN -> None for pyodbc
    df = df.where(df.notna(), None)

    # Bulk insert with fast_executemany
    cursor.fast_executemany = True
    total_rows = 0

    for start in range(0, len(df), batch_size):
        batch = df.iloc[start : start + batch_size]
        # Convert all values to str (or None) — target columns are ALL varchar/datetime,
        # but JSON-parsed Python int/float values cause pyodbc to infer numeric types
        # that overflow (22003 Numeric value out of range).
        rows = [
            tuple(str(v) if v is not None else None for v in row)
            for row in batch.itertuples(index=False, name=None)
        ]
        cursor.executemany(insert_sql, rows)
        conn.commit()
        total_rows += len(rows)
        logger.debug(f"  Inserted batch: rows {start + 1} to {start + len(rows)}")

    logger.info(f"Loaded {total_rows:,} events into {TARGET_TABLE}")
    return total_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Atlas ETL — Power BI Activity Events Extractor (Week 4)"
    )
    parser.add_argument(
        "--days", "-d",
        type=int,
        default=None,
        help=f"Override lookback period in days (default: {PBI_LOOKBACK_DAYS} days)",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Override start date (YYYY-MM-DD format)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="Override end date (YYYY-MM-DD format, default: yesterday)",
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Authenticate and count events only; do not insert data",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug-level logging",
    )
    args = parser.parse_args()

    logger = setup_logging(verbose=args.verbose)
    logger.info("=" * 60)
    logger.info("ATLAS ETL — PBI Events Extractor (Pipeline B)")
    exec_id = os.getenv('ATLAS_EXECUTION_ID', str(uuid.uuid4()))
    logger.info(f"Execution ID: {exec_id}")
    logger.info(f"Target:  {ATLAS_STAGING_SERVER}/{ATLAS_STAGING_DATABASE}")
    logger.info(f"Table:   {TARGET_TABLE}")
    logger.info(f"Mode:    {'DRY RUN' if args.dry_run else 'LIVE'}")
    logger.info("=" * 60)

    overall_start = datetime.now()

    # ------------------------------------------------------------------
    # 1. Validate credentials
    # ------------------------------------------------------------------
    if PBI_CLIENT_SECRET == "" or PBI_TENANT_ID == "your-tenant-id":
        logger.error("Power BI API credentials not configured.")
        logger.error("Set environment variables: PBI_TENANT_ID, PBI_CLIENT_ID, PBI_CLIENT_SECRET")
        logger.error("Or update atlas_config.py with your Azure AD app registration values.")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 2. Determine date range
    # ------------------------------------------------------------------
    # End date: yesterday (API data has ~24h latency)
    if args.end:
        end_date = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end_date = (datetime.now(timezone.utc) - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

    if args.start:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    elif args.days:
        start_date = end_date - timedelta(days=args.days - 1)
    else:
        # Default: 30-day lookback (PBI API retention limit, from atlas_config.py)
        start_date = end_date - timedelta(days=PBI_LOOKBACK_DAYS - 1)

    logger.info(f"Date range: {start_date.date()} -> {end_date.date()}")
    logger.info(f"  ({(end_date - start_date).days + 1} days)")

    # ------------------------------------------------------------------
    # 3. Authenticate with Azure AD
    # ------------------------------------------------------------------
    access_token = get_access_token(logger)
    if not access_token:
        logger.critical("Authentication failed — cannot proceed")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 4. Connect to database
    # ------------------------------------------------------------------
    try:
        conn = get_connection()
        logger.info("Database connection established")
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 5. Extract events from PBI API
    # ------------------------------------------------------------------
    step_seq = 1
    log_id = log_start(conn, exec_id, "Extract PBI Events", step_seq)

    try:
        raw_df = extract_events(access_token, start_date, end_date, logger)
    except Exception as e:
        msg = f"Extraction failed: {e}"
        logger.error(f"  FAILED: Extract PBI Events — {msg}")
        log_end(conn, log_id, 0, "Failure", msg)
        conn.close()
        sys.exit(1)

    # ------------------------------------------------------------------
    # 6. Normalize to target schema
    # ------------------------------------------------------------------
    normalized_df = normalize_events(raw_df, logger)
    event_count = len(normalized_df)

    log_end(conn, log_id, event_count)
    logger.info(f"  Extracted and normalized {event_count:,} events")

    if args.dry_run:
        logger.info(f"  [DRY RUN] Would insert {event_count:,} rows into {TARGET_TABLE}")
        conn.close()
        logger.info("PBI Events extraction complete (dry run)")
        sys.exit(0)

    # ------------------------------------------------------------------
    # 7. Load into raw_v2.PbiActivityEvent
    # ------------------------------------------------------------------
    step_seq += 1
    log_id = log_start(conn, exec_id, "Load PBI Events", step_seq)

    try:
        rows_loaded = load_events_to_db(conn, normalized_df, logger)
        log_end(conn, log_id, rows_loaded)
    except Exception as e:
        msg = f"Load failed: {e}"
        logger.error(f"  FAILED: Load PBI Events — {msg}")
        log_end(conn, log_id, 0, "Failure", msg)
        conn.close()
        sys.exit(1)

    # ------------------------------------------------------------------
    # 8. Summary
    # ------------------------------------------------------------------
    elapsed_total = (datetime.now() - overall_start).total_seconds()

    logger.info(f"PBI Events extraction complete: {rows_loaded:,} total rows ({elapsed_total:.1f}s)")

    conn.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
