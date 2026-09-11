"""
Atlas ETL Migration — Phase 2: atlas_pbi_metadata.py

Power BI metadata extractor — Python translation of pbi_Main_Meta.ps1.

Replaces: pbi_Main_Meta.ps1 (legacy PowerShell script in
          AtlasProject/Reference/pbi_legacy/)

Calls 5 PBI Admin API endpoints in this order:
    1. /v1.0/myorg/admin/groups (workspaces, with $expand=reports,users)
    2. /v1.0/myorg/admin/reports
    3. /v1.0/myorg/admin/datasets
    4. /v1.0/myorg/admin/apps
    5. /v1.0/myorg/admin/capacities

Populates 7 raw_v2 tables (TRUNCATE + bulk insert each run, preserving indexes):
    - PbiWorkspace        — parent workspace fields
    - PbiWorkspaceReport  — workspace→report bridge from $expand=reports
    - PbiWorkspaceUser    — workspace→user bridge from $expand=users
    - PbiReport           — flat report list (KEYSTONE for Phase 3 BizKey JOIN)
    - PbiDataset          — dataset metadata
    - PbiApp              — app list
    - PbiCapacity         — capacity list

Faithful to legacy pbi_Main_Meta.ps1:
    - WorkspaceConnection on PbiReport: NULL on every row.
      Verified 2026-04-07: pbi_Main_Meta.ps1 does not populate this field
      anywhere. Phase 3 BizKey will use an empty segment 2 unless a
      derivation rule is provided by the client.
    - DefaultVisibilityYN on PbiReport: NULL on every row.
      Same reason as WorkspaceConnection — legacy script does not populate.
    - PbiWorkspaceUser.{UserPrincipalName, SamAccountName, Domain}: NULL.
      Populated by Phase 5 (atlas_pbi_user_identity.py via Microsoft Graph).
    - PbiReport.{CreatedByUPN, CreatedBySamAccountName, CreatedByDomain,
                 ModifiedByUPN, ModifiedBySamAccountName, ModifiedByDomain}: NULL.
      Same reason — Phase 5 populates these.

This script runs BEFORE atlas_pbi_events.py in the orchestrator. Phase 4b
of usp_Atlas_RunData JOINs raw_v2.PbiActivityEvent to raw_v2.PbiReport
(after Phase 3 deployment), so the metadata MUST be loaded first.

Pipeline position:
    atlas_pbi_metadata.py  ->  raw_v2.PbiWorkspace, PbiWorkspaceReport,
                                PbiWorkspaceUser, PbiReport, PbiDataset,
                                PbiApp, PbiCapacity
    atlas_pbi_events.py    ->  raw_v2.PbiActivityEvent
    usp_Atlas_RunData      ->  Phase 4a/4b: dedup events, join to PbiReport

Dependencies:
    - Python 3.11+ with pyodbc (urllib.request for HTTP — no requests lib)
    - Azure AD App Registration with Power BI Admin API permissions:
        * Tenant.Read.All  (Application permission)
    - atlas_config.py for credentials and connection strings
    - Atlas_Staging database with all 7 raw_v2.Pbi* tables created
      (see the raw_v2.Pbi* DDL in Atlas_Staging)

Usage:
    python atlas_pbi_metadata.py                # Full extraction (all 5 endpoints)
    python atlas_pbi_metadata.py --dry-run      # Auth + counts only, no inserts
    python atlas_pbi_metadata.py --verbose      # Debug logging
    python atlas_pbi_metadata.py --skip-truncate  # Skip TRUNCATE (test mode)

Author:  Larry Duren / Atlas Migration Project
Date:    April 2026
"""

VERSION = "1.0 — 2026-04-07"

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
    from atlas_config import config as _cfg
    ATLAS_STAGING_SERVER   = _cfg.atlas_staging_server
    ATLAS_STAGING_DATABASE = _cfg.atlas_staging_database
    QUERY_TIMEOUT          = _cfg.query_timeout_seconds
    PBI_TENANT_ID          = _cfg.pbi_tenant_id
    PBI_CLIENT_ID          = _cfg.pbi_client_id
    PBI_CLIENT_SECRET      = _cfg.pbi_client_secret
except ImportError:
    ATLAS_STAGING_SERVER   = os.getenv("ATLAS_STAGING_SERVER", r"10.247.4.56\SQL126")
    ATLAS_STAGING_DATABASE = os.getenv("ATLAS_STAGING_DB", "Atlas_Staging")
    QUERY_TIMEOUT          = int(os.getenv("ATLAS_QUERY_TIMEOUT", "3600"))
    PBI_TENANT_ID          = os.getenv("PBI_TENANT_ID", "your-tenant-id")
    PBI_CLIENT_ID          = os.getenv("PBI_CLIENT_ID", "your-client-id")
    PBI_CLIENT_SECRET      = os.getenv("PBI_CLIENT_SECRET", "")

# API endpoints
AZURE_TOKEN_URL = "https://login.microsoftonline.com/{tenant}/oauth2/token"
PBI_API_BASE    = "https://api.powerbi.com/v1.0/myorg"

# Pagination tuning (matches legacy pbi_Main_Meta.ps1: $top=500 across all
# paginated endpoints; capacities is a single non-paginated call)
PAGE_SIZE = 500


# ---------------------------------------------------------------------------
# Target table column lists
# Each list defines the INSERT column order. Row tuples produced by the
# extract_* functions must match this exact order. Column names match
# the live raw_v2.Pbi* DDL in Atlas_Staging.
# ---------------------------------------------------------------------------

WORKSPACE_TABLE = "raw_v2.PbiWorkspace"
WORKSPACE_COLUMNS = [
    "WorkspaceId", "WorkspaceName", "WorkspaceType", "State",
    "IsOnDedicatedCapacity", "IsReadOnly", "CapacityId", "Description",
]

WORKSPACE_REPORT_TABLE = "raw_v2.PbiWorkspaceReport"
WORKSPACE_REPORT_COLUMNS = [
    "WorkspaceId", "ReportId",
]

WORKSPACE_USER_TABLE = "raw_v2.PbiWorkspaceUser"
WORKSPACE_USER_COLUMNS = [
    "WorkspaceId", "Identifier", "EmailAddress", "DisplayName",
    "GroupUserAccessRight", "PrincipalType", "GraphId",
    "UserPrincipalName", "SamAccountName", "Domain",
]

REPORT_TABLE = "raw_v2.PbiReport"
REPORT_COLUMNS = [
    "ReportId", "ReportName", "Description", "EmbedUrl", "WebUrl",
    "ReportType", "SensitivityLabel", "AppId", "DatasetId",
    "WorkspaceId", "WorkspaceName", "WorkspaceConnection",
    "CapacityId", "CapacityName", "Source",
    "CreatedBy", "CreatedDateTime", "ModifiedBy", "ModifiedDateTime",
    "CreatedByUPN", "CreatedBySamAccountName", "CreatedByDomain",
    "ModifiedByUPN", "ModifiedBySamAccountName", "ModifiedByDomain",
    "DefaultVisibilityYN",
]

# PbiDataset column names match $datasetList property names from
# pbi_Main_Meta.ps1 with two PascalCase renames applied in the
# raw_v2.PbiDataset DDL (verified via sys.columns):
#   id    -> DatasetId   (PK)
#   name  -> DatasetName (matches WorkspaceName/ReportName/AppName convention)
# All other column names below are case-insensitive matches to the live
# table per SQL Server CI_AS collation — case differences are harmless;
# only the literal "name" -> "DatasetName" rename is required to avoid
# an "Invalid column name 'name'" error at INSERT time.
# Column types per live DDL: DatasetId/DatasetName/ContentProviderType/
# ConfiguredBy/SensitivityLabel are sized varchar(100..500),
# CreateReportEmbedURL/QnaEmbedURL/WebUrl are varchar(2000),
# CreatedDate is datetime, all bit-like flag columns are varchar(5),
# Description and SchemaRetrievalError are nvarchar(max).
# Values are passed through SQL Server implicit conversion via the
# str-cast pattern in bulk_insert() — same defensive approach as
# atlas_pbi_events.py.
DATASET_TABLE = "raw_v2.PbiDataset"
DATASET_COLUMNS = [
    "DatasetId",                       # renamed from "id"
    "DatasetName",                     # renamed from "name" (Phase 2 SSMS rename)
    "description",
    "ContentProviderType",
    "CreateReportEmbedURL",
    "CreatedDate",
    "IsEffectiveIdentityRequired",
    "IsEffectiveIdentityRolesRequired",
    "IsOnPremGatewayRequired",
    "IsRefreshable",
    "QnaEmbedURL",
    "addRowsAPIEnabled",
    "configuredBy",
    "schemaMayNotBeUpToDate",
    "schemaRetrievalError",
    "sensitivityLabel",
    "webUrl",
]

APP_TABLE = "raw_v2.PbiApp"
APP_COLUMNS = [
    "AppId", "AppName", "Description", "LastUpdate", "PublishedBy",
]

CAPACITY_TABLE = "raw_v2.PbiCapacity"
CAPACITY_COLUMNS = [
    "CapacityId", "DisplayName", "Sku", "State",
    "CapacityUserAccessRight", "Region",
]

# Truncation order — all 7 tables wiped before any extraction begins.
# No FK constraints exist between these tables, so order is arbitrary.
ALL_TARGET_TABLES = [
    WORKSPACE_TABLE,
    WORKSPACE_REPORT_TABLE,
    WORKSPACE_USER_TABLE,
    REPORT_TABLE,
    DATASET_TABLE,
    APP_TABLE,
    CAPACITY_TABLE,
]


# ---------------------------------------------------------------------------
# Per-table string truncation limits (varchar widths from live sys.columns,
# verified 2026-04-07).
#
# WHY THIS EXISTS:
#   pyodbc's fast_executemany infers the parameter buffer width from the
#   FIRST row in each batch. If a later row contains a longer string for
#   the same column, ODBC throws "String data, right truncation: length N
#   buffer M". This is NOT a DDL width problem — it's a driver-side buffer
#   sizing issue. We sidestep it by pre-truncating every varchar column to
#   its DDL width before handing rows to executemany. Same defensive
#   pattern as atlas_pbi_events.py:normalize_events lines 493-520.
#
# RULES:
#   - Only varchar columns appear here. nvarchar(max) columns
#     (PbiWorkspace.Description, PbiReport.Description, PbiApp.Description,
#     PbiDataset.description, PbiDataset.schemaRetrievalError) are
#     unbounded — no truncation needed.
#   - datetime columns are not truncated (CreatedDateTime, ModifiedDateTime,
#     CreatedDate, LastUpdate, ETL_LoadDate).
#   - ETL_LoadDate is populated by SQL Server DEFAULT — never written by
#     the extractor — so it never appears in any *_COLUMNS list.
#   - Keys must match the corresponding *_COLUMNS list ENTRY VALUES exactly
#     so columns.index(col) resolves correctly.
# ---------------------------------------------------------------------------

WORKSPACE_LIMITS = {
    "WorkspaceId":           100,
    "WorkspaceName":         500,
    "WorkspaceType":         100,
    "State":                  50,
    "IsOnDedicatedCapacity":   5,
    "IsReadOnly":              5,
    "CapacityId":            100,
    # Description is nvarchar(max) — unbounded
}

WORKSPACE_REPORT_LIMITS = {
    "WorkspaceId": 100,
    "ReportId":    100,
}

WORKSPACE_USER_LIMITS = {
    "WorkspaceId":          100,
    "Identifier":           255,
    "EmailAddress":         255,
    "DisplayName":          500,
    "GroupUserAccessRight": 100,
    "PrincipalType":        100,
    "GraphId":              100,
    "UserPrincipalName":    255,
    "SamAccountName":       100,
    "Domain":               100,
}

REPORT_LIMITS = {
    "ReportId":                  100,
    "ReportName":                500,
    # Description is nvarchar(max)
    "EmbedUrl":                 2000,
    "WebUrl":                   2000,
    "ReportType":                100,
    "SensitivityLabel":          255,
    "AppId":                     100,
    "DatasetId":                 100,
    "WorkspaceId":               100,
    "WorkspaceName":             500,
    "WorkspaceConnection":       500,
    "CapacityId":                100,
    "CapacityName":              500,
    "Source":                    200,
    "CreatedBy":                 255,
    # CreatedDateTime is datetime
    "ModifiedBy":                255,
    # ModifiedDateTime is datetime
    "CreatedByUPN":              255,
    "CreatedBySamAccountName":   100,
    "CreatedByDomain":           100,
    "ModifiedByUPN":             255,
    "ModifiedBySamAccountName":  100,
    "ModifiedByDomain":          100,
    "DefaultVisibilityYN":         1,
}

# Keys are the post-rename DATASET_COLUMNS values (mostly lowercase per
# legacy $datasetList property names — case-insensitive on the SQL side
# under CI_AS collation, so the live table's PascalCase columns resolve
# fine).
DATASET_LIMITS = {
    "DatasetId":                          100,
    "DatasetName":                        500,
    # description is nvarchar(max)
    "ContentProviderType":                100,
    "CreateReportEmbedURL":              2000,
    # CreatedDate is datetime
    "IsEffectiveIdentityRequired":          5,
    "IsEffectiveIdentityRolesRequired":     5,
    "IsOnPremGatewayRequired":              5,
    "IsRefreshable":                        5,
    "QnaEmbedURL":                       2000,
    "addRowsAPIEnabled":                    5,
    "configuredBy":                       255,
    "schemaMayNotBeUpToDate":               5,
    # schemaRetrievalError is nvarchar(max)
    "sensitivityLabel":                   255,
    "webUrl":                            2000,
}

APP_LIMITS = {
    "AppId":       100,
    "AppName":     500,
    # Description is nvarchar(max)
    # LastUpdate is datetime
    "PublishedBy": 255,
}

CAPACITY_LIMITS = {
    "CapacityId":              100,
    "DisplayName":             500,
    "Sku":                      50,
    "State":                    50,
    "CapacityUserAccessRight": 100,
    "Region":                  100,
}


# ---------------------------------------------------------------------------
# Per-table setinputsizes() tuples (executemany parameter buffer shapes).
#
# WHY THIS EXISTS (complements the *_LIMITS pre-truncation above):
#   Pre-truncation caps each value BEFORE it reaches executemany, but
#   pyodbc's fast_executemany still infers the parameter buffer width
#   from the first row it sees. If the first row has a 10-char CreatedBy,
#   pyodbc allocates a 10-char buffer — and a subsequent row with a
#   48-char CreatedBy hits "String data, right truncation: length 48
#   buffer 10". Pre-truncation alone doesn't help: even after capping
#   to the DDL width of 255, the first row's ACTUAL length still drives
#   buffer inference.
#
#   setinputsizes() explicitly declares the buffer shape for every
#   parameter, bypassing inference. It is the correct, authoritative
#   fix for the fast_executemany truncation class of errors.
#
# PRE-READ CONFIRMED (2026-04-07): datetime values in all 4 affected
# tables (PbiReport.CreatedDateTime, PbiReport.ModifiedDateTime,
# PbiDataset.CreatedDate, PbiApp.LastUpdate) arrive at executemany as
# JSON-parsed strings like "2024-12-25T14:30:45.123Z" — NOT Python
# datetime objects. Python's json.loads does not auto-convert ISO-8601
# strings to datetime. Therefore datetime positions use
# (SQL_VARCHAR, 30, 0), NOT None. SQL Server implicitly converts the
# varchar string to datetime at the column level on INSERT.
#
# TYPE MAPPING:
#   varchar(N)     → (pyodbc.SQL_VARCHAR, N, 0)
#   nvarchar(max)  → (pyodbc.SQL_WVARCHAR, 0, 0)
#   datetime (str) → (pyodbc.SQL_VARCHAR, 30, 0)  [JSON string passthrough]
#
# RULES:
#   - Tuple order MUST match the corresponding *_COLUMNS list order
#     (INSERT column order). Each entry aligns 1:1 with a column.
#   - ETL_LoadDate is NEVER in any *_COLUMNS list (DEFAULT GETDATE()
#     fires server-side) so it is NEVER in any *_INPUT_SIZES list.
#   - Length MUST equal len(*_COLUMNS) — validated at import time below.
# ---------------------------------------------------------------------------

WORKSPACE_INPUT_SIZES = [
    (pyodbc.SQL_VARCHAR,   100, 0),  # WorkspaceId
    (pyodbc.SQL_VARCHAR,   500, 0),  # WorkspaceName
    (pyodbc.SQL_VARCHAR,   100, 0),  # WorkspaceType
    (pyodbc.SQL_VARCHAR,    50, 0),  # State
    (pyodbc.SQL_VARCHAR,     5, 0),  # IsOnDedicatedCapacity
    (pyodbc.SQL_VARCHAR,     5, 0),  # IsReadOnly
    (pyodbc.SQL_VARCHAR,   100, 0),  # CapacityId
    (pyodbc.SQL_WVARCHAR,    0, 0),  # Description (nvarchar max)
]

WORKSPACE_REPORT_INPUT_SIZES = [
    (pyodbc.SQL_VARCHAR, 100, 0),  # WorkspaceId
    (pyodbc.SQL_VARCHAR, 100, 0),  # ReportId
]

WORKSPACE_USER_INPUT_SIZES = [
    (pyodbc.SQL_VARCHAR, 100, 0),  # WorkspaceId
    (pyodbc.SQL_VARCHAR, 255, 0),  # Identifier
    (pyodbc.SQL_VARCHAR, 255, 0),  # EmailAddress
    (pyodbc.SQL_VARCHAR, 500, 0),  # DisplayName
    (pyodbc.SQL_VARCHAR, 100, 0),  # GroupUserAccessRight
    (pyodbc.SQL_VARCHAR, 100, 0),  # PrincipalType
    (pyodbc.SQL_VARCHAR, 100, 0),  # GraphId
    (pyodbc.SQL_VARCHAR, 255, 0),  # UserPrincipalName (Phase 5)
    (pyodbc.SQL_VARCHAR, 100, 0),  # SamAccountName    (Phase 5)
    (pyodbc.SQL_VARCHAR, 100, 0),  # Domain            (Phase 5)
]

REPORT_INPUT_SIZES = [
    (pyodbc.SQL_VARCHAR,   100, 0),  # ReportId
    (pyodbc.SQL_VARCHAR,   500, 0),  # ReportName
    (pyodbc.SQL_WVARCHAR,    0, 0),  # Description (nvarchar max)
    (pyodbc.SQL_VARCHAR,  2000, 0),  # EmbedUrl
    (pyodbc.SQL_VARCHAR,  2000, 0),  # WebUrl
    (pyodbc.SQL_VARCHAR,   100, 0),  # ReportType
    (pyodbc.SQL_VARCHAR,   255, 0),  # SensitivityLabel
    (pyodbc.SQL_VARCHAR,   100, 0),  # AppId
    (pyodbc.SQL_VARCHAR,   100, 0),  # DatasetId
    (pyodbc.SQL_VARCHAR,   100, 0),  # WorkspaceId
    (pyodbc.SQL_VARCHAR,   500, 0),  # WorkspaceName
    (pyodbc.SQL_VARCHAR,   500, 0),  # WorkspaceConnection
    (pyodbc.SQL_VARCHAR,   100, 0),  # CapacityId
    (pyodbc.SQL_VARCHAR,   500, 0),  # CapacityName
    (pyodbc.SQL_VARCHAR,   200, 0),  # Source
    (pyodbc.SQL_VARCHAR,   255, 0),  # CreatedBy
    (pyodbc.SQL_VARCHAR,    30, 0),  # CreatedDateTime (datetime-as-str)
    (pyodbc.SQL_VARCHAR,   255, 0),  # ModifiedBy
    (pyodbc.SQL_VARCHAR,    30, 0),  # ModifiedDateTime (datetime-as-str)
    (pyodbc.SQL_VARCHAR,   255, 0),  # CreatedByUPN              (Phase 5)
    (pyodbc.SQL_VARCHAR,   100, 0),  # CreatedBySamAccountName   (Phase 5)
    (pyodbc.SQL_VARCHAR,   100, 0),  # CreatedByDomain           (Phase 5)
    (pyodbc.SQL_VARCHAR,   255, 0),  # ModifiedByUPN             (Phase 5)
    (pyodbc.SQL_VARCHAR,   100, 0),  # ModifiedBySamAccountName  (Phase 5)
    (pyodbc.SQL_VARCHAR,   100, 0),  # ModifiedByDomain          (Phase 5)
    (pyodbc.SQL_VARCHAR,     1, 0),  # DefaultVisibilityYN
]

DATASET_INPUT_SIZES = [
    (pyodbc.SQL_VARCHAR,   100, 0),  # DatasetId
    (pyodbc.SQL_VARCHAR,   500, 0),  # DatasetName (renamed from "name")
    (pyodbc.SQL_WVARCHAR,    0, 0),  # description (nvarchar max)
    (pyodbc.SQL_VARCHAR,   100, 0),  # ContentProviderType
    (pyodbc.SQL_VARCHAR,  2000, 0),  # CreateReportEmbedURL
    (pyodbc.SQL_VARCHAR,    30, 0),  # CreatedDate (datetime-as-str)
    (pyodbc.SQL_VARCHAR,     5, 0),  # IsEffectiveIdentityRequired
    (pyodbc.SQL_VARCHAR,     5, 0),  # IsEffectiveIdentityRolesRequired
    (pyodbc.SQL_VARCHAR,     5, 0),  # IsOnPremGatewayRequired
    (pyodbc.SQL_VARCHAR,     5, 0),  # IsRefreshable
    (pyodbc.SQL_VARCHAR,  2000, 0),  # QnaEmbedURL
    (pyodbc.SQL_VARCHAR,     5, 0),  # addRowsAPIEnabled
    (pyodbc.SQL_VARCHAR,   255, 0),  # configuredBy
    (pyodbc.SQL_VARCHAR,     5, 0),  # schemaMayNotBeUpToDate
    (pyodbc.SQL_WVARCHAR,    0, 0),  # schemaRetrievalError (nvarchar max)
    (pyodbc.SQL_VARCHAR,   255, 0),  # sensitivityLabel
    (pyodbc.SQL_VARCHAR,  2000, 0),  # webUrl
]

APP_INPUT_SIZES = [
    (pyodbc.SQL_VARCHAR,   100, 0),  # AppId
    (pyodbc.SQL_VARCHAR,   500, 0),  # AppName
    (pyodbc.SQL_WVARCHAR,    0, 0),  # Description (nvarchar max)
    (pyodbc.SQL_VARCHAR,    30, 0),  # LastUpdate (datetime-as-str)
    (pyodbc.SQL_VARCHAR,   255, 0),  # PublishedBy
]

CAPACITY_INPUT_SIZES = [
    (pyodbc.SQL_VARCHAR, 100, 0),  # CapacityId
    (pyodbc.SQL_VARCHAR, 500, 0),  # DisplayName
    (pyodbc.SQL_VARCHAR,  50, 0),  # Sku
    (pyodbc.SQL_VARCHAR,  50, 0),  # State
    (pyodbc.SQL_VARCHAR, 100, 0),  # CapacityUserAccessRight
    (pyodbc.SQL_VARCHAR, 100, 0),  # Region
]

# Import-time self-check: every *_INPUT_SIZES list length must equal the
# corresponding *_COLUMNS list length. A mismatch here is an editor
# error — catch it before we run, not after pyodbc throws a cryptic
# parameter-count error mid-insert.
assert len(WORKSPACE_INPUT_SIZES)        == len(WORKSPACE_COLUMNS),        \
    "WORKSPACE_INPUT_SIZES length != WORKSPACE_COLUMNS length"
assert len(WORKSPACE_REPORT_INPUT_SIZES) == len(WORKSPACE_REPORT_COLUMNS), \
    "WORKSPACE_REPORT_INPUT_SIZES length != WORKSPACE_REPORT_COLUMNS length"
assert len(WORKSPACE_USER_INPUT_SIZES)   == len(WORKSPACE_USER_COLUMNS),   \
    "WORKSPACE_USER_INPUT_SIZES length != WORKSPACE_USER_COLUMNS length"
assert len(REPORT_INPUT_SIZES)           == len(REPORT_COLUMNS),           \
    "REPORT_INPUT_SIZES length != REPORT_COLUMNS length"
assert len(DATASET_INPUT_SIZES)          == len(DATASET_COLUMNS),          \
    "DATASET_INPUT_SIZES length != DATASET_COLUMNS length"
assert len(APP_INPUT_SIZES)              == len(APP_COLUMNS),              \
    "APP_INPUT_SIZES length != APP_COLUMNS length"
assert len(CAPACITY_INPUT_SIZES)         == len(CAPACITY_COLUMNS),         \
    "CAPACITY_INPUT_SIZES length != CAPACITY_COLUMNS length"


# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------
def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure structured logging (matches atlas_pbi_events.py pattern)."""
    log_level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger("atlas_pbi_metadata")
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
PACKAGE_NAME = "atlas_pbi_metadata"


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
# (verbatim copy of atlas_pbi_events.py:get_access_token)
# ---------------------------------------------------------------------------
def get_access_token(logger: logging.Logger) -> Optional[str]:
    """
    Acquire an Azure AD access token via client_credentials grant.

    Uses the v1.0 OAuth endpoint with resource parameter, matching
    atlas_pbi_events.py and the legacy PowerShell implementation.
    Uses urllib.request (not requests).

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
# HTTP helpers — paginated and single-shot fetches
# ---------------------------------------------------------------------------
def _do_request(access_token: str, url: str, logger: logging.Logger) -> Optional[dict]:
    """
    Perform a single GET against a PBI Admin endpoint with the standard
    bearer token, 429 retry-after handling, and network-error retry.

    Returns the parsed JSON dict on success, or None on a definitive
    HTTP error (non-429, non-transient).

    Mirrors the retry pattern used in atlas_pbi_events.py:fetch_events_for_day.
    """
    network_retries = 0
    max_network_retries = 3
    network_retry_delay = 10

    while True:
        try:
            req = urllib.request.Request(url, method="GET")
            req.add_header("Authorization", f"Bearer {access_token}")

            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))

        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")
            if e.code == 429:
                retry_after = int(e.headers.get("Retry-After", 60))
                logger.warning(f"  Rate limited, retrying in {retry_after}s")
                time.sleep(retry_after)
                continue
            else:
                logger.error(f"  HTTP {e.code}: {err_body[:500]}")
                return None

        except (urllib.error.URLError, ConnectionResetError, TimeoutError) as e:
            network_retries += 1
            reason = getattr(e, "reason", e)
            if network_retries <= max_network_retries:
                logger.warning(
                    f"  Network error (attempt {network_retries}/{max_network_retries}): {reason}"
                )
                time.sleep(network_retry_delay)
                continue
            else:
                logger.error(
                    f"  Network error after {max_network_retries} retries: {reason}"
                )
                return None


def fetch_paged(
    access_token: str,
    url_template: str,
    logger: logging.Logger,
    page_size: int = PAGE_SIZE,
) -> list:
    """
    Fetch all pages from a paginated PBI Admin endpoint via $top/$skip.

    The url_template must contain a literal '{skip}' placeholder where
    the offset will be substituted. Stop condition matches the legacy
    pbi_Main_Meta.ps1 behavior: stop when a page returns 0 items, or
    (as a defensive optimization) stop when a page returns fewer than
    page_size items.

    Args:
        access_token: Bearer token from get_access_token().
        url_template: URL with '{skip}' placeholder for the OData $skip value.
        logger: Logger instance.
        page_size: PBI Admin API page size (default 500, matches legacy).

    Returns:
        Combined list of all 'value' items across all pages.
    """
    all_items = []
    iteration = 0

    while True:
        skip = page_size * iteration
        url = url_template.format(skip=skip)

        data = _do_request(access_token, url, logger)
        if data is None:
            # _do_request already logged the error
            logger.error(f"  Pagination aborted at skip={skip}")
            break

        items = data.get("value", []) or []
        if not items:
            logger.debug(f"  page {iteration + 1}: 0 items — end of results")
            break

        all_items.extend(items)
        logger.debug(f"  page {iteration + 1}: {len(items)} items (skip={skip})")

        # Defensive early-stop: if a page returns fewer than page_size,
        # the next page would be empty anyway. Saves one round-trip.
        if len(items) < page_size:
            break

        iteration += 1

    return all_items


def fetch_single(access_token: str, url: str, logger: logging.Logger) -> list:
    """
    Fetch a single non-paginated PBI Admin endpoint (e.g., /capacities).

    Returns the 'value' list from the response, or [] on error.
    """
    data = _do_request(access_token, url, logger)
    if data is None:
        return []
    return data.get("value", []) or []


# ---------------------------------------------------------------------------
# Endpoint extractors
# Each extract_* function makes the API call(s) and returns row tuples
# ready for bulk_insert(). Row tuple order matches the *_COLUMNS lists above.
# ---------------------------------------------------------------------------
def extract_workspaces(access_token: str, logger: logging.Logger) -> tuple:
    """
    Extract workspaces with $expand=reports,users in a single API call per page.

    Returns three lists for the three target tables:
        (workspace_rows, workspace_report_rows, workspace_user_rows)

    Filter (matches legacy pbi_Main_Meta.ps1 line 86):
        type eq 'Workspace' and state eq 'Active'

    Expand (matches legacy line 88):
        reports,users
    """
    logger.info("Extracting workspaces from /admin/groups (with $expand=reports,users)")

    # URL-encode the filter (spaces and quotes); $top/$expand/$skip can stay raw.
    filter_value = "type eq 'Workspace' and state eq 'Active'"
    filter_encoded = urllib.parse.quote(filter_value, safe="")

    url_template = (
        f"{PBI_API_BASE}/admin/groups"
        f"?$top={PAGE_SIZE}"
        f"&$expand=reports,users"
        f"&$filter={filter_encoded}"
        f"&$skip={{skip}}"
    )

    workspaces = fetch_paged(access_token, url_template, logger)

    workspace_rows = []
    workspace_report_rows = []
    workspace_user_rows = []

    for grp in workspaces:
        # Parent workspace row — 8 columns matching WORKSPACE_COLUMNS order
        workspace_rows.append((
            grp.get("id"),                    # WorkspaceId
            grp.get("name"),                  # WorkspaceName
            grp.get("type"),                  # WorkspaceType
            grp.get("state"),                 # State
            grp.get("isOnDedicatedCapacity"), # IsOnDedicatedCapacity
            grp.get("isReadOnly"),            # IsReadOnly
            grp.get("capacityId"),            # CapacityId
            grp.get("description"),           # Description
        ))

        # Nested $expand=reports — one bridge row per report in this workspace.
        # Matches legacy lines 120-125.
        for rpt in (grp.get("reports") or []):
            workspace_report_rows.append((
                grp.get("id"),  # WorkspaceId
                rpt.get("id"),  # ReportId
            ))

        # Nested $expand=users — one row per user in this workspace.
        # Matches legacy lines 127-140.
        for usr in (grp.get("users") or []):
            workspace_user_rows.append((
                grp.get("id"),                       # WorkspaceId
                usr.get("identifier"),               # Identifier
                usr.get("emailAddress"),             # EmailAddress
                usr.get("displayName"),              # DisplayName
                usr.get("groupUserAccessRight"),     # GroupUserAccessRight
                usr.get("principalType"),            # PrincipalType
                usr.get("graphId"),                  # GraphId
                None,                                # UserPrincipalName — Phase 5
                None,                                # SamAccountName — Phase 5
                None,                                # Domain — Phase 5
            ))

    logger.info(
        f"  Extracted {len(workspace_rows)} workspaces, "
        f"{len(workspace_report_rows)} workspace-reports, "
        f"{len(workspace_user_rows)} workspace-users"
    )

    return workspace_rows, workspace_report_rows, workspace_user_rows


def extract_reports(access_token: str, logger: logging.Logger) -> list:
    """
    Extract all PBI reports from /admin/reports (flat list, $top/$skip paged).

    Property mapping (legacy $reportList → REPORT_COLUMNS):
        id              → ReportId
        name            → ReportName
        description     → Description
        embedUrl        → EmbedUrl
        webUrl          → WebUrl
        reportType      → ReportType
        sensitivityLabel→ SensitivityLabel
        appId           → AppId
        datasetId       → DatasetId
        createdBy       → CreatedBy
        createdDateTime → CreatedDateTime
        modifiedBy      → ModifiedBy
        modifiedDateTime→ ModifiedDateTime

    Columns NOT populated (NULL on every row, matching legacy script):
        WorkspaceId, WorkspaceName, WorkspaceConnection, CapacityId,
        CapacityName, Source, DefaultVisibilityYN
        — these exist in the live raw.PbiReport schema but pbi_Main_Meta.ps1
        does not write them. WorkspaceConnection NULL means Phase 3 BizKey
        will have an empty segment 2.

    Phase 5 placeholders (NULL until atlas_pbi_user_identity.py runs):
        CreatedByUPN, CreatedBySamAccountName, CreatedByDomain,
        ModifiedByUPN, ModifiedBySamAccountName, ModifiedByDomain
    """
    logger.info("Extracting reports from /admin/reports")

    url_template = f"{PBI_API_BASE}/admin/reports?$top={PAGE_SIZE}&$skip={{skip}}"
    items = fetch_paged(access_token, url_template, logger)

    rows = []
    for rpt in items:
        rows.append((
            rpt.get("id"),                # ReportId
            rpt.get("name"),              # ReportName
            rpt.get("description"),       # Description
            rpt.get("embedUrl"),          # EmbedUrl
            rpt.get("webUrl"),            # WebUrl
            rpt.get("reportType"),        # ReportType
            rpt.get("sensitivityLabel"),  # SensitivityLabel
            rpt.get("appId"),             # AppId
            rpt.get("datasetId"),         # DatasetId
            None,                         # WorkspaceId — NULL (legacy script omits)
            None,                         # WorkspaceName — NULL
            None,                         # WorkspaceConnection — NULL (see docstring)
            None,                         # CapacityId — NULL
            None,                         # CapacityName — NULL
            None,                         # Source — NULL
            rpt.get("createdBy"),         # CreatedBy
            rpt.get("createdDateTime"),   # CreatedDateTime
            rpt.get("modifiedBy"),        # ModifiedBy
            rpt.get("modifiedDateTime"),  # ModifiedDateTime
            None,                         # CreatedByUPN — Phase 5
            None,                         # CreatedBySamAccountName — Phase 5
            None,                         # CreatedByDomain — Phase 5
            None,                         # ModifiedByUPN — Phase 5
            None,                         # ModifiedBySamAccountName — Phase 5
            None,                         # ModifiedByDomain — Phase 5
            None,                         # DefaultVisibilityYN — NULL (legacy script omits)
        ))

    logger.info(f"  Extracted {len(rows)} reports")
    return rows


def extract_datasets(access_token: str, logger: logging.Logger) -> list:
    """
    Extract all PBI datasets from /admin/datasets (flat list, $top/$skip paged).

    Property mapping (legacy $datasetList → DATASET_COLUMNS):
        id    → DatasetId    (renamed for PK clarity)
        name  → DatasetName  (Phase 2 SSMS rename — matches *Name convention)
        all other 15 properties → same column name verbatim (case-insensitive)

    The API field names (ds.get(...)) remain lowercase per the legacy
    $datasetList property names — only the destination column names
    were renamed. The 2nd row position now writes the API "name" field
    into the live "DatasetName" column.

    SQL Server implicit conversion handles bit/datetime/string columns
    via the str-cast + per-column truncation pattern in bulk_insert().
    Same defensive pattern as atlas_pbi_events.py.
    """
    logger.info("Extracting datasets from /admin/datasets")

    url_template = f"{PBI_API_BASE}/admin/datasets?$top={PAGE_SIZE}&$skip={{skip}}"
    items = fetch_paged(access_token, url_template, logger)

    rows = []
    for ds in items:
        rows.append((
            ds.get("id"),                              # DatasetId (renamed)
            ds.get("name"),                            # DatasetName (renamed)
            ds.get("description"),                     # description
            ds.get("ContentProviderType"),             # ContentProviderType
            ds.get("CreateReportEmbedURL"),            # CreateReportEmbedURL
            ds.get("CreatedDate"),                     # CreatedDate
            ds.get("IsEffectiveIdentityRequired"),     # IsEffectiveIdentityRequired
            ds.get("IsEffectiveIdentityRolesRequired"),# IsEffectiveIdentityRolesRequired
            ds.get("IsOnPremGatewayRequired"),         # IsOnPremGatewayRequired
            ds.get("IsRefreshable"),                   # IsRefreshable
            ds.get("QnaEmbedURL"),                     # QnaEmbedURL
            ds.get("addRowsAPIEnabled"),               # addRowsAPIEnabled
            ds.get("configuredBy"),                    # configuredBy
            ds.get("schemaMayNotBeUpToDate"),          # schemaMayNotBeUpToDate
            ds.get("schemaRetrievalError"),            # schemaRetrievalError
            ds.get("sensitivityLabel"),                # sensitivityLabel
            ds.get("webUrl"),                          # webUrl
        ))

    logger.info(f"  Extracted {len(rows)} datasets")
    return rows


def extract_apps(access_token: str, logger: logging.Logger) -> list:
    """
    Extract all PBI apps from /admin/apps (flat list, $top/$skip paged).

    Property mapping (legacy $appList → APP_COLUMNS):
        id          → AppId
        name        → AppName
        description → Description
        lastUpdate  → LastUpdate
        publishedBy → PublishedBy
    """
    logger.info("Extracting apps from /admin/apps")

    url_template = f"{PBI_API_BASE}/admin/apps?$top={PAGE_SIZE}&$skip={{skip}}"
    items = fetch_paged(access_token, url_template, logger)

    rows = []
    for app in items:
        rows.append((
            app.get("id"),          # AppId
            app.get("name"),        # AppName
            app.get("description"), # Description
            app.get("lastUpdate"),  # LastUpdate
            app.get("publishedBy"), # PublishedBy
        ))

    logger.info(f"  Extracted {len(rows)} apps")
    return rows


def extract_capacities(access_token: str, logger: logging.Logger) -> list:
    """
    Extract all PBI capacities from /admin/capacities.

    Single non-paginated call (matches legacy pbi_Main_Meta.ps1 lines 339-345).
    The capacity list is small (typically <20 rows for a tenant), so no $top
    or $skip parameters are used.

    Property mapping (legacy $capacityList → CAPACITY_COLUMNS):
        id                      → CapacityId
        displayName             → DisplayName
        sku                     → Sku
        state                   → State
        capacityUserAccessRight → CapacityUserAccessRight
        region                  → Region
    """
    logger.info("Extracting capacities from /admin/capacities")

    url = f"{PBI_API_BASE}/admin/capacities"
    items = fetch_single(access_token, url, logger)

    rows = []
    for cap in items:
        rows.append((
            cap.get("id"),                      # CapacityId
            cap.get("displayName"),             # DisplayName
            cap.get("sku"),                     # Sku
            cap.get("state"),                   # State
            cap.get("capacityUserAccessRight"), # CapacityUserAccessRight
            cap.get("region"),                  # Region
        ))

    logger.info(f"  Extracted {len(rows)} capacities")
    return rows


# ---------------------------------------------------------------------------
# Database operations — TRUNCATE and bulk insert
# ---------------------------------------------------------------------------
def truncate_table(conn: pyodbc.Connection, logger: logging.Logger, table_name: str):
    """Truncate a raw_v2 table (preserves indexes — no DROP/CREATE)."""
    cursor = conn.cursor()
    logger.info(f"Truncating {table_name}")
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    conn.commit()


def bulk_insert(
    conn: pyodbc.Connection,
    logger: logging.Logger,
    table_name: str,
    columns: list,
    rows: list,
    string_limits: Optional[dict] = None,
    input_sizes: Optional[list] = None,
    batch_size: int = 5000,
) -> int:
    """
    Bulk insert row tuples into a raw_v2 table using fast_executemany.

    Uses parameterized INSERT (no string interpolation) and follows the
    same defensive str-cast pattern as atlas_pbi_events.py:load_events_to_db
    (line 562-572) — see that file for the rationale on numeric overflow.

    String pre-truncation (string_limits):
        If string_limits is provided, every value in a column listed there
        is converted to str() and truncated to the dict's max length BEFORE
        executemany sees the batch. This caps individual values to the DDL
        width and is part one of the truncation defense.

    Buffer sizing (input_sizes):
        If input_sizes is provided, cursor.setinputsizes() is called right
        after fast_executemany is enabled and before executemany. Each
        entry is a (sql_type, column_size, decimal_digits) tuple aligned
        positionally with `columns`. This explicitly declares the ODBC
        parameter buffer shape for every column, bypassing pyodbc's
        first-row inference.

        Why both? Pre-truncation alone is NOT enough: even after capping
        every value to the DDL max, fast_executemany would still infer
        the buffer size from the FIRST row's actual length. If row 1 has
        a 10-char CreatedBy and row 2 has a 48-char CreatedBy, the second
        row still throws "right truncation: length 48 buffer 10". Only
        explicit setinputsizes() fixes that — it locks the buffer at the
        DDL width regardless of any individual value's length. The two
        defenses are complementary: setinputsizes guarantees the buffer
        is wide enough; pre-truncation guarantees no value exceeds it.

    Args:
        conn: Database connection.
        logger: Logger instance.
        table_name: Fully-qualified table name (e.g. 'raw_v2.PbiReport').
        columns: Column name list — must match the row tuple order.
        rows: List of tuples, each tuple matching `columns` length and order.
        string_limits: Optional {column_name: max_chars} dict. Each listed
            column is pre-truncated to max_chars before insert. None values
            stay None and are NOT converted to the literal string "None".
            Columns not in this dict are passed through untouched.
        input_sizes: Optional list of (sql_type, column_size, decimal_digits)
            tuples — one per INSERT column, in the same order as `columns`.
            Passed to cursor.setinputsizes() to lock parameter buffer
            shapes. Length MUST equal len(columns).
        batch_size: Rows per executemany call (default 5000).

    Returns:
        Total rows inserted across all batches.
    """
    if not rows:
        logger.info(f"  No rows to insert into {table_name}")
        return 0

    # ------------------------------------------------------------------
    # Pre-truncation pass — only runs if string_limits is provided.
    # Builds a list of (column_index, max_len) tuples once, then walks
    # every row exactly once. None values are preserved as None.
    # Columns absent from string_limits are left untouched (datetime,
    # nvarchar(max), and any future non-truncated types).
    # ------------------------------------------------------------------
    if string_limits:
        truncation_indices = [
            (columns.index(col), max_len)
            for col, max_len in string_limits.items()
            if col in columns
        ]
        if truncation_indices:
            new_rows = []
            for row in rows:
                row_list = list(row)
                for idx, max_len in truncation_indices:
                    v = row_list[idx]
                    if v is None:
                        # Preserve NULL — never coerce to literal "None"
                        continue
                    s = str(v)
                    if len(s) > max_len:
                        s = s[:max_len]
                    row_list[idx] = s
                new_rows.append(tuple(row_list))
            rows = new_rows

    cursor = conn.cursor()

    # Bracket every column name for safety with mixed-case identifiers
    # (e.g. addRowsAPIEnabled, schemaMayNotBeUpToDate on PbiDataset).
    col_list = ", ".join(f"[{c}]" for c in columns)
    placeholders = ", ".join(["?"] * len(columns))
    insert_sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

    cursor.fast_executemany = True

    # Lock ODBC parameter buffer shapes BEFORE executemany inference kicks
    # in. Without this, pyodbc would size each parameter buffer to the
    # FIRST row's actual value length and any later row with a longer
    # string would throw SQLSTATE 22001 ("right truncation: length N
    # buffer M"). With explicit setinputsizes, the buffer is locked at
    # the declared width for the entire executemany call (and all
    # subsequent batches on this cursor until reset). Datetime columns
    # use SQL_VARCHAR with width 30 because the values arrive as
    # JSON-parsed ISO-8601 strings, not Python datetime objects — see
    # the *_INPUT_SIZES module-level comment block for the full rationale.
    if input_sizes:
        if len(input_sizes) != len(columns):
            raise ValueError(
                f"bulk_insert({table_name}): input_sizes length "
                f"{len(input_sizes)} does not match columns length "
                f"{len(columns)}"
            )
        cursor.setinputsizes(input_sizes)

    total = 0

    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        # Convert all values to str (or None) — same pattern as
        # atlas_pbi_events.py. SQL Server implicit conversion handles
        # bit/datetime/numeric from string forms. Avoids pyodbc's numeric
        # overflow on JSON-parsed Python int/float values
        # (error 22003 Numeric value out of range).
        # NOTE: pre-truncated columns are already strings at this point;
        # str("foo") == "foo" so the cast is a no-op for those.
        cleaned = [
            tuple(str(v) if v is not None else None for v in row)
            for row in batch
        ]
        cursor.executemany(insert_sql, cleaned)
        conn.commit()
        total += len(cleaned)
        logger.debug(
            f"  Inserted batch into {table_name}: rows {start + 1} to {start + len(batch)}"
        )

    logger.info(f"  Loaded {total:,} rows into {table_name}")
    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Atlas ETL — Power BI Metadata Extractor (Phase 2)"
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Authenticate and count rows only; do not TRUNCATE or insert data",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug-level logging",
    )
    parser.add_argument(
        "--skip-truncate",
        action="store_true",
        help="Skip the upfront TRUNCATE of all 7 raw_v2 tables (test mode only)",
    )
    args = parser.parse_args()

    logger = setup_logging(verbose=args.verbose)
    logger.info("=" * 60)
    logger.info(f"ATLAS ETL — PBI Metadata Extractor (Pipeline B) v{VERSION}")
    exec_id = os.getenv("ATLAS_EXECUTION_ID", str(uuid.uuid4()))
    logger.info(f"Execution ID: {exec_id}")
    logger.info(f"Target:  {ATLAS_STAGING_SERVER}/{ATLAS_STAGING_DATABASE}")
    logger.info(f"Tables:  {len(ALL_TARGET_TABLES)} raw_v2.Pbi* tables")
    logger.info(f"Mode:    {'DRY RUN' if args.dry_run else 'LIVE'}")
    logger.info("=" * 60)

    overall_start = datetime.now()

    # ------------------------------------------------------------------
    # 1. Validate credentials
    # ------------------------------------------------------------------
    if not PBI_CLIENT_SECRET or PBI_TENANT_ID == "your-tenant-id":
        logger.error("Power BI API credentials not configured.")
        logger.error("Set environment variables: PBI_TENANT_ID, PBI_CLIENT_ID, PBI_CLIENT_SECRET")
        logger.error("Or update atlas_config.py with your Azure AD app registration values.")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 2. Authenticate with Azure AD
    # ------------------------------------------------------------------
    access_token = get_access_token(logger)
    if not access_token:
        logger.critical("Authentication failed — cannot proceed")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 3. Connect to database
    # ------------------------------------------------------------------
    try:
        conn = get_connection()
        logger.info("Database connection established")
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 4. TRUNCATE all 7 target tables upfront
    #    No FK constraints between these tables — order is arbitrary.
    #    Doing all truncates BEFORE any inserts ensures partial-failure
    #    states leave the truncated tables empty rather than mixed.
    # ------------------------------------------------------------------
    if args.dry_run:
        logger.info("[DRY RUN] Skipping TRUNCATE of 7 tables")
    elif args.skip_truncate:
        logger.warning("[--skip-truncate] Skipping TRUNCATE — existing rows will remain")
    else:
        for table in ALL_TARGET_TABLES:
            try:
                truncate_table(conn, logger, table)
            except Exception as e:
                logger.critical(f"TRUNCATE failed for {table}: {e}")
                conn.close()
                sys.exit(1)

    # ------------------------------------------------------------------
    # 5. Extract and load all 5 endpoints / 7 tables
    #    Each endpoint is a separate logged step. Failures in one step
    #    do not abort subsequent steps — extraction is best-effort and
    #    a partial failure is logged but does not stop the pipeline.
    # ------------------------------------------------------------------
    step_seq = 0
    total_rows = 0
    any_failure = False

    # ── Step 1: Workspaces (with $expand=reports,users) — populates 3 tables ──
    step_seq += 1
    log_id = log_start(conn, exec_id, "Extract PBI Workspaces (+reports, +users)", step_seq)
    try:
        ws_rows, wsr_rows, wsu_rows = extract_workspaces(access_token, logger)
        if args.dry_run:
            n = len(ws_rows) + len(wsr_rows) + len(wsu_rows)
            logger.info(
                f"  [DRY RUN] Would insert {len(ws_rows)} workspaces, "
                f"{len(wsr_rows)} workspace-reports, {len(wsu_rows)} workspace-users"
            )
        else:
            n1 = bulk_insert(
                conn, logger, WORKSPACE_TABLE, WORKSPACE_COLUMNS, ws_rows,
                string_limits=WORKSPACE_LIMITS,
                input_sizes=WORKSPACE_INPUT_SIZES,
            )
            n2 = bulk_insert(
                conn, logger, WORKSPACE_REPORT_TABLE, WORKSPACE_REPORT_COLUMNS, wsr_rows,
                string_limits=WORKSPACE_REPORT_LIMITS,
                input_sizes=WORKSPACE_REPORT_INPUT_SIZES,
            )
            n3 = bulk_insert(
                conn, logger, WORKSPACE_USER_TABLE, WORKSPACE_USER_COLUMNS, wsu_rows,
                string_limits=WORKSPACE_USER_LIMITS,
                input_sizes=WORKSPACE_USER_INPUT_SIZES,
            )
            n = n1 + n2 + n3
        log_end(conn, log_id, n)
        total_rows += n
    except Exception as e:
        msg = f"Workspaces extraction failed: {e}"
        logger.error(f"  FAILED: {msg}")
        log_end(conn, log_id, 0, "Failure", msg)
        any_failure = True

    # ── Step 2: Reports ──
    step_seq += 1
    log_id = log_start(conn, exec_id, "Extract PBI Reports", step_seq)
    try:
        rpt_rows = extract_reports(access_token, logger)
        if args.dry_run:
            n = len(rpt_rows)
            logger.info(f"  [DRY RUN] Would insert {n} reports")
        else:
            n = bulk_insert(
                conn, logger, REPORT_TABLE, REPORT_COLUMNS, rpt_rows,
                string_limits=REPORT_LIMITS,
                input_sizes=REPORT_INPUT_SIZES,
            )
        log_end(conn, log_id, n)
        total_rows += n
    except Exception as e:
        msg = f"Reports extraction failed: {e}"
        logger.error(f"  FAILED: {msg}")
        log_end(conn, log_id, 0, "Failure", msg)
        any_failure = True

    # ── Step 3: Datasets ──
    step_seq += 1
    log_id = log_start(conn, exec_id, "Extract PBI Datasets", step_seq)
    try:
        ds_rows = extract_datasets(access_token, logger)
        if args.dry_run:
            n = len(ds_rows)
            logger.info(f"  [DRY RUN] Would insert {n} datasets")
        else:
            n = bulk_insert(
                conn, logger, DATASET_TABLE, DATASET_COLUMNS, ds_rows,
                string_limits=DATASET_LIMITS,
                input_sizes=DATASET_INPUT_SIZES,
            )
        log_end(conn, log_id, n)
        total_rows += n
    except Exception as e:
        msg = f"Datasets extraction failed: {e}"
        logger.error(f"  FAILED: {msg}")
        log_end(conn, log_id, 0, "Failure", msg)
        any_failure = True

    # ── Step 4: Apps ──
    step_seq += 1
    log_id = log_start(conn, exec_id, "Extract PBI Apps", step_seq)
    try:
        app_rows = extract_apps(access_token, logger)
        if args.dry_run:
            n = len(app_rows)
            logger.info(f"  [DRY RUN] Would insert {n} apps")
        else:
            n = bulk_insert(
                conn, logger, APP_TABLE, APP_COLUMNS, app_rows,
                string_limits=APP_LIMITS,
                input_sizes=APP_INPUT_SIZES,
            )
        log_end(conn, log_id, n)
        total_rows += n
    except Exception as e:
        msg = f"Apps extraction failed: {e}"
        logger.error(f"  FAILED: {msg}")
        log_end(conn, log_id, 0, "Failure", msg)
        any_failure = True

    # ── Step 5: Capacities ──
    step_seq += 1
    log_id = log_start(conn, exec_id, "Extract PBI Capacities", step_seq)
    try:
        cap_rows = extract_capacities(access_token, logger)
        if args.dry_run:
            n = len(cap_rows)
            logger.info(f"  [DRY RUN] Would insert {n} capacities")
        else:
            n = bulk_insert(
                conn, logger, CAPACITY_TABLE, CAPACITY_COLUMNS, cap_rows,
                string_limits=CAPACITY_LIMITS,
                input_sizes=CAPACITY_INPUT_SIZES,
            )
        log_end(conn, log_id, n)
        total_rows += n
    except Exception as e:
        msg = f"Capacities extraction failed: {e}"
        logger.error(f"  FAILED: {msg}")
        log_end(conn, log_id, 0, "Failure", msg)
        any_failure = True

    # ------------------------------------------------------------------
    # 6. Summary
    # ------------------------------------------------------------------
    elapsed = (datetime.now() - overall_start).total_seconds()

    if any_failure:
        logger.warning(
            f"PBI Metadata extraction completed with FAILURES: "
            f"{total_rows:,} total rows ({elapsed:.1f}s)"
        )
        conn.close()
        sys.exit(1)
    else:
        logger.info(
            f"PBI Metadata extraction complete: "
            f"{total_rows:,} total rows across {len(ALL_TARGET_TABLES)} tables ({elapsed:.1f}s)"
        )
        conn.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
