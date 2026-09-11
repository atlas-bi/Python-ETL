"""
Atlas ETL Migration — Phase 5: atlas_pbi_user_identity.py

Microsoft Graph API user identity enrichment for PBI metadata and
activity events. Python translation of pbi_User_Identity_Main_Meta.ps1
and pbi_User_Identity_Activity_Events.ps1 (legacy PowerShell scripts
in AtlasProject/Reference/pbi_legacy/).

Replaces:
    pbi_User_Identity_Main_Meta.ps1         (legacy — updates raw.pbi-report)
    pbi_User_Identity_Activity_Events.ps1   (legacy — updates raw.PbiActivityEvent)

Uses the Microsoft Graph $batch endpoint
(POST https://graph.microsoft.com/v1.0/$batch) with up to 20 user lookups
per request, then UPDATEs the resolved identities back into two raw_v2
tables.

Option B: preserve UserId GUID, resolve UPN into a SEPARATE UserUPN
column on raw_v2.PbiActivityEvent. The legacy script overwrote the
userId column with the UPN; Pipeline B keeps both so downstream
consumers can see either the original API value or the human-readable
UPN.

Pipeline position:
    atlas_pbi_metadata.py        -> raw_v2.Pbi* metadata tables (Phase 2)
    atlas_pbi_metadata.py        -> raw_v2.PbiReport.CreatedBy/ModifiedBy (GUIDs)
    atlas_pbi_user_identity.py   -> raw_v2.PbiReport.CreatedByUPN/SAM/Domain
                                 -> raw_v2.PbiReport.ModifiedByUPN/SAM/Domain
                                 -> raw_v2.PbiActivityEvent.UserUPN
    usp_Atlas_PowerBI            -> reads enriched PbiReport (Author/LastModifiedBy)
    atlas_pbi_events.py          -> raw_v2.PbiActivityEvent.UserId (Phase 1)
    usp_Atlas_RunData Phase 4b   -> passes UserUPN through to PbiActivityEventJoined
    usp_Atlas_RunData Phase 7    -> ISNULL(p.UserUPN, p.UserId) AS RunUserName

Step 1 — raw_v2.PbiReport enrichment:
    Collect DISTINCT CreatedBy UNION DISTINCT ModifiedBy GUIDs.
    Batch-resolve via Graph $batch (20 per request).
    UPDATE PbiReport SET CreatedByUPN/SAM/Domain WHERE CreatedBy = guid.
    UPDATE PbiReport SET ModifiedByUPN/SAM/Domain WHERE ModifiedBy = guid.
    (Two UPDATEs per resolved GUID because CreatedBy and ModifiedBy are
    two separate columns that may or may not share the same user.)

Step 2 — raw_v2.PbiActivityEvent enrichment:
    Collect DISTINCT UserId WHERE UserUPN IS NULL (incremental — only
    unresolved users). Batch-resolve via Graph $batch. UPDATE
    PbiActivityEvent SET UserUPN = resolved WHERE UserId = guid.
    Does NOT overwrite UserId — Option B preservation.

Dependencies:
    - Python 3.11+ with pyodbc (urllib.request for HTTP — no requests lib)
    - Azure AD App Registration with:
        * Power BI Admin API: Tenant.Read.All (sanity-check only)
        * Microsoft Graph: User.Read.All (actual use)
      Same AppId as atlas_pbi_events.py / atlas_pbi_metadata.py — no
      new IT request needed.
    - atlas_config.py for credentials and connection strings
    - Atlas_Staging database with:
        * raw_v2.PbiActivityEvent.UserUPN column (Phase 5 Commit 1 DDL)
        * raw_v2.PbiReport.CreatedByUPN/SAM/Domain + ModifiedByUPN/SAM/Domain
          (Phase 2 SSMS DDL)

Usage:
    python atlas_pbi_user_identity.py                  # Full enrichment
    python atlas_pbi_user_identity.py --dry-run        # Count resolvable GUIDs only
    python atlas_pbi_user_identity.py --verbose        # Debug logging

404 handling:
    Graph $batch returns 404 responses INSIDE the response body (each
    item in responses[] has its own 'status' field). A 404 means the
    user was deleted from AD. Those GUIDs are logged and skipped —
    their UPN/SAM/Domain columns remain NULL. Matches legacy script
    behavior.

Rate limiting:
    Graph batch-level 429 is handled with Retry-After header or 10s
    fallback. Per-item errors in the batch response don't trigger
    batch-level retry. Same pattern as atlas_pbi_events.py.

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

# Azure AD / resource URIs
AZURE_TOKEN_URL  = "https://login.microsoftonline.com/{tenant}/oauth2/token"
PBI_RESOURCE     = "https://analysis.windows.net/powerbi/api"  # sanity check only
GRAPH_RESOURCE   = "https://graph.microsoft.com"

# Microsoft Graph endpoints
GRAPH_API_BASE   = "https://graph.microsoft.com/v1.0"
GRAPH_BATCH_URL  = "https://graph.microsoft.com/v1.0/$batch"

# Graph $batch hard limit — max 20 individual requests per batch
GRAPH_BATCH_SIZE = 20

# $select fields — verbatim from legacy pbi_User_Identity_Main_Meta.ps1 line 137
# The legacy script requests UserPrincipalName with capital U, which Graph
# accepts case-insensitively but returns as lowercase 'userPrincipalName'
# in the response body.
GRAPH_SELECT = (
    "userPrincipalName,"
    "onPremisesUserPrincipalName,"
    "onPremisesSamAccountName,"
    "onPremisesDomainName"
)

PACKAGE_NAME = "atlas_pbi_user_identity"


# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------
def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure structured logging (matches atlas_pbi_events.py pattern)."""
    log_level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger("atlas_pbi_user_identity")
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
# Azure AD Authentication (parameterized resource)
# ---------------------------------------------------------------------------
def get_access_token(resource: str, logger: logging.Logger) -> Optional[str]:
    """
    Acquire an Azure AD access token via client_credentials grant for the
    specified resource URI.

    Same urllib + v1.0 OAuth endpoint pattern as atlas_pbi_events.py, but
    parameterized on the `resource` argument so this script can acquire
    tokens for both the Power BI Admin API (sanity check) and Microsoft
    Graph (actual use) without duplicating the function.

    Args:
        resource: Azure AD resource URI, e.g.
                  "https://analysis.windows.net/powerbi/api" (PBI Admin)
                  "https://graph.microsoft.com"              (Graph)
        logger: Logger instance.

    Returns:
        Bearer token string, or None on failure.
    """
    token_url = AZURE_TOKEN_URL.format(tenant=PBI_TENANT_ID)

    payload = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": PBI_CLIENT_ID,
        "client_secret": PBI_CLIENT_SECRET,
        "resource": resource,
    }).encode("utf-8")

    logger.debug(f"Requesting token from: {token_url}")
    logger.debug(f"Resource: {resource}")
    logger.debug(f"Client ID: {PBI_CLIENT_ID[:8]}...")

    try:
        req = urllib.request.Request(token_url, data=payload, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            token_data = json.loads(resp.read().decode("utf-8"))
            access_token = token_data.get("access_token")

            if not access_token:
                logger.error(f"Token response missing 'access_token' field for resource: {resource}")
                logger.debug(f"Response: {json.dumps(token_data, indent=2)}")
                return None

            logger.info(f"Azure AD authentication successful for: {resource}")
            return access_token

    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        logger.error(f"Token request failed (HTTP {e.code}) for resource: {resource}")
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
# Collect GUIDs from the database
# ---------------------------------------------------------------------------
def collect_report_guids(conn: pyodbc.Connection, logger: logging.Logger) -> set:
    """
    Collect DISTINCT user GUIDs from raw_v2.PbiReport.CreatedBy and
    raw_v2.PbiReport.ModifiedBy. Matches the legacy script's GUID
    collection query (pbi_User_Identity_Main_Meta.ps1 line 131) but
    adapted for raw_v2 column names.

    Returns:
        set[str] — distinct GUID strings (may contain both CreatedBy and
        ModifiedBy values; the DISTINCT + UNION ensures no duplicates).
    """
    sql = """
    SELECT DISTINCT CreatedBy AS guid
    FROM raw_v2.PbiReport
    WHERE CreatedBy IS NOT NULL
    UNION
    SELECT DISTINCT ModifiedBy AS guid
    FROM raw_v2.PbiReport
    WHERE ModifiedBy IS NOT NULL
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    guids = {row[0] for row in rows if row[0]}
    logger.info(f"  Collected {len(guids):,} distinct report user GUIDs")
    return guids


def collect_event_guids(conn: pyodbc.Connection, logger: logging.Logger) -> set:
    """
    Collect DISTINCT user GUIDs from raw_v2.PbiActivityEvent.UserId
    WHERE UserUPN IS NULL — incremental resolution, only unresolved users.

    Re-running this script on subsequent pipeline runs will skip users
    whose UPN has already been resolved (via the UserUPN IS NULL filter),
    making the enrichment step efficient on steady-state runs.

    Returns:
        set[str] — distinct GUID strings.
    """
    sql = """
    SELECT DISTINCT UserId AS guid
    FROM raw_v2.PbiActivityEvent
    WHERE UserId IS NOT NULL
      AND UserUPN IS NULL
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    guids = {row[0] for row in rows if row[0]}
    logger.info(f"  Collected {len(guids):,} distinct activity-event user GUIDs (UserUPN IS NULL)")
    return guids


# ---------------------------------------------------------------------------
# Microsoft Graph $batch user lookup
# ---------------------------------------------------------------------------
def build_batch_request(guid_batch: list) -> dict:
    """
    Build the JSON body for a Microsoft Graph $batch request.

    Each individual request in the batch is a GET to /users/{id} with the
    $select parameter URL-encoded. Graph enforces a max of 20 requests per
    batch (GRAPH_BATCH_SIZE).

    Args:
        guid_batch: list of up to 20 GUIDs / UPNs / identifiers.

    Returns:
        dict ready to be JSON-encoded as the Graph $batch POST body.
    """
    # URL-encode $select once; each per-user URL reuses it
    select_encoded = urllib.parse.quote(GRAPH_SELECT, safe=",")

    requests = []
    for i, guid in enumerate(guid_batch):
        # URL-encode the GUID / identifier itself (handles UPNs containing '@')
        guid_encoded = urllib.parse.quote(str(guid), safe="")
        requests.append({
            "id": str(i),
            "method": "GET",
            "url": f"/users/{guid_encoded}?$select={select_encoded}",
        })

    return {"requests": requests}


def call_graph_batch(
    access_token: str,
    guid_batch: list,
    logger: logging.Logger,
) -> dict:
    """
    POST a Graph $batch request and parse the per-item responses.

    Returns a dict mapping each input GUID to its resolved identity dict:
        { guid: {"upn": str|None, "sam": str|None, "domain": str|None} }

    GUIDs that return 404 (user deleted from AD) are mapped to None.
    Per-item errors other than 404 are logged and mapped to None.
    Batch-level 429 is handled with Retry-After header or 10s fallback,
    then a single retry. Batch-level 5xx or network errors are retried
    up to 3 times with exponential backoff.
    """
    body = build_batch_request(guid_batch)
    body_bytes = json.dumps(body).encode("utf-8")

    network_retries = 0
    max_network_retries = 3
    network_retry_delay = 10
    rate_limit_retries = 0
    max_rate_limit_retries = 2

    result = {guid: None for guid in guid_batch}

    while True:
        try:
            req = urllib.request.Request(
                GRAPH_BATCH_URL,
                data=body_bytes,
                method="POST",
            )
            req.add_header("Authorization", f"Bearer {access_token}")
            req.add_header("Content-Type", "application/json")

            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            # Parse per-item responses
            responses = data.get("responses", []) or []
            for item in responses:
                item_id = item.get("id")
                status = item.get("status", 0)
                item_body = item.get("body") or {}

                # Map item_id back to the original GUID via index
                try:
                    idx = int(item_id)
                    guid = guid_batch[idx]
                except (TypeError, ValueError, IndexError):
                    logger.warning(f"  Unexpected batch response id: {item_id}")
                    continue

                if status == 200:
                    # Parse the resolved identity. Legacy script uses
                    # .userPrincipalName (NOT .onPremisesUserPrincipalName).
                    upn    = item_body.get("userPrincipalName")
                    sam    = item_body.get("onPremisesSamAccountName")
                    domain = item_body.get("onPremisesDomainName")
                    result[guid] = {"upn": upn, "sam": sam, "domain": domain}
                elif status == 404:
                    # User deleted from AD — log and continue (matches
                    # legacy script's NotFound handling).
                    logger.debug(f"  Skipping record: Could not find identity: {guid}")
                    result[guid] = None
                else:
                    # Other per-item error — log and continue.
                    err_code = item_body.get("error", {}).get("code", "unknown") \
                        if isinstance(item_body, dict) else "unknown"
                    logger.warning(
                        f"  Graph batch item {guid}: HTTP {status} {err_code}"
                    )
                    result[guid] = None

            return result

        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")
            if e.code == 429 and rate_limit_retries < max_rate_limit_retries:
                retry_after = int(e.headers.get("Retry-After", 10))
                logger.warning(f"  Graph rate limited, retrying in {retry_after}s")
                time.sleep(retry_after)
                rate_limit_retries += 1
                continue
            elif e.code in (500, 502, 503, 504) and network_retries < max_network_retries:
                network_retries += 1
                logger.warning(
                    f"  Graph transient error HTTP {e.code} "
                    f"(attempt {network_retries}/{max_network_retries})"
                )
                time.sleep(network_retry_delay)
                continue
            else:
                logger.error(f"  Graph batch HTTP {e.code}: {err_body[:500]}")
                return result  # all guids remain None

        except (urllib.error.URLError, ConnectionResetError, TimeoutError) as e:
            network_retries += 1
            reason = getattr(e, "reason", e)
            if network_retries <= max_network_retries:
                logger.warning(
                    f"  Graph network error "
                    f"(attempt {network_retries}/{max_network_retries}): {reason}"
                )
                time.sleep(network_retry_delay)
                continue
            else:
                logger.error(
                    f"  Graph network error after {max_network_retries} retries: {reason}"
                )
                return result  # all guids remain None


def resolve_users(
    access_token: str,
    guid_set: set,
    cache: dict,
    logger: logging.Logger,
) -> dict:
    """
    Resolve a set of GUIDs to identity dicts via Microsoft Graph $batch.

    Iterates the set in chunks of GRAPH_BATCH_SIZE, calling call_graph_batch
    for each chunk. Results are merged into the `cache` dict (which persists
    across multiple resolve_users calls within the same main() run — Step 1
    and Step 2 share it so GUIDs that appear in both the report tables and
    the activity events are only resolved once).

    Args:
        access_token: Graph bearer token.
        guid_set: set of GUIDs to resolve.
        cache: persistent dict[guid → identity | None] shared across calls.
               Already-resolved GUIDs are skipped.
        logger: Logger instance.

    Returns:
        The updated cache dict.
    """
    # Remove already-resolved GUIDs from the work set
    to_resolve = [g for g in guid_set if g not in cache]
    logger.info(
        f"  {len(to_resolve):,} new GUIDs to resolve "
        f"({len(guid_set) - len(to_resolve):,} already cached)"
    )

    if not to_resolve:
        return cache

    # Chunk and call Graph batch
    batches_run = 0
    not_found = 0
    resolved_ok = 0
    total_batches = (len(to_resolve) + GRAPH_BATCH_SIZE - 1) // GRAPH_BATCH_SIZE

    for start in range(0, len(to_resolve), GRAPH_BATCH_SIZE):
        chunk = to_resolve[start : start + GRAPH_BATCH_SIZE]
        batch_result = call_graph_batch(access_token, chunk, logger)
        cache.update(batch_result)

        for guid, identity in batch_result.items():
            if identity is None:
                not_found += 1
            else:
                resolved_ok += 1

        batches_run += 1
        if batches_run % 10 == 0:
            logger.info(
                f"  Progress: {batches_run}/{total_batches} batches "
                f"({resolved_ok} resolved, {not_found} not found so far)"
            )

    logger.info(
        f"  Resolved {resolved_ok:,} users, {not_found:,} not found "
        f"across {batches_run} batches"
    )
    return cache


# ---------------------------------------------------------------------------
# Database UPDATEs — enrichment back to raw_v2 tables
# ---------------------------------------------------------------------------
def enrich_pbi_report(
    conn: pyodbc.Connection,
    resolutions: dict,
    logger: logging.Logger,
) -> int:
    """
    UPDATE raw_v2.PbiReport with resolved identity columns.

    Two UPDATEs per resolved GUID: one for CreatedBy match, one for
    ModifiedBy match. A user can be a creator on one report and a
    modifier on another, so both column sets need enrichment.

    Uses executemany where possible for performance. Skips GUIDs with
    None resolution (404 or error — leaves the legacy NULL in place).

    Args:
        conn: Database connection.
        resolutions: dict[guid → identity dict | None]
        logger: Logger instance.

    Returns:
        Total rows affected across both UPDATE statements.
    """
    # Build parameter batches for executemany
    params = [
        (r["upn"], r["sam"], r["domain"], guid)
        for guid, r in resolutions.items()
        if r is not None
    ]

    if not params:
        logger.info("  No resolved PbiReport users to enrich")
        return 0

    update_created = """
        UPDATE raw_v2.PbiReport
        SET CreatedByUPN            = ?,
            CreatedBySamAccountName = ?,
            CreatedByDomain         = ?
        WHERE CreatedBy = ?
    """
    update_modified = """
        UPDATE raw_v2.PbiReport
        SET ModifiedByUPN            = ?,
            ModifiedBySamAccountName = ?,
            ModifiedByDomain         = ?
        WHERE ModifiedBy = ?
    """

    cursor = conn.cursor()
    cursor.fast_executemany = True

    total = 0

    # NOTE on row counting: pyodbc's cursor.rowcount returns -1 after a
    # fast_executemany batch UPDATE — the ODBC driver does not aggregate
    # per-row affected counts when fast_executemany is enabled. Using
    # `cursor.rowcount if cursor.rowcount >= 0 else 0` silently converts
    # every successful batch update into a logged "0 rows" line.
    # To get accurate telemetry, we follow each executemany with a
    # SELECT COUNT(*) WHERE <target column> IS NOT NULL. This is safe
    # because raw_v2.PbiReport is TRUNCATE+repopulated every pipeline
    # run by atlas_pbi_metadata.py, so all Phase 5 enrichment columns
    # start each run as NULL — a post-UPDATE non-NULL count equals the
    # rows affected by this run's UPDATE.

    cursor.executemany(update_created, params)
    conn.commit()
    cursor.execute(
        "SELECT COUNT(*) FROM raw_v2.PbiReport WHERE CreatedByUPN IS NOT NULL"
    )
    created_count = cursor.fetchone()[0]
    logger.debug(f"  UPDATE PbiReport CreatedBy*: {created_count} rows")
    total += created_count

    cursor.executemany(update_modified, params)
    conn.commit()
    cursor.execute(
        "SELECT COUNT(*) FROM raw_v2.PbiReport WHERE ModifiedByUPN IS NOT NULL"
    )
    modified_count = cursor.fetchone()[0]
    logger.debug(f"  UPDATE PbiReport ModifiedBy*: {modified_count} rows")
    total += modified_count

    logger.info(
        f"  Enriched raw_v2.PbiReport: "
        f"{created_count} CreatedBy rows + {modified_count} ModifiedBy rows "
        f"from {len(params)} resolved GUIDs"
    )
    return total


def enrich_pbi_activity_event(
    conn: pyodbc.Connection,
    resolutions: dict,
    logger: logging.Logger,
) -> int:
    """
    UPDATE raw_v2.PbiActivityEvent.UserUPN with the resolved UPN.

    Option B: do NOT overwrite UserId.
    The original API value is preserved; UserUPN is a NEW column that
    holds the Graph-resolved human-readable UPN. Phase 7 downstream
    uses ISNULL(p.UserUPN, p.UserId) to prefer the resolved UPN.

    Uses executemany for performance. Skips GUIDs with None resolution
    (404 or error — leaves UserUPN NULL, Phase 7 falls back to UserId).

    Args:
        conn: Database connection.
        resolutions: dict[guid → identity dict | None]
        logger: Logger instance.

    Returns:
        Total rows affected.
    """
    params = [
        (r["upn"], guid)
        for guid, r in resolutions.items()
        if r is not None and r.get("upn")
    ]

    if not params:
        logger.info("  No resolved PbiActivityEvent users to enrich")
        return 0

    update_sql = """
        UPDATE raw_v2.PbiActivityEvent
        SET UserUPN = ?
        WHERE UserId = ?
    """

    cursor = conn.cursor()
    cursor.fast_executemany = True
    cursor.executemany(update_sql, params)
    conn.commit()

    # pyodbc cursor.rowcount is -1 after a fast_executemany batch UPDATE
    # — see the note in enrich_pbi_report() for the rationale. Use a
    # post-UPDATE COUNT(*) to get the accurate number. raw_v2.PbiActivityEvent
    # is TRUNCATE+repopulated each run by atlas_pbi_events.py, so UserUPN
    # starts each run NULL for every row — a post-UPDATE non-NULL count
    # equals the rows affected by this run's UPDATE.
    cursor.execute(
        "SELECT COUNT(*) FROM raw_v2.PbiActivityEvent WHERE UserUPN IS NOT NULL"
    )
    count = cursor.fetchone()[0]

    logger.info(
        f"  Enriched raw_v2.PbiActivityEvent: "
        f"{count} rows from {len(params)} resolved UPNs"
    )
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Atlas ETL — Power BI User Identity Enrichment (Phase 5)"
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Authenticate and count resolvable GUIDs only; do not UPDATE any rows",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug-level logging",
    )
    args = parser.parse_args()

    logger = setup_logging(verbose=args.verbose)
    logger.info("=" * 60)
    logger.info(f"ATLAS ETL — PBI User Identity Enrichment (Pipeline B) v{VERSION}")
    exec_id = os.getenv("ATLAS_EXECUTION_ID", str(uuid.uuid4()))
    logger.info(f"Execution ID: {exec_id}")
    logger.info(f"Target:  {ATLAS_STAGING_SERVER}/{ATLAS_STAGING_DATABASE}")
    logger.info(f"Mode:    {'DRY RUN' if args.dry_run else 'LIVE'}")
    logger.info("=" * 60)

    overall_start = datetime.now()

    # ------------------------------------------------------------------
    # 1. Validate credentials
    # ------------------------------------------------------------------
    if not PBI_CLIENT_SECRET or PBI_TENANT_ID == "your-tenant-id":
        logger.error("Power BI / Graph API credentials not configured.")
        logger.error("Set environment variables: PBI_TENANT_ID, PBI_CLIENT_ID, PBI_CLIENT_SECRET")
        logger.error("Or update atlas_config.py with your Azure AD app registration values.")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 2. Acquire tokens
    #    Two token calls:
    #      (a) PBI Admin API — sanity check only, confirms the service
    #          principal can still acquire its primary scope. Not used
    #          for any API call in this script.
    #      (b) Microsoft Graph — used for all /users/{id} and /$batch calls.
    # ------------------------------------------------------------------
    logger.info("Acquiring tokens…")

    pbi_token = get_access_token(PBI_RESOURCE, logger)
    if not pbi_token:
        logger.critical("PBI Admin token acquisition failed — credential or scope issue")
        sys.exit(1)
    logger.info("  PBI Admin token acquired (sanity check only — not used by this script)")

    graph_token = get_access_token(GRAPH_RESOURCE, logger)
    if not graph_token:
        logger.critical("Graph API token acquisition failed — cannot enrich identities")
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

    # In-process cache shared between Step 1 and Step 2 so GUIDs that
    # appear in both PbiReport and PbiActivityEvent are only resolved once.
    resolution_cache: dict = {}

    any_failure = False
    total_affected = 0

    # ------------------------------------------------------------------
    # 4. STEP 1 — Enrich raw_v2.PbiReport
    # ------------------------------------------------------------------
    step_seq = 1
    log_id = log_start(conn, exec_id, "Enrich PbiReport via Graph API", step_seq)
    try:
        logger.info("Step 1 — raw_v2.PbiReport enrichment")
        report_guids = collect_report_guids(conn, logger)
        resolve_users(graph_token, report_guids, resolution_cache, logger)

        if args.dry_run:
            resolved = sum(1 for g in report_guids if resolution_cache.get(g) is not None)
            logger.info(
                f"  [DRY RUN] Would enrich {resolved} of {len(report_guids)} "
                f"distinct report user GUIDs"
            )
            log_end(conn, log_id, resolved)
        else:
            # Limit enrichment to GUIDs that showed up in report_guids
            report_only_resolutions = {
                g: resolution_cache[g] for g in report_guids if g in resolution_cache
            }
            n = enrich_pbi_report(conn, report_only_resolutions, logger)
            log_end(conn, log_id, n)
            total_affected += n
    except Exception as e:
        msg = f"PbiReport enrichment failed: {e}"
        logger.error(f"  FAILED: {msg}")
        log_end(conn, log_id, 0, "Failure", msg)
        any_failure = True

    # ------------------------------------------------------------------
    # 5. STEP 2 — Enrich raw_v2.PbiActivityEvent
    # ------------------------------------------------------------------
    step_seq = 2
    log_id = log_start(conn, exec_id, "Enrich PbiActivityEvent via Graph API", step_seq)
    try:
        logger.info("Step 2 — raw_v2.PbiActivityEvent enrichment")
        event_guids = collect_event_guids(conn, logger)
        resolve_users(graph_token, event_guids, resolution_cache, logger)

        if args.dry_run:
            resolved = sum(1 for g in event_guids if resolution_cache.get(g) is not None)
            logger.info(
                f"  [DRY RUN] Would enrich {resolved} of {len(event_guids)} "
                f"distinct activity-event user GUIDs"
            )
            log_end(conn, log_id, resolved)
        else:
            event_only_resolutions = {
                g: resolution_cache[g] for g in event_guids if g in resolution_cache
            }
            n = enrich_pbi_activity_event(conn, event_only_resolutions, logger)
            log_end(conn, log_id, n)
            total_affected += n
    except Exception as e:
        msg = f"PbiActivityEvent enrichment failed: {e}"
        logger.error(f"  FAILED: {msg}")
        log_end(conn, log_id, 0, "Failure", msg)
        any_failure = True

    # ------------------------------------------------------------------
    # 6. Summary
    # ------------------------------------------------------------------
    elapsed = (datetime.now() - overall_start).total_seconds()
    cache_total = len(resolution_cache)
    cache_resolved = sum(1 for v in resolution_cache.values() if v is not None)

    if any_failure:
        logger.warning(
            f"PBI User Identity enrichment completed with FAILURES: "
            f"{total_affected:,} rows updated, "
            f"{cache_resolved}/{cache_total} cache hits ({elapsed:.1f}s)"
        )
        conn.close()
        sys.exit(1)
    else:
        logger.info(
            f"PBI User Identity enrichment complete: "
            f"{total_affected:,} rows updated, "
            f"{cache_resolved}/{cache_total} users resolved ({elapsed:.1f}s)"
        )
        conn.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
