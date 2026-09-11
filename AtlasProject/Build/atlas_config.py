"""
Atlas ETL Suite - Configuration Module
======================================
Centralized configuration for the Atlas ETL pipeline.
Replaces all hardcoded values from the legacy SSIS packages.

Usage:
    from atlas_config import config
    
    # Access configuration values
    server = config.epic_clarity_server
    conn_str = config.get_atlas_staging_connection_string()

Environment Variables:
    ATLAS_ENV - Environment identifier (DEV, TEST, PROD). Default: DEV
    
Version: 3.2 (Pipeline B)
Last Updated: March 2026
"""

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def _load_credential_file() -> dict:
    """Load credentials from a flat JSON file if it exists.

    Expected format (flat, env-var-named keys):
        {
            "ATLAS_SQL_USER": "svc_atlas_prd",
            "ATLAS_SQL_PWD":  "password_here",
            "PBI_TENANT_ID":  "...",
            "PBI_CLIENT_ID":  "...",
            "PBI_CLIENT_SECRET": "..."
        }

    Search order:
      1. Path in ATLAS_CREDENTIALS env var (set by --credentials flag)
      2. C:\\AtlasETL\\credentials.json (default deployment path)

    Returns empty dict if no file found or file is invalid.
    Never logs credential values.
    """
    paths_to_try = []

    env_path = os.getenv('ATLAS_CREDENTIALS', '')
    if env_path:
        paths_to_try.append(Path(env_path))

    paths_to_try.append(Path(r'C:\AtlasETL\credentials.json'))

    for cred_path in paths_to_try:
        if cred_path.is_file():
            try:
                with open(cred_path, 'r') as f:
                    data = json.load(f)
                logger.info(f"Credentials loaded from file: {cred_path}")
                return data
            except (json.JSONDecodeError, OSError) as e:
                logger.warning(f"Failed to read credential file {cred_path}: {e}")
                return {}

    return {}


def _env_or_cred(env_var: str, cred_data: dict, default: str = '',
                 required: bool = False) -> str:
    """Return env var if set, else credential file value (flat key), else default.

    Args:
        env_var:   Environment variable name (also used as the JSON key).
        cred_data: Dict loaded from credentials.json.
        default:   Fallback when neither source provides a value.
        required:  When True, raise ValueError if no value found.
    """
    val = os.getenv(env_var, '')
    if val:
        return val
    # Fall back to credential file (flat format — key matches env var name)
    file_val = cred_data.get(env_var, '')
    if file_val:
        return file_val
    if required and not default:
        raise ValueError(
            f"Required credential '{env_var}' not found. "
            f"Set the {env_var} environment variable or add it to "
            f"credentials.json (see atlas_config.py docstring for format)."
        )
    return default


# Pre-load credential file once at import time
_cred_data = _load_credential_file()


@dataclass
class AtlasConfig:
    """
    Atlas ETL Configuration.
    
    All settings that were previously hardcoded across the 13 SSIS packages
    are centralized here. Values can be overridden via environment variables
    for production deployment.
    """
    
    # ══════════════════════════════════════════════════════════════════════════
    # ENVIRONMENT
    # ══════════════════════════════════════════════════════════════════════════
    
    environment: str = field(default_factory=lambda: _env_or_cred('ATLAS_ENV', _cred_data, 'DEV'))
    
    # ══════════════════════════════════════════════════════════════════════════
    # EPIC CLARITY SOURCE
    # ══════════════════════════════════════════════════════════════════════════
    
    # Primary Epic Clarity server
    epic_clarity_server: str = field(
        default_factory=lambda: os.getenv(
            'EPIC_CLARITY_SERVER', 'EPICCLAPRD.BILH.ITSYSTEMS.ORG')
    )
    epic_clarity_database: str = 'Clarity'
    
    # Service account for Clarity connections
    # Pipeline B: Python scripts connect directly via pyodbc (TCP 1433)
    # If SQL Auth needed, store credentials in env vars or SQL Server Credential
    epic_clarity_use_windows_auth: bool = True
    
    # ══════════════════════════════════════════════════════════════════════════
    # EPIC CABOODLE / CDW SOURCE (Pipeline B addition)
    # Used by atlas_rundata_extractor.py for SlicerDicer stats extraction
    # ══════════════════════════════════════════════════════════════════════════
    
    epic_caboodle_server: str = field(
        default_factory=lambda: os.getenv(
            'EPIC_CABOODLE_SERVER', r'EPICCDWPRD.BILH.ITSYSTEMS.ORG\EpicCDWPRD01')
    )
    epic_caboodle_database: str = field(
        default_factory=lambda: os.getenv('EPIC_CABOODLE_DB', 'CDW_SlicerDicer')
    )
    epic_caboodle_use_windows_auth: bool = True

    # ══════════════════════════════════════════════════════════════════════════
    # EPIC SERVICE ACCOUNT CREDENTIALS (CN2 — 2026-04-07)
    # ══════════════════════════════════════════════════════════════════════════
    # SQL Auth service accounts for Epic sources. When the *_sql_user field
    # has a value, the corresponding get_epic_*_connection_string() builder
    # switches from Trusted_Connection=yes to UID/PWD. Empty values preserve
    # the prior Windows Auth behavior for dev workstation use.
    #
    # Resolution: env var first, then credentials.json (flat keys), then
    # empty string. Same _env_or_cred pattern as atlas_sql_user /
    # atlas_sql_password.
    #
    # Operator setup (required before production E2E run):
    #   EPIC_CLARITY_SQL_USER   -> SVC_REPORTHUB_CLARITY  username
    #   EPIC_CLARITY_SQL_PWD    -> SVC_REPORTHUB_CLARITY  password
    #   EPIC_CABOODLE_SQL_USER  -> SVC_REPORTHUB_CABOODLE username
    #   EPIC_CABOODLE_SQL_PWD   -> SVC_REPORTHUB_CABOODLE password
    epic_clarity_sql_user: str = field(
        default_factory=lambda: _env_or_cred('EPIC_CLARITY_SQL_USER', _cred_data)
    )
    epic_clarity_sql_password: str = field(
        default_factory=lambda: _env_or_cred('EPIC_CLARITY_SQL_PWD', _cred_data)
    )
    epic_caboodle_sql_user: str = field(
        default_factory=lambda: _env_or_cred('EPIC_CABOODLE_SQL_USER', _cred_data)
    )
    epic_caboodle_sql_password: str = field(
        default_factory=lambda: _env_or_cred('EPIC_CABOODLE_SQL_PWD', _cred_data)
    )

    # ══════════════════════════════════════════════════════════════════════════
    # ATLAS DATABASES
    # ══════════════════════════════════════════════════════════════════════════
    
    # Staging database
    atlas_staging_server: str = field(
        default_factory=lambda: os.getenv('ATLAS_STAGING_SERVER', r'10.247.4.56\SQL126')
    )
    atlas_staging_database: str = field(
        default_factory=lambda: os.getenv('ATLAS_STAGING_DB', 'Atlas_Staging')
    )
    
    # Production database
    atlas_prod_server: str = field(
        default_factory=lambda: os.getenv('ATLAS_PROD_SERVER', r'10.247.4.56\SQL126')
    )
    atlas_prod_database: str = field(
        default_factory=lambda: os.getenv('ATLAS_PROD_DB', 'Atlas_Prd')
    )

    # SQL Server authentication (optional — used when Windows auth unavailable)
    # When atlas_sql_user has a value, Atlas staging/prod connections use SQL auth
    # Fallback: credentials.json → ATLAS_SQL_USER / ATLAS_SQL_PWD
    atlas_sql_user: str = field(
        default_factory=lambda: _env_or_cred('ATLAS_SQL_USER', _cred_data)
    )
    atlas_sql_password: str = field(
        default_factory=lambda: _env_or_cred('ATLAS_SQL_PWD', _cred_data)
    )

    # ══════════════════════════════════════════════════════════════════════════
    # OPTIONAL: SSRS CONFIGURATION
    # DISABLED per scope decision — set to True and configure if re-enabled
    # ══════════════════════════════════════════════════════════════════════════
    
    enable_ssrs: bool = False
    
    # SSRS Instance 1 (ssrs_database)
    ssrs1_server: str = 'ssrs_database_server'
    ssrs1_database: str = 'ReportServer'
    ssrs1_base_url: str = 'http://ssrs_database_server/Reports'
    ssrs1_hbi_users_guid: str = 'F1B753AC-0000-0000-0000-000000000000'
    ssrs1_table_prefix: str = 'SSRS1_'
    
    # SSRS Instance 2 (clarity_server)
    ssrs2_server: str = 'clarity_ssrs_server'
    ssrs2_database: str = 'ReportServer'
    ssrs2_base_url: str = 'http://clarity_ssrs_server/Reports'
    ssrs2_hbi_users_guid: str = 'A2C864BD-0000-0000-0000-000000000000'
    ssrs2_table_prefix: str = 'SSRS2_'
    
    # ══════════════════════════════════════════════════════════════════════════
    # OPTIONAL: SSAS CONFIGURATION
    # DISABLED per scope decision — set to True and configure if re-enabled
    # ══════════════════════════════════════════════════════════════════════════
    
    enable_ssas: bool = False
    
    ssas_server: str = r'slicerdicer_server\cobalt'
    ssas_provider: str = 'MSOLAP'
    
    # ══════════════════════════════════════════════════════════════════════════
    # OPTIONAL: TABLEAU CONFIGURATION
    # DISABLED per scope decision — set to True and configure if re-enabled
    # ══════════════════════════════════════════════════════════════════════════
    
    enable_tableau: bool = False
    
    tableau_server: str = 'eptblp01'
    tableau_database: str = 'workgroup'
    
    # ══════════════════════════════════════════════════════════════════════════
    # POWER BI CONFIGURATION
    # ══════════════════════════════════════════════════════════════════════════
    
    enable_power_bi: bool = True
    
    # Azure AD App Registration for Power BI API access
    # Fallback: credentials.json → PBI_TENANT_ID / PBI_CLIENT_ID / PBI_CLIENT_SECRET
    pbi_tenant_id: str = field(
        default_factory=lambda: _env_or_cred('PBI_TENANT_ID', _cred_data, 'your-tenant-id')
    )
    pbi_client_id: str = field(
        default_factory=lambda: _env_or_cred('PBI_CLIENT_ID', _cred_data, 'your-client-id')
    )
    pbi_client_secret: str = field(
        default_factory=lambda: _env_or_cred('PBI_CLIENT_SECRET', _cred_data)
    )
    
    # Power BI Activity Events API lookback window (days).
    # Hard limit enforced by Microsoft Graph API (~28-30 days max).
    # Independent of rundata_lookback_days below.
    pbi_lookback_days: int = 28

    # Clarity + SlicerDicer run data lookback window (days).
    # Default 30 days (per CN1 commitment). For the one-time
    # historical backfill, set to 180 and run the pipeline manually,
    # then reset to 30 for daily operations.
    # Note: Epic Clarity RW_RPT_RUN_DATA retains ~180 days of data
    # (confirmed 2026-04-30 — earliest available: 2025-11-01).
    # Requesting beyond 180 days will return 0 rows from Clarity.
    # Changed from rundata_rollback_weeks = 2 (14 days) — 2026-04-30.
    rundata_lookback_days: int = 30
    
    # ══════════════════════════════════════════════════════════════════════════
    # ACTIVE DIRECTORY / AZURE AD
    # ══════════════════════════════════════════════════════════════════════════
    
    # Domain names used in user identity resolution
    ad_domains: Tuple[str, ...] = ('BILH', 'ITS', 'MR1')
    
    # Primary AD domain for LDAP queries
    ad_primary_domain: str = 'BILH'
    
    # Azure AD tenant for UPN resolution
    azure_ad_tenant: str = 'YOURORGNAME.onmicrosoft.com'
    
    # ══════════════════════════════════════════════════════════════════════════
    # FILE PATHS
    # ══════════════════════════════════════════════════════════════════════════
    
    # Network share for CSV files (Clarity extracts)
    csv_network_share: str = field(
        default_factory=lambda: os.getenv(
            'ATLAS_CSV_SHARE', 
            r'\\10.209.10.225\Shared\PRD\Cogito_Reporting\SSIS_Packages\Atlas\Files'
        )
    )
    
    # Local working directory for temporary files
    working_directory: str = field(
        default_factory=lambda: os.getenv('ATLAS_WORKING_DIR', r'C:\AtlasETL\temp')
    )
    
    # Python executable path (for SQL Server Agent job steps)
    python_executable: str = field(
        default_factory=lambda: os.getenv('ATLAS_PYTHON_EXE', r'C:\Python311\python.exe')
    )
    
    # ══════════════════════════════════════════════════════════════════════════
    # TIMEOUTS AND LIMITS
    # ══════════════════════════════════════════════════════════════════════════
    
    # SQL query timeout in seconds (1 hour default)
    query_timeout_seconds: int = 3600

    # Connection timeout in seconds
    connection_timeout_seconds: int = 30

    # Per-SP query-execution timeout in seconds (default 15 minutes).
    # Applied via pyodbc connection.timeout before cursor.execute() to prevent
    # a stalled SP from blocking the entire pipeline indefinitely.
    sp_timeout_seconds: int = 900

    # RunData-specific override (30 minutes) — RunData is the longest-running
    # SP in Pipeline B, spanning Phases 1-12 with multiple MERGE operations
    # against prd_v2.ReportObjectRunData and ReportObjectRunDataBridge.
    rundata_timeout_seconds: int = 1800

    # Merge-specific override (30 minutes) — Merge runs ~20 MERGE/INSERT
    # operations into Atlas_Prd, including the FK drop/recreate cycle
    # (Merge 0d / Merge 20b). Bumped from the 900s default to match RunData.
    merge_timeout_seconds: int = 1800
    
    # Retry configuration
    max_retries: int = 3
    retry_delay_seconds: int = 10
    
    # Batch size for bulk inserts
    bulk_insert_batch_size: int = 10000

    # Rows fetched per fetchmany() call in atlas_rundata_extractor.py.
    # Controls peak memory usage during large extractions.
    # 50000 rows ≈ 50-100 MB per batch for typical RunData schemas.
    fetch_batch_size: int = 50000
    
    # ══════════════════════════════════════════════════════════════════════════
    # LOGGING
    # ══════════════════════════════════════════════════════════════════════════
    
    # Log level: DEBUG, INFO, WARNING, ERROR
    log_level: str = field(
        default_factory=lambda: os.getenv('ATLAS_LOG_LEVEL', 'INFO')
    )
    
    # Log file path (None = console only)
    log_file_path: Optional[str] = None
    
    # ══════════════════════════════════════════════════════════════════════════
    # DATABASE OBJECT EXTRACTION
    # ══════════════════════════════════════════════════════════════════════════
    
    # Databases to extract sys.all_objects from (EPICCLAPRD).
    # All four databases use SVC_REPORTHUB_CLARITY credentials.
    database_objects_sources: Tuple[str, ...] = ('Clarity', 'RW_PUB', 'EDM', 'CogitoTools')

    # ══════════════════════════════════════════════════════════════════════════
    # CUTOVER-SP DERIVED PROPERTIES
    # ══════════════════════════════════════════════════════════════════════════

    @property
    def prod_schema(self) -> str:
        """Production schema name. 'dbo' in PROD, 'prd_v2' in DEV/TEST."""
        return 'dbo' if self.environment == 'PROD' else 'prd_v2'

    @property
    def stg_schema(self) -> str:
        """Staging schema name. Always 'stage_v2'."""
        return 'stage_v2'

    @property
    def prod_database(self) -> str:
        """Production database qualifier. 'Atlas_Prd' in PROD, '' in DEV/TEST."""
        return 'Atlas_Prd' if self.environment == 'PROD' else ''

    # ══════════════════════════════════════════════════════════════════════════
    # CONNECTION STRING BUILDERS
    # ══════════════════════════════════════════════════════════════════════════
    
    def _build_connection_string(self, server: str, database: str,
                                  use_windows_auth: bool = True,
                                  sql_user: str = '',
                                  sql_password: str = '') -> str:
        """Internal helper to build a pyodbc connection string."""
        if sql_user:
            auth = f"UID={sql_user};PWD={sql_password};"
        elif use_windows_auth:
            auth = "Trusted_Connection=yes;"
        else:
            auth = ""
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"{auth}"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout={self.connection_timeout_seconds};"
        )

    def get_atlas_staging_connection_string(self) -> str:
        """Build pyodbc connection string for Atlas Staging database.
        Auto-detects SQL auth vs Windows auth based on atlas_sql_user."""
        return self._build_connection_string(
            self.atlas_staging_server, self.atlas_staging_database,
            use_windows_auth=not self.atlas_sql_user,
            sql_user=self.atlas_sql_user,
            sql_password=self.atlas_sql_password)

    def get_atlas_prod_connection_string(self) -> str:
        """Build pyodbc connection string for Atlas Production database.
        Auto-detects SQL auth vs Windows auth based on atlas_sql_user."""
        return self._build_connection_string(
            self.atlas_prod_server, self.atlas_prod_database,
            use_windows_auth=not self.atlas_sql_user,
            sql_user=self.atlas_sql_user,
            sql_password=self.atlas_sql_password)
    
    def get_epic_clarity_connection_string(self) -> str:
        """Build pyodbc connection string for Epic Clarity database.
        Pipeline B: Used by atlas_clarity_extractor.py, atlas_db_objects_extractor.py,
        and atlas_rundata_extractor.py for direct TCP 1433 connections.

        CN2 (2026-04-07): Auto-detects SQL auth vs Windows auth based on
        epic_clarity_sql_user. When the env var / credential file provides
        a username, UID/PWD replaces Trusted_Connection=yes. The legacy
        epic_clarity_use_windows_auth flag is superseded by the sql_user
        presence check but remains in the dataclass for backward compatibility."""
        return self._build_connection_string(
            self.epic_clarity_server, self.epic_clarity_database,
            use_windows_auth=not self.epic_clarity_sql_user,
            sql_user=self.epic_clarity_sql_user,
            sql_password=self.epic_clarity_sql_password)

    def get_epic_caboodle_connection_string(self) -> str:
        """Build pyodbc connection string for Epic Caboodle/CDW database.
        Pipeline B: Used by atlas_rundata_extractor.py for SlicerDicer stats.

        CN2 (2026-04-07): Auto-detects SQL auth vs Windows auth based on
        epic_caboodle_sql_user. See get_epic_clarity_connection_string for
        the rationale."""
        return self._build_connection_string(
            self.epic_caboodle_server, self.epic_caboodle_database,
            use_windows_auth=not self.epic_caboodle_sql_user,
            sql_user=self.epic_caboodle_sql_user,
            sql_password=self.epic_caboodle_sql_password)

    def get_epic_clarity_connection_string_for_db(self, database: str) -> str:
        """Build connection string for a specific database on the Clarity server.
        Pipeline B: Used by atlas_db_objects_extractor.py to connect to
        Clarity, CogitoTools, EDM, RW_PUB individually.

        CN2 (2026-04-07): Same auto-detect as get_epic_clarity_connection_string
        — uses the Clarity service account for every database on the Clarity host."""
        return self._build_connection_string(
            self.epic_clarity_server, database,
            use_windows_auth=not self.epic_clarity_sql_user,
            sql_user=self.epic_clarity_sql_user,
            sql_password=self.epic_clarity_sql_password)
    
    def get_ssas_connection_string(self) -> str:
        """Build connection string for SSAS (for pyadomd)."""
        return f"Provider={self.ssas_provider};Data Source={self.ssas_server};"
    
    # ══════════════════════════════════════════════════════════════════════════
    # VALIDATION
    # ══════════════════════════════════════════════════════════════════════════
    
    def validate(self) -> list:
        """
        Validate configuration and return list of warnings/errors.
        Returns empty list if configuration is valid.
        """
        issues = []
        
        # Check required values
        if not self.atlas_staging_server:
            issues.append("ERROR: atlas_staging_server is required")
        if not self.atlas_prod_server:
            issues.append("ERROR: atlas_prod_server is required")
        
        # Pipeline B: Verify source server connectivity settings
        if not self.epic_clarity_server:
            issues.append("ERROR: epic_clarity_server is required (Pipeline B)")
        if not self.epic_caboodle_server:
            issues.append("ERROR: epic_caboodle_server is required (Pipeline B)")
        
        # Check optional component dependencies
        if self.enable_power_bi and self.pbi_client_secret == '':
            issues.append("WARNING: Power BI enabled but PBI_CLIENT_SECRET not set")
        
        # Check file paths exist (if running on Windows)
        if os.name == 'nt':
            if not os.path.exists(self.python_executable):
                issues.append(f"WARNING: Python executable not found: {self.python_executable}")
        
        return issues
    
    def __post_init__(self):
        """Post-initialization validation."""
        # Ensure environment is uppercase
        self.environment = self.environment.upper()

        # Validate environment value
        if self.environment not in ('DEV', 'TEST', 'PROD'):
            raise ValueError(f"Invalid environment: {self.environment}. Must be DEV, TEST, or PROD.")

        # Log credential source (never log actual values)
        if self.atlas_sql_user:
            src = 'env' if os.getenv('ATLAS_SQL_USER', '') else 'credential file'
            logger.info(f"Atlas SQL auth: credentials from {src}")
        if self.epic_clarity_sql_user:
            src = 'env' if os.getenv('EPIC_CLARITY_SQL_USER', '') else 'credential file'
            logger.info(f"Epic Clarity SQL auth: credentials from {src}")
        if self.epic_caboodle_sql_user:
            src = 'env' if os.getenv('EPIC_CABOODLE_SQL_USER', '') else 'credential file'
            logger.info(f"Epic Caboodle SQL auth: credentials from {src}")
        if self.pbi_client_secret:
            src = 'env' if os.getenv('PBI_CLIENT_SECRET', '') else 'credential file'
            logger.info(f"Power BI auth: credentials from {src}")


# ══════════════════════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ══════════════════════════════════════════════════════════════════════════════

# Create singleton config instance
config = AtlasConfig()


# ══════════════════════════════════════════════════════════════════════════════
# CLI INTERFACE
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    """Print current configuration when run directly."""
    print("=" * 60)
    print("Atlas ETL Configuration (Pipeline B)")
    print("=" * 60)
    print(f"Environment: {config.environment}")
    print()
    print("Source Systems (Pipeline B — Direct pyodbc):")
    print(f"  Clarity:  {config.epic_clarity_server}/{config.epic_clarity_database}")
    print(f"  Caboodle: {config.epic_caboodle_server}/{config.epic_caboodle_database}")
    print()
    print("Atlas Databases:")
    print(f"  Staging:    {config.atlas_staging_server}/{config.atlas_staging_database}")
    print(f"  Production: {config.atlas_prod_server}/{config.atlas_prod_database}")
    print()
    print("Optional Components:")
    print(f"  SSRS:     {'ENABLED' if config.enable_ssrs else 'DISABLED'}")
    print(f"  SSAS:     {'ENABLED' if config.enable_ssas else 'DISABLED'}")
    print(f"  Tableau:  {'ENABLED' if config.enable_tableau else 'DISABLED'}")
    print(f"  Power BI: {'ENABLED' if config.enable_power_bi else 'DISABLED'}")
    print()
    print("DB Object Sources:", ', '.join(config.database_objects_sources))
    print()
    print("Validation:")
    issues = config.validate()
    if issues:
        for issue in issues:
            print(f"  {issue}")
    else:
        print("  Configuration is valid")
    print("=" * 60)
