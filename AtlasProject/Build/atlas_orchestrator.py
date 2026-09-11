"""
Atlas ETL Suite - Orchestrator (Pipeline B)
============================================
Master pipeline script that orchestrates the complete Atlas ETL workflow.
 
Pipeline B Amendment: All linked server extractions are replaced by direct
Python/pyodbc connections. The orchestrator now calls Python extraction
scripts BEFORE the corresponding stored procedures, which only handle
staging transforms.
 
Execution flow:
    Phase 1: SETUP
        → usp_Atlas_Setup (drop/recreate staging tables)
    Phase 2: EXTRACTION
        → atlas_clarity_extractor.py   (8 Clarity raw extractions)
        → usp_Atlas_Clarity            (staging transforms only)
        → atlas_csv_loader.py          (8 CSV flat-file loads)
        → usp_Atlas_ClarityHierarchy   (hierarchy staging — needs CSV tables)
        → atlas_db_objects_extractor.py (4 database sys.all_objects)
        → usp_Atlas_DatabaseObjects    (staging transforms only)
        → usp_Atlas_LDAP               (uses raw_v2.CLARITY_EMP)
    Phase 3: RUN DATA & LINEAGE
        → atlas_query_hierarchy.py     (SQL parsing for table lineage)
        → atlas_pbi_metadata.py        (Power BI Admin API metadata — 7 raw_v2 tables)
        → usp_Atlas_PowerBI            (stage PBI reports → ReportObjectsStaging)
        → atlas_pbi_user_identity.py   (Graph API user identity enrichment)
        → atlas_pbi_events.py          (Power BI Activity Events API)
        → atlas_rundata_extractor.py   (Clarity + SlicerDicer run data)
    Phase 3a: RUN DATA TRANSFORM        
        → usp_Atlas_RunData            (Phases 1,3-10: index/transform/merge)
    Phase 4: MERGE
        → usp_Atlas_Merge              (20 merge operations)
    Phase 5: POST-PROCESSING
        → usp_Atlas_PostProcessing     (final cleanup)
 
Usage:
    python atlas_orchestrator.py                    # Full pipeline
    python atlas_orchestrator.py --phase setup      # Setup only
    python atlas_orchestrator.py --phase extraction  # Extraction only
    python atlas_orchestrator.py --test             # Test all connections
    python atlas_orchestrator.py --skip-csv         # Skip CSV loader step
    python atlas_orchestrator.py --credentials C:\\path\\to\\creds.json
 
Version: 4.3
Last Updated: March 2026
Changes:
    v2.0 - Week 1/2: Setup, LDAP, PostProcessing, DatabaseObjects
    v3.0 - Week 3: Clarity, CSV loader, DatabaseObjects, QueryHierarchy
    v4.0 - Pipeline B: Wired in atlas_clarity_extractor.py,
           atlas_db_objects_extractor.py, atlas_rundata_extractor.py.
           Eliminated linked server dependency entirely.
    v4.1 - Cohesion fixes:
           - execute_sp() now injects @ExecutionID into every SP call (M1)
           - PostProcessing params fixed: @StagingSchema/@ProdSchema (C2)
    v4.2 - phase_rundata: run all steps to completion instead of short-circuiting
    v4.3 - Phase 2: add usp_Atlas_ClarityHierarchy after csv_loader
            (hierarchy branches H6-H10 require populated CSV tables)
           on first failure. Logs first failing step name. SP executes even if
           PBI events fails (PBI data is optional; SP handles 0 rows gracefully).
 
 
Updated August 2026:
    Changes: Moved RunData to after Merge & Post Processing due to
             performance issues (bona.allen)          
"""
 
import argparse
import logging
import os
import subprocess
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple
 
import pyodbc
 
from atlas_config import config
 
 
# ══════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ══════════════════════════════════════════════════════════════════════════════
 
def setup_logging(log_level: str = 'INFO') -> logging.Logger:
    """Configure logging for the orchestrator."""
    logger = logging.getLogger('atlas_orchestrator')
    logger.setLevel(getattr(logging, log_level.upper()))
 
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
 
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
 
    return logger
 
 
logger = setup_logging(config.log_level)
 
 
# ══════════════════════════════════════════════════════════════════════════════
# DATABASE HELPERS
# ══════════════════════════════════════════════════════════════════════════════
 
def get_connection() -> pyodbc.Connection:
    """Create a connection to Atlas Staging database."""
    conn_str = config.get_atlas_staging_connection_string()
    return pyodbc.connect(conn_str, timeout=config.connection_timeout_seconds)
 
 
def execute_sp(proc_name: str, exec_id: str, params: dict = None,
               timeout_seconds: int = None) -> Tuple[bool, str]:
    """Execute a stored procedure with logging.
 
    v4.1 fix (M1): Automatically injects @ExecutionID into every SP call
    so that all log entries across the pipeline share the same correlation ID.
    Every Pipeline B SP accepts @ExecutionID as an optional UNIQUEIDENTIFIER
    parameter with a NEWID() default, so this is backwards-compatible.
 
    v4.7: Per-SP query-execution timeout. Caller can override the default
    (config.sp_timeout_seconds, 900s) by passing timeout_seconds explicitly —
    RunData uses config.rundata_timeout_seconds (1800s). When the SP exceeds
    the timeout, pyodbc raises an OperationalError; we catch it, log the
    elapsed time, and return (False, msg) so the orchestrator halts the
    pipeline via its normal error path.
    """
    timeout = timeout_seconds if timeout_seconds is not None else config.sp_timeout_seconds
 
    logger.info(f"Executing SP: {proc_name} (timeout: {timeout}s)")
    start = datetime.now()
    try:
        conn = get_connection()
        # SPs manage their own transactions via SET XACT_ABORT ON / TRY-CATCH.
        # autocommit=False (pyodbc default) wraps the EXEC in an implicit
        # transaction, which conflicts: if the SP's XACT_ABORT marks the txn
        # as uncommittable, conn.commit() fails with error 3930.
        conn.autocommit = True
 
        # v4.7: Query-execution timeout. pyodbc distinguishes between the
        # CONNECTION timeout (set via pyodbc.connect(timeout=N), controls how
        # long to wait for a connection to be established) and the QUERY
        # timeout (the `timeout` attribute on an existing Connection, applied
        # to subsequent cursor.execute() calls). Setting conn.timeout = N
        # here is the correct way to bound SP execution time.
        conn.timeout = timeout
 
        cursor = conn.cursor()
        # Set session options to match SSMS defaults (required for MERGE
        # statements inside sp_executesql in etl.usp_Atlas_RunData Phase 10).
        # ODBC Driver 17 defaults ARITHABORT to OFF; SSMS defaults it to ON.
        # Without this, MERGE statements throw misleading "Incorrect syntax"
        # parser errors. See investigation 2026-05-02.
        cursor.execute("SET NOCOUNT ON; SET ARITHABORT ON; SET QUOTED_IDENTIFIER ON")
 
        # v4.1: Build combined params with ExecutionID always included
        all_params = {}
        if exec_id:
            all_params['ExecutionID'] = exec_id
        if params:
            all_params.update(params)
 
        if all_params:
            param_list = ', '.join(f"@{k}=?" for k in all_params.keys())
            sql = f"EXEC {proc_name} {param_list}"
            cursor.execute(sql, *all_params.values())
        else:
            cursor.execute(f"EXEC {proc_name}")
 
        # Read all result sets to avoid "Results pending" errors
        while cursor.nextset():
            pass
 
        cursor.close()
        conn.close()
        elapsed = (datetime.now() - start).total_seconds()
        logger.info(f"  Completed: {proc_name} ({elapsed:.1f}s)")
        return True, None
 
    except pyodbc.OperationalError as e:
        elapsed = (datetime.now() - start).total_seconds()
        msg = str(e)
        # pyodbc query-timeout errors surface as OperationalError with
        # SQLSTATE HYT00 or text containing "Query timeout expired".
        if 'HYT00' in msg or 'timeout' in msg.lower():
            logger.error(
                f"  TIMEOUT: {proc_name} exceeded {timeout}s "
                f"(elapsed {elapsed:.1f}s) — {msg}"
            )
            return False, f"SP timeout after {timeout}s: {msg}"
        logger.error(f"  Failed: {proc_name} (after {elapsed:.1f}s) — {msg}")
        return False, msg
 
    except Exception as e:
        elapsed = (datetime.now() - start).total_seconds()
        msg = str(e)
        logger.error(f"  Failed: {proc_name} (after {elapsed:.1f}s) — {msg}")
        return False, msg
 
# ──────────────────────────────────────────────────────────────────────
# Manual timeout testing (uncomment for verification):
#
# To confirm the timeout mechanism works end-to-end, temporarily override
# the RunData timeout to 5 seconds and re-run the pipeline. The RunData SP
# should be interrupted after ~5s with a "TIMEOUT" log entry, and the
# orchestrator should halt the pipeline via its standard failure path.
#
#     success, _ = execute_sp('etl.usp_Atlas_RunData', exec_id,
#                             {'SkipPBI': skip_pbi},
#                             timeout_seconds=5)  # FORCE TIMEOUT FOR TESTING
# ──────────────────────────────────────────────────────────────────────
 
 
def execute_python_script(script_name: str, args: list = None,
                          description: str = None,
                          exec_id: str = None) -> Tuple[bool, str]:
    """
    Execute a Python script as a subprocess.
 
    Locates the script relative to this orchestrator file so it works
    regardless of the working directory that SQL Server Agent uses.
 
    Returns:
        (success, error_message_or_None)
    """
    label = description or script_name
    logger.info(f"Executing script: {label}")
 
    # Resolve script path relative to this orchestrator
    orchestrator_dir = Path(__file__).resolve().parent
    script_path = orchestrator_dir / script_name
 
    if not script_path.exists():
        msg = f"Script not found: {script_path}"
        logger.error(f"  {msg}")
        return False, msg
 
    # Use the same Python interpreter that's running the orchestrator
    python_exe = sys.executable
    cmd = [python_exe, str(script_path)]
    if args:
        cmd.extend(args)
 
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=config.query_timeout_seconds,
            env={**os.environ,
                 'ATLAS_STAGING_SERVER': config.atlas_staging_server,
                 'ATLAS_STAGING_DB': config.atlas_staging_database,
                 'ATLAS_CSV_SHARE': config.csv_network_share,
                 'ATLAS_QUERY_TIMEOUT': str(config.query_timeout_seconds),
                 # Pipeline B: Pass source server info to subprocesses
                 'EPIC_CLARITY_SERVER': config.epic_clarity_server,
                 'EPIC_CABOODLE_SERVER': config.epic_caboodle_server,
                 'EPIC_CABOODLE_DB': config.epic_caboodle_database,
                 # Power BI API credentials
                 'PBI_TENANT_ID': config.pbi_tenant_id,
                 'PBI_CLIENT_ID': config.pbi_client_id,
                 'PBI_CLIENT_SECRET': config.pbi_client_secret,
                 # Credential file path (if set via --credentials or env)
                 **({'ATLAS_CREDENTIALS': os.environ['ATLAS_CREDENTIALS']}
                    if os.environ.get('ATLAS_CREDENTIALS') else {}),
                 # ExecutionID for cross-pipeline log correlation
                 **({'ATLAS_EXECUTION_ID': exec_id} if exec_id else {})},
        )
 
        # Stream subprocess output to our logger
        if result.stdout:
            for line in result.stdout.strip().splitlines():
                logger.info(f"  [{script_name}] {line}")
        if result.stderr:
            for line in result.stderr.strip().splitlines():
                logger.warning(f"  [{script_name}] {line}")
 
        if result.returncode == 0:
            logger.info(f"  Completed: {label}")
            return True, None
        else:
            msg = f"Exit code {result.returncode}"
            logger.error(f"  Failed: {label} — {msg}")
            return False, msg
 
    except subprocess.TimeoutExpired:
        msg = f"Timed out after {config.query_timeout_seconds}s"
        logger.error(f"  Failed: {label} — {msg}")
        return False, msg
    except Exception as e:
        msg = str(e)
        logger.error(f"  Failed: {label} — {msg}")
        return False, msg
 
 
# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE PHASES
# ══════════════════════════════════════════════════════════════════════════════
 
def phase_setup(exec_id: str) -> bool:
    """Phase 1: Setup - Drop/recreate staging tables."""
    logger.info("=" * 60)
    logger.info("PHASE 1: SETUP")
    logger.info("=" * 60)
 
    success, _ = execute_sp('etl.usp_Atlas_Setup', exec_id, {'UseTruncate': False})
    return success
 
 
def phase_extraction(exec_id: str, skip_csv: bool = False) -> bool:
    """Phase 2: Extraction - Extract from all source systems.
   
    Pipeline B: Python scripts connect directly to Clarity/Caboodle,
    then SPs handle staging transforms from local raw_v2 tables.
    """
    logger.info("=" * 60)
    logger.info("PHASE 2: EXTRACTION (Pipeline B — Direct Python Connections)")
    logger.info("=" * 60)
 
    ok = True
 
    # ── Clarity raw extraction (Python → raw_v2.*) ───────────────────────
    # Pipeline B: Replaces all 8 linked server queries from usp_Atlas_Clarity
    success, _ = execute_python_script(
        'atlas_clarity_extractor.py',
        description='Clarity Extractor — 8 raw extractions via pyodbc',
        exec_id=exec_id,
    )
    ok = ok and success
 
    if not ok:
        logger.error("Clarity extraction failed — downstream steps depend on raw_v2 data")
        return False
 
    # ── Clarity staging transforms (SP — local only) ─────────────────────
    success, _ = execute_sp('etl.usp_Atlas_Clarity', exec_id)
    ok = ok and success
 
    # ── Clarity CSV loads ─────────────────────────────────────────────────
    if skip_csv:
        logger.info("  [SKIP] atlas_csv_loader.py (--skip-csv flag)")
    else:
        success, _ = execute_python_script(
            'atlas_csv_loader.py',
            description='Clarity CSV Loader (8 files)',
            exec_id=exec_id,
        )
        ok = ok and success
 
    # ── Clarity Hierarchy staging (SP — needs CSV tables populated) ──────
    # Extracted from usp_Atlas_Clarity Stage 18: H6-H10 depend on CSV tables
    # (clarity_hrx_column_mapping, clarity_paf, fds_fdm_map, code_template)
    success, _ = execute_sp('etl.usp_Atlas_ClarityHierarchy', exec_id)
    ok = ok and success
 
    # ── LDAP (SP — uses raw_v2.CLARITY_EMP) ──────────────────────────────
    # Pipeline B: No linked server needed; joins against raw_v2 tables
    success, _ = execute_sp('etl.usp_Atlas_LDAP', exec_id)
    ok = ok and success
 
    # ── Database Objects raw extraction (Python → raw_v2.*) ──────────────
    # Pipeline B: Replaces linked server extraction in usp_Atlas_DatabaseObjects
    success, _ = execute_python_script(
        'atlas_db_objects_extractor.py',
        description='DB Objects Extractor — sys.all_objects from 4 databases',
        exec_id=exec_id,
    )
    ok = ok and success
 
    # ── Database Objects staging transforms (SP — local only) ─────────────
    success, _ = execute_sp('etl.usp_Atlas_DatabaseObjects', exec_id)
    ok = ok and success
 
    # ── SSRS (optional) ──────────────────────────────────────────────────
    if config.enable_ssrs:
        logger.info("  [PENDING] usp_Atlas_SSRS (OPTIONAL — not yet migrated)")
    else:
        logger.info("  [SKIP] SSRS disabled")
 
    # ── SSAS (optional) ──────────────────────────────────────────────────
    if config.enable_ssas:
        logger.info("  [PENDING] atlas_ssas_extract.py (OPTIONAL — not yet migrated)")
    else:
        logger.info("  [SKIP] SSAS disabled")
 
    # ── Tableau (optional) ───────────────────────────────────────────────
    if config.enable_tableau:
        logger.info("  [PENDING] usp_Atlas_Tableau (OPTIONAL — not yet migrated)")
    else:
        logger.info("  [SKIP] Tableau disabled")
 
    return ok
 
 
def phase_rundata(exec_id: str) -> bool:
    """Phase 3: RunData & Lineage - Usage data and query hierarchy.
 
    Pipeline B: Python extracts run data directly from Clarity/Caboodle,
    then SP handles indexing, staging, and merge.
 
    All steps within this phase run to completion even if earlier steps fail.
    The SP handles 0 rows gracefully for optional sources (PBI, SlicerDicer).
    Phase-level failure is reported with the first failing step identified.
    """
    logger.info("=" * 60)
    logger.info("PHASE 3: RUN DATA & LINEAGE (Pipeline B)")
    logger.info("=" * 60)
 
    failed_steps = []
 
    # ── Query Hierarchy (Python SQL parsing) ─────────────────────────────
    success, _ = execute_python_script(
        'atlas_query_hierarchy.py',
        description='Query Hierarchy — SQL parsing for table lineage',
        exec_id=exec_id,
    )
    if not success:
        failed_steps.append('atlas_query_hierarchy.py')
 
    # ── PBI Metadata (Python API extraction) ─────────────────────────────
    # MUST run before atlas_pbi_events.py: Phase 4b of usp_Atlas_RunData
    # joins raw_v2.PbiActivityEvent to raw_v2.PbiReport (Phase 3 of the
    # PBI migration). Metadata must be present before that join executes.
    # Both PBI extractors share the enable_power_bi feature flag.
    if config.enable_power_bi:
        success, _ = execute_python_script(
            'atlas_pbi_metadata.py',
            description='Power BI Metadata (5 endpoints, 7 raw_v2.Pbi* tables)',
            exec_id=exec_id,
        )
        if not success:
            failed_steps.append('atlas_pbi_metadata.py')
    else:
        logger.info("  [SKIP] Power BI metadata disabled (enable_power_bi=False)")
 
    # ── PBI ReportObject staging (SP — local only) ───────────────────────
    # PBI migration Phase 4: stages raw_v2.PbiReport rows into
    # stage_v2.ReportObjectsStaging so they land in prd_v2.ReportObjects
    # via Merge 1. Resolves WorkspaceId via raw_v2.PbiWorkspaceReport
    # bridge and produces the CN4 Option B BizKey (byte-identical to
    # Phase 3's BizKey at 13_usp_Atlas_RunData.sql:420-425).
    #
    # Must run AFTER atlas_pbi_metadata.py (raw_v2 tables populated) and
    # BEFORE usp_Atlas_Merge (Merge 1 consumes the staging rows). Gated
    # on the same enable_power_bi flag as the metadata extractor.
    if config.enable_power_bi:
        success, _ = execute_sp('etl.usp_Atlas_PowerBI', exec_id)
        if not success:
            failed_steps.append('etl.usp_Atlas_PowerBI')
    else:
        logger.info("  [SKIP] Power BI staging disabled (enable_power_bi=False)")
 
    # ── PBI User Identity Enrichment (Python Graph API) ──────────────────
    # PBI migration Phase 5: resolves user GUIDs in raw_v2.PbiReport
    # (CreatedBy/ModifiedBy) and raw_v2.PbiActivityEvent (UserId) to
    # UserPrincipalName via Microsoft Graph $batch endpoint (up to 20
    # users per request). Writes resolved UPN/SamAccountName/Domain
    # back to the raw_v2 tables so usp_Atlas_PowerBI (already run above)
    # will see enriched Author/ModifiedBy values on the NEXT E2E run,
    # and Phase 7 (usp_Atlas_RunData) immediately uses the enriched
    # UserUPN for RunUserName on THIS run via ISNULL(p.UserUPN, p.UserId).
    #
    # Runs AFTER usp_Atlas_PowerBI because the Phase 4 SP reads
    # raw_v2.PbiReport.CreatedByUPN/ModifiedByUPN — which are populated
    # by this script. Author/LastModifiedBy on prd_v2.ReportObjects
    # PBI rows lags by one E2E cycle (enriched this run → staged next
    # run → landed in prd_v2 at next Merge 1). Acceptable trade-off.
    #
    # Must run BEFORE atlas_pbi_events.py? No — atlas_pbi_events.py
    # writes raw_v2.PbiActivityEvent, which is the TARGET of this
    # enrichment. But the existing PbiActivityEvent rows (from prior
    # runs) can be enriched before the new events arrive — both
    # orderings work. Placed BEFORE atlas_pbi_events.py per user
    # instruction to keep all PBI-metadata-related steps grouped.
    #
    # Shares enable_power_bi feature flag with the other PBI extractors.
    if config.enable_power_bi:
        success, _ = execute_python_script(
            'atlas_pbi_user_identity.py',
            description='Power BI User Identity Enrichment (Graph API, batched)',
            exec_id=exec_id,
        )
        if not success:
            failed_steps.append('atlas_pbi_user_identity.py')
    else:
        logger.info("  [SKIP] Power BI user identity enrichment disabled (enable_power_bi=False)")
 
    # ── PBI Events (Python API extraction) ───────────────────────────────
    if config.enable_power_bi:
        success, _ = execute_python_script(
            'atlas_pbi_events.py',
            description='Power BI Activity Events (28-day API extraction)',
            exec_id=exec_id,
        )
        if not success:
            failed_steps.append('atlas_pbi_events.py')
    else:
        logger.info("  [SKIP] Power BI disabled")
 
    # ── RunData raw extraction (Python → raw_v2.*) ───────────────────────
    # Pipeline B: Replaces 5 linked server queries from usp_Atlas_RunData Phase 2
    success, _ = execute_python_script(
        'atlas_rundata_extractor.py',
        description='RunData Extractor — Clarity runs + SlicerDicer stats',
        exec_id=exec_id,
    )
    if not success:
        failed_steps.append('atlas_rundata_extractor.py')
 
    # ── RunData transform/merge (SP — Phases 1,3-10, local only) ─────────
    # v4.7: RunData gets the rundata_timeout_seconds override (30 min default)
    # because it is the longest-running SP in Pipeline B (Phases 1-12 with
    # multiple MERGEs against large prd_v2.ReportObjectRunData* tables).
    #skip_pbi = 0 if config.enable_power_bi else 1
    #success, _ = execute_sp('etl.usp_Atlas_RunData', exec_id, {
    #    'SkipPBI':          skip_pbi,
    #    'StagingSchema':    config.stg_schema,
    #    'ProdSchema':       config.prod_schema,
    #    'ProdDatabase':     config.prod_database,
    #}, timeout_seconds=config.rundata_timeout_seconds)
    #if not success:
    #    failed_steps.append('etl.usp_Atlas_RunData')
 
    if failed_steps:
        logger.error(f"Phase 'rundata' failed — first failure was: {failed_steps[0]}")
        if len(failed_steps) > 1:
            logger.error(f"  Additional failures: {', '.join(failed_steps[1:])}")
        return False
 
    return True
 
 
def phase_rundatatransform(exec_id: str) -> bool:
    """Phase 3a: RunData Transform."""
    logger.info("=" * 60)
    logger.info("PHASE 3a: RunDataTransform")
    logger.info("=" * 60)
 
    skip_pbi = 0 if config.enable_power_bi else 1
    success, _ = execute_sp('etl.usp_Atlas_RunData', exec_id, {
        'SkipPBI':          skip_pbi,
        'StagingSchema':    config.stg_schema,
        'ProdSchema':       config.prod_schema,
        'ProdDatabase':     config.prod_database,
    }, timeout_seconds=config.rundata_timeout_seconds)
    return success
 
 
 
def phase_merge(exec_id: str) -> bool:
    """Phase 4: Merge - Staging → Production consolidation."""
    logger.info("=" * 60)
    logger.info("PHASE 4: MERGE")
    logger.info("=" * 60)
 
    success, _ = execute_sp('etl.usp_Atlas_Merge', exec_id, {
        'StagingSchema':    config.stg_schema,
        'ProdSchema':       config.prod_schema,
        'ProdDatabase':     config.prod_database,
        'SkipPrdCreation':  1 if config.environment == 'PROD' else 0,
    }, timeout_seconds=config.merge_timeout_seconds)
    return success
 
 
def phase_postprocessing(exec_id: str) -> bool:
    """Phase 5: Post-Processing - Final cleanup."""
    logger.info("=" * 60)
    logger.info("PHASE 5: POST-PROCESSING")
    logger.info("=" * 60)
 
    # v4.1 fix (C2): Pass schema names, not database names.
    # CUTOVER-SP: schema + database now derived from config.environment.
    success, _ = execute_sp('etl.usp_Atlas_PostProcessing', exec_id, {
        'StagingSchema':    config.stg_schema,
        'ProdSchema':       config.prod_schema,
        'ProdDatabase':     config.prod_database,
    })
    return success
 
 
# ══════════════════════════════════════════════════════════════════════════════
# PHASE REGISTRY
# ══════════════════════════════════════════════════════════════════════════════
 
PHASE_REGISTRY = {
    'setup':             phase_setup,
    'extraction':        phase_extraction,
    'rundata':           phase_rundata,
    'rundata_transform': phase_rundatatransform,
    'merge':             phase_merge,
    'postprocessing':    phase_postprocessing,
   
}
 
ALL_PHASES = list(PHASE_REGISTRY.keys())
 
 
# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
 
def run_pipeline(phases: List[str] = None, skip_csv: bool = False) -> bool:
    """Run the ETL pipeline."""
    exec_id = str(uuid.uuid4())
    start_time = datetime.now()
 
    logger.info("=" * 60)
    logger.info("ATLAS ETL PIPELINE (Pipeline B — No Linked Servers)")
    logger.info(f"Execution ID:  {exec_id}")
    logger.info(f"Environment:   {config.environment}")
    logger.info(f"Start Time:    {start_time}")
    logger.info(f"Clarity:       {config.epic_clarity_server}")
    logger.info(f"Caboodle:      {config.epic_caboodle_server}")
    logger.info(f"Optional:      SSRS={'ON' if config.enable_ssrs else 'OFF'}  "
                f"SSAS={'ON' if config.enable_ssas else 'OFF'}  "
                f"Tableau={'ON' if config.enable_tableau else 'OFF'}")
    logger.info("=" * 60)
 
    phases_to_run = phases if phases else ALL_PHASES
    results = {}
 
    for phase_name in phases_to_run:
        func = PHASE_REGISTRY.get(phase_name)
        if func is None:
            logger.warning(f"Unknown phase '{phase_name}' — skipping")
            continue
 
        # phase_extraction takes the skip_csv kwarg
        if phase_name == 'extraction':
            results[phase_name] = func(exec_id, skip_csv=skip_csv)
        else:
            results[phase_name] = func(exec_id)
 
        # Stop pipeline on failure (fail-fast)
        if not results[phase_name]:
            logger.error(f"Phase '{phase_name}' failed — halting pipeline")
            break
 
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
 
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"Duration: {duration:.1f} seconds")
    for phase_name, passed in results.items():
        status = "OK" if passed else "FAIL"
        logger.info(f"  [{status}] {phase_name}")
    logger.info("=" * 60)
 
    return all(results.values())
 
 
def test_connection() -> bool:
    """Test all database connections (Pipeline B includes Clarity + Caboodle)."""
    logger.info("Testing database connections...")
    all_ok = True
 
    # Atlas Staging
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT @@SERVERNAME, DB_NAME(), SYSTEM_USER")
        row = cursor.fetchone()
        logger.info(f"  Atlas Staging: {row[0]} / {row[1]} (user: {row[2]})")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"  Atlas Staging FAILED: {e}")
        all_ok = False
 
    # Epic Clarity (Pipeline B: direct connection)
    try:
        conn_str = config.get_epic_clarity_connection_string()
        conn = pyodbc.connect(conn_str, timeout=config.connection_timeout_seconds)
        cursor = conn.cursor()
        cursor.execute("SELECT @@SERVERNAME, DB_NAME()")
        row = cursor.fetchone()
        logger.info(f"  Epic Clarity:  {row[0]} / {row[1]}")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"  Epic Clarity FAILED: {e}")
        all_ok = False
 
    # Epic Caboodle (Pipeline B: direct connection)
    try:
        conn_str = config.get_epic_caboodle_connection_string()
        conn = pyodbc.connect(conn_str, timeout=config.connection_timeout_seconds)
        cursor = conn.cursor()
        cursor.execute("SELECT @@SERVERNAME, DB_NAME()")
        row = cursor.fetchone()
        logger.info(f"  Epic Caboodle: {row[0]} / {row[1]}")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"  Epic Caboodle FAILED: {e}")
        all_ok = False
 
    if all_ok:
        logger.info("All connection tests passed!")
    else:
        logger.error("Some connection tests failed.")
    return all_ok
 
 
def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Atlas ETL Orchestrator (Pipeline B — No Linked Servers)')
    parser.add_argument('--phase', type=str, default=None,
                        help=f"Run specific phase: {', '.join(ALL_PHASES)}")
    parser.add_argument('--test', action='store_true',
                        help='Test all database connections')
    parser.add_argument('--skip-csv', action='store_true',
                        help='Skip CSV loader step')
    parser.add_argument('--credentials', type=str, default=None,
                        metavar='PATH',
                        help='Path to credentials.json (overrides default search)')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable debug logging')
    args = parser.parse_args()
 
    # Set ATLAS_CREDENTIALS before config is used (config is already imported
    # at module level, so we must reload if a custom path is provided)
    if args.credentials:
        os.environ['ATLAS_CREDENTIALS'] = args.credentials
        # Reload config so _load_credential_file() picks up the new path
        import importlib
        import atlas_config as _ac_mod
        importlib.reload(_ac_mod)
        global config
        from atlas_config import config
 
    if args.verbose:
        logger.setLevel(logging.DEBUG)
 
    if args.test:
        success = test_connection()
        sys.exit(0 if success else 1)
 
    phases = [args.phase] if args.phase else None
    success = run_pipeline(phases=phases, skip_csv=args.skip_csv)
    sys.exit(0 if success else 1)
 
 
if __name__ == '__main__':
    main()
 
 