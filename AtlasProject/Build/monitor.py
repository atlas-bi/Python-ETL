"""Atlas ETL Pipeline Monitor — polls etl.Atlas_ETL_Log every 3 seconds.

Usage:
    python monitor.py                                # Use env vars or default credentials.json
    python monitor.py --credentials C:\\path\\creds.json  # Custom credentials file
"""
import argparse
import os
import sys
import time

# Parse --credentials early so ATLAS_CREDENTIALS env var is set before atlas_config loads
_parser = argparse.ArgumentParser(description='Atlas ETL Pipeline Monitor')
_parser.add_argument('--credentials', type=str, default=None, metavar='PATH',
                     help='Path to credentials.json (overrides default search)')
_args = _parser.parse_args()
if _args.credentials:
    os.environ['ATLAS_CREDENTIALS'] = _args.credentials

import pyodbc
from atlas_config import config

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={config.atlas_staging_server};"
    f"DATABASE={config.atlas_staging_database};"
    f"UID={config.atlas_sql_user};"
    f"PWD={config.atlas_sql_password};"
    f"TrustServerCertificate=yes;"
)

QUERY = """
DECLARE @RunStart DATETIME = (
    SELECT MAX(StartTime) FROM etl.Atlas_ETL_Log
    WHERE PackageName LIKE '%Setup%'
);
SELECT TOP 40
    FORMAT(StartTime, 'HH:mm:ss') AS [Start],
    ISNULL(FORMAT(EndTime, 'HH:mm:ss'), '...') AS [End],
    LEFT(PackageName, 30) AS [Package],
    LEFT(StepName, 50) AS [Step],
    ISNULL(CAST(RowsAffected AS VARCHAR), '') AS [Rows],
    ISNULL(Status, 'Running') AS [Status],
    CASE WHEN EndTime IS NOT NULL
         THEN CAST(DATEDIFF(SECOND, StartTime, EndTime) AS VARCHAR) + 's'
         ELSE CAST(DATEDIFF(SECOND, StartTime, GETDATE()) AS VARCHAR) + 's...'
    END AS [Duration]
FROM etl.Atlas_ETL_Log
WHERE StartTime >= ISNULL(@RunStart, '1900-01-01')
ORDER BY StartTime DESC;
"""

ELAPSED_QUERY = """
DECLARE @RunStart DATETIME = (
    SELECT MAX(StartTime) FROM etl.Atlas_ETL_Log
    WHERE PackageName LIKE '%Setup%'
);
DECLARE @LastEnd DATETIME = (
    SELECT MAX(EndTime) FROM etl.Atlas_ETL_Log
    WHERE StartTime >= ISNULL(@RunStart, '1900-01-01') AND EndTime IS NOT NULL
);
SELECT
    MIN(StartTime) AS PipelineStart,
    DATEDIFF(SECOND, MIN(StartTime), GETDATE()) AS ElapsedSecondsLive,
    DATEDIFF(SECOND, MIN(StartTime), ISNULL(@LastEnd, GETDATE())) AS ElapsedSecondsFrozen,
    SUM(CASE WHEN Status = 'Success' OR Status = 'Warning' THEN 1 ELSE 0 END) AS Completed,
    SUM(CASE WHEN (Status IS NULL OR Status = 'Running')
              AND DATEDIFF(MINUTE, StartTime, GETDATE()) < 10 THEN 1 ELSE 0 END) AS Running,
    SUM(CASE WHEN (Status IS NULL OR Status = 'Running')
              AND DATEDIFF(MINUTE, StartTime, GETDATE()) >= 10 THEN 1 ELSE 0 END) AS Stale,
    SUM(CASE WHEN Status LIKE 'Fail%' OR Status = 'Error' THEN 1 ELSE 0 END) AS Failed,
    COUNT(*) AS TotalSteps,
    DATEDIFF(SECOND, ISNULL(@LastEnd, MIN(StartTime)), GETDATE()) AS IdleSeconds
FROM etl.Atlas_ETL_Log
WHERE StartTime >= ISNULL(@RunStart, '1900-01-01');
"""

PACKAGES_QUERY = """
    DECLARE @CurrentExecID UNIQUEIDENTIFIER = (
        SELECT TOP 1 ExecutionID
        FROM etl.Atlas_ETL_Log
        ORDER BY StartTime DESC
    );
    SELECT DISTINCT PackageName
    FROM etl.Atlas_ETL_Log
    WHERE ExecutionID = @CurrentExecID;
"""

# Known pipeline step order for "next step" prediction.
# PackageNames verified against etl.Atlas_ETL_Log (SELECT DISTINCT PackageName).
# Phases match atlas_orchestrator.py v4.1 execution order.
# Each entry: (set of exact PackageName values, display label)
PIPELINE_STEPS = [
    # Phase 1 — Setup
    ({"ETL-Setup"},                                         "Setup"),
    # Phase 2 — Extraction
    ({"ETL-Clarity-Extract"},                               "Clarity Extraction (61 extractions)"),
    ({"ETL-Clarity"},                                       "Clarity Staging (20 steps)"),
    ({"ETL-Clarity-CSV"},                                   "CSV Loader (9 files)"),
    ({"ETL-Clarity-Hierarchy"},                             "Clarity Hierarchy Staging (15 branches)"),
    ({"ETL-LDAP"},                                          "LDAP User/Group Mapping"),
    ({"ETL-DatabaseObjects-Extract"},                       "DB Objects Extraction"),
    ({"ETL-DatabaseObjects", "usp_Atlas_DatabaseObjects"},  "DB Objects Staging"),
    # Phase 3 — RunData & Lineage
    ({"ETL-QueryHierarchy"},                                "Query Hierarchy Parsing"),
    ({"atlas_pbi_metadata"},                                "PBI Metadata (5 endpoints, 7 tables)"),
    ({"ETL-PowerBI"},                                       "PBI Report Object Staging"),
    ({"atlas_pbi_user_identity"},                           "PBI User Identity Enrichment (Graph API)"),
    ({"atlas_pbi_events"},                                  "PBI Activity Events (28-day)"),
    ({"ETL-RunData-Extract"},                               "RunData Extraction"),
    ({"ETL-RunData"},                                       "RunData Staging + Bridge (12 phases)"),
    # Phase 4 — Merge
    ({"ETL-Merge"},                                         "Production Merge (20 operations)"),
    # Phase 5 — Post-Processing
    ({"ETL-PostProcessing"},                                "Post-Processing"),
]

def fmt_elapsed(seconds):
    if seconds is None:
        return "N/A"
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    elif m > 0:
        return f"{m}m {s}s"
    return f"{s}s"

def find_next_step(seen_packages):
    """Guess the next step based on completed packages (exact match)."""
    for pkg_names, label in PIPELINE_STEPS:
        if not pkg_names & seen_packages:  # no intersection = step hasn't run
            return label
    return "Pipeline complete (or unknown)"

print("Atlas ETL Monitor — Ctrl+C to stop\n")
try:
    while True:
        try:
            conn = pyodbc.connect(conn_str, timeout=5)
            cursor = conn.cursor()

            # Get step log
            cursor.execute(QUERY)
            rows = cursor.fetchall()

            # Get elapsed summary
            cursor.execute(ELAPSED_QUERY)
            summary = cursor.fetchone()

            # Get all packages in current run (for prediction)
            cursor.execute(PACKAGES_QUERY)
            seen_packages = {r[0].strip() for r in cursor.fetchall() if r[0]}
            conn.close()

            os.system('cls')
            print(f"Atlas ETL Monitor — {time.strftime('%H:%M:%S')}  (Ctrl+C to stop)\n")

            # Summary bar
            if summary and summary[0]:
                completed = summary[3] or 0
                running = summary[4] or 0
                stale = summary[5] or 0
                failed = summary[6] or 0
                total = summary[7] or 0
                idle_seconds = summary[8] or 0

                # Determine pipeline state
                if running > 0:
                    status_icon = ">>> RUNNING"
                    elapsed = fmt_elapsed(summary[1])  # live clock
                elif idle_seconds < 120:
                    status_icon = "... WORKING"
                    elapsed = fmt_elapsed(summary[1])  # live clock
                elif failed > 0:
                    status_icon = "!!! PIPELINE FAILED"
                    elapsed = fmt_elapsed(summary[2])  # frozen at last activity
                else:
                    status_icon = "=== PIPELINE COMPLETE"
                    elapsed = fmt_elapsed(summary[2])  # frozen at last activity

                step_summary = f"Steps: {completed} done, {running} running, {failed} failed"
                if stale > 0:
                    step_summary += f", {stale} stale"
                step_summary += f"  ({total} total)"
                print(f"  {status_icon}  |  Elapsed: {elapsed}  |  {step_summary}")

                # Show idle time when not actively running (helps gauge if SP is mid-execution)
                if running == 0 and idle_seconds < 120:
                    print(f"  No log activity for {idle_seconds}s (SP may be executing — logs appear on commit)")

                # Active step + next step prediction (skip stale rows)
                if running > 0:
                    for r in rows:
                        r_status = (r[5] or '').strip()
                        r_dur = int(''.join(c for c in (r[6] or '') if c.isdigit()) or '0')
                        if r_status in ('', 'Running') and r_dur < 600:
                            print(f"  Active: {r[3].strip()}  ({r[6].strip()})")
                            break
                next_step = find_next_step(seen_packages)
                if next_step != "Pipeline complete (or unknown)":
                    print(f"  Next up: {next_step}")
                elif running == 0 and idle_seconds >= 120 and failed == 0:
                    print(f"  All phases complete. Total: {elapsed}")
                elif running == 0 and idle_seconds >= 120 and failed > 0:
                    print(f"  Pipeline halted. Check failed steps above.")
            else:
                print("  No pipeline activity detected.")

            print()
            print(f"{'':3} {'Start':<10} {'End':<10} {'Package':<32} {'Step':<52} {'Rows':<10} {'Status':<10} {'Dur':<8}")
            print("-" * 135)
            for r in rows:
                status = (r[5] or 'Running').strip()
                dur_str = (r[6] or '').strip()
                # Parse seconds from duration string (e.g., "4019s..." or "5s")
                dur_num = int(''.join(c for c in dur_str if c.isdigit()) or '0')
                if (status == 'Running' or status == '') and dur_num >= 600:
                    marker = '~~~'  # stale (>10 min with no EndTime)
                    status = 'Stale'
                elif status == 'Running' or status == '':
                    marker = '>>>'
                elif 'Fail' in status or status == 'Error':
                    marker = '!!!'
                else:
                    marker = '   '
                print(f"{marker}{r[0]:<7} {r[1]:<10} {r[2]:<32} {r[3]:<52} {r[4]:<10} {status:<10} {r[6]:<8}")
        except Exception as e:
            print(f"Connection error: {e}")

        time.sleep(3)
except KeyboardInterrupt:
    print("\nMonitor stopped.")
