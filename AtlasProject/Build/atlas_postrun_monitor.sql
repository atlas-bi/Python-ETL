/*******************************************************************************
 * Atlas ETL Suite — Post-Run Health Monitor
 *
 * Purpose:
 *   Self-contained, read-only diagnostic script. Run after each pipeline
 *   execution to verify health, performance, and data freshness.
 *
 * Usage:
 *   sqlcmd -S "10.247.4.56\SQL126" -d Atlas_Staging -i atlas_postrun_monitor.sql
 *   Or execute directly in SSMS against Atlas_Staging.
 *
 * Output (one result set per check):
 *   CHECK 1 — Most recent run summary (phases, overall status, runtime)
 *   CHECK 2 — Errors from the most recent run
 *   CHECK 3 — Step durations vs. 7-day baseline (flags slow steps)
 *   CHECK 4 — Data freshness (Atlas_Prd RunData row counts, depth)
 *   CHECK 5 — Row count deltas vs. prior run (ReportObjectRunData batches)
 *   CHECK 6 — 14-day pipeline trend (one row per run)
 *
 * Dependencies:
 *   - etl.Atlas_ETL_Log (Atlas_Staging)
 *   - Atlas_Prd.dbo.ReportObjectRunData      (same server, four-part name)
 *   - Atlas_Prd.dbo.ReportObjectRunDataBridge (same server, four-part name)
 *
 * Author:  Larry Duren
 * Date:    May 2026
 * Version: 1.0
 *******************************************************************************/

USE Atlas_Staging;
GO

SET NOCOUNT ON;

-- ---------------------------------------------------------------------------
-- Identify the two most recent pipeline runs.
-- A "run" is identified by the ExecutionID whose Setup step started most
-- recently. @PriorExecID handles the 1-run case gracefully (stays NULL).
-- ---------------------------------------------------------------------------
DECLARE @LatestExecID   UNIQUEIDENTIFIER;
DECLARE @PriorExecID    UNIQUEIDENTIFIER;
DECLARE @RunStart       DATETIME;
DECLARE @RunEnd         DATETIME;

SELECT TOP 1
    @LatestExecID = ExecutionID,
    @RunStart     = StartTime
FROM etl.Atlas_ETL_Log
WHERE PackageName LIKE '%Setup%'
ORDER BY StartTime DESC;

SELECT @RunEnd = MAX(EndTime)
FROM etl.Atlas_ETL_Log
WHERE ExecutionID = @LatestExecID;

SELECT TOP 1
    @PriorExecID = ExecutionID
FROM etl.Atlas_ETL_Log
WHERE PackageName LIKE '%Setup%'
  AND ExecutionID <> @LatestExecID
ORDER BY StartTime DESC;

-- ---------------------------------------------------------------------------
-- CHECK 1 — Most recent run summary
-- Shows phase-level pass/fail and overall pipeline status.
-- ---------------------------------------------------------------------------
PRINT '';
PRINT '===========================================================';
PRINT ' CHECK 1 — Most Recent Run Summary';
PRINT '===========================================================';

SELECT
    @LatestExecID                                                   AS ExecutionID,
    CONVERT(VARCHAR(19), @RunStart, 120)                            AS RunStart,
    CASE
        WHEN @RunEnd IS NOT NULL THEN CONVERT(VARCHAR(19), @RunEnd, 120)
        ELSE '(still running)'
    END                                                             AS RunEnd,
    CASE
        WHEN @RunEnd IS NOT NULL
        THEN CAST(DATEDIFF(SECOND, @RunStart, @RunEnd) / 60 AS VARCHAR) + 'm '
             + CAST(DATEDIFF(SECOND, @RunStart, @RunEnd) % 60 AS VARCHAR) + 's'
        ELSE CAST(DATEDIFF(SECOND, @RunStart, GETDATE()) / 60 AS VARCHAR) + 'm '
             + CAST(DATEDIFF(SECOND, @RunStart, GETDATE()) % 60 AS VARCHAR) + 's (live)'
    END                                                             AS TotalRuntime,
    SUM(CASE WHEN Status = 'Success' OR Status = 'Warning' THEN 1 ELSE 0 END) AS StepsSucceeded,
    SUM(CASE WHEN Status LIKE 'Fail%' OR Status = 'Error'  THEN 1 ELSE 0 END) AS StepsFailed,
    SUM(CASE WHEN Status IS NULL OR Status = 'Running'     THEN 1 ELSE 0 END) AS StepsStillRunning,
    COUNT(*)                                                        AS TotalSteps,
    CASE
        WHEN SUM(CASE WHEN Status LIKE 'Fail%' OR Status = 'Error' THEN 1 ELSE 0 END) > 0
             THEN 'FAILED'
        WHEN SUM(CASE WHEN Status IS NULL OR Status = 'Running' THEN 1 ELSE 0 END) > 0
             THEN 'IN PROGRESS'
        WHEN SUM(CASE WHEN Status = 'Warning' THEN 1 ELSE 0 END) > 0
             THEN 'COMPLETE WITH WARNINGS'
        ELSE 'COMPLETE'
    END                                                             AS PipelineStatus
FROM etl.Atlas_ETL_Log
WHERE ExecutionID = @LatestExecID;

-- Phase-level breakdown
SELECT
    PackageName                                                     AS Phase,
    CONVERT(VARCHAR(8), MIN(StartTime), 108)                        AS PhaseStart,
    CASE
        WHEN MAX(EndTime) IS NOT NULL
        THEN CONVERT(VARCHAR(8), MAX(EndTime), 108)
        ELSE '(running)'
    END                                                             AS PhaseEnd,
    CASE
        WHEN MAX(EndTime) IS NOT NULL
        THEN CAST(DATEDIFF(SECOND, MIN(StartTime), MAX(EndTime)) / 60 AS VARCHAR) + 'm '
             + CAST(DATEDIFF(SECOND, MIN(StartTime), MAX(EndTime)) % 60 AS VARCHAR) + 's'
        ELSE CAST(DATEDIFF(SECOND, MIN(StartTime), GETDATE()) / 60 AS VARCHAR) + 'm (live)'
    END                                                             AS PhaseDuration,
    COUNT(*)                                                        AS Steps,
    SUM(CASE WHEN Status LIKE 'Fail%' OR Status = 'Error' THEN 1 ELSE 0 END) AS Failures,
    CASE
        WHEN SUM(CASE WHEN Status LIKE 'Fail%' OR Status = 'Error' THEN 1 ELSE 0 END) > 0
             THEN 'FAIL'
        WHEN SUM(CASE WHEN Status IS NULL OR Status = 'Running' THEN 1 ELSE 0 END) > 0
             THEN 'RUNNING'
        WHEN SUM(CASE WHEN Status = 'Warning' THEN 1 ELSE 0 END) > 0
             THEN 'WARN'
        ELSE 'OK'
    END                                                             AS PhaseStatus
FROM etl.Atlas_ETL_Log
WHERE ExecutionID = @LatestExecID
GROUP BY PackageName
ORDER BY MIN(StartTime);

-- ---------------------------------------------------------------------------
-- CHECK 2 — Errors from the most recent run
-- Empty result set = no errors.
-- ---------------------------------------------------------------------------
PRINT '';
PRINT '===========================================================';
PRINT ' CHECK 2 — Errors From Most Recent Run';
PRINT '===========================================================';

SELECT
    CONVERT(VARCHAR(8), StartTime, 108)     AS [Time],
    PackageName                             AS Package,
    LEFT(StepName, 60)                      AS Step,
    Status,
    LEFT(ISNULL(ErrorMessage, ''), 200)     AS ErrorMessage,
    ErrorProcedure,
    ErrorLine
FROM etl.Atlas_ETL_Log
WHERE ExecutionID = @LatestExecID
  AND (Status LIKE 'Fail%' OR Status = 'Error')
ORDER BY StartTime;

-- ---------------------------------------------------------------------------
-- CHECK 3 — Step performance vs. 7-day baseline
-- Compares each step's current duration to its median and max over the
-- prior 7 days (excluding the current run). Slow_Flag fires when the
-- current step took more than 2x the historical median.
-- Historical stats use PERCENTILE_CONT to compute the median per step.
-- ---------------------------------------------------------------------------
PRINT '';
PRINT '===========================================================';
PRINT ' CHECK 3 — Step Duration vs. 7-Day Baseline';
PRINT '===========================================================';

WITH HistoricalRaw AS (
    SELECT
        PackageName,
        StepName,
        DurationSeconds
    FROM etl.Atlas_ETL_Log
    WHERE ExecutionID <> @LatestExecID
      AND StartTime >= DATEADD(DAY, -7, @RunStart)
      AND EndTime IS NOT NULL
      AND DurationSeconds IS NOT NULL
),
HistoricalMax AS (
    SELECT
        PackageName,
        StepName,
        MAX(DurationSeconds) AS Max7d
    FROM HistoricalRaw
    GROUP BY PackageName, StepName
),
HistoricalMedian AS (
    SELECT DISTINCT
        PackageName,
        StepName,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DurationSeconds)
            OVER (PARTITION BY PackageName, StepName)                   AS Median7d
    FROM HistoricalRaw
),
HistoricalStats AS (
    SELECT m.PackageName, m.StepName, m.Median7d, x.Max7d
    FROM HistoricalMedian m
    JOIN HistoricalMax x
        ON m.PackageName = x.PackageName
       AND m.StepName    = x.StepName
),
CurrentRun AS (
    SELECT
        PackageName,
        StepName,
        DurationSeconds                                                  AS CurrentSec
    FROM etl.Atlas_ETL_Log
    WHERE ExecutionID = @LatestExecID
      AND EndTime IS NOT NULL
      AND DurationSeconds IS NOT NULL
)
SELECT
    c.PackageName                           AS Package,
    LEFT(c.StepName, 55)                    AS Step,
    c.CurrentSec                            AS CurrentSec,
    CAST(h.Median7d AS INT)                 AS Median7d,
    h.Max7d                                 AS Max7d,
    CASE
        WHEN h.Median7d IS NULL OR h.Median7d = 0 THEN NULL
        ELSE CAST(
            ROUND(100.0 * (c.CurrentSec - h.Median7d) / h.Median7d, 1)
        AS DECIMAL(9,1))
    END                                     AS PctChangeVsMedian,
    CASE
        WHEN h.Median7d IS NULL THEN 'NEW'
        WHEN h.Median7d = 0    THEN ''
        WHEN c.CurrentSec > 2 * h.Median7d THEN 'SLOW'
        ELSE ''
    END                                     AS Slow_Flag
FROM CurrentRun c
LEFT JOIN HistoricalStats h
    ON c.PackageName = h.PackageName
   AND c.StepName    = h.StepName
ORDER BY
    CASE WHEN h.Median7d > 0 AND c.CurrentSec > 2 * h.Median7d THEN 0 ELSE 1 END,
    c.CurrentSec DESC;

-- ---------------------------------------------------------------------------
-- CHECK 4 — Data freshness
-- Reads from Atlas_Prd (same server) via four-part name.
-- Shows row counts, date range, and depth of RunData in production.
-- ---------------------------------------------------------------------------
PRINT '';
PRINT '===========================================================';
PRINT ' CHECK 4 — Data Freshness (Atlas_Prd RunData Tables)';
PRINT '===========================================================';

SELECT
    'ReportObjectRunData'                                           AS TableName,
    COUNT(*)                                                        AS [RowCount],
    CONVERT(VARCHAR(10), MIN(RunStartTime), 120)                    AS OldestRun,
    CONVERT(VARCHAR(10), MAX(RunStartTime), 120)                    AS NewestRun,
    DATEDIFF(DAY, MIN(RunStartTime), MAX(RunStartTime))             AS DepthDays,
    CONVERT(VARCHAR(19), MAX(LastLoadDate), 120)                    AS LastLoadDate
FROM Atlas_Prd.dbo.ReportObjectRunData

UNION ALL

SELECT
    'ReportObjectRunDataBridge'                                     AS TableName,
    COUNT(*)                                                        AS [RowCount],
    NULL                                                            AS OldestRun,
    NULL                                                            AS NewestRun,
    NULL                                                            AS DepthDays,
    NULL                                                            AS LastLoadDate
FROM Atlas_Prd.dbo.ReportObjectRunDataBridge;

-- ---------------------------------------------------------------------------
-- CHECK 5 — Row count deltas vs. prior run
-- Compares the most recent LastLoadDate batch in ReportObjectRunData
-- against the prior batch. If only one run exists, PriorBatchRows = 0.
-- Bridge table delta comes from ETL log RowsAffected for Phase 12 steps.
-- ---------------------------------------------------------------------------
PRINT '';
PRINT '===========================================================';
PRINT ' CHECK 5 — Row Count Deltas vs. Prior Run';
PRINT '===========================================================';

DECLARE @LatestLoadDate DATE;
DECLARE @PriorLoadDate  DATE;

SELECT @LatestLoadDate = CAST(MAX(LastLoadDate) AS DATE)
FROM Atlas_Prd.dbo.ReportObjectRunData;

SELECT @PriorLoadDate = CAST(MAX(LastLoadDate) AS DATE)
FROM Atlas_Prd.dbo.ReportObjectRunData
WHERE CAST(LastLoadDate AS DATE) < @LatestLoadDate;

SELECT
    'ReportObjectRunData'                                           AS TableName,
    CONVERT(VARCHAR(10), @LatestLoadDate, 120)                      AS LatestBatchDate,
    SUM(CASE WHEN CAST(LastLoadDate AS DATE) = @LatestLoadDate
             THEN 1 ELSE 0 END)                                     AS LatestBatchRows,
    CONVERT(VARCHAR(10), @PriorLoadDate, 120)                       AS PriorBatchDate,
    SUM(CASE WHEN @PriorLoadDate IS NOT NULL
              AND CAST(LastLoadDate AS DATE) = @PriorLoadDate
             THEN 1 ELSE 0 END)                                     AS PriorBatchRows,
    SUM(CASE WHEN CAST(LastLoadDate AS DATE) = @LatestLoadDate
             THEN 1 ELSE 0 END)
    - SUM(CASE WHEN @PriorLoadDate IS NOT NULL
               AND CAST(LastLoadDate AS DATE) = @PriorLoadDate
               THEN 1 ELSE 0 END)                                   AS RowDelta
FROM Atlas_Prd.dbo.ReportObjectRunData

UNION ALL

-- Bridge delta from ETL log (no LastLoadDate column on bridge table)
SELECT
    'ReportObjectRunDataBridge'                                     AS TableName,
    CONVERT(VARCHAR(10), @LatestLoadDate, 120)                      AS LatestBatchDate,
    ISNULL(SUM(CASE WHEN ExecutionID = @LatestExecID
                    THEN RowsAffected ELSE 0 END), 0)               AS LatestBatchRows,
    CONVERT(VARCHAR(10), @PriorLoadDate, 120)                       AS PriorBatchDate,
    ISNULL(SUM(CASE WHEN ExecutionID = @PriorExecID
                    THEN RowsAffected ELSE 0 END), 0)               AS PriorBatchRows,
    ISNULL(SUM(CASE WHEN ExecutionID = @LatestExecID
                    THEN RowsAffected ELSE 0 END), 0)
    - ISNULL(SUM(CASE WHEN ExecutionID = @PriorExecID
                      THEN RowsAffected ELSE 0 END), 0)             AS RowDelta
FROM etl.Atlas_ETL_Log
WHERE StepName LIKE '%Phase 12%'
  AND (ExecutionID = @LatestExecID OR ExecutionID = @PriorExecID);

-- ---------------------------------------------------------------------------
-- CHECK 6 — 14-day pipeline trend
-- One row per pipeline run. Helps identify recurring failures or
-- runtime drift. Runs with no EndTime show as still-in-progress.
-- ---------------------------------------------------------------------------
PRINT '';
PRINT '===========================================================';
PRINT ' CHECK 6 — 14-Day Pipeline Trend';
PRINT '===========================================================';

WITH AllRuns AS (
    SELECT
        ExecutionID,
        CAST(MIN(StartTime) AS DATE)                                AS RunDate,
        MIN(StartTime)                                              AS RunStart,
        MAX(EndTime)                                                AS RunEnd,
        SUM(CASE WHEN Status LIKE 'Fail%' OR Status = 'Error'
                 THEN 1 ELSE 0 END)                                 AS FailedSteps,
        SUM(CASE WHEN Status = 'Warning' THEN 1 ELSE 0 END)         AS WarnSteps,
        COUNT(*)                                                    AS TotalSteps
    FROM etl.Atlas_ETL_Log
    WHERE StartTime >= DATEADD(DAY, -14, GETDATE())
    GROUP BY ExecutionID
)
SELECT
    CONVERT(VARCHAR(10), RunDate, 120)                              AS RunDate,
    CONVERT(VARCHAR(8),  RunStart, 108)                             AS StartTime,
    CASE
        WHEN RunEnd IS NOT NULL
        THEN CAST(DATEDIFF(SECOND, RunStart, RunEnd) / 60 AS VARCHAR) + 'm '
             + CAST(DATEDIFF(SECOND, RunStart, RunEnd) % 60 AS VARCHAR) + 's'
        ELSE '(in progress)'
    END                                                             AS Runtime,
    TotalSteps,
    FailedSteps,
    WarnSteps,
    CASE
        WHEN FailedSteps > 0  THEN 'FAILED'
        WHEN RunEnd IS NULL   THEN 'IN PROGRESS'
        WHEN WarnSteps  > 0   THEN 'WARN'
        ELSE 'OK'
    END                                                             AS Result,
    LEFT(CAST(ExecutionID AS VARCHAR(36)), 36)                      AS ExecutionID
FROM AllRuns
ORDER BY RunStart DESC;

PRINT '';
PRINT '===========================================================';
PRINT ' Post-Run Monitor Complete';
PRINT '===========================================================';

SET NOCOUNT OFF;
GO
