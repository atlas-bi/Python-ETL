"""
Atlas ETL Suite - Query Hierarchy / Table References (V2 Schema)
================================================================
Migrated from: ETL-QueryHierarchy SSIS Package
Purpose: Parse SQL object definitions to extract table references for lineage

*** UPDATED: Uses stage_v2 and raw_v2 schemas ***

Usage:
    python atlas_query_hierarchy.py                    # Full run
    python atlas_query_hierarchy.py --test             # Test connection only
    python atlas_query_hierarchy.py --limit 100        # Process first 100 objects

Version: 2.0.1
Last Updated: February 2026
"""

import argparse
import logging
import re
import sys
import uuid
import os
from datetime import datetime
from typing import List, Set, Tuple, Optional

import pyodbc

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
logger = logging.getLogger('atlas_query_hierarchy')


# ══════════════════════════════════════════════════════════════════════════════
# SQL PARSING
# ══════════════════════════════════════════════════════════════════════════════

TABLE_PATTERNS = [
    r'(?:FROM|JOIN)\s+(\[?\w+\]?\.?\[?\w+\]?\.?\[?\w+\]?)',
    r'INTO\s+(\[?\w+\]?\.?\[?\w+\]?\.?\[?\w+\]?)',
    r'UPDATE\s+(\[?\w+\]?\.?\[?\w+\]?\.?\[?\w+\]?)',
    r'DELETE\s+(?:FROM\s+)?(\[?\w+\]?\.?\[?\w+\]?\.?\[?\w+\]?)',
    r'MERGE\s+(?:INTO\s+)?(\[?\w+\]?\.?\[?\w+\]?\.?\[?\w+\]?)',
    r'TRUNCATE\s+TABLE\s+(\[?\w+\]?\.?\[?\w+\]?\.?\[?\w+\]?)',
]

EXCLUDED_KEYWORDS = {
    'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'JOIN', 'WHERE',
    'AND', 'OR', 'ON', 'AS', 'SET', 'VALUES', 'INTO', 'TABLE',
    'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS', 'FULL',
    'NULL', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE',
    'ORDER', 'BY', 'GROUP', 'HAVING', 'UNION', 'ALL',
    'TOP', 'DISTINCT', 'WITH', 'NOLOCK', 'ROWLOCK',
    'BEGIN', 'END', 'IF', 'ELSE', 'WHILE', 'CASE', 'WHEN', 'THEN',
    'DECLARE', 'EXEC', 'EXECUTE', 'RETURN', 'GOTO',
    'CREATE', 'ALTER', 'DROP', 'INDEX', 'VIEW', 'PROCEDURE', 'FUNCTION',
    'TRIGGER', 'SCHEMA', 'DATABASE',
}

EXCLUDED_SCHEMAS = {'sys', 'INFORMATION_SCHEMA', 'msdb', 'master', 'tempdb'}


def remove_comments(sql: str) -> str:
    sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    return sql


def remove_string_literals(sql: str) -> str:
    sql = re.sub(r"'[^']*'", "''", sql)
    sql = re.sub(r'"[^"]*"', '""', sql)
    return sql


def normalize_table_name(table_ref: str) -> Tuple[Optional[str], Optional[str], str]:
    table_ref = table_ref.replace('[', '').replace(']', '')
    parts = table_ref.split('.')
    
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        return None, parts[0], parts[1]
    elif len(parts) == 1:
        return None, None, parts[0]
    else:
        return None, None, table_ref


def extract_table_references(sql: str) -> Set[Tuple[Optional[str], Optional[str], str]]:
    if not sql:
        return set()
    
    sql = remove_comments(sql)
    sql = remove_string_literals(sql)
    sql = sql.upper()
    
    tables = set()
    
    for pattern in TABLE_PATTERNS:
        matches = re.findall(pattern, sql, re.IGNORECASE)
        for match in matches:
            if match:
                db, schema, table = normalize_table_name(match)
                
                if table.upper() in EXCLUDED_KEYWORDS:
                    continue
                if schema and schema.upper() in EXCLUDED_SCHEMAS:
                    continue
                if table.startswith('#') or table.startswith('@'):
                    continue
                
                tables.add((db, schema, table))
    
    return tables


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE OPERATIONS (V2 SCHEMA)
# ══════════════════════════════════════════════════════════════════════════════

def get_connection() -> pyodbc.Connection:
    conn_str = config.get_atlas_staging_connection_string()
    return pyodbc.connect(conn_str, timeout=config.connection_timeout_seconds)


def log_start(conn: pyodbc.Connection, exec_id: str, package: str, step: str, seq: int) -> int:
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
    """, exec_id, package, step, seq)
    
    log_id = cursor.fetchone()[0]
    conn.commit()
    return log_id


def log_end(conn: pyodbc.Connection, log_id: int, rows: int, status: str = 'Success'):
    cursor = conn.cursor()
    cursor.execute("""
        EXEC etl.usp_Atlas_LogEnd 
            @LogID = ?,
            @RowsAffected = ?,
            @Status = ?
    """, log_id, rows, status)
    conn.commit()


def log_error(conn: pyodbc.Connection, log_id: int, error_msg: str):
    cursor = conn.cursor()
    cursor.execute("""
        EXEC etl.usp_Atlas_LogErrorManual 
            @LogID = ?,
            @ErrorMessage = ?
    """, log_id, error_msg[:4000])
    conn.commit()


def get_database_objects(conn: pyodbc.Connection, limit: Optional[int] = None) -> List[dict]:
    """Fetch database objects from raw_v2.DatabaseObjects."""
    cursor = conn.cursor()
    
    sql = """
        SELECT
            SourceServer + '||' + SourceDB + '||' + ISNULL(SourceSchema, 'dbo') + '||' + Name AS BizKey,
            Name AS ObjectName,
            ReportObjectType AS ObjectType,
            SourceServer AS ServerName,
            SourceDB AS DatabaseName,
            SourceSchema AS SchemaName
        FROM raw_v2.DatabaseObjects
        WHERE ReportObjectType IN ('SQL Stored Procedure', 'SQL View', 'SQL Function',
                                   'SQL Table-Valued Function', 'SQL Inline Table-Valued Function')
    """
    
    if limit:
        sql = sql.replace('SELECT', f'SELECT TOP {limit}')
    
    cursor.execute(sql)
    
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_object_definition(conn: pyodbc.Connection, server: str, database: str, 
                          schema: str, name: str) -> Optional[str]:
    """Fetch SQL definition from raw_v2.DatabaseObjects."""
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT Query
        FROM raw_v2.DatabaseObjects
        WHERE SourceServer = ?
          AND SourceDB = ?
          AND ISNULL(SourceSchema, 'dbo') = ?
          AND Name = ?
    """, server, database, schema or 'dbo', name)
    
    row = cursor.fetchone()
    return row[0] if row else None


def save_table_references(conn: pyodbc.Connection, references: List[dict]):
    """Save to stage_v2.TableReferenceStaging."""
    if not references:
        return
    
    cursor = conn.cursor()
    
    # Truncate existing data
    cursor.execute("TRUNCATE TABLE stage_v2.TableReferenceStaging")
    
    # Insert new references
    cursor.executemany("""
        INSERT INTO stage_v2.TableReferenceStaging (
            BizKey, ReferencedTable, ReferencedSchema, 
            ReferencedDatabase, ReferenceType, ExtractDate
        ) VALUES (?, ?, ?, ?, ?, GETDATE())
    """, [(r['BizKey'], r['Table'], r['Schema'], r['Database'], r['Type']) 
          for r in references])
    
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def process_query_hierarchy(limit: Optional[int] = None) -> bool:
    exec_id = os.getenv('ATLAS_EXECUTION_ID', str(uuid.uuid4()))
    package_name = 'ETL-QueryHierarchy'
    step_seq = 0
    
    logger.info("=" * 60)
    logger.info("ATLAS ETL - Query Hierarchy Processing (V2 Schema)")
    logger.info(f"Execution ID: {exec_id}")
    logger.info("=" * 60)
    
    conn = get_connection()
    
    try:
        # Step 1: Get database objects from stage_v2
        step_seq += 1
        log_id = log_start(conn, exec_id, package_name, 
                          'Step 1 - Fetch from raw_v2.DatabaseObjects', step_seq)
        
        objects = get_database_objects(conn, limit)
        logger.info(f"Found {len(objects)} database objects to process")
        
        log_end(conn, log_id, len(objects))
        
        # Step 2: Parse SQL and extract references
        step_seq += 1
        log_id = log_start(conn, exec_id, package_name,
                          'Step 2 - Parse SQL for Table References', step_seq)
        
        all_references = []
        processed = 0
        errors = 0
        
        for obj in objects:
            try:
                sql_def = get_object_definition(
                    conn, 
                    obj['ServerName'], 
                    obj['DatabaseName'],
                    obj['SchemaName'],
                    obj['ObjectName']
                )
                
                if sql_def:
                    tables = extract_table_references(sql_def)
                    
                    for db, schema, table in tables:
                        all_references.append({
                            'BizKey': obj['BizKey'],
                            'Database': db,
                            'Schema': schema,
                            'Table': table,
                            'Type': 'References'
                        })
                
                processed += 1
                
                if processed % 100 == 0:
                    logger.info(f"  Processed {processed}/{len(objects)} objects...")
                    
            except Exception as e:
                errors += 1
                logger.warning(f"  Error processing {obj['ObjectName']}: {e}")
        
        logger.info(f"Extracted {len(all_references)} table references ({errors} errors)")
        
        log_end(conn, log_id, len(all_references))
        
        # Step 3: Save to stage_v2.TableReferenceStaging
        step_seq += 1
        log_id = log_start(conn, exec_id, package_name,
                          'Step 3 - Save to stage_v2.TableReferenceStaging', step_seq)
        
        save_table_references(conn, all_references)
        
        log_end(conn, log_id, len(all_references))
        
        logger.info("=" * 60)
        logger.info("Query Hierarchy processing completed successfully")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        if 'log_id' in locals():
            log_error(conn, log_id, str(e))
        return False
        
    finally:
        conn.close()


def test_connection() -> bool:
    logger.info("Testing database connection...")
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT @@SERVERNAME, DB_NAME()")
        row = cursor.fetchone()
        logger.info(f"  Connected to: {row[0]} / {row[1]}")
        
        # Check stage_v2 schema exists
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'stage_v2'
        """)
        table_count = cursor.fetchone()[0]
        logger.info(f"  Tables in stage_v2 schema: {table_count}")
        
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Atlas Query Hierarchy (V2 Schema)')
    parser.add_argument('--test', action='store_true', help='Test connection only')
    parser.add_argument('--limit', type=int, help='Limit objects to process')
    args = parser.parse_args()
    
    if args.test:
        success = test_connection()
        sys.exit(0 if success else 1)
    
    success = process_query_hierarchy(args.limit)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
