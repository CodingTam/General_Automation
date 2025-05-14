#!/usr/bin/env python3

import sqlite3
import os
from datetime import datetime
from typing import Optional
from .config_loader import config
from .db_config import db_config

def connect_db() -> Optional[sqlite3.Connection]:
    """Connect to database using configuration"""
    try:
        conn = sqlite3.connect(db_config.database_path)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def get_test_case_execution_ids(conn):
    """Get all test case names and their associated execution IDs."""
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT DISTINCT execution_id, test_case_name, traceability_id, execution_run_id
            FROM testcase_execution_stats
            WHERE execution_id IS NOT NULL
        """)
        
        results = cursor.fetchall()
        print(f"Found {len(results)} test case executions with execution IDs")
        return results
    except sqlite3.Error as e:
        print(f"Error getting test case execution IDs: {e}")
        return []

def update_execution_logs(conn, execution_data):
    """Update execution logs with the correct execution_id."""
    cursor = conn.cursor()
    updated_count = 0
    
    print("\nUpdating execution logs...")
    
    for execution_id, test_case_name, traceability_id, execution_run_id in execution_data:
        try:
            # Match logs by test_case_name if available
            if test_case_name:
                cursor.execute("""
                    UPDATE execution_log
                    SET execution_id = ?
                    WHERE test_case_name = ? AND execution_id IS NULL
                """, (execution_id, test_case_name))
                rows_updated = cursor.rowcount
                updated_count += rows_updated
                print(f"Updated {rows_updated} logs for test case '{test_case_name}' with execution_id: {execution_id}")
            
            # Match logs by traceability_id if available
            if traceability_id:
                cursor.execute("""
                    UPDATE execution_log
                    SET execution_id = ?
                    WHERE traceability_id = ? AND execution_id IS NULL
                """, (execution_id, traceability_id))
                rows_updated = cursor.rowcount
                updated_count += rows_updated
                print(f"Updated {rows_updated} logs for traceability_id '{traceability_id}' with execution_id: {execution_id}")
            
            # Match logs by execution_run_id if available
            if execution_run_id:
                cursor.execute("""
                    UPDATE execution_log
                    SET execution_id = ?
                    WHERE execution_run_id = ? AND execution_id IS NULL
                """, (execution_id, execution_run_id))
                rows_updated = cursor.rowcount
                updated_count += rows_updated
                print(f"Updated {rows_updated} logs for execution_run_id '{execution_run_id}' with execution_id: {execution_id}")
            
            # For nearby logs in a time range (assuming logs for the same execution are close together)
            # First get the timestamp range for this execution
            cursor.execute("""
                SELECT MIN(timestamp), MAX(timestamp)
                FROM execution_log
                WHERE execution_id = ?
            """, (execution_id,))
            
            time_range = cursor.fetchone()
            if time_range[0] is not None:
                min_time, max_time = time_range
                
                # Add a small buffer (10 seconds) before and after
                cursor.execute("""
                    UPDATE execution_log
                    SET execution_id = ?
                    WHERE timestamp BETWEEN datetime(?, '-10 seconds') AND datetime(?, '+10 seconds')
                    AND execution_id IS NULL
                """, (execution_id, min_time, max_time))
                
                rows_updated = cursor.rowcount
                updated_count += rows_updated
                print(f"Updated {rows_updated} logs by timestamp proximity for execution_id: {execution_id}")
                
        except sqlite3.Error as e:
            print(f"Error updating logs for execution_id {execution_id}: {e}")
    
    conn.commit()
    print(f"\nTotal logs updated: {updated_count}")

def main():
    # Connect to the database
    conn = connect_db()
    if not conn:
        return
    
    try:
        # Get test case execution IDs
        execution_data = get_test_case_execution_ids(conn)
        
        # Update execution logs
        update_execution_logs(conn, execution_data)
        
        # Check how many logs still have NULL execution_id
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM execution_log WHERE execution_id IS NULL")
        null_count = cursor.fetchone()[0]
        
        print(f"\nRemaining logs with NULL execution_id: {null_count}")
        
        if null_count > 0:
            # As a fallback, assign a timestamp-based group ID to remaining NULL rows
            print("\nAssigning timestamp-based group IDs to remaining NULL rows...")
            
            # Group NULL rows by timestamp with 5-second intervals
            cursor.execute("""
                SELECT DISTINCT strftime('%Y-%m-%d %H:%M', timestamp) as time_group
                FROM execution_log
                WHERE execution_id IS NULL
                ORDER BY timestamp
            """)
            
            time_groups = cursor.fetchall()
            
            for time_group in time_groups:
                # Generate a unique execution ID for this time group
                fallback_execution_id = f"fallback-{datetime.now().strftime('%Y%m%d%H%M%S')}-{time_group[0]}"
                
                cursor.execute("""
                    UPDATE execution_log
                    SET execution_id = ?
                    WHERE strftime('%Y-%m-%d %H:%M', timestamp) = ?
                    AND execution_id IS NULL
                """, (fallback_execution_id, time_group[0]))
                
                rows_updated = cursor.rowcount
                print(f"Assigned {fallback_execution_id} to {rows_updated} logs from time group {time_group[0]}")
            
            conn.commit()
    
    except Exception as e:
        print(f"Error during execution: {e}")
    finally:
        conn.close()
        print("Database connection closed")

if __name__ == "__main__":
    main() 