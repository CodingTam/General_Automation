#!/usr/bin/env python3

import sqlite3
import os
from datetime import datetime, timedelta
import uuid
from .config_loader import config
from typing import Optional
from .db_config import db_config
from .db_utils import get_db_connection

def connect_db() -> Optional[sqlite3.Connection]:
    """Connect to database using configuration"""
    try:
        conn = get_db_connection()
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def populate_execution_id():
    """Populate execution ID in the database."""
    print(f"Connecting to database at: {config.database_path}")
    conn = sqlite3.connect(config.database_path)
    cursor = conn.cursor()
    
    try:
        print("Getting test case executions from testcase_execution_stats...")
        cursor.execute("""
            SELECT 
                execution_id,
                test_case_name,
                execution_start_time,
                execution_end_time
            FROM 
                testcase_execution_stats
            WHERE 
                execution_id IS NOT NULL
            ORDER BY 
                execution_start_time
        """)
        
        executions = cursor.fetchall()
        print(f"Found {len(executions)} test case executions")
        
        total_logs_updated = 0
        
        for execution in executions:
            execution_id, test_case_name, start_time, end_time = execution
            
            # Add a small buffer before and after to catch logs that might be slightly outside
            # If start_time or end_time is None, use reasonable defaults
            if not start_time:
                # Try to estimate start time from logs
                cursor.execute("""
                    SELECT MIN(timestamp) FROM execution_log 
                    WHERE test_case_name = ? OR message LIKE ?
                """, (test_case_name, f"%{test_case_name}%"))
                
                result = cursor.fetchone()
                if result and result[0]:
                    start_time = result[0]
                else:
                    # Default to 10 minutes before end time if available
                    if end_time:
                        try:
                            end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
                            start_time = (end_dt - timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            start_time = '2000-01-01 00:00:00'  # Default fallback
                    else:
                        start_time = '2000-01-01 00:00:00'  # Default fallback
            
            if not end_time:
                # Try to estimate end time from logs
                cursor.execute("""
                    SELECT MAX(timestamp) FROM execution_log 
                    WHERE test_case_name = ? OR message LIKE ?
                """, (test_case_name, f"%{test_case_name}%"))
                
                result = cursor.fetchone()
                if result and result[0]:
                    end_time = result[0]
                else:
                    # Default to 10 minutes after start time
                    try:
                        start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                        end_time = (start_dt + timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        end_time = '2099-12-31 23:59:59'  # Default fallback
            
            try:
                # Add a buffer around the time window
                start_time_with_buffer = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S') - timedelta(seconds=30)
                end_time_with_buffer = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=30)
                
                start_time_buffered = start_time_with_buffer.strftime('%Y-%m-%d %H:%M:%S')
                end_time_buffered = end_time_with_buffer.strftime('%Y-%m-%d %H:%M:%S')
            except:
                # If there's an error with datetime parsing, use the original values
                start_time_buffered = start_time
                end_time_buffered = end_time
            
            print(f"\nProcessing test case: {test_case_name} (Execution ID: {execution_id})")
            print(f"  Time window: {start_time_buffered} to {end_time_buffered}")
            
            # Update logs by test_case_name, if it matches
            cursor.execute("""
                UPDATE execution_log
                SET execution_id = ?
                WHERE 
                    test_case_name = ? 
                    AND execution_id IS NULL
            """, (execution_id, test_case_name))
            
            test_case_name_updates = cursor.rowcount
            total_logs_updated += test_case_name_updates
            print(f"  Updated {test_case_name_updates} logs by test_case_name match")
            
            # Update logs within the time window
            cursor.execute("""
                UPDATE execution_log
                SET execution_id = ?
                WHERE 
                    timestamp BETWEEN ? AND ?
                    AND execution_id IS NULL
            """, (execution_id, start_time_buffered, end_time_buffered))
            
            time_window_updates = cursor.rowcount
            total_logs_updated += time_window_updates
            print(f"  Updated {time_window_updates} logs by time window match")
            
            # Update logs with messages containing the test case name
            if test_case_name:
                cursor.execute("""
                    UPDATE execution_log
                    SET execution_id = ?
                    WHERE 
                        message LIKE ? 
                        AND execution_id IS NULL
                """, (execution_id, f"%{test_case_name}%"))
                
                message_updates = cursor.rowcount
                total_logs_updated += message_updates
                print(f"  Updated {message_updates} logs by message content match")
        
        conn.commit()
        print(f"\nTotal logs updated with execution_id: {total_logs_updated}")
        
        # Check if there are still any logs without execution_id
        cursor.execute("SELECT COUNT(*) FROM execution_log WHERE execution_id IS NULL")
        null_count = cursor.fetchone()[0]
        
        if null_count > 0:
            print(f"\nThere are still {null_count} logs without an execution_id")
            
            # Create a default execution_id for remaining NULL rows
            default_execution_id = f"default-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            cursor.execute("""
                UPDATE execution_log
                SET execution_id = ?
                WHERE execution_id IS NULL
            """, (default_execution_id,))
            
            print(f"Assigned '{default_execution_id}' to all remaining logs")
            conn.commit()
        
        print("\nAll logs now have an execution_id")
    
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()
        print("Database connection closed")

if __name__ == "__main__":
    populate_execution_id() 