#!/usr/bin/env python3

import sqlite3
import re
from .config_loader import config
from typing import Optional
from .db_config import db_config

def connect_db() -> Optional[sqlite3.Connection]:
    """Connect to database using configuration"""
    try:
        conn = sqlite3.connect(db_config.database_path)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def infer_module_from_function(function_name):
    """Infer module name based on function name and common patterns."""
    module_mapping = {
        'create_tables': 'db_handler',
        'alter_tables': 'db_handler',
        'verify_tables': 'db_handler',
        'connect': 'db_handler',
        'execute_with_retry': 'db_handler',
        'log': 'custom_logger',
        'debug': 'custom_logger',
        'info': 'custom_logger',
        'warning': 'custom_logger',
        'error': 'custom_logger',
        'critical': 'custom_logger',
        'direct_log': 'custom_logger',
        'function_logger': 'custom_logger',
        'populate_execution_id': 'populate_execution_id',
        'update_execution_logs': 'update_execution_logs'
    }
    return module_mapping.get(function_name, "unknown")

def infer_from_message_context(message):
    """Infer module and function based on message context."""
    # Database-related messages
    if any(phrase in message.lower() for phrase in [
        'database', 'table', 'sqlite', 'sql', 'query', 'insert', 'update', 'select'
    ]):
        if 'Creating database tables' in message:
            return 'db_handler', 'create_tables'
        elif 'Verifying' in message and 'table' in message:
            return 'db_handler', 'verify_tables'
        elif 'Connected to database' in message:
            return 'db_handler', 'connect'
        elif 'Altering' in message and 'table' in message:
            return 'db_handler', 'alter_tables'
        else:
            return 'db_handler', 'execute_query'
    
    # Logger-related messages
    if any(phrase in message.lower() for phrase in [
        'log', 'debug', 'info', 'warning', 'error', 'critical'
    ]):
        level = next((word for word in ['debug', 'info', 'warning', 'error', 'critical'] 
                     if word in message.lower()), 'log')
        return 'custom_logger', level
    
    return "unknown", "unknown"

def fix_unknown_logs():
    """Fix logs with unknown module and function names based on message content."""
    print(f"Connecting to database at: {config.database_path}")
    conn = sqlite3.connect(config.database_path)
    cursor = conn.cursor()
    
    try:
        # Common patterns to extract module and function names from messages
        function_patterns = [
            # Function Started/Completed/Failed patterns
            r'Function (?:Started|Completed|Failed): ([a-zA-Z_][a-zA-Z0-9_]*)',
            # Function name in square brackets
            r'\[FUNCTION:([^\]]+)\]',
            # Function calls mentioned in messages
            r'Calling function: ([a-zA-Z_][a-zA-Z0-9_]*)',
            # Function name at start of message
            r'^([a-zA-Z_][a-zA-Z0-9_]*)\(',
            # Function mentioned after 'in'
            r' in ([a-zA-Z_][a-zA-Z0-9_]*)\(',
        ]
        
        module_patterns = [
            # Module in square brackets
            r'\[MODULE:([^\]]+)\]',
            # Module after 'in module'
            r'in module ([a-zA-Z_][a-zA-Z0-9_]*)',
            # Module path
            r'from ([a-zA-Z_][a-zA-Z0-9_]*) import',
        ]
        
        # Get all logs with unknown module or function
        cursor.execute("""
            SELECT log_entry_id, message, level
            FROM execution_log
            WHERE module = 'unknown' OR function = 'unknown'
        """)
        
        unknown_logs = cursor.fetchall()
        print(f"Found {len(unknown_logs)} logs with unknown module/function")
        
        updates = 0
        for log_id, message, level in unknown_logs:
            module_name = "unknown"
            function_name = "unknown"
            
            # Try to extract function name from patterns
            for pattern in function_patterns:
                match = re.search(pattern, message)
                if match:
                    function_name = match.group(1).strip()
                    break
            
            # Try to extract module name from patterns
            for pattern in module_patterns:
                match = re.search(pattern, message)
                if match:
                    module_name = match.group(1).strip()
                    break
            
            # If still unknown, try to infer from function name
            if function_name != "unknown" and module_name == "unknown":
                module_name = infer_module_from_function(function_name)
            
            # If still unknown, try to infer from message context
            if module_name == "unknown" or function_name == "unknown":
                inferred_module, inferred_function = infer_from_message_context(message)
                if module_name == "unknown":
                    module_name = inferred_module
                if function_name == "unknown":
                    function_name = inferred_function
            
            # If we found better information, update the log
            if module_name != "unknown" or function_name != "unknown":
                cursor.execute("""
                    UPDATE execution_log
                    SET module = ?, function = ?
                    WHERE log_entry_id = ?
                """, (module_name, function_name, log_id))
                updates += 1
        
        conn.commit()
        print(f"Updated {updates} logs with better module/function information")
        
        # Show remaining unknown logs
        cursor.execute("""
            SELECT COUNT(*)
            FROM execution_log
            WHERE module = 'unknown' OR function = 'unknown'
        """)
        remaining = cursor.fetchone()[0]
        print(f"Remaining logs with unknown module/function: {remaining}")
        
        if remaining > 0:
            print("\nSample of remaining unknown logs:")
            cursor.execute("""
                SELECT DISTINCT message, level, module, function
                FROM execution_log
                WHERE module = 'unknown' OR function = 'unknown'
                LIMIT 5
            """)
            for row in cursor.fetchall():
                print(f"- Message: {row[0]}")
                print(f"  Level: {row[1]}")
                print(f"  Module: {row[2]}")
                print(f"  Function: {row[3]}")
                print()
        
    except Exception as e:
        print(f"Error fixing unknown logs: {e}")
    finally:
        conn.close()
        print("Database connection closed")

if __name__ == "__main__":
    fix_unknown_logs() 