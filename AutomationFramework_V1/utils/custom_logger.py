import logging
import sqlite3
import os
import sys
import inspect
import traceback
from datetime import datetime
import uuid
from typing import Optional, Dict, Any
from .config_loader import config
from .db_config import db_config
from .db_utils import get_db_connection

# Configuration option to store all logs in the database
STORE_ALL_LOGS_IN_DB = os.environ.get('STORE_ALL_LOGS_IN_DB', 'true').lower() == 'true'

class CustomLogger:
    """Custom logger that logs to both console and database"""
    
    def __init__(self, name: str):
        """Initialize custom logger with database configuration"""
        self.name = name
        self._setup_logger()

    def _setup_logger(self):
        """Set up the logger with database connection"""
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.INFO)
        
        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # Add database handler
        self.conn = get_db_connection()
        self._ensure_log_table()

    def _ensure_log_table(self):
        """Ensure the log table exists in the database"""
        cursor = self.conn.cursor()
        
        # Check if execution_log table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='execution_log'")
        execution_log_exists = cursor.fetchone() is not None
        
        if not execution_log_exists:
            # Create the execution_log table if it doesn't exist
            print("Creating new execution_log table")
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS execution_log (
                log_entry_id TEXT PRIMARY KEY,
                execution_id TEXT,
                timestamp TIMESTAMP,
                level TEXT,
                module TEXT,
                function TEXT,
                message TEXT,
                error_details TEXT,
                traceability_id TEXT,
                execution_run_id TEXT,
                test_case_name TEXT,
                FOREIGN KEY (traceability_id) REFERENCES testcase(traceability_id),
                FOREIGN KEY (execution_run_id) REFERENCES execution_run(execution_run_id) ON DELETE SET NULL
            )
            ''')
            
            # Create the indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_execution_log_execution_id ON execution_log(execution_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_execution_log_level ON execution_log(level)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_execution_log_timestamp ON execution_log(timestamp)')
            
            self.conn.commit()
            print("Created execution_log table")
            
            # Set the ID column name to our schema
            self.id_column_name = 'log_entry_id'
        else:
            print("Using existing execution_log table")
            
            # Check what columns exist in the table
            cursor.execute("PRAGMA table_info(execution_log)")
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            # Check which ID column exists
            if 'log_entry_id' in column_names:
                self.id_column_name = 'log_entry_id'
            elif 'log_id' in column_names:
                self.id_column_name = 'log_id'
            else:
                # If neither exists, add log_entry_id
                print("Adding log_entry_id column to execution_log")
                cursor.execute('ALTER TABLE execution_log ADD COLUMN log_entry_id TEXT')
                self.id_column_name = 'log_entry_id'
            
            # Check for execution_id column
            if 'execution_id' not in column_names:
                print("Adding execution_id column to execution_log")
                cursor.execute('ALTER TABLE execution_log ADD COLUMN execution_id TEXT')
                # After adding the column, create the index
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_execution_log_execution_id ON execution_log(execution_id)')
            
            # Create other indexes if they don't exist
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_execution_log_level ON execution_log(level)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_execution_log_timestamp ON execution_log(timestamp)')
        
        self.conn.commit()
    
    def _create_log_table(self):
        """Create execution_log table if it doesn't exist."""
        cursor = self.conn.cursor()
        
        # Check if error_log table exists first
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='error_log'")
        error_log_exists = cursor.fetchone() is not None
        
        # Check if execution_log table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='execution_log'")
        execution_log_exists = cursor.fetchone() is not None
        
        # If force_recreate is True and execution_log exists, back it up first
        if self.force_recreate and execution_log_exists:
            # Create a backup with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            print(f"Force recreate is enabled. Backing up execution_log to execution_log_backup_{timestamp}")
            cursor.execute(f"ALTER TABLE execution_log RENAME TO execution_log_backup_{timestamp}")
            self.conn.commit()
            execution_log_exists = False
        
        if error_log_exists and not execution_log_exists:
            # Rename error_log to execution_log
            print("Found error_log table. Renaming to execution_log")
            cursor.execute("ALTER TABLE error_log RENAME TO execution_log")
            self.conn.commit()
            print("Renamed error_log to execution_log")
            execution_log_exists = True
        
        if not execution_log_exists:
            # Create the execution_log table if it doesn't exist
            print("Creating new execution_log table")
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS execution_log (
                log_entry_id TEXT PRIMARY KEY,
                execution_id TEXT,
                timestamp TIMESTAMP,
                level TEXT,
                module TEXT,
                function TEXT,
                message TEXT,
                error_details TEXT,
                traceability_id TEXT,
                execution_run_id TEXT,
                test_case_name TEXT,
                FOREIGN KEY (traceability_id) REFERENCES testcase(traceability_id),
                FOREIGN KEY (execution_run_id) REFERENCES execution_run(execution_run_id) ON DELETE SET NULL
            )
            ''')
            
            # Create the indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_execution_log_execution_id ON execution_log(execution_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_execution_log_level ON execution_log(level)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_execution_log_timestamp ON execution_log(timestamp)')
            
            self.conn.commit()
            print("Created execution_log table")
            
            # Set the ID column name to our schema
            self.id_column_name = 'log_entry_id'
        else:
            print("Using existing execution_log table")
            
            # Check what columns exist in the table
            cursor.execute("PRAGMA table_info(execution_log)")
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            # Check which ID column exists
            if 'log_entry_id' in column_names:
                self.id_column_name = 'log_entry_id'
            elif 'log_id' in column_names:
                self.id_column_name = 'log_id'
            else:
                # If neither exists, add log_entry_id
                print("Adding log_entry_id column to execution_log")
                cursor.execute('ALTER TABLE execution_log ADD COLUMN log_entry_id TEXT')
                self.id_column_name = 'log_entry_id'
            
            # Check for execution_id column
            if 'execution_id' not in column_names:
                print("Adding execution_id column to execution_log")
                cursor.execute('ALTER TABLE execution_log ADD COLUMN execution_id TEXT')
                # After adding the column, create the index
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_execution_log_execution_id ON execution_log(execution_id)')
            
            # Create other indexes if they don't exist
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_execution_log_level ON execution_log(level)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_execution_log_timestamp ON execution_log(timestamp)')
        
        self.conn.commit()
    
    def _get_caller_info(self):
        """Get the calling module and function name."""
        try:
            # Get the current frame and go back twice to get the actual caller
            frame = inspect.currentframe()
            if frame:
                # Go back to the caller's caller
                caller = frame.f_back
                if caller:
                    caller = caller.f_back
                
                if caller:
                    # Get module info
                    module = inspect.getmodule(caller)
                    module_name = module.__name__ if module else os.path.splitext(os.path.basename(caller.f_code.co_filename))[0]
                    
                    # Get function name
                    function_name = caller.f_code.co_name
                    
                    # Clean up the frame references
                    del frame
                    del caller
                    
                    return module_name, function_name
        except Exception as e:
            self.logger.debug(f"Error getting caller info: {e}")
        
        # If anything fails, try to get at least some information from the stack
        try:
            stack = inspect.stack()
            if len(stack) >= 3:  # We need at least 3 levels: current, _get_caller_info caller, and the actual caller
                frame_info = stack[2]  # The actual caller
                module_name = os.path.splitext(os.path.basename(frame_info.filename))[0]
                function_name = frame_info.function
                return module_name, function_name
        except Exception as e:
            self.logger.debug(f"Error getting caller info from stack: {e}")
        finally:
            del stack  # Clean up stack references
        
        return "unknown", "unknown"
    
    def log(self, level: str, message: str, error: Exception = None, 
            traceability_id: str = None, execution_run_id: str = None,
            test_case_name: str = None, context: Dict[str, Any] = None,
            execution_id: str = None, **kwargs):
        """
        Log a message to both console and database.
        
        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            message: The log message
            error: Exception object if applicable
            traceability_id: Test case traceability ID
            execution_run_id: Execution run ID
            test_case_name: Name of the test case
            context: Additional context information
            execution_id: Test execution ID to use as log ID (optional)
            **kwargs: Additional contextual information that may contain execution_id
        """
        module_name, function_name = self._get_caller_info()
        
        # Format message with function information - add function name to the actual message
        formatted_message = f"[{function_name}] {message}"
        
        # Modify the original message to include the function name for database storage
        enriched_message = f"[FUNCTION:{function_name}] {message}"
        
        # Log to console with function name
        log_method = getattr(self.logger, level.lower())
        log_method(formatted_message)
        
        # Format error details if present
        error_details = None
        if error:
            error_details = f"{type(error).__name__}: {str(error)}\n"
            error_details += traceback.format_exc()
        
        # Debug log initial execution_id
        if level.upper() in ('WARNING', 'ERROR', 'CRITICAL'):
            self.logger.debug(f"[{function_name}] Initial execution_id={execution_id}")
        
        # Check for execution_id in kwargs if not provided directly - ONLY if execution_id is None
        if execution_id is None and 'execution_id' in kwargs:
            execution_id = kwargs.pop('execution_id')  # Remove it from kwargs to avoid duplication
            if level.upper() in ('WARNING', 'ERROR', 'CRITICAL'):
                self.logger.debug(f"[{function_name}] Found execution_id={execution_id} in kwargs")
        
        # Check for other context values in kwargs if not provided directly
        if traceability_id is None and 'traceability_id' in kwargs:
            traceability_id = kwargs.pop('traceability_id')  # Remove to avoid duplication
        
        if execution_run_id is None and 'execution_run_id' in kwargs:
            execution_run_id = kwargs.pop('execution_run_id')  # Remove to avoid duplication
            
        if test_case_name is None and 'test_case_name' in kwargs:
            test_case_name = kwargs.pop('test_case_name')  # Remove to avoid duplication
        
        # Only store in database for WARNING, ERROR, CRITICAL levels
        # or if explicitly asked through context
        # or if STORE_ALL_LOGS_IN_DB is enabled
        if STORE_ALL_LOGS_IN_DB or level.upper() in ('WARNING', 'ERROR', 'CRITICAL') or (context and context.get('store_in_db', False)):
            # Use execution_id for correlation, but still generate a unique entry ID
            log_entry_id = str(uuid.uuid4())
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            if execution_id is None and level.upper() in ('ERROR', 'CRITICAL'):
                self.logger.debug(f"[{function_name}] WARNING: No execution_id for error/critical log: {message}")
            
            # Final explicit debug of parameters being stored
            if level.upper() in ('WARNING', 'ERROR', 'CRITICAL'):
                self.logger.debug(f"[{function_name}] Storing log with execution_id={execution_id}, traceability_id={traceability_id}, execution_run_id={execution_run_id}")
            
            cursor = self.conn.cursor()
            
            # Dynamic SQL construction based on existing schema
            query = f'''
            INSERT INTO execution_log (
                {self.id_column_name}, execution_id, timestamp, level, module, function, 
                message, error_details, traceability_id, 
                execution_run_id, test_case_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            cursor.execute(query, (
                log_entry_id, execution_id, timestamp, level.upper(), module_name, function_name,
                enriched_message, error_details, traceability_id, 
                execution_run_id, test_case_name
            ))
            self.conn.commit()
            
            # Verify the insert was successful - particularly for errors and critical logs
            if level.upper() in ('ERROR', 'CRITICAL'):
                cursor.execute(f"SELECT {self.id_column_name}, execution_id FROM execution_log WHERE {self.id_column_name} = ?", (log_entry_id,))
                result = cursor.fetchone()
                if result:
                    self.logger.debug(f"[{function_name}] Verified log stored with execution_id={result[1]}")
                else:
                    self.logger.debug(f"[{function_name}] WARNING: Could not verify log was stored")
    
    def debug(self, message: str, **kwargs):
        """Log a DEBUG level message."""
        self.log('debug', message, **kwargs)
    
    def info(self, message: str, **kwargs):
        """Log an INFO level message."""
        self.log('info', message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log a WARNING level message."""
        self.log('warning', message, **kwargs)
    
    def error(self, message: str, error: Exception = None, **kwargs):
        """Log an ERROR level message."""
        self.log('error', message, error=error, **kwargs)
    
    def critical(self, message: str, error: Exception = None, **kwargs):
        """Log a CRITICAL level message."""
        self.log('critical', message, error=error, **kwargs)
    
    def function_logger(self, traceability_id: Optional[str] = None, 
                      execution_run_id: Optional[str] = None,
                      test_case_name: Optional[str] = None,
                      execution_id: Optional[str] = None):
        """
        Decorator to log function entry, exit, and any exceptions.
        
        Args:
            traceability_id: Test case traceability ID
            execution_run_id: Execution run ID
            test_case_name: Name of the test case
            execution_id: Execution ID for correlating logs
        
        Returns:
            Decorated function
        """
        def decorator(func):
            def wrapper(*args, **kwargs):
                # Get function details for better logging
                func_name = func.__name__
                module_name = func.__module__
                
                # Create a full path for the function
                full_func_path = f"{module_name}.{func_name}"
                
                # Debug context extraction
                self.debug(f"Function logger for {full_func_path}: initial execution_id={execution_id}")
                
                # Log kwargs content to help with debugging
                for k, v in kwargs.items():
                    if k in ['exec_context', 'context'] or k.endswith('_context'):
                        if isinstance(v, dict):
                            self.debug(f"Context parameter '{k}' in {full_func_path}(): {v}")
                    elif k in ['execution_id', 'traceability_id', 'execution_run_id', 'test_case_name']:
                        self.debug(f"Direct parameter '{k}' in {full_func_path}(): {v}")
                
                # Check for context in kwargs
                # Extract the execution context from any **kwargs passed to the wrapped function
                # This allows the context to flow through from the decorated function to the logs
                _execution_id = execution_id
                _traceability_id = traceability_id
                _execution_run_id = execution_run_id
                _test_case_name = test_case_name
                
                # Look for exec_context or similar in kwargs
                for key, value in kwargs.items():
                    if isinstance(value, dict) and (key == 'exec_context' or key.endswith('_context')):
                        # Extract context values if present
                        if 'execution_id' in value and not _execution_id:
                            _execution_id = value['execution_id']
                            self.debug(f"Found execution_id={_execution_id} in context param {key}")
                        if 'traceability_id' in value and not _traceability_id:
                            _traceability_id = value['traceability_id']
                        if 'execution_run_id' in value and not _execution_run_id:
                            _execution_run_id = value['execution_run_id']
                        if 'test_case_name' in value and not _test_case_name:
                            _test_case_name = value['test_case_name']
                
                # Also check for direct parameters
                if 'execution_id' in kwargs and not _execution_id:
                    _execution_id = kwargs.pop('execution_id')  # Remove to avoid duplication
                    self.debug(f"Found direct execution_id={_execution_id} in kwargs")
                if 'traceability_id' in kwargs and not _traceability_id:
                    _traceability_id = kwargs.pop('traceability_id')  # Remove to avoid duplication
                if 'execution_run_id' in kwargs and not _execution_run_id:
                    _execution_run_id = kwargs.pop('execution_run_id')  # Remove to avoid duplication
                if 'test_case_name' in kwargs and not _test_case_name:
                    _test_case_name = kwargs.pop('test_case_name')  # Remove to avoid duplication
                
                # Final execution context that will be used for logging
                log_context = {
                    'execution_id': _execution_id,
                    'traceability_id': _traceability_id,
                    'execution_run_id': _execution_run_id,
                    'test_case_name': _test_case_name
                }
                self.debug(f"Final log context for {full_func_path}: {log_context}")
                
                # Create a detailed message about function execution
                start_message = f"ENTER: {full_func_path}()"
                
                self.info(start_message, 
                         traceability_id=_traceability_id,
                         execution_run_id=_execution_run_id,
                         test_case_name=_test_case_name,
                         execution_id=_execution_id,
                         context={'store_in_db': True})
                
                try:
                    result = func(*args, **kwargs)
                    
                    # Create a detailed success message
                    success_message = f"EXIT: {full_func_path}() - Completed successfully"
                    
                    self.info(success_message, 
                             traceability_id=_traceability_id,
                             execution_run_id=_execution_run_id,
                             test_case_name=_test_case_name,
                             execution_id=_execution_id,
                             context={'store_in_db': True})
                    return result
                except Exception as e:
                    # Create a detailed error message
                    error_message = f"FAILED: {full_func_path}() - Error: {str(e)}"
                    
                    self.error(error_message, 
                              error=e,
                              traceability_id=_traceability_id,
                              execution_run_id=_execution_run_id,
                              test_case_name=_test_case_name,
                              execution_id=_execution_id)
                    raise
            
            return wrapper
        
        return decorator
    
    def close(self):
        """Close the database connection."""
        self.conn.close()
        self.logger.info("Logger database connection closed")

    def debug_log_with_execution_id(self, module_name, function_name, message, execution_id=None, **kwargs):
        """Special debug log that always shows execution_id details"""
        # Extract execution_id from kwargs if present
        if not execution_id and 'execution_id' in kwargs:
            execution_id = kwargs['execution_id']
        
        # Extract from context dict if present
        if not execution_id:
            for k, v in kwargs.items():
                if isinstance(v, dict) and (k == 'exec_context' or k.endswith('_context')):
                    if 'execution_id' in v:
                        execution_id = v['execution_id']
                        break
        
        formatted = f"[{function_name}] {message} [execution_id={execution_id}]"
        self.logger.debug(formatted)
        
        # Actually do the database logging with execution_id
        if execution_id:
            # Use execution_id for correlation, but still generate a unique entry ID
            log_entry_id = str(uuid.uuid4())
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            cursor = self.conn.cursor()
            
            # Special debugging message for database
            debug_msg = f"EXECUTION_ID_DEBUG: {message}"
            
            # Dynamic SQL construction based on existing schema
            query = f'''
            INSERT INTO execution_log (
                {self.id_column_name}, execution_id, timestamp, level, module, function, 
                message, error_details, traceability_id, 
                execution_run_id, test_case_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            cursor.execute(query, (
                log_entry_id, execution_id, timestamp, 'DEBUG', module_name, function_name,
                debug_msg, None, kwargs.get('traceability_id'), 
                kwargs.get('execution_run_id'), kwargs.get('test_case_name')
            ))
            self.conn.commit()

    def direct_log(self, level, message, execution_id, details=None):
        """
        Direct logging to execution_log table.
        Log directly to the database with the provided execution_id.
        """
        # First, print directly to stdout for visibility
        print(f"===== LOG [{level}] [execution_id={execution_id}] {message} =====")
        
        # Also log to console through logger
        self.logger.debug(f"DIRECT_LOG [{level}] [execution_id={execution_id}] {message}")
        
        try:
            # Get module and function for context
            module_name, function_name = self._get_caller_info()
            
            # Log to the execution_log table
            log_entry_id = str(uuid.uuid4())
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            cursor = self.conn.cursor()
            
            # Insert the log
            query = f'''
            INSERT INTO execution_log (
                {self.id_column_name}, execution_id, timestamp, level, module, function, 
                message, error_details, traceability_id, 
                execution_run_id, test_case_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            cursor.execute(query, (
                log_entry_id, execution_id, timestamp, level, module_name, function_name,
                message, details, None, None, None
            ))
            
            # Force commit immediately
            self.conn.commit()
            
            # Verify the insert was successful
            cursor.execute(f"SELECT {self.id_column_name}, execution_id FROM execution_log WHERE {self.id_column_name} = ?", (log_entry_id,))
            result = cursor.fetchone()
            if result:
                print(f"===== LOG: Verified log stored with execution_id={result[1]} =====")
            else:
                print(f"===== LOG: WARNING: Could not verify log was stored =====")
        except Exception as e:
            print(f"===== LOG ERROR: {e} =====")
            # Print stack trace
            import traceback
            print(f"===== LOG TRACEBACK: {traceback.format_exc()} =====")
            self.logger.error(f"Error storing log: {e}")

# Create a global logger instance
# Set force_recreate to False so we don't recreate the table every time
db_logger = CustomLogger(name="db_logger")

def get_logger():
    """Get the global logger instance."""
    return db_logger 