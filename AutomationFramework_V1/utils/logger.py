import logging
from logging.handlers import RotatingFileHandler
import os
import sqlite3
import uuid
import re
from typing import Any, Dict, Optional
from utils.common import ensure_directory_exists, get_timestamp
from .config_loader import config
import inspect
from datetime import datetime
from .db_config import db_config

class SensitiveDataFilter:
    """Filter to sanitize sensitive data from log messages."""
    
    # Patterns for sensitive data
    PATTERNS = {
        'password': re.compile(r'password["\s]*[:=]\s*["\']?([^"\'\s,}]+)', re.IGNORECASE),
        'api_key': re.compile(r'api[_-]?key["\s]*[:=]\s*["\']?([^"\'\s,}]+)', re.IGNORECASE),
        'token': re.compile(r'token["\s]*[:=]\s*["\']?([^"\'\s,}]+)', re.IGNORECASE),
        'secret': re.compile(r'secret["\s]*[:=]\s*["\']?([^"\'\s,}]+)', re.IGNORECASE),
        'access_key': re.compile(r'access[_-]?key["\s]*[:=]\s*["\']?([^"\'\s,}]+)', re.IGNORECASE),
        'connection_string': re.compile(r'(mongodb|jdbc|mysql|postgresql|redis)://[^\s<>"]+', re.IGNORECASE),
        'email': re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'),
        'ip_address': re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b'),
        'credit_card': re.compile(r'\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b')
    }
    
    @classmethod
    def sanitize(cls, message: str) -> str:
        """
        Sanitize sensitive data from the message.
        
        Args:
            message: The log message to sanitize
            
        Returns:
            Sanitized message with sensitive data replaced by placeholders
        """
        sanitized = message
        
        for pattern_name, pattern in cls.PATTERNS.items():
            sanitized = pattern.sub(f'[REDACTED_{pattern_name.upper()}]', sanitized)
        
        return sanitized

class SecureRotatingFileHandler(RotatingFileHandler):
    """Secure file handler with proper permissions and sanitization."""
    
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, 
                 encoding=None, delay=False):
        super().__init__(filename, mode, maxBytes, backupCount, encoding, delay)
        
        # Ensure log file has restricted permissions (600)
        if os.path.exists(filename):
            os.chmod(filename, 0o600)
    
    def emit(self, record):
        """Override emit to sanitize log records."""
        if isinstance(record.msg, str):
            record.msg = SensitiveDataFilter.sanitize(record.msg)
        super().emit(record)

class SecureDBHandler:
    """Secure database handler for logging with proper sanitization."""
    
    def __init__(self, db_path: str = None):
        """Initialize the secure database handler with configurable path."""
        self.db_path = db_path or db_config.database_path
        ensure_directory_exists(os.path.dirname(self.db_path))
        
        # Set secure permissions if file exists
        if os.path.exists(self.db_path):
            os.chmod(self.db_path, 0o600)
    
    def get_connection(self) -> sqlite3.Connection:
        """Get a secure database connection."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")  # Enable Write-Ahead Logging
        conn.execute("PRAGMA synchronous=NORMAL")  # Balance durability and performance
        return conn
    
    def log_to_db(self, level: str, msg: str, context: Dict[str, Any]) -> None:
        """Securely log message to database with sanitization."""
        try:
            log_entry_id = str(uuid.uuid4())
            timestamp = get_timestamp()
            
            # Sanitize message and context
            sanitized_msg = SensitiveDataFilter.sanitize(msg)
            sanitized_context = {
                k: SensitiveDataFilter.sanitize(str(v)) if isinstance(v, str) else v
                for k, v in context.items()
            }
            
            conn = self.get_connection()
            try:
                cursor = conn.cursor()
                
                query = '''
                INSERT INTO execution_log (
                    log_entry_id, execution_id, timestamp, level, module, function, 
                    message, error_details, traceability_id, 
                    execution_run_id, test_case_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
                
                cursor.execute(query, (
                    log_entry_id,
                    sanitized_context.get('execution_id'),
                    timestamp,
                    level.upper(),
                    sanitized_context.get('module_name', 'unknown'),
                    sanitized_context.get('function_name', 'unknown'),
                    sanitized_msg,
                    sanitized_context.get('error_details'),
                    sanitized_context.get('traceability_id'),
                    sanitized_context.get('execution_run_id'),
                    sanitized_context.get('test_case_name')
                ))
                
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            # Log to console without sensitive data
            print(f"Error logging to database: {str(e)}")

def _get_caller_info():
    """Get the calling module and function name with improved frame handling."""
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
        print(f"Error getting caller info: {e}")
    
    # If anything fails, try to get at least some information from the stack
    try:
        stack = inspect.stack()
        if len(stack) >= 3:  # We need at least 3 levels
            frame_info = stack[2]  # The actual caller
            module_name = os.path.splitext(os.path.basename(frame_info.filename))[0]
            function_name = frame_info.function
            return module_name, function_name
    except Exception as e:
        print(f"Error getting caller info from stack: {e}")
    finally:
        if 'stack' in locals():
            del stack  # Clean up stack references
    
    return "unknown", "unknown"

class SecureLogger:
    """Thread-safe secure logger with sanitization."""
    
    def __init__(self, name: str = "AutomationLogger", 
                 db_path: str = None,
                 log_dir: str = "logs"):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)
        
        # Ensure log directory exists with proper permissions
        ensure_directory_exists(log_dir)
        os.chmod(log_dir, 0o700)  # Restricted directory permissions
        
        # Console handler with sanitization
        self._console_handler = logging.StreamHandler()
        self._console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        self._console_handler.setFormatter(console_format)
        self._logger.addHandler(self._console_handler)
        
        # Secure file handler
        log_file = os.path.join(log_dir, "framework.log")
        file_handler = SecureRotatingFileHandler(
            log_file,
            maxBytes=1000000,
            backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s'
        )
        file_handler.setFormatter(file_format)
        self._logger.addHandler(file_handler)
        
        # Secure database handler
        self.db_handler = SecureDBHandler(db_path or db_config.database_path)
        
        # Add the _get_caller_info method
        self._get_caller_info = _get_caller_info

    def direct_log(self, level: str, msg: str, execution_id: str = None, error_details: str = None, **kwargs) -> None:
        """
        Legacy method for direct logging with execution context.
        Maintains backward compatibility while ensuring security.
        
        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            msg: Log message
            execution_id: Execution ID for context
            error_details: Optional error details
            **kwargs: Additional context parameters
        """
        context = {
            'execution_id': execution_id,
            'error_details': error_details
        }
        
        # Add any additional context from kwargs
        context.update(kwargs)
        
        # Use the secure logging method
        self._log(level.upper(), msg, **context)

    def configure_logging(self, level: str = 'INFO', 
                        formatter: logging.Formatter = None,
                        root_level: str = None) -> None:
        """
        Configure logging settings.
        
        Args:
            level: Logging level for the console handler ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
            formatter: Custom formatter for console output. If None, keeps current formatter.
            root_level: Optional logging level for the root logger. If None, only this logger is affected.
        """
        # Set console handler level
        level = getattr(logging, level.upper())
        self._console_handler.setLevel(level)
        self._logger.setLevel(level)
        
        # Set formatter if provided
        if formatter:
            self._console_handler.setFormatter(formatter)
            
        # Configure root logger if requested
        if root_level:
            root_logger = logging.getLogger()
            root_logger.setLevel(getattr(logging, root_level.upper()))
            
            # Update other handlers in root logger to match
            for handler in root_logger.handlers:
                if isinstance(handler, logging.StreamHandler) and handler != self._console_handler:
                    handler.setLevel(getattr(logging, root_level.upper()))
                    if formatter:
                        handler.setFormatter(formatter)

    def _prepare_context(self, **kwargs) -> Dict[str, Any]:
        """Prepare and sanitize logging context with improved caller info."""
        context = {}
        
        # Get caller information
        module_name, function_name = self._get_caller_info()
        
        # Set module and function names, allowing override from kwargs
        context['module_name'] = kwargs.get('module_name', module_name)
        context['function_name'] = kwargs.get('function_name', function_name)
        
        # Extract standard context fields
        for key in ['traceability_id', 'execution_id', 'execution_run_id', 
                   'test_case_name']:
            if key in kwargs:
                context[key] = kwargs[key]
        
        # Extract error details if present
        if 'exc_info' in kwargs and kwargs['exc_info']:
            import traceback
            context['error_details'] = traceback.format_exc()
        
        return context
    
    def _log(self, level: str, msg: str, *args, **kwargs) -> None:
        """Internal logging method with sanitization."""
        context = self._prepare_context(**kwargs)
        
        # Sanitize message
        sanitized_msg = SensitiveDataFilter.sanitize(str(msg))
        
        # Add context info to message if present
        context_parts = []
        for key in ['traceability_id', 'execution_id', 'execution_run_id', 'test_case_name']:
            if key in context and context[key]:
                context_parts.append(f"{key}={context[key]}")
        
        if context_parts:
            sanitized_msg = f"{sanitized_msg} [{', '.join(context_parts)}]"
        
        # Log to standard logger
        log_func = getattr(self._logger, level.lower())
        log_func(sanitized_msg, *args)
        
        # Log to database
        self.db_handler.log_to_db(level, sanitized_msg, context)
    
    def debug(self, msg: str, *args, **kwargs) -> None:
        self._log('DEBUG', msg, *args, **kwargs)
    
    def info(self, msg: str, *args, **kwargs) -> None:
        self._log('INFO', msg, *args, **kwargs)
    
    def warning(self, msg: str, *args, **kwargs) -> None:
        self._log('WARNING', msg, *args, **kwargs)
    
    def error(self, msg: str, *args, **kwargs) -> None:
        self._log('ERROR', msg, *args, **kwargs)
    
    def critical(self, msg: str, *args, **kwargs) -> None:
        self._log('CRITICAL', msg, *args, **kwargs)
    
    def exception(self, msg: str, *args, **kwargs) -> None:
        kwargs['exc_info'] = True
        self._log('ERROR', msg, *args, **kwargs)

# Create a single logger instance
logger = SecureLogger()

# Backward compatibility aliases
_logger = logger._logger
console_handler = logger._console_handler

# Export only the public interface
__all__ = ['logger', 'SecureLogger']

# Secure decorator for function logging
def secure_function_logger(func):
    """Decorator to securely log function entry and exit."""
    def wrapper(*args, **kwargs):
        # Extract context safely
        context = {
            key: kwargs.get(key)
            for key in ['execution_id', 'traceability_id', 'execution_run_id', 'test_case_name']
            if key in kwargs
        }
        
        # Log function entry
        logger.debug(f"Function Started: {func.__name__}", **context)
        
        try:
            result = func(*args, **kwargs)
            logger.debug(f"Function Completed: {func.__name__}", **context)
            return result
        except Exception as e:
            logger.exception(f"Function Failed: {func.__name__}", **context)
            raise
    
    return wrapper

# For backward compatibility
function_logger = secure_function_logger

class Logger:
    def __init__(self):
        """Initialize logger with database configuration"""
        self.db_path = db_config.database_path
        self._setup_logger()
    
    def _setup_logger(self):
        """Set up the logger with database connection"""
        self.logger = logging.getLogger('framework')
        self.logger.setLevel(logging.INFO)
        
        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # Add database handler
        self.conn = sqlite3.connect(self.db_path)
        self._ensure_log_table()
    
    def _ensure_log_table(self):
        """Ensure the log table exists in the database"""
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS execution_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                level TEXT,
                message TEXT,
                module TEXT,
                function TEXT,
                line INTEGER
            )
        ''')
        self.conn.commit() 