#!/usr/bin/env python3
"""
Logger Self-Tests
These tests validate the logger functionality of the framework.
"""

import os
import sys
import unittest
import tempfile
import re
import time
from pathlib import Path

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import framework modules
from logger import logger, function_logger

class LoggerTestClass:
    """Test class for function logger decorator."""
    
    @function_logger
    def test_logged_function(self, arg1, arg2=None):
        """Test function with function_logger decorator."""
        logger.info(f"Inside test_logged_function with {arg1} and {arg2}")
        return arg1, arg2

class TestLogger(unittest.TestCase):
    """Test logger functionality."""
    
    def setUp(self):
        """Set up test environment."""
        # Check if logs directory exists, create if not
        logs_dir = Path('logs')
        if not logs_dir.exists():
            logs_dir.mkdir(exist_ok=True)
    
    def tearDown(self):
        """Clean up after tests."""
        pass
    
    def _get_latest_log_file(self):
        """Get the latest log file in the logs directory."""
        log_dir = Path('logs')
        log_files = list(log_dir.glob('*.log'))
        if log_files:
            return max(log_files, key=os.path.getmtime)
        return None
    
    def _read_last_n_lines(self, file_path, n=10):
        """Read the last n lines of a file."""
        with open(file_path, 'r') as f:
            return list(f)[-n:]
    
    def test_logger_info(self):
        """Test logger info level logging."""
        test_message = f"Test INFO message {time.time()}"
        logger.info(test_message)
        
        # Get the latest log file
        log_file = self._get_latest_log_file()
        self.assertIsNotNone(log_file, "No log file found")
        
        # Read the last few lines of the log file
        last_lines = self._read_last_n_lines(log_file)
        
        # Check if our message is in the log
        found = False
        for line in last_lines:
            if test_message in line and "INFO" in line:
                found = True
                break
        
        self.assertTrue(found, f"Test info message not found in log file: {test_message}")
    
    def test_logger_error(self):
        """Test logger error level logging."""
        test_message = f"Test ERROR message {time.time()}"
        logger.error(test_message)
        
        # Get the latest log file
        log_file = self._get_latest_log_file()
        self.assertIsNotNone(log_file, "No log file found")
        
        # Read the last few lines of the log file
        last_lines = self._read_last_n_lines(log_file)
        
        # Check if our message is in the log
        found = False
        for line in last_lines:
            if test_message in line and "ERROR" in line:
                found = True
                break
        
        self.assertTrue(found, f"Test error message not found in log file: {test_message}")
    
    def test_logger_warning(self):
        """Test logger warning level logging."""
        test_message = f"Test WARNING message {time.time()}"
        logger.warning(test_message)
        
        # Get the latest log file
        log_file = self._get_latest_log_file()
        self.assertIsNotNone(log_file, "No log file found")
        
        # Read the last few lines of the log file
        last_lines = self._read_last_n_lines(log_file)
        
        # Check if our message is in the log
        found = False
        for line in last_lines:
            if test_message in line and "WARNING" in line:
                found = True
                break
        
        self.assertTrue(found, f"Test warning message not found in log file: {test_message}")
    
    def test_function_logger_decorator(self):
        """Test function_logger decorator."""
        # Create an instance of the test class
        test_obj = LoggerTestClass()
        
        # Call the decorated method
        arg1 = "test_arg"
        arg2 = 123
        result = test_obj.test_logged_function(arg1, arg2=arg2)
        
        # Verify the function returned correctly
        self.assertEqual(result, (arg1, arg2))
        
        # Get the latest log file
        log_file = self._get_latest_log_file()
        self.assertIsNotNone(log_file, "No log file found")
        
        # Read the last few lines of the log file
        last_lines = self._read_last_n_lines(log_file)
        
        # Check if our function entry and exit messages are in the log
        entry_found = False
        exit_found = False
        
        for line in last_lines:
            if "Entering LoggerTestClass.test_logged_function" in line and "test_arg" in line:
                entry_found = True
            if "Exiting LoggerTestClass.test_logged_function" in line:
                exit_found = True
        
        self.assertTrue(entry_found, "Function entry log message not found")
        self.assertTrue(exit_found, "Function exit log message not found")
    
    def test_logger_formatting(self):
        """Test logger message formatting."""
        test_message = f"Test format message {time.time()}"
        logger.info(test_message)
        
        # Get the latest log file
        log_file = self._get_latest_log_file()
        self.assertIsNotNone(log_file, "No log file found")
        
        # Read the last few lines of the log file
        last_lines = self._read_last_n_lines(log_file)
        
        # Find our message and check its format
        for line in last_lines:
            if test_message in line:
                # Check if the log line follows expected format (timestamp, level, message)
                # Format: YYYY-MM-DD HH:MM:SS,mmm - LEVEL - MESSAGE
                log_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - INFO - .*'
                self.assertTrue(re.match(log_pattern, line), f"Log line format incorrect: {line}")
                break

if __name__ == '__main__':
    unittest.main() 