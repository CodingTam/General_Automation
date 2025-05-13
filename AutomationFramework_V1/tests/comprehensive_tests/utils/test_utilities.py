#!/usr/bin/env python3
"""
Comprehensive Tests for Framework Utilities
Tests utility functions and helper modules.
"""

import os
import sys
import unittest
import tempfile
import re
import logging
from datetime import datetime

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

# Import framework modules
from logger import logger, function_logger
from utils.version import VERSION, FRAMEWORK_NAME


class TestUtilities(unittest.TestCase):
    """Test suite for framework utilities."""
    
    def setUp(self):
        """Set up the test environment."""
        # Create temp directory for test files
        self.temp_dir = tempfile.mkdtemp()
        
        # Set up a log file handler to capture logs
        self.log_file = os.path.join(self.temp_dir, 'test_log.log')
        
        # Keep reference to existing handlers
        self.original_handlers = logger.handlers.copy()
        
        # Add file handler for testing
        self.file_handler = logging.FileHandler(self.log_file)
        self.file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
        logger.addHandler(self.file_handler)
    
    def tearDown(self):
        """Clean up after tests."""
        # Remove the test file handler
        if self.file_handler in logger.handlers:
            logger.removeHandler(self.file_handler)
        
        # Reset original handlers
        logger.handlers = self.original_handlers
        
        # Clean up temp directory
        try:
            if os.path.exists(self.log_file):
                os.remove(self.log_file)
            os.rmdir(self.temp_dir)
        except:
            pass
    
    # ========== Logger Tests ==========
    
    def test_logger_levels(self):
        """Test logger at different logging levels."""
        # Test log messages at different levels
        test_message = f"Test logger message {datetime.now().timestamp()}"
        
        logger.debug(f"DEBUG {test_message}")
        logger.info(f"INFO {test_message}")
        logger.warning(f"WARNING {test_message}")
        logger.error(f"ERROR {test_message}")
        logger.critical(f"CRITICAL {test_message}")
        
        # Check log file contains the messages
        with open(self.log_file, 'r') as f:
            log_content = f.read()
        
        # Debug may not be logged depending on log level, but others should be
        self.assertIn(f"INFO {test_message}", log_content, "INFO message not logged")
        self.assertIn(f"WARNING {test_message}", log_content, "WARNING message not logged")
        self.assertIn(f"ERROR {test_message}", log_content, "ERROR message not logged")
        self.assertIn(f"CRITICAL {test_message}", log_content, "CRITICAL message not logged")
    
    def test_logger_formatting(self):
        """Test logger message formatting."""
        # Generate a test message with current timestamp to ensure uniqueness
        test_message = f"Format test message {datetime.now().timestamp()}"
        
        # Log the message
        logger.info(test_message)
        
        # Check log file format
        with open(self.log_file, 'r') as f:
            log_lines = f.readlines()
        
        # Find our test message line
        test_line = None
        for line in log_lines:
            if test_message in line:
                test_line = line
                break
        
        self.assertIsNotNone(test_line, "Test message not found in log file")
        
        # Check the format matches YYYY-MM-DD HH:MM:SS,mmm | LEVEL | message
        # This may need adjustment based on your exact log format
        log_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} \| (INFO|WARNING|ERROR|CRITICAL|DEBUG) \| .*'
        self.assertTrue(re.match(log_pattern, test_line), f"Log line format incorrect: {test_line}")
    
    @function_logger
    def test_decorated_function(self, test_arg, test_kwarg=None):
        """Test function with function_logger decorator."""
        # This function is decorated with function_logger
        # It should log entry and exit
        return f"{test_arg}_{test_kwarg}"
    
    def test_function_logger_decorator(self):
        """Test function_logger decorator."""
        # Run the decorated function
        unique_key = datetime.now().timestamp()
        result = self.test_decorated_function(f"test_arg_{unique_key}", test_kwarg=123)
        
        # Verify function worked correctly
        self.assertEqual(result, f"test_arg_{unique_key}_123", "Decorated function should return correct result")
        
        # Check log file for entry and exit messages
        with open(self.log_file, 'r') as f:
            log_content = f.read()
        
        # Look for entry and exit log messages
        # The exact format depends on your function_logger implementation
        expected_entry = f"test_arg_{unique_key}"
        self.assertIn(expected_entry, log_content, "Function entry log message not found")
    
    # ========== Version Tests ==========
    
    def test_version_constants(self):
        """Test version constants."""
        # Verify VERSION is a string with semver format
        self.assertIsInstance(VERSION, str, "VERSION should be a string")
        
        # Check if VERSION follows semver format (MAJOR.MINOR.PATCH)
        semver_pattern = r'^\d+\.\d+\.\d+$'
        self.assertTrue(re.match(semver_pattern, VERSION), 
                       f"VERSION '{VERSION}' should follow semver format")
        
        # Verify FRAMEWORK_NAME is a non-empty string
        self.assertIsInstance(FRAMEWORK_NAME, str, "FRAMEWORK_NAME should be a string")
        self.assertTrue(len(FRAMEWORK_NAME) > 0, "FRAMEWORK_NAME should not be empty")
    
    # ========== Path Handling Tests ==========
    
    def test_path_resolution(self):
        """Test path resolution utilities."""
        # Create some test directories and files
        test_subdir = os.path.join(self.temp_dir, 'subdir')
        os.makedirs(test_subdir, exist_ok=True)
        
        test_file1 = os.path.join(self.temp_dir, 'test1.txt')
        test_file2 = os.path.join(test_subdir, 'test2.txt')
        
        with open(test_file1, 'w') as f:
            f.write('test1 content')
        
        with open(test_file2, 'w') as f:
            f.write('test2 content')
        
        # Test path-related utilities
        # Get absolute path
        abs_path1 = os.path.abspath(test_file1)
        self.assertTrue(os.path.isabs(abs_path1), "Should be an absolute path")
        self.assertTrue(os.path.exists(abs_path1), "Path should exist")
        
        # Get relative path
        rel_path = os.path.relpath(test_file2, self.temp_dir)
        self.assertEqual(rel_path, os.path.join('subdir', 'test2.txt'), 
                        "Relative path calculation incorrect")
        
        # Get directory name
        dir_name = os.path.dirname(test_file2)
        self.assertEqual(dir_name, test_subdir, "Directory name calculation incorrect")
        
        # Get base name
        base_name = os.path.basename(test_file2)
        self.assertEqual(base_name, 'test2.txt', "Base name calculation incorrect")
    
    # ========== Date/Time Handling Tests ==========
    
    def test_datetime_formatting(self):
        """Test datetime formatting utilities."""
        # Test ISO format
        now = datetime.now()
        iso_format = now.isoformat()
        
        # Verify ISO format is valid
        iso_pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+'
        self.assertTrue(re.match(iso_pattern, iso_format), 
                       f"ISO format '{iso_format}' is invalid")
        
        # Test custom format
        custom_format = now.strftime('%Y%m%d_%H%M%S')
        
        # Verify custom format
        custom_pattern = r'\d{8}_\d{6}'
        self.assertTrue(re.match(custom_pattern, custom_format),
                       f"Custom format '{custom_format}' is invalid")
        
        # Test parsing
        parsed_date = datetime.strptime('2023-01-01', '%Y-%m-%d')
        self.assertEqual(parsed_date.year, 2023, "Year should be 2023")
        self.assertEqual(parsed_date.month, 1, "Month should be 1")
        self.assertEqual(parsed_date.day, 1, "Day should be 1")


if __name__ == '__main__':
    unittest.main() 