#!/usr/bin/env python3
"""
Core Framework Self-Tests
These tests validate the essential functionality of the framework itself.
"""

import os
import sys
import unittest
from pathlib import Path

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import framework modules
from logger import logger
from utils.db_handler import DBHandler

class TestCoreFramework(unittest.TestCase):
    """Test core framework functionality."""
    
    def setUp(self):
        """Set up test environment."""
        # Configure logger for testing
        logger.info("Setting up core framework tests")
    
    def tearDown(self):
        """Clean up after tests."""
        logger.info("Tearing down core framework tests")
    
    def test_framework_initialization(self):
        """Test framework initialization."""
        # Verify core directories exist
        core_dirs = ['core', 'utils', 'helpers', 'plugins']
        for directory in core_dirs:
            self.assertTrue(os.path.isdir(directory), f"Core directory {directory} is missing")
    
    def test_database_connection(self):
        """Test database connection."""
        try:
            db = DBHandler()
            self.assertIsNotNone(db.conn, "Database connection should be established")
            db.close()
        except Exception as e:
            self.fail(f"Database connection failed: {str(e)}")
    
    def test_logger_functionality(self):
        """Test logger functionality."""
        test_log_message = "Test log message for core framework test"
        logger.info(test_log_message)
        
        # Check if log file exists and contains the message
        log_dir = Path('logs')
        self.assertTrue(log_dir.exists(), "Log directory does not exist")
        
        # Find the latest log file
        log_files = list(log_dir.glob('*.log'))
        if log_files:
            latest_log = max(log_files, key=os.path.getmtime)
            with open(latest_log, 'r') as f:
                log_content = f.read()
                self.assertIn(test_log_message, log_content, 
                             "Test log message not found in log file")
        else:
            self.fail("No log files found")

if __name__ == '__main__':
    unittest.main() 