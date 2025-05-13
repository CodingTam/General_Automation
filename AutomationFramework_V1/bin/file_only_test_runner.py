#!/usr/bin/env python3
"""
File-Only Test Runner
A test runner that only writes output to a log file (no console output).
"""

import os
import sys
import unittest
import time
import traceback
from datetime import datetime
from pathlib import Path

# Create log file and directory
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = log_dir / f"test_run_{timestamp}.log"

def log(message):
    """Write message to log file only."""
    with open(log_file, 'a') as f:
        f.write(f"{message}\n")

def run_tests(test_dirs):
    """Run tests from specified directories."""
    log(f"Starting test run at {timestamp}")
    log(f"Log file: {log_file}")
    
    for test_dir in test_dirs:
        log(f"Processing directory: {test_dir}")
        
        if not os.path.exists(test_dir):
            log(f"Directory {test_dir} does not exist")
            continue
        
        # Set up test loader
        try:
            loader = unittest.TestLoader()
            log(f"Created test loader")
            
            # Discover tests
            log(f"Discovering tests in {test_dir}")
            suite = loader.discover(test_dir, pattern="test_*.py")
            
            test_count = suite.countTestCases()
            log(f"Discovered {test_count} tests")
            
            if test_count == 0:
                log(f"No tests found in {test_dir}")
                continue
            
            # Create a custom result class to capture output
            class LoggingTestResult(unittest.TestResult):
                def startTest(self, test):
                    super().startTest(test)
                    log(f"Starting test: {test.id()}")
                
                def stopTest(self, test):
                    super().stopTest(test)
                    log(f"Completed test: {test.id()}")
                
                def addSuccess(self, test):
                    super().addSuccess(test)
                    log(f"✓ PASS: {test.id()}")
                
                def addFailure(self, test, err):
                    super().addFailure(test, err)
                    log(f"✗ FAIL: {test.id()}")
                    log(f"Error: {err[1]}")
                    log(f"Traceback: {traceback.format_tb(err[2])}")
                
                def addError(self, test, err):
                    super().addError(test, err)
                    log(f"! ERROR: {test.id()}")
                    log(f"Error: {err[1]}")
                    log(f"Traceback: {traceback.format_tb(err[2])}")
            
            # Run tests with custom result handler
            log(f"Running {test_count} tests")
            result = LoggingTestResult()
            suite.run(result)
            
            # Log results
            log(f"Tests run: {result.testsRun}")
            log(f"Failures: {len(result.failures)}")
            log(f"Errors: {len(result.errors)}")
            
            # Detailed results are already logged by the custom result handler
                    
        except Exception as e:
            log(f"Error running tests: {str(e)}")
            log(f"Traceback: {traceback.format_exc()}")
    
    log("Test run completed")
    # Create a marker file to indicate we're done
    with open(log_dir / f"COMPLETED_{timestamp}.txt", 'w') as f:
        f.write(f"Test run completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    test_dirs = sys.argv[1:] or ["tests/framework_self_tests"]
    log(f"Using test directories: {test_dirs}")
    run_tests(test_dirs)
    # Write the path to this log file in a known location
    with open("latest_test_log.txt", 'w') as f:
        f.write(str(log_file)) 