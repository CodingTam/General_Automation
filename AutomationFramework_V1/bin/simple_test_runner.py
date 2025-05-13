#!/usr/bin/env python3
"""
Simple Test Runner
A minimal test runner that writes output to a log file.
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
log_file = log_dir / f"test_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

def log(message):
    """Write message to log file and print to console."""
    with open(log_file, 'a') as f:
        f.write(f"{message}\n")
    print(message)

def run_tests(test_dirs):
    """Run tests from specified directories."""
    log(f"Starting test run. Writing log to {log_file}")
    
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
            
            # Run tests and collect results
            log(f"Running {test_count} tests")
            result = unittest.TextTestRunner(verbosity=1).run(suite)
            
            # Log results
            log(f"Tests run: {result.testsRun}")
            log(f"Failures: {len(result.failures)}")
            log(f"Errors: {len(result.errors)}")
            
            if result.failures:
                log("=== FAILURES ===")
                for i, (test, reason) in enumerate(result.failures, 1):
                    log(f"{i}. {test}")
                    log(f"   {reason[:200]}...")  # Truncate long error messages
            
            if result.errors:
                log("=== ERRORS ===")
                for i, (test, reason) in enumerate(result.errors, 1):
                    log(f"{i}. {test}")
                    log(f"   {reason[:200]}...")  # Truncate long error messages
                    
        except Exception as e:
            log(f"Error running tests: {str(e)}")
            log(traceback.format_exc())
    
    log("Test run completed")

if __name__ == "__main__":
    test_dirs = sys.argv[1:] or ["tests/framework_self_tests"]
    log(f"Using test directories: {test_dirs}")
    run_tests(test_dirs) 