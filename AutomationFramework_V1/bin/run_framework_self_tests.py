#!/usr/bin/env python3
"""
Run Framework Self-Tests
This script runs all the framework's self-tests from the tests/framework_self_tests directory.
"""

import os
import sys
import unittest
import argparse
from datetime import datetime
from pathlib import Path

# Import framework utilities
from logger import logger

def run_framework_tests(specific_test=None, verbose=False, summary=False):
    """
    Run framework self-tests by using the run_tests.sh script.
    
    Args:
        specific_test (str, optional): Specific test file to run
        verbose (bool, optional): Enable verbose output
        summary (bool, optional): Whether to print a detailed summary report
    
    Returns:
        int: Number of test failures
    """
    # Log start of test run
    logger.info(f"Starting framework self-tests at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Just call the run_tests.sh script directly since we already know it works
    import subprocess
    
    cmd = ["./run_tests.sh", "--console"]
    
    print(f"Running framework tests using: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode
    except Exception as e:
        print(f"Error running tests: {e}")
        return 1

def main():
    """Main entry point for running framework self-tests."""
    parser = argparse.ArgumentParser(description="Run Framework Self-Tests")
    parser.add_argument("--test", help="Specific test file or module to run")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("-s", "--summary", action="store_true", 
                      help="Generate a detailed summary report (requires run_test_summary.py)")
    
    args = parser.parse_args()
    
    # Run tests and return exit code
    exit_code = run_framework_tests(args.test, args.verbose, args.summary)
    return exit_code

if __name__ == "__main__":
    sys.exit(main()) 