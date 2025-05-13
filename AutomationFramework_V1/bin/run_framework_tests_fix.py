#!/usr/bin/env python3
"""
Simple Framework Tests Runner
This script runs each test file in the tests/framework_self_tests directory directly using unittest.
"""

import os
import sys
import unittest
from datetime import datetime

def run_tests():
    """Run all test files in tests/framework_self_tests directory using unittest."""
    print("\n" + "="*80)
    print("FRAMEWORK SELF-TESTS".center(80))
    print("="*80 + "\n")
    
    # Find all test files
    test_dir = os.path.join('tests', 'framework_self_tests')
    
    if not os.path.exists(test_dir):
        print(f"ERROR: Test directory {test_dir} does not exist")
        return 1
        
    # Create a test loader
    loader = unittest.TestLoader()
    
    # Try to load tests from the directory
    try:
        suite = loader.discover(test_dir, pattern="test_*.py")
    except Exception as e:
        print(f"ERROR loading tests: {e}")
        return 1
    
    # Count the tests
    test_count = 0
    test_files = []
    for module_tests in suite:
        for test_class in module_tests:
            if hasattr(test_class, '_tests'):
                # Get the file name from the test class
                file_name = sys.modules.get(test_class.__class__.__module__).__file__
                if file_name and file_name not in test_files:
                    test_files.append(file_name)
                test_count += len(test_class._tests)
    
    if not test_count:
        print("No tests found")
        return 0
    
    print(f"Found {test_count} tests in {len(test_files)} files")
    
    # Create a test runner that will output to stdout
    runner = unittest.TextTestRunner(verbosity=2)
    
    # Run the tests
    print("\nRunning tests...")
    print("-"*80)
    
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "="*80)
    print("TEST RESULTS".center(80))
    print("="*80)
    print(f"Total tests run: {result.testsRun}")
    print(f"Passed: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failed: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if not result.failures and not result.errors:
        print("\nOVERALL STATUS: PASS")
        return 0
    else:
        print("\nOVERALL STATUS: FAIL")
        return 1

if __name__ == "__main__":
    sys.exit(run_tests())

#!/usr/bin/env python3
"""
Simple Framework Tests Runner
This script runs individual tests in the framework_self_tests directory using direct subprocess calls.
"""

import os
import sys
import glob
import subprocess
from datetime import datetime

def run_individual_test(test_file, verbose=True):
    """Run a single test file directly with Python."""
    print(f"\nRunning: {test_file}")
    print("-" * 60)
    
    # Build command to run an individual test
    cmd = [sys.executable, "-m", "unittest"]
    if verbose:
        cmd.append("-v")
    cmd.append(test_file)
    
    try:
        # Run with 20 second timeout to prevent hanging
        process = subprocess.run(
            cmd, 
            check=False,
            timeout=20
        )
        return process.returncode == 0
    except subprocess.TimeoutExpired:
        print(f"ERROR: Test {test_file} timed out after 20 seconds")
        return False
    except Exception as e:
        print(f"ERROR: Failed to run test {test_file}: {e}")
        return False

def run_tests():
    """Run all test files individually."""
    print("\n" + "="*60)
    print("FRAMEWORK SELF-TESTS".center(60))
    print("="*60 + "\n")
    
    # Find all test files
    test_dir = "tests/framework_self_tests"
    test_pattern = os.path.join(test_dir, "test_*.py")
    test_files = glob.glob(test_pattern)
    test_files.sort()  # Sort for consistent order
    
    if not test_files:
        print(f"No test files found matching pattern: {test_pattern}")
        return 0
    
    print(f"Found {len(test_files)} test files:")
    for i, test_file in enumerate(test_files, 1):
        print(f"{i}. {test_file}")
    
    # Run each test individually
    passed = []
    failed = []
    
    for test_file in test_files:
        success = run_individual_test(test_file)
        if success:
            passed.append(test_file)
        else:
            failed.append(test_file)
    
    # Print summary
    print("\n" + "="*60)
    print("TEST RESULTS".center(60))
    print("="*60)
    print(f"Test files processed: {len(test_files)}")
    print(f"Passed: {len(passed)}")
    print(f"Failed: {len(failed)}")
    
    if failed:
        print("\nFailed tests:")
        for test_file in failed:
            print(f"- {test_file}")
    
    if not failed:
        print("\nOVERALL STATUS: PASS")
        return 0
    else:
        print("\nOVERALL STATUS: FAIL")
        return 1

if __name__ == "__main__":
    sys.exit(run_tests()) 