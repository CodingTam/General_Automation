#!/usr/bin/env python3
"""
Run Internal Framework Tests
This script runs all the internal tests from the tests/ directory.
"""

import os
import sys
import unittest
import argparse
from datetime import datetime
from pathlib import Path

# Import framework utilities
from logger import logger

def run_internal_tests(specific_test=None, verbose=False, exclude_self_tests=False, summary=False, console_output=False):
    """
    Run internal framework tests using direct subprocess calls to ensure output appears in the console.
    
    Args:
        specific_test (str, optional): Specific test file to run
        verbose (bool, optional): Enable verbose output
        exclude_self_tests (bool, optional): Exclude framework self-tests
        summary (bool, optional): Whether to print a detailed summary report
        console_output (bool, optional): Display detailed console output during test runs
    
    Returns:
        int: Number of test failures
    """
    # Log start of test run
    logger.info(f"Starting internal framework tests at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print(f"\n{'=' * 80}")
    print(f"{'RUNNING INTERNAL FRAMEWORK TESTS':^80}")
    print(f"{'=' * 80}\n")
    
    import glob
    import subprocess
    
    # Find all test files
    if specific_test:
        if os.path.exists(specific_test):
            test_files = [specific_test]
        elif os.path.exists(os.path.join('tests', specific_test)):
            test_files = [os.path.join('tests', specific_test)]
        else:
            potential_files = glob.glob(os.path.join('tests', f"*{specific_test}*.py"))
            if potential_files:
                test_files = potential_files
            else:
                print(f"ERROR: Could not find test file matching: {specific_test}")
                return 1
    else:
        test_files = glob.glob(os.path.join('tests', 'test_*.py'))
        
    # Filter out framework self-tests if requested
    if exclude_self_tests:
        test_files = [f for f in test_files if 'framework_self_tests' not in f]
    
    # Sort for consistent ordering
    test_files.sort()
    
    if not test_files:
        print("No test files found to run.")
        return 0
    
    print(f"Found {len(test_files)} test files to run:")
    for i, test_file in enumerate(test_files, 1):
        print(f"{i}. {test_file}")
    print("")
    
    # Initialize counters
    total_tests = 0
    total_failures = 0
    total_errors = 0
    
    # Create reports directory if it doesn't exist
    reports_dir = Path('reports')
    if not reports_dir.exists():
        reports_dir.mkdir(exist_ok=True)
    
    # Create report file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = reports_dir / f"internal_test_report_{timestamp}.txt"
    
    with open(report_file, 'w') as f:
        f.write(f"Internal Framework Test Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"{'=' * 80}\n\n")
    
    # Run each test file in a separate process
    for test_file in test_files:
        print(f"\nRunning tests from: {test_file}")
        print("-" * 80)
        
        cmd = [sys.executable, '-m', 'unittest']
        if verbose:
            cmd.append('-v')
        cmd.append(test_file)
        
        # Run the test with a timeout
        try:
            if console_output:
                # Run with output displayed in console
                result = subprocess.run(cmd, timeout=30)
                output = f"Exit code: {result.returncode}"
                error_output = ""
                
                # Parse exit code
                if result.returncode == 0:
                    failures = 0
                    errors = 0
                    status = "PASS"
                else:
                    failures = 1
                    errors = 0
                    status = "FAIL"
                    
                # We don't know how many tests ran, just count each file as 1
                test_count = 1
            else:
                # Capture output
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                output = result.stdout
                error_output = result.stderr
                
                # Parse test results
                import re
                
                # Print the output (mimic console output)
                print(output)
                if error_output:
                    print("ERRORS:")
                    print(error_output)
                
                # Extract test count
                ran_match = re.search(r"Ran (\d+) tests", output)
                test_count = int(ran_match.group(1)) if ran_match else 0
                
                # Extract failure and error counts
                if "OK" in output:
                    failures = 0
                    errors = 0
                    status = "PASS"
                else:
                    fail_match = re.search(r"failures=(\d+)", output)
                    failures = int(fail_match.group(1)) if fail_match else 0
                    
                    error_match = re.search(r"errors=(\d+)", output)
                    errors = int(error_match.group(1)) if error_match else 0
                    
                    status = "FAIL"
        except subprocess.TimeoutExpired:
            print(f"ERROR: Test execution timed out after 30 seconds for {test_file}")
            output = "ERROR: Test execution timed out after 30 seconds"
            error_output = "Timeout error"
            failures = 1
            errors = 0
            test_count = 0
            status = "FAIL"
        except Exception as e:
            print(f"ERROR: Failed to run test {test_file}: {e}")
            output = f"ERROR: Exception occurred: {e}"
            error_output = str(e)
            failures = 1
            errors = 0
            test_count = 0
            status = "FAIL"
        
        total_tests += test_count
        total_failures += failures
        total_errors += errors
        
        # Append to report file
        with open(report_file, 'a') as f:
            f.write(f"Test File: {test_file}\n")
            f.write(f"Status: {status}\n")
            f.write(f"Tests Run: {test_count}\n")
            f.write(f"Failures: {failures}\n")
            f.write(f"Errors: {errors}\n")
            f.write("-" * 80 + "\n\n")
            f.write(output + "\n\n")
            if error_output:
                f.write("ERRORS:\n")
                f.write(error_output + "\n\n")
    
    # Log and display final results
    overall_status = "PASS" if total_failures == 0 and total_errors == 0 else "FAIL"
    
    print(f"\n{'=' * 80}")
    print(f"{'INTERNAL FRAMEWORK TEST RESULTS':^80}")
    print(f"{'=' * 80}")
    print(f"\nTotal Test Files: {len(test_files)}")
    print(f"Total Tests Run: {total_tests}")
    print(f"Total Failures: {total_failures}")
    print(f"Total Errors: {total_errors}")
    print(f"Overall Status: {overall_status}")
    
    # Append summary to report file
    with open(report_file, 'a') as f:
        f.write(f"\n{'=' * 80}\n")
        f.write(f"SUMMARY\n")
        f.write(f"{'=' * 80}\n\n")
        f.write(f"Total Test Files: {len(test_files)}\n")
        f.write(f"Total Tests Run: {total_tests}\n")
        f.write(f"Total Failures: {total_failures}\n")
        f.write(f"Total Errors: {total_errors}\n")
        f.write(f"Overall Status: {overall_status}\n")
    
    logger.info(f"Internal framework tests complete. "
                f"Ran {total_tests} tests with "
                f"{total_failures} failures and "
                f"{total_errors} errors.")
    
    print(f"\nTest report saved to: {report_file}")
    
    # Return non-zero exit code if there were failures or errors
    return 1 if (total_failures > 0 or total_errors > 0) else 0

def main():
    """Main entry point for running internal framework tests."""
    parser = argparse.ArgumentParser(description="Run Internal Framework Tests")
    parser.add_argument("--test", help="Specific test file or module to run")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--exclude-self-tests", action="store_true", 
                       help="Exclude framework self-tests from the test run")
    parser.add_argument("-s", "--summary", action="store_true", 
                      help="Generate a detailed summary report (requires run_test_summary.py)")
    parser.add_argument("--console-output", action="store_true",
                      help="Display detailed test information on console as tests run")
    
    args = parser.parse_args()
    
    # Run tests and return exit code
    exit_code = run_internal_tests(
        args.test, 
        args.verbose, 
        args.exclude_self_tests, 
        args.summary,
        args.console_output
    )
    return exit_code

if __name__ == "__main__":
    sys.exit(main()) 