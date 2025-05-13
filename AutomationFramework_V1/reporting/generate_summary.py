#!/usr/bin/env python3
"""
Generate Summary Script
This script runs tests and generates a summary file without printing to console.
"""

import os
import sys
import unittest
from datetime import datetime

def run_tests_silently():
    """Run tests and generate a summary file without printing to console."""
    # Redirect stdout temporarily to avoid any terminal clearing issues
    original_stdout = sys.stdout
    sys.stdout = open(os.devnull, 'w')
    
    try:
        # Create output directory
        if not os.path.exists('test_results'):
            os.makedirs('test_results')
            
        # Time stamp for file names
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        summary_file = os.path.join('test_results', f'test_summary_{timestamp}.txt')
        
        # Open summary file for writing
        with open(summary_file, 'w') as f:
            f.write(f"TEST SUMMARY - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            # Test directories to check
            test_dirs = ['tests/framework_self_tests', 'tests']
            
            total_tests = 0
            total_passed = 0
            total_failures = 0
            total_errors = 0
            
            # Run tests from each directory
            for test_dir in test_dirs:
                f.write(f"Directory: {test_dir}\n")
                f.write("-" * 40 + "\n")
                
                if not os.path.exists(test_dir):
                    f.write(f"Directory {test_dir} does not exist\n\n")
                    continue
                
                try:
                    # Discover and run tests
                    loader = unittest.TestLoader()
                    suite = loader.discover(test_dir, pattern="test_*.py")
                    
                    if suite.countTestCases() == 0:
                        f.write("No tests found in this directory\n\n")
                        continue
                    
                    # Run tests
                    result = unittest.TextTestRunner(stream=f, verbosity=2).run(suite)
                    
                    # Update totals
                    total_tests += result.testsRun
                    total_passed += result.testsRun - len(result.failures) - len(result.errors)
                    total_failures += len(result.failures)
                    total_errors += len(result.errors)
                    
                    # Add separator
                    f.write("\n" + "-" * 40 + "\n\n")
                    
                except Exception as e:
                    f.write(f"Error running tests: {str(e)}\n\n")
            
            # Add overall summary
            f.write("\nOVERALL SUMMARY\n")
            f.write("=" * 40 + "\n")
            f.write(f"Total Tests: {total_tests}\n")
            f.write(f"Passed: {total_passed}\n")
            f.write(f"Failures: {total_failures}\n")
            f.write(f"Errors: {total_errors}\n")
            
        # Write the location to a consistent file so it can be found easily
        with open('latest_test_summary.txt', 'w') as f:
            f.write(summary_file)
            
        return summary_file
        
    finally:
        # Restore stdout
        sys.stdout.close()
        sys.stdout = original_stdout

if __name__ == "__main__":
    summary_file = run_tests_silently()
    # Only write to a file, don't print to console
    with open('test_result_location.txt', 'w') as f:
        f.write(f"Test summary saved to: {summary_file}")
        f.write("\nRun the following command to view it:\n")
        f.write(f"cat {summary_file}") 