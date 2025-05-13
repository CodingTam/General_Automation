#!/usr/bin/env python3
"""
Master script to run all tests in the framework.
Automatically discovers and runs all test scripts in the tests/ directory.
"""

import os
import sys
import importlib
import inspect
import time
from datetime import datetime
from logger import logger
from utils.common import ensure_directory_exists
from utils.banner import print_intro

def get_test_files():
    """Get all Python files in the tests directory."""
    test_dir = "tests"
    if not os.path.exists(test_dir):
        logger.error(f"Tests directory not found: {test_dir}")
        return []
    
    test_files = []
    for file in os.listdir(test_dir):
        if file.startswith("test_") and file.endswith(".py"):
            test_files.append(os.path.join(test_dir, file))
    
    return test_files

def get_test_functions(module_name):
    """Get all test functions from a module."""
    try:
        module = importlib.import_module(module_name)
        test_functions = []
        
        for name, obj in inspect.getmembers(module):
            if inspect.isfunction(obj) and name.startswith("run_") and name.endswith("_test"):
                test_functions.append(obj)
        
        return test_functions
    except Exception as e:
        logger.error(f"Error loading module {module_name}: {e}")
        return []

def run_all_tests():
    """Run all test functions from all test files."""
    start_time = time.time()
    test_files = get_test_files()
    
    if not test_files:
        logger.error("No test files found")
        return False
    
    logger.info(f"Found {len(test_files)} test files")
    
    # Create reports directory
    reports_dir = "reports"
    ensure_directory_exists(reports_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(reports_dir, f"test_report_{timestamp}.txt")
    
    total_tests = 0
    passed_tests = 0
    failed_tests = []
    
    with open(report_file, 'w') as report:
        report.write(f"Test Run: {timestamp}\n")
        report.write("=" * 50 + "\n\n")
        
        for test_file in test_files:
            file_name = os.path.basename(test_file)
            module_name = f"tests.{os.path.splitext(file_name)[0]}"
            
            logger.info(f"Running tests from {file_name}")
            report.write(f"File: {file_name}\n")
            report.write("-" * 50 + "\n")
            
            test_functions = get_test_functions(module_name)
            if not test_functions:
                logger.warning(f"No test functions found in {file_name}")
                report.write("No test functions found\n\n")
                continue
            
            for func in test_functions:
                func_name = func.__name__
                total_tests += 1
                
                logger.info(f"Running {func_name}")
                report.write(f"Test: {func_name}\n")
                
                try:
                    start = time.time()
                    result = func()
                    end = time.time()
                    duration = end - start
                    
                    if result:
                        passed_tests += 1
                        status = "PASSED"
                        logger.info(f"{func_name} PASSED in {duration:.2f}s")
                    else:
                        status = "FAILED"
                        failed_tests.append(func_name)
                        logger.error(f"{func_name} FAILED in {duration:.2f}s")
                        
                    report.write(f"Status: {status}\n")
                    report.write(f"Duration: {duration:.2f}s\n\n")
                    
                except Exception as e:
                    failed_tests.append(func_name)
                    logger.error(f"Error in {func_name}: {e}")
                    report.write(f"Status: ERROR\n")
                    report.write(f"Error: {str(e)}\n\n")
            
            report.write("\n")
        
        # Write summary
        end_time = time.time()
        total_duration = end_time - start_time
        
        logger.info(f"Total tests: {total_tests}")
        logger.info(f"Passed: {passed_tests}")
        logger.info(f"Failed: {len(failed_tests)}")
        logger.info(f"Total duration: {total_duration:.2f}s")
        
        report.write("Summary\n")
        report.write("=" * 50 + "\n")
        report.write(f"Total tests: {total_tests}\n")
        report.write(f"Passed: {passed_tests}\n")
        report.write(f"Failed: {len(failed_tests)}\n")
        report.write(f"Success rate: {(passed_tests / total_tests) * 100:.2f}%\n")
        report.write(f"Total duration: {total_duration:.2f}s\n\n")
        
        if failed_tests:
            report.write("Failed tests:\n")
            for test in failed_tests:
                report.write(f"- {test}\n")
    
    logger.info(f"Test report written to {report_file}")
    return passed_tests == total_tests

if __name__ == "__main__":
    # Display the introductory banner
    print_intro("QA")  # Set the environment as needed
    
    logger.info("Starting test run")
    success = run_all_tests()
    logger.info("Test run complete")
    
    # Set exit code based on test results
    sys.exit(0 if success else 1) 