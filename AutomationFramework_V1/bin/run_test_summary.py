#!/usr/bin/env python3
"""
Framework Test Summary Script
This script runs all framework tests and prints detailed summary information.
"""

import os
import sys
import unittest
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import json
from collections import defaultdict, OrderedDict
import traceback
from colorama import init, Fore, Style
import textwrap
import sqlite3
from typing import List, Dict, Optional
from utils.logger import logger
from utils.db_handler import DBHandler

# Initialize colorama for cross-platform colored terminal output
init()

print("Debug: Script started")

# Import framework utilities if available
try:
    from logger import logger
    print("Debug: Logger imported")
except ImportError:
    # Create a simple mock logger if the real one isn't available
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("test_summary")
    print("Debug: Created mock logger")

class TestSummaryRunner:
    """Class to run tests and generate a detailed summary."""
    
    def __init__(self, test_dirs=None, verbose=False, output_file=None):
        """
        Initialize the test runner.
        
        Args:
            test_dirs (list): List of test directories to run tests from
            verbose (bool): Enable verbose output
            output_file (str): Path to save the test summary report
        """
        print(f"Debug: Initializing TestSummaryRunner with dirs={test_dirs}")
        self.test_dirs = test_dirs or ['tests/framework_self_tests', 'tests']
        self.verbose = verbose
        self.output_file = output_file
        self.start_time = None
        self.end_time = None
        self.test_results = {}
        self.summary = {
            'total': 0,
            'passed': 0,
            'failed': 0,
            'errors': 0,
            'skipped': 0,
            'duration': 0,
            'details': {}
        }
    
    def _collect_tests(self, start_dir):
        """
        Collect test modules from the given directory.
        
        Args:
            start_dir (str): Directory to collect tests from
            
        Returns:
            unittest.TestSuite: Suite containing all discovered tests
        """
        print(f"Debug: Collecting tests from {start_dir}")
        loader = unittest.TestLoader()
        suite = None
        
        try:
            # Add the parent directory to the Python path to enable imports
            sys.path.insert(0, os.path.abspath(os.path.dirname(start_dir)))
            
            # Discover tests in the directory
            suite = loader.discover(start_dir, pattern="test_*.py")
            print(f"Debug: Discovered {suite.countTestCases()} tests")
            
            # Print information about discovered tests if verbose
            if self.verbose:
                print(f"\nDiscovered tests in {start_dir}:")
                self._print_test_hierarchy(suite, prefix="  ")
            
            return suite
        except Exception as e:
            logger.error(f"Error discovering tests in {start_dir}: {str(e)}")
            print(f"{Fore.RED}Error discovering tests in {start_dir}: {str(e)}{Style.RESET_ALL}")
            print(f"Debug: Exception traceback: {traceback.format_exc()}")
            return unittest.TestSuite()
    
    def _print_test_hierarchy(self, suite, prefix=""):
        """Print a hierarchical view of the test suite."""
        for test in suite:
            if isinstance(test, unittest.TestSuite):
                if test.countTestCases() > 0:
                    # Only print non-empty suites
                    test_name = test.__class__.__name__
                    if hasattr(test, "_tests") and len(test._tests) > 0:
                        if hasattr(test._tests[0], "__class__"):
                            test_name = test._tests[0].__class__.__name__
                    print(f"{prefix}{Fore.CYAN}{test_name} ({test.countTestCases()} tests){Style.RESET_ALL}")
                    self._print_test_hierarchy(test, prefix=prefix + "  ")
            else:
                print(f"{prefix}{test.id().split('.')[-1]}")
    
    def run_tests(self):
        """
        Run all tests and collect results.
        
        Returns:
            dict: Summary of test results
        """
        print("Debug: Starting run_tests")
        self.start_time = time.time()
        print(f"\n{Fore.GREEN}{'=' * 80}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{'RUNNING FRAMEWORK TESTS':^80}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{'=' * 80}{Style.RESET_ALL}\n")
        
        for test_dir in self.test_dirs:
            print(f"Debug: Processing directory {test_dir}")
            if os.path.exists(test_dir):
                print(f"\n{Fore.CYAN}Running tests from: {test_dir}{Style.RESET_ALL}")
                
                suite = self._collect_tests(test_dir)
                if suite.countTestCases() == 0:
                    print(f"{Fore.YELLOW}No tests found in {test_dir}{Style.RESET_ALL}")
                    continue
                
                # Create result collector
                result = unittest.TestResult()
                print(f"Debug: Created test result collector")
                
                # Run tests
                print(f"Debug: Running tests from {test_dir}")
                suite.run(result)
                print(f"Debug: Completed running tests from {test_dir}")
                
                # Store results
                self.test_results[test_dir] = result
                
                # Update summary counts
                self.summary['total'] += result.testsRun
                self.summary['passed'] += result.testsRun - len(result.failures) - len(result.errors) - len(result.skipped)
                self.summary['failed'] += len(result.failures)
                self.summary['errors'] += len(result.errors)
                self.summary['skipped'] += len(result.skipped)
                
                # Store details
                details = self.summary['details'][test_dir] = {
                    'total': result.testsRun,
                    'passed': result.testsRun - len(result.failures) - len(result.errors) - len(result.skipped),
                    'failed': [],
                    'errors': [],
                    'skipped': []
                }
                
                # Store failure details
                for test, reason in result.failures:
                    details['failed'].append({
                        'test': test.id(),
                        'reason': reason
                    })
                
                # Store error details
                for test, reason in result.errors:
                    details['errors'].append({
                        'test': test.id(),
                        'reason': reason
                    })
                
                # Store skipped details
                if hasattr(result, 'skipped'):
                    for test, reason in result.skipped:
                        details['skipped'].append({
                            'test': test.id(),
                            'reason': reason
                        })
            else:
                print(f"{Fore.YELLOW}Warning: Test directory {test_dir} does not exist{Style.RESET_ALL}")
        
        self.end_time = time.time()
        self.summary['duration'] = self.end_time - self.start_time
        print("Debug: run_tests completed")
        
        return self.summary
    
    def print_summary(self):
        """Print a detailed summary of test results."""
        print("Debug: Starting print_summary")
        if not self.end_time:
            print("No tests have been run")
            return
        
        # Print overall summary banner
        print(f"\n{Fore.GREEN}{'=' * 80}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{'FRAMEWORK TEST SUMMARY':^80}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{'=' * 80}{Style.RESET_ALL}\n")
        
        # Print summary statistics
        print(f"{Fore.CYAN}Test Duration: {self.summary['duration']:.2f} seconds{Style.RESET_ALL}")
        print(f"{Fore.CYAN}Total Tests: {self.summary['total']}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}Passed: {self.summary['passed']}{Style.RESET_ALL}")
        if self.summary['failed'] > 0:
            print(f"{Fore.RED}Failed: {self.summary['failed']}{Style.RESET_ALL}")
        else:
            print(f"Failed: {self.summary['failed']}")
        if self.summary['errors'] > 0:
            print(f"{Fore.RED}Errors: {self.summary['errors']}{Style.RESET_ALL}")
        else:
            print(f"Errors: {self.summary['errors']}")
        if self.summary['skipped'] > 0:
            print(f"{Fore.YELLOW}Skipped: {self.summary['skipped']}{Style.RESET_ALL}")
        else:
            print(f"Skipped: {self.summary['skipped']}")
        
        # Print details for each test directory
        for test_dir, details in self.summary['details'].items():
            print(f"\n{Fore.CYAN}{'=' * 40}{Style.RESET_ALL}")
            print(f"{Fore.CYAN}Results for: {test_dir}{Style.RESET_ALL}")
            print(f"{Fore.CYAN}{'=' * 40}{Style.RESET_ALL}")
            print(f"Total: {details['total']}")
            print(f"{Fore.GREEN}Passed: {details['passed']}{Style.RESET_ALL}")
            
            # Print failed tests
            if details['failed']:
                print(f"\n{Fore.RED}Failed Tests ({len(details['failed'])}):{Style.RESET_ALL}")
                for i, failure in enumerate(details['failed'], 1):
                    test_id = failure['test']
                    print(f"\n{Fore.RED}{i}. {test_id}{Style.RESET_ALL}")
                    
                    if self.verbose:
                        # Print truncated reason with proper indentation
                        reason_lines = failure['reason'].split('\n')
                        # Get the error message from the traceback (usually the last line)
                        error_message = reason_lines[-1] if reason_lines else "Unknown error"
                        print(f"   {Fore.RED}Error: {error_message}{Style.RESET_ALL}")
                        print(f"   {Fore.YELLOW}Traceback: (truncated, see full report for details){Style.RESET_ALL}")
                        for line in reason_lines[:5]:  # Show only first 5 lines of traceback
                            print(f"     {line}")
                        if len(reason_lines) > 5:
                            print(f"     {Fore.YELLOW}... (truncated){Style.RESET_ALL}")
            
            # Print errors
            if details['errors']:
                print(f"\n{Fore.RED}Test Errors ({len(details['errors'])}):{Style.RESET_ALL}")
                for i, error in enumerate(details['errors'], 1):
                    test_id = error['test']
                    print(f"\n{Fore.RED}{i}. {test_id}{Style.RESET_ALL}")
                    
                    if self.verbose:
                        # Print truncated reason with proper indentation
                        reason_lines = error['reason'].split('\n')
                        # Get the error message from the traceback (usually the last line)
                        error_message = reason_lines[-1] if reason_lines else "Unknown error"
                        print(f"   {Fore.RED}Error: {error_message}{Style.RESET_ALL}")
                        print(f"   {Fore.YELLOW}Traceback: (truncated, see full report for details){Style.RESET_ALL}")
                        for line in reason_lines[:5]:  # Show only first 5 lines of traceback
                            print(f"     {line}")
                        if len(reason_lines) > 5:
                            print(f"     {Fore.YELLOW}... (truncated){Style.RESET_ALL}")
        
        print("Debug: print_summary completed")
    
    def generate_report(self):
        """Generate a detailed report file."""
        print("Debug: Starting generate_report")
        if not self.end_time:
            print("No tests have been run")
            return
        
        # Determine output file
        if not self.output_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            reports_dir = Path('reports')
            reports_dir.mkdir(exist_ok=True)
            self.output_file = str(reports_dir / f"framework_test_summary_{timestamp}.txt")
        
        # Generate report content
        report_content = []
        report_content.append("=" * 80)
        report_content.append(f"FRAMEWORK TEST SUMMARY - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_content.append("=" * 80)
        report_content.append("")
        report_content.append(f"Test Duration: {self.summary['duration']:.2f} seconds")
        report_content.append(f"Total Tests: {self.summary['total']}")
        report_content.append(f"Passed: {self.summary['passed']}")
        report_content.append(f"Failed: {self.summary['failed']}")
        report_content.append(f"Errors: {self.summary['errors']}")
        report_content.append(f"Skipped: {self.summary['skipped']}")
        report_content.append("")
        
        # Add details for each test directory
        for test_dir, details in self.summary['details'].items():
            report_content.append("=" * 40)
            report_content.append(f"Results for: {test_dir}")
            report_content.append("=" * 40)
            report_content.append(f"Total: {details['total']}")
            report_content.append(f"Passed: {details['passed']}")
            
            # Add failed tests
            if details['failed']:
                report_content.append("")
                report_content.append(f"Failed Tests ({len(details['failed'])}):")
                for i, failure in enumerate(details['failed'], 1):
                    test_id = failure['test']
                    report_content.append("")
                    report_content.append(f"{i}. {test_id}")
                    report_content.append("-" * 40)
                    report_content.append(failure['reason'])
            
            # Add errors
            if details['errors']:
                report_content.append("")
                report_content.append(f"Test Errors ({len(details['errors'])}):")
                for i, error in enumerate(details['errors'], 1):
                    test_id = error['test']
                    report_content.append("")
                    report_content.append(f"{i}. {test_id}")
                    report_content.append("-" * 40)
                    report_content.append(error['reason'])
            
            # Add skipped tests
            if details['skipped']:
                report_content.append("")
                report_content.append(f"Skipped Tests ({len(details['skipped'])}):")
                for i, skipped in enumerate(details['skipped'], 1):
                    test_id = skipped['test']
                    report_content.append(f"{i}. {test_id}: {skipped['reason']}")
        
        # Write report to file
        with open(self.output_file, 'w') as f:
            f.write('\n'.join(report_content))
        
        print(f"\n{Fore.GREEN}Test report saved to: {self.output_file}{Style.RESET_ALL}")
        
        # Also generate JSON for machine-readable output if needed
        json_file = os.path.splitext(self.output_file)[0] + '.json'
        with open(json_file, 'w') as f:
            json.dump(self.summary, f, indent=2)
        
        print(f"{Fore.GREEN}JSON report saved to: {json_file}{Style.RESET_ALL}")
        print("Debug: generate_report completed")
        
        return self.output_file

def main():
    """Main entry point for running test summary."""
    print("Debug: Starting main function")
    parser = argparse.ArgumentParser(description="Run Framework Tests and Generate Summary Report")
    parser.add_argument("--dir", action="append", help="Test directory to run tests from (can be specified multiple times)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("-o", "--output", help="Output file for test report")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    
    args = parser.parse_args()
    print(f"Debug: Parsed arguments: {args}")
    
    # Set default test directories if none specified
    test_dirs = args.dir if args.dir else ['tests/framework_self_tests', 'tests']
    
    # Run tests and generate summary
    try:
        print(f"Debug: Creating TestSummaryRunner with dirs={test_dirs}")
        runner = TestSummaryRunner(test_dirs=test_dirs, verbose=args.verbose, output_file=args.output)
        print("Debug: Calling run_tests()")
        runner.run_tests()
        print("Debug: Calling print_summary()")
        runner.print_summary()
        print("Debug: Calling generate_report()")
        runner.generate_report()
        print("Debug: All operations completed")
        
        # Return exit code based on test results
        if runner.summary['failed'] > 0 or runner.summary['errors'] > 0:
            return 1
        return 0
    except Exception as e:
        print(f"Debug: Error in main function: {str(e)}")
        print(f"Debug: Exception traceback: {traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    print("Debug: Script execution started")
    try:
        exit_code = main()
        print(f"Debug: Script completed with exit code {exit_code}")
        sys.exit(exit_code)
    except Exception as e:
        print(f"Debug: Unhandled exception: {str(e)}")
        print(f"Debug: Exception traceback: {traceback.format_exc()}")
        sys.exit(1) 