#!/usr/bin/env python3
"""
All Framework Tests Runner
Runs all framework self-tests and generates a comprehensive report
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

def run_all_tests():
    """Run all framework tests using subprocess to avoid output issues"""
    # Create output directory
    results_dir = Path('test_results')
    results_dir.mkdir(exist_ok=True)
    
    # Generate timestamp for file names
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Create main results file
    result_file = results_dir / f"framework_tests_{timestamp}.txt"
    summary_file = results_dir / f"summary_{timestamp}.txt"
    
    # Define all test files
    test_files = [
        'tests/framework_self_tests/test_yaml_processor.py',
        'tests/framework_self_tests/test_logger.py',
        'tests/framework_self_tests/test_db_handler.py',
        'tests/framework_self_tests/test_cli.py'
    ]
    
    # Initialize summary data
    total_tests = 0
    total_passed = 0
    total_failed = 0
    total_errors = 0
    file_results = {}
    
    # Run each test file and collect results
    with open(result_file, 'w') as f:
        f.write(f"FRAMEWORK SELF-TESTS RESULTS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
        
        for test_file in test_files:
            if not os.path.exists(test_file):
                f.write(f"Test file not found: {test_file}\n\n")
                continue
            
            f.write(f"Running tests from: {test_file}\n")
            f.write("-" * 60 + "\n")
            
            # Create a temporary file to capture the result
            temp_output = results_dir / f"temp_{os.path.basename(test_file)}.txt"
            
            try:
                # Run the test with subprocess
                result = subprocess.run(
                    ['python3', '-m', 'unittest', test_file],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True
                )
                
                # Write the output to our results file
                f.write(result.stdout)
                f.write("\n\n")
                
                # Parse results for summary
                if "OK" in result.stdout:
                    # Successful test run, extract test count
                    for line in result.stdout.split('\n'):
                        if "Ran " in line:
                            test_count = int(line.split("Ran ")[1].split()[0])
                            total_tests += test_count
                            total_passed += test_count
                            file_results[test_file] = {
                                'status': 'PASS',
                                'tests': test_count,
                                'passed': test_count,
                                'failed': 0,
                                'errors': 0
                            }
                            break
                elif "FAILED" in result.stdout:
                    # Failed test run, extract counts
                    test_count = 0
                    failures = 0
                    errors = 0
                    
                    for line in result.stdout.split('\n'):
                        if "Ran " in line:
                            test_count = int(line.split("Ran ")[1].split()[0])
                        if "failures=" in line:
                            parts = line.split("failures=")[1]
                            failures = int(parts.split(',')[0])
                            if "errors=" in parts:
                                errors = int(parts.split("errors=")[1].split(')')[0])
                    
                    total_tests += test_count
                    total_passed += (test_count - failures - errors)
                    total_failed += failures
                    total_errors += errors
                    
                    file_results[test_file] = {
                        'status': 'FAIL',
                        'tests': test_count,
                        'passed': test_count - failures - errors,
                        'failed': failures,
                        'errors': errors
                    }
            except Exception as e:
                f.write(f"Error running test: {str(e)}\n\n")
    
    # Create summary file
    with open(summary_file, 'w') as f:
        f.write(f"FRAMEWORK SELF-TESTS SUMMARY - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
        
        f.write("Individual Test Files:\n")
        f.write("-" * 60 + "\n")
        
        for test_file, result in file_results.items():
            status_str = result['status']
            f.write(f"{status_str} - {test_file}\n")
            f.write(f"  Tests: {result['tests']}, Passed: {result['passed']}, Failed: {result['failed']}, Errors: {result['errors']}\n\n")
        
        f.write("\nOverall Summary:\n")
        f.write("-" * 60 + "\n")
        f.write(f"Total Test Files: {len(file_results)}\n")
        f.write(f"Total Tests Run: {total_tests}\n")
        f.write(f"Total Tests Passed: {total_passed}\n")
        f.write(f"Total Tests Failed: {total_failed}\n")
        f.write(f"Total Errors: {total_errors}\n\n")
        
        if total_failed == 0 and total_errors == 0:
            f.write("OVERALL STATUS: PASS\n")
        else:
            f.write("OVERALL STATUS: FAIL\n")
    
    # Create marker files with paths
    with open('latest_test_results.txt', 'w') as f:
        f.write(str(result_file))
    
    with open('latest_test_summary.txt', 'w') as f:
        f.write(str(summary_file))
    
    return summary_file, result_file

if __name__ == "__main__":
    summary_file, result_file = run_all_tests()
    
    # Output file locations to help find results
    print(f"Test results saved to:")
    print(f"- Summary: {summary_file}")
    print(f"- Detailed results: {result_file}")
    print(f"\nTo view the summary:")
    print(f"cat {summary_file}") 