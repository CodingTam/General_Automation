#!/usr/bin/env python3
"""
HTML Test Report Generator
Runs all tests and generates an HTML report.
"""

import os
import sys
import unittest
import time
from datetime import datetime
from pathlib import Path
import traceback
import webbrowser

def run_tests_and_generate_html():
    """Run tests and generate an HTML report."""
    # Create output directory
    reports_dir = Path('test_results')
    reports_dir.mkdir(exist_ok=True)
    
    # Create HTML output file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    html_file = reports_dir / f"test_report_{timestamp}.html"
    
    # Start HTML content
    html = []
    html.append("<!DOCTYPE html>")
    html.append("<html>")
    html.append("<head>")
    html.append("    <title>Framework Test Results</title>")
    html.append("    <style>")
    html.append("        body { font-family: Arial, sans-serif; margin: 20px; }")
    html.append("        h1 { color: #333366; }")
    html.append("        h2 { color: #333366; margin-top: 20px; }")
    html.append("        .summary { background-color: #f5f5f5; padding: 10px; border-radius: 5px; }")
    html.append("        .pass { color: green; }")
    html.append("        .fail { color: red; }")
    html.append("        .error { color: darkred; }")
    html.append("        .testcase { margin-bottom: 10px; padding: 5px; border-left: 5px solid #ccc; }")
    html.append("        .testcase.pass { border-left-color: green; }")
    html.append("        .testcase.fail { border-left-color: red; background-color: #fff0f0; }")
    html.append("        .testcase.error { border-left-color: darkred; background-color: #fff0f0; }")
    html.append("        pre { background-color: #f8f8f8; padding: 10px; border-radius: 5px; overflow-x: auto; }")
    html.append("    </style>")
    html.append("</head>")
    html.append("<body>")
    html.append(f"    <h1>Framework Test Results</h1>")
    html.append(f"    <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>")
    
    # Define test directories
    test_dirs = ["tests/framework_self_tests", "tests"]
    total_tests = 0
    total_passed = 0
    total_failed = 0
    total_errors = 0
    
    # Process each test directory
    for test_dir in test_dirs:
        html.append(f"    <h2>Test Directory: {test_dir}</h2>")
        
        if not os.path.exists(test_dir):
            html.append(f"    <p>Directory does not exist</p>")
            continue
        
        # Discover tests
        try:
            loader = unittest.TestLoader()
            suite = loader.discover(test_dir, pattern="test_*.py")
            test_count = suite.countTestCases()
            
            if test_count == 0:
                html.append(f"    <p>No tests found</p>")
                continue
            
            html.append(f"    <p>Found {test_count} tests</p>")
            
            # Custom test result class to capture HTML output
            class HTMLTestResult(unittest.TestResult):
                def __init__(self):
                    super().__init__()
                    self.test_results = []
                
                def startTest(self, test):
                    super().startTest(test)
                
                def addSuccess(self, test):
                    super().addSuccess(test)
                    self.test_results.append({
                        'id': test.id(),
                        'status': 'pass',
                        'details': None
                    })
                
                def addFailure(self, test, err):
                    super().addFailure(test, err)
                    self.test_results.append({
                        'id': test.id(),
                        'status': 'fail',
                        'details': {
                            'type': err[0].__name__,
                            'message': str(err[1]),
                            'traceback': ''.join(traceback.format_tb(err[2]))
                        }
                    })
                
                def addError(self, test, err):
                    super().addError(test, err)
                    self.test_results.append({
                        'id': test.id(),
                        'status': 'error',
                        'details': {
                            'type': err[0].__name__,
                            'message': str(err[1]),
                            'traceback': ''.join(traceback.format_tb(err[2]))
                        }
                    })
            
            # Run tests
            result = HTMLTestResult()
            suite.run(result)
            
            # Update totals
            total_tests += result.testsRun
            total_passed += result.testsRun - len(result.failures) - len(result.errors)
            total_failed += len(result.failures)
            total_errors += len(result.errors)
            
            # Add results to HTML
            html.append(f"    <div class='summary'>")
            html.append(f"        <p>Tests Run: {result.testsRun}</p>")
            html.append(f"        <p>Passed: <span class='pass'>{result.testsRun - len(result.failures) - len(result.errors)}</span></p>")
            html.append(f"        <p>Failed: <span class='fail'>{len(result.failures)}</span></p>")
            html.append(f"        <p>Errors: <span class='error'>{len(result.errors)}</span></p>")
            html.append(f"    </div>")
            
            # Add detailed results
            if result.test_results:
                html.append(f"    <h3>Test Details</h3>")
                
                for test_result in result.test_results:
                    status_class = test_result['status']
                    status_label = {
                        'pass': 'PASS',
                        'fail': 'FAIL',
                        'error': 'ERROR'
                    }.get(status_class, status_class.upper())
                    
                    html.append(f"    <div class='testcase {status_class}'>")
                    html.append(f"        <h4><span class='{status_class}'>{status_label}</span>: {test_result['id']}</h4>")
                    
                    if test_result['details']:
                        html.append(f"        <p><strong>Error Type:</strong> {test_result['details']['type']}</p>")
                        html.append(f"        <p><strong>Message:</strong> {test_result['details']['message']}</p>")
                        html.append(f"        <pre>{test_result['details']['traceback']}</pre>")
                    
                    html.append(f"    </div>")
            
        except Exception as e:
            html.append(f"    <p class='error'>Error running tests: {str(e)}</p>")
            html.append(f"    <pre>{traceback.format_exc()}</pre>")
    
    # Add overall summary
    html.append(f"    <h2>Overall Summary</h2>")
    html.append(f"    <div class='summary'>")
    html.append(f"        <p>Total Tests: {total_tests}</p>")
    html.append(f"        <p>Total Passed: <span class='pass'>{total_passed}</span></p>")
    html.append(f"        <p>Total Failed: <span class='fail'>{total_failed}</span></p>")
    html.append(f"        <p>Total Errors: <span class='error'>{total_errors}</span></p>")
    html.append(f"    </div>")
    
    # Close HTML
    html.append("</body>")
    html.append("</html>")
    
    # Write to file
    with open(html_file, 'w') as f:
        f.write('\n'.join(html))
    
    # Create a marker file with the HTML file path
    with open("latest_test_report.txt", 'w') as f:
        f.write(str(html_file))
    
    return str(html_file)

if __name__ == "__main__":
    # Run tests and generate report
    report_file = run_tests_and_generate_html()
    
    # Attempt to open the report in a browser
    try:
        webbrowser.open('file://' + os.path.abspath(report_file))
        with open("test_report_location.txt", 'w') as f:
            f.write(f"Test report saved to: {report_file}")
    except:
        # If browser open fails, just write the file location
        with open("test_report_location.txt", 'w') as f:
            f.write(f"Test report saved to: {report_file}") 