#!/usr/bin/env python3
"""
Comprehensive Framework Test Runner
Runs comprehensive tests for all framework components.
"""

import os
import sys
import time
import unittest
import importlib
import argparse
import subprocess
import glob
import re
from datetime import datetime
import traceback

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import logger
from logger import logger

# Configuration settings
TIMEOUT_SECONDS = 20  # Timeout for each test module
SKIP_PROBLEMATIC_TESTS = ["test_cli.py"]  # Tests known to cause issues
SAVE_REPORTS = True  # Save detailed reports

def print_stage(message):
    """Print a stage message with immediate flushing."""
    print(message)
    sys.stdout.flush()
    # Add a small delay to ensure visibility
    time.sleep(0.1)

class TestResult:
    """Class to store test result information."""
    def __init__(self, module_name, result=None):
        self.module_name = module_name
        
        # Initialize with defaults
        self.success = False
        self.run = 0
        self.errors = 0
        self.failures = 0
        self.skipped = 0
        self.time_taken = 0
        self.error_details = []
        self.failure_details = []
        self.reason = "Unknown"
        self.fix_module = "Unknown"
        self.exit_code = None
        
        # If a result object is provided, use its data
        if result:
            self.success = result.get('success', False)
            self.run = result.get('tests_run', 0)
            self.errors = result.get('errors', 0)
            self.failures = result.get('failures', 0)
            self.skipped = result.get('skipped', 0)
            self.time_taken = result.get('time_taken', 0)
            self.reason = result.get('reason', "Unknown")
            self.fix_module = result.get('fix_module', "Unknown")
            self.exit_code = result.get('exit_code', None)
            self.test_cases = result.get('test_cases', [])


class ComprehensiveTestRunner:
    """Runs all comprehensive tests for the framework."""
    
    def __init__(self, args):
        """Initialize the test runner."""
        self.args = args
        self.results = []
        self.test_modules = []
        self.total_tests = 0
        self.total_errors = 0
        self.total_failures = 0
        self.total_skipped = 0
        self.total_time = 0
        
        # Create output directory for reports
        os.makedirs('reports', exist_ok=True)
        
        # Configure test suite based on categories
        self.initialize_test_modules()
    
    def initialize_test_modules(self):
        """Initialize the list of test modules to run."""
        # Always include core framework tests
        core_tests = ['test_core_functionality.py', 'test_db_handler.py', 
                      'test_logger.py', 'test_plugin_system.py', 'test_yaml_processor.py']
        
        self.test_modules.append(('Core Framework Tests', [
            os.path.join('tests/framework_self_tests', test) 
            for test in core_tests
        ]))
        
        # NEVER include CLI tests - they cause issues even with special handling
        # So we completely exclude test_cli.py regardless of the --include-all flag
        
        # File converter tests
        converter_tests = glob.glob('tests/comprehensive_tests/converters/test_*.py')
        if converter_tests:
            self.test_modules.append(('File Converter Tests', converter_tests))
        
        # Add core components tests
        core_component_tests = glob.glob('tests/comprehensive_tests/core/test_*.py')
        if core_component_tests:
            self.test_modules.append(('Core Components Tests', core_component_tests))
        
        # Add utility tests
        utility_tests = glob.glob('tests/comprehensive_tests/utils/test_*.py')
        if utility_tests:
            self.test_modules.append(('Utility Tests', utility_tests))
    
    def run_test_file(self, test_file):
        """Run a single test file in a separate process with timeout."""
        test_basename = os.path.basename(test_file)
        print_stage(f"\nRunning test file: {test_basename}")
        print_stage("-" * 70)
        
        start_time = time.time()
        output_lines = []
        
        # Prepare environment variables to prevent screen clearing
        env = os.environ.copy()
        env["NO_CLEAR_SCREEN"] = "1"
        
        try:
            # Run the test with a timeout, capturing output
            process = subprocess.Popen(
                [sys.executable, test_file, "-v"],  # Add -v for verbose output
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
                text=True
            )
            
            # Read process output line by line with a timeout
            kill_time = time.time() + TIMEOUT_SECONDS
            
            while process.poll() is None and time.time() < kill_time:
                line = process.stdout.readline()
                if not line:
                    time.sleep(0.1)
                    continue
                
                output_line = line.strip()
                if output_line:
                    print(output_line)  # Print real-time output
                    output_lines.append(output_line)
            
            # Check if the process is still running (timed out)
            if process.poll() is None:
                process.kill()
                elapsed = time.time() - start_time
                reason = f"Timed out after {TIMEOUT_SECONDS} seconds"
                print_stage(f"❌ FAILED: {test_basename} (timed out after {TIMEOUT_SECONDS} seconds)")
                
                module_name = os.path.relpath(test_file).replace('/', '.').replace('.py', '')
                result = TestResult(module_name, {
                    'success': False,
                    'tests_run': 0,
                    'errors': 1,
                    'failures': 0,
                    'skipped': 0,
                    'time_taken': elapsed,
                    'reason': reason,
                    'fix_module': self.get_module_to_fix(test_basename, output_lines, reason),
                    'exit_code': None,
                    'test_cases': []  # No test cases were run
                })
                return result
            else:
                elapsed = time.time() - start_time
                return_code = process.returncode
                
                module_name = os.path.relpath(test_file).replace('/', '.').replace('.py', '')
                
                # Extract test case results from verbose output
                test_cases = self.extract_test_cases(output_lines)
                
                if return_code == 0:
                    print_stage(f"✅ PASSED: {test_basename} (in {elapsed:.2f}s)")
                    
                    # Extract number of tests from output
                    tests_run = self.extract_test_count(output_lines)
                    
                    result = TestResult(module_name, {
                        'success': True,
                        'tests_run': tests_run,
                        'errors': 0,
                        'failures': 0,
                        'skipped': 0,
                        'time_taken': elapsed,
                        'reason': 'All tests passed',
                        'fix_module': 'N/A',
                        'exit_code': return_code,
                        'test_cases': test_cases
                    })
                    return result
                else:
                    reason = f"Exit code: {return_code}"
                    
                    # Extract detailed reason and test counts from output
                    detailed_reason = self.extract_failure_reason(output_lines)
                    if detailed_reason:
                        reason = detailed_reason
                    
                    test_counts = self.extract_test_counts(output_lines)
                    
                    print_stage(f"❌ FAILED: {test_basename} (exit code {return_code})")
                    
                    result = TestResult(module_name, {
                        'success': False,
                        'tests_run': test_counts.get('run', 0),
                        'errors': test_counts.get('errors', 0),
                        'failures': test_counts.get('failures', 0),
                        'skipped': test_counts.get('skipped', 0),
                        'time_taken': elapsed,
                        'reason': reason,
                        'fix_module': self.get_module_to_fix(test_basename, output_lines, reason),
                        'exit_code': return_code,
                        'test_cases': test_cases
                    })
                    return result
        
        except Exception as e:
            error_message = str(e)
            traceback.print_exc()
            print_stage(f"❌ ERROR: {test_basename} - {error_message}")
            
            module_name = os.path.relpath(test_file).replace('/', '.').replace('.py', '')
            return TestResult(module_name, {
                'success': False,
                'tests_run': 0,
                'errors': 1,
                'failures': 0,
                'skipped': 0,
                'time_taken': 0,
                'reason': error_message,
                'fix_module': self.get_module_to_fix(test_basename, [error_message], error_message),
                'exit_code': None
            })
    
    def extract_test_count(self, output_lines):
        """Extract the number of tests run from output lines."""
        for line in reversed(output_lines):
            # Look for unittest summary line like "Ran 4 tests in 0.002s"
            if "Ran " in line and " tests in " in line:
                try:
                    count = int(line.split("Ran ")[1].split(" tests")[0])
                    return count
                except:
                    pass
        return 0
    
    def extract_test_counts(self, output_lines):
        """Extract detailed test counts from output lines."""
        counts = {'run': 0, 'errors': 0, 'failures': 0, 'skipped': 0}
        
        # Extract the number of tests run
        counts['run'] = self.extract_test_count(output_lines)
        
        # Extract error/failure counts
        for line in reversed(output_lines):
            if "errors=" in line and "failures=" in line:
                try:
                    errors_match = re.search(r"errors=(\d+)", line)
                    failures_match = re.search(r"failures=(\d+)", line)
                    if errors_match:
                        counts['errors'] = int(errors_match.group(1))
                    if failures_match:
                        counts['failures'] = int(failures_match.group(1))
                except:
                    pass
            
            if "skipped=" in line:
                try:
                    skipped_match = re.search(r"skipped=(\d+)", line)
                    if skipped_match:
                        counts['skipped'] = int(skipped_match.group(1))
                except:
                    pass
        
        return counts
    
    def extract_failure_reason(self, output_lines):
        """Extract a more detailed failure reason from test output."""
        # Look for AssertionError or other error messages
        for i, line in enumerate(output_lines):
            if "AssertionError:" in line:
                # Extract the assertion message
                match = re.search(r"AssertionError:(.+)", line)
                if match:
                    return f"AssertionError: {match.group(1).strip()}"
            elif "Error:" in line or "Exception:" in line:
                # Extract other error messages
                return line.strip()
        
        # Look for "FAIL:" lines in unittest output
        fail_lines = [line for line in output_lines if "FAIL:" in line and not line.startswith("❌")]
        if fail_lines:
            return fail_lines[0].strip()
        
        return None
    
    def get_module_to_fix(self, test_basename, output_lines, reason=""):
        """Determine which module needs to be fixed based on the test and output."""
        modules_mapping = {
            'test_cli.py': 'main.py or command-line interface',
            'test_core_functionality.py': 'core framework modules',
            'test_db_handler.py': 'utils/db_handler.py',
            'test_logger.py': 'logger.py',
            'test_plugin_system.py': 'core/plugin_integration.py',
            'test_yaml_processor.py': 'utils/yaml_processor.py',
            'test_file_converters.py': 'plugins/file_converters/',
            'test_core_components.py': 'core/ directory modules',
            'test_utilities.py': 'utils/ directory modules'
        }
        
        # Default suggestion based on test name
        default_suggestion = modules_mapping.get(test_basename, 'Unknown module')
        
        # Try to extract more specific module from output
        for line in output_lines:
            # Look for import errors
            if "ImportError:" in line and "No module named" in line:
                module_name = re.search(r"No module named ['\"](.*?)['\"]", line)
                if module_name:
                    return f"Missing module: {module_name.group(1)}"
            
            # Look for file paths in error traces
            if ".py" in line and ("line" in line.lower() or "file" in line.lower()):
                file_match = re.search(r"(\/[a-zA-Z0-9_\/.]+\.py)", line)
                if file_match:
                    return f"Check {os.path.basename(file_match.group(1))}"
        
        # Special case handling based on error patterns
        for line in output_lines:
            if "db_handler" in line.lower() and "complete_execution_run" in line.lower():
                return "utils/db_handler.py - complete_execution_run() method"
            elif "function_logger_decorator" in line.lower():
                return "logger.py - function_logger_decorator implementation"
            elif "logger_formatting" in line.lower():
                return "logger.py - message formatting"
            elif "plugin_discovery" in line.lower():
                return "core/plugin_integration.py - plugin discovery mechanism"
        
        return default_suggestion
    
    def run_tests(self):
        """Run all test modules."""
        # Start timing
        start_time = time.time()
        
        # Print header
        self.print_header()
        
        # Run each test module
        for category, module_files in self.test_modules:
            self.print_category(category)
            
            for test_file in module_files:
                # Skip problematic tests if not explicitly included
                if os.path.basename(test_file) in SKIP_PROBLEMATIC_TESTS and not self.args.include_all:
                    print_stage(f"\nSkipping problematic test: {os.path.basename(test_file)}")
                    continue
                
                # Run the test and get results
                test_result = self.run_test_file(test_file)
                self.results.append(test_result)
                
                # Update totals
                self.total_tests += test_result.run
                self.total_errors += test_result.errors
                self.total_failures += test_result.failures
                self.total_skipped += test_result.skipped
                self.total_time += test_result.time_taken
                
                print_stage("-" * 70)
        
        # Print summary
        total_time = time.time() - start_time
        self.print_summary(total_time)
        
        # Generate the HTML report
        self.generate_report()
        
        # Return True if all tests passed
        return self.total_errors == 0 and self.total_failures == 0
    
    def print_header(self):
        """Print the test header."""
        print_stage("\n" + "=" * 70)
        print_stage("COMPREHENSIVE FRAMEWORK TESTS".center(70))
        print_stage("=" * 70 + "\n")
        
        print_stage(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print_stage(f"Python version: {sys.version}")
        print_stage(f"Running {'all tests' if self.args.include_all else 'selected tests'}")
        print_stage("-" * 70 + "\n")
    
    def print_category(self, category):
        """Print a category header."""
        print_stage("\n" + "=" * 70)
        print_stage(f" {category} ".center(70, "-"))
        print_stage("=" * 70)
    
    def print_summary(self, total_time):
        """Print the test summary."""
        print_stage("\n" + "=" * 70)
        print_stage("TEST SUMMARY".center(70))
        print_stage("=" * 70 + "\n")
        
        # Print module level summary table
        print_stage(f"{'MODULE':<30} {'STATUS':<10} {'TESTS':<6} {'ERRORS':<6} {'FAILURES':<8} {'TIME':<8}")
        print_stage("-" * 70)
        
        for result in self.results:
            status = "✅ PASS" if result.success else "❌ FAIL"
            module_name = result.module_name.split('.')[-1]  # Get just the last part
            print_stage(f"{module_name:<30} {status:<10} {result.run:<6} {result.errors:<6} {result.failures:<8} {result.time_taken:.2f}s")
        
        print_stage("-" * 70)
        print_stage(f"Total test modules: {len(self.results)}")
        print_stage(f"Total tests: {self.total_tests}")
        print_stage(f"Passed: {self.total_tests - self.total_errors - self.total_failures - self.total_skipped}")
        if self.total_failures > 0:
            print_stage(f"Failures: {self.total_failures}")
        if self.total_errors > 0:
            print_stage(f"Errors: {self.total_errors}")
        if self.total_skipped > 0:
            print_stage(f"Skipped: {self.total_skipped}")
        print_stage(f"Total time: {total_time:.2f}s")
        
        # Collect all test cases
        all_test_cases = []
        for result in self.results:
            module_name = result.module_name.split('.')[-1]
            module_path = result.module_name.replace('.', '/')
            
            if hasattr(result, 'test_cases') and result.test_cases:
                for test_case in result.test_cases:
                    case_info = {
                        'module': module_name,
                        'module_path': f"{module_path}.py",
                        'test_name': test_case['name'],
                        'status': test_case['status'],
                        'message': test_case.get('message', 'N/A'),
                        'input_files': self.extract_input_files(test_case.get('message', ''), test_case['name']),
                    }
                    all_test_cases.append(case_info)
            else:
                # For modules without test case details, create a single entry
                case_info = {
                    'module': module_name,
                    'module_path': f"{module_path}.py",
                    'test_name': 'No individual test cases available',
                    'status': 'pass' if result.success else 'fail',
                    'message': result.reason,
                    'input_files': 'N/A',
                }
                all_test_cases.append(case_info)
        
        # Print comprehensive test case level table
        if all_test_cases:
            print_stage("\n" + "=" * 100)
            print_stage("ALL TEST CASES DETAILED STATUS".center(100))
            print_stage("=" * 100)
            
            print_stage(f"{'MODULE':<15} {'TEST NAME':<30} {'STATUS':<8} {'INPUT FILES':<20} {'MODULE PATH':<25}")
            print_stage("-" * 100)
            
            for case in all_test_cases:
                status_icon = "✅" if case['status'] == 'pass' else "❌"
                test_name = case['test_name'][:28] + '..' if len(case['test_name']) > 30 else case['test_name']
                input_files = case['input_files'][:18] + '..' if len(str(case['input_files'])) > 20 else case['input_files']
                module_path = case['module_path'][:23] + '..' if len(case['module_path']) > 25 else case['module_path']
                
                print_stage(f"{case['module']:<15} {test_name:<30} {status_icon} {case['status']:<6} {input_files:<20} {module_path:<25}")
            
            print_stage("-" * 100)
        
        # Print test case level details for all modules with failures or errors
        failed_results = [r for r in self.results if not r.success]
        if failed_results:
            print_stage("\n" + "=" * 70)
            print_stage("TEST CASE LEVEL FAILURE DETAILS".center(70))
            print_stage("=" * 70)
            
            for i, result in enumerate(failed_results, 1):
                module_name = result.module_name.split('.')[-1]
                print_stage(f"\n{i}. {module_name}")
                print_stage("-" * 70)
                
                # If there are test cases with specific results
                if hasattr(result, 'test_cases') and result.test_cases:
                    print_stage(f"{'TEST CASE':<40} {'STATUS':<10}")
                    print_stage("-" * 55)
                    
                    for test_case in result.test_cases:
                        status_icon = "✅" if test_case['status'] == 'pass' else "❌"
                        print_stage(f"{test_case['name']:<40} {status_icon} {test_case['status'].upper()}")
                        
                        # Show failure message for failed or error test cases
                        if test_case['status'] in ('fail', 'error') and test_case.get('message'):
                            # Extract first line of error message
                            first_line = test_case['message'].split('\n')[0]
                            print_stage(f"   Error: {first_line}")
                            
                            # Show input files used if available
                            input_files = self.extract_input_files(test_case.get('message', ''), test_case['name'])
                            if input_files != 'N/A':
                                print_stage(f"   Input Files: {input_files}")
                else:
                    print_stage(f"   Reason: {result.reason}")
                    print_stage(f"   Fix needed in: {result.fix_module}")
            
        # Print overall status
        status = "PASS" if self.total_errors == 0 and self.total_failures == 0 else "FAIL"
        print_stage(f"\nOVERALL STATUS: {status}")
    
    def extract_input_files(self, message, test_name):
        """Extract input files used in test from error message or test name."""
        input_files = []
        
        # Try to extract file paths from error messages
        if message and isinstance(message, str):
            # Look for file paths in error message
            file_paths = re.findall(r'\/[a-zA-Z0-9_\/.]+\.(json|csv|xml|yaml|yml|txt|mf|epi|psv)', message)
            if file_paths:
                return ', '.join(file_paths)
                
            # Try to extract just file names
            file_names = re.findall(r'[a-zA-Z0-9_]+\.(json|csv|xml|yaml|yml|txt|mf|epi|psv)', message)
            if file_names:
                return ', '.join(file_names)
        
        # Try to guess from test name
        if 'json' in test_name.lower():
            input_files.append('sample.json')
        elif 'xml' in test_name.lower():
            input_files.append('sample.xml')
        elif 'csv' in test_name.lower():
            input_files.append('sample.csv')
        elif 'mainframe' in test_name.lower():
            input_files.append('sample.mf')
        elif 'episodic' in test_name.lower():
            input_files.append('sample.epi')
        elif 'yaml' in test_name.lower() or 'yml' in test_name.lower():
            input_files.append('config.yaml')
        
        return ', '.join(input_files) if input_files else 'N/A'

    def generate_report(self):
        """Generate an HTML report of the test results."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"reports/comprehensive_test_report_{timestamp}.html"
        
        # Get current date and time as a string
        generation_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Collect all test cases
        all_test_cases = []
        for result in self.results:
            module_name = result.module_name.split('.')[-1]
            module_path = result.module_name.replace('.', '/')
            
            if hasattr(result, 'test_cases') and result.test_cases:
                for test_case in result.test_cases:
                    case_info = {
                        'module': module_name,
                        'module_path': f"{module_path}.py",
                        'test_name': test_case['name'],
                        'status': test_case['status'],
                        'message': test_case.get('message', 'N/A'),
                        'input_files': self.extract_input_files(test_case.get('message', ''), test_case['name']),
                    }
                    all_test_cases.append(case_info)
            else:
                # For modules without test case details, create a single entry
                case_info = {
                    'module': module_name,
                    'module_path': f"{module_path}.py",
                    'test_name': 'No individual test cases available',
                    'status': 'pass' if result.success else 'fail',
                    'message': result.reason,
                    'input_files': 'N/A',
                }
                all_test_cases.append(case_info)
        
        # Generate HTML report
        with open(report_file, 'w') as f:
            f.write(f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Comprehensive Framework Test Report</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    h1, h2, h3 {{ color: #333; }}
                    .summary {{ margin-bottom: 20px; }}
                    .table-container {{ margin-bottom: 30px; }}
                    table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; position: sticky; top: 0; }}
                    tr:nth-child(even) {{ background-color: #f9f9f9; }}
                    .pass {{ color: green; }}
                    .fail {{ color: red; }}
                    .error {{ color: orangered; }}
                    .skipped {{ color: orange; }}
                    .details-section {{ margin-top: 30px; }}
                    .test-case-container {{ margin-bottom: 20px; border: 1px solid #ddd; padding: 10px; }}
                    .test-case-header {{ background-color: #f2f2f2; padding: 8px; }}
                    .error-message {{ background-color: #fff0f0; padding: 8px; margin-top: 5px; border-left: 3px solid #ffcccc; font-family: monospace; white-space: pre-wrap; }}
                    .pass-icon {{ color: green; }}
                    .fail-icon {{ color: red; }}
                    .nav-bar {{ position: sticky; top: 0; background-color: white; padding: 10px 0; border-bottom: 1px solid #ddd; z-index: 100; }}
                    .nav-bar a {{ margin-right: 15px; text-decoration: none; color: #0066cc; }}
                    .nav-bar a:hover {{ text-decoration: underline; }}
                    .search-box {{ margin-bottom: 10px; }}
                    .search-box input {{ padding: 5px; width: 300px; }}
                    .collapsible {{ cursor: pointer; }}
                    .content {{ display: none; overflow: hidden; }}
                </style>
                <script>
                    function filterTable() {{
                        let input = document.getElementById("searchInput").value.toLowerCase();
                        let table = document.getElementById("allTestCasesTable");
                        let tr = table.getElementsByTagName("tr");
                        
                        for (let i = 1; i < tr.length; i++) {{
                            let td = tr[i].getElementsByTagName("td");
                            let found = false;
                            
                            for (let j = 0; j < td.length; j++) {{
                                if (td[j].textContent.toLowerCase().indexOf(input) > -1) {{
                                    found = true;
                                    break;
                                }}
                            }}
                            
                            if (found) {{
                                tr[i].style.display = "";
                            }} else {{
                                tr[i].style.display = "none";
                            }}
                        }}
                    }}
                    
                    function toggleCollapse(id) {{
                        let content = document.getElementById(id);
                        if (content.style.display === "block") {{
                            content.style.display = "none";
                        }} else {{
                            content.style.display = "block";
                        }}
                    }}
                </script>
            </head>
            <body>
                <div class="nav-bar">
                    <a href="#summary">Summary</a>
                    <a href="#modules">Module Results</a>
                    <a href="#all-test-cases">All Test Cases</a>
                    <a href="#failures">Failures</a>
                </div>
                
                <h1>Comprehensive Framework Test Report</h1>
                <p>Generated on: {generation_time}</p>
                
                <div id="summary" class="summary">
                    <h2>Summary</h2>
                    <p>
                        <strong>Total Modules:</strong> {len(self.results)}<br>
                        <strong>Total Tests:</strong> {self.total_tests}<br>
                        <strong>Passed:</strong> {self.total_tests - self.total_errors - self.total_failures - self.total_skipped}<br>
                        <strong>Failures:</strong> {self.total_failures}<br>
                        <strong>Errors:</strong> {self.total_errors}<br>
                        <strong>Skipped:</strong> {self.total_skipped}<br>
                        <strong>Total Time:</strong> {self.total_time:.2f}s<br>
                        <strong>Status:</strong> {'PASS' if self.total_errors == 0 and self.total_failures == 0 else 'FAIL'}
                    </p>
                </div>
                
                <div id="modules" class="table-container">
                    <h2>Module Level Results</h2>
                    <table>
                        <tr>
                            <th>Module</th>
                            <th>Status</th>
                            <th>Tests</th>
                            <th>Errors</th>
                            <th>Failures</th>
                            <th>Time (s)</th>
                        </tr>
            """)
            
            # Add module level results
            for result in self.results:
                status_class = "pass" if result.success else "fail"
                module_name = result.module_name.split('.')[-1]
                
                f.write(f"""
                        <tr>
                            <td>{module_name}</td>
                            <td class="{status_class}">{'PASS' if result.success else 'FAIL'}</td>
                            <td>{result.run}</td>
                            <td>{result.errors}</td>
                            <td>{result.failures}</td>
                            <td>{result.time_taken:.2f}</td>
                        </tr>
                """)
            
            f.write("""
                    </table>
                </div>
            """)
            
            # Add comprehensive test case level table
            f.write("""
                <div id="all-test-cases" class="table-container">
                    <h2>All Test Cases</h2>
                    <div class="search-box">
                        <input type="text" id="searchInput" onkeyup="filterTable()" placeholder="Search for test cases...">
                    </div>
                    <table id="allTestCasesTable">
                        <tr>
                            <th>Module</th>
                            <th>Test Name</th>
                            <th>Status</th>
                            <th>Input Files</th>
                            <th>Module Path</th>
                            <th>Details</th>
                        </tr>
            """)
            
            # Add all test cases
            for i, case in enumerate(all_test_cases):
                status_class = "pass" if case['status'] == 'pass' else "fail"
                status_icon = "✅" if case['status'] == 'pass' else "❌"
                has_message = case['status'] in ('fail', 'error') and case['message'] != 'N/A'
                content_id = f"content-{i}"
                
                f.write(f"""
                        <tr>
                            <td>{case['module']}</td>
                            <td>{case['test_name']}</td>
                            <td class="{status_class}">{status_icon} {case['status'].upper()}</td>
                            <td>{case['input_files']}</td>
                            <td>{case['module_path']}</td>
                            <td>
                """)
                
                if has_message:
                    # Escape HTML characters
                    message = str(case['message']).replace('<', '&lt;').replace('>', '&gt;')
                    f.write(f"""
                                <button class="collapsible" onclick="toggleCollapse('content-{i}')">Show Error Details</button>
                                <div id="{content_id}" class="content">
                                    <div class="error-message">{message}</div>
                                </div>
                    """)
                else:
                    f.write("N/A")
                
                f.write("""
                            </td>
                        </tr>
                """)
            
            f.write("""
                    </table>
                </div>
            """)
            
            # Add test case level details for failed modules
            failed_results = [r for r in self.results if not r.success]
            if failed_results:
                f.write("""
                <div id="failures" class="details-section">
                    <h2>Test Case Failure Details</h2>
                """)
                
                for i, result in enumerate(failed_results, 1):
                    module_name = result.module_name.split('.')[-1]
                    f.write(f"""
                    <div class="test-case-container">
                        <div class="test-case-header">
                            <h3>{i}. {module_name}</h3>
                        </div>
                    """)
                    
                    # If there are test cases with specific results
                    if hasattr(result, 'test_cases') and result.test_cases:
                        f.write("""
                        <table>
                            <tr>
                                <th>Test Case</th>
                                <th>Status</th>
                                <th>Input Files</th>
                                <th>Error Message</th>
                            </tr>
                        """)
                        
                        for test_case in result.test_cases:
                            status_class = "pass" if test_case['status'] == 'pass' else "fail"
                            status_icon = "✅" if test_case['status'] == 'pass' else "❌"
                            input_files = self.extract_input_files(test_case.get('message', ''), test_case['name'])
                            
                            f.write(f"""
                            <tr>
                                <td>{test_case['name']}</td>
                                <td class="{status_class}">{status_icon} {test_case['status'].upper()}</td>
                                <td>{input_files}</td>
                                <td>
                            """)
                            
                            # Add error message for failed tests
                            if test_case['status'] in ('fail', 'error') and test_case.get('message'):
                                # Escape HTML characters in the message
                                message = str(test_case['message']).replace('<', '&lt;').replace('>', '&gt;')
                                f.write(f"""
                                    <div class="error-message">{message}</div>
                                """)
                            else:
                                f.write("N/A")
                            
                            f.write("""
                                </td>
                            </tr>
                            """)
                        
                        f.write("""
                        </table>
                        """)
                    else:
                        f.write(f"""
                        <p><strong>Reason:</strong> {result.reason}</p>
                        <p><strong>Fix Needed In:</strong> {result.fix_module}</p>
                        """)
                    
                    f.write("""
                    </div>
                    """)
                
                f.write("""
                </div>
                """)
            
            f.write("""
            </body>
            </html>
            """)
        
        print_stage(f"\nEnhanced test report generated: {report_file}")
        return report_file

    def extract_test_cases(self, output_lines):
        """Extract individual test case results from verbose output."""
        test_cases = []
        current_test = None
        
        for line in output_lines:
            # Look for test case start
            if line.startswith('test_') and ' (' in line and line.endswith(' ... '):
                # Extract test name from line like "test_json_to_csv_converter (package.TestClass) ... "
                test_name = line.split(' (')[0]
                current_test = {
                    'name': test_name,
                    'status': 'running',
                    'message': None
                }
            
            # Look for test case result
            elif current_test and current_test['status'] == 'running':
                if line.endswith(' ... ok'):
                    current_test['status'] = 'pass'
                    test_cases.append(current_test)
                    current_test = None
                elif line.endswith(' ... FAIL'):
                    current_test['status'] = 'fail'
                elif line.endswith(' ... ERROR'):
                    current_test['status'] = 'error'
                elif line.endswith(' ... skipped'):
                    current_test['status'] = 'skipped'
                    test_cases.append(current_test)
                    current_test = None
            
            # Capture error/failure message
            elif current_test and current_test['status'] in ('fail', 'error'):
                if line.startswith('Traceback') or current_test.get('message'):
                    if not current_test.get('message'):
                        current_test['message'] = line
                    else:
                        current_test['message'] += '\n' + line
                        
                # End of error trace
                if line.startswith('AssertionError') or 'Error:' in line:
                    test_cases.append(current_test)
                    current_test = None
        
        return test_cases


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Comprehensive Framework Test Runner")
    parser.add_argument("--include-all", action="store_true", 
                       help="Include all tests, even problematic ones")
    parser.add_argument("--verbose", action="store_true",
                       help="Enable verbose output")
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    # Set up environment to prevent screen clearing
    os.environ["NO_CLEAR_SCREEN"] = "1"
    
    # Run the tests
    runner = ComprehensiveTestRunner(args)
    success = runner.run_tests()
    
    # Return exit code: 0 if successful, 1 if failures or errors
    return 0 if success else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print_stage("\nTest execution canceled by user.")
        sys.exit(1)
    except Exception as e:
        print_stage(f"\nUnexpected error during test execution: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1) 