#!/usr/bin/env python3
"""
Main Driver Script for Automation Framework
This script serves as the central entry point for all framework functionalities.
It provides both command-line and interactive interfaces to access all major features.
"""

import os
import sys
import argparse
from datetime import datetime
import subprocess
from typing import Optional, List, Dict
import yaml

# Import framework utilities
from utils.banner import print_intro
from utils.logger import logger

def clear_screen():
    """Clear the terminal screen."""
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header(title: str = "AUTOMATION FRAMEWORK"):
    """Print a formatted header."""
    clear_screen()
    print("=" * 60)
    print(f"{title:^60}")
    print("=" * 60)
    print()

def run_command(command: str, description: str) -> int:
    """Run a command and handle its output."""
    print(f"\n{description}...")
    try:
        result = subprocess.run(command.split(), check=True)
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
        return e.returncode
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1

class AutomationFramework:
    def __init__(self):
        self.env = "QA"  # Default environment

    def show_option_banner(self, title: str, description: str = ""):
        """Display a banner for the selected option."""
        clear_screen()
        print("=" * 60)
        print(f"{title:^60}")
        print("=" * 60)
        if description:
            print(f"\n{description}\n")
        print("-" * 60)
        print()

    def run_tests(self, test_file: Optional[str] = None, scheduled: bool = False,
                pattern: Optional[str] = None, parallel: Optional[int] = None) -> int:
        """Run test cases."""
        cmd = ["python3", "bin/run_test_cases.py"]
        if test_file:
            cmd.append(test_file)
        if scheduled:
            cmd.append("--scheduled")
        if pattern:
            cmd.extend(["--pattern", pattern])
        if parallel:
            cmd.extend(["--parallel", str(parallel)])
        
        return run_command(" ".join(cmd), "Running test cases")

    def run_visual_tests(self, test_file: Optional[str] = None, debug: bool = False) -> int:
        """Run tests with visual progress display."""
        cmd = ["python3", "bin/run_tests_visual.py"]
        if test_file:
            cmd.extend(["--test-file", test_file])
        if debug:
            cmd.append("--debug")
        
        return run_command(" ".join(cmd), "Running visual tests")

    def run_scheduler_operations(self, operation: str, **kwargs) -> int:
        """Handle scheduler operations."""
        if operation == "list":
            # Create a new script to list scheduled tests
            return self._list_scheduled_tests()
        
        cmd = ["python3", "bin/run_scheduler.py"]
        if operation == "add":
            cmd.extend(["--add"])
            if kwargs.get("traceability_id"):
                cmd.extend(["--traceability_id", kwargs["traceability_id"]])
            if kwargs.get("interval"):
                cmd.extend(["--interval", kwargs["interval"]])
            if kwargs.get("time"):
                cmd.extend(["--time", kwargs["time"]])
        elif operation == "run":
            cmd.extend(["--run-now"])
        elif operation == "remove":
            if kwargs.get("schedule_id"):
                cmd.extend(["--remove", kwargs["schedule_id"]])
        
        return run_command(" ".join(cmd), f"Performing scheduler {operation} operation")

    def run_plugin_operations(self, operation: str, **kwargs) -> int:
        """Handle plugin operations."""
        try:
            if operation == "list":
                from helpers.framework_help import FrameworkHelp
                helper = FrameworkHelp()
                helper.list_plugins()
                return 0
            elif operation == "run":
                from core.plugin_integration import run_plugins_from_yaml
                if not kwargs.get("config"):
                    print("Error: Plugin configuration file is required")
                    return 1
                return run_command(f"python3 bin/run_file_converter.py --config {kwargs['config']}", 
                                 "Running plugins")
            elif operation == "create":
                return self._create_plugin_template(kwargs.get("name", "new_plugin"))
            return 1
        except Exception as e:
            print(f"Error in plugin operation: {str(e)}")
            return 1

    def _create_plugin_template(self, plugin_name: str) -> int:
        """Create a new plugin template."""
        if not plugin_name.endswith('.py'):
            plugin_name += '.py'
        
        plugin_path = os.path.join("plugins", plugin_name)
        
        if os.path.exists(plugin_path):
            print(f"Error: Plugin {plugin_name} already exists")
            return 1
        
        template = '''"""
{plugin_name} Plugin

This plugin performs custom data transformations.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame

def run(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Plugin entry point.
    
    Args:
        context: Dictionary containing:
            - params: Configuration parameters
            - source_df: Source DataFrame
            - target_df: Target DataFrame
            
    Returns:
        Dictionary containing:
            - transformed_df: Transformed DataFrame
            - metadata: Transformation metadata
    """
    # Extract parameters from context
    params = context.get('params', {})
    source_df = params.get('source_df')
    
    # Validate inputs
    if not source_df or not isinstance(source_df, DataFrame):
        raise ValueError("source_df parameter must be provided and must be a PySpark DataFrame")
    
    # TODO: Implement your transformation logic here
    transformed_df = source_df
    
    # Return the transformed DataFrame and metadata
    return {{
        'transformed_df': transformed_df,
        'metadata': {{
            'plugin_name': '{name}',
            'records_processed': transformed_df.count()
        }}
    }}
'''.format(plugin_name=plugin_name, name=plugin_name[:-3])
        
        try:
            os.makedirs("plugins", exist_ok=True)
            with open(plugin_path, 'w') as f:
                f.write(template)
            print(f"\nCreated plugin template: {plugin_path}")
            print("Edit the file to implement your transformation logic.")
            return 0
        except Exception as e:
            print(f"Error creating plugin template: {str(e)}")
            return 1

    def _list_scheduled_tests(self) -> int:
        """List all scheduled tests from the database."""
        try:
            from utils.db_handler import DBHandler
            from datetime import datetime
            
            # Initialize database handler
            db_handler = DBHandler()
            
            try:
                cursor = db_handler.conn.cursor()
                
                # Get all scheduled tests
                cursor.execute('''
                SELECT s.schedule_id, s.traceability_id, s.test_case_name, 
                       s.frequency, s.next_run_time, s.enabled
                FROM scheduler s
                INNER JOIN (
                    SELECT test_case_name, sid, MAX(created_at) as latest_created
                    FROM scheduler
                    GROUP BY test_case_name, sid
                ) latest ON s.test_case_name = latest.test_case_name 
                        AND s.sid = latest.sid 
                        AND s.created_at = latest.latest_created
                ORDER BY s.next_run_time
                ''')
                
                results = cursor.fetchall()
                
                if not results:
                    print("\nNo scheduled tests found.")
                    return 0
                
                print("\nScheduled Tests:")
                print("-" * 100)
                print(f"{'ID':<8} {'Test ID':<15} {'Test Name':<30} {'Frequency':<10} {'Next Run':<20} {'Status'}")
                print("-" * 100)
                
                for row in results:
                    schedule_id, trace_id, name, freq, next_run, enabled = row
                    status = "Enabled" if enabled else "Disabled"
                    print(f"{schedule_id:<8} {trace_id:<15} {name[:30]:<30} {freq:<10} {next_run:<20} {status}")
                
                print("-" * 100)
                return 0
                
            finally:
                db_handler.close()
                
        except Exception as e:
            print(f"\nError listing scheduled tests: {str(e)}")
            return 1

    def run_file_converter(self, config: str, input_file: Optional[str] = None,
                        output_file: Optional[str] = None) -> int:
        """Run file converter operations."""
        cmd = ["python3", "bin/run_file_converter.py", "--config", config]
        if input_file:
            cmd.extend(["--input", input_file])
        if output_file:
            cmd.extend(["--output", output_file])
        
        return run_command(" ".join(cmd), "Running file converter")

    def run_all_tests(self) -> int:
        """Run all framework tests."""
        return run_command("python3 run_all_tests.py", "Running all tests")

    def clean_project(self) -> int:
        """Clean project artifacts."""
        return run_command("python3 clean.py", "Cleaning project")

    def show_help(self) -> int:
        """Show framework help."""
        return run_command("python3 helpers/framework_help.py", "Showing framework help")

    def run_internal_framework_tests(self, verbose=False, console_output=True) -> int:
        """Run internal framework tests from tests/ directory.
        
        Args:
            verbose: Enable verbose output
            console_output: Display output on console as tests run
        """
        # Use the direct test runner approach that avoids hanging issues
        # Exclude framework self-tests since they are covered by the other option
        return run_command(f"python3 -m unittest discover -v tests -k 'not framework_self_tests'", 
                         "Running internal framework tests")

    def run_framework_self_tests(self, verbose=False, console_output=True, skip_problematic=True) -> int:
        """Run core framework self-tests from tests/framework_self_tests/ directory.
        
        Args:
            verbose: Enable verbose output
            console_output: Display output on console and generate detailed summary
            skip_problematic: Skip tests known to cause issues (like test_cli.py)
        """
        # Use our enhanced test runner that shows detailed initialization progress
        # and provides a comprehensive test summary table
        cmd = ["python3", "run_framework_test.py"]
        
        # If running with all tests including problematic ones
        if not skip_problematic:
            cmd.append("--include-all")
            
        return run_command(" ".join(cmd), 
                         "Running framework self-tests with enhanced reporting")

    def run_comprehensive_tests(self, include_all=False, verbose=False) -> int:
        """Run comprehensive tests of all framework components.
        
        Args:
            include_all: Include all tests, even potentially problematic ones
            verbose: Enable verbose output
        """
        # Use the comprehensive test runner that tests all framework components
        cmd = ["python3", "tests/comprehensive_tests/run_comprehensive_tests.py"]
        
        # Add options
        if include_all:
            cmd.append("--include-all")
        if verbose:
            cmd.append("--verbose")
        
        # Set environment variables to control screen clearing during tests
        env = os.environ.copy()
        env["NO_CLEAR_SCREEN"] = "1"
        
        # Run with custom environment
        try:
            print(f"\nRunning comprehensive framework tests...")
            process = subprocess.run(cmd, env=env, check=True)
            return process.returncode
        except subprocess.CalledProcessError as e:
            print(f"Error executing command: {e}")
            return e.returncode
        except Exception as e:
            print(f"Unexpected error: {e}")
            return 1

    def interactive_menu(self):
        """Display interactive menu for framework operations."""
        while True:
            print_header()
            print("=== TEST EXECUTION ===")
            print("1.  Run Single Test Case")
            print("2.  Run Scheduled Test Case Now")
            print("3.  Run Visual Test Execution")
            print("4.  Run Tests in Parallel")
            print("\n=== SCHEDULER OPERATIONS ===")
            print("5.  Add Test to Scheduler")
            print("6.  Add Test to Scheduler via YAML")
            print("7.  List Scheduled Tests")
            print("8.  Remove Scheduled Test")
            print("9.  Run Scheduled Tests")
            print("\n=== PLUGIN OPERATIONS ===")
            print("10. List Available Plugins")
            print("11. Run Plugin")
            print("12. Create New Plugin")
            print("\n=== FILE OPERATIONS ===")
            print("13. Run File Converter")
            print("14. Clean Project Files")
            print("\n=== FRAMEWORK SELF-TESTING ===")
            print("15. Run Internal Framework Tests (Tests from 'tests/' directory)")
            print("16. Run Framework Self-Tests (Core functionality tests with detailed reporting)")
            print("17. Run Comprehensive Framework Tests (All components including converters)")
            print("\n=== FRAMEWORK SUPPORT ===")
            print("18. Show Framework Architecture")
            print("19. Show Flow Chart")
            print("20. Show Directory Structure")
            print("21. Show Format Versions")
            print("22. Show Examples")
            print("23. Show Configuration Examples")
            print("24. List Services")
            print("\n=== HELP & MAINTENANCE ===")
            print("25. Show Framework Help")
            print("26. Show Available Commands")
            print("27. Exit")
            
            try:
                choice = input("\nEnter your choice (1-27): ").strip()
                
                if choice == "1":
                    self.show_option_banner("RUN SINGLE TEST CASE", "Execute a specific test case by providing the test file path")
                    test_file = input("Enter test file path (or press Enter for all): ").strip()
                    self.run_tests(test_file)
                elif choice == "2":
                    self.show_option_banner("RUN SCHEDULED TEST CASE NOW", "Execute all scheduled test cases immediately")
                    self.run_scheduler_operations("run")
                elif choice == "3":
                    self.show_option_banner("RUN VISUAL TEST EXECUTION", "Execute tests with visual progress indicators")
                    test_file = input("Enter test file path (or press Enter for all): ").strip()
                    debug = input("Enable debug mode? (y/N): ").lower() == 'y'
                    self.run_visual_tests(test_file, debug)
                elif choice == "4":
                    self.show_option_banner("RUN TESTS IN PARALLEL", "Execute multiple tests simultaneously for faster results")
                    workers = input("Enter number of parallel workers (default 2): ").strip()
                    workers = int(workers) if workers.isdigit() else 2
                    self.run_tests(parallel=workers)
                elif choice == "5":
                    self.show_option_banner("ADD TEST TO SCHEDULER", "Schedule a test to run automatically at specified intervals")
                    traceability_id = input("Enter traceability ID: ").strip()
                    interval = input("Enter interval (daily/weekly/monthly): ").strip()
                    time = input("Enter time (HH:MM): ").strip()
                    self.run_scheduler_operations("add", traceability_id=traceability_id,
                                             interval=interval, time=time)
                elif choice == "6":
                    self.show_option_banner("ADD TEST TO SCHEDULER VIA YAML", "Schedule a test case by providing a YAML file")
                    yaml_file = input("Enter YAML file path: ").strip()
                    if not yaml_file:
                        print("YAML file path is required")
                        continue
                    
                    if not os.path.exists(yaml_file):
                        print(f"YAML file not found: {yaml_file}")
                        continue
                    
                    # Simply pass the YAML file to add_to_scheduler.py
                    # It will read the scheduler configuration from the YAML file
                    cmd = ["python3", "add_to_scheduler.py", yaml_file]
                    run_command(" ".join(cmd), "Adding test case to scheduler")
                elif choice == "7":
                    self.show_option_banner("LIST SCHEDULED TESTS", "Display all tests scheduled for automatic execution")
                    self.run_scheduler_operations("list")
                elif choice == "8":
                    self.show_option_banner("REMOVE SCHEDULED TEST", "Remove a test from the scheduler")
                    schedule_id = input("Enter schedule ID to remove: ").strip()
                    self.run_scheduler_operations("remove", schedule_id=schedule_id)
                elif choice == "9":
                    self.show_option_banner("RUN SCHEDULED TESTS", "Manually trigger execution of scheduled tests")
                    self.run_scheduler_operations("run")
                elif choice == "10":
                    self.show_option_banner("LIST AVAILABLE PLUGINS", "Display all available data transformation plugins")
                    self.run_plugin_operations("list")
                elif choice == "11":
                    self.show_option_banner("RUN PLUGIN", "Execute a specific plugin for data transformation")
                    config = input("Enter plugin configuration file: ").strip()
                    self.run_plugin_operations("run", config=config)
                elif choice == "12":
                    self.show_option_banner("CREATE NEW PLUGIN", "Create a new data transformation plugin template")
                    name = input("Enter plugin name (without .py): ").strip()
                    self.run_plugin_operations("create", name=name)
                elif choice == "13":
                    self.show_option_banner("RUN FILE CONVERTER", "Convert data between different file formats")
                    config = input("Enter converter configuration: ").strip()
                    input_file = input("Enter input file path (optional): ").strip()
                    output_file = input("Enter output file path (optional): ").strip()
                    self.run_file_converter(config, input_file or None, output_file or None)
                elif choice == "14":
                    self.show_option_banner("CLEAN PROJECT FILES", "Remove temporary files and clean project workspace")
                    self.clean_project()
                elif choice == "15":
                    self.show_option_banner("RUN INTERNAL FRAMEWORK TESTS", "Run tests that validate the framework's internal functionality")
                    verbose = input("Enable verbose output? (y/N): ").lower() == 'y'
                    self.run_internal_framework_tests(verbose, console_output=True)
                elif choice == "16":
                    self.show_option_banner("RUN FRAMEWORK SELF-TESTS", "Run core functionality tests with detailed reporting")
                    verbose = input("Enable verbose output? (y/N): ").lower() == 'y'
                    skip_problematic = input("Skip problematic tests? (Y/n): ").lower() != 'n'
                    self.run_framework_self_tests(verbose, console_output=True, skip_problematic=skip_problematic)
                elif choice == "17":
                    self.show_option_banner("RUN COMPREHENSIVE FRAMEWORK TESTS", "Run tests for all framework components including converters")
                    include_all = input("Include all tests (including potentially problematic ones)? (y/N): ").lower() == 'y'
                    verbose = input("Enable verbose output? (y/N): ").lower() == 'y'
                    self.run_comprehensive_tests(include_all=include_all, verbose=verbose)
                elif choice == "18":
                    self.show_option_banner("SHOW FRAMEWORK ARCHITECTURE", "Display the overall architecture diagram of the framework")
                    run_command("python3 helpers/framework_help.py --architecture", "Showing framework architecture")
                elif choice == "19":
                    self.show_option_banner("SHOW FLOW CHART", "Display the flow chart of test execution process")
                    run_command("python3 helpers/framework_help.py --flow", "Showing framework flow chart")
                elif choice == "20":
                    self.show_option_banner("SHOW DIRECTORY STRUCTURE", "Display the structure of project directories")
                    run_command("python3 helpers/framework_help.py --structure", "Showing directory structure")
                elif choice == "21":
                    self.show_option_banner("SHOW FORMAT VERSIONS", "Display supported file format versions")
                    run_command("python3 helpers/framework_help.py --formats", "Showing format versions")
                elif choice == "22":
                    self.show_option_banner("SHOW EXAMPLES", "Display usage examples for the framework")
                    run_command("python3 helpers/framework_help.py --examples", "Showing framework examples")
                elif choice == "23":
                    self.show_option_banner("SHOW CONFIGURATION EXAMPLES", "Display example configuration files")
                    run_command("python3 helpers/framework_help.py --configs", "Showing configuration examples")
                elif choice == "24":
                    self.show_option_banner("LIST SERVICES", "Display available services integrated with the framework")
                    run_command("python3 helpers/framework_help.py --services", "Listing available services")
                elif choice == "25":
                    self.show_option_banner("SHOW FRAMEWORK HELP", "Display comprehensive help information")
                    self.show_help()
                elif choice == "26":
                    self.show_option_banner("SHOW AVAILABLE COMMANDS", "Display all available command-line options")
                    run_command("python3 helpers/command_helper.py --list", "Showing available commands")
                elif choice == "27":
                    clear_screen()
                    print("\nExiting framework. Goodbye!")
                    sys.exit(0)
                else:
                    print("\nInvalid choice. Please enter a number between 1 and 27.")
                
                if choice != "27":
                    input("\nPress Enter to return to main menu...")
            
            except ValueError as e:
                print(f"\nError: Invalid input - {str(e)}")
                input("\nPress Enter to continue...")
            except Exception as e:
                print(f"\nUnexpected error: {str(e)}")
                input("\nPress Enter to continue...")

def main():
    """Main entry point for the framework."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Automation Framework Main Driver")
    
    # Make all arguments optional since interactive is the default mode
    group = parser.add_argument_group('Non-Interactive Mode Options')
    group.add_argument("--non-interactive", action="store_true", help="Run in non-interactive mode")
    group.add_argument("--run-test", help="Run specific test case file")
    group.add_argument("--run-all-tests", action="store_true", help="Run all tests")
    group.add_argument("--visual", action="store_true", help="Run tests with visual display")
    group.add_argument("--parallel", type=int, help="Run tests in parallel with N workers")
    group.add_argument("--schedule-add", action="store_true", help="Add test to scheduler")
    group.add_argument("--schedule-list", action="store_true", help="List scheduled tests")
    group.add_argument("--schedule-remove", help="Remove scheduled test by ID")
    group.add_argument("--convert", help="Run file converter with specified config")
    group.add_argument("--input", help="Input file for converter")
    group.add_argument("--output", help="Output file for converter")
    group.add_argument("--clean", action="store_true", help="Clean project")
    
    # Framework Self-Testing Options
    self_test_group = parser.add_argument_group('Framework Self-Testing Options')
    self_test_group.add_argument("--run-internal-tests", action="store_true", 
                              help="Run internal framework tests from tests/ directory")
    self_test_group.add_argument("--run-framework-self-tests", action="store_true", 
                               help="Run framework self-tests with detailed reporting")
    self_test_group.add_argument("--run-comprehensive-tests", action="store_true",
                              help="Run comprehensive tests for all framework components")
    self_test_group.add_argument("--include-all", action="store_true",
                              help="Include all tests in the test run, even problematic ones")
    self_test_group.add_argument("--verbose", action="store_true", 
                              help="Enable verbose output for test runs")
    
    # Framework Support Options
    support_group = parser.add_argument_group('Framework Support Options')
    support_group.add_argument("--architecture", action="store_true", help="Show framework architecture diagram")
    support_group.add_argument("--flow", action="store_true", help="Show framework flow chart")
    support_group.add_argument("--structure", action="store_true", help="Show directory structure")
    support_group.add_argument("--formats", action="store_true", help="Show supported format versions")
    support_group.add_argument("--examples", action="store_true", help="Show framework examples")
    support_group.add_argument("--configs", action="store_true", help="Show configuration examples")
    support_group.add_argument("--services", action="store_true", help="List available services")
    support_group.add_argument("--show-help", action="store_true", help="Show framework help")
    
    args = parser.parse_args()
    
    # Display the introductory banner
    print_intro("QA")
    
    framework = AutomationFramework()
    
    # Run in interactive mode by default unless --non-interactive is specified
    if not args.non_interactive and not any([
        args.run_test, args.run_all_tests, args.visual, args.parallel,
        args.schedule_add, args.schedule_list, args.schedule_remove,
        args.convert, args.clean, args.show_help, args.architecture,
        args.flow, args.structure, args.formats,
        args.examples, args.configs, args.services,
        args.run_internal_tests, args.run_framework_self_tests,
        args.run_comprehensive_tests
    ]):
        framework.interactive_menu()
        return 0
    
    # Handle command line arguments for non-interactive mode
    exit_code = 0
    
    if args.run_test:
        exit_code = framework.run_tests(args.run_test)
    elif args.run_all_tests:
        exit_code = framework.run_all_tests()
    elif args.visual:
        exit_code = framework.run_visual_tests(args.run_test)
    elif args.schedule_add:
        exit_code = framework.run_scheduler_operations("add")
    elif args.schedule_list:
        exit_code = framework.run_scheduler_operations("list")
    elif args.schedule_remove:
        exit_code = framework.run_scheduler_operations("remove", schedule_id=args.schedule_remove)
    elif args.convert:
        exit_code = framework.run_file_converter(args.convert, args.input, args.output)
    elif args.clean:
        exit_code = framework.clean_project()
    # Framework Self-Testing Options
    elif args.run_internal_tests:
        exit_code = framework.run_internal_framework_tests(args.verbose, console_output=True)
    elif args.run_framework_self_tests:
        exit_code = framework.run_framework_self_tests(args.verbose, console_output=True, skip_problematic=not args.include_all)
    elif args.run_comprehensive_tests:
        exit_code = framework.run_comprehensive_tests(include_all=args.include_all, verbose=args.verbose)
    # Framework Support Options
    elif args.architecture:
        exit_code = run_command("python3 helpers/framework_help.py --architecture", "Showing framework architecture")
    elif args.flow:
        exit_code = run_command("python3 helpers/framework_help.py --flow", "Showing framework flow chart")
    elif args.structure:
        exit_code = run_command("python3 helpers/framework_help.py --structure", "Showing directory structure")
    elif args.formats:
        exit_code = run_command("python3 helpers/framework_help.py --formats", "Showing format versions")
    elif args.examples:
        exit_code = run_command("python3 helpers/framework_help.py --examples", "Showing framework examples")
    elif args.configs:
        exit_code = run_command("python3 helpers/framework_help.py --configs", "Showing configuration examples")
    elif args.services:
        exit_code = run_command("python3 helpers/framework_help.py --services", "Listing available services")
    elif args.show_help:
        exit_code = framework.show_help()
    else:
        # If no specific command is provided in non-interactive mode, show help
        parser.print_help()
        exit_code = 1
    
    return exit_code

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nOperation cancelled by user. Exiting...")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1) 