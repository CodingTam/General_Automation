#!/usr/bin/env python3
"""
Command Helper

This module provides helper functions for executing various commands in the AutomationFramework.
It serves as a centralized interface for running tests, scheduling, file conversions, and more.
"""

import os
import sys
import argparse
import subprocess
import textwrap
from datetime import datetime
import uuid

def print_section(title):
    """Print a formatted section title."""
    width = 80
    print("\n" + "=" * width)
    print(f"{title.center(width)}")
    print("=" * width + "\n")

def print_command(command, description=None):
    """Print a formatted command with description."""
    if description:
        print(f"\033[1m{command}\033[0m")
        print(textwrap.fill(description, initial_indent="  ", subsequent_indent="  ", width=78))
        print()
    else:
        print(f"\033[1m{command}\033[0m\n")

def execute_command(command):
    """Execute a shell command and return the result."""
    try:
        result = subprocess.run(command, shell=True, check=True, 
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
                               universal_newlines=True)
        print(result.stdout)
        if result.stderr:
            print(f"WARNINGS/ERRORS:\n{result.stderr}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Command failed with exit code {e.returncode}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        return False

# ===== Scheduler Commands =====

def add_to_scheduler(traceability_id, interval, time, day=None, date=None, description=None, force=False):
    """Add a test to the scheduler."""
    command = f"python run_scheduler.py --add --traceability_id {traceability_id} --interval {interval} --time {time}"
    
    if interval.lower() == "weekly" and day:
        command += f" --day {day}"
    elif interval.lower() == "monthly" and date:
        command += f" --date {date}"
    
    if description:
        command += f" --description \"{description}\""
    
    if force:
        command += " --force"
    
    print_command(command, "Adding test to scheduler")
    return execute_command(command)

def add_yaml_to_scheduler(yaml_file):
    """Add a test to the scheduler directly from a YAML file."""
    command = f"python add_to_scheduler.py {yaml_file}"
    
    print_command(command, f"Adding test from YAML file to scheduler: {yaml_file}")
    return execute_command(command)

def list_scheduled_tests(traceability_id=None, interval=None, status=None):
    """List scheduled tests, optionally filtered."""
    command = "python run_scheduler.py --list"
    
    if traceability_id:
        command += f" --traceability_id {traceability_id}"
    
    if interval:
        command += f" --interval {interval}"
    
    if status:
        command += f" --status {status}"
    
    print_command(command, "Listing scheduled tests")
    return execute_command(command)

def remove_from_scheduler(traceability_id=None, schedule_id=None):
    """Remove a test from the scheduler."""
    command = "python run_scheduler.py --remove"
    
    if traceability_id:
        command += f" --traceability_id {traceability_id}"
    elif schedule_id:
        command += f" --schedule_id {schedule_id}"
    else:
        print("ERROR: Either traceability_id or schedule_id must be specified")
        return False
    
    print_command(command, "Removing test from scheduler")
    return execute_command(command)

def run_scheduled_tests(traceability_id=None, schedule_id=None, now=False):
    """Run scheduled tests."""
    command = "python run_scheduler.py --run"
    
    if traceability_id:
        command += f" --traceability_id {traceability_id}"
    
    if schedule_id:
        command += f" --schedule_id {schedule_id}"
    
    if now:
        command += " --now"
    
    print_command(command, "Running scheduled tests")
    return execute_command(command)

def run_scheduler_service(interval=300):
    """Run the scheduler service in continuous mode."""
    command = f"python run_schedule_test_cases.py --continuous --interval {interval}"
    
    print_command(command, f"Starting scheduler service (checking every {interval} seconds)")
    return execute_command(command)

# ===== Test Execution Commands =====

def run_single_test(yaml_file, traceability_id=None, execution_run_id=None):
    """Run a single test case."""
    command = f"python validation/Comparison.py --yaml-file {yaml_file} --single"
    
    if traceability_id:
        command += f" --traceability_id {traceability_id}"
    
    if execution_run_id:
        command += f" --execution_run_id {execution_run_id}"
    
    print_command(command, "Running single test case")
    return execute_command(command)

def run_batch_tests(test_dir="Testcases", pattern=None, execution_run_id=None):
    """Run tests in batch mode."""
    command = "python run_test_cases.py"
    
    if test_dir != "Testcases":
        command += f" --test-dir {test_dir}"
    
    if pattern:
        command += f" --pattern {pattern}"
    
    if execution_run_id:
        command += f" --execution_run_id {execution_run_id}"
    else:
        # Generate a UUID for the execution run
        run_id = str(uuid.uuid4())
        command += f" --execution_run_id {run_id}"
    
    print_command(command, "Running tests in batch mode")
    return execute_command(command)

def run_visual_tests(test_file=None, test_dir="Testcases", run_id=None, debug=False):
    """Run tests with visual display."""
    command = "python run_tests_visual.py"
    
    if test_file:
        command += f" --test-file {test_file}"
    
    if test_dir != "Testcases":
        command += f" --test-dir {test_dir}"
    
    if run_id:
        command += f" --run-id {run_id}"
    
    if debug:
        command += " --debug"
    
    print_command(command, "Running tests with visual display")
    return execute_command(command)

# ===== File Converter Commands =====

def convert_file(config, input_file, output_file):
    """Convert a file using a specific converter configuration."""
    command = f"python run_file_converter.py --config {config} --input {input_file} --output {output_file}"
    
    print_command(command, f"Converting file using {config} configuration")
    return execute_command(command)

def csv_to_psv(input_file, output_file):
    """Convert CSV to PSV format."""
    return convert_file("csv_to_psv_config", input_file, output_file)

def json_to_csv(input_file, output_file):
    """Convert JSON to CSV format."""
    return convert_file("json_to_csv_config", input_file, output_file)

def xml_to_csv(input_file, output_file):
    """Convert XML to CSV format."""
    return convert_file("xml_to_csv_config", input_file, output_file)

def mainframe_to_csv(input_file, output_file, copybook=None):
    """Convert mainframe/EBCDIC to CSV format."""
    command = f"python run_file_converter.py --config mainframe_to_csv_config --input {input_file} --output {output_file}"
    
    if copybook:
        command += f" --copybook {copybook}"
    
    print_command(command, "Converting mainframe/EBCDIC file to CSV format")
    return execute_command(command)

def episodic_to_csv(input_file, output_file):
    """Convert episodic data to CSV format."""
    return convert_file("episodic_to_csv_config", input_file, output_file)

# ===== Help & Documentation Commands =====

def show_framework_help(option=None):
    """Show framework help information."""
    command = "python framework_help.py"
    
    if option:
        command += f" --{option}"
    
    print_command(command, "Showing framework help information")
    return execute_command(command)

def show_plugins_list():
    """Show list of available plugins."""
    return show_framework_help("plugins")

def show_services_list():
    """Show list of available services."""
    return show_framework_help("services")

def show_examples():
    """Show usage examples."""
    return show_framework_help("examples")

def show_configs():
    """Show configuration examples."""
    return show_framework_help("configs")

def show_flow():
    """Show framework flow diagram."""
    return show_framework_help("flow")

# ===== Main Command Helper Function =====

def main():
    """Main function to provide interactive command selection."""
    parser = argparse.ArgumentParser(description="Command helper for the Automation Framework")
    parser.add_argument("--category", choices=["scheduler", "test", "converter", "help", "all"],
                       help="Command category to display")
    parser.add_argument("--list", action="store_true", help="List available commands")
    parser.add_argument("--interactive", action="store_true", help="Run in interactive mode")
    
    args = parser.parse_args()
    
    if args.list or (not args.category and not args.interactive):
        print_section("AUTOMATION FRAMEWORK COMMANDS")
        print("Available command categories:")
        print("  1. scheduler   - Test scheduling commands")
        print("  2. test        - Test execution commands")
        print("  3. converter   - File converter commands")
        print("  4. help        - Help and documentation commands")
        print()
        print("Use --category [name] to see commands for a specific category")
        print("Use --interactive to enter interactive mode")
        return
    
    if args.interactive:
        while True:
            print_section("AUTOMATION FRAMEWORK COMMAND HELPER")
            print("Select a command category:")
            print("  1. Test Scheduler Commands")
            print("  2. Test Execution Commands")
            print("  3. File Converter Commands")
            print("  4. Help & Documentation Commands")
            print("  0. Exit")
            
            choice = input("\nEnter your choice (0-4): ").strip()
            
            if choice == "0":
                print("Exiting command helper.")
                break
            elif choice == "1":
                show_scheduler_commands()
            elif choice == "2":
                show_test_commands()
            elif choice == "3":
                show_converter_commands()
            elif choice == "4":
                show_help_commands()
            else:
                print("Invalid choice. Please try again.")
    
    elif args.category:
        if args.category == "scheduler" or args.category == "all":
            show_scheduler_commands()
        
        if args.category == "test" or args.category == "all":
            show_test_commands()
        
        if args.category == "converter" or args.category == "all":
            show_converter_commands()
        
        if args.category == "help" or args.category == "all":
            show_help_commands()

def show_scheduler_commands():
    """Show scheduler commands and examples."""
    print_section("TEST SCHEDULER COMMANDS")
    
    print_command("python run_scheduler.py --add --traceability_id <ID> --interval <INTERVAL> --time <TIME> [options]",
                 "Add a test to the scheduler")
    
    print_command("python add_to_scheduler.py <YAML_FILE>",
                 "Add a test to the scheduler directly from a YAML file")
    
    print_command("python run_scheduler.py --list [--traceability_id <ID>] [--interval <INTERVAL>] [--status <STATUS>]",
                 "List scheduled tests")
    
    print_command("python run_scheduler.py --remove --traceability_id <ID>",
                 "Remove a test from the scheduler")
    
    print_command("python run_scheduler.py --run [--traceability_id <ID>] [--schedule_id <ID>] [--now]",
                 "Run scheduled tests")
    
    print_command("python run_schedule_test_cases.py --continuous --interval <SECONDS>",
                 "Run scheduler service in continuous mode")
    
    print("\nExamples:")
    print("  1. Add a daily test at 8 AM:")
    print("     python run_scheduler.py --add --traceability_id TEST-001 --interval daily --time \"08:00\"")
    
    print("\n  2. Add a weekly test on Monday at 11 PM:")
    print("     python run_scheduler.py --add --traceability_id TEST-002 --interval weekly --day monday --time \"23:00\"")
    
    print("\n  3. Add a test directly from YAML file:")
    print("     python add_to_scheduler.py Testcases/TC_003.yaml")
    
    print("\n  4. List all scheduled tests:")
    print("     python run_scheduler.py --list")
    
    print("\n  5. Run all tests scheduled for the current time:")
    print("     python run_scheduler.py --run")
    
    print("\n  6. Run the scheduler service, checking every 10 minutes:")
    print("     python run_schedule_test_cases.py --continuous --interval 600")
    
    input("\nPress Enter to continue...")

def show_test_commands():
    """Show test execution commands and examples."""
    print_section("TEST EXECUTION COMMANDS")
    
    print_command("python validation/Comparison.py --yaml-file <FILE> --single [--traceability_id <ID>]",
                 "Run a single test case")
    
    print_command("python run_test_cases.py [--test-dir <DIR>] [--pattern <PATTERN>] [--execution_run_id <ID>]",
                 "Run tests in batch mode")
    
    print_command("python run_tests_visual.py [--test-file <FILE>] [--test-dir <DIR>] [--run-id <ID>] [--debug]",
                 "Run tests with visual display")
    
    print("\nExamples:")
    print("  1. Run a single test case:")
    print("     python validation/Comparison.py --yaml-file Testcases/example_test.yaml --single")
    
    print("\n  2. Run all test cases in a directory:")
    print("     python run_test_cases.py --test-dir Testcases")
    
    print("\n  3. Run tests with pattern matching:")
    print("     python run_test_cases.py --pattern \"customer_*.yaml\"")
    
    print("\n  4. Run tests with visual display:")
    print("     python run_tests_visual.py --test-file Testcases/example_test.yaml --debug")
    
    input("\nPress Enter to continue...")

def show_converter_commands():
    """Show file converter commands and examples."""
    print_section("FILE CONVERTER COMMANDS")
    
    print_command("python run_file_converter.py --config <CONFIG> --input <INPUT> --output <OUTPUT>",
                 "Convert a file using a specific converter configuration")
    
    print("\nAvailable converters:")
    print("  - csv_to_psv_config:      Convert CSV to PSV")
    print("  - json_to_csv_config:     Convert JSON to CSV")
    print("  - xml_to_csv_config:      Convert XML to CSV")
    print("  - mainframe_to_csv_config: Convert mainframe/EBCDIC to CSV")
    print("  - episodic_to_csv_config: Convert episodic data to CSV")
    
    print("\nExamples:")
    print("  1. Convert CSV to PSV:")
    print("     python run_file_converter.py --config csv_to_psv_config --input data/sample.csv --output data/output.psv")
    
    print("\n  2. Convert JSON to CSV:")
    print("     python run_file_converter.py --config json_to_csv_config --input data/sample.json --output data/output.csv")
    
    print("\n  3. Convert mainframe file to CSV with copybook:")
    print("     python run_file_converter.py --config mainframe_to_csv_config --input data/mainframe.dat --output data/output.csv --copybook copybooks/structure.cpy")
    
    input("\nPress Enter to continue...")

def show_help_commands():
    """Show help and documentation commands."""
    print_section("HELP & DOCUMENTATION COMMANDS")
    
    print_command("python framework_help.py",
                 "Show general framework help information")
    
    print_command("python framework_help.py --plugins",
                 "Show list of available plugins")
    
    print_command("python framework_help.py --services",
                 "Show list of available services")
    
    print_command("python framework_help.py --examples",
                 "Show usage examples")
    
    print_command("python framework_help.py --configs",
                 "Show configuration examples")
    
    print_command("python framework_help.py --flow",
                 "Show framework flow diagram")
    
    print("\nDocumentation Location:")
    print("  Documentation is located in the 'docs/READMEs' directory:")
    print("  - Main README:          docs/READMEs/README.md")
    print("  - Commands Reference:   docs/READMEs/Commands_Readme.md")
    print("  - Plugins Documentation: docs/READMEs/README_PLUGINS.md")
    print("  - File Converters:      docs/READMEs/README_FILE_CONVERTERS.md")
    
    input("\nPress Enter to continue...")

if __name__ == "__main__":
    main() 