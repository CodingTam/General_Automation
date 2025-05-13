#!/usr/bin/env python3
"""
Interactive Execution UI for Automation Framework
This script provides a friendly command-line interface to run the main components
of the Automation Framework interactively.
"""

import os
import sys
import subprocess
import glob
from datetime import datetime
import argparse
from utils.banner import print_intro

def clear_screen():
    """Clear the terminal screen based on OS"""
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header():
    """Print the header banner"""
    clear_screen()
    print("\n" + "=" * 80)
    print("AUTOMATION FRAMEWORK - INTERACTIVE MODE".center(80))
    print("=" * 80)
    print("\nSelect an operation to perform:\n")

def run_command(command):
    """Run a command and return the process"""
    try:
        print(f"\nExecuting: {' '.join(command)}\n")
        print("-" * 80)
        process = subprocess.run(command, check=True)
        print("-" * 80)
        print(f"\nCommand completed with exit code: {process.returncode}")
        return process.returncode
    except subprocess.CalledProcessError as e:
        print(f"\nError executing command: {e}")
        return e.returncode
    except FileNotFoundError:
        print(f"\nError: Could not find the command: {command[0]}")
        return -1

def get_test_cases():
    """Get list of test case files in Testcases directory"""
    try:
        test_cases = glob.glob("Testcases/*.yaml")
        return test_cases
    except Exception as e:
        print(f"Error getting test cases: {e}")
        return []

def run_scheduler():
    """Interactive menu for scheduler operations"""
    while True:
        clear_screen()
        print("\n=== SCHEDULER OPERATIONS ===\n")
        print("1. List scheduled tests")
        print("2. Run scheduled tests")
        print("3. Run scheduled tests now (ignoring scheduled time)")
        print("4. Add a test to the scheduler")
        print("5. Remove a test from the scheduler")
        print("6. Run scheduler in continuous mode")
        print("0. Return to main menu")
        
        choice = input("\nEnter your choice (0-6): ")
        
        if choice == '0':
            break
        elif choice == '1':
            run_command(["python3", "run_scheduler.py", "--list"])
        elif choice == '2':
            run_command(["python3", "run_scheduler.py", "--run"])
        elif choice == '3':
            run_command(["python3", "run_scheduler.py", "--run-now"])
        elif choice == '4':
            test_cases = get_test_cases()
            if not test_cases:
                print("No test cases found in Testcases directory.")
                input("\nPress Enter to continue...")
                continue
                
            print("\nAvailable test cases:")
            for i, tc in enumerate(test_cases):
                print(f"{i+1}. {tc}")
                
            try:
                tc_idx = int(input("\nSelect test case number: ")) - 1
                if tc_idx < 0 or tc_idx >= len(test_cases):
                    print("Invalid selection.")
                    input("\nPress Enter to continue...")
                    continue
                    
                frequency = input("Enter frequency (daily/weekly/monthly) [daily]: ") or "daily"
                
                if frequency == "weekly":
                    day = input("Enter day of week (1-7, where 1=Monday) [1]: ") or "1"
                    command = ["python3", "add_to_scheduler.py", test_cases[tc_idx], "--frequency", frequency, "--day-of-week", day]
                elif frequency == "monthly":
                    day = input("Enter day of month (1-31) [1]: ") or "1"
                    command = ["python3", "add_to_scheduler.py", test_cases[tc_idx], "--frequency", frequency, "--day-of-month", day]
                else:
                    command = ["python3", "add_to_scheduler.py", test_cases[tc_idx], "--frequency", frequency]
                
                run_command(command)
            except ValueError:
                print("Invalid input. Please enter a number.")
                
            input("\nPress Enter to continue...")
        elif choice == '5':
            # List tests first
            run_command(["python3", "run_scheduler.py", "--list"])
            test_id = input("\nEnter test ID to remove: ")
            if test_id:
                run_command(["python3", "run_scheduler.py", "--remove", "--traceability_id", test_id])
            input("\nPress Enter to continue...")
        elif choice == '6':
            interval = input("Enter check interval in seconds [300]: ") or "300"
            print("\nRunning in continuous mode. Press Ctrl+C to stop.")
            print("Starting scheduler in 3 seconds...")
            import time
            time.sleep(3)
            run_command(["python3", "run_scheduler.py", "--continuous", "--interval", interval])
            input("\nPress Enter to continue...")
        else:
            print("Invalid choice. Please try again.")
            input("\nPress Enter to continue...")

def run_test_cases():
    """Interactive menu for test case execution"""
    while True:
        clear_screen()
        print("\n=== TEST CASE EXECUTION ===\n")
        print("1. Run a specific test case")
        print("2. Run all test cases")
        print("3. Run test cases matching a pattern")
        print("4. Run including scheduled tests")
        print("0. Return to main menu")
        
        choice = input("\nEnter your choice (0-4): ")
        
        if choice == '0':
            break
        elif choice == '1':
            test_cases = get_test_cases()
            if not test_cases:
                print("No test cases found in Testcases directory.")
                input("\nPress Enter to continue...")
                continue
                
            print("\nAvailable test cases:")
            for i, tc in enumerate(test_cases):
                print(f"{i+1}. {tc}")
                
            try:
                tc_idx = int(input("\nSelect test case number: ")) - 1
                if tc_idx < 0 or tc_idx >= len(test_cases):
                    print("Invalid selection.")
                else:
                    run_command(["python3", "run_test_cases.py", test_cases[tc_idx]])
            except ValueError:
                print("Invalid input. Please enter a number.")
                
            input("\nPress Enter to continue...")
        elif choice == '2':
            run_command(["python3", "run_test_cases.py"])
            input("\nPress Enter to continue...")
        elif choice == '3':
            pattern = input("Enter pattern (e.g. TC_00*.yaml): ")
            if pattern:
                run_command(["python3", "run_test_cases.py", "--pattern", pattern])
            input("\nPress Enter to continue...")
        elif choice == '4':
            run_command(["python3", "run_test_cases.py", "--scheduled"])
            input("\nPress Enter to continue...")
        else:
            print("Invalid choice. Please try again.")
            input("\nPress Enter to continue...")

def run_visual_tests():
    """Interactive menu for visual test execution"""
    while True:
        clear_screen()
        print("\n=== VISUAL TEST EXECUTION ===\n")
        print("1. Run a specific test case visually")
        print("2. Run all test cases visually")
        print("3. Run tests visually with debug mode")
        print("0. Return to main menu")
        
        choice = input("\nEnter your choice (0-3): ")
        
        if choice == '0':
            break
        elif choice == '1':
            test_cases = get_test_cases()
            if not test_cases:
                print("No test cases found in Testcases directory.")
                input("\nPress Enter to continue...")
                continue
                
            print("\nAvailable test cases:")
            for i, tc in enumerate(test_cases):
                print(f"{i+1}. {tc}")
                
            try:
                tc_idx = int(input("\nSelect test case number: ")) - 1
                if tc_idx < 0 or tc_idx >= len(test_cases):
                    print("Invalid selection.")
                else:
                    run_command(["python3", "run_tests_visual.py", "--test-file", test_cases[tc_idx]])
            except ValueError:
                print("Invalid input. Please enter a number.")
                
            input("\nPress Enter to continue...")
        elif choice == '2':
            run_command(["python3", "run_tests_visual.py"])
            input("\nPress Enter to continue...")
        elif choice == '3':
            run_command(["python3", "run_tests_visual.py", "--debug"])
            input("\nPress Enter to continue...")
        else:
            print("Invalid choice. Please try again.")
            input("\nPress Enter to continue...")

def main():
    """Main interactive menu"""
    parser = argparse.ArgumentParser(description='Interactive mode for Automation Framework')
    parser.add_argument('--mode', choices=['scheduler', 'test', 'visual'], 
                        help='Start directly in a specific mode')
    args = parser.parse_args()
    
    # Display intro banner once at startup
    print_intro("QA", add_extra_line=True)
    
    # If a specific mode is requested, jump directly to it
    if args.mode == 'scheduler':
        run_scheduler()
        return
    elif args.mode == 'test':
        run_test_cases()
        return
    elif args.mode == 'visual':
        run_visual_tests()
        return
    
    while True:
        print_header()
        print("1. Scheduler Operations")
        print("2. Test Case Execution")
        print("3. Visual Test Execution")
        print("4. Exit")
        
        choice = input("\nEnter your choice (1-4): ")
        
        if choice == '1':
            run_scheduler()
        elif choice == '2':
            run_test_cases()
        elif choice == '3':
            run_visual_tests()
        elif choice == '4':
            print("\nExiting interactive mode. Goodbye!")
            sys.exit(0)
        else:
            print("Invalid choice. Please try again.")
            input("\nPress Enter to continue...")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user. Exiting...")
        sys.exit(0) 