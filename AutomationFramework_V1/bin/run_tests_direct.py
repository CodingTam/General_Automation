#!/usr/bin/env python3
"""
Direct Test Runner
Runs test files individually without using unittest discovery to avoid hanging issues.
Shows detailed progress information during initialization.
"""

import os
import sys
import glob
import subprocess
import time
import importlib.util

def print_flush(message):
    """Print a message and flush stdout to ensure immediate display."""
    print(message)
    sys.stdout.flush()

def main():
    """Run all test files in the framework_self_tests directory by executing each Python file directly."""
    # Start timing the entire process
    total_start_time = time.time()
    
    print_flush("Starting Framework Test Runner...")
    print_flush("Initializing required components...\n")
    
    print_flush("Step 1/5: Loading system modules...")
    start_time = time.time()
    import unittest  # Import unittest to measure module load time
    print_flush(f"Complete - Loaded core modules in {time.time() - start_time:.2f}s")
    
    print_flush("\nStep 2/5: Checking test directory...")
    start_time = time.time()
    test_dir = "tests/framework_self_tests"
    if not os.path.exists(test_dir):
        print_flush(f"ERROR: Test directory does not exist: {test_dir}")
        return 1
    print_flush(f"Complete - Test directory '{test_dir}' verified in {time.time() - start_time:.2f}s")
    
    print_flush("\nStep 3/5: Discovering test files...")
    start_time = time.time()
    test_files = glob.glob(os.path.join(test_dir, "test_*.py"))
    test_files.sort()  # Sort for consistent order
    discovery_time = time.time() - start_time
    
    if not test_files:
        print_flush(f"Complete - No test files found in {test_dir}")
        return 0
    
    print_flush(f"Complete - Found {len(test_files)} test files in {discovery_time:.2f}s")
    
    print_flush("\nStep 4/5: Validating test modules...")
    start_time = time.time()
    valid_test_files = []
    for test_file in test_files:
        # Check if the file is a valid Python module
        try:
            # Don't actually import it, just check if it's valid
            spec = importlib.util.spec_from_file_location("temp_module", test_file)
            if spec:
                valid_test_files.append(test_file)
            else:
                print_flush(f"  - Warning: {test_file} is not a valid Python module")
        except Exception as e:
            print_flush(f"  - Warning: Error checking {test_file}: {str(e)}")
    
    validation_time = time.time() - start_time
    print_flush(f"Complete - Validated {len(valid_test_files)} test modules in {validation_time:.2f}s")
    
    # Only continue with valid test files
    test_files = valid_test_files
    
    print_flush("\nStep 5/5: Preparing test environment...")
    start_time = time.time()
    # Create any necessary directories or resources for tests
    os.makedirs("reports", exist_ok=True)
    print_flush(f"Complete - Environment prepared in {time.time() - start_time:.2f}s")
    
    # Print setup summary
    print_flush(f"\nTest setup complete in {time.time() - total_start_time:.2f}s")
    print_flush("\n" + "="*70)
    print_flush("FRAMEWORK DIRECT TEST RUNNER".center(70))
    print_flush("="*70 + "\n")
    
    print_flush(f"Ready to execute {len(test_files)} test files:")
    for i, test_file in enumerate(test_files, 1):
        print_flush(f"{i}. {test_file}")
    print_flush("")
    
    # Run each test file
    passed = []
    failed = []
    
    for test_file in test_files:
        result = run_test_file(test_file)
        if result:
            passed.append(test_file)
        else:
            failed.append(test_file)
    
    # Print summary
    print_flush("\n" + "="*70)
    print_flush("TEST RESULTS".center(70))
    print_flush("="*70)
    print_flush(f"Total test files: {len(test_files)}")
    print_flush(f"Passed: {len(passed)}")
    print_flush(f"Failed: {len(failed)}")
    
    if failed:
        print_flush("\nFailed tests:")
        for test in failed:
            print_flush(f"- {test}")
    
    total_test_time = time.time() - total_start_time
    overall_status = "PASS" if not failed else "FAIL"
    print_flush(f"\nOVERALL STATUS: {overall_status}")
    print_flush(f"Total execution time: {total_test_time:.2f}s")
    
    return 1 if failed else 0

def run_test_file(test_file):
    """Run a single test file and return whether it succeeded."""
    print_flush(f"\nRunning: {test_file}")
    print_flush("-" * 70)
    
    start_time = time.time()
    
    try:
        # Special handling for test_cli.py to prevent screen clearing issues
        is_cli_test = os.path.basename(test_file) == "test_cli.py"
        if is_cli_test:
            print_flush("NOTE: Running CLI test with special handling to prevent screen clearing...")
            # Set environment variable to inform the test not to clear the screen
            env = os.environ.copy()
            env["NO_CLEAR_SCREEN"] = "1"
        else:
            env = None
            
        # Run the test file with a timeout to prevent hanging
        print_flush(f"  Executing test file (timeout: 10s)...")
        process = subprocess.run(
            [sys.executable, test_file],
            check=False,
            timeout=10,  # 10 second timeout
            capture_output=False,  # Display output directly
            env=env  # Pass custom environment if needed
        )
        
        elapsed = time.time() - start_time
        if process.returncode == 0:
            print_flush(f"PASS: {test_file} (completed in {elapsed:.2f}s)")
            return True
        else:
            print_flush(f"FAIL: {test_file} returned exit code {process.returncode}")
            return False
            
    except subprocess.TimeoutExpired:
        print_flush(f"FAIL: {test_file} timed out after 10 seconds")
        return False
    except Exception as e:
        print_flush(f"ERROR: {test_file} - {str(e)}")
        return False

if __name__ == "__main__":
    try:
        print_flush("Initiating Framework Self-Tests...")
        sys.exit(main())
    except KeyboardInterrupt:
        print_flush("\nTest execution canceled by user")
        sys.exit(1)
    except Exception as e:
        print_flush(f"\nUnexpected error during test execution: {e}")
        sys.exit(1) 