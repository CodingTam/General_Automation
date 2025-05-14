#!/usr/bin/env python3
"""
Visual Test Runner Wrapper

A wrapper around run_test_cases.py that provides a visual representation of
test case execution with stage-by-stage progress indicators.
"""

import os
import sys
import time
import glob
import yaml
import subprocess
import argparse
from datetime import datetime

# Global debug flag
DEBUG = False

# Custom ANSI color codes
BLUE = '\033[1;34m'
GREEN = '\033[1;32m'
PURPLE = '\033[1;35m'
CYAN = '\033[1;36m'
RED = '\033[1;31m'
YELLOW = '\033[1;33m'
WHITE = '\033[1;37m'
NC = '\033[0m'  # No Color

# Test execution stages for visualization
STAGES = [
    "Initialization",
    "Loading Configuration",
    "Spark Submit",  # New stage for spark-submit
    "Schema Validation",
    "Data Loading",
    "Count Validation",
    "Data Comparison",
    "Rule Validation",
    "Report Generation",
    "Results Storage",
    "Jira Update"
]

def clear_screen():
    """Clear the terminal screen based on OS."""
    os.system('cls' if os.name == 'nt' else 'clear')

def find_test_cases(directory="Testcases"):
    """Find all YAML test case files in the specified directory."""
    print(f"{CYAN}Looking for test cases in {directory}...{NC}")
    test_files = glob.glob(f"{directory}/*.yaml") + glob.glob(f"{directory}/*.yml")
    if not test_files:
        print(f"{RED}No test cases found in {directory}!{NC}")
        sys.exit(1)
    return sorted(test_files)

def is_scheduled_test(yaml_file):
    """Check if a test case is scheduled based on its YAML file."""
    try:
        yaml_content = parse_yaml_file_content(yaml_file)
        # Check if scheduler section exists and is enabled
        if 'scheduler' in yaml_content and yaml_content.get('scheduler', {}).get('enabled', '').lower() in ['yes', 'true', '1']:
            return True
        return False
    except Exception as e:
        if DEBUG:
            print(f"{RED}Error checking if test is scheduled: {e}{NC}")
        return False

def parse_yaml_file(yaml_file):
    """Parse a YAML file and return the test case name."""
    try:
        yaml_content = parse_yaml_file_content(yaml_file)
        return yaml_content.get('test_case_name', os.path.basename(yaml_file))
    except Exception as e:
        return os.path.basename(yaml_file)

def parse_yaml_file_content(yaml_file):
    """Parse a YAML file and return the content."""
    try:
        with open(yaml_file, 'r') as f:
            yaml_content = yaml.safe_load(f)
        return yaml_content
    except Exception as e:
        if DEBUG:
            print(f"{RED}Error parsing YAML file {yaml_file}: {e}{NC}")
        return {}

def draw_progress_flow(test_case, current_stage, status_dict, test_result=None):
    """Draw a flow representation of the test execution progress."""
    width = 80
    print("\n" + "=" * width)
    
    # Test case header
    header = f"Test Case: {test_case}"
    padding = (width - len(header)) // 2
    print(" " * padding + f"{CYAN}{header}{NC}")
    print("=" * width)
    
    # Draw flow diagram
    print("\n" + " " * 10 + "╔══════════════════════════════════════════════════════════╗")
    print(" " * 10 + "║                    TEST EXECUTION FLOW                    ║")
    print(" " * 10 + "╚═════════════════════════════╦════════════════════════════╝")
    print(" " * 10 + "                              ║")
    
    # Draw flow for each stage
    for i, stage in enumerate(STAGES):
        status = status_dict.get(i, "PENDING")
        
        if i < current_stage:
            # Completed stage
            if status == "COMPLETE":
                stage_color = GREEN
                status_icon = "✓"
            elif status == "SKIP":
                stage_color = YELLOW
                status_icon = "→"
            elif status == "FAIL":
                stage_color = RED
                status_icon = "✗"
            else:
                stage_color = CYAN
                status_icon = "?"
        elif i == current_stage:
            # Current stage
            stage_color = YELLOW
            status_icon = "•"
        else:
            # Future stage - no color
            stage_color = ""  # Changed from WHITE to no color
            status_icon = " "
            
        # Draw the stage box
        if i > 0:
            print(" " * 10 + "                              ║")
            print(" " * 10 + "                              ▼")
        
        box_width = 50
        stage_text = f" {status_icon} {stage}"
        padding = (box_width - len(stage_text)) // 2
        
        print(" " * 10 + f"              ┌─────────────────────────────────────────────┐")
        print(" " * 10 + f"              │ {stage_color}{stage_text}{NC}{' ' * (box_width - len(stage_text) - 2)}│")
        print(" " * 10 + f"              └─────────────────────────────────────────────┘")
        
    # Draw the end of the flow
    if current_stage >= len(STAGES):
        print(" " * 10 + "                              ║")
        print(" " * 10 + "                              ▼")
        
        # Final status box - use test_result if provided
        if test_result:
            # Use the provided test result
            if test_result == "FAILED":
                result = f"{RED}✗ FAILED{NC}"
            else:
                result = f"{GREEN}✓ PASSED{NC}"
        else:
            # Fallback to checking status_dict
            if "FAIL" in status_dict.values():
                result = f"{RED}✗ FAILED{NC}"
            else:
                result = f"{GREEN}✓ PASSED{NC}"
            
        print(" " * 10 + f"              ┌─────────────────────────────────────────────┐")
        print(" " * 10 + f"              │                  {result}                  │")
        print(" " * 10 + f"              └─────────────────────────────────────────────┘")

def filter_output_line(line):
    """Filter output line to remove banners, headers, and emoji logs, keeping only Spark logs."""
    lower_line = line.lower()
    
    # Check if this is a Spark log (keep these)
    if "spark" in lower_line and any(term in lower_line for term in ["info", "warn", "error", "debug", "job", "stage", "task", "executor", "driver"]):
        return False  # Don't filter out Spark logs
    
    # Check for engine details lines to filter
    engine_terms = ["engine", "mode", "environment", "author", "test version", "framework version"]
    if any(term in lower_line for term in engine_terms) and (":" in line or "-" in line or "=" in line):
        return True
    
    # Filter out non-Spark content
    if (
        "welcome to" in lower_line or 
        "==" in line or 
        "---" in line or
        ("engine" in lower_line and "mode" in lower_line) or
        "timestamp" in lower_line or
        "--" in line or
        line.strip() == "" or
        "###" in line or
        all(c == '=' for c in line.strip()) or
        all(c == '-' for c in line.strip()) or
        all(c == '*' for c in line.strip()) or
        "copyright" in lower_line or
        "version" in lower_line or
        "-------------------------" in line or
        "initializing modules" in lower_line or
        "loading test assets" in lower_line or
        "preparing test cases" in lower_line or
        "engine ready" in lower_line or
        "executing test suite" in lower_line or
        line.startswith("🔍") or line.startswith("📦") or 
        line.startswith("🧪") or line.startswith("✅") or
        line.startswith("📊") or line.startswith("🔧") or
        line.startswith("📋") or line.startswith("⚙️") or
        line.startswith("📈") or line.startswith("📝") or
        line.startswith("🔹") or  # Filter out blue diamond prefixed lines (engine details)
        "engine" in lower_line or  # Filter any engine lines
        "mode" in lower_line or  # Filter any mode lines  
        "environment" in lower_line or  # Filter any environment lines
        "author" in lower_line or  # Filter any author lines
        "QA" in line or "DEV" in line or "PROD" in line or  # Environment indicators
        any(emoji in line for emoji in ["✓", "✗", "→", "•", "✅", "❌", "⚠️", "ℹ️", "🔧", "🧠"])
    ):
        return True
    return False

def get_relevant_output(output_lines, max_lines=8):
    """Extract relevant output lines, prioritizing Spark logs and other useful information."""
    # First try to find Spark logs
    spark_logs = [line for line in output_lines if "spark" in line.lower() and not filter_output_line(line)]
    
    # Look for validation results and data processing logs
    important_terms = [
        "validation", "comparing", "processing", "reading", "loading data", 
        "count", "mismatch", "row", "column", "record", "values", "executing",
        "data", "schema", "rule", "error", "warning", "info", "results"
    ]
    
    important_logs = [
        line for line in output_lines 
        if any(term in line.lower() for term in important_terms) 
        and not filter_output_line(line)
        and not line in spark_logs  # Avoid duplicates
    ]
    
    # Combine prioritized logs
    combined_logs = spark_logs + important_logs
    
    # If we have enough prioritized logs, return those
    if len(combined_logs) >= 2:
        return combined_logs[-max_lines:]
    
    # Otherwise, return any non-filtered lines
    filtered_lines = [
        line for line in output_lines 
        if not filter_output_line(line) 
        and not line in combined_logs  # Avoid duplicates
    ]
    
    all_relevant = combined_logs + filtered_lines
    
    if len(all_relevant) >= 2:
        return all_relevant[-max_lines:]
    
    # If all else fails, just return the last few lines
    return output_lines[-max_lines:]

def execute_test_case(test_file, execution_run_id=None):
    """Execute a single test case and visualize the progress."""
    test_case_name = parse_yaml_file(test_file)
    stages_status = {}
    test_result = "UNKNOWN"  # Track actual test result, not just execution status
    
    # Store all output for accurate result detection
    all_output = []
    validation_errors_detected = False  # Flag to track if any validation errors were detected
    
    # Initial setup
    clear_screen()
    print(f"{CYAN}Executing Test Case: {test_case_name}{NC}")
    print(f"{CYAN}File: {test_file}{NC}")
    
    # Initial flow display - start with no stages completed
    draw_progress_flow(test_case_name, 0, stages_status)
    
    # Prepare output area
    output_lines = []
    # Add initial output lines to ensure we never show empty output
    output_lines.append(f"{CYAN}Starting test execution for {test_case_name}...{NC}")
    output_lines.append(f"{CYAN}Preparing test environment...{NC}")
    last_update_time = time.time()
    
    # Execute actual test case
    cmd = ["python3", "bin/run_test_cases.py", test_file]  # Updated path to use bin directory
    if execution_run_id:
        cmd.extend(["--execution_run_id", execution_run_id])
    
    print(f"\n{YELLOW}Executing: {' '.join(cmd)}{NC}")
    print(f"{YELLOW}{'─' * 60}{NC}")
    
    # Always show some initial output
    for output_line in output_lines:
        print(output_line)
    
    # Initialize first stage as active
    current_stage = 0
    stages_status[0] = "ACTIVE"  # Mark initialization as active right away
    
    # Stage detection patterns (expanded to catch more variations)
    stage_patterns = [
        # Initialization - Stage 0
        ["initializing", "starting", "beginning", "test case", "execution start"],
        # Loading Configuration - Stage 1
        ["loading yaml", "loading config", "config loaded", "reading yaml", "processing config", "parsing yaml"],
        # Spark Submit - Stage 2
        ["spark-submit", "submitting spark job", "spark job submitted", "spark application", "spark context", "spark session"],
        # Schema Validation - Stage 3
        ["schema validation", "validating schema", "schema check", "table schema", "schema comparison"],
        # Data Loading - Stage 4
        ["loading data", "data load", "reading data", "dataframe created", "loading table", "table loaded"],
        # Count Validation - Stage 5
        ["count validation", "row count", "record count", "count check", "comparing counts", "count match"],
        # Data Comparison - Stage 6
        ["data comparison", "comparing data", "row comparison", "column compare", "value mismatch", "comparing values"],
        # Rule Validation - Stage 7
        ["rule validation", "validating rules", "business rule", "rule check", "applying rules", "rule engine"],
        # Report Generation - Stage 8
        ["report generation", "generating report", "creating report", "report created", "building report"],
        # Results Storage - Stage 9
        ["storing results", "save result", "database insert", "writing results", "persisting results"],
        # Jira Update - Stage 10
        ["jira update", "updating jira", "ticket update", "issue link", "ticket created"]
    ]
    
    # Failure detection patterns (common phrases that indicate test failure)
    failure_patterns = [
        "test failed", 
        "validation failed",
        "check failed",
        "error in validation",
        "validation error",
        "failed validation",
        "count mismatch",
        "data mismatch",
        "rule violation",
        "validation checks failed",
        "comparison failed",
        "failed to match",
        "values do not match",
        "discrepancy detected",
        "error: expected",
        "failed: expected",
        "test case failed",
        "error occurred",
        "exception occurred",
        "failure detected",
        "invalid data",
        "validation issues",
        "records failed",
        "validation status: failed",
        "result: failure",
        "result: fail",
        "validation: fail"
    ]
    
    # Success detection patterns
    success_patterns = [
        "test passed",
        "validation passed",
        "all checks passed",
        "successful validation",
        "validation successful",
        "passed all validations",
        "count validation passed",
        "data comparison successful",
        "all rules passed",
        "no discrepancies found",
        "test case passed"
    ]
    
    # Stage timeouts and progress tracking
    stage_start_times = [time.time()] * len(STAGES)
    stage_timeouts = [3, 3, 4, 3, 4, 4, 4, 4, 3, 3, 3]  # Updated timeouts for each stage in seconds
    forced_stage_progression = False  # Flag to track if we're forcing stages due to lack of logs
    
    # Force stage progression when needed
    def force_stage_progression():
        nonlocal current_stage, forced_stage_progression
        if current_stage < len(STAGES) - 1:  # Don't force the Jira stage
            forced_stage_progression = True
            stages_status[current_stage] = "COMPLETE"
            current_stage += 1
            stages_status[current_stage] = "ACTIVE"
            stage_start_times[current_stage] = time.time()
            if DEBUG:
                print(f"\n{YELLOW}DEBUG: Forced progression to stage {current_stage}: {STAGES[current_stage]}{NC}")
    
    # Content tracking to detect changes in output
    last_content_hash = ""
    no_new_content_time = time.time()
    content_timeout = 2  # If no new content for X seconds, advance stage
    
    try:
        # Start the actual process
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, 
            universal_newlines=True,
            bufsize=1
        )
        
        # Tracking flags
        has_jira_component = False
        show_progress_warning = True  # To show warning once about slow progress
        
        # Process output and update display
        for line in process.stdout:
            line = line.strip()
            
            # Skip only specific emoji-prefixed lines and engine details
            # Be more selective to ensure we're not filtering too much
            skip_line = False
            if (line.startswith("🔍 Initializing") or 
                line.startswith("📦 Loading test") or
                line.startswith("🧪 Preparing test") or 
                line.startswith("✅ Engine ready") or
                line.startswith("🔹 Engine") or
                line.startswith("🔹 Mode") or
                line.startswith("🔹 Environment") or
                line.startswith("🔹 Author") or
                "Engine" in line and ":" in line and "v" in line or
                "Mode" in line and ":" in line and "Validation" in line or
                "Environment" in line and ":" in line and "QA" in line or
                "Author" in line and ":" in line):
                # Still add to all_output for analysis but don't display
                all_output.append(line)
                skip_line = True
            
            # Don't skip other lines with potentially useful information
            if not skip_line:
                output_lines.append(line)
                all_output.append(line)  # Store all output for final analysis
            
            # Keep only the last 5 lines for display
            if len(output_lines) > 5:
                output_lines.pop(0)
            
            # Update content tracking - detect if we're getting new meaningful content
            current_content = "".join(output_lines)
            current_hash = hash(current_content)
            if current_hash != last_content_hash and len(line) > 3:  # Only count non-empty lines
                last_content_hash = current_hash
                no_new_content_time = time.time()
            
            # Print debug info if enabled
            if DEBUG and current_stage < len(STAGES):
                print(f"\r{BLUE}DEBUG: Checking line for {STAGES[current_stage]} stage: {line[:50]}...{NC}", end="")
             
            # Check if Jira is mentioned anywhere in the logs
            if "jira" in line.lower() or "ticket" in line.lower() or "issue" in line.lower():
                has_jira_component = True
                if DEBUG:
                    print(f"\n{BLUE}DEBUG: Detected Jira component in logs{NC}")
            
            # Track the actual test result from each line
            for pattern in failure_patterns:
                if pattern in line.lower():
                    test_result = "FAILED"
                    validation_errors_detected = True
                    if DEBUG:
                        print(f"\n{RED}DEBUG: Test FAILED detected in logs: '{pattern}'{NC}")
                    break
                    
            for pattern in success_patterns:
                # Only mark as passed if no failures detected yet
                if pattern in line.lower() and not validation_errors_detected:
                    test_result = "PASSED"
                    if DEBUG:
                        print(f"\n{GREEN}DEBUG: Test PASSED detected in logs: '{pattern}'{NC}")
                    break
            
            # Enhanced pattern matching for each stage
            for stage_index, patterns in enumerate(stage_patterns):
                # Only check for stages we haven't reached yet or are currently on
                if stage_index <= current_stage:
                    for pattern in patterns:
                        if pattern in line.lower():
                            # If we find a pattern for the current stage, mark it complete and advance
                            if stage_index == current_stage:
                                stages_status[current_stage] = "COMPLETE"
                                current_stage += 1
                                if current_stage < len(STAGES):
                                    stages_status[current_stage] = "ACTIVE"
                                    stage_start_times[current_stage] = time.time()
                                if DEBUG:
                                    print(f"\n{GREEN}DEBUG: Advanced to stage {current_stage}: {STAGES[current_stage] if current_stage < len(STAGES) else 'Complete'}{NC}")
                                break
                            # If we find a pattern for an earlier stage that hasn't been marked complete, mark it now
                            elif stages_status.get(stage_index) != "COMPLETE":
                                stages_status[stage_index] = "COMPLETE"
                                if DEBUG:
                                    print(f"\n{GREEN}DEBUG: Retroactively completed stage {stage_index}: {STAGES[stage_index]}{NC}")
            
            # Only refresh the display a maximum of 5 times per second
            current_time = time.time()
            if current_time - last_update_time > 0.2:
                last_update_time = current_time
                
                # Check if current stage has been active for too long and should auto-advance
                if current_stage < len(STAGES) and not forced_stage_progression:
                    stage_elapsed = current_time - stage_start_times[current_stage]
                    
                    # Auto-advance if we've been on this stage too long
                    if stage_elapsed > stage_timeouts[current_stage]:
                        # If we've spent enough time on this stage, move to next
                        stages_status[current_stage] = "COMPLETE"
                        current_stage += 1
                        
                        # Don't auto-advance beyond available stages
                        if current_stage < len(STAGES):
                            stages_status[current_stage] = "ACTIVE"
                            stage_start_times[current_stage] = current_time
                            if DEBUG:
                                print(f"\n{YELLOW}DEBUG: Auto-advanced to stage {current_stage}: {STAGES[current_stage]} (timeout after {stage_elapsed:.1f}s){NC}")
                
                # Check if we've had no new content for a while - force stage progression if needed
                if current_stage < len(STAGES) - 1 and not forced_stage_progression:  # Don't force Jira stage
                    content_elapsed = current_time - no_new_content_time
                    if content_elapsed > content_timeout:
                        force_stage_progression()
                        no_new_content_time = current_time  # Reset timer
                
                # Show warning about slow progress if test seems stuck
                if current_stage < 4 and current_time - stage_start_times[0] > 15 and show_progress_warning:
                    print(f"\n{YELLOW}Warning: Test execution seems slow. Stages may be auto-advancing due to timeout.{NC}")
                    show_progress_warning = False  # Only show this once
                
                # Redraw everything
                clear_screen()
                print(f"{CYAN}Executing Test Case: {test_case_name}{NC}")
                print(f"{CYAN}File: {test_file}{NC}")
                draw_progress_flow(test_case_name, current_stage, stages_status, test_result)
                print(f"\n{YELLOW}Latest Output:{NC}")
                print(f"{YELLOW}{'─' * 60}{NC}")
                
                # Display output lines or a message if no output
                if output_lines:
                    for output_line in output_lines:
                        print(output_line)
                else:
                    print(f"{CYAN}Waiting for output...{NC}")
        
        # Wait for process to complete
        process.wait()
        
        # Process completion - make sure all regular stages are complete
        for i in range(len(STAGES) - 1):  # All stages except Jira
            if i not in stages_status or stages_status[i] != "COMPLETE":
                stages_status[i] = "COMPLETE"
        
        # Handle Jira stage separately
        if has_jira_component:
            stages_status[len(STAGES) - 1] = "COMPLETE"
        else:
            stages_status[len(STAGES) - 1] = "SKIP"
        
        # Final stage
        current_stage = len(STAGES)
        
        # Final analysis of full output for test result determination
        # First, check if any validation errors were detected during execution
        if validation_errors_detected:
            test_result = "FAILED"
        else:
            # Use the specialized parser to extract result from summary
            summary_result = parse_test_summary(all_output)
            if summary_result != "UNKNOWN":
                test_result = summary_result
            elif test_result == "UNKNOWN":
                # If still unknown, use the process exit code
                test_result = "PASSED" if process.returncode == 0 else "FAILED"
        
        # FORCE DETECTION: Only force FAILED if we see specific error patterns
        if test_result == "PASSED":
            last_chunk = "\n".join(all_output[-50:]).lower()
            # Only force FAILED if we see actual error messages, not just keywords
            if (("error:" in last_chunk or "exception:" in last_chunk) and 
                ("fail" in last_chunk or "error" in last_chunk or "exception" in last_chunk)):
                test_result = "FAILED"
                if DEBUG:
                    print(f"\n{RED}DEBUG: Forced FAILED result due to error messages in final output{NC}")
        
        # Final display
        clear_screen()
        print(f"{CYAN}Completed Test Case: {test_case_name}{NC}")
        print(f"{CYAN}File: {test_file}{NC}")
        
        # Set the color based on result
        result_color = GREEN if test_result == "PASSED" else RED
        print(f"{CYAN}Result: {result_color}{test_result}{NC}")
        
        draw_progress_flow(test_case_name, current_stage, stages_status, test_result)
        
        # Show final output
        print(f"\n{YELLOW}Latest Output:{NC}")
        print(f"{YELLOW}{'─' * 60}{NC}")
        
        # Display output lines or completion message if no output
        if output_lines:
            for output_line in output_lines:
                print(output_line)
        else:
            print(f"{GREEN}Test completed successfully.{NC}")
            print(f"{CYAN}No output available to display.{NC}")
        
        # Add debug info if needed
        if DEBUG:
            print(f"\n{YELLOW}DEBUG: Final determination based on:")
            print(f"  - Validation errors during execution: {validation_errors_detected}")
            print(f"  - Test summary parser result: {parse_test_summary(all_output)}")
            print(f"  - Process exit code: {process.returncode}")
            print(f"  - Final result: {test_result}{NC}")
        
        return test_result
            
    except Exception as e:
        print(f"{RED}Error executing test case: {str(e)}{NC}")
        if current_stage < len(STAGES):
            stages_status[current_stage] = "FAIL"
        return "FAILED"

def draw_execution_flow_summary(results):
    """Draw a flow diagram summary of all test executions."""
    width = 80
    print("\n" + "=" * width)
    
    # Header
    header = "TEST SUITE EXECUTION FLOW"
    padding = (width - len(header)) // 2
    print(" " * padding + f"{CYAN}{header}{NC}")
    print("=" * width + "\n")
    
    # Start of flow
    print(" " * 10 + "╔══════════════════════════════════════════════════════════╗")
    print(" " * 10 + "║                   TEST SUITE STARTED                     ║")
    print(" " * 10 + "╚═════════════════════════════╦════════════════════════════╝")
    print(" " * 10 + "                              ║")
    print(" " * 10 + "                              ▼")
    
    # Results for each test case
    for i, (test_file, status) in enumerate(results.items()):
        test_name = parse_yaml_file(test_file)
        
        # Draw test case box
        status_color = GREEN if status == "PASSED" else RED
        status_icon = "✓" if status == "PASSED" else "✗"
        
        print(" " * 10 + f"              ┌─────────────────────────────────────────────┐")
        print(" " * 10 + f"              │ Test Case {i+1}: {test_name:<30} │")
        print(" " * 10 + f"              │ Validation Result: {status_color}{status_icon} {status}{NC}{' ' * 21}│")
        print(" " * 10 + f"              └─────────────────────────────────────────────┘")
        
        if i < len(results) - 1:
            print(" " * 10 + "                              ║")
            print(" " * 10 + "                              ▼")
    
    # Final summary
    passed = sum(1 for test_file, status in results.items() 
                if status == "PASSED")
    
    failed = sum(1 for test_file, status in results.items() 
               if status == "FAILED")
    
    overall_status = "PASSED" if failed == 0 else "FAILED"
    status_color = GREEN if overall_status == "PASSED" else RED
    status_icon = "✓" if overall_status == "PASSED" else "✗"
    
    print(" " * 10 + "                              ║")
    print(" " * 10 + "                              ▼")
    print(" " * 10 + f"              ┌─────────────────────────────────────────────┐")
    print(" " * 10 + f"              │ Overall Validation: {status_color}{status_icon} {overall_status}{NC}{' ' * 21}│")
    print(" " * 10 + f"              │ Total: {len(results)}   Passed: {GREEN}{passed}{NC}   Failed: {RED}{failed}{NC}{' ' * 16}│")
    print(" " * 10 + f"              └─────────────────────────────────────────────┘")

def run_all_test_cases(test_files, execution_run_id=None):
    """Execute all test cases with visual progress tracking."""
    start_time = datetime.now()
    results = {}
    
    # Generate a new execution_run_id if none provided
    if not execution_run_id:
        from uuid import uuid4
        execution_run_id = str(uuid4())
        print(f"{CYAN}Generated Execution Run ID: {execution_run_id}{NC}")
    
    clear_screen()
    print(f"{CYAN}Starting Test Suite Execution{NC}")
    print(f"{CYAN}Total Test Cases: {len(test_files)}{NC}")
    print(f"{CYAN}Execution Run ID: {execution_run_id}{NC}")
    print(f"{CYAN}Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}{NC}")
    print()
    
    for i, test_file in enumerate(test_files):
        print(f"{CYAN}[{i+1}/{len(test_files)}] Processing: {test_file}{NC}")
        time.sleep(1)  # Brief pause to show the message
        
        # Execute the test case and get the validation result (PASSED/FAILED)
        status = execute_test_case(test_file, execution_run_id)
        results[test_file] = status
        
        # If not the last test, prompt to continue
        if i < len(test_files) - 1:
            input(f"{YELLOW}Press Enter to continue to next test case...{NC}")
    
    # Show summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Don't clear the screen to keep the last test case flow diagram visible
    print(f"\n{CYAN}Test Suite Execution Summary{NC}")
    print(f"{CYAN}Execution Run ID: {execution_run_id}{NC}")
    print(f"{CYAN}Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}{NC}")
    print(f"{CYAN}End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}{NC}")
    print(f"{CYAN}Duration: {duration}{NC}")
    
    # Draw flow diagram summary
    input(f"\nPress Enter to show the TEST SUITE EXECUTION FLOW...")
    # Clear screen only after user presses Enter
    clear_screen()
    draw_execution_flow_summary(results)

def main():
    parser = argparse.ArgumentParser(description='Run test cases with visual progress')
    parser.add_argument('--test-dir', default='Testcases', help='Directory containing test case YAML files')
    parser.add_argument('--test-file', help='Run a specific test case file')
    parser.add_argument('--run-id', help='Execution run ID for tracking batch runs')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--scheduled', action='store_true', help='Include scheduled tests')
    
    args = parser.parse_args()
    
    global DEBUG
    DEBUG = args.debug
    
    if DEBUG:
        print(f"{YELLOW}Debug mode enabled{NC}")
    
    # Find test case files
    if args.test_file:
        if not os.path.exists(args.test_file):
            print(f"{RED}Test file not found: {args.test_file}{NC}")
            sys.exit(1)
        test_files = [args.test_file]
    else:
        test_files = find_test_cases(args.test_dir)
    
    # Filter out scheduled tests if not explicitly included
    if not args.scheduled:
        original_count = len(test_files)
        test_files = [file for file in test_files if not is_scheduled_test(file)]
        skipped_count = original_count - len(test_files)
        if skipped_count > 0:
            print(f"{YELLOW}Skipped {skipped_count} scheduled test case(s).{NC}")
            print(f"{YELLOW}Use --scheduled flag to include scheduled tests.{NC}")
    
    if not test_files:
        print(f"{RED}No test cases to run after filtering!{NC}")
        sys.exit(1)

    # Create a unique execution run ID if not provided
    execution_run_id = args.run_id if args.run_id else f"visual_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    if DEBUG:
        print(f"{CYAN}Using execution run ID: {execution_run_id}{NC}")
    
    # Run all test cases
    run_all_test_cases(test_files, execution_run_id)

def parse_test_summary(output_lines):
    """
    Parse test execution output to determine test result.
    Returns 'PASSED', 'FAILED', or 'UNKNOWN'.
    """
    if not output_lines:
        return 'UNKNOWN'
        
    # Convert list to string if needed
    if isinstance(output_lines, list):
        output_text = '\n'.join(output_lines)
    else:
        output_text = str(output_lines)
        
    # Look for failure indicators
    failure_indicators = [
        'ERROR',
        'CRITICAL',
        'CATASTROPHIC FAILURE',
        'Test case execution encountered an error',
        'Failed to convert',
        'RuntimeError',
        'Exception',
        'FAILED'
    ]
    
    # Look for success indicators
    success_indicators = [
        'Test case completed successfully',
        'All validations passed',
        'PASSED'
    ]
    
    # Check for failures first
    for indicator in failure_indicators:
        if indicator in output_text:
            return 'FAILED'
            
    # Then check for success
    for indicator in success_indicators:
        if indicator in output_text:
            return 'PASSED'
            
    # If no clear indicators found
    return 'UNKNOWN'

if __name__ == "__main__":
    main() 