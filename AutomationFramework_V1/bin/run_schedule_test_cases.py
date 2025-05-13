#!/usr/bin/env python3
"""
Script to run scheduled test cases from the scheduler table.

This script:
1. Reads test cases from the scheduler table that are due for execution
2. Runs each test case using spark-submit
3. Updates the scheduler table with the execution results
"""

import os
import subprocess
import argparse
from datetime import datetime
import time
import yaml
from utils.db_handler import DBHandler
from utils.custom_logger import get_logger
from utils.banner import print_intro

# Initialize logger
logger = get_logger()

def create_execution_context(execution_run_id=None, test_case_name=None, traceability_id=None, 
                            execution_id=None, schedule_id=None, parent_context=None):
    """Create a dictionary with execution context for logging"""
    # Start with empty context
    context = {}
    
    # If parent_context is provided, use it as a base to preserve existing values
    if parent_context and isinstance(parent_context, dict):
        context.update(parent_context)
    
    # Add/update with explicitly provided values, only if not None
    if execution_run_id is not None:
        context['execution_run_id'] = execution_run_id
    if test_case_name is not None:
        context['test_case_name'] = test_case_name
    if traceability_id is not None:
        context['traceability_id'] = traceability_id
    if execution_id is not None:
        context['execution_id'] = execution_id
    if schedule_id is not None:
        context['schedule_id'] = schedule_id
    
    return context

def submit_spark_job(yaml_file_path, traceability_id, schedule_id, db_handler, execution_run_id=None):
    """
    Submit a Spark job for a scheduled test case using spark-submit.
    
    Args:
        yaml_file_path (str): Path to the YAML file
        traceability_id (str): Traceability ID of the test case
        schedule_id (str): Schedule ID from the scheduler table
        db_handler (DBHandler): Database handler instance
        execution_run_id (str, optional): ID of the execution run
    """
    # Create initial execution context
    exec_context = create_execution_context(
        execution_run_id=execution_run_id,
        traceability_id=traceability_id,
        schedule_id=schedule_id
    )
    
    # Read YAML file to get test case name
    try:
        with open(yaml_file_path, 'r') as file:
            yaml_data = yaml.safe_load(file)
            test_case_name = yaml_data.get('test_case_name')
            exec_context['test_case_name'] = test_case_name
    except Exception as e:
        logger.error(f"Error reading YAML file {yaml_file_path}: {e}", error=e, **exec_context)
        # Update scheduler with error status
        db_handler.update_scheduler_after_execution(schedule_id, "ERROR")
        return
    
    logger.info(f"Processing scheduled test case: {test_case_name}", **exec_context)
    
    # Construct the spark-submit command
    spark_submit_cmd = [
        "spark-submit",
        "--master", "local[*]",
        "--name", f"DataComparison_{test_case_name}",
        "validation/Comparison.py",
        "--single",
        "--yaml-file", yaml_file_path,
        "--traceability-id", traceability_id
    ]
    
    # Add execution_run_id if provided
    if execution_run_id:
        spark_submit_cmd.extend(["--run-id", execution_run_id])
    
    # Add schedule_id for tracking in logs
    spark_submit_cmd.extend(["--schedule-id", schedule_id])
    
    logger.info(f"Submitting Spark job for scheduled test case: {test_case_name}", **exec_context)
    logger.debug(f"Command: {' '.join(spark_submit_cmd)}", **exec_context)
    
    try:
        # Execute the spark-submit command
        process = subprocess.Popen(spark_submit_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate()
        
        # Check for errors
        if process.returncode != 0:
            logger.error(f"Error executing spark-submit for {test_case_name}: {stderr}", **exec_context)
            db_handler.update_scheduler_after_execution(schedule_id, "FAILED")
        else:
            logger.info(f"Successfully completed job for {test_case_name}", **exec_context)
            db_handler.update_scheduler_after_execution(schedule_id, "COMPLETED")
    except Exception as e:
        logger.error(f"Exception running spark-submit for {test_case_name}: {str(e)}", error=e, **exec_context)
        db_handler.update_scheduler_after_execution(schedule_id, "ERROR")

def main():
    # Display the introductory banner
    print_intro("QA")  # Set the environment as needed
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run scheduled test cases from the scheduler table.")
    parser.add_argument("--continuous", action="store_true", help="Run in continuous mode, checking for scheduled tests every interval")
    parser.add_argument("--interval", type=int, default=300, help="Check interval in seconds (default: 300 seconds = 5 minutes)")
    args = parser.parse_args()
    
    continuous_mode = args.continuous
    check_interval = args.interval
    
    # Initialize database handler
    db_handler = DBHandler()
    
    try:
        # If running in continuous mode, keep checking for scheduled tests
        while True:
            # Create initial execution context
            exec_context = create_execution_context()
            
            # Create an execution run for this batch
            run_name = f"Scheduled Run {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            user_id = "scheduler_system"
            notes = "Running scheduled test cases"
            
            logger.info(f"Creating execution run with name: {run_name}")
            execution_run_id = db_handler.create_execution_run(
                run_name=run_name,
                executed_by=user_id,
                notes=notes
            )
            
            # Update context with execution_run_id
            exec_context['execution_run_id'] = execution_run_id
            
            # Get scheduled test cases that are due for execution
            scheduled_tests = db_handler.get_scheduled_test_cases()
            
            if not scheduled_tests:
                logger.info("No scheduled test cases due for execution", **exec_context)
                
                # Complete the execution run with no tests
                db_handler.complete_execution_run(execution_run_id, "No test cases to execute")
                
                # If not in continuous mode, exit
                if not continuous_mode:
                    break
                
                # In continuous mode, wait before checking again
                logger.info(f"Waiting {check_interval} seconds before checking again...", **exec_context)
                time.sleep(check_interval)
                continue
            
            logger.info(f"Found {len(scheduled_tests)} scheduled test cases to execute", **exec_context)
            
            # Submit each scheduled test case as a separate Spark job
            success_count = 0
            failure_count = 0
            
            for test in scheduled_tests:
                try:
                    schedule_id = test['schedule_id']
                    traceability_id = test['traceability_id']
                    yaml_file_path = test['yaml_file_path']
                    test_case_name = test['test_case_name']
                    
                    # Update context with schedule-specific information
                    test_context = create_execution_context(
                        execution_run_id=execution_run_id,
                        test_case_name=test_case_name,
                        traceability_id=traceability_id,
                        schedule_id=schedule_id,
                        parent_context=exec_context
                    )
                    
                    logger.info(f"Processing scheduled test: {test_case_name}", **test_context)
                    
                    # Submit the job
                    submit_spark_job(yaml_file_path, traceability_id, schedule_id, db_handler, execution_run_id)
                    success_count += 1
                except Exception as e:
                    logger.error(f"Error processing scheduled test case: {str(e)}", error=e, **exec_context)
                    failure_count += 1
            
            # Complete the execution run
            total_count = success_count + failure_count
            additional_notes = f"Scheduled summary: {success_count} succeeded, {failure_count} failed out of {total_count} test cases."
            logger.info(f"Completing execution run. {additional_notes}", **exec_context)
            
            db_handler.complete_execution_run(execution_run_id, additional_notes)
            
            # Get and print the final summary
            execution_summary = db_handler.get_execution_run_summary(execution_run_id)
            logger.info(f"Execution run completed with status: {execution_summary['overall_status']}", **exec_context)
            
            logger.info(f"Total: {execution_summary['total_test_cases']}, "
                      f"Passed: {execution_summary['passed_test_cases']}, "
                      f"Failed: {execution_summary['failed_test_cases']}", **exec_context)
            
            # If not in continuous mode, exit after one run
            if not continuous_mode:
                break
            
            # In continuous mode, wait before checking again
            logger.info(f"Waiting {check_interval} seconds before checking again...", **exec_context)
            time.sleep(check_interval)
    
    finally:
        logger.info("Closing database connection")
        db_handler.close()

if __name__ == "__main__":
    main() 