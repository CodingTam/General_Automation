import os
import subprocess
from typing import List
import glob
import yaml
import argparse
from datetime import datetime
import concurrent.futures
import logging
from utils.db_handler import DBHandler
from utils.logger import logger, _logger, console_handler
from utils.banner import print_intro

def set_logging_mode(silent=False, quiet=False):
    """
    Configure logging based on silent or quiet mode.
    
    Args:
        silent (bool): If True, suppress INFO level logs
        quiet (bool): If True, only show minimal information
    """
    # Set environment variable to control Spark logging
    if silent:
        os.environ["SPARK_LOG_LEVEL"] = "WARN"
    else:
        os.environ["SPARK_LOG_LEVEL"] = "INFO"
    
    if silent:
        # In silent mode, only show warnings and errors
        logger.configure_logging(level='WARNING', root_level='WARNING')
        
        # Set Spark log level via system property (will be picked up by spark-submit)
        os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.driver.extraJavaOptions=-Dlog4j.rootCategory=WARN,console " + os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    elif quiet:
        # In quiet mode, show only test case names and status
        quiet_format = logging.Formatter('%(message)s')
        logger.configure_logging(level='WARNING', formatter=quiet_format, root_level='WARNING')
    else:
        # Default mode - keep standard formatter and INFO level
        console_format = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        logger.configure_logging(level='INFO', formatter=console_format, root_level='INFO')

def find_yaml_files(test_cases_dir: str) -> List[str]:
    """
    Find all YAML files in the test cases directory.
    
    Args:
        test_cases_dir (str): Path to the test cases directory
    
    Returns:
        List[str]: List of paths to YAML files
    """
    return glob.glob(os.path.join("testcases", "*.yaml"))

def is_scheduled_test(yaml_file: str) -> bool:
    """
    Check if a test case is scheduled based on its YAML file.
    
    Args:
        yaml_file (str): Path to the YAML file
    
    Returns:
        bool: True if the test is scheduled, False otherwise
    """
    try:
        yaml_data = read_yaml_file(yaml_file)
        # Check if scheduler section exists and is enabled
        if 'scheduler' in yaml_data and yaml_data['scheduler'].get('enabled', '').lower() in ['yes', 'true', '1']:
            return True
        return False
    except Exception as e:
        logger.error(f"Error checking if test is scheduled: {e}")
        return False

def read_yaml_file(yaml_file: str) -> dict:
    """
    Read and parse a YAML file.
    
    Args:
        yaml_file (str): Path to the YAML file
    
    Returns:
        dict: Parsed YAML content
    """
    with open(yaml_file, 'r') as file:
        return yaml.safe_load(file)

# Add a function to create execution context similar to what we did in Comparison.py
def create_execution_context(execution_run_id=None, test_case_name=None, traceability_id=None, execution_id=None, parent_context=None):
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
    
    return context

def submit_spark_job(yaml_file: str, db_handler: DBHandler, execution_run_id: str = None, quiet: bool = False, silent: bool = False) -> bool:
    """
    Submit a Spark job for a single YAML test case using spark-submit.
    
    Args:
        yaml_file (str): Path to the YAML file
        db_handler (DBHandler): Database handler instance
        execution_run_id (str, optional): ID of the execution run
        quiet (bool): If True, only show minimal output
        silent (bool): If True, suppress INFO level logs
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Create initial execution context
    exec_context = create_execution_context(execution_run_id=execution_run_id)
    
    # Get the base name of the YAML file for the app name
    test_case_name = os.path.splitext(os.path.basename(yaml_file))[0]
    
    # Update context with test case name
    exec_context['test_case_name'] = test_case_name
    
    logger.info(f"Processing YAML file: {yaml_file}")
    
    # Read YAML file
    yaml_data = read_yaml_file(yaml_file)
    
    # Get actual test case name from YAML
    if 'test_case_name' in yaml_data:
        test_case_name = yaml_data['test_case_name']
        exec_context['test_case_name'] = test_case_name
    
    # For quiet mode, print directly to ensure visibility
    if quiet:
        print(f"Running test: {test_case_name}")
    
    logger.info(f"Inserting test case {test_case_name} into database")
    
    try:
        # Insert test case into database and get traceability ID
        traceability_id = db_handler.insert_test_case(yaml_data)
        if not traceability_id:
            logger.error(f"Failed to get traceability ID for test case: {test_case_name}")
            return False
            
        # Update context with traceability ID
        exec_context['traceability_id'] = traceability_id
        
        sid = yaml_data.get('sid')
        table_name = yaml_data.get('table_name')
        logger.info(f"Test case inserted with traceability ID: {traceability_id}")
        
        # Create a copy of the current environment
        env = os.environ.copy()
        
        # Set environment variables for logging level
        if silent:
            env["AUTOMATION_LOG_LEVEL"] = "WARNING"
            env["SPARK_LOGGING_LEVEL"] = "WARN" 
        elif quiet:
            env["AUTOMATION_LOG_LEVEL"] = "QUIET"
            env["SPARK_LOGGING_LEVEL"] = "WARN"
        else:
            env["AUTOMATION_LOG_LEVEL"] = "INFO"
            env["SPARK_LOGGING_LEVEL"] = "INFO"
        
        # Construct the spark-submit command with execution_run_id if provided
        spark_submit_cmd = [
            "spark-submit",
        ]
        
        # Add log level configuration if in silent mode
        if silent:
            spark_submit_cmd.extend([
                "--conf", "spark.driver.extraJavaOptions=-Dlog4j.rootCategory=WARN,console -Dlog4j.threshold=WARN",
                "--conf", "spark.executor.extraJavaOptions=-Dlog4j.rootCategory=WARN,console -Dlog4j.threshold=WARN",
                "--conf", "spark.log.level=WARN",
                "--conf", "spark.logger.level=WARN"
            ])
        elif quiet:
            spark_submit_cmd.extend([
                "--conf", "spark.driver.extraJavaOptions=-Dlog4j.rootCategory=WARN,console -Dlog4j.threshold=WARN",
                "--conf", "spark.executor.extraJavaOptions=-Dlog4j.rootCategory=WARN,console -Dlog4j.threshold=WARN",
                "--conf", "spark.log.level=WARN",
                "--conf", "spark.logger.level=WARN"
            ])
        
        # Add network timeout configurations
        spark_submit_cmd.extend([
            "--conf", "spark.network.timeout=120s",
            "--conf", "spark.executor.heartbeatInterval=60s",
            "--conf", "spark.storage.blockManagerSlaveTimeoutMs=120000",
            "--conf", "spark.rpc.askTimeout=120s",
            "--conf", "spark.rpc.lookupTimeout=120s"
        ])
        
        # Continue with standard configuration
        spark_submit_cmd.extend([
            "--master", "local[*]",
            "--name", f"DataComparison_{test_case_name}",
            "validation/Comparison.py",
            "--single",
            "--yaml-file", yaml_file,
            "--traceability-id", traceability_id
        ])
        
        # Add execution_run_id if provided
        if execution_run_id:
            spark_submit_cmd.extend(["--run-id", execution_run_id])
        
        logger.info(f"Submitting Spark job for test case: {test_case_name}")
        logger.debug(f"Command: {' '.join(spark_submit_cmd)}")
        
        try:
            # Execute the spark-submit command with the modified environment
            subprocess.run(spark_submit_cmd, check=True, env=env)
            logger.info(f"Successfully submitted job for {test_case_name}")
            
            # For quiet mode, print directly to ensure visibility
            if quiet:
                print(f"Completed test: {test_case_name} - SUCCESS")
                
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Error executing spark-submit for {test_case_name}: {e}")
            
            # For quiet mode, print directly to ensure visibility
            if quiet:
                print(f"Completed test: {test_case_name} - FAILED")
            
            return False
            
    except Exception as e:
        logger.error(f"Error in submit_spark_job: {str(e)}")
        return False

def run_scheduled_job(yaml_file: str, traceability_id: str, execution_run_id: str, schedule_id: str = None, quiet: bool = False, silent: bool = False) -> bool:
    """
    Run a scheduled test case with existing traceability ID.
    
    Args:
        yaml_file (str): Path to the YAML file
        traceability_id (str): Existing traceability ID
        execution_run_id (str): Execution run ID
        schedule_id (str, optional): Schedule ID for the test
        quiet (bool): If True, only show minimal output
        silent (bool): If True, suppress INFO level logs
        
    Returns:
        bool: True if successful, False otherwise
    """
    test_case_name = os.path.splitext(os.path.basename(yaml_file))[0]
    
    # For quiet mode, print directly to ensure visibility
    if quiet:
        print(f"Running test: {test_case_name}")
    
    # Create a copy of the current environment
    env = os.environ.copy()
    
    # Set environment variables for logging level
    if silent:
        env["AUTOMATION_LOG_LEVEL"] = "WARNING"
        env["SPARK_LOGGING_LEVEL"] = "WARN"
    elif quiet:
        env["AUTOMATION_LOG_LEVEL"] = "QUIET"
        env["SPARK_LOGGING_LEVEL"] = "WARN"
    else:
        env["AUTOMATION_LOG_LEVEL"] = "INFO"
        env["SPARK_LOGGING_LEVEL"] = "INFO"
    
    # Construct the spark-submit command with execution_run_id
    spark_submit_cmd = [
        "spark-submit",
    ]
    
    # Add log level configuration if in silent mode
    if silent:
        spark_submit_cmd.extend([
            "--conf", "spark.driver.extraJavaOptions=-Dlog4j.rootCategory=WARN,console -Dlog4j.threshold=WARN",
            "--conf", "spark.executor.extraJavaOptions=-Dlog4j.rootCategory=WARN,console -Dlog4j.threshold=WARN",
            "--conf", "spark.log.level=WARN",
            "--conf", "spark.logger.level=WARN"
        ])
    elif quiet:
        spark_submit_cmd.extend([
            "--conf", "spark.driver.extraJavaOptions=-Dlog4j.rootCategory=WARN,console -Dlog4j.threshold=WARN",
            "--conf", "spark.executor.extraJavaOptions=-Dlog4j.rootCategory=WARN,console -Dlog4j.threshold=WARN",
            "--conf", "spark.log.level=WARN",
            "--conf", "spark.logger.level=WARN"
        ])
    
    # Continue with standard configuration
    spark_submit_cmd.extend([
        "--master", "local[*]",
        "--name", f"DataComparison_{test_case_name}",
        "validation/Comparison.py",
        "--single",
        "--yaml-file", yaml_file,
        "--traceability-id", traceability_id,
        "--run-id", execution_run_id
    ])
    
    # Add schedule_id if provided
    if schedule_id:
        spark_submit_cmd.extend(["--schedule-id", schedule_id])
    
    logger.info(f"Submitting Spark job for test case: {test_case_name}")
    logger.debug(f"Command: {' '.join(spark_submit_cmd)}")
    
    try:
        # Execute the spark-submit command with the modified environment
        subprocess.run(spark_submit_cmd, check=True, env=env)
        logger.info(f"Successfully submitted job for {test_case_name}")
        
        # For quiet mode, print directly to ensure visibility
        if quiet:
            print(f"Completed test: {test_case_name} - SUCCESS")
        
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error submitting job for {test_case_name}: {e}")
        
        # For quiet mode, print directly to ensure visibility
        if quiet:
            print(f"Completed test: {test_case_name} - FAILED")
        
        return False

def main():
    # Display the introductory banner
    print_intro("QA")  # Set the environment as needed
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run test cases')
    parser.add_argument('yaml_file', nargs='?', help='Specific YAML file to process')
    parser.add_argument('--scheduled', action='store_true', help='Flag to indicate this is a scheduled run')
    parser.add_argument('--traceability-id', help='Existing traceability ID for the test case')
    parser.add_argument('--schedule-id', help='Schedule ID for scheduled test case')
    parser.add_argument('--test-dir', default='Testcases', help='Directory containing test case YAML files')
    parser.add_argument('--pattern', help='File pattern for YAML files (e.g. "TC_*.yaml")')
    parser.add_argument('--execution_run_id', help='Execution run ID for tracking batch runs')
    
    # Add new arguments for silent, quiet, and parallel modes
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--silent', action='store_true', help='Silent mode: suppress INFO level logs')
    group.add_argument('--quiet', action='store_true', help='Quiet mode: only show minimal output (test case name and completion status)')
    
    parser.add_argument('--parallel', nargs='?', const='2', type=int, 
                        metavar='N', help='Run test cases in parallel with N workers (default: 2)')
    
    args = parser.parse_args()
    
    # Configure logging based on silent/quiet mode
    set_logging_mode(silent=args.silent, quiet=args.quiet)
    
    # Initialize database handler
    db_handler = DBHandler()
    
    # Create initial execution context
    exec_context = create_execution_context()
    
    try:
        # Configuration
        test_cases_dir = args.test_dir  # Directory containing YAML test cases
        
        # Determine which YAML files to process
        yaml_files = []
        
        if args.yaml_file:
            # If a specific YAML file is provided, use only that one
            if os.path.exists(args.yaml_file):
                yaml_files = [args.yaml_file]
            else:
                logger.error(f"Specified YAML file not found: {args.yaml_file}")
                return
        elif args.pattern:
            # If a pattern is provided, find matching files
            yaml_files = glob.glob(os.path.join(test_cases_dir, args.pattern))
        else:
            # Otherwise, find all YAML files in the directory
            yaml_files = find_yaml_files(test_cases_dir)
        
        if not yaml_files:
            logger.warning(f"No YAML files found to process")
            return
        
        # Filter out scheduled tests if this is not a scheduled run
        if not args.scheduled:
            original_count = len(yaml_files)
            yaml_files = [file for file in yaml_files if not is_scheduled_test(file)]
            skipped_count = original_count - len(yaml_files)
            if skipped_count > 0:
                logger.info(f"Skipped {skipped_count} scheduled test case(s). Use --scheduled flag to run them.")
        
        if not yaml_files:
            logger.warning(f"No non-scheduled YAML files found to process")
            return
        
        logger.info(f"Found {len(yaml_files)} test case(s) to process")
        
        # Create an execution run for this batch
        run_name = f"{'Scheduled' if args.scheduled else 'Batch'} Run {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        user_id = "automatic_system"  # Can be replaced with actual user info
        notes = f"Running {len(yaml_files)} test cases from {test_cases_dir}"
        
        logger.info(f"Creating execution run with name: {run_name}")
        execution_run_id = db_handler.create_execution_run(
            run_name=run_name,
            executed_by=user_id,
            notes=notes
        )
        
        # Update context with execution_run_id
        exec_context['execution_run_id'] = execution_run_id
        
        logger.info(f"Created execution run with ID: {execution_run_id}")
        
        success_count = 0
        failure_count = 0
        
        # If parallel execution is enabled, use concurrent.futures
        if args.parallel:
            max_workers = args.parallel
            logger.info(f"Running tests in parallel with {max_workers} workers")
            
            # For quiet mode, print directly to ensure visibility
            if args.quiet:
                print(f"Starting {len(yaml_files)} tests with {max_workers} parallel workers")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                if args.scheduled and args.traceability_id:
                    # For scheduled runs with existing traceability_id
                    futures = {
                        executor.submit(
                            run_scheduled_job, 
                            yaml_file, 
                            args.traceability_id, 
                            execution_run_id, 
                            args.schedule_id,
                            args.quiet,
                            args.silent
                        ): yaml_file for yaml_file in yaml_files
                    }
                else:
                    # For regular test runs
                    futures = {
                        executor.submit(
                            submit_spark_job, 
                            yaml_file, 
                            db_handler, 
                            execution_run_id,
                            args.quiet,
                            args.silent
                        ): yaml_file for yaml_file in yaml_files
                    }
                
                # Process futures as they complete
                for future in concurrent.futures.as_completed(futures):
                    yaml_file = futures[future]
                    test_case_name = os.path.splitext(os.path.basename(yaml_file))[0]
                    
                    try:
                        success = future.result()
                        if success:
                            success_count += 1
                        else:
                            failure_count += 1
                    except Exception as e:
                        logger.error(f"Error processing {test_case_name}: {str(e)}")
                        failure_count += 1
        else:
            # Sequential execution (original behavior)
            logger.info(f"Running tests sequentially")
            
            for yaml_file in yaml_files:
                try:
                    # For scheduled runs with existing traceability_id, don't insert into testcase table
                    if args.scheduled and args.traceability_id:
                        success = run_scheduled_job(
                            yaml_file, 
                            args.traceability_id, 
                            execution_run_id, 
                            args.schedule_id,
                            args.quiet,
                            args.silent
                        )
                        if success:
                            success_count += 1
                        else:
                            failure_count += 1
                    else:
                        # Regular flow - insert into testcase table first
                        success = submit_spark_job(
                            yaml_file, 
                            db_handler, 
                            execution_run_id, 
                            args.quiet,
                            args.silent
                        )
                        if success:
                            success_count += 1
                        else:
                            failure_count += 1
                except Exception as e:
                    logger.error(f"Error processing {yaml_file}: {str(e)}")
                    failure_count += 1
        
        # Complete the execution run
        total_count = success_count + failure_count
        additional_notes = f"Summary: {success_count} succeeded, {failure_count} failed out of {total_count} test cases."
        logger.info(f"Completing execution run. {additional_notes}")
        
        db_handler.complete_execution_run(execution_run_id, additional_notes)
        
        # Get and print the final summary
        execution_summary = db_handler.get_execution_run_summary(execution_run_id)
        logger.info(f"Execution run completed with status: {execution_summary['overall_status']}")
        
        logger.info(f"Total: {execution_summary['total_test_cases']}, "
                   f"Passed: {execution_summary['passed_test_cases']}, "
                   f"Failed: {execution_summary['failed_test_cases']}")
        
        # For quiet mode, print the summary directly
        if args.quiet:
            print(f"\nExecution Summary: {execution_summary['overall_status']}")
            print(f"Total: {execution_summary['total_test_cases']}, "
                 f"Passed: {execution_summary['passed_test_cases']}, "
                 f"Failed: {execution_summary['failed_test_cases']}")
        
    finally:
        logger.info("Closing database connection")
        db_handler.close()

if __name__ == "__main__":
    main() 