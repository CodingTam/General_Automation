from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import os
import argparse
import sqlite3
from datetime import datetime
import uuid

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Now import modules from utils
from utils.db_handler import DBHandler
from utils.logger import logger, function_logger
from utils.utils import find_key_columns
from validation.comparison_utils import perform_comparison, generate_comparison_report, generate_html_report
from utils.yaml_processor import process_test_case
from utils.db_config import db_config

def create_execution_context(traceability_id=None, execution_run_id=None, test_case_name=None, execution_id=None, schedule_id=None, parent_context=None):
    """Create a dictionary with execution context for logging"""
    # Start with empty context
    context = {}
    
    # If parent_context is provided, use it as a base to preserve existing values
    if parent_context and isinstance(parent_context, dict):
        context.update(parent_context)
    
    # Add/update with explicitly provided values, only if not None
    if traceability_id is not None:
        context['traceability_id'] = traceability_id
    if execution_run_id is not None:
        context['execution_run_id'] = execution_run_id
    if test_case_name is not None:
        context['test_case_name'] = test_case_name
    if execution_id is not None:
        context['execution_id'] = execution_id
    if schedule_id is not None:
        context['schedule_id'] = schedule_id
    
    return context

def extract_status_from_summary(summary, validation_type):
    for row in summary:
        if row["Validation Type"].lower().startswith(validation_type.lower()):
            return row["Status"]
    return "NULL"

@function_logger
def main(yaml_file: str, traceability_id: str, execution_run_id: str = None, schedule_id: str = None):
    spark = None
    try:
        # Initialize database handler with the new class name
        db_handler = DBHandler()
        
        # Create initial execution context
        exec_context = create_execution_context(
            traceability_id=traceability_id,
            execution_run_id=execution_run_id,
            schedule_id=schedule_id
        )
        
        try:
            # Initialize Spark session
            logger.info(f"Creating Spark session for test case: {os.path.basename(yaml_file)}", 
                       **exec_context)
            
            # Load database configuration
            db_settings = db_config.get_all_config()
            
            # Create Spark session with database configuration
            spark = SparkSession.builder \
                .appName(f"Data Comparison - {os.path.basename(yaml_file)}") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
                .config("spark.network.timeout", "120s") \
                .config("spark.executor.heartbeatInterval", "60s") \
                .config("spark.storage.blockManagerSlaveTimeoutMs", "120000") \
                .config("spark.rpc.askTimeout", "120s") \
                .config("spark.rpc.lookupTimeout", "120s") \
                .config("spark.jars", "drivers/mssql-jdbc-12.6.2.jre11.jar") \
                .config("spark.driver.extraClassPath", "drivers/mssql-jdbc-12.6.2.jre11.jar") \
                .getOrCreate()
            
            # Record start time
            start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            try:
                # Process test case
                logger.info(f"Processing YAML test case file: {yaml_file}", 
                           **exec_context)
                
                # Generate a test case ID and execution ID
                test_case_id = str(uuid.uuid4())
                execution_id = str(uuid.uuid4())
                
                # Update context with IDs
                exec_context.update({
                    'test_case_id': test_case_id,
                    'execution_id': execution_id
                })
                
                # Process test case with execution context
                test_config = process_test_case(
                    yaml_file,
                    spark,
                    execution_id=execution_id,
                    test_case_id=test_case_id,
                    traceability_id=traceability_id
                )
                test_case_name = test_config.get('test_case_name')
                
                # Update context with test case name
                exec_context['test_case_name'] = test_case_name
                
                logger.info(f"Test case loaded: {test_case_name}", 
                           **exec_context)
                
                # Debug the SID in test_config
                sid = test_config.get('sid')
                logger.debug(f"SID from test config: '{sid}'", 
                            **exec_context)
                
                # Show initial data
                logger.info(f"Source DataFrame loaded with {test_config['source_df'].count()} rows",
                           **exec_context)
                
                logger.info(f"Target DataFrame loaded with {test_config['target_df'].count()} rows",
                           **exec_context)
                
                # Log validation types
                logger.info(f"Validation types: {', '.join(test_config['validation_types'])}",
                           **exec_context)
                
                # Perform comparison
                logger.info("Starting data comparison", 
                           **exec_context)
                
                results = perform_comparison(
                    source_df=test_config["source_df"],
                    target_df=test_config["target_df"],
                    validation_types=test_config["validation_types"],
                    rules=test_config["rules"]
                )
                
                logger.info("Data comparison completed", 
                           **exec_context)
                
                # Add SID to results
                results["sid"] = test_config.get('sid')
                # Add table_name to results
                results["table_name"] = test_config.get('table_name')
                
                # Generate report
                logger.info("Generating text comparison report", 
                           **exec_context)
                
                generate_comparison_report(results)
                
                # Check if HTML report generation is enabled in the YAML config
                html_report_enabled = test_config.get('generate_html_report', False)
                if html_report_enabled:
                    # Create a directory for reports if it doesn't exist
                    os.makedirs('reports', exist_ok=True)
                    
                    # Generate HTML report
                    report_filename = f"reports/{test_config['test_case_name']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
                    logger.info(f"Generating HTML report: {report_filename}", 
                               **exec_context)
                    
                    generate_html_report(results, report_filename)
                
                # Extract summary from results after report generation
                summary = results.get("final_summary", [])
                logger.debug("Extracting validation statuses from summary", 
                            **exec_context)
                
                count_status = extract_status_from_summary(summary, "Count Validation")
                schema_status = extract_status_from_summary(summary, "Schema Validation")
                data_status = extract_status_from_summary(summary, "Data Validation")
                rule_status = extract_status_from_summary(summary, "Rule Validation")
                overall_status = extract_status_from_summary(summary, "Overall Status")
                null_status = extract_status_from_summary(summary, "Null")
                duplicate_status = extract_status_from_summary(summary, "Duplicate")
                
                logger.info(f"Overall validation status: {overall_status}", 
                           **exec_context)
                
                # Get source/target count from count_validation
                source_count = results.get('count_validation', {}).get('source_count', 0)
                target_count = results.get('count_validation', {}).get('target_count', 0)
                execution_results = {
                    'start_time': start_time,
                    'end_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'status': overall_status,
                    'schema_validation': {'status': schema_status},
                    'count_validation': {'status': count_status},
                    'data_validation': {'status': data_status},
                    'null_check': {'status': null_status},
                    'duplicate_check': {'status': duplicate_status},
                    'rule_validation': {'status': rule_status},
                    'source_count': source_count,
                    'target_count': target_count,
                    'mismatched_records': results.get('mismatched_records', 0),
                    'null_records': results.get('null_records', 0),
                    'duplicate_records': results.get('duplicate_records', 0),
                    'error_message': None,
                    'table_name': test_config.get('table_name'),
                    'test_case_name': test_config.get('test_case_name'),
                    'source_type': test_config.get('source_type'),
                    'source_version': test_config.get('source_version'),
                    'source_module': test_config.get('source_module'),
                    'target_type': test_config.get('target_type'),
                    'target_version': test_config.get('target_version'),
                    'target_module': test_config.get('target_module')
                }
                
                # Insert execution stats
                logger.info("Storing execution results in database", 
                           **exec_context)
                
                sid = test_config.get('sid')
                try:
                    execution_id = db_handler.insert_execution_stats(traceability_id, execution_results, test_config.get('jira', {}), sid, execution_run_id)
                    
                    # Create a new context with the execution_id for subsequent calls
                    # This ensures that all future logs use this execution_id
                    exec_context['execution_id'] = execution_id
                    
                    # Use direct logging to verify
                    logger.direct_log("INFO", f"Successfully stored execution results for test case {test_case_name}", execution_id)
                    
                    logger.info("Successfully stored execution results", 
                               **exec_context)
                except sqlite3.Error as e:
                    logger.error(f"SQLite error while storing execution results: {e}", 
                                error=e, **exec_context)
                    
                    # Use direct logging
                    emergency_id = f"EMERGENCY-{str(uuid.uuid4())}"
                    logger.direct_log("ERROR", f"SQLite error while storing execution results: {e}", emergency_id, str(e))
                    
                    logger.info("Trying again without execution_run_id", 
                               **exec_context)
                    
                    try:
                        execution_id = db_handler.insert_execution_stats(traceability_id, execution_results, test_config.get('jira', {}), sid, None)
                        
                        # Update context with execution_id
                        exec_context['execution_id'] = execution_id
                        
                        # Use direct logging
                        logger.direct_log("INFO", f"Successfully stored execution results (without execution_run_id)", execution_id)
                        
                        logger.info("Successfully stored execution results (without execution_run_id)", 
                                   **exec_context)
                    except Exception as inner_e:
                        # Use direct logging
                        emergency_id = f"EMERGENCY-{str(uuid.uuid4())}"
                        logger.direct_log("CRITICAL", f"Failed to store execution results on second attempt: {inner_e}", emergency_id, str(inner_e))
                        
                        logger.critical("Failed to store execution results on second attempt", 
                                       error=inner_e, **exec_context)
                        raise inner_e
                
            except Exception as e:
                logger.info(f"Test case execution encountered an error", **exec_context)
                
                # Record error in execution stats
                execution_results = {
                    'start_time': start_time,
                    'end_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'status': 'FAILED',
                    'error_message': str(e)
                }
                # Add table_name to execution_results 
                if 'test_config' in locals() and test_config:
                    execution_results['table_name'] = test_config.get('table_name')
                    execution_results['test_case_name'] = test_config.get('test_case_name')
                    test_case_name = test_config.get('test_case_name')
                
                logger.info("Storing failed execution results in database", **exec_context)
                
                sid = test_config.get('sid') if 'test_config' in locals() else None
                try:
                    # Insert execution stats and get execution_id BEFORE logging the detailed error
                    execution_id = db_handler.insert_execution_stats(traceability_id, execution_results, test_config.get('jira', {}) if 'test_config' in locals() else None, sid, execution_run_id)
                    
                    # Use direct logging 
                    logger.direct_log("ERROR", f"Test case execution failed: {str(e)}", execution_id, str(e))
                    
                    # Debug the execution_id explicitly
                    logger.debug(f"After insert_execution_stats, got execution_id: {execution_id}")
                    
                    # Use special debug logger to verify execution_id
                    module_name = __name__
                    function_name = "main"
                    logger.debug_log_with_execution_id(module_name, function_name, 
                                                    "Verifying execution_id after insert_execution_stats", 
                                                    execution_id=execution_id)
                    
                    # Update context with execution_id - make sure it's not None
                    if execution_id:
                        exec_context['execution_id'] = execution_id
                        
                        # Use direct logging with the execution_id
                        logger.direct_log(
                            "ERROR", 
                            f"Direct-logged test case execution failure: {str(e)}", 
                            execution_id,
                            str(e)
                        )
                    else:
                        logger.warning(f"Got None for execution_id from insert_execution_stats!")
                        
                        # Use direct logging with a hardcoded ID
                        hardcoded_id = f"HARDCODED-{str(uuid.uuid4())}"
                        logger.direct_log(
                            "ERROR", 
                            f"Direct-logged with hardcoded ID - test case execution failure: {str(e)}", 
                            hardcoded_id,
                            str(e)
                        )
                    
                    # Explicitly log context contents for debugging
                    logger.debug(f"Updated exec_context: {exec_context}")
                    
                    # Use special debug again with context
                    logger.debug_log_with_execution_id(module_name, function_name, 
                                                    "Verifying execution_id through context", 
                                                    **exec_context)
                    
                    # Log with explicit execution_id parameter for certainty
                    logger.error(f"Test case execution failed: {str(e)}", 
                                error=e, 
                                **exec_context)
                except Exception as db_e:
                    # Use direct logging with hardcoded ID
                    emergency_id = f"EMERGENCY-DB-{str(uuid.uuid4())}"
                    logger.direct_log("CRITICAL", f"Failed to store failure information in database: {db_e}", emergency_id, f"Original error: {e}, DB error: {db_e}")
                    
                    logger.critical("Failed to store failure information in database", error=db_e, **exec_context)
                raise e
                
            finally:
                logger.info("Stopping Spark session", **exec_context)
                spark.stop()
                
        finally:
            logger.info("Closing database connection", **exec_context)
            db_handler.close()
    except Exception as e:
        logger.error(f"Critical error in main execution: {str(e)}", error=e)
        raise
    finally:
        if spark:
            try:
                spark.stop()
                logger.info("Spark session stopped successfully", **exec_context)
            except Exception as e:
                logger.error(f"Error stopping Spark session: {str(e)}", error=e, **exec_context)

@function_logger
def run_batch(yaml_files, user_id=None, environment=None, run_name=None, execution_run_id=None):
    """
    Run multiple test cases in a single batch execution.
    
    Args:
        yaml_files (list): List of YAML file paths to execute
        user_id (str, optional): ID of the user running the batch
        environment (str, optional): Environment in which tests are running
        run_name (str, optional): Name for this execution run
        execution_run_id (str, optional): Execution run ID for batch runs
    """
    # Initialize database handler with the new class name
    db_handler = DBHandler()
    
    try:
        # Use provided execution_run_id or create a new one
        if not execution_run_id:
            # Create a new execution run
            notes = f"Batch execution of {len(yaml_files)} test cases"
            logger.info(f"Creating execution run with name: {run_name or 'Batch Run'}")
            
            execution_run_id = db_handler.create_execution_run(
                run_name=run_name, 
                executed_by=user_id,
                notes=notes
            )
            logger.info(f"Created execution run with ID: {execution_run_id}")
        else:
            logger.info(f"Using provided execution run ID: {execution_run_id}")
        
        # Process each test case
        success_count = 0
        failure_count = 0
        
        for yaml_file in yaml_files:
            yaml_name = os.path.basename(yaml_file)
            try:
                logger.info(f"Processing test case: {yaml_name}", execution_run_id=execution_run_id)
                
                # Insert test case into database to get traceability ID
                with open(yaml_file, 'r') as f:
                    yaml_content = f.read()
                
                from utils.yaml_processor import parse_yaml_to_dict
                yaml_data = parse_yaml_to_dict(yaml_content)
                test_case_name = yaml_data.get('test_case_name', yaml_name)
                
                logger.info(f"Inserting test case into database: {test_case_name}", 
                           execution_run_id=execution_run_id,
                           test_case_name=test_case_name)
                
                traceability_id = db_handler.insert_test_case(yaml_data)
                
                # Update context with test case information
                batch_exec_context = {
                    'execution_run_id': execution_run_id,
                    'test_case_name': test_case_name,
                    'traceability_id': traceability_id
                }
                
                logger.info(f"Running test case with traceability ID: {traceability_id}", 
                           **batch_exec_context)
                
                # Run test case
                main(yaml_file, traceability_id, execution_run_id)
                success_count += 1
                logger.info(f"Successfully completed test case: {test_case_name}", 
                           **batch_exec_context)
            except Exception as e:
                # Create execution results to store the error
                error_results = {
                    'start_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'end_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'status': 'FAILED',
                    'error_message': str(e),
                    'test_case_name': test_case_name if 'test_case_name' in locals() else yaml_name
                }
                
                # Only try to record in database if we have a traceability_id
                if 'traceability_id' in locals():
                    try:
                        # Log first without execution_id
                        logger.info(f"Recording batch error for {yaml_name}", execution_run_id=execution_run_id)
                        
                        # Create execution record to get execution_id
                        execution_id = db_handler.insert_execution_stats(
                            traceability_id, 
                            error_results,
                            execution_run_id=execution_run_id
                        )
                        
                        # Update context with execution_id
                        batch_exec_context['execution_id'] = execution_id
                        
                        # Now log with the complete context including execution_id
                        logger.error(f"Error processing {yaml_name}: {str(e)}", 
                                    error=e, 
                                    **batch_exec_context)
                    except Exception as db_e:
                        logger.critical(f"Failed to record batch error in database: {db_e}", 
                                      error=db_e, 
                                      execution_run_id=execution_run_id)
                else:
                    # If we don't have a traceability_id, we can't record in the database
                    logger.error(f"Error processing {yaml_name}: {str(e)}", 
                                error=e, 
                                execution_run_id=execution_run_id,
                                test_case_name=test_case_name if 'test_case_name' in locals() else yaml_name)
                
                failure_count += 1
        
        # Complete the execution run
        total_count = success_count + failure_count
        additional_notes = f"Manual summary: {success_count} succeeded, {failure_count} failed out of {total_count} test cases."
        logger.info(f"Completing execution run. {additional_notes}", execution_run_id=execution_run_id)
        
        db_handler.complete_execution_run(execution_run_id, additional_notes)
        
        # Get the final summary
        execution_summary = db_handler.get_execution_run_summary(execution_run_id)
        logger.info(f"Execution run completed with status: {execution_summary['overall_status']}",
                   execution_run_id=execution_run_id)
        
        logger.info(f"Total: {execution_summary['total_test_cases']}, "
                   f"Passed: {execution_summary['passed_test_cases']}, "
                   f"Failed: {execution_summary['failed_test_cases']}",
                   execution_run_id=execution_run_id)
        
    finally:
        logger.info("Closing database connection", execution_run_id=execution_run_id)
        db_handler.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Data Comparison Tool')
    
    # Create a group for mode selection
    mode_group = parser.add_argument_group('execution modes')
    mode_group.add_argument('--single', dest='single_mode', action='store_true', 
                      help='Run in single test case mode')
    mode_group.add_argument('--batch', dest='batch_mode', action='store_true',
                      help='Run in batch mode with multiple test cases')
    
    # Add remaining named arguments
    parser.add_argument('--yaml-file', dest='yaml_file', type=str,
                      help='Path to the YAML file for single mode')
    parser.add_argument('--traceability-id', dest='traceability_id', type=str,
                      help='Traceability ID for single mode')
    parser.add_argument('--yaml-dir', dest='yaml_dir', type=str,
                      help='Directory containing YAML files for batch mode')
    parser.add_argument('--run-name', dest='run_name', type=str,
                      help='Name for the execution run in batch mode')
    parser.add_argument('--user', dest='user_id', type=str,
                      help='User ID running the tests')
    parser.add_argument('--env', dest='environment', type=str,
                      help='Environment (dev, test, prod)')
    parser.add_argument('--run-id', dest='execution_run_id', type=str,
                      help='Execution run ID for batch runs')
    parser.add_argument('--schedule-id', dest='schedule_id', type=str,
                      help='Schedule ID for scheduled test runs')
    
    # Add positional arguments for legacy support
    parser.add_argument('yaml_file_pos', nargs='?', help='YAML file path (positional)')
    parser.add_argument('traceability_id_pos', nargs='?', help='Traceability ID (positional)')
    
    args = parser.parse_args()
    
    # Check for legacy positional arguments first
    if args.yaml_file_pos and args.traceability_id_pos:
        print(f"Running in legacy positional argument mode: {args.yaml_file_pos} {args.traceability_id_pos}")
        main(args.yaml_file_pos, args.traceability_id_pos, args.execution_run_id)
    
    elif args.single_mode:
        if not args.yaml_file or not args.traceability_id:
            print("Error: Single mode requires --yaml-file and --traceability-id parameters")
            sys.exit(1)
        main(args.yaml_file, args.traceability_id, args.execution_run_id, args.schedule_id)
    
    elif args.batch_mode:
        if not args.yaml_dir:
            print("Error: Batch mode requires --yaml-dir parameter")
            sys.exit(1)
        
        # Find all YAML files in the specified directory
        yaml_files = []
        if os.path.isdir(args.yaml_dir):
            for file in os.listdir(args.yaml_dir):
                if file.endswith('.yaml') or file.endswith('.yml'):
                    yaml_files.append(os.path.join(args.yaml_dir, file))
        
        if not yaml_files:
            print(f"Error: No YAML files found in directory: {args.yaml_dir}")
            sys.exit(1)
        
        run_batch(
            yaml_files=yaml_files,
            user_id=args.user_id,
            environment=args.environment,
            run_name=args.run_name,
            execution_run_id=args.execution_run_id
        )
    
    else:
        # If no arguments were provided at all
        parser.print_help()
        sys.exit(1)