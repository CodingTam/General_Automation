import sys
import os
import unittest
import yaml
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_handler import DBHandler
from logger import logger

def run_db_connection_test():
    """
    Test database connection and basic operations.
    """
    logger.info("Starting DB connection test")
    
    # Initialize DB handler
    db = DBHandler()
    
    # Test table creation
    db.create_tables()
    logger.info("Tables created successfully")
    
    # Get and verify tables
    db.verify_tables()
    
    # Close connection
    db.close()
    logger.info("DB connection test completed successfully")
    return True

def run_test_case_insertion():
    """
    Test inserting a test case into the database.
    """
    logger.info("Starting test case insertion test")
    
    # Initialize DB handler
    db = DBHandler()
    
    # Create a sample test case
    test_case = {
        'test_case_name': 'Sample Test Case',
        'sid': 'TEST001',
        'table_name': 'sample_table',
        'source_configuration': {
            'source_type': 'csv'
        },
        'target_configuration': {
            'target_type': 'parquet'
        }
    }
    
    # Insert test case
    traceability_id = db.insert_test_case(test_case)
    logger.info(f"Test case inserted with ID: {traceability_id}")
    
    # Close connection
    db.close()
    logger.info("Test case insertion test completed successfully")
    return traceability_id is not None

def run_execution_run_test():
    """
    Test creating and completing an execution run.
    """
    logger.info("Starting execution run test")
    
    # Initialize DB handler
    db = DBHandler()
    
    # Create execution run
    run_name = "Test Execution Run"
    execution_run_id = db.create_execution_run(run_name=run_name, executed_by="test_script")
    logger.info(f"Created execution run with ID: {execution_run_id}")
    
    # Update execution run with a test case result
    test_case = {
        'test_case_name': 'Sample Test Case',
        'sid': 'TEST001',
        'table_name': 'sample_table'
    }
    traceability_id = db.insert_test_case(test_case)
    
    # Create a sample execution result
    execution_result = {
        'test_case_name': 'Sample Test Case',
        'sid': 'TEST001',
        'table_name': 'sample_table',
        'start_time': '2023-01-01 12:00:00',
        'end_time': '2023-01-01 12:05:00',
        'status': 'PASS',
        'source_count': 100,
        'target_count': 100,
        'mismatched_records': 0
    }
    
    # Insert execution stats
    execution_id = db.insert_execution_stats(
        traceability_id, 
        execution_result, 
        execution_run_id=execution_run_id
    )
    logger.info(f"Inserted execution stats with ID: {execution_id}")
    
    # Complete execution run
    db.complete_execution_run(execution_run_id)
    logger.info("Completed execution run")
    
    # Get execution run summary
    summary = db.get_execution_run_summary(execution_run_id)
    logger.info(f"Execution run summary: {summary}")
    
    # Close connection
    db.close()
    logger.info("Execution run test completed successfully")
    return summary.get('overall_status') == 'PASS'

if __name__ == "__main__":
    # Run as standalone test
    run_db_connection_test()
    run_test_case_insertion()
    run_execution_run_test() 