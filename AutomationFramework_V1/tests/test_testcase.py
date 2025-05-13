import sys
import os
import unittest
import yaml
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.testcase_handler import TestcaseHandler
from utils.db_handler import DBHandler
from utils.logger import logger

def run_testcase_load_test():
    """
    Test loading a test case from a YAML file.
    """
    logger.info("Starting test case load test")
    
    # Initialize handler
    testcase_handler = TestcaseHandler()
    
    # Create a sample test case YAML file (if needed)
    test_yaml_path = "tests/test_data/sample_test.yaml"
    os.makedirs(os.path.dirname(test_yaml_path), exist_ok=True)
    
    if not os.path.exists(test_yaml_path):
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
        
        with open(test_yaml_path, 'w') as f:
            yaml.dump(test_case, f, default_flow_style=False)
    
    # Load test case
    yaml_data = testcase_handler.load_test_case(test_yaml_path)
    logger.info(f"Loaded test case: {yaml_data['test_case_name']}")
    
    # Close connection
    testcase_handler.close()
    logger.info("Test case load test completed successfully")
    return yaml_data is not None

def run_testcase_register_test():
    """
    Test registering a test case in the database.
    """
    logger.info("Starting test case register test")
    
    # Initialize handler
    testcase_handler = TestcaseHandler()
    
    # Create a sample test case YAML file (if needed)
    test_yaml_path = "tests/test_data/register_test.yaml"
    os.makedirs(os.path.dirname(test_yaml_path), exist_ok=True)
    
    if not os.path.exists(test_yaml_path):
        test_case = {
            'test_case_name': 'Register Test Case',
            'sid': 'REGISTER001',
            'table_name': 'register_table',
            'source_configuration': {
                'source_type': 'csv'
            },
            'target_configuration': {
                'target_type': 'parquet'
            }
        }
        
        with open(test_yaml_path, 'w') as f:
            yaml.dump(test_case, f, default_flow_style=False)
    
    # Register test case
    traceability_id = testcase_handler.register_test_case(test_yaml_path)
    logger.info(f"Registered test case with ID: {traceability_id}")
    
    # Close connection
    testcase_handler.close()
    logger.info("Test case register test completed successfully")
    return traceability_id is not None

def run_testcase_history_test():
    """
    Test getting execution history for a test case.
    """
    logger.info("Starting test case history test")
    
    # Initialize handler
    testcase_handler = TestcaseHandler()
    
    # First register a test case
    test_yaml_path = "tests/test_data/history_test.yaml"
    os.makedirs(os.path.dirname(test_yaml_path), exist_ok=True)
    
    if not os.path.exists(test_yaml_path):
        test_case = {
            'test_case_name': 'History Test Case',
            'sid': 'HISTORY001',
            'table_name': 'history_table',
            'source_configuration': {
                'source_type': 'csv'
            },
            'target_configuration': {
                'target_type': 'parquet'
            }
        }
        
        with open(test_yaml_path, 'w') as f:
            yaml.dump(test_case, f, default_flow_style=False)
    
    # Register test case
    traceability_id = testcase_handler.register_test_case(test_yaml_path)
    
    # Create a dummy execution result
    db = DBHandler()
    execution_result = {
        'test_case_name': 'History Test Case',
        'sid': 'HISTORY001',
        'table_name': 'history_table',
        'start_time': '2023-01-01 12:00:00',
        'end_time': '2023-01-01 12:05:00',
        'status': 'PASS',
        'source_count': 100,
        'target_count': 100,
        'mismatched_records': 0
    }
    
    # Insert execution stats
    db.insert_execution_stats(traceability_id, execution_result)
    
    # Get history
    history = testcase_handler.get_test_case_history(traceability_id)
    logger.info(f"Found {len(history)} execution records")
    
    # Close connections
    db.close()
    testcase_handler.close()
    logger.info("Test case history test completed successfully")
    return len(history) > 0

if __name__ == "__main__":
    # Run as standalone test
    run_testcase_load_test()
    run_testcase_register_test()
    run_testcase_history_test() 