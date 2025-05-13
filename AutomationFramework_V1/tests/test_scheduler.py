import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.scheduler_handler import SchedulerHandler
from utils.db_handler import DBHandler
from logger import logger
import unittest
import yaml
from datetime import datetime
import sqlite3

def run_scheduler_add_test():
    """
    Test adding a test case to the scheduler.
    """
    logger.info("Starting scheduler add test")
    
    # Initialize handlers
    db = DBHandler()
    scheduler = SchedulerHandler(db_handler=db)
    
    # Create a sample test case YAML file (if needed)
    test_yaml_path = "tests/test_data/sample_scheduled_test.yaml"
    os.makedirs(os.path.dirname(test_yaml_path), exist_ok=True)
    
    if not os.path.exists(test_yaml_path):
        test_case = {
            'test_case_name': 'Scheduled Test Case',
            'sid': 'SCHEDULE001',
            'table_name': 'scheduled_table',
            'source_configuration': {
                'source_type': 'csv'
            },
            'target_configuration': {
                'target_type': 'parquet'
            }
        }
        
        with open(test_yaml_path, 'w') as f:
            yaml.dump(test_case, f, default_flow_style=False)
    
    # Add to scheduler
    schedule_id = scheduler.schedule_test_case(
        yaml_file_path=test_yaml_path,
        frequency="daily",
        username="test_user"
    )
    
    logger.info(f"Added test case to scheduler with ID: {schedule_id}")
    
    # Verify it was added
    scheduled_tasks = scheduler.get_pending_scheduled_tasks()
    logger.info(f"Found {len(scheduled_tasks)} scheduled tasks")
    
    # Close connections
    scheduler.close()
    logger.info("Scheduler add test completed successfully")
    return schedule_id is not None

def run_scheduler_get_tasks_test():
    """
    Test getting scheduled tasks.
    """
    logger.info("Starting scheduler get tasks test")
    
    # Initialize handlers
    db = DBHandler()
    scheduler = SchedulerHandler(db_handler=db)
    
    # Get scheduled tasks
    tasks = scheduler.get_pending_scheduled_tasks()
    logger.info(f"Found {len(tasks)} scheduled tasks")
    
    # Print task details
    for i, task in enumerate(tasks):
        logger.info(f"Task {i+1}: {task['test_case_name']} (ID: {task['schedule_id']})")
    
    # Close connections
    scheduler.close()
    logger.info("Scheduler get tasks test completed successfully")
    return True

def run_scheduler_iteration_test():
    """
    Test the scheduler loop for a single iteration.
    """
    logger.info("Starting scheduler iteration test")
    
    # Initialize handlers
    db = DBHandler()
    scheduler = SchedulerHandler(db_handler=db)
    
    # Run a single iteration of the scheduler loop
    try:
        scheduler.run_scheduler_loop(max_iterations=1)
        logger.info("Completed one iteration of scheduler loop")
        result = True
    except Exception as e:
        logger.error(f"Error in scheduler loop: {e}")
        result = False
    
    # Close connections
    scheduler.close()
    logger.info("Scheduler iteration test completed")
    return result

if __name__ == "__main__":
    # Run as standalone test
    run_scheduler_add_test()
    run_scheduler_get_tasks_test()
    run_scheduler_iteration_test() 