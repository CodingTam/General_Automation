import os
import time
from datetime import datetime, timedelta
import subprocess
import yaml
from logger import logger, function_logger
from utils.common import get_timestamp, read_yaml_file
from utils.db_handler import DBHandler

class SchedulerHandler:
    """Handler for managing scheduled test executions"""
    
    def __init__(self, db_handler=None):
        """
        Initialize the scheduler handler.
        
        Args:
            db_handler: Optional DBHandler instance. If not provided, a new one will be created.
        """
        self.db_handler = db_handler
        self._own_db_handler = db_handler is None
        if self._own_db_handler:
            try:
                self.db_handler = DBHandler(max_retries=5)
            except Exception as e:
                logger.error(f"Failed to initialize database handler: {e}")
                raise
        logger.info("Scheduler handler initialized")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    @function_logger
    def get_pending_scheduled_tasks(self):
        """
        Get all scheduled tasks that are pending execution.
        
        Returns:
            list: List of scheduled tasks
        """
        try:
            if self._own_db_handler:
                with DBHandler(max_retries=5) as db:
                    return db.get_scheduled_test_cases()
            return self.db_handler.get_scheduled_test_cases()
        except Exception as e:
            logger.error(f"Error getting pending scheduled tasks: {e}")
            return []
    
    @function_logger
    def execute_scheduled_task(self, task):
        """
        Execute a scheduled task.
        
        Args:
            task: Dictionary containing task details
            
        Returns:
            bool: True if execution was successful, False otherwise
        """
        if self._own_db_handler:
            db_handler = DBHandler(max_retries=5)
        else:
            db_handler = self.db_handler
            
        try:
            yaml_file_path = task['yaml_file_path']
            schedule_id = task['schedule_id']
            traceability_id = task['traceability_id']
            
            if not os.path.exists(yaml_file_path):
                logger.error(f"YAML file not found: {yaml_file_path}")
                db_handler.update_scheduler_after_execution(schedule_id, "FAILED")
                return False
            
            logger.info(f"Executing scheduled task: {task['test_case_name']} (ID: {traceability_id})")
            
            # Create an execution run for this scheduled run
            run_name = f"Scheduled Run - {task['test_case_name']} - {get_timestamp()}"
            execution_run_id = db_handler.create_execution_run(
                run_name=run_name,
                executed_by="scheduler",
                notes=f"Scheduled execution of {task['test_case_name']}"
            )
            
            # Build command to execute the test case
            try:
                cmd = [
                    "python3", 
                    "run_test_cases.py", 
                    yaml_file_path, 
                    "--scheduled",
                    "--traceability-id", traceability_id,
                    "--schedule-id", schedule_id,
                    "--execution_run_id", execution_run_id
                ]
                
                logger.info(f"Running command: {' '.join(cmd)}")
                
                # Execute the command
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True
                )
                
                # Monitor the process output
                output = []
                for line in process.stdout:
                    line = line.strip()
                    output.append(line)
                    logger.debug(line)
                
                # Wait for process to complete
                process.wait()
                
                # Check if process was successful
                if process.returncode == 0:
                    logger.info(f"Successfully executed scheduled task: {task['test_case_name']}")
                    db_handler.update_scheduler_after_execution(schedule_id, "SUCCESS")
                    
                    # Complete the execution run
                    db_handler.complete_execution_run(
                        execution_run_id,
                        additional_notes="Scheduled execution completed successfully"
                    )
                    
                    return True
                else:
                    logger.error(f"Failed to execute scheduled task: {task['test_case_name']}")
                    db_handler.update_scheduler_after_execution(schedule_id, "FAILED")
                    
                    # Complete the execution run with error status
                    db_handler.complete_execution_run(
                        execution_run_id,
                        additional_notes="Scheduled execution failed"
                    )
                    
                    return False
                    
            except Exception as e:
                logger.error(f"Error executing scheduled task: {e}")
                db_handler.update_scheduler_after_execution(schedule_id, "FAILED")
                return False
        finally:
            if self._own_db_handler and db_handler:
                db_handler.close()
    
    @function_logger
    def schedule_test_case(self, yaml_file_path, frequency, username=None, day_of_week=None, day_of_month=None):
        """
        Schedule a test case for periodic execution.
        
        Args:
            yaml_file_path: Path to the YAML file
            frequency: Frequency of execution (daily, weekly, monthly)
            username: User who scheduled the test case
            day_of_week: Day of the week for weekly execution (0-6, 0=Monday)
            day_of_month: Day of the month for monthly execution (1-31)
            
        Returns:
            str: Schedule ID if successful, None otherwise
        """
        try:
            # Read the YAML file
            yaml_data = read_yaml_file(yaml_file_path)
            
            # Get traceability ID or create a new one
            traceability_id = yaml_data.get('traceability_id')
            if not traceability_id:
                # Insert test case to get a traceability ID
                traceability_id = self.db_handler.insert_test_case(yaml_data)
            
            # Default username if not provided
            if not username:
                username = yaml_data.get('sid', 'system')
            
            # Add to scheduler
            schedule_id = self.db_handler.add_to_scheduler(
                traceability_id=traceability_id,
                yaml_file_path=yaml_file_path,
                username=username,
                frequency=frequency,
                day_of_week=day_of_week,
                day_of_month=day_of_month
            )
            
            logger.info(f"Scheduled test case: {yaml_data.get('test_case_name')} with ID: {schedule_id}")
            return schedule_id
        
        except Exception as e:
            logger.error(f"Error scheduling test case: {e}")
            return None
    
    @function_logger
    def run_scheduler_loop(self, interval=60, max_iterations=None):
        """
        Run the scheduler loop to check and execute pending tasks.
        
        Args:
            interval: Interval in seconds between checks
            max_iterations: Maximum number of iterations (None for unlimited)
        """
        logger.info(f"Starting scheduler loop with {interval}s interval")
        
        iteration = 0
        try:
            while True:
                # Check if we've reached max iterations
                if max_iterations is not None and iteration >= max_iterations:
                    logger.info(f"Reached maximum iterations ({max_iterations}), exiting scheduler loop")
                    break
                
                iteration += 1
                logger.info(f"Scheduler check iteration {iteration}")
                
                # Get pending scheduled tasks
                tasks = self.get_pending_scheduled_tasks()
                logger.info(f"Found {len(tasks)} pending scheduled tasks")
                
                # Execute each task
                for task in tasks:
                    logger.info(f"Processing scheduled task: {task['test_case_name']}")
                    self.execute_scheduled_task(task)
                
                # Wait for next check
                logger.info(f"Sleeping for {interval} seconds until next check")
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Scheduler loop interrupted by user")
        except Exception as e:
            logger.error(f"Error in scheduler loop: {e}")
        finally:
            logger.info("Scheduler loop stopped")
    
    def close(self):
        """Close any open resources."""
        if self._own_db_handler and self.db_handler:
            self.db_handler.close()
            self.db_handler = None 