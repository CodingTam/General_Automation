import os
import subprocess
import yaml
from utils.logger import logger, function_logger
from utils.common import get_timestamp, read_yaml_file, generate_unique_id
from utils.db_handler import DBHandler
from datetime import datetime
import uuid

class TestcaseHandler:
    """Handler for test case operations"""
    
    def __init__(self, db_handler=None):
        """
        Initialize the test case handler.
        
        Args:
            db_handler: Optional DBHandler instance. If not provided, a new one will be created.
        """
        self.db_handler = db_handler if db_handler else DBHandler()
        logger.info("Testcase handler initialized")
    
    @function_logger
    def load_test_case(self, yaml_file_path):
        """
        Load a test case from a YAML file.
        
        Args:
            yaml_file_path: Path to the YAML file
            
        Returns:
            dict: Test case data
        """
        if not os.path.exists(yaml_file_path):
            logger.error(f"YAML file not found: {yaml_file_path}")
            return None
        
        try:
            # Read the YAML file
            yaml_data = read_yaml_file(yaml_file_path)
            
            # Ensure test case has a name
            if 'test_case_name' not in yaml_data:
                # Use the file name as the test case name
                yaml_data['test_case_name'] = os.path.splitext(os.path.basename(yaml_file_path))[0]
            
            logger.info(f"Loaded test case: {yaml_data['test_case_name']} from {yaml_file_path}")
            return yaml_data
            
        except Exception as e:
            logger.error(f"Error loading test case from {yaml_file_path}: {e}")
            return None
    
    @function_logger
    def register_test_case(self, yaml_file_path):
        """
        Register a test case in the database.
        
        Args:
            yaml_file_path: Path to the YAML file
            
        Returns:
            str: Traceability ID
        """
        yaml_data = self.load_test_case(yaml_file_path)
        if not yaml_data:
            return None
        
        # Check if the test case already has a traceability ID
        traceability_id = yaml_data.get('traceability_id')
        if not traceability_id:
            traceability_id = generate_unique_id()
            yaml_data['traceability_id'] = traceability_id
            
            # Update the YAML file with the traceability ID
            try:
                with open(yaml_file_path, 'w') as f:
                    yaml.dump(yaml_data, f, default_flow_style=False)
                logger.info(f"Updated YAML file with traceability ID: {traceability_id}")
            except Exception as e:
                logger.error(f"Error updating YAML file with traceability ID: {e}")
        
        # Insert test case into the database
        traceability_id = self.db_handler.insert_test_case(yaml_data)
        logger.info(f"Registered test case with traceability ID: {traceability_id}")
        
        return traceability_id
    
    @function_logger
    def execute_test_case(self, yaml_file_path, execution_run_id=None):
        """
        Execute a test case.
        
        Args:
            yaml_file_path: Path to the YAML file
            execution_run_id: Optional execution run ID for batch runs
            
        Returns:
            dict: Execution results
        """
        # Register test case if needed
        traceability_id = self.register_test_case(yaml_file_path)
        if not traceability_id:
            logger.error(f"Failed to register test case: {yaml_file_path}")
            return {"status": "FAILED", "error": "Failed to register test case"}
        
        yaml_data = self.load_test_case(yaml_file_path)
        test_case_name = yaml_data.get('test_case_name')
        logger.info(f"Executing test case: {test_case_name}")
        
        # Build command to execute the test case
        cmd = ["python", "validation/Comparison.py", "--single", "--yaml-file", yaml_file_path, "--traceability-id", traceability_id]
        
        # Add execution_run_id if provided
        if execution_run_id:
            cmd.extend(["--run-id", execution_run_id])
        
        logger.info(f"Running command: {' '.join(cmd)}")
        
        try:
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
            
            # Parse output for results
            return self._parse_test_results(output, process.returncode, traceability_id, test_case_name)
            
        except Exception as e:
            logger.error(f"Error executing test case: {e}")
            
            # Create failure result
            error_result = {
                "status": "FAILED",
                "error_message": str(e),
                "start_time": get_timestamp(),
                "end_time": get_timestamp(),
                "test_case_name": test_case_name,
                "traceability_id": traceability_id
            }
            
            # Store error result in database
            self.db_handler.insert_execution_stats(
                traceability_id, 
                error_result,
                execution_run_id=execution_run_id
            )
            
            return error_result
    
    def _parse_test_results(self, output, return_code, traceability_id, test_case_name):
        """
        Parse test execution output for results.
        
        Args:
            output: List of output lines
            return_code: Process return code
            traceability_id: Test case traceability ID
            test_case_name: Test case name
            
        Returns:
            dict: Parsed test results
        """
        # Initialize default result
        result = {
            "status": "FAILED" if return_code != 0 else "PASSED",
            "start_time": get_timestamp(),
            "end_time": get_timestamp(),
            "test_case_name": test_case_name,
            "traceability_id": traceability_id,
            "error_message": None
        }
        
        # Try to extract more details from output
        for line in output:
            if "validation failed" in line.lower():
                result["status"] = "FAILED"
            
            if "validation passed" in line.lower():
                result["status"] = "PASSED"
                
            if "source count:" in line.lower():
                try:
                    result["source_count"] = int(line.split(":", 1)[1].strip())
                except:
                    pass
                
            if "target count:" in line.lower():
                try:
                    result["target_count"] = int(line.split(":", 1)[1].strip())
                except:
                    pass
                
            if "error:" in line.lower():
                result["error_message"] = line
        
        logger.info(f"Test case {test_case_name} completed with status: {result['status']}")
        return result
    
    @function_logger
    def get_test_case_history(self, traceability_id):
        """
        Get execution history for a test case.
        
        Args:
            traceability_id: Test case traceability ID
            
        Returns:
            list: Execution history
        """
        cursor = self.db_handler.conn.cursor()
        
        try:
            cursor.execute('''
            SELECT execution_id, execution_start_time, execution_end_time, status
            FROM testcase_execution_stats
            WHERE traceability_id = ?
            ORDER BY execution_start_time DESC
            ''', (traceability_id,))
            
            history = []
            for row in cursor.fetchall():
                history.append({
                    'execution_id': row[0],
                    'start_time': row[1],
                    'end_time': row[2],
                    'status': row[3]
                })
            
            logger.info(f"Retrieved {len(history)} execution records for test case: {traceability_id}")
            return history
            
        except Exception as e:
            logger.error(f"Error getting test case history: {e}")
            return []
    
    def close(self):
        """Close any open resources."""
        if self.db_handler:
            self.db_handler.close() 