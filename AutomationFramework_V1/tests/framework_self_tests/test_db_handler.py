#!/usr/bin/env python3
"""
Database Handler Self-Tests
These tests validate the database handler functionality of the framework.
"""

import os
import sys
import unittest
import tempfile
import json
from pathlib import Path
import sqlite3

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import framework modules
from logger import logger
from utils.db_handler import DBHandler

class TestDBHandler(unittest.TestCase):
    """Test database handler functionality."""
    
    def setUp(self):
        """Set up test environment with temporary database."""
        # Create a temporary database file
        self.temp_db_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db_file.close()
        
        # Initialize DBHandler with the temporary database
        self.db_handler = DBHandler(db_path=self.temp_db_file.name)
        logger.info(f"Created temporary database at {self.temp_db_file.name}")
    
    def tearDown(self):
        """Clean up after tests."""
        # Close database connection
        if hasattr(self, 'db_handler'):
            self.db_handler.close()
        
        # Remove temporary database file
        if hasattr(self, 'temp_db_file') and os.path.exists(self.temp_db_file.name):
            try:
                os.unlink(self.temp_db_file.name)
                logger.info(f"Removed temporary database at {self.temp_db_file.name}")
            except Exception as e:
                logger.warning(f"Failed to remove temporary database: {e}")
    
    def test_db_connection(self):
        """Test database connection."""
        # Verify that we have a valid connection
        self.assertIsNotNone(self.db_handler.conn, "Database connection should be established")
        
        # Verify that we can execute a simple query
        cursor = self.db_handler.conn.cursor()
        cursor.execute("SELECT sqlite_version();")
        version = cursor.fetchone()
        self.assertIsNotNone(version, "Should be able to query SQLite version")
    
    def test_table_creation(self):
        """Test that the required tables are created."""
        # Check if key tables exist
        cursor = self.db_handler.conn.cursor()
        
        # Get list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
        
        # Check for required tables
        required_tables = [
            'testcase', 
            'execution_run', 
            'testcase_execution_stats', 
            'scheduler',
            'execution_log',
            'version_fallback_decisions',
            'module_execution_issues'
        ]
        
        for table in required_tables:
            self.assertIn(table, table_names, f"Required table {table} is missing")
    
    def test_insert_test_case(self):
        """Test inserting a test case."""
        # Create a test case YAML data
        test_case_data = {
            'test_case_name': 'Test Case for DB',
            'sid': 'TEST_SID',
            'table_name': 'test_table',
            'run_type': 'batch',
            'validation_types': ['schema', 'count', 'data'],
            'source_configuration': {
                'source_type': 'csv',
                'source_location': '/path/to/source.csv'
            },
            'target_configuration': {
                'target_type': 'parquet',
                'target_location': '/path/to/target.parquet'
            },
            'rules': [
                {
                    'column': 'id',
                    'validation': {'type': 'not_null'}
                }
            ],
            'jira': {
                'parent_ticket': 'JIRA-123',
                'test_plan_ticket': 'JIRA-456',
                'environment': 'TEST',
                'release_name': 'Test Release'
            }
        }
        
        # Insert the test case
        traceability_id = self.db_handler.insert_test_case(test_case_data)
        
        # Verify the test case was inserted
        self.assertIsNotNone(traceability_id, "Traceability ID should be generated")
        
        # Query the database to verify the test case exists
        cursor = self.db_handler.conn.cursor()
        cursor.execute("SELECT * FROM testcase WHERE traceability_id = ?", (traceability_id,))
        result = cursor.fetchone()
        
        self.assertIsNotNone(result, "Test case should exist in the database")
        
        # Verify specific fields
        column_names = [description[0] for description in cursor.description]
        test_case_name_index = column_names.index('test_case_name')
        sid_index = column_names.index('sid')
        table_name_index = column_names.index('table_name')
        
        self.assertEqual(result[test_case_name_index], 'Test Case for DB')
        self.assertEqual(result[sid_index], 'TEST_SID')
        self.assertEqual(result[table_name_index], 'test_table')
    
    def test_create_execution_run(self):
        """Test creating an execution run."""
        # Create an execution run
        run_name = "Test Execution Run"
        executed_by = "Test User"
        notes = "Test execution run notes"
        
        execution_run_id = self.db_handler.create_execution_run(run_name, executed_by, notes)
        
        # Verify the execution run was created
        self.assertIsNotNone(execution_run_id, "Execution run ID should be generated")
        
        # Query the database to verify the execution run exists
        cursor = self.db_handler.conn.cursor()
        cursor.execute("SELECT * FROM execution_run WHERE execution_run_id = ?", (execution_run_id,))
        result = cursor.fetchone()
        
        self.assertIsNotNone(result, "Execution run should exist in the database")
        
        # Verify specific fields
        column_names = [description[0] for description in cursor.description]
        run_name_index = column_names.index('run_name')
        executed_by_index = column_names.index('executed_by')
        notes_index = column_names.index('notes')
        
        self.assertEqual(result[run_name_index], run_name)
        self.assertEqual(result[executed_by_index], executed_by)
        self.assertEqual(result[notes_index], notes)
    
    def test_update_execution_run(self):
        """Test updating an execution run."""
        # Create an execution run
        run_name = "Test Execution Run for Update"
        execution_run_id = self.db_handler.create_execution_run(run_name)
        
        # Update the execution run
        additional_notes = "Updated notes"
        overall_status = "COMPLETED"
        
        self.db_handler.update_execution_run(
            execution_run_id=execution_run_id,
            overall_status=overall_status,
            additional_notes=additional_notes
        )
        
        # Query the database to verify the update
        cursor = self.db_handler.conn.cursor()
        cursor.execute("SELECT overall_status, notes FROM execution_run WHERE execution_run_id = ?", 
                     (execution_run_id,))
        result = cursor.fetchone()
        
        self.assertIsNotNone(result, "Execution run should exist in the database")
        self.assertEqual(result[0], overall_status)
        self.assertEqual(result[1], additional_notes)
    
    def test_error_handling(self):
        """Test error handling in the database operations."""
        # Test handling of a non-existent execution run ID
        non_existent_id = "non_existent_id"
        
        # This should not raise an exception, but should return False or None
        result = self.db_handler.update_execution_run(
            execution_run_id=non_existent_id,
            overall_status="FAILED"
        )
        
        # Verify the result indicates failure
        self.assertFalse(result, "Update of non-existent execution run should return False")
    
    def test_complete_execution_run(self):
        """Test completing an execution run."""
        # Create an execution run
        run_name = "Test Execution Run for Completion"
        execution_run_id = self.db_handler.create_execution_run(run_name)
        
        # Use a dummy traceability ID for a test case
        dummy_traceability_id = "dummy_test_case_id"
        
        # Insert a test execution stat to simulate a passed test case
        cursor = self.db_handler.conn.cursor()
        cursor.execute("""
            INSERT INTO testcase_execution_stats (
                execution_id, traceability_id, status, execution_run_id, 
                test_case_name, created_at
            ) VALUES (?, ?, ?, ?, ?, datetime('now'))
        """, (
            "test_execution_id", dummy_traceability_id, "PASS", 
            execution_run_id, "Dummy Test Case"
        ))
        self.db_handler.conn.commit()
        
        # Complete the execution run
        additional_notes = "Completed successfully"
        result = self.db_handler.complete_execution_run(
            execution_run_id=execution_run_id,
            additional_notes=additional_notes
        )
        
        # Verify the completion was successful
        self.assertTrue(result, "Execution run completion should return True")
        
        # Query the database to verify the update
        cursor.execute("""
            SELECT overall_status, passed_test_cases, total_test_cases, notes
            FROM execution_run WHERE execution_run_id = ?
        """, (execution_run_id,))
        result = cursor.fetchone()
        
        self.assertIsNotNone(result, "Execution run should exist in the database")
        self.assertEqual(result[0], "PASS")  # overall_status
        self.assertEqual(result[1], 1)  # passed_test_cases
        self.assertEqual(result[2], 1)  # total_test_cases
        self.assertEqual(result[3], additional_notes)  # notes

if __name__ == '__main__':
    unittest.main() 