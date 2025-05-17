import sqlite3
import jaydebeapi
from datetime import datetime, timedelta
import uuid
from typing import Dict, List, Optional, Union
import os
from utils.logger import logger, function_logger
import json
import yaml
import time
import threading
from utils.common import get_timestamp, generate_unique_id, ensure_directory_exists
from utils.validation import (
    ValidationError, validate_status, validate_frequency,
    validate_day_of_week, validate_day_of_month, validate_file_path,
    validate_traceability_id, validate_counts, validate_module_type,
    validate_format_type, validate_date_format
)
from .config_loader import config
from .db_config import db_config
from .db_utils import get_db_connection, create_table_if_not_exists, insert_data
#from pyspark.sql import SparkSession

class DBHandler:
    """Handler for database operations"""
    
    _thread_local = threading.local()
    
    def __init__(self):
        """Initialize database connection using configuration"""
        self.conn = get_db_connection()
        self._ensure_tables_exist()
        
    def _ensure_tables_exist(self):
        """Ensure necessary tables exist in the database"""
        self.create_tables()
        self.alter_tables()
        self.verify_tables()
        
    def _ensure_connection(self):
        """Ensure we have a valid database connection."""
        try:
            if isinstance(self.conn, (sqlite3.Connection, jaydebeapi.Connection)):
                # Test if connection is alive
                if self.conn is not None:
                    try:
                        cursor = self.conn.cursor()
                        cursor.execute("SELECT 1")
                        cursor.close()
                        return True
                    except (sqlite3.OperationalError, sqlite3.ProgrammingError, Exception):
                        logger.debug("Database connection test failed, reconnecting...")
                        self.conn = None
        except Exception:
            self.conn = None
        
        # If we get here, we need to reconnect
        return self._connect()
    
    @property
    def conn(self):
        """Get the thread-local database connection."""
        if not hasattr(self._thread_local, 'conn'):
            self._thread_local.conn = None
        return self._thread_local.conn
    
    @conn.setter
    def conn(self, value):
        """Set the thread-local database connection."""
        self._thread_local.conn = value
        
    def _connect(self):
        """Establish database connection with retries."""
        retry_count = 0
        last_error = None
        
        while retry_count < db_config.max_retries:
            try:
                if self.conn is not None:
                    try:
                        if isinstance(self.conn, (sqlite3.Connection, jaydebeapi.Connection)):
                            self.conn.close()
                    except Exception:
                        pass
                    self.conn = None
                
                # Get new connection
                self.conn = get_db_connection()
                
                if isinstance(self.conn, sqlite3.Connection):
                    # Enable WAL mode for better concurrency
                    self.conn.execute('PRAGMA journal_mode=WAL')
                    self.conn.execute('PRAGMA busy_timeout=60000')  # 60 second busy timeout
                    # Enable foreign keys
                    self.conn.execute('PRAGMA foreign_keys=ON')
                
                logger.info("Successfully connected to database")
                
                # Initialize tables only on successful connection
                self.create_tables()
                self.alter_tables()
                self.verify_tables()
                return True
                
            except Exception as e:
                last_error = e
                retry_count += 1
                logger.warning(f"Database connection attempt {retry_count} failed: {str(e)}")
                time.sleep(1)  # Wait 1 second before retrying
        
        # If we get here, all retries failed
        raise Exception(f"Failed to connect to database after {db_config.max_retries} attempts. Last error: {str(last_error)}")

    def execute_with_retry(self, func, *args, **kwargs):
        """Execute a database operation with retry logic."""
        retry_count = 0
        last_error = None
        
        while retry_count < db_config.max_retries:
            try:
                # Ensure we have a valid connection
                if not self._ensure_connection():
                    raise Exception("Failed to establish database connection")
                    
                return func(*args, **kwargs)
                
            except Exception as e:
                if isinstance(self.conn, (sqlite3.Connection, jaydebeapi.Connection)) and ("database is locked" in str(e) or "no such table" in str(e).lower()):
                    last_error = e
                    retry_count += 1
                    logger.warning(f"Database operation attempt {retry_count} failed: {str(e)}")
                    time.sleep(1)  # Wait 1 second before retrying
                    
                    # Force reconnection on next attempt
                    self.conn = None
                else:
                    raise
            except Exception as e:
                logger.error(f"Unexpected error in database operation: {str(e)}")
                raise
        
        raise Exception(f"Failed to execute database operation after {db_config.max_retries} attempts. Last error: {str(last_error)}")

    def __enter__(self):
        self._ensure_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close the database connection properly."""
        try:
            if self.conn:
                if isinstance(self.conn, (sqlite3.Connection, jaydebeapi.Connection)):
                    if isinstance(self.conn, sqlite3.Connection):
                        self.conn.commit()  # Commit any pending transactions
                    self.conn.close()
                self.conn = None
                logger.debug("Database connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing database connection: {str(e)}")
            self.conn = None

    @function_logger
    def verify_tables(self):
        """Verify that tables exist and print their structure."""
        if isinstance(self.conn, sqlite3.Connection):
            cursor = self.conn.cursor()
            
            # Get list of tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            logger.info("Verifying database tables")
            for table in tables:
                table_name = table[0]
                logger.debug(f"Verifying table: {table_name}")
                
                # Get table structure
                cursor.execute(f"PRAGMA table_info({table_name});")
                columns = cursor.fetchall()
                
                column_info = [f"{col[1]} ({col[2]})" for col in columns]
                logger.debug(f"Table {table_name} columns: {', '.join(column_info)}")
            
            logger.info(f"Verified {len(tables)} tables in database")
        else:
            # For SQL Server, we'll use Spark to verify tables
            try:
                tables = self.conn.catalog.listTables()
                logger.info("Verifying database tables")
                for table in tables:
                    table_name = table.name
                    logger.debug(f"Verifying table: {table_name}")
                    
                    # Get table structure
                    df = self.conn.table(table_name)
                    columns = df.schema.fields
                    
                    column_info = [f"{col.name} ({col.dataType})" for col in columns]
                    logger.debug(f"Table {table_name} columns: {', '.join(column_info)}")
                
                logger.info(f"Verified {len(tables)} tables in database")
            except Exception as e:
                logger.error(f"Error verifying tables in SQL Server: {e}")
                raise

    @function_logger
    def create_tables(self):
        """Create necessary database tables if they don't exist."""
        # Define table schemas
        tables = {
            'testcase': {
                'traceability_id': 'VARCHAR(255) PRIMARY KEY',
                'sid': 'VARCHAR(255)',
                'table_name': 'VARCHAR(255)',
                'test_case_name': 'VARCHAR(255)',
                'run_type': 'VARCHAR(50)',
                'source_type': 'VARCHAR(50)',
                'source_location': 'VARCHAR(255)',
                'source_partition_column': 'VARCHAR(255)',
                'source_partition_date': 'VARCHAR(50)',
                'source_copybook_location': 'VARCHAR(255)',
                'source_supporting_file': 'VARCHAR(255)',
                'is_multi_source': 'INTEGER',
                'source_type_2': 'VARCHAR(50)',
                'source_location_2': 'VARCHAR(255)',
                'source_partition_column_2': 'VARCHAR(255)',
                'source_partition_date_2': 'VARCHAR(50)',
                'source_copybook_location_2': 'VARCHAR(255)',
                'source_supporting_file_2': 'VARCHAR(255)',
                'source_processor_enabled': 'INTEGER',
                'source_processor_code': 'TEXT',
                'schema_definition': 'TEXT',
                'target_type': 'VARCHAR(50)',
                'target_location': 'VARCHAR(255)',
                'target_partition_column': 'VARCHAR(255)',
                'target_partition_date': 'VARCHAR(50)',
                'target_copybook_location': 'VARCHAR(255)',
                'target_supporting_file': 'VARCHAR(255)',
                'column_names': 'TEXT',
                'execution_notes': 'TEXT',
                'key_columns': 'TEXT',
                'validation_types': 'TEXT',
                'rules': 'TEXT',
                'alert_rules': 'TEXT',
                'parent_jira_ticket': 'VARCHAR(255)',
                'test_plan_jira_ticket': 'VARCHAR(255)',
                'environment': 'VARCHAR(50)',
                'release_name': 'VARCHAR(255)',
                'created_at': 'DATETIME2',
                'updated_at': 'DATETIME2',
                'format_versions': 'TEXT'
            },
            'execution_run': {
                'execution_run_id': 'VARCHAR(255) PRIMARY KEY',
                'run_name': 'VARCHAR(255)',
                'start_time': 'DATETIME2',
                'end_time': 'DATETIME2',
                'total_test_cases': 'INTEGER DEFAULT 0',
                'passed_test_cases': 'INTEGER DEFAULT 0',
                'failed_test_cases': 'INTEGER DEFAULT 0',
                'overall_status': 'VARCHAR(50)',
                'executed_by': 'VARCHAR(255)',
                'notes': 'TEXT',
                'created_at': 'DATETIME2'
            },
            'testcase_execution_stats': {
                'traceability_id': 'VARCHAR(255)',
                'execution_id': 'VARCHAR(255) PRIMARY KEY',
                'sid': 'VARCHAR(255)',
                'table_name': 'VARCHAR(255)',
                'test_case_name': 'VARCHAR(255)',
                'execution_start_time': 'DATETIME2',
                'execution_end_time': 'DATETIME2',
                'status': 'VARCHAR(50)',
                'schema_validation_status': 'VARCHAR(50)',
                'count_validation_status': 'VARCHAR(50)',
                'data_validation_status': 'VARCHAR(50)',
                'null_check_status': 'VARCHAR(50)',
                'duplicate_check_status': 'VARCHAR(50)',
                'rule_validation_status': 'VARCHAR(50)',
                'source_count': 'INTEGER',
                'target_count': 'INTEGER',
                'mismatched_records': 'INTEGER',
                'null_records': 'INTEGER',
                'duplicate_records': 'INTEGER',
                'error_message': 'TEXT',
                'created_at': 'DATETIME2',
                'parent_jira_ticket': 'VARCHAR(255)',
                'test_plan_jira_ticket': 'VARCHAR(255)',
                'environment': 'VARCHAR(50)',
                'release_name': 'VARCHAR(255)',
                'execution_run_id': 'VARCHAR(255)',
                'format_versions': 'TEXT',
                'source_type': 'VARCHAR(50)',
                'source_version': 'VARCHAR(50)',
                'source_module': 'VARCHAR(255)',
                'target_type': 'VARCHAR(50)',
                'target_version': 'VARCHAR(50)',
                'target_module': 'VARCHAR(255)'
            }
        }
        
        # Create each table
        for table_name, columns in tables.items():
            create_table_if_not_exists(self.conn, table_name, columns)
        
        logger.info("Tables created successfully")

    @function_logger
    def alter_tables(self):
        """Alter existing tables to add new columns if they don't exist."""
        if isinstance(self.conn, sqlite3.Connection):
            cursor = self.conn.cursor()
            
            # Check if format_versions column exists in testcase table
            cursor.execute("PRAGMA table_info(testcase)")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'format_versions' not in columns:
                logger.info("Adding format_versions column to testcase table")
                cursor.execute('ALTER TABLE testcase ADD COLUMN format_versions TEXT')
                self.conn.commit()
                logger.info("Successfully added format_versions column to testcase table")
            
            # Check if format_versions column exists in testcase_execution_stats table
            cursor.execute("PRAGMA table_info(testcase_execution_stats)")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'format_versions' not in columns:
                logger.info("Adding format_versions column to testcase_execution_stats table")
                cursor.execute('ALTER TABLE testcase_execution_stats ADD COLUMN format_versions TEXT')
                self.conn.commit()
                logger.info("Successfully added format_versions column to testcase_execution_stats table")

            # Add new columns for source/target type, version, and module
            new_columns = [
                ('source_type', 'TEXT'),
                ('source_version', 'TEXT'),
                ('source_module', 'TEXT'),
                ('target_type', 'TEXT'),
                ('target_version', 'TEXT'),
                ('target_module', 'TEXT'),
            ]
            for col_name, col_type in new_columns:
                if col_name not in columns:
                    logger.info(f"Adding {col_name} column to testcase_execution_stats table")
                    cursor.execute(f'ALTER TABLE testcase_execution_stats ADD COLUMN {col_name} {col_type}')
                    self.conn.commit()
                    logger.info(f"Successfully added {col_name} column to testcase_execution_stats table")
        else:
            # For SQL Server, we'll use Spark to alter tables
            try:
                # Add format_versions column to testcase table if it doesn't exist
                if not self._column_exists('testcase', 'format_versions'):
                    self.conn.sql("ALTER TABLE testcase ADD COLUMN format_versions TEXT")
                    logger.info("Successfully added format_versions column to testcase table")
                
                # Add format_versions column to testcase_execution_stats table if it doesn't exist
                if not self._column_exists('testcase_execution_stats', 'format_versions'):
                    self.conn.sql("ALTER TABLE testcase_execution_stats ADD COLUMN format_versions TEXT")
                    logger.info("Successfully added format_versions column to testcase_execution_stats table")
                
                # Add new columns for source/target type, version, and module
                new_columns = [
                    ('source_type', 'VARCHAR(50)'),
                    ('source_version', 'VARCHAR(50)'),
                    ('source_module', 'VARCHAR(255)'),
                    ('target_type', 'VARCHAR(50)'),
                    ('target_version', 'VARCHAR(50)'),
                    ('target_module', 'VARCHAR(255)'),
                ]
                
                for col_name, col_type in new_columns:
                    if not self._column_exists('testcase_execution_stats', col_name):
                        self.conn.sql(f"ALTER TABLE testcase_execution_stats ADD COLUMN {col_name} {col_type}")
                        logger.info(f"Successfully added {col_name} column to testcase_execution_stats table")
            except Exception as e:
                logger.error(f"Error altering tables in SQL Server: {e}")
                raise

    def _column_exists(self, table_name: str, column_name: str) -> bool:
        """Check if a column exists in a table."""
        try:
            if isinstance(self.conn, sqlite3.Connection):
                cursor = self.conn.cursor()
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [col[1] for col in cursor.fetchall()]
                return column_name in columns
            else:
                # For SQL Server, use Spark to check column existence
                df = self.conn.table(table_name)
                return column_name in [field.name for field in df.schema.fields]
        except Exception as e:
            logger.error(f"Error checking column existence: {e}")
            return False

    def generate_traceability_id(self):
        """Generate a unique traceability ID."""
        return generate_unique_id()
    
    @function_logger
    def insert_test_case(self, yaml_data):
        """Insert a test case into the database and return its traceability ID."""
        def _do_insert():
            cursor = self.conn.cursor()
            
            # Ensure traceability_id is set
            traceability_id = yaml_data.get('traceability_id')
            if not traceability_id:
                traceability_id = self.generate_traceability_id()
                yaml_data['traceability_id'] = traceability_id
            
            # Extract format versions from the YAML
            format_versions = {}
            for config_key in ['source_configuration', 'target_configuration']:
                if config_key in yaml_data:
                    data_type = yaml_data[config_key].get('source_type' if config_key == 'source_configuration' else 'target_type')
                    if data_type:
                        format_versions[data_type] = self.get_format_version(data_type)
            
            # Extract detailed information from YAML
            current_time = get_timestamp()
            
            # Basic information
            sid = yaml_data.get('sid')
            test_case_name = yaml_data.get('test_case_name')
            table_name = yaml_data.get('table_name')
            run_type = yaml_data.get('run_type')
            
            # Source configuration
            source_config = yaml_data.get('source_configuration', {})
            source_type = source_config.get('source_type')
            source_location = source_config.get('source_location')
            source_partition_column = source_config.get('partition_column')
            source_partition_date = source_config.get('partition_date')
            source_copybook_location = source_config.get('copybook_location')
            source_supporting_file = source_config.get('supporting_file')
            
            # Multi-source configuration
            is_multi_source = 1 if yaml_data.get('is_multi_source') else 0
            source_config_2 = yaml_data.get('source_configuration_2', {})
            source_type_2 = source_config_2.get('source_type')
            source_location_2 = source_config_2.get('source_location')
            source_partition_column_2 = source_config_2.get('partition_column')
            source_partition_date_2 = source_config_2.get('partition_date')
            source_copybook_location_2 = source_config_2.get('copybook_location')
            source_supporting_file_2 = source_config_2.get('supporting_file')
            
            # Source processor
            source_processor = yaml_data.get('source_processor', {})
            source_processor_enabled = 1 if source_processor.get('enabled') else 0
            source_processor_code = source_processor.get('code')
            
            # Schema definition
            schema_definition = yaml_data.get('schema_definition')
            if schema_definition and not isinstance(schema_definition, str):
                schema_definition = json.dumps(schema_definition)
            
            # Target configuration
            target_config = yaml_data.get('target_configuration', {})
            target_type = target_config.get('target_type')
            target_location = target_config.get('target_location')
            target_partition_column = target_config.get('partition_column')
            target_partition_date = target_config.get('partition_date')
            target_copybook_location = target_config.get('copybook_location')
            target_supporting_file = target_config.get('supporting_file')
            
            # Column information
            column_names = yaml_data.get('column_names')
            if column_names and not isinstance(column_names, str):
                column_names = json.dumps(column_names)
            
            key_columns = yaml_data.get('key_columns')
            if key_columns and not isinstance(key_columns, str):
                key_columns = json.dumps(key_columns)
            
            # Validation configuration
            validation_types = yaml_data.get('validation_types')
            if validation_types and not isinstance(validation_types, str):
                validation_types = json.dumps(validation_types)
            
            rules = yaml_data.get('rules')
            if rules and not isinstance(rules, str):
                rules = json.dumps(rules)
            
            alert_rules = yaml_data.get('alert_rules')
            if alert_rules and not isinstance(alert_rules, str):
                alert_rules = json.dumps(alert_rules)
            
            # Other information
            execution_notes = yaml_data.get('execution_notes')
            
            # JIRA information
            jira_info = yaml_data.get('jira', {})
            parent_jira_ticket = jira_info.get('parent_ticket')
            test_plan_jira_ticket = jira_info.get('test_plan_ticket')
            environment = jira_info.get('environment')
            release_name = jira_info.get('release_name')
            
            # Insert test case with all available data
            cursor.execute('''
            INSERT INTO testcase (
                traceability_id, sid, table_name, test_case_name, run_type,
                source_type, source_location, source_partition_column, source_partition_date,
                source_copybook_location, source_supporting_file,
                is_multi_source, source_type_2, source_location_2, source_partition_column_2,
                source_partition_date_2, source_copybook_location_2, source_supporting_file_2,
                source_processor_enabled, source_processor_code, schema_definition,
                target_type, target_location, target_partition_column, target_partition_date,
                target_copybook_location, target_supporting_file,
                column_names, execution_notes, key_columns, validation_types, rules, alert_rules,
                parent_jira_ticket, test_plan_jira_ticket, environment, release_name,
                created_at, updated_at, format_versions
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ''', (
                traceability_id, sid, table_name, test_case_name, run_type,
                source_type, source_location, source_partition_column, source_partition_date,
                source_copybook_location, source_supporting_file,
                is_multi_source, source_type_2, source_location_2, source_partition_column_2,
                source_partition_date_2, source_copybook_location_2, source_supporting_file_2,
                source_processor_enabled, source_processor_code, schema_definition,
                target_type, target_location, target_partition_column, target_partition_date,
                target_copybook_location, target_supporting_file,
                column_names, execution_notes, key_columns, validation_types, rules, alert_rules,
                parent_jira_ticket, test_plan_jira_ticket, environment, release_name,
                current_time, current_time, json.dumps(format_versions)
            ))
            
            self.conn.commit()
            return traceability_id
            
        # Execute the insert with retry logic
        return self.execute_with_retry(_do_insert)
    
    def get_format_version(self, format_type: str) -> str:
        """Get the current version for a format type from format_versions.yaml."""
        try:
            with open('configs/format_versions.yaml', 'r') as f:
                version_map = yaml.safe_load(f)['formats']
                if format_type not in version_map:
                    raise ValueError(f"No version specified for format '{format_type}'")
                return version_map[format_type]['current_version']
        except Exception as e:
            logger.error(f"Error reading format versions: {str(e)}")
            raise
    
    @function_logger
    def insert_execution_stats(self, traceability_id, execution_results, jira_info=None, sid=None, execution_run_id=None):
        """
        Insert execution statistics into the database.
        
        Args:
            traceability_id: Test case traceability ID
            execution_results: Results of the test execution
            jira_info: JIRA ticket information
            sid: Source ID
            execution_run_id: ID of the batch execution run
            
        Returns:
            str: The execution ID that was created
            
        Raises:
            ValidationError: If any input parameters are invalid
        """
        # Validate traceability ID
        if not validate_traceability_id(traceability_id):
            raise ValidationError(f"Invalid traceability ID format: {traceability_id}")
            
        # Validate execution results
        if not isinstance(execution_results, dict):
            raise ValidationError("Execution results must be a dictionary")
            
        # Validate counts in execution results
        for count_field in ['source_count', 'target_count', 'mismatched_records', 
                          'null_records', 'duplicate_records']:
            if count_field in execution_results and not validate_counts(execution_results[count_field]):
                raise ValidationError(f"Invalid {count_field}: {execution_results[count_field]}")
                
        # Validate status fields
        for status_field in ['status', 'schema_validation_status', 'count_validation_status',
                           'data_validation_status', 'null_check_status', 
                           'duplicate_check_status', 'rule_validation_status']:
            if status_field in execution_results and not validate_status(execution_results[status_field]):
                raise ValidationError(f"Invalid {status_field}: {execution_results[status_field]}")
                
        # Validate timestamps
        for time_field in ['start_time', 'end_time']:
            if time_field in execution_results and not validate_date_format(execution_results[time_field]):
                raise ValidationError(f"Invalid {time_field} format: {execution_results[time_field]}")
                
        # Validate JIRA info if provided
        if jira_info:
            if not isinstance(jira_info, dict):
                raise ValidationError("JIRA info must be a dictionary")
                
            for ticket_field in ['parent_ticket', 'test_plan_ticket']:
                if ticket_field in jira_info and not isinstance(jira_info[ticket_field], str):
                    raise ValidationError(f"Invalid JIRA {ticket_field}")
                    
        # Validate execution run ID if provided
        if execution_run_id and not validate_traceability_id(execution_run_id):
            raise ValidationError(f"Invalid execution run ID format: {execution_run_id}")

        cursor = self.conn.cursor()
        
        # Check if we already have an entry for this test case in this execution run
        # This prevents duplicate entries
        if execution_run_id and traceability_id:
            cursor.execute("""
                SELECT execution_id FROM testcase_execution_stats 
                WHERE traceability_id = ? AND execution_run_id = ?
                ORDER BY created_at DESC LIMIT 1
            """, (traceability_id, execution_run_id))
            existing_execution = cursor.fetchone()
            
            if existing_execution:
                # If we found an existing record, reuse its execution_id
                execution_id = existing_execution[0]
                logger.info(f"Found existing execution record {execution_id} for traceability_id {traceability_id} in run {execution_run_id}, updating instead of creating new.")
                
                # Prepare data for update
                current_time = get_timestamp()
                
                # Ensure required fields aren't empty
                if not execution_results.get('test_case_name'):
                    execution_results['test_case_name'] = "unknown"
                if not execution_results.get('table_name'):
                    execution_results['table_name'] = "unknown"
                if not sid:
                    sid = execution_results.get('sid', "unknown")
                if not execution_results.get('status'):
                    execution_results['status'] = "FAIL"
                    
                # Format versions
                format_versions = json.dumps({
                    "source": {
                        "type": execution_results.get('source_type'),
                        "version": execution_results.get('source_version')
                    },
                    "target": {
                        "type": execution_results.get('target_type'),
                        "version": execution_results.get('target_version')
                    }
                })
                    
                # Update the existing record
                cursor.execute("""
                    UPDATE testcase_execution_stats SET
                    execution_end_time = ?,
                    status = ?,
                    schema_validation_status = ?,
                    count_validation_status = ?,
                    data_validation_status = ?,
                    null_check_status = ?,
                    duplicate_check_status = ?,
                    rule_validation_status = ?,
                    source_count = ?,
                    target_count = ?,
                    mismatched_records = ?,
                    null_records = ?,
                    duplicate_records = ?,
                    error_message = ?
                    WHERE execution_id = ?
                """, (
                    execution_results.get('end_time', current_time),
                    execution_results.get('status'),
                    execution_results.get('schema_validation', {}).get('status'),
                    execution_results.get('count_validation', {}).get('status'),
                    execution_results.get('data_validation', {}).get('status'),
                    execution_results.get('null_check', {}).get('status'),
                    execution_results.get('duplicate_check', {}).get('status'),
                    execution_results.get('rule_validation', {}).get('status'),
                    execution_results.get('source_count', 0),
                    execution_results.get('target_count', 0),
                    execution_results.get('mismatched_records', 0),
                    execution_results.get('null_records', 0),
                    execution_results.get('duplicate_records', 0),
                    execution_results.get('error_message'),
                    execution_id
                ))
                
                self.conn.commit()
                logger.info(f"Updated execution stats with execution_id: {execution_id}")
                
                # If this is part of an execution run, update stats based on status change
                if execution_run_id and execution_results.get('status'):
                    test_status = execution_results.get('status')
                    logger.info(f"Updating execution run {execution_run_id} with test result: {test_status}")
                    
                    # No need to increment counters as we're updating an existing record
                    
                # Update logs with execution ID
                self._update_logs_with_execution_id(execution_id, execution_results.get('test_case_name'), 
                                              execution_results.get('start_time'), 
                                              execution_results.get('end_time'),
                                              traceability_id, execution_run_id)
                
                return execution_id
        
        # If no existing record found or not enough info to check, create a new execution ID
        execution_id = self.generate_traceability_id()
        
        logger.debug(f"Generated execution_id: {execution_id}")
        
        current_time = get_timestamp()
        
        # Validate execution_run_id if provided
        if execution_run_id:
            cursor.execute("SELECT COUNT(*) FROM execution_run WHERE execution_run_id = ?", (execution_run_id,))
            if cursor.fetchone()[0] == 0:
                logger.warning(f"Execution run ID {execution_run_id} not found in execution_run table. Setting to NULL.")
                execution_run_id = None
        
        # Flatten statuses
        schema_status = execution_results.get('schema_validation', {}).get('status')
        count_status = execution_results.get('count_validation', {}).get('status')
        data_status = execution_results.get('data_validation', {}).get('status')
        null_status = execution_results.get('null_check', {}).get('status')
        duplicate_status = execution_results.get('duplicate_check', {}).get('status')
        rule_status = execution_results.get('rule_validation', {}).get('status')
        
        # JIRA info
        parent_jira = None
        test_plan_jira = None
        environment = None
        release_name = None
        if jira_info:
            parent_jira = jira_info.get('parent_ticket')
            test_plan_jira = jira_info.get('test_plan_ticket')
            environment = jira_info.get('environment')
            release_name = jira_info.get('release_name')
        
        # Try to get sid from execution_results if not provided
        if not sid:
            sid = execution_results.get('sid')
        
        test_case_name = execution_results.get('test_case_name')
        table_name = execution_results.get('table_name')
        
        # Ensure required fields aren't empty to prevent empty rows
        if not test_case_name:
            logger.warning("No test_case_name provided, using 'unknown'")
            test_case_name = "unknown"
        
        if not table_name:
            logger.warning("No table_name provided, using 'unknown'")
            table_name = "unknown"
        
        if not sid:
            logger.warning("No sid provided, using 'unknown'")
            sid = "unknown"
        
        # Make sure we have a valid status
        status = execution_results.get('status')
        if not status:
            logger.warning("No status provided, using 'FAIL'")
            status = "FAIL"
        
        logger.info(f"Inserting execution stats for test case '{test_case_name}' with status '{status}'")
        
        # Insert into testcase_execution_stats table
        try:
            format_versions = json.dumps({
                "source": {
                    "type": execution_results.get('source_type'),
                    "version": execution_results.get('source_version')
                },
                "target": {
                    "type": execution_results.get('target_type'),
                    "version": execution_results.get('target_version')
                }
            })
            cursor.execute('''
            INSERT INTO testcase_execution_stats (
                traceability_id, execution_id, sid, 
                table_name, test_case_name, execution_start_time, execution_end_time, 
                status, schema_validation_status, count_validation_status, 
                data_validation_status, null_check_status, duplicate_check_status, 
                rule_validation_status, source_count, target_count, 
                mismatched_records, null_records, duplicate_records, 
                error_message, created_at, parent_jira_ticket, 
                test_plan_jira_ticket, environment, release_name,
                execution_run_id,
                format_versions,
                source_type, source_version, source_module,
                target_type, target_version, target_module
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                traceability_id, execution_id, sid,
                table_name,
                test_case_name,
                execution_results.get('start_time', current_time),
                execution_results.get('end_time', current_time),
                status,
                schema_status, count_status,
                data_status, null_status, duplicate_status,
                rule_status,
                execution_results.get('source_count', 0),
                execution_results.get('target_count', 0),
                execution_results.get('mismatched_records', 0),
                execution_results.get('null_records', 0),
                execution_results.get('duplicate_records', 0),
                execution_results.get('error_message'),
                current_time,
                parent_jira,
                test_plan_jira,
                environment,
                release_name,
                execution_run_id,
                format_versions,
                execution_results.get('source_type'),
                execution_results.get('source_version'),
                execution_results.get('source_module'),
                execution_results.get('target_type'),
                execution_results.get('target_version'),
                execution_results.get('target_module')
            ))
            
            self.conn.commit()
            logger.info(f"Inserted execution stats with execution_id: {execution_id}")
            
            # If this is part of an execution run, update stats
            if execution_run_id:
                # Update the execution run with new test case result
                test_status = status
                logger.info(f"Updating execution run {execution_run_id} with test result: {test_status}")
                
                if test_status == "PASS":
                    cursor.execute("UPDATE execution_run SET passed_test_cases = passed_test_cases + 1, total_test_cases = total_test_cases + 1 WHERE execution_run_id = ?", (execution_run_id,))
                elif test_status == "FAIL" or test_status == "FAILED":
                    cursor.execute("UPDATE execution_run SET failed_test_cases = failed_test_cases + 1, total_test_cases = total_test_cases + 1 WHERE execution_run_id = ?", (execution_run_id,))
                else:
                    cursor.execute("UPDATE execution_run SET total_test_cases = total_test_cases + 1 WHERE execution_run_id = ?", (execution_run_id,))
                
                self.conn.commit()
            
            # Update logs with execution ID
            self._update_logs_with_execution_id(execution_id, test_case_name, 
                                          execution_results.get('start_time'), 
                                          execution_results.get('end_time'),
                                          traceability_id, execution_run_id)
            
            return execution_id
        except Exception as e:
            logger.error(f"Error inserting execution stats: {e}")
            self.conn.rollback()
            raise
            
    def _update_logs_with_execution_id(self, execution_id, test_case_name, start_time, end_time, 
                                   traceability_id=None, execution_run_id=None):
        """
        Update all related logs with the same execution_id.
        
        Args:
            execution_id: The execution ID to set
            test_case_name: Name of the test case
            start_time: Start time of the test execution
            end_time: End time of the test execution
            traceability_id: Test case traceability ID
            execution_run_id: Execution run ID for batch runs
        """
        cursor = self.conn.cursor()
        
        try:
            # Create a buffer around the execution time window
            if start_time and end_time:
                try:
                    # Add a 30-second buffer before and after
                    start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S') - timedelta(seconds=30)
                    end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=30)
                    start_time_buffer = start_dt.strftime('%Y-%m-%d %H:%M:%S')
                    end_time_buffer = end_dt.strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    # If datetime parsing fails, use the original values
                    start_time_buffer = start_time
                    end_time_buffer = end_time
            else:
                # If we don't have start/end times, use the current time as a reference
                now = datetime.now()
                start_time_buffer = (now - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
                end_time_buffer = (now + timedelta(seconds=30)).strftime('%Y-%m-%d %H:%M:%S')
            
            # Find all logs in the execution time window and update them with this execution_id
            if test_case_name:
                # If we know the test case name, include it in the filter
                query = '''
                UPDATE execution_log 
                SET execution_id = ?, test_case_name = ?
                WHERE execution_id IS NULL
                AND timestamp BETWEEN ? AND ?
                AND (test_case_name IS NULL OR test_case_name = ?)
                '''
                cursor.execute(query, (execution_id, test_case_name, start_time_buffer, end_time_buffer, test_case_name))
            else:
                # Otherwise just use the time window
                query = '''
                UPDATE execution_log 
                SET execution_id = ?
                WHERE execution_id IS NULL
                AND timestamp BETWEEN ? AND ?
                '''
                cursor.execute(query, (execution_id, start_time_buffer, end_time_buffer))
            
            # Also update any logs with matching traceability_id or execution_run_id
            if traceability_id:
                query = '''
                UPDATE execution_log 
                SET execution_id = ?
                WHERE execution_id IS NULL
                AND traceability_id = ?
                '''
                cursor.execute(query, (execution_id, traceability_id))
            
            if execution_run_id:
                query = '''
                UPDATE execution_log 
                SET execution_id = ?
                WHERE execution_id IS NULL
                AND execution_run_id = ?
                '''
                cursor.execute(query, (execution_id, execution_run_id))
            
            # Commit the updates
            self.conn.commit()
            logger.debug(f"Updated logs with execution_id: {execution_id}")
            
        except Exception as e:
            logger.error(f"Error updating logs with execution_id: {e}")
            self.conn.rollback()
    
    @function_logger
    def create_execution_run(self, run_name=None, executed_by=None, notes=None):
        """
        Create a new execution run for tracking batch test case executions.
        
        Args:
            run_name: Name of the execution run
            executed_by: User or process that initiated the run
            notes: Additional notes about the run
            
        Returns:
            str: The execution run ID that was created
        """
        cursor = self.conn.cursor()
        
        # Generate execution run ID
        execution_run_id = self.generate_traceability_id()
        
        current_time = get_timestamp()
        
        # Use default values if not provided
        if not run_name:
            run_name = f"Execution Run - {current_time}"
        
        if not executed_by:
            executed_by = "system"
        
        try:
            cursor.execute('''
            INSERT INTO execution_run (
                execution_run_id, run_name, start_time, 
                executed_by, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                execution_run_id, run_name, current_time,
                executed_by, notes, current_time
            ))
            
            self.conn.commit()
            logger.info(f"Created execution run with ID: {execution_run_id}")
            
            return execution_run_id
        except Exception as e:
            logger.error(f"Error creating execution run: {e}")
            self.conn.rollback()
            raise
    
    @function_logger
    def update_execution_run(self, execution_run_id, end_time=None, overall_status=None, additional_notes=None):
        """
        Update an existing execution run with new information.
        
        Args:
            execution_run_id: ID of the execution run to update
            end_time: End time of the run
            overall_status: Overall status of the run
            additional_notes: Additional notes to append
            
        Raises:
            ValidationError: If any input parameters are invalid
        """
        # Validate inputs
        if not validate_traceability_id(execution_run_id):
            raise ValidationError(f"Invalid execution run ID format: {execution_run_id}")
            
        if end_time and not validate_date_format(end_time):
            raise ValidationError(f"Invalid end time format: {end_time}")
            
        if overall_status and not validate_status(overall_status):
            raise ValidationError(f"Invalid status: {overall_status}")
            
        if additional_notes and not isinstance(additional_notes, str):
            raise ValidationError("Additional notes must be a string")

        cursor = self.conn.cursor()
        
        try:
            # Get existing notes if we need to append
            if additional_notes:
                cursor.execute("SELECT notes FROM execution_run WHERE execution_run_id = ?", (execution_run_id,))
                result = cursor.fetchone()
                existing_notes = result[0] if result and result[0] else ""
                
                # Combine existing and new notes
                if existing_notes:
                    notes = f"{existing_notes}\n{additional_notes}"
                else:
                    notes = additional_notes
            else:
                notes = None
            
            # Build update query dynamically based on what needs to be updated
            update_parts = []
            params = []
            
            if end_time:
                update_parts.append("end_time = ?")
                params.append(end_time)
            
            if overall_status:
                update_parts.append("overall_status = ?")
                params.append(overall_status)
            
            if notes:
                update_parts.append("notes = ?")
                params.append(notes)
            
            # Only proceed if we have something to update
            if update_parts:
                query = f"UPDATE execution_run SET {', '.join(update_parts)} WHERE execution_run_id = ?"
                params.append(execution_run_id)
                
                cursor.execute(query, params)
                self.conn.commit()
                logger.info(f"Updated execution run: {execution_run_id}")
        except Exception as e:
            logger.error(f"Error updating execution run: {e}")
            self.conn.rollback()
            raise
    
    @function_logger
    def complete_execution_run(self, execution_run_id, additional_notes=None):
        """
        Complete an execution run by setting end time and overall status.
        
        Args:
            execution_run_id: ID of the execution run to complete
            additional_notes: Additional notes to append
        """
        cursor = self.conn.cursor()
        
        try:
            # Get current counts
            cursor.execute('''
            SELECT total_test_cases, passed_test_cases, failed_test_cases 
            FROM execution_run 
            WHERE execution_run_id = ?
            ''', (execution_run_id,))
            
            result = cursor.fetchone()
            if not result:
                logger.error(f"Execution run not found: {execution_run_id}")
                return
            
            total, passed, failed = result
            
            # Determine overall status
            if total == 0:
                overall_status = "UNKNOWN"
            elif failed == 0:
                overall_status = "PASS"
            elif passed == 0:
                overall_status = "FAIL"
            else:
                overall_status = "PARTIAL"
            
            # Set end time to current time
            end_time = get_timestamp()
            
            # Complete notes
            if additional_notes:
                notes = f"{additional_notes}\nSummary: {passed} passed, {failed} failed out of {total} test cases."
            else:
                notes = f"Summary: {passed} passed, {failed} failed out of {total} test cases."
            
            # Update execution run
            self.update_execution_run(
                execution_run_id=execution_run_id,
                end_time=end_time,
                overall_status=overall_status,
                additional_notes=notes
            )
            
            logger.info(f"Completed execution run: {execution_run_id} with status: {overall_status}")
        except Exception as e:
            logger.error(f"Error completing execution run: {e}")
            raise
    
    @function_logger
    def get_execution_run_summary(self, execution_run_id):
        """
        Get a summary of an execution run.
        
        Args:
            execution_run_id: ID of the execution run
            
        Returns:
            dict: Summary of the execution run
        """
        cursor = self.conn.cursor()
        
        try:
            cursor.execute('''
            SELECT run_name, start_time, end_time, 
                   total_test_cases, passed_test_cases, failed_test_cases, 
                   overall_status, executed_by, notes
            FROM execution_run 
            WHERE execution_run_id = ?
            ''', (execution_run_id,))
            
            result = cursor.fetchone()
            if not result:
                logger.error(f"Execution run not found: {execution_run_id}")
                return {}
            
            # Create summary dictionary
            summary = {
                'execution_run_id': execution_run_id,
                'run_name': result[0],
                'start_time': result[1],
                'end_time': result[2],
                'total_test_cases': result[3],
                'passed_test_cases': result[4],
                'failed_test_cases': result[5],
                'overall_status': result[6],
                'executed_by': result[7],
                'notes': result[8]
            }
            
            return summary
        except Exception as e:
            logger.error(f"Error getting execution run summary: {e}")
            return {}
    
    @function_logger
    def add_to_scheduler(self, traceability_id, yaml_file_path, username, frequency, day_of_week=None, day_of_month=None):
        """
        Add a test case to the scheduler.
        
        Args:
            traceability_id: Test case traceability ID
            yaml_file_path: Path to the YAML file
            username: User who scheduled the test case
            frequency: Frequency of execution (daily, weekly, monthly)
            day_of_week: Day of the week for weekly execution (0-6, 0=Monday)
            day_of_month: Day of the month for monthly execution (1-31)
            
        Returns:
            str: The schedule ID
            
        Raises:
            ValidationError: If any input parameters are invalid
        """
        # Validate inputs
        if not validate_traceability_id(traceability_id):
            raise ValidationError(f"Invalid traceability ID format: {traceability_id}")
            
        if not validate_file_path(yaml_file_path):
            raise ValidationError(f"YAML file does not exist or is not accessible: {yaml_file_path}")
            
        if not username or not isinstance(username, str):
            raise ValidationError("Username is required and must be a string")
            
        if not validate_frequency(frequency):
            raise ValidationError(f"Invalid frequency: {frequency}. Must be one of: daily, weekly, monthly")
            
        if frequency.lower() == 'weekly' and not validate_day_of_week(day_of_week):
            raise ValidationError(f"Invalid day of week: {day_of_week}. Must be between 0 and 6")
            
        if frequency.lower() == 'monthly' and not validate_day_of_month(day_of_month):
            raise ValidationError(f"Invalid day of month: {day_of_month}. Must be between 1 and 31")

        cursor = self.conn.cursor()
        
        # Generate schedule ID
        schedule_id = self.generate_traceability_id()
        
        current_time = get_timestamp()
        
        # Calculate next run time based on frequency
        now = datetime.now()
        if frequency.lower() == 'daily':
            # Run tomorrow at the same time
            next_run = now.replace(hour=6, minute=0, second=0) + timedelta(days=1)
        elif frequency.lower() == 'weekly':
            # Run next week on the specified day
            if day_of_week is None:
                day_of_week = 0  # Default to Monday
            days_ahead = day_of_week - now.weekday()
            if days_ahead <= 0:  # Target day already happened this week
                days_ahead += 7
            next_run = now.replace(hour=6, minute=0, second=0) + timedelta(days=days_ahead)
        elif frequency.lower() == 'monthly':
            # Run next month on the specified day
            if day_of_month is None:
                day_of_month = 1  # Default to 1st day of month
            
            # Get the next month
            if now.month == 12:
                next_month = 1
                next_year = now.year + 1
            else:
                next_month = now.month + 1
                next_year = now.year
            
            # Make sure the day is valid for the month
            import calendar
            _, days_in_month = calendar.monthrange(next_year, next_month)
            valid_day = min(day_of_month, days_in_month)
            
            next_run = datetime(next_year, next_month, valid_day, 6, 0, 0)
        else:
            # Unknown frequency, default to tomorrow
            next_run = now.replace(hour=6, minute=0, second=0) + timedelta(days=1)
        
        next_run_time = next_run.strftime('%Y-%m-%d %H:%M:%S')
        
        # Get test case details
        try:
            with open(yaml_file_path, 'r') as f:
                yaml_content = yaml.safe_load(f)
                test_case_name = yaml_content.get('test_case_name', os.path.basename(yaml_file_path))
                sid = yaml_content.get('sid')
                table_name = yaml_content.get('table_name')
        except Exception as e:
            logger.warning(f"Error reading YAML file for scheduler: {e}")
            test_case_name = os.path.basename(yaml_file_path)
            sid = None
            table_name = None
        
        try:
            cursor.execute('''
            INSERT INTO scheduler (
                schedule_id, traceability_id, yaml_file_path, 
                test_case_name, sid, table_name,
                username, frequency, day_of_week, day_of_month,
                next_run_time, status, enabled,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                schedule_id, traceability_id, yaml_file_path,
                test_case_name, sid, table_name,
                username, frequency, day_of_week, day_of_month,
                next_run_time, 'SCHEDULED', 1,
                current_time, current_time
            ))
            
            self.conn.commit()
            logger.info(f"Added test case to scheduler with ID: {schedule_id}")
            
            return schedule_id
        except Exception as e:
            logger.error(f"Error adding test case to scheduler: {e}")
            self.conn.rollback()
            raise
    
    @function_logger
    def get_scheduled_test_cases(self):
        """
        Get all scheduled test cases that are due to run.
        
        Returns:
            list: List of dictionaries containing scheduled test case details
        """
        cursor = self.conn.cursor()
        
        current_time = get_timestamp()
        
        try:
            cursor.execute('''
            SELECT schedule_id, traceability_id, yaml_file_path, 
                   test_case_name, sid, table_name,
                   username, frequency, day_of_week, day_of_month,
                   next_run_time
            FROM scheduler 
            WHERE enabled = 1 
            AND next_run_time <= ?
            ''', (current_time,))
            
            results = cursor.fetchall()
            
            scheduled_tests = []
            for row in results:
                scheduled_tests.append({
                    'schedule_id': row[0],
                    'traceability_id': row[1],
                    'yaml_file_path': row[2],
                    'test_case_name': row[3],
                    'sid': row[4],
                    'table_name': row[5],
                    'username': row[6],
                    'frequency': row[7],
                    'day_of_week': row[8],
                    'day_of_month': row[9],
                    'next_run_time': row[10]
                })
            
            return scheduled_tests
        except Exception as e:
            logger.error(f"Error getting scheduled test cases: {e}")
            return []
    
    @function_logger
    def update_scheduler_after_execution(self, schedule_id, status):
        """
        Update a scheduled test case after execution.
        
        Args:
            schedule_id: ID of the scheduled test case
            status: Status of the execution
        """
        cursor = self.conn.cursor()
        
        current_time = get_timestamp()
        
        try:
            # Get frequency to calculate next run time
            cursor.execute('''
            SELECT frequency, day_of_week, day_of_month 
            FROM scheduler 
            WHERE schedule_id = ?
            ''', (schedule_id,))
            
            result = cursor.fetchone()
            if not result:
                logger.error(f"Scheduled test case not found: {schedule_id}")
                return
            
            frequency, day_of_week, day_of_month = result
            
            # Calculate next run time based on frequency
            now = datetime.now()
            if frequency.lower() == 'daily':
                next_run = now.replace(hour=6, minute=0, second=0) + timedelta(days=1)
            elif frequency.lower() == 'weekly':
                days_ahead = day_of_week - now.weekday()
                if days_ahead <= 0:  # Target day already happened this week
                    days_ahead += 7
                next_run = now.replace(hour=6, minute=0, second=0) + timedelta(days=days_ahead)
            elif frequency.lower() == 'monthly':
                if now.month == 12:
                    next_month = 1
                    next_year = now.year + 1
                else:
                    next_month = now.month + 1
                    next_year = now.year
                
                import calendar
                _, days_in_month = calendar.monthrange(next_year, next_month)
                valid_day = min(day_of_month, days_in_month)
                
                next_run = datetime(next_year, next_month, valid_day, 6, 0, 0)
            else:
                next_run = now.replace(hour=6, minute=0, second=0) + timedelta(days=1)
            
            next_run_time = next_run.strftime('%Y-%m-%d %H:%M:%S')
            
            # Update scheduler with execution results
            cursor.execute('''
            UPDATE scheduler 
            SET status = ?, last_run_time = ?, next_run_time = ?, updated_at = ?
            WHERE schedule_id = ?
            ''', (status, current_time, next_run_time, current_time, schedule_id))
            
            self.conn.commit()
            logger.info(f"Updated scheduler after execution: {schedule_id} with status: {status}")
        except Exception as e:
            logger.error(f"Error updating scheduler after execution: {e}")
            self.conn.rollback()
    
    @function_logger
    def log_version_fallback(self, execution_id: str, test_case_id: str, traceability_id: str,
                           module_type: str, operation_type: str, initial_version: str,
                           fallback_version: str, error_message: str, successful: bool,
                           format_type: str, file_path: str = None, user_id: str = None,
                           environment: str = None) -> int:
        """
        Log a version fallback decision.
        
        Args:
            execution_id: Current execution ID
            test_case_id: Test case ID
            traceability_id: Traceability ID
            module_type: Type of module (converter, validator, etc.)
            operation_type: Type of operation being performed
            initial_version: Initially attempted version
            fallback_version: Version fallen back to
            error_message: Error message from the initial attempt
            successful: Whether the fallback was successful
            format_type: Data format type (CSV, XML, etc.)
            file_path: Path to the file being processed
            user_id: ID of the user running the operation
            environment: Environment where the operation was performed
            
        Returns:
            int: ID of the created record
        """
        cursor = self.conn.cursor()
        cursor.execute('''
        INSERT INTO version_fallback_decisions (
            execution_id, test_case_id, traceability_id, module_type,
            operation_type, initial_version, fallback_version, error_message,
            successful, format_type, file_path, user_id, environment
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            execution_id, test_case_id, traceability_id, module_type,
            operation_type, initial_version, fallback_version, error_message,
            successful, format_type, file_path, user_id, environment
        ))
        self.conn.commit()
        return cursor.lastrowid
    
    @function_logger
    def log_module_issue(self, execution_id: str, test_case_id: str, traceability_id: str,
                        user_id: str, environment: str, module_name: str, module_type: str,
                        attempted_version: str, framework_version: str, error_message: str,
                        tool_version: str = None, plugin_version: str = None,
                        converter_version: str = None, stack_trace: str = None,
                        file_path: str = None, additional_context: str = None) -> int:
        """
        Log a module execution issue.
        
        Args:
            execution_id: Current execution ID
            test_case_id: Test case ID
            traceability_id: Traceability ID
            user_id: ID of the user running the operation
            environment: Environment where the operation was performed
            module_name: Name of the module that failed
            module_type: Type of module
            attempted_version: Version that was attempted
            framework_version: Framework version
            error_message: Error message from the failure
            tool_version: Version of the tool being used
            plugin_version: Version of the plugin being used
            converter_version: Version of the converter being used
            stack_trace: Full stack trace of the error
            file_path: Path to the file being processed (can be local or HDFS path)
            additional_context: Any additional context about the error
            
        Returns:
            int: ID of the created record
            
        Raises:
            ValidationError: If any input parameters are invalid
        """
        # Validate required inputs
        if not validate_traceability_id(execution_id):
            raise ValidationError(f"Invalid execution ID format: {execution_id}")
            
        if not validate_traceability_id(test_case_id):
            raise ValidationError(f"Invalid test case ID format: {test_case_id}")
            
        if not validate_traceability_id(traceability_id):
            raise ValidationError(f"Invalid traceability ID format: {traceability_id}")
            
        if not user_id or not isinstance(user_id, str):
            raise ValidationError("User ID is required and must be a string")
            
        if not environment or not isinstance(environment, str):
            raise ValidationError("Environment is required and must be a string")
            
        if not module_name or not isinstance(module_name, str):
            raise ValidationError("Module name is required and must be a string")
            
        if not validate_module_type(module_type):
            raise ValidationError(f"Invalid module type: {module_type}")
            
        if not attempted_version or not isinstance(attempted_version, str):
            raise ValidationError("Attempted version is required and must be a string")
            
        if not framework_version or not isinstance(framework_version, str):
            raise ValidationError("Framework version is required and must be a string")
            
        if not error_message or not isinstance(error_message, str):
            raise ValidationError("Error message is required and must be a string")
            
        # Validate optional inputs
        if file_path and not validate_file_path(file_path):
            logger.warning(f"File path validation failed: {file_path}")
            # Don't raise an error, just log a warning since the file might be temporarily unavailable
        
        # Validate version strings if provided
        for version, name in [(tool_version, "Tool"), (plugin_version, "Plugin"), 
                            (converter_version, "Converter")]:
            if version and not isinstance(version, str):
                raise ValidationError(f"{name} version must be a string")

        cursor = self.conn.cursor()
        cursor.execute('''
        INSERT INTO module_execution_issues (
            execution_id, test_case_id, traceability_id, user_id,
            environment, module_name, module_type, attempted_version,
            framework_version, tool_version, plugin_version,
            converter_version, error_message, stack_trace,
            file_path, additional_context
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            execution_id, test_case_id, traceability_id, user_id,
            environment, module_name, module_type, attempted_version,
            framework_version, tool_version, plugin_version,
            converter_version, error_message, stack_trace,
            file_path, additional_context
        ))
        self.conn.commit()
        return cursor.lastrowid
    
    @function_logger
    def get_version_fallback_history(self, test_case_id: str = None,
                                   module_type: str = None,
                                   format_type: str = None,
                                   start_date: str = None,
                                   end_date: str = None) -> List[Dict]:
        """
        Get history of version fallback decisions with optional filters.
        
        Returns:
            List[Dict]: List of fallback decision records
        """
        cursor = self.conn.cursor()
        query = "SELECT * FROM version_fallback_decisions WHERE 1=1"
        params = []
        
        if test_case_id:
            query += " AND test_case_id = ?"
            params.append(test_case_id)
        if module_type:
            query += " AND module_type = ?"
            params.append(module_type)
        if format_type:
            query += " AND format_type = ?"
            params.append(format_type)
        if start_date:
            query += " AND decision_timestamp >= ?"
            params.append(start_date)
        if end_date:
            query += " AND decision_timestamp <= ?"
            params.append(end_date)
        
        cursor.execute(query, params)
        columns = [description[0] for description in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    @function_logger
    def get_module_issues(self, status: str = None,
                         module_type: str = None,
                         start_date: str = None,
                         end_date: str = None) -> List[Dict]:
        """
        Get module execution issues with optional filters.
        
        Returns:
            List[Dict]: List of module issue records
        """
        cursor = self.conn.cursor()
        query = "SELECT * FROM module_execution_issues WHERE 1=1"
        params = []
        
        if status:
            query += " AND resolution_status = ?"
            params.append(status)
        if module_type:
            query += " AND module_type = ?"
            params.append(module_type)
        if start_date:
            query += " AND error_timestamp >= ?"
            params.append(start_date)
        if end_date:
            query += " AND error_timestamp <= ?"
            params.append(end_date)
        
        cursor.execute(query, params)
        columns = [description[0] for description in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
        
    @function_logger
    def update_issue_resolution(self, issue_id: int,
                              resolution_status: str,
                              resolution_notes: str) -> bool:
        """
        Update the resolution status and notes for a module issue.
        
        Returns:
            bool: True if update was successful
        """
        cursor = self.conn.cursor()
        cursor.execute('''
        UPDATE module_execution_issues
        SET resolution_status = ?, resolution_notes = ?
        WHERE id = ?
        ''', (resolution_status, resolution_notes, issue_id))
        self.conn.commit()
        return cursor.rowcount > 0