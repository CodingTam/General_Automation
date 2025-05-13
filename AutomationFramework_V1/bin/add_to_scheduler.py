#!/usr/bin/env python3
"""
Script to add a YAML test case to the scheduler.

This script:
1. Takes a YAML file path as input
2. Optionally takes frequency, day of week, or day of month as arguments
3. Adds the test case to the scheduler table
"""

import os
import argparse
import yaml
import uuid
from datetime import datetime, timedelta
from utils.db_handler import DBHandler
from utils.custom_logger import get_logger

# Initialize logger
logger = get_logger()

def load_yaml_file(yaml_file_path):
    """Load and parse a YAML file."""
    try:
        with open(yaml_file_path, 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Error loading YAML file {yaml_file_path}: {str(e)}")
        return None

def add_yaml_to_scheduler(yaml_file_path, frequency=None, day_of_week=None, day_of_month=None, force=False):
    """
    Add a YAML test case to the scheduler.
    
    Args:
        yaml_file_path: Path to the YAML file
        frequency: Override the frequency (daily, weekly, monthly)
        day_of_week: Day of week for weekly frequency (1-7, Monday=1)
        day_of_month: Day of month for monthly frequency (1-31)
        force: Force add even if already scheduled
    
    Returns:
        bool: Success status
    """
    # Load YAML file
    yaml_data = load_yaml_file(yaml_file_path)
    if not yaml_data:
        return False
    
    # Extract test case info
    test_case_name = yaml_data.get('test_case_name')
    sid = yaml_data.get('sid')
    table_name = yaml_data.get('table_name', '')
    
    if not test_case_name or not sid:
        logger.error(f"YAML file {yaml_file_path} is missing required fields (test_case_name or sid)")
        return False
    
    # Initialize database handler
    db_handler = DBHandler()
    
    try:
        # Check if test case already exists in testcase table
        cursor = db_handler.conn.cursor()
        cursor.execute('''
        SELECT traceability_id 
        FROM testcase 
        WHERE test_case_name = ? AND sid = ? 
        ORDER BY created_at DESC LIMIT 1
        ''', (test_case_name, sid))
        
        result = cursor.fetchone()
        if result:
            traceability_id = result[0]
            logger.info(f"Test case {test_case_name} with SID {sid} already exists with traceability_id: {traceability_id}")
        else:
            # Insert into testcase table
            traceability_id = insert_test_case_from_yaml(db_handler, yaml_data, yaml_file_path)
            if not traceability_id:
                logger.error(f"Failed to insert test case details into testcase table")
                return False
            logger.info(f"Created new test case with traceability_id: {traceability_id}")
        
        # Check if already scheduled
        cursor.execute('''
        SELECT schedule_id
        FROM scheduler
        WHERE test_case_name = ? AND sid = ?
        ''', (test_case_name, sid))
        
        existing_schedule = cursor.fetchone()
        if existing_schedule:
            existing_schedule_id = existing_schedule[0]
            if force:
                # Delete the existing schedule entry if --force is used
                logger.info(f"Deleting existing schedule entry (ID: {existing_schedule_id}) for test case {test_case_name}")
                cursor.execute('''
                DELETE FROM scheduler
                WHERE schedule_id = ?
                ''', (existing_schedule_id,))
                db_handler.conn.commit()
            else:
                logger.warning(f"Test case {test_case_name} is already scheduled. Use --force to override.")
                return False
        
        # Get scheduler settings from YAML or arguments
        scheduler_config = yaml_data.get('scheduler', {})
        
        # Command-line arguments override YAML settings
        if frequency:
            # Override frequency
            sched_frequency = frequency
        else:
            # Use YAML setting or default to daily
            sched_frequency = scheduler_config.get('frequency', 'daily')
        
        # Handle day settings based on frequency
        sched_day_of_week = None
        sched_day_of_month = None
        
        if sched_frequency == 'weekly':
            if day_of_week:
                sched_day_of_week = day_of_week
            else:
                sched_day_of_week = scheduler_config.get('day_of_week', 1)  # Default to Monday
        
        if sched_frequency == 'monthly':
            if day_of_month:
                sched_day_of_month = day_of_month
            else:
                sched_day_of_month = scheduler_config.get('day_of_month', 1)  # Default to 1st of month
        
        # Username from SID
        username = sid
        
        # Calculate initial next run time
        next_run_time = calculate_next_run_time(sched_frequency, sched_day_of_week, sched_day_of_month)
        
        # Generate a UUID for schedule_id
        schedule_id = str(uuid.uuid4())
        
        # Add to scheduler
        cursor.execute('''
        INSERT INTO scheduler (
            schedule_id, traceability_id, yaml_file_path, test_case_name, sid, table_name,
            username, frequency, day_of_week, day_of_month, 
            next_run_time, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            schedule_id,
            traceability_id, 
            yaml_file_path,
            test_case_name,
            sid,
            table_name,
            username, 
            sched_frequency,
            sched_day_of_week,
            sched_day_of_month,
            next_run_time,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ))
        
        db_handler.conn.commit()
        
        if schedule_id:
            logger.info(f"Test case {test_case_name} has been added to scheduler with ID: {schedule_id}")
            logger.info(f"Frequency: {sched_frequency}")
            
            if sched_frequency == 'weekly' and sched_day_of_week:
                day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                day_name = day_names[int(sched_day_of_week) - 1]
                logger.info(f"Day of week: {day_name} (day {sched_day_of_week})")
            
            if sched_frequency == 'monthly' and sched_day_of_month:
                logger.info(f"Day of month: {sched_day_of_month}")
            
            logger.info(f"Next run time: {next_run_time}")
            
            return True
        else:
            logger.error("Failed to add test case to scheduler")
            return False
            
    except Exception as e:
        logger.error(f"Error adding test case to scheduler: {str(e)}")
        return False
    finally:
        db_handler.close()

def insert_test_case_from_yaml(db_handler, yaml_data, yaml_file_path):
    """
    Insert test case details from YAML into the testcase table
    
    Args:
        db_handler: Database handler instance
        yaml_data: Parsed YAML data
        yaml_file_path: Path to the YAML file
        
    Returns:
        traceability_id if successful, None otherwise
    """
    try:
        cursor = db_handler.conn.cursor()
        
        # Generate traceability ID
        traceability_id = str(uuid.uuid4())
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Extract basic info
        test_case_name = yaml_data.get('test_case_name')
        sid = yaml_data.get('sid')
        table_name = yaml_data.get('table_name', '')
        run_type = yaml_data.get('run_type', '')
        
        # Extract source configuration
        source_config = yaml_data.get('source_configuration', {})
        source_type = source_config.get('source_type', '')
        source_location = source_config.get('source_location', '')
        source_partition_column = source_config.get('partition_column', '')
        source_partition_date = source_config.get('partition_date', '')
        source_copybook_location = source_config.get('copybook_location', '')
        source_supporting_file = source_config.get('supporting_file', '')
        
        # Multi-source configuration
        is_multi_source = 1 if yaml_data.get('is_multi_source') else 0
        source_config_2 = yaml_data.get('source_configuration_2', {})
        source_type_2 = source_config_2.get('source_type', '')
        source_location_2 = source_config_2.get('source_location', '')
        source_partition_column_2 = source_config_2.get('partition_column', '')
        source_partition_date_2 = source_config_2.get('partition_date', '')
        source_copybook_location_2 = source_config_2.get('copybook_location', '')
        source_supporting_file_2 = source_config_2.get('supporting_file', '')
        
        # Source processor
        source_processor = yaml_data.get('source_processor', {})
        source_processor_enabled = 1 if source_processor.get('enabled') else 0
        source_processor_code = source_processor.get('code', '')
        
        # Schema definition
        schema_definition = yaml_data.get('schema_definition', '')
        
        # Target configuration
        target_config = yaml_data.get('target_configuration', {})
        target_type = target_config.get('target_type', '')
        target_location = target_config.get('target_location', '')
        target_partition_column = target_config.get('partition_column', '')
        target_partition_date = target_config.get('partition_date', '')
        target_copybook_location = target_config.get('copybook_location', '')
        target_supporting_file = target_config.get('supporting_file', '')
        
        # Column information
        column_names = str(yaml_data.get('column_names', []))
        key_columns = str(yaml_data.get('key_columns', []))
        
        # Validation configuration
        validation_types = str(yaml_data.get('validation_types', []))
        rules = str(yaml_data.get('rules', {}))
        alert_rules = str(yaml_data.get('alert_rules', {}))
        
        # JIRA information
        jira_info = yaml_data.get('jira', {})
        parent_jira_ticket = jira_info.get('parent_ticket', '')
        test_plan_jira_ticket = jira_info.get('test_plan_ticket', '')
        environment = jira_info.get('environment', '')
        release_name = jira_info.get('release_name', '')
        
        # Additional information
        execution_notes = yaml_data.get('execution_notes', '')
        
        # Insert into testcase table
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
            created_at, updated_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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
            current_time, current_time
        ))
        
        db_handler.conn.commit()
        logger.info(f"Inserted test case details into testcase table with traceability_id: {traceability_id}")
        return traceability_id
        
    except Exception as e:
        logger.error(f"Error inserting test case into database: {str(e)}")
        return None

def calculate_next_run_time(frequency, day_of_week=None, day_of_month=None):
    """
    Calculate the next run date based on frequency and day preferences.
    
    Args:
        frequency: daily, weekly, or monthly
        day_of_week: Day of week for weekly frequency (1-7, Monday=1)
        day_of_month: Day of month for monthly frequency (1-31)
        
    Returns:
        Next run date as string (YYYY-MM-DD HH:MM:SS)
    """
    now = datetime.now()
    
    if frequency == 'daily':
        # Next day, same time
        next_date = now + timedelta(days=1)
    
    elif frequency == 'weekly':
        # Calculate days until next occurrence of day_of_week
        day_of_week = int(day_of_week) if day_of_week else 1  # Default to Monday
        days_until = day_of_week - now.isoweekday()
        if days_until <= 0:  # If today or already passed this week
            days_until += 7  # Schedule for next week
        next_date = now + timedelta(days=days_until)
    
    elif frequency == 'monthly':
        # Calculate next occurrence of day_of_month
        day_of_month = int(day_of_month) if day_of_month else 1  # Default to 1st
        
        # Start with next month, same day
        if now.day < day_of_month:
            # This month's day hasn't occurred yet
            next_date = datetime(now.year, now.month, day_of_month)
        else:
            # This month's day has passed, go to next month
            if now.month == 12:
                next_date = datetime(now.year + 1, 1, day_of_month)
            else:
                next_date = datetime(now.year, now.month + 1, day_of_month)
        
        # Handle invalid dates (e.g., Feb 30)
        while True:
            try:
                next_date = datetime(next_date.year, next_date.month, day_of_month)
                break
            except ValueError:
                # Invalid date (e.g., Feb 30), reduce day by 1
                day_of_month -= 1
    
    else:
        # Default to tomorrow
        next_date = now + timedelta(days=1)
    
    # Return formatted date string
    return next_date.strftime("%Y-%m-%d %H:%M:%S")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Add a YAML test case to the scheduler')
    parser.add_argument('yaml_file', help='Path to the YAML test case file')
    parser.add_argument('--frequency', choices=['daily', 'weekly', 'monthly'], 
                        help='Frequency of scheduling (daily, weekly, monthly)')
    parser.add_argument('--day-of-week', type=int, choices=range(1, 8),
                        help='Day of week for weekly frequency (1-7, Monday=1)')
    parser.add_argument('--day-of-month', type=int, choices=range(1, 32),
                        help='Day of month for monthly frequency (1-31)')
    parser.add_argument('--force', action='store_true',
                        help='Force add even if already scheduled')
    
    args = parser.parse_args()
    
    # Check if the YAML file exists
    if not os.path.exists(args.yaml_file):
        logger.error(f"YAML file not found: {args.yaml_file}")
        return 1
    
    # Validate day arguments based on frequency
    if args.frequency == 'weekly' and args.day_of_month:
        logger.warning("Day of month is ignored for weekly frequency")
    
    if args.frequency == 'monthly' and args.day_of_week:
        logger.warning("Day of week is ignored for monthly frequency")
    
    # Add to scheduler
    success = add_yaml_to_scheduler(
        args.yaml_file,
        frequency=args.frequency,
        day_of_week=args.day_of_week,
        day_of_month=args.day_of_month,
        force=args.force
    )
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(main()) 