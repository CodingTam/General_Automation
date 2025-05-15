#!/usr/bin/env python3
"""
Script to run scheduled test cases from the scheduler table.

This script:
1. Retrieves test cases due for execution from the scheduler table
2. Executes them using the test runner
3. Updates their next run date

Options:
--run-now: Run all scheduled test cases immediately regardless of next_run_date
--schedule-id: Run a specific test case by its schedule ID
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
import subprocess
from utils.db_handler import DBHandler
from utils.custom_logger import get_logger
from utils.banner import print_intro

# Initialize logger
logger = get_logger()

def get_scheduled_test_cases(db_handler, run_now=False, schedule_id=None):
    """
    Get test cases that are due to be executed.
    
    Args:
        db_handler: Database handler instance
        run_now: If True, get all scheduled test cases regardless of next_run_date
        schedule_id: If provided, get only this specific scheduled test case
        
    Returns:
        List of dictionaries containing test case information
    """
    cursor = db_handler.conn.cursor()
    
    if schedule_id:
        # Get specific test case by schedule_id
        cursor.execute('''
        SELECT s.schedule_id, s.traceability_id, s.yaml_file_path, s.username, 
               s.frequency, s.day_of_week, s.day_of_month, s.next_run_time,
               s.test_case_name, s.sid, s.table_name
        FROM scheduler s
        WHERE s.schedule_id = ?
        ''', (schedule_id,))
    elif run_now:
        # Get all scheduled test cases, but only the latest entry for each test_case_name+sid combination
        cursor.execute('''
        SELECT s.schedule_id, s.traceability_id, s.yaml_file_path, s.username, 
               s.frequency, s.day_of_week, s.day_of_month, s.next_run_time,
               s.test_case_name, s.sid, s.table_name
        FROM scheduler s
        INNER JOIN (
            SELECT test_case_name, sid, MAX(created_at) as latest_created
            FROM scheduler
            GROUP BY test_case_name, sid
        ) latest ON s.test_case_name = latest.test_case_name 
                AND s.sid = latest.sid 
                AND s.created_at = latest.latest_created
        WHERE s.enabled = 1
        ''')
    else:
        # Get test cases due for execution (where next_run_time <= now)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''
        SELECT s.schedule_id, s.traceability_id, s.yaml_file_path, s.username, 
               s.frequency, s.day_of_week, s.day_of_month, s.next_run_time,
               s.test_case_name, s.sid, s.table_name
        FROM scheduler s
        INNER JOIN (
            SELECT test_case_name, sid, MAX(created_at) as latest_created
            FROM scheduler
            GROUP BY test_case_name, sid
        ) latest ON s.test_case_name = latest.test_case_name 
                AND s.sid = latest.sid 
                AND s.created_at = latest.latest_created
        WHERE s.next_run_time <= ? AND s.enabled = 1
        ''', (now,))
    
    results = cursor.fetchall()
    
    test_cases = []
    for row in results:
        test_cases.append({
            'schedule_id': row[0],
            'traceability_id': row[1],
            'yaml_file_path': row[2],
            'username': row[3],
            'frequency': row[4],
            'day_of_week': row[5],
            'day_of_month': row[6],
            'next_run_time': row[7],
            'test_case_name': row[8],
            'sid': row[9],
            'table_name': row[10]
        })
    
    return test_cases

def run_test_case(yaml_file_path, traceability_id=None, schedule_id=None):
    """Run a test case using the test runner."""
    if not os.path.exists(yaml_file_path):
        logger.error(f"YAML file not found: {yaml_file_path}")
        return False
    
    logger.info(f"Running test case: {yaml_file_path}")
    try:
        # Use sys.executable to get the current Python interpreter path
        # Pass --scheduled flag to prevent processing all test cases
        cmd = [sys.executable, 'bin/run_test_cases.py', yaml_file_path, '--scheduled']
        
        # Add traceability_id if provided
        if traceability_id:
            cmd.extend(['--traceability-id', traceability_id])
            
        # Add schedule_id if provided
        if schedule_id:
            cmd.extend(['--schedule-id', schedule_id])
            
        # Run directly without capturing output so it displays in real-time
        logger.info("--- Beginning test case execution ---")
        result = subprocess.run(cmd, check=False)
        logger.info("--- End of test case execution ---")
        
        if result.returncode == 0:
            logger.info(f"Test case executed successfully: {yaml_file_path}")
            return True
        else:
            logger.error(f"Test case execution failed: {yaml_file_path}")
            return False
    except Exception as e:
        logger.error(f"Error running test case {yaml_file_path}: {str(e)}")
        return False

def calculate_next_run_date(frequency, day_of_week=None, day_of_month=None):
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

def update_next_run_date(db_handler, schedule_id, frequency, day_of_week=None, day_of_month=None):
    """Update the next run date for a scheduled test case."""
    next_run_date = calculate_next_run_date(frequency, day_of_week, day_of_month)
    
    cursor = db_handler.conn.cursor()
    cursor.execute('''
    UPDATE scheduler
    SET next_run_time = ?, updated_at = ?
    WHERE schedule_id = ?
    ''', (next_run_date, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), schedule_id))
    
    db_handler.conn.commit()
    logger.info(f"Updated next run date for schedule ID {schedule_id} to {next_run_date}")
    return next_run_date

def main():
    # Display the introductory banner
    print_intro("QA")  # Set the environment as needed
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run scheduled test cases')
    parser.add_argument('--run-now', action='store_true',
                        help='Run all scheduled test cases immediately')
    parser.add_argument('--schedule-id', type=str,
                        help='Run a specific test case by its schedule ID')
    
    args = parser.parse_args()
    
    # Initialize database handler
    db_handler = DBHandler()
    
    try:
        # Get scheduled test cases
        test_cases = get_scheduled_test_cases(
            db_handler,
            run_now=args.run_now,
            schedule_id=args.schedule_id
        )
        
        if not test_cases:
            logger.info("No scheduled test cases found to run.")
            return 0
        
        logger.info(f"Found {len(test_cases)} scheduled test case(s) to run.")
        
        # Run each test case
        for tc in test_cases:
            logger.info(f"Processing: {tc['test_case_name']} (Schedule ID: {tc['schedule_id']})")
            
            # Run the test case
            success = run_test_case(
                tc['yaml_file_path'],
                tc['traceability_id'],
                tc['schedule_id']
            )
            
            # Update next run date
            if success:
                next_run = update_next_run_date(
                    db_handler,
                    tc['schedule_id'],
                    tc['frequency'],
                    tc['day_of_week'],
                    tc['day_of_month']
                )
                logger.info(f"Next run scheduled for: {next_run}")
        
        return 0
    
    except Exception as e:
        logger.error(f"Error running scheduled test cases: {str(e)}")
        return 1
    
    finally:
        db_handler.close()

if __name__ == "__main__":
    exit(main()) 