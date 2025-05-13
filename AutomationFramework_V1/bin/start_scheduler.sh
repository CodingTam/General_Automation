#!/bin/bash
# Shell script to start the test scheduler in continuous mode

# Set path to the Python executable
PYTHON="python3"

# Set script path
SCHEDULER_SCRIPT="run_schedule_test_cases.py"

# Set check interval (in seconds) - 10 minutes = 600 seconds
CHECK_INTERVAL=600

echo "Starting test scheduler in continuous mode..."
echo "Checking for scheduled tests every $CHECK_INTERVAL seconds (10 minutes)"

# Start the scheduler in continuous mode with the specified interval
$PYTHON $SCHEDULER_SCRIPT --continuous --interval $CHECK_INTERVAL

# This script will keep running until manually stopped with Ctrl+C 