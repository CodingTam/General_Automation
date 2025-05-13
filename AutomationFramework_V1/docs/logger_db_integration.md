# Logger Database Integration

## Overview

The logger has been enhanced to automatically record all logging activities in the `execution_log` database table. This ensures comprehensive tracking of all actions within the automation framework.

## Features

- All log entries (debug, info, warning, error, critical) are recorded in the execution_log table
- Context information (execution_id, traceability_id, execution_run_id, test_case_name) is preserved in the database
- Stack traces for exceptions are captured in the error_details column
- Module and function information is automatically captured for better traceability
- All existing logger methods are compatible with the database integration

## Usage

The logger usage remains the same as before, with no code changes required in existing modules:

```python
from logger import logger

# Basic logging
logger.info("Some informational message")
logger.error("An error occurred")

# Logging with context
logger.info("Test completed", 
           execution_id="EXEC-123", 
           test_case_name="My Test Case")

# Exception logging
try:
    # some code that might fail
    result = process_data()
except Exception as e:
    logger.exception("Error processing data", 
                    execution_id="EXEC-123", 
                    traceability_id="TRACE-456")
```

## Database Structure

The logs are stored in the `execution_log` table with the following structure:

- `log_entry_id`: Unique identifier for the log entry
- `execution_id`: ID of the test execution (if available)
- `timestamp`: When the log was created
- `level`: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `module`: Source module where the log was generated
- `function`: Function where the log was generated
- `message`: Log message
- `error_details`: Stack trace for exceptions
- `traceability_id`: Test case traceability ID (if available)
- `execution_run_id`: Batch execution run ID (if available)
- `test_case_name`: Name of the test case (if available)

## Benefits

- Complete audit trail of all system activities
- Better debugging capabilities with context-aware logging
- Improved test execution traceability
- Consistent logging between file and database
- No performance impact on existing code

## Testing

A test script (`test_logger.py`) is included to verify the database logging functionality:

```
python test_logger.py
```

You can verify logs are written to the database with:

```
sqlite3 /path/to/automation.db "SELECT * FROM execution_log ORDER BY timestamp DESC LIMIT 10;"
``` 