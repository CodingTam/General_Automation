# Automation Framework Database Structure

## Tables Overview

The framework uses an SQLite database to store test definitions, execution results, and scheduler information. The database consists of five main tables with relationships between them.

## Table Schemas

### 1. testcase

The `testcase` table is the central table that stores all test case definitions and metadata.

**Primary Key**: `traceability_id`

**Key Fields**:
- `traceability_id`: Unique identifier for the test case
- `test_case_name`: Name of the test case
- `sid`: Source identifier
- `table_name`: Name of the table being tested
- `source_type`, `source_location`: Source data configuration
- `target_type`, `target_location`: Target data configuration
- `validation_types`: Types of validations to perform

### 2. testcase_execution_stats

The `testcase_execution_stats` table stores the results of test case executions.

**Primary Key**: `execution_id`

**Foreign Keys**:
- `traceability_id` → `testcase.traceability_id`
- `execution_run_id` → `execution_run.execution_run_id`

**Key Fields**:
- `execution_id`: Unique identifier for the execution
- `status`: Overall execution status (PASS/FAIL)
- `schema_validation_status`, `count_validation_status`, `data_validation_status`: Status for each validation type
- `source_count`, `target_count`: Record counts
- `mismatched_records`: Number of records that don't match
- `execution_start_time`, `execution_end_time`: Timing information

### 3. execution_run

The `execution_run` table tracks batch execution runs and contains summary statistics.

**Primary Key**: `execution_run_id`

**Key Fields**:
- `execution_run_id`: Unique identifier for the batch run
- `start_time`, `end_time`: Timing information
- `total_test_cases`, `passed_test_cases`, `failed_test_cases`: Summary statistics
- `overall_status`: Overall status of the batch run

### 4. execution_log

The `execution_log` table stores detailed logs for each test execution step.

**Primary Key**: `log_entry_id`

**Foreign Keys**:
- `traceability_id` → `testcase.traceability_id`
- `execution_run_id` → `execution_run.execution_run_id`

**Key Fields**:
- `execution_id`: References the execution in testcase_execution_stats
- `timestamp`: When the log entry was created
- `level`: Log level (INFO, WARNING, ERROR, etc.)
- `message`: Log message
- `error_details`: Detailed error information if applicable

### 5. scheduler

The `scheduler` table maintains test schedules with frequency and timing information.

**Primary Key**: `schedule_id`

**Foreign Keys**:
- `traceability_id` → `testcase.traceability_id`

**Key Fields**:
- `yaml_file_path`: Path to the YAML test definition file
- `frequency`: How often to run (daily, weekly, monthly)
- `day_of_week`, `day_of_month`: Specific timing information
- `next_run_time`: When this test is next scheduled to run
- `last_run_time`: When this test was last run
- `enabled`: Whether the schedule is active

## Table Relationships Diagram

```
┌───────────────┐                  ┌──────────────────────┐
│   testcase    │                  │ testcase_execution_stats │
│               │                  │                      │
│ traceability_id │◄─────────────────┤ traceability_id     │
│               │                  │ execution_id        │
└───────┬───────┘                  │                      │
        │                          └──────────┬───────────┘
        │                                     │
        │                                     │ execution_id (implicit)
        │                                     │
        │                          ┌──────────▼───────────┐
        │                          │    execution_log     │
        │                          │                      │
        └────────────────────────►│ traceability_id     │
                                   │ log_entry_id        │
        ┌───────────────┐         └──────────┬───────────┘
        │ execution_run │                    │
        │               │                    │
        │execution_run_id│◄───────────────────┘
        │               │         execution_run_id
        └───────┬───────┘
                │
                │ execution_run_id
                │
        ┌───────▼───────┐
        │   scheduler   │
        │               │
        │  schedule_id  │
        │ traceability_id │◄───────┐
        └───────────────┘         │
                                  │
                                  │
                                  │
                  ┌───────────────┘
                  │ traceability_id
                  │
        ┌─────────┴─────┐
        │   testcase    │
        └───────────────┘
```

## Key Relationships Explained

1. **Test Case Definition to Execution (1:Many)**
   - A single test case (`testcase.traceability_id`) can have multiple executions (`testcase_execution_stats.traceability_id`)
   - Each execution record is tied to exactly one test case definition

2. **Test Case Execution to Logs (1:Many)**
   - A single test execution (`testcase_execution_stats.execution_id`) can have multiple log entries (`execution_log.execution_id`)
   - Each log entry is associated with exactly one test execution

3. **Batch Run to Test Executions (1:Many)**
   - A batch run (`execution_run.execution_run_id`) can include multiple test executions (`testcase_execution_stats.execution_run_id`)
   - Each test execution can belong to at most one batch run

4. **Test Case Definition to Schedule (1:Many)**
   - A test case (`testcase.traceability_id`) can have multiple schedules (`scheduler.traceability_id`)
   - Each schedule is tied to exactly one test case

5. **Batch Run to Logs (1:Many)**
   - A batch run (`execution_run.execution_run_id`) can have multiple log entries (`execution_log.execution_run_id`)
   - Log entries can be associated with both individual test executions and batch runs

## Database Normalization

The database follows normalization principles to minimize redundancy:

- Test case definitions are stored once in the `testcase` table
- Execution results reference test cases by ID rather than duplicating information
- Log entries are separated from execution statistics for better performance
- Scheduling information is kept separate from both test cases and execution records

This structure allows for efficient querying of test results, scheduling information, and detailed execution logs while maintaining data integrity. 