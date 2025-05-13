# Automation Framework Command Reference

This document provides a comprehensive reference for all commands used in the Automation Framework, with a focus on the test scheduling system.

## Table of Contents
- [Test Scheduler Commands](#test-scheduler-commands)
  - [Adding Tests to Scheduler](#adding-tests-to-scheduler)
  - [Listing Scheduled Tests](#listing-scheduled-tests)
  - [Removing Tests from Scheduler](#removing-tests-from-scheduler)
  - [Running Scheduled Tests](#running-scheduled-tests)
- [Examples](#examples)
  - [Basic Scheduling Workflow](#basic-scheduling-workflow)
  - [Advanced Scheduling Options](#advanced-scheduling-options)
- [Troubleshooting](#troubleshooting)

## Test Scheduler Commands

### Adding Tests to Scheduler

Add a test case to the scheduler with specified frequency and execution time.

```bash
python run_scheduler.py --add --traceability_id <ID> --interval <INTERVAL> --time <TIME> [--description <DESC>] [--force]
```

**Parameters:**
- `--add`: Required flag to add a test to the scheduler
- `--traceability_id`: Unique identifier for the test case (e.g., TEST-123)
- `--interval`: Frequency of execution
  - Options: `daily`, `weekly`, `monthly`
- `--time`: Time to run in 24-hour format (HH:MM)
- `--day`: Day of week (required when interval is weekly)
  - Options: `monday`, `tuesday`, `wednesday`, `thursday`, `friday`, `saturday`, `sunday`
- `--date`: Day of month (required when interval is monthly)
  - Range: 1-31
- `--description`: Optional description of the scheduled test
- `--force`: Override existing schedule if one exists

#### Adding Tests from YAML Files

You can also add tests directly from YAML test definition files:

```bash
python add_to_scheduler.py <YAML_FILE>
```

**Parameters:**
- `<YAML_FILE>`: Path to the YAML test case file

This approach automatically extracts test details from the YAML file and adds the test to the scheduler table.

### Listing Scheduled Tests

List all tests currently in the scheduler.

```bash
python run_scheduler.py --list
```

**Options:**
- `--traceability_id <ID>`: Filter by specific test ID
- `--interval <INTERVAL>`: Filter by interval (daily, weekly, monthly)
- `--status <STATUS>`: Filter by status (active, inactive)

### Removing Tests from Scheduler

Remove a test from the scheduler.

```bash
python run_scheduler.py --remove --traceability_id <ID>
```

**Parameters:**
- `--remove`: Required flag to remove a test
- `--traceability_id`: ID of the test to remove
- `--schedule_id`: Alternative option to remove by schedule ID

### Running Scheduled Tests

Execute tests that are scheduled to run.

```bash
python run_scheduler.py --run [--traceability_id <ID>] [--schedule_id <SCHEDULE_ID>]
```

**Parameters:**
- `--run`: Required flag to execute scheduled tests
- `--traceability_id`: Optional; run a specific test by ID
- `--schedule_id`: Optional; run a specific schedule by ID
- `--now`: Execute tests immediately, regardless of scheduled time

## Examples

### Basic Scheduling Workflow

**1. Add a daily test:**
```bash
python run_scheduler.py --add --traceability_id CUSTOMER-DATA-001 --interval daily --time "08:00" --description "Daily customer data validation"
```

**2. List all scheduled tests:**
```bash
python run_scheduler.py --list
```

**3. Run all currently scheduled tests:**
```bash
python run_scheduler.py --run
```

**4. Remove a test from scheduler:**
```bash
python run_scheduler.py --remove --traceability_id CUSTOMER-DATA-001
```

### Advanced Scheduling Options

**1. Add a weekly test that runs every Monday:**
```bash
python run_scheduler.py --add --traceability_id INVENTORY-002 --interval weekly --day monday --time "23:00" --description "Weekly inventory check"
```

**2. Add a monthly test that runs on the 1st of each month:**
```bash
python run_scheduler.py --add --traceability_id FINANCIAL-003 --interval monthly --date 1 --time "02:00" --description "Monthly financial report validation"
```

**3. Update an existing scheduled test (using --force):**
```bash
python run_scheduler.py --add --traceability_id CUSTOMER-DATA-001 --interval daily --time "12:00" --description "Updated daily customer validation" --force
```

**4. Run a specific scheduled test immediately:**
```bash
python run_scheduler.py --run --traceability_id INVENTORY-002 --now
```

## Troubleshooting

**Issue: Schedule not running at the expected time**
- Verify the system time matches the expected execution time
- Check that the scheduler service is running
- Examine logs for any errors during execution

**Issue: Duplicate schedule errors**
- Use the `--force` parameter to override existing schedules
- Remove the existing schedule first with `--remove`

**Issue: Test fails to execute**
- Verify the test case exists and is valid
- Check database connectivity
- Examine test case execution logs for specific errors

---

For more detailed information on the automation framework, please refer to the main documentation. 