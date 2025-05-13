# Automation Framework Testing Checklist

This document provides a comprehensive testing checklist to verify all components and workflows of the Automation Framework are functioning correctly.

## Table of Contents
- [Test Case Execution](#test-case-execution)
- [Visual Test Runner](#visual-test-runner)
- [Scheduler Operations](#scheduler-operations)
- [File Converter Operations](#file-converter-operations)
- [Database Operations](#database-operations)
- [Command Helper](#command-helper)
- [End-to-End Workflow](#end-to-end-workflow)

## Test Case Execution

### 1. Run a Single Test Case

**Prerequisites:**
- A valid YAML test case file exists (e.g., `Testcases/TC_001.yaml`)

**Command:**
```bash
python run_test_cases.py Testcases/TC_001.yaml
```

**Expected Output:**
- Test runs successfully
- Log shows test progress and results
- Test results are stored in the database
- Exit code 0 for success

**Validation:**
- Check the log output for success message
- Verify the execution stats in the database

### 2. Run All Test Cases in Directory

**Prerequisites:**
- Multiple YAML test case files exist in the Testcases directory

**Command:**
```bash
python run_test_cases.py
```

**Expected Output:**
- All non-scheduled tests are executed
- Skipped message for scheduled tests
- Summary of execution results
- Exit code 0 for success

**Validation:**
- Check that all non-scheduled test cases were executed
- Verify execution stats in the database
- Confirm scheduled tests were skipped

### 3. Run Tests with Pattern Matching

**Prerequisites:**
- YAML test case files with specific naming pattern exist

**Command:**
```bash
python run_test_cases.py --pattern "TC_00*.yaml"
```

**Expected Output:**
- Only tests matching the pattern are executed
- Summary of execution results
- Exit code 0 for success

**Validation:**
- Verify only the matching test cases were executed
- Check execution stats in the database

### 4. Run Tests Including Scheduled Tests

**Prerequisites:**
- YAML test case files with scheduler section exist

**Command:**
```bash
python run_test_cases.py --scheduled
```

**Expected Output:**
- All tests (including scheduled) are executed
- Summary of execution results
- Exit code 0 for success

**Validation:**
- Verify all test cases (including scheduled) were executed
- Check execution stats in the database

## Visual Test Runner

### 1. Run a Single Test Case Visually

**Prerequisites:**
- A valid YAML test case file exists (e.g., `Testcases/TC_001.yaml`)

**Command:**
```bash
python run_tests_visual.py --test-file Testcases/TC_001.yaml
```

**Expected Output:**
- Visual representation of test execution stages
- Color-coded status indicators
- Final test result summary
- Exit code 0 for success

**Validation:**
- Check that all stages are shown in the flow diagram
- Verify final test result matches expected outcome
- Confirm color coding is accurate

### 2. Run All Tests Visually

**Prerequisites:**
- Multiple YAML test case files exist in the Testcases directory

**Command:**
```bash
python run_tests_visual.py
```

**Expected Output:**
- Sequential visual execution of all non-scheduled tests
- Skipped message for scheduled tests
- Summary flow diagram at the end
- Exit code 0 for success

**Validation:**
- Verify all non-scheduled tests were visually executed
- Check that the final summary shows correct pass/fail counts
- Confirm scheduled tests were skipped

### 3. Run Tests Visually with Debug Mode

**Prerequisites:**
- YAML test case files exist

**Command:**
```bash
python run_tests_visual.py --debug
```

**Expected Output:**
- Detailed debug information during execution
- More verbose status updates
- Final test result with debug summary
- Exit code 0 for success

**Validation:**
- Verify additional debug information is displayed
- Check final result determination details

## Scheduler Operations

### 1. Add a Test to the Scheduler

**Prerequisites:**
- A valid YAML test case file exists (e.g., `Testcases/TC_002.yaml`)

**Command:**
```bash
python run_scheduler.py --add --traceability_id TEST-001 --interval daily --time "08:00" --description "Daily test run"
```

**Expected Output:**
- Test is added to the scheduler table
- Confirmation message
- Exit code 0 for success

**Validation:**
- Check the scheduler table in the database
- Verify the test was added with correct parameters

### 2. Add a Test from YAML File

**Prerequisites:**
- A valid YAML test case file exists with scheduler section

**Command:**
```bash
python add_to_scheduler.py Testcases/TC_003.yaml
```

**Expected Output:**
- Test is added to the scheduler table
- Confirmation message
- Exit code 0 for success

**Validation:**
- Check the scheduler table in the database
- Verify the test was added with correct parameters from YAML

### 3. List Scheduled Tests

**Prerequisites:**
- Tests have been added to the scheduler

**Command:**
```bash
python run_scheduler.py --list
```

**Expected Output:**
- List of all scheduled tests
- Details including frequency, time, etc.
- Exit code 0 for success

**Validation:**
- Verify all scheduled tests are displayed
- Check that the details match what was added

### 4. Remove a Test from Scheduler

**Prerequisites:**
- A test exists in the scheduler

**Command:**
```bash
python run_scheduler.py --remove --traceability_id TEST-001
```

**Expected Output:**
- Test is removed from the scheduler table
- Confirmation message
- Exit code 0 for success

**Validation:**
- Check the scheduler table in the database
- Verify the test was removed

### 5. Run Scheduled Tests

**Prerequisites:**
- Tests have been added to the scheduler

**Command:**
```bash
python run_scheduler.py --run
```

**Expected Output:**
- Scheduled tests due for execution are run
- Summary of execution results
- Exit code 0 for success

**Validation:**
- Verify scheduled tests were executed
- Check execution stats in the database
- Confirm next scheduled time was updated

### 6. Run Specific Scheduled Test Immediately

**Prerequisites:**
- A test exists in the scheduler

**Command:**
```bash
python run_scheduler.py --run --traceability_id TEST-001 --now
```

**Expected Output:**
- Specified test is executed immediately
- Execution results
- Exit code 0 for success

**Validation:**
- Verify the test was executed
- Check execution stats in the database

## File Converter Operations

### 1. Convert CSV to PSV

**Prerequisites:**
- A CSV file exists for conversion

**Command:**
```bash
python run_file_converter.py --config csv_to_psv_config --input data/sample.csv --output data/output.psv
```

**Expected Output:**
- PSV file is created with pipe-separated values
- Confirmation message
- Exit code 0 for success

**Validation:**
- Verify output file exists
- Check the content is correctly formatted with pipe separators

### 2. Convert JSON to CSV

**Prerequisites:**
- A JSON file exists for conversion

**Command:**
```bash
python run_file_converter.py --config json_to_csv_config --input data/sample.json --output data/output.csv
```

**Expected Output:**
- CSV file is created from JSON data
- Confirmation message
- Exit code 0 for success

**Validation:**
- Verify output file exists
- Check the content is correctly formatted as CSV

### 3. Convert XML to CSV

**Prerequisites:**
- An XML file exists for conversion

**Command:**
```bash
python run_file_converter.py --config xml_to_csv_config --input data/sample.xml --output data/output.csv
```

**Expected Output:**
- CSV file is created from XML data
- Confirmation message
- Exit code 0 for success

**Validation:**
- Verify output file exists
- Check the content is correctly formatted as CSV

### 4. Convert Mainframe to CSV

**Prerequisites:**
- A mainframe/EBCDIC file exists for conversion
- A COBOL copybook exists for structure definition

**Command:**
```bash
python run_file_converter.py --config mainframe_to_csv_config --input data/mainframe.dat --output data/output.csv --copybook copybooks/structure.cpy
```

**Expected Output:**
- CSV file is created from mainframe data
- Confirmation message
- Exit code 0 for success

**Validation:**
- Verify output file exists
- Check the content is correctly formatted as CSV

## Database Operations

### 1. Verify Test Case Table

**Prerequisites:**
- Tests have been executed

**Command:**
```bash
sqlite3 .testdb.db "SELECT * FROM testcase LIMIT 5;"
```

**Expected Output:**
- List of test case records
- Column headers and data
- Exit code 0 for success

**Validation:**
- Verify the table structure is correct
- Check that test case data is properly stored

### 2. Verify Execution Stats Table

**Prerequisites:**
- Tests have been executed

**Command:**
```bash
sqlite3 .testdb.db "SELECT * FROM execution_stats LIMIT 5;"
```

**Expected Output:**
- List of execution records
- Column headers and data
- Exit code 0 for success

**Validation:**
- Verify the table structure is correct
- Check that execution data is properly stored

### 3. Verify Scheduler Table

**Prerequisites:**
- Tests have been added to the scheduler

**Command:**
```bash
sqlite3 .testdb.db "SELECT * FROM scheduler;"
```

**Expected Output:**
- List of scheduled test records
- Column headers and data
- Exit code 0 for success

**Validation:**
- Verify the table structure is correct
- Check that scheduler data is properly stored

### 4. Verify Execution Run Table

**Prerequisites:**
- Batch runs have been executed

**Command:**
```bash
sqlite3 .testdb.db "SELECT * FROM execution_run LIMIT 5;"
```

**Expected Output:**
- List of execution run records
- Column headers and data
- Exit code 0 for success

**Validation:**
- Verify the table structure is correct
- Check that execution run data is properly stored

## Command Helper

### 1. Test Command Helper List Mode

**Prerequisites:**
- Command helper is installed

**Command:**
```bash
python command_helper.py --list
```

**Expected Output:**
- List of available command categories
- Instructions for using the tool
- Exit code 0 for success

**Validation:**
- Verify all command categories are displayed
- Check that instructions are clear

### 2. Test Command Helper Category Display

**Prerequisites:**
- Command helper is installed

**Command:**
```bash
python command_helper.py --category scheduler
```

**Expected Output:**
- Detailed list of scheduler commands
- Examples for each command
- Exit code 0 for success

**Validation:**
- Verify all scheduler commands are displayed
- Check that examples are provided and correct

### 3. Test Command Helper Function Execution

**Prerequisites:**
- Command helper is installed
- A valid YAML test case file exists

**Command:**
```python
# In Python code or interactive shell
from command_helper import add_yaml_to_scheduler
add_yaml_to_scheduler('Testcases/TC_003.yaml')
```

**Expected Output:**
- Function executes successfully
- Confirmation message
- Return value True for success

**Validation:**
- Verify the function worked as expected
- Check the scheduler table in the database

## End-to-End Workflow

### 1. Full Workflow Test

**Prerequisites:**
- All previous tests have passed

**Commands (in sequence):**
```bash
# 1. Create a test case
cp Testcases/TC_001.yaml Testcases/TC_Workflow.yaml

# 2. Add to scheduler
python add_to_scheduler.py Testcases/TC_Workflow.yaml

# 3. List scheduled tests
python run_scheduler.py --list

# 4. Run the scheduled test
python run_scheduler.py --run --traceability_id TC_Workflow --now

# 5. Check execution results
python run_tests_visual.py --test-file Testcases/TC_Workflow.yaml

# 6. Remove from scheduler
python run_scheduler.py --remove --traceability_id TC_Workflow

# 7. Clean up
rm Testcases/TC_Workflow.yaml
```

**Expected Output:**
- All steps complete successfully
- Consistent data across all tables
- Exit code 0 for each step

**Validation:**
- Verify the entire workflow completes without errors
- Check database for consistency across tables
- Confirm each step produced expected output

---

## Testing Checklist Summary

Use this table to track test execution status:

| Test Category | Test Case | Status | Notes |
|---------------|-----------|--------|-------|
| Test Execution | Run Single Test | ⬜ | |
| Test Execution | Run All Tests | ⬜ | |
| Test Execution | Run with Pattern | ⬜ | |
| Test Execution | Run with Scheduled | ⬜ | |
| Visual Runner | Run Single Test | ⬜ | |
| Visual Runner | Run All Tests | ⬜ | |
| Visual Runner | Run with Debug | ⬜ | |
| Scheduler | Add Test | ⬜ | |
| Scheduler | Add from YAML | ⬜ | |
| Scheduler | List Tests | ⬜ | |
| Scheduler | Remove Test | ⬜ | |
| Scheduler | Run Scheduled | ⬜ | |
| Scheduler | Run Specific | ⬜ | |
| File Converter | CSV to PSV | ⬜ | |
| File Converter | JSON to CSV | ⬜ | |
| File Converter | XML to CSV | ⬜ | |
| File Converter | Mainframe to CSV | ⬜ | |
| Database | Test Case Table | ⬜ | |
| Database | Execution Stats | ⬜ | |
| Database | Scheduler Table | ⬜ | |
| Database | Execution Run | ⬜ | |
| Command Helper | List Mode | ⬜ | |
| Command Helper | Category Display | ⬜ | |
| Command Helper | Function Execution | ⬜ | |
| End-to-End | Full Workflow | ⬜ | |

Legend:
- ⬜ Not Tested
- ✅ Passed
- ❌ Failed 