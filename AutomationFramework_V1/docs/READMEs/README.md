# AutomationFramework_V1

## Overview

AutomationFramework_V1 is a PySpark-based data validation framework with a plugin system for data transformations and file conversions. The framework consists of the following main components:

- **Data Validation**: Core functionality for comparing source and target datasets
- **Plugin System**: Extensible plugin architecture for data transformations
- **File Converters**: Specialized plugins for converting files between different formats
- **Visual Test Runner**: Real-time visualization of test execution with stage progression

## Recent Updates

### May 2024
- **Enhanced Test Scheduler**:
  - Added new option in interactive menu: "Add Test to Scheduler via YAML"
  - Added support for reading scheduler configuration directly from test case YAML files
  - Simplified scheduling process by automatically reading frequency and other settings from YAML
  - Example YAML configuration:
    ```yaml
    scheduler:
      enabled: 'yes'
      frequency: daily  # Options: daily, weekly, monthly
      username: S123455
    ```

## Directory Structure

The framework is organized into the following directories:

```
AutomationFramework_V1/
├── core/               # Core plugin system
├── utils/              # Utility functions and helpers
├── validation/         # Data validation components
├── reporting/          # Reporting components
├── plugins/            # Data processing plugins
│   └── file_converters/ # File converter plugins
├── config/             # Configuration files
│   └── file_converters/ # File converter configurations
├── copybooks/          # COBOL copybooks for EBCDIC files
├── Testcases/          # Test case definitions in YAML format
├── run_test_cases.py   # Batch test runner
├── run_tests_visual.py # Visual test runner
├── run_file_converter.py # File converter runner
├── framework_help.py   # Help script
└── README.md           # This file
```

## Getting Started

### Dependencies

- Python 3.7+
- PySpark 3.x
- Required Python packages: pyspark, yaml, pandas

### Interactive Mode

The framework includes a comprehensive interactive interface that provides easy menu-driven access to all major components:

```bash
# Start the interactive menu
python3 run_interactive.py
```

The interactive menu allows you to:
- Manage scheduler operations (list, add, remove, run scheduled tests)
- Execute test cases individually or in batch
- Run tests with the visual interface
- Access all functionality through a simple menu system

You can also jump directly to a specific module:

```bash
# Jump directly to scheduler operations
python3 run_interactive.py --mode scheduler

# Jump directly to test case execution
python3 run_interactive.py --mode test

# Jump directly to visual test execution
python3 run_interactive.py --mode visual
```

This interactive interface is ideal for new users and those who prefer a guided approach over remembering command-line arguments.

### Using the Help Script

The framework also includes a comprehensive help script that provides information about the available services, plugins, and how to use them:

```bash
# Show all information
python3 framework_help.py

# List all available plugins
python3 framework_help.py --plugins

# List all available services
python3 framework_help.py --services

# Show usage examples
python3 framework_help.py --examples

# Show configuration examples
python3 framework_help.py --configs

# Show framework flow diagram
python3 framework_help.py --flow
```

## Core Services

### Data Validation

The data validation service compares source and target datasets based on test case definitions in YAML format:

```bash
python3 validation/Comparison.py --yaml-file Testcases/example_test.yaml --single
```

### Batch Test Runner

Run multiple test cases in batch mode:

```bash
python3 run_test_cases.py
```

### Visual Test Runner

The Visual Test Runner provides a real-time visualization of test case execution with stage-by-stage progression:

```bash
python3 run_tests_visual.py
```

Options:
- `--test-dir <directory>`: Directory containing test case YAML files (default: Testcases)
- `--test-file <file>`: Run a specific test case file
- `--run-id <id>`: Execution run ID for tracking batch runs
- `--debug`: Enable debug mode with more detailed output

Example:
```bash
python3 run_tests_visual.py --test-file Testcases/example_test.yaml --debug
```

The Visual Test Runner displays:
- Flow diagram showing sequential test execution stages
- Color-coded status indicators for each stage (✓, ✗, →, •)
- Real-time test progress updates
- Aggregated test suite summary with pass/fail status

### Framework Self-Testing

The framework includes self-testing capabilities to ensure its core functionalities are working correctly:

```bash
# Run framework self-tests with detailed summary
python3 run_framework_self_tests.py

# Run internal framework tests
python3 run_internal_tests.py
```

Options:
- `--output-file <file>`: Save test results to a specified file (default: test_results.txt)
- `--html-report`: Generate an HTML report with detailed test results
- `--silent`: Suppress terminal output during tests
- `--debug`: Enable debug mode with more detailed output

Example:
```bash
python3 run_framework_self_tests.py --html-report --output-file framework_tests_result.txt
```

The framework self-tests validate:
- **Core Framework Functionality**: Tests basic framework operations and utilities
- **YAML Processor**: Tests YAML configuration parsing and validation
- **Logger**: Tests logging functionality including function logger decorator
- **Database Handler**: Tests database operations including execution runs
- **Command-Line Interface**: Tests CLI argument parsing and command execution

Test results are preserved in output files to avoid terminal clearing issues, and HTML reports provide detailed test summaries with pass/fail status for each test case.

### Test Scheduler

The framework includes a scheduler system that allows test cases to be scheduled for automatic execution at specified intervals:

```bash
python3 run_schedule_test_cases.py
```

Options:
- `--continuous`: Run in continuous mode, periodically checking for tests due for execution
- `--interval <seconds>`: Check interval in seconds (default: 300 seconds = 5 minutes)

Example for running in continuous mode:
```bash
python3 run_schedule_test_cases.py --continuous --interval 600
```

Test cases can be added to the scheduler in three ways:
1. Through the interactive menu (Option 5: Add Test to Scheduler)
2. Through the interactive menu (Option 6: Add Test to Scheduler via YAML)
3. Through direct command: `python3 add_to_scheduler.py <yaml_file>`

When using YAML files for scheduling, include a scheduler section in your YAML file:
```yaml
scheduler:
  enabled: 'yes'
  frequency: daily  # Options: daily, weekly, monthly
  username: S123455
```

Scheduler features:
- Test cases can be scheduled to run daily, weekly, or monthly
- The scheduler records the last execution time and next scheduled run time
- Failed tests remain in the scheduler and will be retried at the next scheduled time
- Test execution results are logged in the same format as manually executed tests
- Scheduling configuration can be defined directly in test case YAML files

### File Converter

Convert files between different formats using specialized plugins:

```bash
python3 run_file_converter.py --config csv_to_psv_config --input data/sample.csv --output data/output.psv
```

## Plugin System

The framework includes both data processing plugins and file converter plugins:

### Data Processing Plugins
- column_renamer.py: Rename DataFrame columns
- data_filter.py: Filter rows based on conditions
- data_type_converter.py: Convert column data types

### File Converter Plugins
- csv_to_psv_converter.py: Convert CSV files to PSV format
- json_to_csv_converter.py: Convert JSON files to CSV format
- xml_to_csv_converter.py: Convert XML files to CSV format
- mainframe_to_csv_converter.py: Convert mainframe/EBCDIC files to CSV format
- episodic_to_csv_converter.py: Convert episodic data to CSV format

## Modular, Versioned Data Loader System and Format Versioning

The framework uses a modular, versioned data loader system for reading source and target files in various formats (CSV, PSV, XML, Mainframe, Episodic, etc.).

### How It Works
- Each supported file format has its own directory of versioned loader modules in `converters/` (e.g., `csv2sparkdf_v1_1_2.py`).
- Each loader module implements a `convertToSparkDataFrame` function that returns a PySpark DataFrame.
- The loader version to use for each format is controlled by `format_versions.yaml`.
- The loader system will automatically use the specified version, and will fall back to the latest available if the specified version is missing.

### Example: Adding or Upgrading a Converter
1. **Add a new versioned loader module:**
   - Place a new file in `converters/` (e.g., `csv2sparkdf_v1_2_0.py`).
   - Implement the `convertToSparkDataFrame` function.
2. **Update `format_versions.yaml`:**
   - Set the `current_version` for the format to the new version (e.g., `1.2.0`).
   - List all available versions under `available_versions`.
3. **No code changes needed elsewhere!** The loader will pick up the new version automatically.

### Example `format_versions.yaml`
```yaml
formats:
  csv:
    current_version: 1.1.2
    available_versions:
      - 1.1.1
      - 1.1.2
  xml:
    current_version: 1.0.0
    available_versions:
      - 1.0.0
```

### Format Version Tracking in the Database
- Every test case execution records the source and target format type and version in the `testcase_execution_stats` table.
- The `format_versions` column contains a JSON summary, e.g.:
  ```json
  {
    "source": {"type": "csv", "version": "1.1.2"},
    "target": {"type": "xml", "version": "1.0.0"}
  }
  ```
- This enables full traceability of which loader version was used for every test case run.

### Benefits
- **Modularity:** Add or update format loaders without changing core code.
- **Traceability:** Know exactly which code version was used for every test.
- **Extensibility:** Easily support new formats or loader versions.

## YAML Test Case Structure

Test cases are defined in YAML files located in the `Testcases/` directory. Each YAML file describes a single test case, including source/target configuration, validation types, rules, and optional scheduler settings.

### Basic Structure Example
```yaml
test_case_name: "Customer Data Validation"
sid: "S123"
table_name: "customers"
run_type: "full"

source_configuration:
  source_type: "csv"
  source_location: "data/source/customers.csv"
  options:
    header: true
    delimiter: ","
  schema: null

target_configuration:
  target_type: "xml"
  target_location: "data/target/customers.xml"
  options:
    rowTag: "customer"
  schema: null

validation_types:
  - schema
  - count
  - data
  - rule

rules:
  - column: "email"
    validation:
      type: "regex"
      pattern: "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"

alert_rules: []

generate_html_report: true

scheduler:
  enabled: "no"
  frequency: "daily"
  day_of_week: 1
  day_of_month: null
```

### Key Fields

- **test_case_name**: Name of the test case (required)
- **sid**: Source system or user ID (optional)
- **table_name**: Logical table name (optional)
- **run_type**: "full" or "incremental" (optional)
- **source_configuration**:  
  - **source_type**: Format (csv, xml, psv, mainframe, etc.) (required)
  - **source_location**: Path to source file (required)
  - **options**: Format-specific options (optional)
  - **schema**: Custom schema (optional)
- **target_configuration**:  
  - **target_type**: Format (csv, xml, psv, mainframe, etc.) (required)
  - **target_location**: Path to target file (required)
  - **options**: Format-specific options (optional)
  - **schema**: Custom schema (optional)
- **validation_types**: List of validations to perform (schema, count, data, rule, etc.)
- **rules**: List of rule validations (see example above)
- **alert_rules**: List of alerting rules (optional)
- **generate_html_report**: Whether to generate an HTML report (optional)
- **scheduler**:  
  - **enabled**: "yes" or "no"
  - **frequency**: "daily", "weekly", "monthly"
  - **day_of_week**: 1-7 (for weekly)
  - **day_of_month**: 1-31 (for monthly)

### Notes
- All fields are case-sensitive.
- You can add custom fields as needed; they will be ignored by the framework unless used by a plugin.

## Test Execution Flow

The validation framework follows a sequential flow of execution stages:

1. **Initialization**: Set up the test environment and parameters
2. **Loading Configuration**: Parse the YAML test configuration
3. **Schema Validation**: Validate and compare source/target schemas
4. **Data Loading**: Load the source and target datasets
5. **Count Validation**: Compare record counts between datasets
6. **Data Comparison**: Compare actual data values between datasets
7. **Rule Validation**: Apply business rules and validations
8. **Report Generation**: Generate test results reports
9. **Results Storage**: Store test results in the database
10. **Jira Update**: Update Jira tickets with test results (optional)

The Visual Test Runner shows real-time progress through these stages, providing immediate feedback on test execution.

## Documentation

For more detailed information about specific components:

- [README_PLUGINS.md](README_PLUGINS.md): Information about the plugin system
- [README_FILE_CONVERTERS.md](README_FILE_CONVERTERS.md): Information about file converter plugins

## Creating Custom Plugins

See [README_PLUGINS.md](README_PLUGINS.md) for instructions on creating custom plugins.

## Troubleshooting & FAQ

### Common Errors and Solutions

**Q: I get an error like `X values for Y columns` when running tests.**
- **A:** This means the number of values in your database insert does not match the number of columns in the table. This can happen if you add new columns to the database schema but do not update the insert statement in the code. Make sure your insert statements match the current schema exactly.

**Q: My test case fails with `ModuleNotFoundError` or `ImportError` for a converter.**
- **A:** This usually means the required converter module (e.g., `csv2sparkdf_v1_1_2.py`) is missing from the `converters/` directory, or the version specified in `format_versions.yaml` does not exist. Check that the file exists and the version is correct.

**Q: The `format_versions` column in the database is empty.**
- **A:** The code must populate this column with a JSON summary of the source and target format types and versions. Make sure your code is passing this value in the insert statement (see documentation above for the correct structure).

**Q: My YAML test case is not being picked up or is skipped.**
- **A:**
  - Make sure the YAML file is in the correct directory (`Testcases/`) and has the `.yaml` or `.yml` extension.
  - If you are running in non-scheduled mode, scheduled test cases (with `scheduler.enabled: yes`) are skipped by default. Use the `--scheduled` flag to include them.

**Q: I get a `KeyError` or `ValueError` about missing fields in the YAML.**
- **A:** Check that your YAML test case includes all required fields, especially `source_configuration` and `target_configuration` with the correct keys (`source_type`, `source_location`, etc.). See the YAML Test Case Structure section for details.

**Q: The visual runner does not show the test suite execution flow until I press Enter.**
- **A:** This is intentional! The runner now prompts you to press Enter before displaying the summary, so you can review results at your own pace.

**Q: How do I add a new data format or upgrade a loader version?**
- **A:** See the Modular, Versioned Data Loader System and Format Versioning section above for step-by-step instructions.

**Q: How do I create a custom plugin?**
- **A:** See [README_PLUGINS.md](README_PLUGINS.md) for instructions and examples.

**Q: Where can I find more help or examples?**
- **A:**
  - Use the help script: `python3 framework_help.py`
  - See the documentation in the `docs/` directory
  - Review the sample YAML files in `Testcases/` 