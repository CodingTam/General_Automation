# Framework Self-Tests

This directory contains tests that validate the core functionality of the automation framework itself. These tests are designed to ensure that the framework's internal components are working as expected.

## Purpose

The framework self-tests serve several important purposes:

1. Validate that core framework components are functioning correctly
2. Detect regressions when changes are made to the framework
3. Provide documentation of expected behavior through test cases
4. Ensure reliability of the testing infrastructure itself

## Test Categories

The self-tests cover the following areas:

- **Core Functionality**: Basic framework operations and utilities
- **Database Integration**: Tests for database connections and operations
- **Logging Subsystem**: Validation of logging functionality
- **Plugin Architecture**: Tests for plugin loading and execution
- **File Operations**: Tests for file handling and conversions

## Running the Tests

There are two ways to run the framework self-tests:

1. From the main menu:
   - Select option 15: "Run Core Framework Self-Tests"

2. From the command line:
   ```bash
   python main.py --run-framework-self-tests [--verbose]
   ```
   or directly:
   ```bash
   python run_framework_self_tests.py [--verbose]
   ```

## Adding New Tests

When adding new core framework functionality, it's recommended to add corresponding self-tests to validate the new features. Follow these steps:

1. Create a new test file with the naming convention `test_*.py`
2. Import the necessary framework modules
3. Create test classes inheriting from `unittest.TestCase`
4. Implement test methods with descriptive names starting with `test_`
5. Use appropriate assertions to validate expected behavior

## Test Reports

Test reports are automatically generated in the `reports/` directory when tests are run. These reports include details about test execution, failures, and errors. 