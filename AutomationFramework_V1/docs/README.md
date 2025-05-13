# Automation Framework

The documentation for this project has been moved to the [docs directory](docs/READMEs/README.md).

## Quick Links

- [Main Documentation](docs/READMEs/README.md)
- [Commands Reference](docs/READMEs/Commands_Readme.md)
- [Plugins Documentation](docs/READMEs/README_PLUGINS.md)
- [File Converters Documentation](docs/READMEs/README_FILE_CONVERTERS.md)

## Quick Start

### New Interactive Mode

Use the new interactive execution interface to easily run all framework components:

```bash
# Start the interactive menu
python3 run_interactive.py

# Jump directly to a specific module
python3 run_interactive.py --mode scheduler
python3 run_interactive.py --mode test
python3 run_interactive.py --mode visual
```

The interactive interface provides menu-driven access to all major functions including scheduler operations, test case execution, and visual test execution.

### Command Helper

For detailed command assistance:

```bash
# List all available commands
python command_helper.py --list

# Start interactive mode
python command_helper.py --interactive

# Show specific command category
python command_helper.py --category scheduler
```

---

For more information, see the [full documentation index](docs/index.md).

## ONE TEST - INTELLIGENT QA AUTOMATION FRAMEWORK

This framework provides a comprehensive solution for automated testing and data validation.

### Version Management

The framework now uses a centralized version management system:

- Version information is stored in `utils/version.py`
- Consistent banner display is managed through `utils/banner.py`
- All scripts use the same version display mechanism

### Framework Components

- **Test Runners**:
  - `run_all_tests.py`: Run all tests in the test directory
  - `run_test_cases.py`: Run specific test cases
  - `run_tests_visual.py`: Visual progress display for test execution
  - `run_interactive.py`: Interactive menu for test execution

- **Scheduler**:
  - `run_scheduler.py`: Run scheduled test cases
  - `run_schedule_test_cases.py`: Execute scheduled tests via Spark

- **Utilities**:
  - `version_info.py`: Display framework version information

- **Framework Self-Testing**:
  - `run_framework_self_tests.py`: Run tests from framework_self_tests directory
  - `run_internal_tests.py`: Run tests from the main tests directory
  - `run_tests.sh`: Bash script that runs tests and saves output to files
  - Framework tests validate core functionality including:
    - Core framework operations
    - YAML processing
    - Logging functionality
    - Database operations
    - Command-line interface

### Version Information

To check the framework version:

```bash
python version_info.py
```

For detailed version information:

```bash
python version_info.py --detailed
```

To display the framework banner:

```bash
python version_info.py --banner
```

To get version info in JSON format (for API integrations):

```bash
python version_info.py --json
```

### Environment Configuration

The framework can be configured for different environments (QA, DEV, UAT, PROD) 
by specifying the environment when calling the main entry points or using 
environment variables. 