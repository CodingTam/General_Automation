# OneTest Framework

The documentation for this project has been moved to the [docs directory](docs/READMEs/README.md).

## Quick Links

- [Main Documentation](docs/READMEs/README.md)
- [Commands Reference](docs/READMEs/Commands_Readme.md)
- [Plugins Documentation](docs/READMEs/README_PLUGINS.md)
- [File Converters Documentation](docs/READMEs/README_FILE_CONVERTERS.md)

## Onboarding

To onboard the OneTest framework into your environment, follow these setup steps:

1. **Create and Activate a Virtual Environment**
   ```bash
   python3 -m venv onetest
   source onetest/bin/activate  # For Unix/macOS
   onetest\Scripts\activate     # For Windows
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the Framework**
   ```bash
   ./onetest.sh
   ```
   This will initialize the framework using the configuration and environment you've set up.

### Next Steps
- Launch the interactive runner: `python3 run_interactive.py`
- Run a specific test case: `python3 run_test_cases.py Testcases/TC_001.yaml`
- Use the visual mode: `python3 run_tests_visual.py`

**Note:** Make sure you're using Python 3.6 or newer.

## Quick Start

### Interactive Mode

Use the interactive execution interface to easily run all framework components:

```bash
# Start the interactive menu
python3 main.py

# Run specific test case
python3 main.py --run-test path/to/test.py

# Run tests with visual progress
python3 main.py --visual

# Run tests in parallel
python3 main.py --parallel 4
```

The interactive interface provides menu-driven access to all major functions including:
- Test execution (single, scheduled, visual, parallel)
- Scheduler operations (add, list, remove, run)
- Plugin operations (list, run, create)
- File operations (converter, cleanup)
- Framework self-testing (internal, self-tests, comprehensive)
- Framework support (architecture, flow charts, examples)

### Command Helper

For detailed command assistance:

```bash
# List all available commands
python3 helpers/command_helper.py --list

# Show specific command category
python3 helpers/command_helper.py --category scheduler
```

---

For more information, see the [full documentation index](docs/index.md).

## ONE TEST - INTELLIGENT QA AUTOMATION FRAMEWORK

This framework provides a comprehensive solution for automated testing and data validation with role-based access control and extensive logging capabilities.

### Key Features

- **Role-Based Access Control**
  - Developer role: Full access to all features
  - Tester role: Restricted access to framework self-testing features

- **Test Execution**
  - Single test case execution
  - Scheduled test execution
  - Visual test progress display
  - Parallel test execution
  - Comprehensive test reporting

- **Scheduler Operations**
  - Add tests by traceability ID
  - Add tests by YAML configuration
  - List scheduled tests
  - Remove scheduled tests
  - Run scheduled tests

- **Plugin System**
  - List available plugins
  - Run plugins
  - Create new plugins
  - Plugin template generation

- **File Operations**
  - File format conversion
  - Project cleanup
  - Temporary file management

- **Framework Self-Testing**
  - Internal framework tests
  - Framework self-tests
  - Comprehensive component tests

- **Framework Support**
  - Architecture visualization
  - Flow charts
  - Directory structure
  - Format versions
  - Examples and configurations
  - Service listing

### Version Management

The framework uses a centralized version management system:

- Version information is stored in `utils/version.py`
- Consistent banner display is managed through `utils/banner.py`
- All scripts use the same version display mechanism

### Environment Configuration

The framework can be configured for different environments (QA, DEV, UAT, PROD) through:
- Command-line arguments
- Environment variables
- Configuration files

### Database Support

- SQLite (default)
- SQL Server (DB2)
- Comprehensive logging of database operations
- Graceful error handling and fallback mechanisms

### Getting Help

```bash
# Show framework help
python3 main.py --show-help

# Show framework architecture
python3 main.py --architecture

# Show available commands
python3 main.py --help
```

## Framework Components

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

### Quick Start

```bash
# Start the framework
./onetest.sh

# Run specific test case
./onetest.sh --run-test path/to/test.py

# Run tests with visual progress
./onetest.sh --visual

# Run tests in parallel
./onetest.sh --parallel 4
``` 