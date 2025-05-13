#!/usr/bin/env python3
"""
AutomationFramework_V1 Help Script

This script provides information about the framework's components and how to run them.
"""

import os
import sys
import argparse
import importlib.util
import textwrap
import yaml


class FrameworkHelp:
    def __init__(self):
        # Get the helpers directory path (current script location)
        self.helpers_dir = os.path.dirname(os.path.abspath(__file__))
        # Get the framework root directory (one level up)
        self.framework_root = os.path.dirname(self.helpers_dir)
        self.terminal_width = min(os.get_terminal_size().columns, 100)

    def print_header(self, text):
        """Print a section header."""
        print("\n" + "=" * self.terminal_width)
        print(f"{text.upper()}")
        print("=" * self.terminal_width)

    def print_subheader(self, text):
        """Print a subsection header."""
        print("\n" + "-" * self.terminal_width)
        print(f"{text}")
        print("-" * self.terminal_width)

    def wrap_text(self, text, indent=0):
        """Wrap text to fit the terminal width."""
        wrapped = textwrap.fill(
            text, 
            width=self.terminal_width - indent,
            initial_indent=" " * indent,
            subsequent_indent=" " * indent
        )
        return wrapped

    def show_flow_chart(self):
        """Display an ASCII flow chart of the framework's components."""
        self.print_header("Framework Flow Chart")
        
        # Create ASCII art flow chart
        flow_chart = """
                                  +------------------------+
                                  |                        |
                                  |  run_test_cases.py     |
                                  |  (Batch Test Runner)   |
                                  |                        |
                                  +------------+-----------+
                                               |
                                               v
                  +------------------------+   |   +------------------------+
                  |                        |   |   |                        |
                  |  YAML Configuration    |<--+-->|  Database Logger       |
                  |  (Test Cases)          |       |  (Results Storage)     |
                  |                        |       |                        |
                  +------------+-----------+       +--------+-------+-------+
                               |                            |       |
                               v                            |       |
+------------------------+    |    +------------------------+       |    +------------------------+
|                        |    |    |                        |       |    |                        |
|  Plugin System         |<---+--->|  Data Validation       |       +--->|  run_tests_visual.py  |
|  (Transformations)     |         |  (Comparison Engine)   |<---+       |  (Visual Test Runner) |
|                        |         |                        |    |       |                        |
+------------+-----------+         +------------+-----------+    |       +------------------------+
             |                                  |                |
             v                                  v                |       +------------------------+
+------------------------+         +------------------------+    |       |                        |
|                        |         |                        |    +------>| run_schedule_test_cases.py |
|  File Converters       |         |  Reporting System      |            |  (Test Scheduler)     |
|  (Format Conversion)   |         |  (Results & HTML)      |            |                        |
|                        |         |                        |            +------------------------+
+------------------------+         +------------------------+

Main Components:
1. Test Runner - Orchestrates the execution of test cases
2. YAML Configuration - Defines test cases and validation rules
3. Plugin System - Allows extending functionality with custom transformations
4. Data Validation - Compares datasets and validates data quality
5. File Converters - Converts files between different formats
6. Reporting System - Generates reports of validation results
7. Database Logger - Stores execution results and test information
8. Visual Test Runner - Provides real-time visualization of test execution with stage progression
9. Test Scheduler - Schedules test cases for automatic execution at specified intervals
10. Version-Controlled Modular Converters - A system to manage and version different converters

Data Flow:
- Test cases are defined in YAML and processed by the test runner
- Data undergoes transformations via the plugin system if needed
- File converters prepare data for validation when format conversion is required
- Data validation compares datasets and applies validation rules
- Results are logged to the database and reports are generated
- The Visual Test Runner provides real-time feedback on test execution stages
- The Test Scheduler automatically runs tests according to their schedule
"""
        print(flow_chart)

    def show_detailed_architecture_diagram(self):
        """Display a detailed architecture diagram of the framework including all flows."""
        self.print_header("Detailed Architecture Diagram")
        
        # Create detailed architecture diagram
        architecture_diagram = """
+----------------------------------------------------------------------------------------------------------+
|                                     AUTOMATION FRAMEWORK ARCHITECTURE                                     |
+----------------------------------------------------------------------------------------------------------+

+----------------+    +-------------------+    +-------------------+    +--------------------+
| run_interactive|    | run_test_cases.py |    | run_scheduler.py  |    | run_tests_visual.py|
|      .py       |    |                   |    |                   |    |                    |
+-------+--------+    +--------+----------+    +---------+---------+    +---------+----------+
        |                     |                          |                        |
        v                     v                          v                        v
+-------+---------------------+---------------------------------------------+    |
|                        USER INTERFACE LAYER                               |    |
|                                                                           |    |
|  +----------------+ +---------------+ +---------------+ +---------------+ |    |
|  | Interactive UI | | Command Line  | | Scheduler UI  | | Visual Test UI| |    |
|  |                | | Interface     | |               | |               | |    |
|  +-------+--------+ +-------+-------+ +-------+-------+ +-------+-------+ |    |
+-----------|-------------------|-----------------|----------------|--------+    |
            |                   |                 |                |             |
            v                   v                 v                v             |
+-----------|-------------------|-----------------|----------------|-------------|----+
|                                     CORE EXECUTION LAYER                               |
|                                                                                        |
| +------------------------+   +--------------------------+   +------------------------+ |
| |    Test Case Runner    |<->|       Scheduler         |<->|  Execution Manager     | |
| |                        |   |                          |   |                        | |
| +-----+-------------+----+   +-------------+------------+   +-------------+----------+ |
|       |             |                      |                              |            |
|       v             |                      v                              v            |
| +-----+------+    +-v-----------+   +-----+--------+   +-----------------+----------+ |
| |  Validator |    | Data Source |   | Job Manager  |   |     Visual Execution       | |
| |            |    | Connector   |   |              |   |                            | |
| +-----+------+    +-------------+   +--------------+   +----------------------------+ |
+-------|-----------------------------------------------------------------------------+
        |
        v
+-------|--------------------------------------------------------------------+
|  +----+----------------+   VALIDATION ENGINE LAYER                          |
|  |                     |                                                    |
|  |  +----------------+ |   +---------------+   +--------------------+       |
|  |  | Comparison.py  | |   | Schema        |   | Data Validation    |       |
|  |  |                | |-->| Validation    |-->|                    |       |
|  |  +----------------+ |   |               |   |                    |       |
|  |                     |   +---------------+   +--------------------+       |
|  |  +----------------+ |   +---------------+   +--------------------+       |
|  |  | Rule          | |   | Count         |   | Duplicate/Null     |       |
|  |  | Validation    | |-->| Validation    |-->| Validation         |       |
|  |  +----------------+ |   |               |   |                    |       |
|  +---------------------+   +---------------+   +--------------------+       |
+----------------------------------------------------------------------------+
        |
        v
+----------------------------------------------------------------------------+
|                            DATA LAYER                                       |
|                                                                             |
|  +---------------+    +----------------+    +-----------------+             |
|  | Data Sources  |    | Data          |    | Plugin API      |             |
|  | Connectors    |<-->| Converters    |<-->|                 |             |
|  |               |    |                |    |                 |             |
|  +---------------+    +----------------+    +-----------------+             |
|                                                                             |
|  +-----------------------+    +----------------------------+                |
|  | YAML Processor        |    | Database Handler           |                |
|  |                       |<-->|                            |                |
|  |                       |    | (SQLite)                   |                |
|  +-----------------------+    +----------------------------+                |
+----------------------------------------------------------------------------+
        |
        v
+----------------------------------------------------------------------------+
|                          DATABASE LAYER                                     |
|                                                                             |
|  +----------------+   +----------------+   +---------------------+          |
|  | Test Cases DB  |   | Executions DB  |   | Scheduler DB        |          |
|  | - test metadata|   | - results      |   | - scheduled jobs    |          |
|  | - configurations   | - statistics   |   | - job status        |          |
|  +----------------+   +----------------+   +---------------------+          |
|                                                                             |
|  +----------------+   +----------------+                                    |
|  | Execution Logs |   | Execution Runs |                                    |
|  | - log entries  |   | - batch runs   |                                    |
|  | - errors       |   | - summary      |                                    |
|  +----------------+   +----------------+                                    |
+----------------------------------------------------------------------------+
        |
        v
+----------------------------------------------------------------------------+
|                      REPORTING & LOGGING LAYER                              |
|                                                                             |
|  +----------------+   +----------------+   +---------------+                |
|  | Logger         |   | Reports        |   | HTML Reports  |                |
|  | - console      |   | - test results |   | - formatting  |                |
|  | - file         |   | - summary      |   | - templates   |                |
|  | - database     |   | - validation   |   |               |                |
|  +----------------+   +----------------+   +---------------+                |
+----------------------------------------------------------------------------+

Main Data Flows:

1. Test Execution Flow:
   - User initiates test through UI → Test Case Runner loads YAML configuration → 
     Validator executes tests → Results stored in DB → Reports generated

2. Scheduled Test Flow:
   - Scheduler checks for due tests → Executes tests via Test Case Runner → 
     Results stored with schedule ID → Updates schedule status → Reports generated

3. Data Validation Flow:
   - Validation Engine loads source and target data → Performs validations 
     (schema, count, data, rules) → Generates comparison results → Stores results in DB

4. File Conversion Flow:
   - Data Source identifies file type → File Converter applies transformation → 
     Converted data passed to Validation Engine

5. Reporting Flow:
   - Test execution completes → Execution results retrieved from DB → 
     Format into reports → Generate HTML/text output → Store in reports directory

6. Logging Flow:
   - Components generate log messages → Logger adds context (execution ID, etc.) → 
     Writes to console/file → Stores in execution logs table

7. Interactive Command Flow:
   - User selects operation → Interactive UI presents options → 
     Command executed → Results displayed → Return to menu
"""
        print(architecture_diagram)

    def list_plugins(self):
        """List all available plugins."""
        self.print_header("Available Plugins")
        
        # Core plugins
        plugins_dir = os.path.join(self.framework_root, "plugins")
        file_converters_dir = os.path.join(plugins_dir, "file_converters")
        
        # Regular plugins
        self.print_subheader("Data Processing Plugins")
        if os.path.exists(plugins_dir):
            regular_plugins = [f for f in os.listdir(plugins_dir) 
                              if f.endswith('.py') and f != '__init__.py' and not os.path.isdir(os.path.join(plugins_dir, f))]
            
            for plugin in sorted(regular_plugins):
                plugin_path = os.path.join(plugins_dir, plugin)
                module_name = plugin[:-3]  # Remove .py extension
                
                # Load the plugin to get its docstring
                try:
                    spec = importlib.util.spec_from_file_location(module_name, plugin_path)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    
                    # Get the docstring for the run function if available
                    docstring = ""
                    if hasattr(module, 'run') and module.run.__doc__:
                        docstring = module.run.__doc__.strip().split('\n')[0]
                    elif module.__doc__:
                        docstring = module.__doc__.strip().split('\n')[0]
                    
                    print(f"- {module_name}: {docstring}")
                except Exception as e:
                    print(f"- {module_name}: (Error loading plugin: {str(e)})")
        else:
            print("  Plugins directory not found.")
        
        # File converter plugins
        self.print_subheader("File Converter Plugins")
        if os.path.exists(file_converters_dir):
            converter_plugins = [f for f in os.listdir(file_converters_dir) 
                               if f.endswith('.py') and f != '__init__.py']
            
            for plugin in sorted(converter_plugins):
                plugin_path = os.path.join(file_converters_dir, plugin)
                module_name = plugin[:-3]  # Remove .py extension
                
                try:
                    spec = importlib.util.spec_from_file_location(module_name, plugin_path)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    
                    # Get the docstring for the run function if available
                    docstring = ""
                    if hasattr(module, 'run') and module.run.__doc__:
                        docstring = module.run.__doc__.strip().split('\n')[0]
                    elif module.__doc__:
                        docstring = module.__doc__.strip().split('\n')[0]
                    
                    print(f"- {module_name}: {docstring}")
                except Exception as e:
                    print(f"- {module_name}: (Error loading plugin: {str(e)})")
        else:
            print("  File converters directory not found.")
        
        # List modular converters
        self.print_subheader("Modular Versioned Converters")
        converters_dir = os.path.join(self.framework_root, "converters")
        if os.path.exists(converters_dir):
            # Group by type and version
            converter_modules = {}
            for f in os.listdir(converters_dir):
                if f.endswith('.py') and f != '__init__.py':
                    if f.count('2sparkdf_v') > 0:
                        parts = f.split('2sparkdf_v')
                        format_type = parts[0]
                        version = parts[1].split('.py')[0].replace('_', '.')
                        
                        if format_type not in converter_modules:
                            converter_modules[format_type] = []
                        converter_modules[format_type].append(version)
            
            # Print organized by format type
            for format_type, versions in sorted(converter_modules.items()):
                print(f"- {format_type}: versions {', '.join(sorted(versions))}")
        else:
            print("  Modular converters directory not found.")

    def show_format_versions(self):
        """Show information about the format versions and converter modules."""
        self.print_header("Format Versions Configuration")
        
        format_versions_path = os.path.join(self.framework_root, "format_versions.yaml")
        if os.path.exists(format_versions_path):
            try:
                with open(format_versions_path, 'r') as f:
                    format_config = yaml.safe_load(f)
                
                self.print_subheader("Current Format Versions")
                if 'formats' in format_config:
                    for format_name, format_info in sorted(format_config['formats'].items()):
                        current_version = format_info.get('current_version', 'N/A')
                        available_versions = format_info.get('available_versions', [])
                        print(f"- {format_name}: current version {current_version}")
                        print(f"  Available versions: {', '.join(str(v) for v in available_versions)}")
                else:
                    print("No formats found in configuration.")
                
                self.print_subheader("Modular Converter System")
                print(self.wrap_text("""
The AutomationFramework uses a modular, versioned system for data format converters. Each format (CSV, PSV, XML, etc.) has its own versioned loader modules (e.g., csv2sparkdf_v1_1_2.py). 

The format_versions.yaml file specifies which version to use for each format, and the system will automatically load the correct module version at runtime.

When a converter is requested, the system:
1. Checks format_versions.yaml for the current version
2. Dynamically imports the corresponding module
3. Falls back to the latest version if needed
4. Executes the converter with the provided parameters
                """))
            except Exception as e:
                print(f"Error reading format_versions.yaml: {str(e)}")
        else:
            print("format_versions.yaml not found.")

    def list_services(self):
        """List all available services/components."""
        self.print_header("Available Services")
        
        services = [
            {
                "name": "Data Validation",
                "description": "Compare source and target datasets and validate data quality",
                "runner": "validation/Comparison.py",
                "usage": "python3 run_test_cases.py"
            },
            {
                "name": "Batch Test Runner",
                "description": "Run multiple test cases in batch mode",
                "runner": "run_test_cases.py",
                "usage": "python3 run_test_cases.py"
            },
            {
                "name": "Visual Test Runner",
                "description": "Real-time visualization of test execution with stage-by-stage progression",
                "runner": "run_tests_visual.py",
                "usage": "python3 run_tests_visual.py [--test-file <file_path>] [--test-dir <directory>] [--run-id <id>] [--debug]"
            },
            {
                "name": "Test Scheduler",
                "description": "Schedule test cases for automatic execution at specified intervals",
                "runner": "run_schedule_test_cases.py",
                "usage": "python3 run_schedule_test_cases.py [--continuous] [--interval <seconds>]"
            },
            {
                "name": "File Converter",
                "description": "Convert files between different formats",
                "runner": "run_file_converter.py",
                "usage": "python3 run_file_converter.py --config <config_name> [--input <input_path>] [--output <output_path>]"
            },
            {
                "name": "Plugin System",
                "description": "Core plugin system for extending functionality",
                "runner": "core/plugin_runner.py and core/plugin_integration.py",
                "usage": "Used as part of other services"
            },
            {
                "name": "Framework Helper",
                "description": "Interactive command line tool for executing framework commands",
                "runner": "helpers/command_helper.py",
                "usage": "python3 helpers/command_helper.py --interactive"
            },
            {
                "name": "Framework Help",
                "description": "Documentation and user guide for the automation framework",
                "runner": "helpers/framework_help.py",
                "usage": "python3 helpers/framework_help.py --interactive"
            }
        ]
        
        for service in services:
            self.print_subheader(service["name"])
            print(f"Description: {service['description']}")
            print(f"Runner: {service['runner']}")
            print(f"Usage: {service['usage']}")

    def show_examples(self):
        """Show usage examples for the framework."""
        self.print_header("Usage Examples")
        
        examples = [
            {
                "title": "Running Batch Test Cases",
                "command": "python3 run_test_cases.py",
                "description": "Run all test cases in the Testcases directory in batch mode."
            },
            {
                "title": "Running Tests with Visual Progress",
                "command": "python3 run_tests_visual.py",
                "description": "Run test cases with a visual flowchart showing real-time execution progress and stage-by-stage results."
            },
            {
                "title": "Running a Specific Test with Visual Feedback",
                "command": "python3 run_tests_visual.py --test-file Testcases/example_test.yaml",
                "description": "Run a specific test case with visual progress tracking and detailed stage flow visualization."
            },
            {
                "title": "Running Scheduled Test Cases",
                "command": "python3 run_schedule_test_cases.py",
                "description": "Run scheduled test cases that are due for execution based on the scheduler table."
            },
            {
                "title": "Running Scheduler in Continuous Mode",
                "command": "python3 run_schedule_test_cases.py --continuous --interval 600",
                "description": "Run the scheduler in continuous mode, checking for test cases every 10 minutes."
            },
            {
                "title": "Converting a CSV File to PSV",
                "command": "python3 run_file_converter.py --config csv_to_psv_config --input data/sample.csv --output data/output.psv",
                "description": "Convert a CSV file to PSV format using the CSV to PSV converter plugin."
            },
            {
                "title": "Converting a JSON File to CSV",
                "command": "python3 run_file_converter.py --config json_to_csv_config --input data/sample.json --output data/output.csv",
                "description": "Convert a JSON file to CSV format using the JSON to CSV converter plugin."
            },
            {
                "title": "Converting an XML File to CSV",
                "command": "python3 run_file_converter.py --config xml_to_csv_config --input data/sample.xml --output data/output.csv",
                "description": "Convert an XML file to CSV format using the XML to CSV converter plugin."
            },
            {
                "title": "Converting a Mainframe File to CSV with Cobrix",
                "command": "python3 run_file_converter.py --config mainframe_to_csv_config --input data/sample.dat --output data/output.csv --copybook data/copybooks/sample.cpy",
                "description": "Convert a mainframe file to CSV format using the Mainframe to CSV converter with Cobrix and a copybook definition."
            },
            {
                "title": "Converting an EBCDIC File to CSV with Cobrix",
                "command": "python3 run_file_converter.py --config ebcdic_to_csv_config --input data/sample.dat --output data/output.csv --copybook data/copybooks/sample.cpy",
                "description": "Convert an EBCDIC file to CSV format using Cobrix and a copybook definition."
            },
            {
                "title": "Using the Interactive Command Helper",
                "command": "python3 helpers/command_helper.py --interactive",
                "description": "Start an interactive session to execute various framework commands through a menu-driven interface."
            },
            {
                "title": "Using the Interactive Framework Help",
                "command": "python3 helpers/framework_help.py --interactive",
                "description": "Start an interactive help session to browse documentation about the framework components and features."
            }
        ]
        
        for example in examples:
            self.print_subheader(example["title"])
            print(f"Command: {example['command']}")
            print(self.wrap_text(f"Description: {example['description']}", indent=0))

    def show_configuration_examples(self):
        """Show configuration file examples."""
        self.print_header("Configuration Examples")
        
        # Test Case YAML Example
        self.print_subheader("Test Case Configuration (YAML)")
        test_case_yaml = """
# Example test_case.yaml
test_case_name: Example Test Case
description: Compare source and target tables with primary key validation
source:
  type: csv
  path: path/to/source.csv
  options:
    header: true
    delimiter: ","
target:
  type: xml
  path: path/to/target.xml
  options:
    rowTag: "record" 
validation:
  primary_keys:
    - id
  columns_to_validate:
    - name
    - amount
    - date
"""
        print(test_case_yaml)
        
        # File Converter Config Example
        self.print_subheader("File Converter Configuration (YAML)")
        converter_yaml = """
# Example csv_to_psv_config.yaml
plugins:
  - name: csv_to_psv_converter.py
    function: run
    params:
      input_path: "/path/to/input/file.csv"
      output_path: "/path/to/output/file.psv"
      header: true
      delimiter: ","
"""
        print(converter_yaml)
        
        # Format Versions YAML Example
        self.print_subheader("Format Versions Configuration (YAML)")
        format_versions_yaml = """
# format_versions.yaml
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
  ebcdic:
    current_version: 1.0.0
    available_versions:
      - 1.0.0
"""
        print(format_versions_yaml)
        
        # EBCDIC Converter Config Example
        self.print_subheader("EBCDIC with Cobrix Configuration")
        ebcdic_yaml = """
# Example YAML test case using EBCDIC with Cobrix
source:
  type: ebcdic
  path: /path/to/ebcdic_file.dat
  options:
    copybook_path: /path/to/copybook.cpy
    encoding: ebcdic
    record_length: 80

# Or using a converter configuration:
plugins:
  - name: ebcdic_to_csv_converter.py
    function: run
    params:
      input_path: "/path/to/input/ebcdic_data.dat"
      output_path: "/path/to/output/data.csv"
      copybook_path: "/path/to/copybook.cpy"
      encoding: "ebcdic"
"""
        print(ebcdic_yaml)

    def show_directory_structure(self):
        """Show the directory structure of the framework."""
        self.print_header("Framework Directory Structure")
        
        directory_structure = """
AutomationFramework_V1/
├── converters/                  # Modular versioned data format converters
│   ├── avro2sparkdf_v1_0_0.py
│   ├── binary2sparkdf_v1_0_0.py
│   ├── csv2sparkdf_v1_1_1.py
│   ├── csv2sparkdf_v1_1_2.py
│   ├── ebcdic2sparkdf_v1_0_0.py
│   ├── ...
├── core/                        # Core framework functionality
├── docs/                        # Documentation files
├── helpers/                     # Helper tools and utilities
│   ├── command_helper.py        # Interactive command line interface
│   ├── framework_help.py        # This help script
├── plugins/                     # Plugin system components
│   ├── file_converters/         # File converter plugins
├── reports/                     # Generated test reports
├── Testcases/                   # Test case YAML files
├── validation/                  # Data validation components
├── format_versions.yaml         # Format converter version configuration
├── run_test_cases.py            # Main batch test runner
├── run_tests_visual.py          # Visual test execution UI
└── run_schedule_test_cases.py   # Test scheduler
"""
        print(directory_structure)
        
        self.print_subheader("Directory Usage")
        print(self.wrap_text("""
The framework is organized into modular components:

- converters/ contains versioned data format converters (new modular system)
- helpers/ contains command-line utilities and help documentation
- plugins/ contains extension points for data transformations
- Testcases/ contains YAML test case definitions
- validation/ contains data validation components
- reports/ stores test execution results

Key files:
- format_versions.yaml specifies which converter version to use for each format
- run_test_cases.py is the main entry point for batch test execution
- run_tests_visual.py provides visual feedback during test execution
- run_schedule_test_cases.py handles scheduled execution of tests
        """, indent=2))

    def show_help(self):
        """Show main help menu or run interactive mode."""
        parser = argparse.ArgumentParser(
            description="Help script for AutomationFramework_V1",
            formatter_class=argparse.RawTextHelpFormatter
        )
        
        parser.add_argument(
            "--plugins", action="store_true",
            help="List available plugins"
        )
        parser.add_argument(
            "--services", action="store_true",
            help="List available services/components"
        )
        parser.add_argument(
            "--examples", action="store_true",
            help="Show usage examples"
        )
        parser.add_argument(
            "--configs", action="store_true",
            help="Show configuration examples"
        )
        parser.add_argument(
            "--flow", action="store_true",
            help="Show framework flow chart"
        )
        parser.add_argument(
            "--architecture", action="store_true",
            help="Show detailed architecture diagram with all flows"
        )
        parser.add_argument(
            "--formats", action="store_true",
            help="Show format versions information"
        )
        parser.add_argument(
            "--structure", action="store_true",
            help="Show directory structure"
        )
        parser.add_argument(
            "--all", action="store_true",
            help="Show all help information"
        )
        parser.add_argument(
            "--interactive", action="store_true",
            help="Run in interactive mode"
        )
        
        args = parser.parse_args()
        
        # If interactive mode is selected
        if args.interactive:
            self.run_interactive()
            return
        
        # If no arguments (or only interactive), show welcome message and brief help
        if not (args.plugins or args.services or args.examples or args.configs or 
                args.flow or args.architecture or args.formats or args.structure or args.all):
            self.welcome()
            parser.print_help()
            return
        
        # Process standard arguments
        if args.all:
            self.welcome()
            self.list_services()
            self.list_plugins()
            self.show_flow_chart()
            self.show_detailed_architecture_diagram()
            self.show_format_versions()
            self.show_examples()
            self.show_configuration_examples()
            self.show_directory_structure()
        else:
            if args.services:
                self.list_services()
            if args.plugins:
                self.list_plugins()
            if args.examples:
                self.show_examples()
            if args.configs:
                self.show_configuration_examples()
            if args.flow:
                self.show_flow_chart()
            if args.architecture:
                self.show_detailed_architecture_diagram()
            if args.formats:
                self.show_format_versions()
            if args.structure:
                self.show_directory_structure()

    def run_interactive(self):
        """Run the help script in interactive mode."""
        while True:
            self.print_header("FRAMEWORK HELP - INTERACTIVE MODE")
            print("Select a help topic:")
            print("  1. List Available Services")
            print("  2. List Available Plugins")
            print("  3. Show Framework Flow Chart")
            print("  4. Show Detailed Architecture Diagram")
            print("  5. Show Usage Examples")
            print("  6. Show Configuration Examples")
            print("  7. Show Format Versions Information")
            print("  8. Show Directory Structure")
            print("  9. Show All Help Information")
            print("  0. Exit")
            
            choice = input("\nEnter your choice (0-9): ").strip()
            
            action = None
            if choice == "0":
                print("Exiting interactive help.")
                break
            elif choice == "1":
                action = self.list_services
            elif choice == "2":
                action = self.list_plugins
            elif choice == "3":
                action = self.show_flow_chart
            elif choice == "4":
                action = self.show_detailed_architecture_diagram
            elif choice == "5":
                action = self.show_examples
            elif choice == "6":
                action = self.show_configuration_examples
            elif choice == "7":
                action = self.show_format_versions
            elif choice == "8":
                action = self.show_directory_structure
            elif choice == "9":
                self.welcome()
                self.list_services()
                self.list_plugins()
                self.show_flow_chart()
                self.show_detailed_architecture_diagram()
                self.show_format_versions()
                self.show_examples()
                self.show_configuration_examples()
                self.show_directory_structure()
                action = None  # Already printed
            else:
                print("Invalid choice. Please try again.")
                action = None

            if action:
                action()
                try:
                    input("\nPress Enter to continue...")
                except EOFError:
                    print("\nExiting interactive help.")  # Handle EOF if input is piped
                    break
            elif choice != "0":  # Don't pause if invalid choice
                try:
                    if choice == "9":  # If 'All' was chosen, pause after showing everything
                        input("\nPress Enter to continue...")
                except EOFError:
                    print("\nExiting interactive help.")
                    break

    def welcome(self):
        """Show welcome message."""
        self.print_header("Welcome to AutomationFramework_V1")
        
        welcome_text = """
AutomationFramework_V1 is a PySpark-based data validation framework with a plugin system
for data transformations and file conversions. This help script provides information
about the framework's components and how to use them.

Use one of the following commands (run from the framework root directory):

  python3 helpers/framework_help.py --services     # List available services
  python3 helpers/framework_help.py --plugins      # List available plugins
  python3 helpers/framework_help.py --examples     # Show usage examples
  python3 helpers/framework_help.py --configs      # Show configuration examples
  python3 helpers/framework_help.py --flow         # Show framework flow chart
  python3 helpers/framework_help.py --architecture # Show detailed architecture diagram
  python3 helpers/framework_help.py --formats      # Show format versions information
  python3 helpers/framework_help.py --structure    # Show directory structure
  python3 helpers/framework_help.py --all          # Show all help information
  python3 helpers/framework_help.py --interactive  # Run in interactive mode

The framework now includes:
- A modular, versioned converter system for different data formats
- Helper utilities (now in the 'helpers' directory)
- Enhanced support for EBCDIC and Mainframe files using Cobrix
"""
        print(welcome_text)

if __name__ == "__main__":
    helper = FrameworkHelp()
    helper.show_help() 