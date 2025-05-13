# File Converter Plugins

This document explains how to use the file converter plugins in the PySpark-based Data Validation Framework.

## Folder Structure

The file converter plugins are organized as follows:

```
AutomationFramework_V1/
├── plugins/
│   ├── file_converters/           # Directory for all file converter plugins
│   │   ├── __init__.py
│   │   ├── csv_to_psv_converter.py
│   │   ├── json_to_csv_converter.py
│   │   ├── xml_to_csv_converter.py
│   │   ├── mainframe_to_csv_converter.py
│   │   └── episodic_to_csv_converter.py
│   └── ... (other plugins)
├── config/
│   └── file_converters/           # Directory for file converter configurations
│       ├── csv_to_psv_config.yaml
│       ├── json_to_csv_config.yaml
│       ├── xml_to_csv_config.yaml
│       ├── ebcdic_to_csv_config.yaml
│       └── ... (other configurations)
└── run_file_converter.py          # Script to run file converters
```

## Overview

The file converter plugins allow you to convert data files between different formats. These plugins run independently of the main validation workflow and can be used as standalone tools.

Supported conversions:

- CSV to PSV
- JSON to CSV/PSV
- XML to CSV/PSV
- Mainframe files (fixed-width) to CSV/PSV
- EBCDIC files (with copybooks) to CSV/PSV
- Episodic data to CSV/PSV

## Plugin Structure

Each file converter plugin follows the standard plugin architecture, implementing a `run()` function that accepts a context dictionary and returns a result dictionary. The plugins handle file loading, format conversion, and writing output files.

## Running File Converters

File converters can be run using the `run_file_converter.py` script, which is separate from the main validation workflow.

### Command-Line Usage

```bash
python run_file_converter.py --config <config_name> [--input <input_path>] [--output <output_path>] [--packages <spark_packages>]
```

Options:
- `--config, -c`: Name of the converter configuration file (required)
- `--input, -i`: Override the input path specified in the config
- `--output, -o`: Override the output path specified in the config
- `--spark-name`: Custom name for the Spark application (default: "File Converter")
- `--packages`: Additional Spark packages to include, comma-separated

Example:
```bash
python run_file_converter.py --config csv_to_psv_config --input data/my_file.csv --output data/output.psv
```

## Examples

### CSV to PSV Conversion

```bash
python run_file_converter.py --config csv_to_psv_config
```

Configuration File (`config/file_converters/csv_to_psv_config.yaml`):
```yaml
plugins:
  - name: csv_to_psv_converter.py
    function: run
    params:
      input_path: "/path/to/input/file.csv"
      output_path: "/path/to/output/file.psv"
      header: true
      delimiter: ","
```

### EBCDIC to CSV with Copybook

```bash
python run_file_converter.py --config ebcdic_to_csv_config
```

Configuration File (`config/file_converters/ebcdic_to_csv_config.yaml`):
```yaml
plugins:
  - name: mainframe_to_csv_converter.py
    function: run
    params:
      input_path: "/path/to/input/ebcdic_data.dat"
      output_path: "/path/to/output/data.csv"
      file_type: "ebcdic"
      copybook_path: "/path/to/copybook.cpy"
      codepage: 1047
```

Note: The EBCDIC converter requires the Cobricks library.

### Mainframe Fixed-width File Conversion

For mainframe fixed-width files, you need to define the field structure in the configuration:

```yaml
plugins:
  - name: mainframe_to_csv_converter.py
    function: run
    params:
      input_path: "/path/to/input/mainframe_data.txt"
      output_path: "/path/to/output/data.csv"
      file_type: "fixed-width"
      field_definitions:
        - name: "EMPLOYEE_ID"
          start: 0
          length: 6
          type: "string"
        - name: "FIRST_NAME"
          start: 6
          length: 15
          type: "string"
        # ... more fields
```

## File Formats

### CSV/PSV Format
- CSV (Comma-Separated Values): Fields are separated by commas
- PSV (Pipe-Separated Values): Fields are separated by pipe characters (`|`)

### JSON Format
The JSON converter supports:
- Single-line JSON objects (one per line)
- Multi-line JSON (set `multiline: true`)
- Nested JSON structures (set `flatten: true` to attempt flattening)

### XML Format
The XML converter requires:
- Row tag parameter to identify records
- Databricks `spark-xml` package

### Mainframe Formats
- Fixed-width: Character positions define fields
- EBCDIC: Binary encoded mainframe format that requires a copybook for parsing

### EBCDIC with Copybooks
EBCDIC files use COBOL copybooks to define their structure:
- Copybook: A metadata file that defines the layout of EBCDIC data
- Cobricks library: Used to process EBCDIC files using their copybook definitions
- Codepage: Specifies the EBCDIC character encoding (default: 1047)

### Episodic Format
Supports hierarchical data in various source formats:
- JSON
- XML
- Custom formats with defined parsers

## Creating Custom Converters

To create a custom converter, follow these steps:

1. Create a new plugin file in the `/plugins/file_converters/` directory
2. Implement the `run()` function to handle the conversion
3. Create a configuration YAML file in the `/config/file_converters/` directory
4. Run with the `run_file_converter.py` script

See the existing converter plugins for implementation examples.

## Requirements

- PySpark environment
- Additional dependencies based on converter type:
  - XML: `spark-xml` package
  - EBCDIC: `cobricks` library
  - Custom formats may require additional packages

## Troubleshooting

If you encounter errors:

1. Check your configuration file for correct paths and parameters
2. Ensure required packages are installed
3. For EBCDIC files:
   - Verify the copybook path is correct and the file exists
   - Make sure the Cobricks library is installed
   - Check that the correct codepage is specified
4. Check file permissions for input/output paths
5. For large files, ensure Spark has enough memory
6. Check the error message for specific issues with the file format 