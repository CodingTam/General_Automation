# Data Validation Framework - Plugin System

This document explains how to use the plugin system in the PySpark-based Data Validation Framework.

## Overview

The plugin system allows users to add custom data transformations that run before the main comparison engine. Plugins can perform various tasks such as:

- Renaming columns to match schemas between source and target
- Filtering data to validate specific subsets
- Applying transformations to make data comparable
- Adding derived columns
- Aggregating data

## Plugin Structure

Each plugin is a standalone Python script located in the `/plugins/` directory. Every plugin must define a function named `run` with the following signature:

```python
def run(context: dict) -> dict:
    # Plugin implementation
    return result_dict
```

### Input Context

The `context` dictionary contains all information the plugin needs to perform its task. It typically includes:

- `params`: A dictionary of parameters from the YAML configuration
- `source_df`: The source PySpark DataFrame (automatically included)
- `target_df`: The target PySpark DataFrame (automatically included)

### Output Dictionary

Plugins must return a dictionary. Key fields in the output dictionary include:

- `transformed_df`: A single transformed DataFrame (applied to source or target based on configuration)
- `source_df`: The transformed source DataFrame (if you want to specify source explicitly)
- `target_df`: The transformed target DataFrame (if you want to specify target explicitly)
- `metadata`: Optional dictionary with metadata about the transformation

## Creating a Plugin

To create a plugin:

1. Create a new Python file in the `/plugins/` directory (e.g., `my_plugin.py`)
2. Define a `run()` function that accepts a context dictionary
3. Implement your transformation logic
4. Return a dictionary with the transformed DataFrame(s) and optional metadata

### Example Plugin

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def run(context: dict) -> dict:
    # Get parameters
    params = context.get('params', {})
    source_df = params.get('source_df')
    
    # Perform transformation
    transformed_df = source_df.filter(col("status") == "active")
    
    # Return result
    return {
        'transformed_df': transformed_df,
        'metadata': {
            'plugin_name': 'active_filter',
            'records_before': source_df.count(),
            'records_after': transformed_df.count()
        }
    }
```

## Configuring Plugins

Plugins are configured in a YAML file. You can specify:

- Which plugins to run and in what order
- Parameters for each plugin
- Whether to apply the plugin to source, target, or both DataFrames

### Example Configuration

```yaml
plugins:
  - name: column_renamer.py
    function: run
    params:
      transform_target: source
      columns_map:
        old_column_name: new_column_name
        
  - name: data_filter.py
    function: run
    params:
      transform_target: both
      filter_conditions:
        - column: customer_id
          operator: "!="
          value: null
```

## Running Plugins

To run plugins as part of your data validation process:

```python
from plugin_integration import run_plugins_from_yaml

# Load DataFrames
source_df = ...
target_df = ...

# Run plugins
result = run_plugins_from_yaml(source_df, target_df, "my_config.yaml")

# Get transformed DataFrames
transformed_source_df = result['source_df']
transformed_target_df = result['target_df']

# Run validation with transformed DataFrames
perform_comparison(transformed_source_df, transformed_target_df, ...)
```

## Built-in Plugins

The framework comes with several built-in plugins:

1. **column_renamer.py**: Renames columns in a DataFrame
2. **data_filter.py**: Filters data based on conditions

## Creating Custom Plugins

To create your own plugins:

1. Create a new Python file in the `/plugins/` directory
2. Define the `run()` function with the required signature
3. Access parameters from `context['params']`
4. Perform your transformations
5. Return a dictionary with the results

See the existing plugins for examples of how to structure your code.

## Best Practices

- Keep plugins focused on a single task
- Add detailed metadata to help with debugging
- Handle errors gracefully
- Document parameters in function docstrings
- Validate inputs before processing
- Use appropriate PySpark functions for scale
- Return both the transformed data and useful metadata 