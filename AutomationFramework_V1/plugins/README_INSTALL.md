# File Comparator Plugin Installation

## Quick Start

1. **Install the dependencies**
   ```bash
   pip install fastapi uvicorn pandas openpyxl
   ```

2. **Using the plugin in your code**
   
   You can import the plugin directly by adding the plugin directory to your Python path:

   ```python
   import sys
   import os

   # Add the plugin directory to the Python path
   plugin_dir = '/path/to/plugin/file_comparator'
   if plugin_dir not in sys.path:
       sys.path.insert(0, plugin_dir)

   # Import the plugin function
   from plugin import launch_comparator_plugin

   # Use the plugin
   launch_comparator_plugin(
       source_file="/path/to/source_file.csv",
       target_file="/path/to/target_file.csv"
   )
   ```

3. **Alternative: Install as a package**

   You can also install the plugin as a package for easier importing:

   ```bash
   cd /path/to/plugin/file_comparator
   pip install -e .
   ```

   Then import it in your code:

   ```python
   from file_comparator import launch_comparator_plugin

   launch_comparator_plugin(
       source_file="/path/to/source_file.csv",
       target_file="/path/to/target_file.csv"
   )
   ```

## Troubleshooting

- If you encounter import errors, make sure the plugin directory is in your Python path
- For "module not found" errors, check that all dependencies are installed
- If the server fails to start, ensure port 8081 is available or specify a different port

## Using with the Example Script

The plugin comes with an example script that demonstrates its usage:

```bash
python plugin/file_comparator/example.py
```

This will:
1. Create sample CSV files
2. Launch the file comparator with those files
3. Open your browser to the comparison UI

Follow the instructions in the terminal to:
1. Select 'ID' as the key column
2. Click 'Compare' to see differences
3. Optionally use the decode file to decode the 'Status' column 