# Automation Framework Documentation

This directory contains all documentation for the Automation Framework.

## README Files

- [Main README](READMEs/README.md) - Overview of the automation framework
- [Commands Reference](READMEs/Commands_Readme.md) - Complete reference for all scheduler commands
- [Plugins Documentation](READMEs/README_PLUGINS.md) - Information about the plugin system
- [File Converters Documentation](READMEs/README_FILE_CONVERTERS.md) - Information about file converter plugins

## Helper Tools

- **Command Helper** (`command_helper.py`) - Interactive command line tool for executing framework commands:
  ```bash
  # List all command categories
  python command_helper.py --list
  
  # Show commands in a specific category
  python command_helper.py --category scheduler
  
  # Start interactive mode
  python command_helper.py --interactive
  ``` 