#!/usr/bin/env python3
"""
File Comparator Setup and Launcher

This script creates a fresh virtual environment, installs all dependencies,
and launches the file comparator.
"""

import os
import sys
import subprocess
import tempfile
import shutil
import platform

def run_command(command, cwd=None):
    """Run a shell command and print output"""
    try:
        print(f"Running: {' '.join(command)}")
        subprocess.run(command, check=True, cwd=cwd)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e}")
        return False

def main():
    # Determine the operating system
    is_windows = platform.system() == "Windows"
    
    # Create a temporary directory for the virtual environment
    temp_dir = tempfile.mkdtemp(prefix="file_comparator_")
    print(f"Creating temporary environment in: {temp_dir}")
    
    try:
        # Path to Python executable
        python_exe = sys.executable
        
        # Path to the plugin directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        plugin_dir = os.path.join(script_dir, "plugin", "file_comparator")
        
        # Create virtual environment
        venv_dir = os.path.join(temp_dir, "venv")
        if not run_command([python_exe, "-m", "venv", venv_dir]):
            print("Failed to create virtual environment")
            return 1
        
        # Determine path to pip and python in the virtual environment
        if is_windows:
            venv_python = os.path.join(venv_dir, "Scripts", "python.exe")
            venv_pip = os.path.join(venv_dir, "Scripts", "pip.exe")
        else:
            venv_python = os.path.join(venv_dir, "bin", "python")
            venv_pip = os.path.join(venv_dir, "bin", "pip")
        
        # Upgrade pip in the virtual environment
        if not run_command([venv_pip, "install", "--upgrade", "pip"]):
            print("Failed to upgrade pip")
            return 1
        
        # Install dependencies
        if not run_command([venv_pip, "install", "pandas", "fastapi", "uvicorn", "openpyxl"]):
            print("Failed to install dependencies")
            return 1
        
        # Run the file comparator
        standalone_script = os.path.join(plugin_dir, "run_comparator.py")
        if not run_command([venv_python, standalone_script]):
            print("Failed to run file comparator")
            return 1
        
        print("\nFile comparator is running. Press Ctrl+C to stop.")
        # Keep the script running until user interrupts
        try:
            while True:
                pass
        except KeyboardInterrupt:
            print("\nShutting down...")
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        # Clean up the temporary directory when done
        # We keep it for now for debugging purposes
        # shutil.rmtree(temp_dir)
        pass

if __name__ == "__main__":
    sys.exit(main()) 