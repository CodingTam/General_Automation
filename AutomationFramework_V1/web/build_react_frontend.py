#!/usr/bin/env python3
"""
Build React Frontend

This script installs Node.js dependencies and builds the React frontend for the file comparator.
"""

import os
import sys
import subprocess
import platform

def run_command(command, cwd=None):
    """Run a shell command and print output"""
    try:
        print(f"Running: {' '.join(command)}")
        process = subprocess.run(command, check=True, cwd=cwd, text=True, capture_output=True)
        print(process.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e}")
        print(f"Command output: {e.output}")
        print(f"Command stderr: {e.stderr}")
        return False

def check_node_installed():
    """Check if Node.js is installed"""
    try:
        subprocess.run(["node", "--version"], check=True, capture_output=True)
        subprocess.run(["npm", "--version"], check=True, capture_output=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Node.js and npm are required to build the frontend.")
        print("Please install Node.js from https://nodejs.org/")
        return False

def main():
    # Check if Node.js is installed
    if not check_node_installed():
        return 1
    
    # Path to the frontend directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    frontend_dir = os.path.join(script_dir, "plugin", "file_comparator", "frontend")
    
    # Check if the frontend directory exists
    if not os.path.exists(frontend_dir):
        print(f"Error: Frontend directory not found at {frontend_dir}")
        return 1
    
    # Install dependencies
    print("\nInstalling Node.js dependencies...")
    if not run_command(["npm", "install"], cwd=frontend_dir):
        print("Failed to install dependencies")
        return 1
    
    # Build the frontend
    print("\nBuilding React frontend...")
    if not run_command(["npm", "run", "build"], cwd=frontend_dir):
        print("Failed to build frontend")
        return 1
    
    print("\nFrontend built successfully!")
    print("You can now run the file comparator and it will use the built UI.")
    print("To run the file comparator: python3 plugin/file_comparator/run_comparator.py")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 