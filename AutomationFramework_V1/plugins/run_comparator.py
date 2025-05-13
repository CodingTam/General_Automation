#!/usr/bin/env python3
"""
Standalone File Comparator Launcher

This script creates sample files and launches the file comparator without relying on imports.
"""

import os
import sys
import time
import socket
import webbrowser
import subprocess
import threading
from urllib.parse import quote

# Check if required packages are installed
try:
    import pandas as pd
    import fastapi
    import uvicorn
except ImportError:
    print("Required packages are not installed. Please run:")
    print("pip install pandas fastapi uvicorn openpyxl")
    sys.exit(1)

# =================================================================
# Utility functions for launching the server
# =================================================================

def is_port_in_use(port):
    """Check if the given port is already in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def start_server(port=8081):
    """Start the FastAPI server if not already running."""
    if is_port_in_use(port):
        print(f"Server already running on port {port}")
        return

    # Get the directory of the plugin
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Start the server in a subprocess
    process = subprocess.Popen(
        ["uvicorn", "backend.main:app", "--port", str(port)],
        cwd=base_dir
    )
    
    # Wait for server to start
    max_tries = 10
    tries = 0
    while tries < max_tries:
        if is_port_in_use(port):
            break
        time.sleep(0.5)
        tries += 1
    
    if tries == max_tries:
        raise RuntimeError("Failed to start the server")

def launch_comparator(source_file, target_file, port=8081):
    """Launch the file comparator with source and target files."""
    # Convert file paths to absolute paths
    source_file = os.path.abspath(source_file)
    target_file = os.path.abspath(target_file)
    
    # Start the server in a separate thread
    server_thread = threading.Thread(target=start_server, args=(port,))
    server_thread.daemon = True
    server_thread.start()
    
    # Wait briefly to ensure the server has time to start
    time.sleep(1)
    
    # Open comparison UI in browser
    url = f"http://localhost:{port}/compare_ui?source={quote(source_file)}&target={quote(target_file)}"
    webbrowser.open(url)
    
    print(f"File comparison started at {url}")
    print("Close the browser when done.")
    
    return url

# =================================================================
# Create sample files and launch the comparator
# =================================================================

def create_sample_files():
    """Create sample CSV files for demonstration."""
    # Create a directory for sample files
    sample_dir = os.path.join(os.path.dirname(__file__), 'sample_data')
    os.makedirs(sample_dir, exist_ok=True)
    
    # Sample source data
    source_data = {
        'ID': [1, 2, 3, 4, 5],
        'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eva'],
        'Department': ['HR', 'IT', 'Finance', 'Marketing', 'Operations'],
        'Salary': [75000, 85000, 92000, 67000, 78000],
        'Status': ['A', 'A', 'I', 'A', 'P']
    }
    
    # Sample target data with some differences
    target_data = {
        'ID': [1, 2, 3, 6, 7],  # 4,5 missing, 6,7 added
        'Name': ['Alice', 'Bobby', 'Charlie', 'Frank', 'Grace'],  # Bobby changed
        'Department': ['HR', 'IT', 'Finance', 'Sales', 'Legal'],
        'Salary': [75000, 90000, 92000, 72000, 95000],  # Salary for Bob changed
        'Status': ['A', 'A', 'A', 'P', 'A']  # Status for Charlie changed
    }
    
    # Sample decode data
    decode_data = {
        'Code': ['A', 'I', 'P'],
        'Description': ['Active', 'Inactive', 'Pending']
    }
    
    # Create the CSV files
    source_path = os.path.join(sample_dir, 'source.csv')
    target_path = os.path.join(sample_dir, 'target.csv')
    decode_path = os.path.join(sample_dir, 'status_codes.csv')
    
    pd.DataFrame(source_data).to_csv(source_path, index=False)
    pd.DataFrame(target_data).to_csv(target_path, index=False)
    pd.DataFrame(decode_data).to_csv(decode_path, index=False)
    
    print(f"Sample files created in {sample_dir}")
    
    return source_path, target_path, decode_path

def main():
    """Main function that creates sample files and launches the comparator."""
    try:
        # First, create sample files
        source_path, target_path, decode_path = create_sample_files()
        
        print("\nLaunching file comparator with sample files...")
        print(f"Source: {source_path}")
        print(f"Target: {target_path}")
        print(f"Decode file (for reference): {decode_path}")
        
        # Launch the comparator with the sample files
        launch_comparator(source_path, target_path)
        
        print("\nComparator launched in your browser.")
        print("Steps to use:")
        print("1. Select 'ID' as the key column")
        print("2. Click 'Compare' to see differences")
        print("3. Optionally use the decode file to decode the 'Status' column")
        
        # Keep the script running so the server stays alive
        print("\nPress Ctrl+C to exit when done")
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nExiting file comparator")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 