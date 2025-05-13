#!/usr/bin/env python3
"""
File Comparator Plugin

A plugin that allows visual comparison of CSV and Excel files side-by-side.
"""

import os
import time
import socket
import webbrowser
import subprocess
import threading
from urllib.parse import quote


def is_port_in_use(port):
    """Check if the given port is already in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def start_server(port=8081):
    """Start the FastAPI server if not already running."""
    if is_port_in_use(port):
        print(f"Server already running on port {port}")
        return

    # Get the directory of this file
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


def launch_comparator_plugin(source_file, target_file, port=8081):
    """
    Launch the file comparator plugin with the given source and target files.
    
    Args:
        source_file (str): Path to the source file (CSV or Excel)
        target_file (str): Path to the target file (CSV or Excel)
        port (int, optional): Port to run the server on. Defaults to 8081.
    """
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