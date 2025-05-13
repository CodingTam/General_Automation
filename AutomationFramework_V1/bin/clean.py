#!/usr/bin/env python3
"""
Clean script to remove generated files and temporary artifacts.
"""

import os
import shutil
import glob

def clean_project():
    """Remove generated files and directories."""
    print("Cleaning project...")
    
    # Files and directories to remove
    paths_to_remove = [
        # Python bytecode
        "**/__pycache__",
        "**/*.pyc",
        "**/*.pyo",
        
        # Logs
        "logs",
        "*.log",
        
        # Reports
        "reports",
        
        # Generated data
        "tests/test_data",
        
        # Database files
        "*.db",
        "*.sqlite",
        
        # Temp files
        "*.bak",
        "*.tmp",
        "**/*~"
    ]
    
    for pattern in paths_to_remove:
        for path in glob.glob(pattern, recursive=True):
            try:
                if os.path.isdir(path):
                    print(f"Removing directory: {path}")
                    shutil.rmtree(path)
                else:
                    print(f"Removing file: {path}")
                    os.remove(path)
            except Exception as e:
                print(f"Error removing {path}: {e}")
    
    print("Project cleaned successfully")

if __name__ == "__main__":
    clean_project() 