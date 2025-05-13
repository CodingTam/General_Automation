#!/usr/bin/env python3
"""
Example script demonstrating how to use the file comparator plugin.
"""

import os
import sys
import pandas as pd

# Direct import without relying on package structure
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
from plugin import launch_comparator_plugin

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
        launch_comparator_plugin(source_path, target_path)
        
        print("\nComparator launched in your browser.")
        print("Steps to use:")
        print("1. Select 'ID' as the key column")
        print("2. Click 'Compare' to see differences")
        print("3. Optionally use the decode file to decode the 'Status' column")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 