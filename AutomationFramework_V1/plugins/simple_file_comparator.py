#!/usr/bin/env python3
"""
Simple File Comparator

A basic implementation of the file comparator that doesn't require the React frontend to be built.
It loads two files (CSV or Excel), compares them, and outputs the differences.
"""

import os
import sys
import pandas as pd
import csv
from tabulate import tabulate
import difflib

def compare_files(source_path, target_path, key_columns):
    """
    Compare two files and return the differences.
    
    Args:
        source_path: Path to the source file
        target_path: Path to the target file
        key_columns: List of column names to use as keys for matching rows
        
    Returns:
        A tuple of (differences_list, summary_dict)
    """
    try:
        # Load the files
        source_df = load_file(source_path)
        target_df = load_file(target_path)
        
        # Validate key columns
        for key_col in key_columns:
            if key_col not in source_df.columns:
                raise ValueError(f"Key column '{key_col}' not found in source file")
            if key_col not in target_df.columns:
                raise ValueError(f"Key column '{key_col}' not found in target file")
        
        # Get common columns for comparison
        common_columns = [col for col in source_df.columns if col in target_df.columns]
        comparison_columns = [col for col in common_columns if col not in key_columns]
        
        # Prepare results container
        differences = []
        
        # Find rows in source that are missing in target or have different values
        for _, source_row in source_df.iterrows():
            # Create filter for key columns
            mask = pd.Series(True, index=target_df.index)
            for key_col in key_columns:
                mask = mask & (target_df[key_col] == source_row[key_col])
            
            # Get matching rows in target
            matching_rows = target_df[mask]
            
            if len(matching_rows) == 0:
                # Row exists in source but not in target
                key_values = {key: source_row[key] for key in key_columns}
                differences.append({
                    "key_values": key_values,
                    "status": "missing_in_target",
                    "differences": []
                })
            else:
                # Row exists in both, compare values
                target_row = matching_rows.iloc[0]
                diffs = []
                
                for col in comparison_columns:
                    source_val = source_row[col]
                    target_val = target_row[col]
                    
                    # Compare values
                    if pd.isna(source_val) and pd.isna(target_val):
                        continue
                    elif pd.isna(source_val):
                        source_val = ""
                    elif pd.isna(target_val):
                        target_val = ""
                    
                    if str(source_val) != str(target_val):
                        diffs.append({
                            "column": col,
                            "source_value": str(source_val),
                            "target_value": str(target_val)
                        })
                
                if diffs:
                    key_values = {key: source_row[key] for key in key_columns}
                    differences.append({
                        "key_values": key_values,
                        "status": "values_differ",
                        "differences": diffs
                    })
        
        # Find rows in target that don't exist in source
        for _, target_row in target_df.iterrows():
            # Create filter for key columns
            mask = pd.Series(True, index=source_df.index)
            for key_col in key_columns:
                mask = mask & (source_df[key_col] == target_row[key_col])
            
            # Get matching rows in source
            matching_rows = source_df[mask]
            
            if len(matching_rows) == 0:
                # Row exists in target but not in source
                key_values = {key: target_row[key] for key in key_columns}
                differences.append({
                    "key_values": key_values,
                    "status": "missing_in_source",
                    "differences": []
                })
        
        # Create summary
        summary = {
            "total_differences": len(differences),
            "missing_in_target": sum(1 for d in differences if d["status"] == "missing_in_target"),
            "missing_in_source": sum(1 for d in differences if d["status"] == "missing_in_source"),
            "values_differ": sum(1 for d in differences if d["status"] == "values_differ")
        }
        
        return differences, summary
        
    except Exception as e:
        print(f"Error comparing files: {e}")
        raise

def load_file(file_path):
    """Load a CSV or Excel file into a pandas DataFrame."""
    file_ext = os.path.splitext(file_path)[1].lower()
    
    if file_ext in ('.csv', '.txt'):
        return pd.read_csv(file_path)
    elif file_ext in ('.xlsx', '.xls'):
        return pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_ext}. Only CSV and Excel files are supported.")

def print_file_info(df, title):
    """Print information about a DataFrame."""
    print(f"\n{title} ({len(df)} rows, {len(df.columns)} columns)")
    print("-" * 80)
    print(f"Columns: {', '.join(df.columns)}")
    print(f"First 5 rows:")
    print(tabulate(df.head(), headers="keys", tablefmt="grid"))

def format_key_value(key_values):
    """Format key values as a string."""
    return ", ".join(f"{k}: {v}" for k, v in key_values.items())

def print_differences(differences, summary):
    """Print the differences in a readable format."""
    print("\n" + "=" * 80)
    print("COMPARISON RESULTS".center(80))
    print("=" * 80)
    
    print(f"\nTotal differences: {summary['total_differences']}")
    print(f"Missing in source: {summary['missing_in_source']}")
    print(f"Missing in target: {summary['missing_in_target']}")
    print(f"Rows with different values: {summary['values_differ']}")
    
    if not differences:
        print("\nNo differences found. The files are identical.")
        return
    
    # Print rows missing in target
    if summary['missing_in_target'] > 0:
        print("\n" + "-" * 80)
        print("ROWS MISSING IN TARGET".center(80))
        print("-" * 80)
        
        for diff in [d for d in differences if d["status"] == "missing_in_target"]:
            print(f"\nKey: {format_key_value(diff['key_values'])}")
    
    # Print rows missing in source
    if summary['missing_in_source'] > 0:
        print("\n" + "-" * 80)
        print("ROWS MISSING IN SOURCE".center(80))
        print("-" * 80)
        
        for diff in [d for d in differences if d["status"] == "missing_in_source"]:
            print(f"\nKey: {format_key_value(diff['key_values'])}")
    
    # Print rows with different values
    if summary['values_differ'] > 0:
        print("\n" + "-" * 80)
        print("ROWS WITH DIFFERENT VALUES".center(80))
        print("-" * 80)
        
        for diff in [d for d in differences if d["status"] == "values_differ"]:
            print(f"\nKey: {format_key_value(diff['key_values'])}")
            
            # Prepare table data
            table_data = []
            for cell_diff in diff["differences"]:
                table_data.append([
                    cell_diff["column"],
                    cell_diff["source_value"],
                    cell_diff["target_value"]
                ])
            
            # Print table
            print(tabulate(
                table_data,
                headers=["Column", "Source Value", "Target Value"],
                tablefmt="grid"
            ))

def create_sample_files():
    """Create sample CSV files for demonstration."""
    # Create a directory for sample files
    sample_dir = "sample_data"
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
    
    # Create the CSV files
    source_path = os.path.join(sample_dir, 'source.csv')
    target_path = os.path.join(sample_dir, 'target.csv')
    
    pd.DataFrame(source_data).to_csv(source_path, index=False)
    pd.DataFrame(target_data).to_csv(target_path, index=False)
    
    print(f"Sample files created in {sample_dir}")
    
    return source_path, target_path

def main():
    """Main function that runs the file comparison."""
    try:
        # Check if tabulate is installed
        try:
            import tabulate
        except ImportError:
            print("The 'tabulate' package is required. Please install it with:")
            print("pip install tabulate")
            return 1
        
        # Parse arguments
        if len(sys.argv) > 3:
            source_path = sys.argv[1]
            target_path = sys.argv[2]
            key_columns = sys.argv[3].split(',')
        else:
            # Create sample files
            print("No input files specified, creating sample files...")
            source_path, target_path = create_sample_files()
            key_columns = ['ID']
            print(f"Using key column: {key_columns[0]}")
        
        # Load the files
        source_df = load_file(source_path)
        target_df = load_file(target_path)
        
        # Print file info
        print_file_info(source_df, "Source File")
        print_file_info(target_df, "Target File")
        
        # Compare the files
        differences, summary = compare_files(source_path, target_path, key_columns)
        
        # Print the differences
        print_differences(differences, summary)
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 