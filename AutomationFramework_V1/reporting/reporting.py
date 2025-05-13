from typing import Dict, List
from datetime import datetime
import os
import sys

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from reporting.html_reporting import generate_html_report as generate_html

def generate_comparison_report(results: Dict) -> None:
    """
    Print a formatted comparison report from the comparison results.
    
    Args:
        results (Dict): Results from perform_comparison function
    """
    print("\nComparison Report")
    print("=" * 50)
    print(f"Validations Performed: {', '.join(results['validations_performed'])}")
    print("=" * 50)
    
    # Print source and target record counts
    if 'count_validation' in results and results['count_validation']:
        source_count = results['count_validation'].get('source_count', 0)
        target_count = results['count_validation'].get('target_count', 0)
        print(f"Source Record Count: {source_count}")
        print(f"Target Record Count: {target_count}")

    # Print each validation result if it was performed
    if "Schema Validation" in results["validations_performed"]:
        print("\nSchema Validation:")
        schema_validation_full = results.get("schema_validation_full", {})
        print(f"Schema Match: {'Yes' if schema_validation_full.get('schema_match') else 'No'}")
        
        if not schema_validation_full.get('schema_match'):
            # Get all columns from schema info
            all_columns = results["schema_info"]["all_columns"]
            
            # Create schema comparison table
            headers = ["Source Column", "Target Column", "Source Data Type", "Target Data Type"]
            rows = []
            
            for col_name in all_columns:
                # Get source type
                if col_name not in results["schema_info"]["source_columns"]:
                    source_type = "MISSING"
                else:
                    type_mismatch = next((m for m in schema_validation_full["type_mismatches"] if m["column"] == col_name), None)
                    source_type = type_mismatch["source_type"] if type_mismatch else schema_validation_full["source_schema"][col_name]
                
                # Get target type
                if col_name not in results["schema_info"]["target_columns"]:
                    target_type = "MISSING"
                else:
                    type_mismatch = next((m for m in schema_validation_full["type_mismatches"] if m["column"] == col_name), None)
                    target_type = type_mismatch["target_type"] if type_mismatch else schema_validation_full["target_schema"][col_name]
                
                source_col = col_name if col_name in results["schema_info"]["source_columns"] else "MISSING"
                target_col = col_name if col_name in results["schema_info"]["target_columns"] else "MISSING"
                
                rows.append({
                    "source_column": source_col,
                    "target_column": target_col,
                    "source_data_type": source_type,
                    "target_data_type": target_type
                })
            
            # Calculate column widths
            col_widths = [
                max(len(h), max(len(str(r[h.lower().replace(' ', '_')])) for r in rows)) 
                for h in headers
            ]
            col_widths = [w + 2 for w in col_widths]
            
            # Print schema comparison table
            print("\nSchema Comparison:")
            print("+" + "+".join("-" * w for w in col_widths) + "+")
            print("|" + "|".join(h.center(w) for h, w in zip(headers, col_widths)) + "|")
            print("+" + "+".join("-" * w for w in col_widths) + "+")
            
            for row in rows:
                print("|" + "|".join(str(row[h.lower().replace(' ', '_')]).center(w) for h, w in zip(headers, col_widths)) + "|")
            
            print("+" + "+".join("-" * w for w in col_widths) + "+")
    
    # Print final summary
    print("\nFinal Summary:")
    print("=" * 80)
    
    # Print summary table
    if results["final_summary"]:
        headers = ["Validation Type", "Status", "Details"]
        col_widths = [
            max(len(h), max(len(str(r[h])) for r in results["final_summary"]))
            for h in headers
        ]
        # Make columns wider for better readability
        col_widths[0] = max(col_widths[0], 25)  # Validation Type column
        col_widths[1] = max(col_widths[1], 10)  # Status column
        col_widths[2] = max(col_widths[2], 40)  # Details column
        col_widths = [w + 4 for w in col_widths]  # More padding
        
        table_width = sum(col_widths) + len(col_widths) + 1
        print("\n" + "=" * table_width)
        print(" " * ((table_width - 14) // 2) + "FINAL SUMMARY" + " " * ((table_width - 14) // 2))
        print("=" * table_width + "\n")
        
        print("+" + "+".join("-" * w for w in col_widths) + "+")
        print("|" + "|".join(h.center(w) for h, w in zip(headers, col_widths)) + "|")
        print("+" + "+".join("-" * w for w in col_widths) + "+")
        for row in results["final_summary"]:
            # Highlight the Overall Status row
            if row["Validation Type"] == "Overall Status":
                print("+" + "+".join("=" * w for w in col_widths) + "+")
            
            # Create formatted status with visual indicators
            status = row["Status"]
            if status == "PASS":
                status_formatted = "✓ " + status
            elif status == "FAIL":
                status_formatted = "✗ " + status
            else:
                status_formatted = status
                
            cells = [
                str(row["Validation Type"]).center(col_widths[0]),
                status_formatted.center(col_widths[1]),
                str(row["Details"]).center(col_widths[2])
            ]
            
            # Print the row
            print("|" + "|".join(cells) + "|")
            
            # Add another separator after the Overall Status
            if row["Validation Type"] == "Overall Status":
                print("+" + "+".join("=" * w for w in col_widths) + "+")
            
        print("+" + "+".join("-" * w for w in col_widths) + "+")
        print("\n" + "=" * table_width)


def generate_html_report(results: Dict, output_path: str) -> None:
    """
    Generate a styled HTML report from comparison results.
    
    Args:
        results (Dict): Results from perform_comparison function
        output_path (str): Path to save the HTML report
    """
    # Create reports directory if it doesn't exist
    ensure_reports_directory(os.path.dirname(output_path))
    
    # Generate HTML report
    generate_html(results, output_path)


# Create the reports directory if it doesn't exist
def ensure_reports_directory(base_path: str = "reports") -> str:
    """Create the reports directory if it doesn't exist."""
    if not os.path.exists(base_path):
        os.makedirs(base_path)
    return base_path 