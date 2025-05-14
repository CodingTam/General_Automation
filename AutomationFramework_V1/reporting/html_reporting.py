from typing import Dict, List
from datetime import datetime
import yaml
import os
import json
import csv
import base64


def generate_html_report(results: Dict, output_path: str) -> None:
    """
    Generate a styled HTML report from comparison results.
    
    Args:
        results (Dict): Results from perform_comparison function
        output_path (str): Path to save the HTML report
    """
    # Get table name and test case info
    table_name = results.get("table_name", "Unknown")
    test_case_name = results.get("test_case_name", os.path.basename(results.get("yaml_file", "Unknown")))
    
    # Calculate execution duration if available
    start_time = results.get("start_time")
    end_time = results.get("end_time")
    execution_duration = None
    if start_time and end_time:
        start = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        execution_duration = str(end - start)
    
    # Start building HTML content
    html_content = f'''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>ONETEST - Data Comparison Report - {table_name}</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
        <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                line-height: 1.6;
                color: #333;
                background-color: #f8f9fa;
                padding: 20px;
            }}
            .container {{
                max-width: 1400px;
                background-color: white;
                padding: 30px;
                border-radius: 8px;
                box-shadow: 0 0 20px rgba(0,0,0,0.1);
                margin-bottom: 30px;
            }}
            h1, h2, h3, h4 {{
                color: #264653;
                margin-bottom: 10px;
            }}
            h1 {{
                border-bottom: 2px solid #2a9d8f;
                padding-bottom: 6px;
            }}
            h2 {{
                border-bottom: 1px solid #e9c46a;
                padding-bottom: 5px;
                margin-top: 15px;
            }}
            .section {{
                margin-bottom: 15px;
                background-color: #ffffff;
                border-radius: 6px;
                padding: 12px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            }}
            .summary-section {{
                background-color: #f8f9fc;
                border-left: 4px solid #2a9d8f;
            }}
            table {{
                width: 100%;
                margin-bottom: 15px;
                border-collapse: collapse;
            }}
            th, td {{
                padding: 8px 12px;
                text-align: left;
                border: 1px solid #ddd;
            }}
            th {{
                background-color: #264653;
                color: white;
                font-weight: 600;
            }}
            tr:nth-child(even) {{
                background-color: #f2f2f2;
            }}
            .pass {{
                background-color: #d4edda;
                color: #155724;
                font-weight: bold;
            }}
            .fail {{
                background-color: #f8d7da;
                color: #721c24;
                font-weight: bold;
            }}
            .summary-box {{
                padding: 12px;
                margin-bottom: 15px;
                border-radius: 6px;
            }}
            .pass-box {{
                background-color: #d4edda;
                border: 1px solid #c3e6cb;
            }}
            .fail-box {{
                background-color: #f8d7da;
                border: 1px solid #f5c6cb;
            }}
            .badge {{
                display: inline-block;
                padding: 4px 8px;
                border-radius: 3px;
                font-size: 12px;
                font-weight: bold;
                text-transform: uppercase;
            }}
            .badge-pass {{
                background-color: #28a745;
                color: white;
            }}
            .badge-fail {{
                background-color: #dc3545;
                color: white;
            }}
            .mismatch-sample {{
                margin-top: 12px;
                padding: 12px;
                background-color: #f8f9fa;
                border: 1px solid #e9ecef;
                border-radius: 6px;
            }}
            .timestamp {{
                color: #6c757d;
                font-size: 14px;
                margin-bottom: 15px;
            }}
            .total-count {{
                font-size: 18px;
                font-weight: bold;
                margin-bottom: 8px;
            }}
            .report-header {{
                display: flex;
                justify-content: space-between;
                align-items: flex-start;
                margin-bottom: 20px;
            }}
            .validation-summary {{
                display: flex;
                flex-wrap: wrap;
                gap: 8px;
                margin-bottom: 10px;
            }}
            .validation-item {{
                padding: 8px 12px;
                border-radius: 6px;
                flex-grow: 1;
                text-align: center;
                min-width: 180px;
            }}
            .table-responsive {{
                overflow-x: auto;
            }}
            .config-table {{
                width: 100%;
                margin-bottom: 15px;
            }}
            .config-table th {{
                width: 30%;
                text-align: left;
            }}
            .config-table td {{
                width: 70%;
            }}
            .summary-info-box {{
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                border-radius: 8px;
                padding: 12px;
                margin-bottom: 15px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            }}
            .summary-info-box h4 {{
                color: #264653;
                margin-bottom: 12px;
                border-bottom: 2px solid #2a9d8f;
                padding-bottom: 6px;
            }}
            .summary-info-item {{
                margin-bottom: 8px;
            }}
            .summary-info-label {{
                font-weight: 600;
                color: #495057;
            }}
            .summary-info-value {{
                color: #212529;
            }}
            details {{
                margin: 15px 0;
                padding: 12px;
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                border-radius: 6px;
            }}
            details summary {{
                cursor: pointer;
                font-weight: 600;
                color: #264653;
            }}
            details pre {{
                margin-top: 12px;
                padding: 12px;
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                border-radius: 4px;
                overflow-x: auto;
            }}
            .recommendations {{
                background-color: #fff3cd;
                border: 1px solid #ffeeba;
                border-radius: 6px;
                padding: 12px;
                margin: 15px 0;
            }}
            .recommendations h3 {{
                color: #856404;
                margin-bottom: 12px;
            }}
            .recommendations ul {{
                margin-bottom: 0;
            }}
            .recommendations li {{
                margin-bottom: 6px;
            }}
            .section-icon {{
                margin-right: 6px;
                color: #264653;
            }}
            .section-divider {{
                height: 1px;
                background-color: #dee2e6;
                margin: 15px 0;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="report-header">
                <div>
                    <h1>ONETEST - Data Comparison Report</h1>
                    <p class="timestamp">Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                <div class="summary-info-box" style="width: 300px;">
                    <h4>Test Summary</h4>
                    <div class="summary-info-item">
                        <span class="summary-info-label">Test Case:</span>
                        <span class="summary-info-value">{test_case_name}</span>
                    </div>
                    <div class="summary-info-item">
                        <span class="summary-info-label">Table:</span>
                        <span class="summary-info-value">{table_name}</span>
                    </div>
                    <div class="summary-info-item">
                        <span class="summary-info-label">Source Type:</span>
                        <span class="summary-value">{results.get('source_type', 'N/A')}</span>
                    </div>
                    <div class="summary-info-item">
                        <span class="summary-info-label">Target Type:</span>
                        <span class="summary-value">{results.get('target_type', 'N/A')}</span>
                    </div>
                    <div class="summary-info-item">
                        <span class="summary-info-label">SID:</span>
                        <span class="summary-value">{results.get('sid', 'N/A')}</span>
                    </div>
                    <div class="summary-info-item">
                        <span class="summary-info-label">Status:</span>
                        <span class="summary-value">{results.get('overall_status', 'N/A')}</span>
                    </div>
                </div>
            </div>
            
            <div class="section-divider"></div>
            
            <!-- Run Metadata Section -->
            <div class="section">
                <h2><i class="fas fa-flask section-icon"></i>Run Metadata</h2>
                <div class="table-responsive">
                    <table class="table table-bordered">
                        <tbody>
                            <tr>
                                <th>Framework Version</th>
                                <td>ONETEST 1.0.0</td>
                            </tr>
                            <tr>
                                <th>Execution Environment</th>
                                <td>{results.get('environment', 'N/A')}</td>
                            </tr>
                            <tr>
                                <th>Execution Timestamp</th>
                                <td>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td>
                            </tr>
                            <tr>
                                <th>Execution Duration</th>
                                <td>{execution_duration or 'N/A'}</td>
                            </tr>
                            <tr>
                                <th>Partition Date</th>
                                <td>{results.get('partition_date', 'N/A')}</td>
                            </tr>
                            <tr>
                                <th>Triggered By</th>
                                <td>{results.get('sid', 'N/A')}</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
            
            <div class="section-divider"></div>
    '''
    
    # Add test case configuration section
    html_content += _generate_config_section(results)
    
    # Add raw YAML view
    html_content += _generate_raw_yaml_section(results)
    
    # Add overview section
    html_content += _generate_overview_section(results)
    
    # Add schema validation section if performed
    if "Schema Validation" in results["validations_performed"]:
        html_content += _generate_schema_section(results)
    
    # Add data validation section if performed
    if "Data Validation" in results["validations_performed"]:
        html_content += _generate_data_section(results, table_name)
    
    # Add null checks section if performed
    if "Null Checks" in results["validations_performed"]:
        html_content += _generate_null_section(results)
    
    # Add duplicate checks section if performed
    if "Duplicate Checks" in results["validations_performed"]:
        html_content += _generate_duplicate_section(results)
    
    # Add rule validation section if performed
    if "Rule Validation" in results["validations_performed"]:
        html_content += _generate_rule_section(results)
    
    # Add recommendations section
    html_content += _generate_recommendations_section(results)
    
    # Add final assessment section
    html_content += _generate_final_section(results)
    
    # Close HTML document
    html_content += '''
        </div>
        
        <footer class="text-center py-4">
            <p class="text-muted">Generated by ONETEST Data Comparison Framework</p>
        </footer>
        
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    </body>
    </html>
    '''
    
    # Write HTML to file
    with open(output_path, 'w') as f:
        f.write(html_content)
    
    print(f"HTML report generated successfully at: {output_path}")


def _generate_config_section(results: Dict) -> str:
    """Generate the HTML test case configuration section."""
    html = '''
            <div class="section">
                <h2><i class="fas fa-cog section-icon"></i>Test Case Configuration</h2>
                <div class="table-responsive">
                    <table class="table table-bordered config-table">
                        <thead>
                            <tr>
                                <th>Field Name</th>
                                <th>Value</th>
                            </tr>
                        </thead>
                        <tbody>
    '''
    
    # Load YAML file if available
    yaml_file = results.get("yaml_file")
    if yaml_file and os.path.exists(yaml_file):
        try:
            with open(yaml_file, 'r') as f:
                yaml_data = yaml.safe_load(f)
                
                # Flatten nested YAML structure
                def flatten_dict(d, parent_key='', sep='_'):
                    items = []
                    for k, v in d.items():
                        new_key = f"{parent_key}{sep}{k}" if parent_key else k
                        if isinstance(v, dict):
                            items.extend(flatten_dict(v, new_key, sep=sep).items())
                        else:
                            items.append((new_key, v))
                    return dict(items)
                
                # Flatten and sort the YAML data
                flat_data = flatten_dict(yaml_data)
                sorted_items = sorted(flat_data.items())
                
                # Add each field to the table
                for field_name, value in sorted_items:
                    # Format the value
                    if value is None:
                        value = ""
                    elif isinstance(value, (list, dict)):
                        value = str(value)
                    
                    html += f'''
                            <tr>
                                <td>{field_name}</td>
                                <td>{value}</td>
                            </tr>
                    '''
        except Exception as e:
            html += f'''
                            <tr>
                                <td colspan="2" class="text-danger">Error loading YAML configuration: {str(e)}</td>
                            </tr>
            '''
    else:
        html += '''
                            <tr>
                                <td colspan="2" class="text-warning">No YAML configuration file available</td>
                            </tr>
        '''
    
    html += '''
                        </tbody>
                    </table>
                </div>
            </div>
    '''
    
    return html


def _generate_raw_yaml_section(results: Dict) -> str:
    """Generate the HTML raw YAML view section."""
    yaml_file = results.get("yaml_file")
    if not yaml_file or not os.path.exists(yaml_file):
        return ""
    
    try:
        with open(yaml_file, 'r') as f:
            yaml_content = f.read()
        
        return f'''
            <div class="section">
                <details>
                    <summary><i class="fas fa-code section-icon"></i>View Raw YAML</summary>
                    <pre>{yaml_content}</pre>
                </details>
            </div>
        '''
    except Exception as e:
        return f'''
            <div class="section">
                <details>
                    <summary><i class="fas fa-code section-icon"></i>View Raw YAML</summary>
                    <pre>Error loading YAML file: {str(e)}</pre>
                </details>
            </div>
        '''


def _generate_overview_section(results: Dict) -> str:
    """Generate the HTML overview section."""
    html = '''
            <div class="section summary-section">
                <h2><i class="fas fa-chart-bar section-icon"></i>Overview</h2>
                <div class="row">
                    <div class="col-md-6">
                        <div class="card mb-3">
                            <div class="card-header bg-primary text-white">Record Counts</div>
                            <div class="card-body">
    '''
    
    html += f'''
                                <div class="total-count">Source: {results.get('count_validation', {}).get('source_count', 0):,}</div>
                                <div class="total-count">Target: {results.get('count_validation', {}).get('target_count', 0):,}</div>
                                <div>Difference: {results.get('count_validation', {}).get('count_difference', 0):,}</div>
    '''
    
    html += '''
                            </div>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="card mb-3">
                            <div class="card-header bg-primary text-white">Overall Status</div>
                            <div class="card-body text-center">
    '''
    
    html += f'''
                                <h3 class="{'' if results.get('overall_status') == 'PASS' else 'text-danger'}">
                                    {results.get('overall_status', 'Unknown')}
                                </h3>
                                <p>{results.get('overall_details', '')}</p>
    '''
    
    html += '''
                            </div>
                        </div>
                    </div>
                </div>

                <h3><i class="fas fa-clipboard-check section-icon"></i>Validation Summary</h3>
                <div class="validation-summary">
    '''
    
    # Add validation summary boxes
    final_summary = results.get("final_summary", [])
    for validation in final_summary:
        validation_type = validation.get("Validation Type", "")
        status = validation.get("Status", "")
        details = validation.get("Details", "")
        
        status_class = "pass-box" if "PASS" in status else "fail-box"
        badge_class = "badge-pass" if "PASS" in status else "badge-fail"
        
        html += f'''
                    <div class="validation-item {status_class}">
                        <div><span class="badge {badge_class}">{status}</span></div>
                        <div><strong>{validation_type}</strong></div>
                        <div class="small">{details}</div>
                    </div>
        '''
    
    html += '''
                </div>
            </div>
    '''
    
    return html


def _generate_schema_section(results: Dict) -> str:
    """Generate the HTML schema validation section."""
    schema_validation_full = results.get("schema_validation_full", {})
    html = '''
            <div class="section">
                <h2>Schema Validation</h2>
    '''
    
    if not schema_validation_full.get('schema_match', True):
        html += '''
                <div class="table-responsive">
                    <table class="table table-bordered">
                        <thead>
                            <tr>
                                <th>Source Column</th>
                                <th>Target Column</th>
                                <th>Source Data Type</th>
                                <th>Target Data Type</th>
                            </tr>
                        </thead>
                        <tbody>
        '''
        
        # Get all columns from schema info
        all_columns = results["schema_info"]["all_columns"]
        for col_name in all_columns:
            # Source info
            if col_name not in results["schema_info"]["source_columns"]:
                source_col = "MISSING"
                source_type = "MISSING"
            else:
                source_col = col_name
                type_mismatch = next((m for m in schema_validation_full["type_mismatches"] if m["column"] == col_name), None)
                source_type = type_mismatch["source_type"] if type_mismatch else schema_validation_full["source_schema"][col_name]
            
            # Target info
            if col_name not in results["schema_info"]["target_columns"]:
                target_col = "MISSING"
                target_type = "MISSING"
            else:
                target_col = col_name
                type_mismatch = next((m for m in schema_validation_full["type_mismatches"] if m["column"] == col_name), None)
                target_type = type_mismatch["target_type"] if type_mismatch else schema_validation_full["target_schema"][col_name]
            
            # Row class based on mismatch
            row_class = ""
            if source_col == "MISSING" or target_col == "MISSING" or source_type != target_type:
                row_class = "fail"
            
            html += f'''
                            <tr class="{row_class}">
                                <td>{source_col}</td>
                                <td>{target_col}</td>
                                <td>{source_type}</td>
                                <td>{target_type}</td>
                            </tr>
            '''
        
        html += '''
                        </tbody>
                    </table>
                </div>
        '''
    else:
        html += '''
                <div class="alert alert-success">
                    <strong>Success:</strong> Source and target schemas match!
                </div>
        '''
    
    html += '''
            </div>
    '''
    
    return html


def _generate_data_section(results: Dict, table_name: str) -> str:
    """Generate the HTML data validation section."""
    data_validation = results.get("data_validation", {})
    html = '''
            <div class="section">
                <h2>Data Validation</h2>
    '''
    
    # All Columns table
    if data_validation.get("column_comparisons"):
        html += '''
                <h3>All Columns</h3>
                <div class="table-responsive">
                    <table class="table table-bordered">
                        <thead>
                            <tr>
                                <th>Table Name</th>
                                <th>Column Name</th>
                                <th>Total Records</th>
                                <th>Null Count</th>
                                <th>Not Null Count</th>
                                <th>Pass Count</th>
                                <th>Fail Count</th>
                                <th>Pass %</th>
                                <th>Status</th>
                            </tr>
                        </thead>
                        <tbody>
        '''
        
        for col_result in data_validation["column_comparisons"]:
            fail_count = col_result['total_records'] - col_result['pass_count']
            row_class = "pass" if col_result['status'] == "PASS" else "fail"
            
            html += f'''
                            <tr class="{row_class}">
                                <td>{table_name}</td>
                                <td>{col_result['column_name']}</td>
                                <td>{col_result['total_records']}</td>
                                <td>{col_result['source_null_count']}</td>
                                <td>{col_result['source_not_null_count']}</td>
                                <td>{col_result['pass_count']}</td>
                                <td>{fail_count}</td>
                                <td>{col_result['pass_percentage']:.2f}%</td>
                                <td><span class="badge badge-{row_class.replace('pass', 'pass').replace('fail', 'fail')}">{col_result['status']}</span></td>
                            </tr>
            '''
        
        html += '''
                        </tbody>
                    </table>
                </div>
        '''
        
        # Failed Columns table
        failed_columns = [col for col in data_validation["column_comparisons"] if col['status'] == "FAIL"]
        if failed_columns:
            html += '''
                <h3>Failed Columns Only</h3>
                <div class="table-responsive">
                    <table class="table table-bordered table-danger">
                        <thead>
                            <tr>
                                <th>Table Name</th>
                                <th>Column Name</th>
                                <th>Total Records</th>
                                <th>Null Count</th>
                                <th>Not Null Count</th>
                                <th>Pass Count</th>
                                <th>Fail Count</th>
                                <th>Pass %</th>
                                <th>Status</th>
                            </tr>
                        </thead>
                        <tbody>
            '''
            
            for col_result in failed_columns:
                fail_count = col_result['total_records'] - col_result['pass_count']
                
                html += f'''
                            <tr>
                                <td>{table_name}</td>
                                <td>{col_result['column_name']}</td>
                                <td>{col_result['total_records']}</td>
                                <td>{col_result['source_null_count']}</td>
                                <td>{col_result['source_not_null_count']}</td>
                                <td>{col_result['pass_count']}</td>
                                <td>{fail_count}</td>
                                <td>{col_result['pass_percentage']:.2f}%</td>
                                <td><span class="badge badge-fail">{col_result['status']}</span></td>
                            </tr>
                '''
            
            html += '''
                        </tbody>
                    </table>
                </div>
            '''
            
            # Mismatch samples for failed columns
            html += '''
                <h3>Detailed Mismatch Samples (Top 5 per column)</h3>
            '''
            
            # Handle mismatches for each failed column
            for mismatch in results.get("mismatches", []):
                column_name = mismatch["column"]
                if not mismatch.get("details"):
                    continue
                
                html += f'''
                <div class="mismatch-sample">
                    <h4>Failed Column: {column_name}</h4>
                    <div class="table-responsive">
                        <table class="table table-bordered">
                            <thead>
                                <tr>
                '''
                
                # Get keys from first mismatch
                first_mismatch = mismatch["details"][0]
                keys = [k for k in first_mismatch.asDict().keys() if k not in ['source_value', 'target_value']]
                
                # Add key columns header
                key_header = f"Key ({', '.join(keys)})"
                html += f'''
                                    <th>{key_header}</th>
                                    <th>Source Value</th>
                                    <th>Target Value</th>
                                </tr>
                            </thead>
                            <tbody>
                '''
                
                # Add rows for each mismatch
                for detail in mismatch["details"][:5]:
                    key_values = ", ".join(str(detail[k]) for k in keys)
                    html += f'''
                                <tr>
                                    <td>{key_values}</td>
                                    <td>{detail['source_value']}</td>
                                    <td>{detail['target_value']}</td>
                                </tr>
                    '''
                
                html += '''
                            </tbody>
                        </table>
                    </div>
                </div>
                '''
            
        else:
            html += '''
                <div class="alert alert-success">
                    <strong>Success:</strong> All columns passed validation!
                </div>
            '''
    
    html += '''
            </div>
    '''
    
    return html


def _generate_null_section(results: Dict) -> str:
    """Generate the HTML null checks section."""
    null_checks = results.get("null_checks", {}).get("columns", {})
    html = '''
            <div class="section">
                <h2>Null Checks</h2>
                <div class="table-responsive">
                    <table class="table table-bordered">
                        <thead>
                            <tr>
                                <th>Column</th>
                                <th>Null Count</th>
                                <th>Total Count</th>
                                <th>Null %</th>
                            </tr>
                        </thead>
                        <tbody>
    '''
    
    for col_name, stats in null_checks.items():
        null_percent = stats.get('null_percentage', 0)
        row_class = "fail" if null_percent > 0 else ""
        
        html += f'''
                            <tr class="{row_class}">
                                <td>{col_name}</td>
                                <td>{stats.get('null_count', 0)}</td>
                                <td>{stats.get('total_count', 0)}</td>
                                <td>{null_percent:.2f}%</td>
                            </tr>
        '''
    
    html += '''
                        </tbody>
                    </table>
                </div>
            </div>
    '''
    
    return html


def _generate_duplicate_section(results: Dict) -> str:
    """Generate the HTML duplicate checks section."""
    dup_checks = results.get("duplicate_checks", {})
    html = f'''
            <div class="section">
                <h2>Duplicate Checks</h2>
                <div class="card">
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-3">
                                <div class="card mb-3">
                                    <div class="card-body text-center">
                                        <h3>{dup_checks.get('total_records', 0):,}</h3>
                                        <p>Total Records</p>
                                    </div>
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="card mb-3">
                                    <div class="card-body text-center">
                                        <h3>{dup_checks.get('unique_records', 0):,}</h3>
                                        <p>Unique Records</p>
                                    </div>
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="card mb-3">
                                    <div class="card-body text-center">
                                        <h3>{dup_checks.get('duplicate_count', 0):,}</h3>
                                        <p>Duplicate Count</p>
                                    </div>
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="card mb-3">
                                    <div class="card-body text-center">
                                        <h3>{dup_checks.get('duplicate_percentage', 0):.2f}%</h3>
                                        <p>Duplicate Percentage</p>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
    '''
    
    return html


def _generate_rule_section(results: Dict) -> str:
    """Generate the HTML rule validation section."""
    rule_validation = results.get("rule_validation", {})
    html = '''
            <div class="section">
                <h2>Rule Validation</h2>
    '''
    
    if rule_validation.get("failed_rules"):
        html += '''
                <div class="table-responsive">
                    <table class="table table-bordered">
                        <thead>
                            <tr>
                                <th>Column</th>
                                <th>Rule</th>
                                <th>Status</th>
                                <th>Message</th>
                            </tr>
                        </thead>
                        <tbody>
        '''
        
        for rule in rule_validation["failed_rules"]:
            html += f'''
                            <tr class="fail">
                                <td>{rule.get('column', '')}</td>
                                <td>{rule.get('rule', '')}</td>
                                <td>{rule.get('status', '')}</td>
                                <td>{rule.get('message', '')}</td>
                            </tr>
            '''
        
        html += '''
                        </tbody>
                    </table>
                </div>
        '''
    else:
        html += '''
                <div class="alert alert-success">
                    <strong>Success:</strong> All rules passed!
                </div>
        '''
    
    html += '''
            </div>
    '''
    
    return html


def _generate_recommendations_section(results: Dict) -> str:
    """Generate the HTML recommendations section."""
    recommendations = []
    
    # Check for failed columns
    data_validation = results.get("data_validation", {})
    if data_validation.get("column_comparisons"):
        failed_columns = [col for col in data_validation["column_comparisons"] if col['status'] == "FAIL"]
        for col in failed_columns:
            recommendations.append(
                f"Column '{col['column_name']}' failed with {col['total_records'] - col['pass_count']} mismatches — "
                f"check data formatting or join logic."
            )
    
    # Check for null issues
    null_checks = results.get("null_checks", {}).get("columns", {})
    for col_name, stats in null_checks.items():
        if stats.get('null_percentage', 0) > 0:
            recommendations.append(
                f"Column '{col_name}' has {stats.get('null_percentage', 0):.2f}% null values — "
                f"verify if this is expected."
            )
    
    # Check for duplicate issues
    dup_checks = results.get("duplicate_checks", {})
    if dup_checks.get('duplicate_percentage', 0) > 0:
        recommendations.append(
            f"Found {dup_checks.get('duplicate_percentage', 0):.2f}% duplicate records — "
            f"investigate if this is acceptable."
        )
    
    if not recommendations:
        return ""
    
    html = '''
            <div class="section recommendations">
                <h3><i class="fas fa-lightbulb section-icon"></i>What to Investigate</h3>
                <ul>
    '''
    
    for rec in recommendations:
        html += f'''
                    <li>{rec}</li>
        '''
    
    html += '''
                </ul>
            </div>
    '''
    
    return html


def _generate_final_section(results: Dict) -> str:
    """Generate the final assessment section."""
    final_summary = results.get("final_summary", [])
    html = '''
            <div class="section summary-section" style="background: linear-gradient(to right, #f8f9fa, #e9ecef);">
                <h2 style="text-align: center; color: #343a40; margin-bottom: 15px;">Final Assessment</h2>
                <div class="row">
    '''
    
    for validation in final_summary:
        validation_type = validation.get("Validation Type", "")
        status = validation.get("Status", "")
        details = validation.get("Details", "")
        
        # Determine card colors and icons based on status
        if "PASS" in status:
            card_class = "border-success"
            header_class = "bg-success text-white"
            icon = '<i class="fas fa-check-circle" style="margin-right: 5px;"></i>'
        else:
            card_class = "border-danger"
            header_class = "bg-danger text-white"
            icon = '<i class="fas fa-times-circle" style="margin-right: 5px;"></i>'
        
        # Special formatting for overall status
        if validation_type == "Overall Status":
            html += f'''
                    <div class="col-12 mb-3">
                        <div class="card {card_class}" style="box-shadow: 0 3px 6px rgba(0,0,0,0.1);">
                            <div class="card-header {header_class}" style="text-align: center; padding: 10px;">
                                <h4 class="mb-0">{validation_type}</h4>
                            </div>
                            <div class="card-body text-center" style="padding: 15px;">
                                <h3>{icon} {status}</h3>
                                <p>{details}</p>
                            </div>
                        </div>
                    </div>
            '''
        else:
            html += f'''
                    <div class="col-md-4 mb-2">
                        <div class="card h-100 {card_class}" style="border-radius: 8px; transition: transform 0.3s; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                            <div class="card-header {header_class}" style="border-radius: 8px 8px 0 0; padding: 8px 12px;">
                                <h5 class="mb-0">{validation_type}</h5>
                            </div>
                            <div class="card-body d-flex flex-column py-2 px-3">
                                <div class="text-center mb-2" style="font-size: 1.1rem;">
                                    {icon} {status}
                                </div>
                                <p class="card-text text-muted mt-auto small">{details}</p>
                            </div>
                        </div>
                    </div>
            '''
    
    html += '''
                </div>
            </div>
    '''
    
    return html 