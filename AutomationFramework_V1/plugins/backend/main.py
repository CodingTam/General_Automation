#!/usr/bin/env python3
"""
FastAPI backend for the file comparator plugin.
"""

import os
import pandas as pd
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="File Comparator API")

# Add CORS middleware to allow requests from the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static files (React frontend)
static_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")


class CompareRequest(BaseModel):
    source_file: str
    target_file: str
    key_columns: List[str]


class DecodeRequest(BaseModel):
    decode_file: str
    column_name: str


@app.get("/compare_ui", response_class=HTMLResponse)
async def get_compare_ui(source: Optional[str] = None, target: Optional[str] = None):
    """Serve the HTML page for the comparison UI."""
    # Read the HTML file
    html_path = os.path.join(static_dir, "index.html")
    
    try:
        with open(html_path, "r") as f:
            html_content = f.read()
        return html_content
    except FileNotFoundError:
        # If the file doesn't exist yet, return a placeholder
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <title>File Comparator</title>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <script>
                // Pass URL parameters to the app when it loads
                window.sourceFile = "{source}";
                window.targetFile = "{target}";
            </script>
        </head>
        <body>
            <div id="root">
                <h1>File Comparator</h1>
                <p>The React app is not built yet. Please build the frontend first.</p>
            </div>
        </body>
        </html>
        """.format(source=source or "", target=target or "")


@app.post("/load_files")
async def load_files(request: CompareRequest):
    """Load and parse the source and target files into JSON."""
    try:
        # Load source file
        source_df = load_file(request.source_file)
        
        # Load target file
        target_df = load_file(request.target_file)
        
        # Get column names
        source_columns = source_df.columns.tolist()
        target_columns = target_df.columns.tolist()
        
        # Convert DataFrames to records
        source_data = source_df.fillna("").to_dict(orient="records")
        target_data = target_df.fillna("").to_dict(orient="records")
        
        return {
            "source": {
                "columns": source_columns,
                "data": source_data,
                "file_path": request.source_file,
                "row_count": len(source_data)
            },
            "target": {
                "columns": target_columns,
                "data": target_data,
                "file_path": request.target_file,
                "row_count": len(target_data)
            },
            "common_columns": [col for col in source_columns if col in target_columns]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading files: {str(e)}")


@app.post("/compare")
async def compare_files(request: CompareRequest):
    """Perform a row/column comparison using selected key columns."""
    try:
        # Load source and target files
        source_df = load_file(request.source_file)
        target_df = load_file(request.target_file)
        
        # Validate key columns
        for key_col in request.key_columns:
            if key_col not in source_df.columns or key_col not in target_df.columns:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Key column '{key_col}' not found in both files"
                )
        
        # Common columns for comparison
        common_columns = [col for col in source_df.columns if col in target_df.columns]
        comparison_columns = [col for col in common_columns if col not in request.key_columns]
        
        # Results container
        diff_results = []
        
        # For each row in source file, find matching row in target file by key columns
        for _, source_row in source_df.iterrows():
            # Create filter for key columns
            filter_expr = True
            for key_col in request.key_columns:
                filter_expr = filter_expr & (target_df[key_col] == source_row[key_col])
            
            # Find matching rows in target
            matching_rows = target_df[filter_expr]
            
            if len(matching_rows) == 0:
                # Row exists in source but not in target
                key_values = {key: source_row[key] for key in request.key_columns}
                diff_results.append({
                    "key_values": key_values,
                    "status": "missing_in_target",
                    "differences": []
                })
            else:
                # Row exists in both, compare values
                target_row = matching_rows.iloc[0]
                differences = []
                
                for col in comparison_columns:
                    source_val = source_row[col]
                    target_val = target_row[col]
                    
                    # Handle NaN values
                    if pd.isna(source_val) and pd.isna(target_val):
                        continue
                    elif pd.isna(source_val):
                        source_val = ""
                    elif pd.isna(target_val):
                        target_val = ""
                    
                    # Compare values and add to differences if they don't match
                    if source_val != target_val:
                        differences.append({
                            "column": col,
                            "source_value": str(source_val),
                            "target_value": str(target_val)
                        })
                
                if differences:
                    key_values = {key: source_row[key] for key in request.key_columns}
                    diff_results.append({
                        "key_values": key_values,
                        "status": "values_differ",
                        "differences": differences
                    })
        
        # Find rows in target that don't exist in source
        for _, target_row in target_df.iterrows():
            # Create filter for key columns
            filter_expr = True
            for key_col in request.key_columns:
                filter_expr = filter_expr & (source_df[key_col] == target_row[key_col])
            
            # Find matching rows in source
            matching_rows = source_df[filter_expr]
            
            if len(matching_rows) == 0:
                # Row exists in target but not in source
                key_values = {key: target_row[key] for key in request.key_columns}
                diff_results.append({
                    "key_values": key_values,
                    "status": "missing_in_source",
                    "differences": []
                })
        
        return {
            "results": diff_results,
            "summary": {
                "total_differences": len(diff_results),
                "missing_in_target": sum(1 for r in diff_results if r["status"] == "missing_in_target"),
                "missing_in_source": sum(1 for r in diff_results if r["status"] == "missing_in_source"),
                "values_differ": sum(1 for r in diff_results if r["status"] == "values_differ")
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error comparing files: {str(e)}")


@app.post("/decode")
async def decode_values(request: DecodeRequest):
    """Load decode mappings from a CSV file."""
    try:
        # Load decode file
        decode_df = load_file(request.decode_file)
        
        # Check if the file has at least 2 columns
        if len(decode_df.columns) < 2:
            raise HTTPException(
                status_code=400,
                detail="Decode file must have at least 2 columns (code and description)"
            )
        
        # Create a mapping from the first column to the second column
        code_col = decode_df.columns[0]
        desc_col = decode_df.columns[1]
        
        mapping = dict(zip(decode_df[code_col], decode_df[desc_col]))
        
        return {
            "column_name": request.column_name,
            "mapping": mapping
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading decode file: {str(e)}")


def load_file(file_path: str) -> pd.DataFrame:
    """Load a file (CSV or Excel) into a pandas DataFrame."""
    file_path = file_path.strip()
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
    
    file_ext = os.path.splitext(file_path)[1].lower()
    
    try:
        if file_ext in ('.csv', '.txt'):
            return pd.read_csv(file_path)
        elif file_ext in ('.xlsx', '.xls'):
            return pd.read_excel(file_path)
        else:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported file type: {file_ext}. Only CSV and Excel files are supported."
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading file {file_path}: {str(e)}")


@app.get("/")
async def root():
    """Root endpoint that redirects to the compare_ui endpoint."""
    return {"message": "File Comparator API", "docs_url": "/docs"} 