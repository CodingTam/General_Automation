#!/usr/bin/env python3
"""
Single Test Runner
Runs a single test file without any interactive output
"""

import os
import sys
import unittest
import subprocess
from pathlib import Path

def run_without_python_process():
    """Run test using subprocess to avoid any stdout issues"""
    # Create output directory
    Path('test_results').mkdir(exist_ok=True)
    
    # Run a single test file using subprocess
    result_file = os.path.join('test_results', 'single_test_result.txt')
    
    # Use subprocess to run the test
    with open(result_file, 'w') as f:
        # Run test_yaml_processor.py directly with unittest
        subprocess.run(
            ['python3', '-m', 'unittest', 'tests/framework_self_tests/test_yaml_processor.py'], 
            stdout=f, 
            stderr=f
        )
    
    # Write test completion marker
    with open('test_complete.txt', 'w') as f:
        f.write(f"Test completed. Results in {result_file}")
    
    # Return the result file path
    return result_file

if __name__ == "__main__":
    # Run directly via subprocess to isolate any stdout issues
    result_file = run_without_python_process() 