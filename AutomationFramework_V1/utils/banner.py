#!/usr/bin/env python3
"""
Banner Utilities for ONE TEST Framework

This module provides utilities for rendering a consistent banner/intro
across all entry points of the framework.
"""

from datetime import datetime
from utils.version import VERSION, FRAMEWORK_NAME, FRAMEWORK_DESCRIPTION

def print_intro(environment="QA", add_extra_line=False):
    """
    Print an introduction banner for the test framework.
    
    Args:
        environment (str): The environment name to display (e.g., QA, DEV, UAT, PROD)
        add_extra_line (bool): If True, adds an extra blank line after the banner
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("=" * 71)
    print(f"  🚀 WELCOME TO {FRAMEWORK_NAME} - {FRAMEWORK_DESCRIPTION} 🚀")
    print("=" * 71)
    print()
    print(f"   🔹 Engine       : {FRAMEWORK_NAME} v{VERSION}")
    print(f"   🔹 Mode         : Automated Validation")
    print(f"   🔹 Environment  : 🔧 {environment.upper()}")
    print(f"   🔹 Timestamp    : 🕒 {timestamp}")
    print(f"   🔹 Author       : 🧠 Tamil & Team")
    print()
    print("-" * 71)
    print("🔍 Initializing modules...")
    print("📦 Loading test assets...")
    print("🧪 Preparing test cases...")
    print("✅ Engine ready. Executing test suite...")
    print()
    print("=" * 71)
    
    # Add an extra blank line if requested
    if add_extra_line:
        print() 