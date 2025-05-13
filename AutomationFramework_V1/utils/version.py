#!/usr/bin/env python3
"""
ONE TEST Framework Version Information

This file contains the central version information for the ONE TEST framework.
All scripts should import this file to access version information.
"""

# Framework version components
MAJOR = 1
MINOR = 0
PATCH = 0

# Build in string format
VERSION = f"{MAJOR}.{MINOR}.{PATCH}"

# Release information
RELEASE_DATE = "2025-07-01"
FRAMEWORK_NAME = "ONE TEST"
FRAMEWORK_DESCRIPTION = "INTELLIGENT QA AUTOMATION FRAMEWORK"

def get_version_string():
    """Return the framework version as a formatted string."""
    return f"v{VERSION}"

def get_full_version_info():
    """Return a dictionary with complete version information."""
    return {
        "name": FRAMEWORK_NAME,
        "version": get_version_string(),
        "major": MAJOR,
        "minor": MINOR,
        "patch": PATCH,
        "release_date": RELEASE_DATE,
        "description": FRAMEWORK_DESCRIPTION
    }

# You can add more version-related functions here as needed 