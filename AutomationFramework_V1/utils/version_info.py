#!/usr/bin/env python3
"""
ONE TEST Framework Version Information

This script displays the version information for the ONE TEST framework.
"""

import argparse
import json
from utils.version import get_full_version_info, FRAMEWORK_NAME, VERSION
from utils.banner import print_intro

def display_version_info(detailed=False, json_format=False, banner=False):
    """
    Display version information for the framework.
    
    Args:
        detailed (bool): If True, displays detailed version information
        json_format (bool): If True, outputs in JSON format
        banner (bool): If True, displays the banner
    """
    if banner:
        print_intro()
        return
    
    if json_format:
        # Output as JSON
        print(json.dumps(get_full_version_info(), indent=2))
    elif detailed:
        # Detailed version information
        info = get_full_version_info()
        print(f"{info['name']} Framework Version Information")
        print("=" * 40)
        print(f"Version:       {info['version']}")
        print(f"Major:         {info['major']}")
        print(f"Minor:         {info['minor']}")
        print(f"Patch:         {info['patch']}")
        print(f"Release Date:  {info['release_date']}")
        print(f"Description:   {info['description']}")
    else:
        # Simple version display
        print(f"{FRAMEWORK_NAME} v{VERSION}")

def main():
    parser = argparse.ArgumentParser(description='Display ONE TEST framework version information')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--detailed', '-d', action='store_true', help='Display detailed version information')
    group.add_argument('--json', '-j', action='store_true', help='Output version information in JSON format')
    group.add_argument('--banner', '-b', action='store_true', help='Display the framework banner')
    
    args = parser.parse_args()
    
    display_version_info(
        detailed=args.detailed,
        json_format=args.json,
        banner=args.banner
    )

if __name__ == "__main__":
    main() 