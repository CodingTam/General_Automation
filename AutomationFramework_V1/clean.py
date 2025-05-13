#!/usr/bin/env python3

import os
import shutil
import glob
from typing import List, Tuple

def get_patterns_to_clean() -> Tuple[List[str], List[str]]:
    """Get patterns of files and directories to clean."""
    # Files to clean
    file_patterns = [
        "*.pyc",
        "*.pyo",
        "*.pyd",
        "*~",
        "*.bak",
        "*.swp",
        "*.log",
        "*.sqlite",
        "*.sqlite3",
        "*.sqlite-journal",
        "*.sqlite-wal",
        "*.sqlite-shm",
        ".DS_Store",
        "Thumbs.db",
        ".coverage",
        "coverage.xml",
        ".pytest_cache",
        "__pycache__",
        ".hypothesis",
        "*.egg-info",
        "dist",
        "build",
        ".eggs"
    ]
    
    # Directories to clean
    dir_patterns = [
        "__pycache__",
        ".pytest_cache",
        ".coverage",
        ".hypothesis",
        "*.egg-info",
        "dist",
        "build",
        ".eggs",
        "logs"
    ]
    
    return file_patterns, dir_patterns

def clean_workspace(dry_run: bool = False) -> None:
    """
    Clean the project workspace by removing temporary files and directories.
    
    Args:
        dry_run: If True, only show what would be deleted without actually deleting
    """
    file_patterns, dir_patterns = get_patterns_to_clean()
    
    # Get the project root directory
    root_dir = os.path.dirname(os.path.abspath(__file__))
    
    print("============================================================")
    print("                    CLEAN PROJECT FILES                     ")
    print("============================================================")
    print("\nCleaning project...")
    
    # Track counts
    files_removed = 0
    dirs_removed = 0
    
    # Walk through the directory tree
    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=False):
        # Skip .git directory
        if '.git' in dirpath:
            continue
        
        # Clean files
        for pattern in file_patterns:
            for file in glob.glob(os.path.join(dirpath, pattern)):
                if os.path.isfile(file):
                    if dry_run:
                        print(f"Would remove file: {file}")
                    else:
                        try:
                            os.remove(file)
                            print(f"Removed file: {file}")
                            files_removed += 1
                        except Exception as e:
                            print(f"Error removing {file}: {e}")
        
        # Clean directories
        for pattern in dir_patterns:
            for dir_path in glob.glob(os.path.join(dirpath, pattern)):
                if os.path.isdir(dir_path):
                    if dry_run:
                        print(f"Would remove directory: {dir_path}")
                    else:
                        try:
                            shutil.rmtree(dir_path)
                            print(f"Removed directory: {dir_path}")
                            dirs_removed += 1
                        except Exception as e:
                            print(f"Error removing {dir_path}: {e}")
    
    print("\n------------------------------------------------------------")
    if dry_run:
        print("Dry run completed. No files were actually removed.")
    else:
        print(f"Cleanup completed:")
        print(f"- {files_removed} files removed")
        print(f"- {dirs_removed} directories removed")
    print("------------------------------------------------------------\n")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Clean project workspace")
    parser.add_argument('--dry-run', action='store_true', 
                      help="Show what would be deleted without actually deleting")
    
    args = parser.parse_args()
    clean_workspace(dry_run=args.dry_run) 