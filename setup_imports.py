# save as: setup_imports.py
import sys
import os
from pathlib import Path

def setup_project_imports():
    """Add project root to Python path for imports"""
    # Get the directory containing this script
    current_dir = Path(__file__).parent.absolute()
    
    # Add to Python path if not already there
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    
    # Also add parent directory if needed
    parent_dir = current_dir.parent
    if str(parent_dir) not in sys.path:
        sys.path.insert(0, str(parent_dir))
    
    print(f"Project root added to path: {current_dir}")
    return current_dir

# If running directly, show the setup
if __name__ == "__main__":
    project_root = setup_project_imports()
    print("\nPython path after setup:")
    for i, path in enumerate(sys.path[:5]):  # Show first 5 paths
        print(f"{i}: {path}")