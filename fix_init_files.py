# save as: fix_init_files.py
import os
from pathlib import Path

def create_init_files(root_dir='.'):
    """Create __init__.py files in all Python package directories"""
    created_files = []
    
    for root, dirs, files in os.walk(root_dir):
        # Skip hidden directories and __pycache__
        dirs[:] = [d for d in dirs if not d.startswith('.') and d != '__pycache__']
        
        # Check if this directory contains Python files
        python_files = [f for f in files if f.endswith('.py') and f != '__init__.py']
        
        if python_files:
            init_path = os.path.join(root, '__init__.py')
            if not os.path.exists(init_path):
                Path(init_path).touch()
                created_files.append(init_path)
                print(f"Created: {init_path}")
    
    if not created_files:
        print("All __init__.py files already exist!")
    else:
        print(f"\nCreated {len(created_files)} __init__.py files")

# Run the fix
create_init_files()