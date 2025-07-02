# save as: check_structure.py
import os
import sys
from pathlib import Path

def print_project_structure(start_path='.', prefix='', max_depth=5, current_depth=0):
    """Print the project directory structure"""
    if current_depth > max_depth:
        return
    
    items = sorted(os.listdir(start_path))
    for i, item in enumerate(items):
        if item.startswith('.') or item == '__pycache__':
            continue
            
        path = os.path.join(start_path, item)
        is_last = i == len(items) - 1
        
        print(prefix + ('└── ' if is_last else '├── ') + item)
        
        if os.path.isdir(path) and current_depth < max_depth:
            extension = '    ' if is_last else '│   '
            print_project_structure(path, prefix + extension, max_depth, current_depth + 1)

print("Current Working Directory:", os.getcwd())
print("\nPython Path:")
for path in sys.path:
    print(f"  - {path}")

print("\n\nProject Structure:")
print_project_structure()

# Check for specific files
print("\n\nChecking for data_mapper_enhanced.py:")
for root, dirs, files in os.walk('.'):
    for file in files:
        if 'data_mapper' in file.lower():
            print(f"Found: {os.path.join(root, file)}")