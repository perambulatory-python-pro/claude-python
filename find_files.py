# save as find_files.py
import os
from pathlib import Path

# Files we're looking for
files_to_find = [
    'fixed_date_converter.py',
    'smart_duplicate_handler.py',
    'data_mapper_enhanced.py',
    'capital_project_streamlit_integration.py'
]

print("Searching for files...\n")

for filename in files_to_find:
    found = False
    for root, dirs, files in os.walk('.'):
        if 'venv' in root or '__pycache__' in root:
            continue
        if filename in files:
            path = Path(root) / filename
            # Convert to module path
            module_path = str(path).replace('\\', '.').replace('/', '.').replace('.py', '')
            if module_path.startswith('.'):
                module_path = module_path[2:]  # Remove './'
            
            print(f"✓ {filename}")
            print(f"  Location: {path}")
            print(f"  Import as: from {module_path} import ...")
            print()
            found = True
            break
    
    if not found:
        print(f"✗ {filename} - NOT FOUND")
        print()