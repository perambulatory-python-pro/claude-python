# save as check_imports.py
import os
import re

# Common third-party packages to look for
packages = {
    'plotly', 'matplotlib', 'seaborn', 'sqlalchemy', 
    'psycopg2', 'dotenv', 'scipy', 'sklearn', 'numpy', 
    'pandas', 'openpyxl', 'xlrd', 'xlsxwriter'
}

found_imports = set()

# Scan all Python files
for root, dirs, files in os.walk('.'):
    if 'venv' in root:
        continue
    for file in files:
        if file.endswith('.py'):
            filepath = os.path.join(root, file)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # Find import statements
                    imports = re.findall(r'(?:from|import)\s+(\w+)', content)
                    for imp in imports:
                        if imp in packages:
                            found_imports.add(imp)
            except:
                pass

print("Packages used in your project:")
for pkg in sorted(found_imports):
    print(f"  - {pkg}")