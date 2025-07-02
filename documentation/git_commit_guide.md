# Git Commit Guide for Python Development

## Table of Contents
- [Understanding Git Commits](#understanding-git-commits)
- [Python Development Best Practices](#python-development-best-practices)
- [Setting Up .gitignore](#setting-up-gitignore)
- [Commit Workflow in VS Code](#commit-workflow-in-vs-code)
- [Good Commit Message Examples](#good-commit-message-examples)
- [Command Line Alternative](#command-line-alternative)
- [For Your Invoice Management Project](#for-your-invoice-management-project)
- [Professional Development Workflow](#professional-development-workflow)
- [Key Python Development Concepts](#key-python-development-concepts)

## Understanding Git Commits

**What commits do:**
- Create permanent snapshots of your code at specific points in time
- Track the history of changes with messages explaining what you changed
- Allow you to revert to previous versions if needed
- Enable collaboration with others by sharing your changes

## Python Development Best Practices

### What should be committed:
- ✅ Your Python scripts (.py files)
- ✅ Requirements files (requirements.txt, pyproject.toml)
- ✅ Configuration files
- ✅ Documentation and README files
- ✅ Project structure files

### What should NOT be committed:
- ❌ Virtual environment folder (venv, .venv)
- ❌ __pycache__ folders
- ❌ .pyc files
- ❌ Sensitive data (passwords, API keys)
- ❌ Large data files

## Setting Up .gitignore

Create a `.gitignore` file in your project root with the following content:

```gitignore
# Python virtual environments
venv/
.venv/
env/
ENV/

# Python cache files
__pycache__/
*.py[cod]
*$py.class

# Distribution / packaging
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
share/python-wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# PyInstaller
*.manifest
*.spec

# Unit test / coverage reports
htmlcov/
.tox/
.nox/
.coverage
.coverage.*
.cache
nosetests.xml
coverage.xml
*.cover
*.py,cover
.hypothesis/
.pytest_cache/
cover/

# Jupyter Notebook
.ipynb_checkpoints

# IPython
profile_default/
ipython_config.py

# Environment variables
.env
.env.local
.env.development.local
.env.test.local
.env.production.local

# Sensitive data files
*password*
*secret*
*key*
config.ini
credentials.json

# Database files
*.db
*.sqlite
*.sqlite3

# Excel/CSV data files (add specific patterns for your data)
# invoice_details_*.csv
# *.xlsx

# IDE settings
.vscode/settings.json
.idea/
*.swp
*.swo

# OS generated files
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db

# Log files
*.log
logs/

# Temporary files
temp/
tmp/
*.tmp
```

## Commit Workflow in VS Code

### 1. Check Status
Look at the Source Control panel (Ctrl+Shift+G) to see what files have changed.

### 2. Stage Changes
- Click the + button next to files you want to commit
- Or use "Stage All Changes" for all files

### 3. Write Commit Message
Follow the best practices outlined below.

### 4. Commit the Changes
- Click "Commit" button or use Ctrl+Enter
- This saves the snapshot locally

### 5. Push to Remote (if applicable)
- Click "Sync Changes" or "Push" to share with your team
- This uploads your commits to GitHub/remote repository

## Good Commit Message Examples

### Format Guidelines
- **First line:** 50 characters or less
- **Use imperative mood:** "Add feature" not "Added feature"
- **Be specific:** Explain what and why, not how

### Examples

#### Feature additions
```
Add invoice processing automation script
Create BCI/AUS data transformation module
Implement EDI-based dimension lookup system
```

#### Bug fixes
```
Fix date parsing error in AUS transformer
Resolve building code mapping issue
Correct EMID lookup for duplicate job codes
```

#### Improvements
```
Optimize memory usage in large file processing
Enhance error handling in invoice validator
Refactor duplicate detection algorithm
```

#### Documentation
```
Update README with installation instructions
Add comments to transformation functions
Document API endpoints for invoice system
```

#### Configuration
```
Update requirements.txt with pandas 2.0
Add .gitignore for Python virtual environments
Configure VS Code settings for project
```

#### Multi-line example for complex changes:
```
Add comprehensive invoice transformation system

- Create unified schema for BCI and AUS formats  
- Implement EDI-based dimensional lookups
- Add memory optimization for large datasets
- Include data quality validation checks
- Support invoice revision handling (R, Z, A suffixes)

Resolves issues with duplicate EMID mappings and 
improves processing speed by 300% for 25K+ records.
```

## Command Line Alternative

If you prefer command line (good to understand these concepts):

```bash
# Check what's changed
git status

# Add specific files
git add scripts/invoice_transformer.py
git add requirements.txt

# Or add all changes
git add .

# Commit with message
git commit -m "Add invoice processing automation script"

# Push to remote repository
git push origin main
```

## For Your Invoice Management Project

Given your work with complex financial data processing, here's a recommended commit strategy:

### Small, Focused Commits

Instead of one big commit:
```bash
git commit -m "Update entire invoice system"
```

Do several smaller commits:
```bash
git commit -m "Add BCI data transformer module"
git commit -m "Implement AUS job number lookup"
git commit -m "Add error handling for missing EMIDs"
git commit -m "Update documentation for new workflow"
```

### When to Commit
- ✅ After completing a specific function/feature
- ✅ After fixing a bug
- ✅ Before trying a risky change (so you can revert)
- ✅ End of each work session
- ❌ Don't commit broken/untested code
- ❌ Don't commit sensitive data (passwords, real invoice data)

## Professional Development Workflow

For your team environment, consider this workflow:

### 1. Create feature branches for major changes:
```bash
git checkout -b feature/invoice-automation
```

### 2. Make small commits as you develop

### 3. Test thoroughly before final commit

### 4. Create pull requests for team review

### 5. Merge to main after approval

## Key Python Development Concepts

### Version Control Benefits for Data Engineering:
- **Rollback capability**: If a script breaks your invoice processing, you can instantly revert
- **Change tracking**: See exactly what changed between working and broken versions  
- **Collaboration**: Multiple team members can work on different parts of the system
- **Backup**: Your code is safely stored in multiple places
- **Documentation**: Commit messages create a history of why changes were made

### Memory for Learning:
The more you commit, the better you'll understand:
- How your code evolves over time
- Which changes cause problems
- How to structure your development process
- Collaboration patterns with your team

## Summary

Yes, absolutely commit your changes! It's one of the most important habits for professional software development. Start with small, frequent commits and detailed messages - this will serve you well as your Python skills grow and your automation projects become more complex.

### Quick Reference Commands

```bash
# Daily workflow
git status                    # See what's changed
git add .                     # Stage all changes
git commit -m "Your message"  # Commit changes
git push                      # Share with team

# Safety commands
git log --oneline            # See commit history
git diff                     # See what changed
git checkout -- filename    # Discard changes to file
git reset HEAD~1             # Undo last commit (keep changes)
```

Remember: Committing frequently with good messages is a professional habit that will save you time and headaches as your invoice automation system grows in complexity!