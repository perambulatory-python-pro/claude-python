"""
Setup Script for Database-Powered Invoice System
This script helps you set up and validate your database system

Run with: python setup_db_system.py
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

def check_python_version():
    """Check if Python version is compatible"""
    print("1. Checking Python version...")
    version = sys.version_info
    if version.major == 3 and version.minor >= 8:
        print(f"   ✅ Python {version.major}.{version.minor}.{version.micro} - Compatible")
        return True
    else:
        print(f"   ❌ Python {version.major}.{version.minor}.{version.micro} - Need Python 3.8+")
        return False

def check_required_packages():
    """Check if required packages are installed"""
    print("\n2. Checking required packages...")
    
    required_packages = [
        'sqlalchemy',
        'psycopg2',
        'pandas',
        'streamlit',
        'python-dotenv',
        'openpyxl'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            if package == 'psycopg2':
                import psycopg2
            elif package == 'sqlalchemy':
                import sqlalchemy
            elif package == 'pandas':
                import pandas
            elif package == 'streamlit':
                import streamlit
            elif package == 'python-dotenv':
                import dotenv
            elif package == 'openpyxl':
                import openpyxl
            
            print(f"   ✅ {package}")
        except ImportError:
            print(f"   ❌ {package} - Not installed")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n   📦 Install missing packages with:")
        print(f"   pip install {' '.join(missing_packages)}")
        return False
    
    return True

def check_env_file():
    """Check if .env file exists and has required variables"""
    print("\n3. Checking .env configuration...")
    
    env_path = Path('.env')
    if not env_path.exists():
        print("   ❌ .env file not found")
        print("   📝 Creating sample .env file...")
        
        sample_env = """# Database Configuration
DATABASE_URL=postgresql://username:password@your-neon-host/database_name

# App Configuration
ENVIRONMENT=development
DEBUG=True

# Instructions:
# 1. Replace the DATABASE_URL with your actual NeonDB connection string
# 2. You can find this in your NeonDB dashboard under "Connection Details"
# 3. Format: postgresql://username:password@host/database_name
"""
        with open('.env', 'w') as f:
            f.write(sample_env)
        
        print("   ✅ Sample .env file created")
        print("   ⚠️  Please edit .env with your actual database credentials")
        return False
    
    # Check if DATABASE_URL exists
    load_dotenv()
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("   ❌ DATABASE_URL not found in .env file")
        return False
    
    if 'username:password@your-neon-host' in database_url:
        print("   ⚠️  .env file contains sample data - please update with real credentials")
        return False
    
    print("   ✅ .env file configured")
    return True

def test_database_connection():
    """Test database connection"""
    print("\n4. Testing database connection...")
    
    try:
        from database_manager import DatabaseManager
        
        db = DatabaseManager()
        if db.test_connection():
            print("   ✅ Database connection successful")
            
            # Get table stats
            stats = db.get_table_stats()
            print("   📊 Database statistics:")
            for table, count in stats.items():
                print(f"      - {table}: {count:,} records")
            
            return True
        else:
            print("   ❌ Database connection failed")
            return False
            
    except Exception as e:
        print(f"   ❌ Database connection error: {e}")
        return False

def check_file_structure():
    """Check if all required files are present"""
    print("\n5. Checking file structure...")
    
    required_files = [
        'database_models.py',
        'database_manager.py', 
        'data_mapper.py',
        'invoice_app_db.py',
        'migrate_excel_to_db.py'
    ]
    
    missing_files = []
    
    for file in required_files:
        if Path(file).exists():
            print(f"   ✅ {file}")
        else:
            print(f"   ❌ {file} - Missing")
            missing_files.append(file)
    
    if missing_files:
        print(f"\n   ⚠️  Missing files: {missing_files}")
        print("   📝 Please ensure all required files are in the current directory")
        return False
    
    return True

def run_sample_operations():
    """Run sample database operations to verify everything works"""
    print("\n6. Running sample operations...")
    
    try:
        from database_manager import DatabaseManager
        from data_mapper import DataMapper
        
        db = DatabaseManager()
        mapper = DataMapper()
        
        # Test data mapping
        sample_data = [{
            'Invoice No.': 'SETUP-TEST-001',
            'EMID': 'TEST-EMID',
            'Service Area': 'Test Setup Area',
            'Invoice Total': 100.00
        }]
        
        mapped_data = mapper.map_invoice_data(pd.DataFrame(sample_data))
        print("   ✅ Data mapping successful")
        
        # Test database insert
        result = db.upsert_invoices(mapped_data)
        print(f"   ✅ Database upsert successful: {result}")
        
        # Test database query
        search_result = db.search_invoices('SETUP-TEST-001')
        print(f"   ✅ Database search successful: {len(search_result)} records found")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Sample operations failed: {e}")
        return False

def main():
    """Main setup function"""
    print("🚀 DATABASE SYSTEM SETUP")
    print("=" * 50)
    print("This script will verify your database system setup and configuration.")
    print()
    
    # Run all checks
    checks = [
        check_python_version(),
        check_required_packages(),
        check_file_structure(),
        check_env_file(),
        test_database_connection(),
        run_sample_operations()
    ]
    
    # Summary
    print("\n" + "=" * 50)
    print("📋 SETUP SUMMARY")
    print("=" * 50)
    
    passed_checks = sum(checks)
    total_checks = len(checks)
    
    if passed_checks == total_checks:
        print("🎉 ALL CHECKS PASSED!")
        print()
        print("✅ Your database system is ready to use!")
        print()
        print("🚀 Next steps:")
        print("   1. Run the Streamlit app: streamlit run invoice_app_db.py")
        print("   2. Upload your Excel files through the web interface")
        print("   3. Or use the migration script for batch processing")
        print()
        print("📚 Available commands:")
        print("   - streamlit run invoice_app_db.py  (Start web app)")
        print("   - python migrate_excel_to_db.py -d /path/to/excel/files  (Batch migrate)")
        print("   - python test_database.py  (Test database anytime)")
        
    else:
        print(f"⚠️  {passed_checks}/{total_checks} checks passed")
        print()
        print("❌ Please fix the failed checks above before proceeding.")
        print()
        print("💡 Common solutions:")
        print("   - Install missing packages: pip install -r requirements.txt")
        print("   - Update .env file with correct database credentials")
        print("   - Check your NeonDB connection details")
        
    print("\n" + "=" * 50)

if __name__ == "__main__":
    # Import pandas here so we can check if it's installed first
    try:
        import pandas as pd
        main()
    except ImportError:
        print("❌ pandas not installed. Please install required packages first:")
        print("pip install pandas sqlalchemy psycopg2-binary python-dotenv streamlit openpyxl")
