import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from tabulate import tabulate

load_dotenv()

def explore_database():
    """Explore current database structure and understand what we have"""
    
    print("🔍 Database Schema Explorer")
    print("="*60)
    
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cursor = conn.cursor()
        print("✅ Connected to database\n")
        
        # 1. List all tables
        print("📋 Current Tables in Database:")
        cursor.execute("""
            SELECT table_name, 
                   pg_size_pretty(pg_total_relation_size(table_schema||'.'||table_name)) as size
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        if tables:
            for table, size in tables:
                print(f"  • {table} ({size})")
        else:
            print("  No tables found")
        
        # 2. Explore invoices table structure
        print("\n📊 Invoices Table Structure:")
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'invoices'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print(tabulate(columns, headers=['Column', 'Type', 'Max Length', 'Nullable'], tablefmt='grid'))
        
        # 3. Sample invoice data
        print("\n📄 Sample Invoice Data:")
        cursor.execute("""
            SELECT invoice_no, emid, service_area, invoice_date, invoice_total
            FROM invoices
            LIMIT 5;
        """)
        
        sample = cursor.fetchall()
        print(tabulate(sample, headers=['Invoice No', 'EMID', 'Service Area', 'Date', 'Total'], tablefmt='grid'))
        
        # 4. Invoice statistics
        print("\n📈 Invoice Statistics:")
        cursor.execute("""
            SELECT 
                COUNT(*) as total_invoices,
                COUNT(DISTINCT emid) as unique_emids,
                COUNT(DISTINCT service_area) as unique_areas,
                MIN(invoice_date) as earliest_date,
                MAX(invoice_date) as latest_date,
                SUM(invoice_total) as total_amount
            FROM invoices;
        """)
        
        stats = cursor.fetchone()
        print(f"  Total Invoices: {stats[0]:,}")
        print(f"  Unique EMIDs: {stats[1]}")
        print(f"  Unique Service Areas: {stats[2]}")
        print(f"  Date Range: {stats[3]} to {stats[4]}")
        print(f"  Total Amount: ${stats[5]:,.2f}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

def check_invoice_details_files():
    """Check what invoice detail files we have available"""
    
    print("\n\n📁 Checking for Invoice Detail Files:")
    print("="*60)
    
    files_to_check = [
        ('invoice_details_bci.csv', 'BCI (Company) invoice details'),
        ('invoice_details_aus.csv', 'AUS (Subcontractor) invoice details'),
        ('invoice_details_bci.xlsx', 'BCI Excel format'),
        ('invoice_details_aus.xlsx', 'AUS Excel format')
    ]
    
    found_files = []
    
    for filename, description in files_to_check:
        if os.path.exists(filename):
            size = os.path.getsize(filename) / 1024 / 1024  # Size in MB
            print(f"  ✅ {filename} - {description} ({size:.2f} MB)")
            found_files.append(filename)
            
            # Show sample of the file
            try:
                if filename.endswith('.csv'):
                    df = pd.read_csv(filename, nrows=5)
                else:
                    df = pd.read_excel(filename, nrows=5)
                
                print(f"     Columns: {len(df.columns)}")
                print(f"     Sample columns: {list(df.columns)[:5]}...")
                
            except Exception as e:
                print(f"     ⚠️ Could not read file: {e}")
        else:
            print(f"  ❌ {filename} - Not found")
    
    return found_files

def analyze_invoice_details_structure():
    """Analyze the structure of BCI and AUS files"""
    
    print("\n\n📊 Analyzing Invoice Details Structure:")
    print("="*60)
    
    # Check BCI structure
    if os.path.exists('invoice_details_bci.csv'):
        print("\n🏢 BCI Invoice Details Structure:")
        bci_df = pd.read_csv('invoice_details_bci.csv', nrows=100)
        
        print(f"  Total columns: {len(bci_df.columns)}")
        print("\n  Column listing:")
        for i, col in enumerate(bci_df.columns):
            print(f"    [{i:2d}] {col}")
        
        print(f"\n  Sample data types:")
        print(bci_df.dtypes.head(10))
    
    # Check AUS structure  
    if os.path.exists('invoice_details_aus.csv'):
        print("\n\n🏢 AUS Invoice Details Structure:")
        aus_df = pd.read_csv('invoice_details_aus.csv', nrows=100)
        
        print(f"  Total columns: {len(aus_df.columns)}")
        print("\n  Column listing:")
        for i, col in enumerate(aus_df.columns):
            print(f"    [{i:2d}] {col}")
        
        print(f"\n  Sample data types:")
        print(aus_df.dtypes.head(10))
    
    # Compare structures
    if os.path.exists('invoice_details_bci.csv') and os.path.exists('invoice_details_aus.csv'):
        print("\n\n🔄 Comparing BCI vs AUS Structures:")
        print("="*40)
        
        bci_cols = set(bci_df.columns)
        aus_cols = set(aus_df.columns)
        
        common_cols = bci_cols.intersection(aus_cols)
        bci_only = bci_cols - aus_cols
        aus_only = aus_cols - bci_cols
        
        print(f"  Common columns: {len(common_cols)}")
        if common_cols:
            print(f"    Examples: {list(common_cols)[:5]}")
        
        print(f"\n  BCI-only columns: {len(bci_only)}")
        if bci_only:
            print(f"    Examples: {list(bci_only)[:5]}")
        
        print(f"\n  AUS-only columns: {len(aus_only)}")
        if aus_only:
            print(f"    Examples: {list(aus_only)[:5]}")

if __name__ == "__main__":
    # Explore current database
    explore_database()
    
    # Check for invoice detail files
    found_files = check_invoice_details_files()
    
    # Analyze structure if files found
    if found_files:
        analyze_invoice_details_structure()
    
    print("\n\n🎯 Next Steps:")
    print("  1. Create unified schema for invoice details")
    print("  2. Build transformation functions for BCI and AUS formats")
    print("  3. Create invoice_details table in PostgreSQL")
    print("  4. Migrate and link data to invoice master")
