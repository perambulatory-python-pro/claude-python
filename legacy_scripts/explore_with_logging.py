import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
import sys

load_dotenv()

class DualOutput:
    """Class to write output to both console and file"""
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, 'w', encoding='utf-8')
        
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        
    def flush(self):
        self.terminal.flush()
        self.log.flush()
        
    def close(self):
        self.log.close()

def explore_database_with_logging():
    """Explore database and automatically save output to file"""
    
    # Create output filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'database_exploration_{timestamp}.txt'
    
    # Set up dual output
    dual_output = DualOutput(output_file)
    old_stdout = sys.stdout
    sys.stdout = dual_output
    
    try:
        print(f"📝 Output is being saved to: {output_file}")
        print(f"📅 Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        # Run the exploration
        explore_database()
        found_files = check_invoice_details_files()
        if found_files:
            analyze_invoice_details_structure()
        
        print("\n\n✅ Output saved successfully!")
        
    finally:
        # Restore original stdout
        sys.stdout = old_stdout
        dual_output.close()
        print(f"\n✅ Results saved to: {output_file}")
        return output_file

def explore_database():
    """Explore current database structure"""
    
    print("\n🔍 Database Schema Explorer")
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
        print("-" * 60)
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'invoices'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print(f"{'Column':<25} {'Type':<15} {'Max Length':<10} {'Nullable'}")
        print("-" * 60)
        for col in columns:
            col_name = col[0]
            data_type = col[1]
            max_len = col[2] if col[2] else 'N/A'
            nullable = col[3]
            print(f"{col_name:<25} {data_type:<15} {str(max_len):<10} {nullable}")
        
        # 3. Sample invoice data
        print("\n📄 Sample Invoice Data:")
        print("-" * 80)
        cursor.execute("""
            SELECT invoice_no, emid, service_area, invoice_date, invoice_total
            FROM invoices
            LIMIT 5;
        """)
        
        sample = cursor.fetchall()
        print(f"{'Invoice No':<12} {'EMID':<6} {'Service Area':<30} {'Date':<12} {'Total':>12}")
        print("-" * 80)
        for row in sample:
            invoice = row[0]
            emid = row[1] or 'N/A'
            area = row[2][:28] + '..' if row[2] and len(row[2]) > 30 else row[2] or 'N/A'
            date = str(row[3]) if row[3] else 'N/A'
            total = f"${row[4]:,.2f}" if row[4] else '$0.00'
            print(f"{invoice:<12} {emid:<6} {area:<30} {date:<12} {total:>12}")
        
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
        import traceback
        traceback.print_exc()

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
        try:
            bci_df = pd.read_csv('invoice_details_bci.csv', nrows=100)
            
            print(f"  Total columns: {len(bci_df.columns)}")
            print("\n  Column listing:")
            for i, col in enumerate(bci_df.columns):
                print(f"    [{i:2d}] {col}")
            
            # Show sample data
            print("\n  Sample of first row:")
            first_row = bci_df.iloc[0]
            for col in ['Invoice_No', 'Employee_First_Name', 'Employee_Last_Name', 
                        'Date', 'Billed_Regular_Hours', ' Billed_Regular_Wages ']:
                if col in bci_df.columns:
                    print(f"    {col}: {first_row[col]}")
        except Exception as e:
            print(f"  ❌ Error reading BCI file: {e}")
    
    # Check AUS structure  
    if os.path.exists('invoice_details_aus.csv'):
        print("\n\n🏢 AUS Invoice Details Structure:")
        try:
            aus_df = pd.read_csv('invoice_details_aus.csv', nrows=100)
            
            print(f"  Total columns: {len(aus_df.columns)}")
            print("\n  Column listing:")
            for i, col in enumerate(aus_df.columns):
                print(f"    [{i:2d}] {col}")
            
            # Show sample data
            print("\n  Sample of first row:")
            first_row = aus_df.iloc[0]
            for col in ['Invoice Number', 'Employee Name', 'Work Date', 
                        'Hours', 'Bill Amount']:
                if col in aus_df.columns:
                    print(f"    {col}: {first_row[col]}")
        except Exception as e:
            print(f"  ❌ Error reading AUS file: {e}")
    
    # Compare structures
    if 'bci_df' in locals() and 'aus_df' in locals():
        print("\n\n🔄 Comparing BCI vs AUS Structures:")
        print("="*40)
        
        bci_cols = set(bci_df.columns)
        aus_cols = set(aus_df.columns)
        
        # Key field mappings
        print("\n  Key Field Mappings:")
        print("  BCI                          → AUS")
        print("  " + "-"*40)
        print("  Invoice_No                   → Invoice Number")
        print("  Employee_First/Last_Name     → Employee Name")
        print("  Date                         → Work Date")
        print("  Billed_Regular_Hours         → Hours")
        print("  Billed_Regular_Wages         → Bill Amount")
        
        print(f"\n  Total BCI columns: {len(bci_cols)}")
        print(f"  Total AUS columns: {len(aus_cols)}")

if __name__ == "__main__":
    # Run with automatic logging
    output_file = explore_database_with_logging()
    
    # Also create a simple summary file
    with open('quick_summary.txt', 'w') as f:
        f.write("Database Exploration Quick Summary\n")
        f.write("="*40 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Full output saved to: {output_file}\n\n")
        
        # Add quick stats if database is accessible
        try:
            conn = psycopg2.connect(os.getenv('DATABASE_URL'))
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM invoices")
            count = cursor.fetchone()[0]
            f.write(f"Total invoices in database: {count:,}\n")
            cursor.close()
            conn.close()
        except:
            f.write("Could not connect to database\n")
