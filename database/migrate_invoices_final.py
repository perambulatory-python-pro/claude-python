import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def connect_to_database():
    """Create database connection"""
    try:
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            print("⚠️  DATABASE_URL not found in .env file")
            database_url = input("Enter your DATABASE_URL: ").strip()
        
        conn = psycopg2.connect(database_url)
        print("✅ Connected to database")
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def clean_value(value):
    """Clean and prepare value for database insertion"""
    if pd.isna(value) or value == '' or str(value).upper() in ['NAN', 'NAT', 'NONE']:
        return None
    if isinstance(value, (int, float)) and not np.isnan(value):
        return value
    return str(value).strip()

def clean_invoice_number(value):
    """Specifically clean invoice numbers and ensure they're strings"""
    if pd.isna(value) or value == '' or str(value).upper() in ['NAN', 'NAT', 'NONE']:
        return None
    
    # Convert to string and strip whitespace
    invoice_str = str(value).strip()
    
    # Remove .0 if it's there from float conversion
    if invoice_str.endswith('.0'):
        invoice_str = invoice_str[:-2]
    
    return invoice_str

def parse_date(date_value):
    """Parse various date formats"""
    if pd.isna(date_value) or date_value is None:
        return None
    
    # Handle string representations of null
    if isinstance(date_value, str) and date_value.upper() in ['NAT', 'NAN', 'NONE', '']:
        return None
    
    if isinstance(date_value, pd.Timestamp):
        return date_value.date()
    
    if isinstance(date_value, datetime):
        return date_value.date()
    
    try:
        return pd.to_datetime(date_value).date()
    except:
        return None

def migrate_invoices():
    """Main migration function"""
    print("🚀 Starting Invoice Migration")
    print("="*60)
    
    # Connect to database
    conn = connect_to_database()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    # Read Excel file
    file_path = "invoice_master.xlsx"
    print(f"\n📁 Reading {file_path}...")
    
    try:
        # Read Excel with specific dtype for invoice number
        df = pd.read_excel(file_path, dtype={'Invoice No.': str})
        print(f"✅ Loaded {len(df)} rows")
        
        # Find the invoice column
        invoice_col = None
        for col in df.columns:
            if 'Invoice No' in col:
                invoice_col = col
                break
        
        if not invoice_col:
            print("❌ Could not find invoice column!")
            print(f"Available columns: {list(df.columns)}")
            return
        
        print(f"\n✅ Using invoice column: '{invoice_col}'")
        
        # Clean invoice numbers
        df[invoice_col] = df[invoice_col].apply(clean_invoice_number)
        
        # Check valid invoices
        valid_invoices = df[df[invoice_col].notna()]
        print(f"📊 Found {len(valid_invoices)} rows with valid invoice numbers")
        
        # Show sample
        print("\n📄 Sample data (first 5 rows):")
        sample_cols = [col for col in ['EMID', invoice_col, 'Service Area', 'Invoice Total'] 
                      if col in df.columns]
        sample_df = df[sample_cols].head()
        print(sample_df.to_string())
        
        # Show data types
        print(f"\n📊 Invoice number samples:")
        for idx, inv in enumerate(df[invoice_col].dropna().head(10)):
            print(f"  {idx+1}: '{inv}' (type: {type(inv).__name__})")
        
        # Prepare for insertion
        inserted_count = 0
        skipped_count = 0
        error_count = 0
        batch_size = 100
        batch_data = []
        
        print("\n🔄 Processing invoices...")
        
        for idx, row in df.iterrows():
            try:
                invoice_no = clean_invoice_number(row.get(invoice_col))
                
                if not invoice_no:
                    skipped_count += 1
                    continue
                
                # Check if exists - WITH EXPLICIT STRING CASTING
                cursor.execute(
                    "SELECT 1 FROM invoices WHERE invoice_no = %s::varchar",
                    (invoice_no,)
                )
                
                if cursor.fetchone():
                    skipped_count += 1
                    continue
                
                # Prepare data
                data = (
                    invoice_no,  # Already a string
                    clean_value(row.get('EMID')),
                    clean_value(row.get('NUID')),
                    clean_value(row.get('SERVICE REQ\'D BY')),
                    clean_value(row.get('Service Area')),
                    clean_value(row.get('Post Name')),
                    parse_date(row.get('Invoice From')),
                    parse_date(row.get('Invoice To')),
                    parse_date(row.get('Invoice Date')),
                    parse_date(row.get('EDI Date')),
                    parse_date(row.get('Release Date')),
                    parse_date(row.get('Add-On Date')),
                    clean_value(row.get('Chartfield')),
                    float(row.get('Invoice Total', 0)) if pd.notna(row.get('Invoice Total')) else 0.0,
                    clean_value(row.get('Notes')),
                    bool(row.get('Not Transmitted', False)),
                    clean_value(row.get('Invoice No. History')),
                    parse_date(row.get('Original EDI Date')),
                    parse_date(row.get('Original Add-On Date')),
                    parse_date(row.get('Original Release Date'))
                )
                
                batch_data.append(data)
                
                # Insert in batches
                if len(batch_data) >= batch_size:
                    insert_batch(cursor, batch_data)
                    inserted_count += len(batch_data)
                    batch_data = []
                    conn.commit()
                    print(f"  ✅ Inserted {inserted_count} records so far...")
                    
            except Exception as e:
                error_count += 1
                if error_count <= 5:
                    print(f"  ❌ Error on row {idx}: {str(e)}")
                conn.rollback()
                batch_data = []  # Clear batch on error
                continue
        
        # Insert remaining batch
        if batch_data:
            insert_batch(cursor, batch_data)
            inserted_count += len(batch_data)
            conn.commit()
        
        # Print summary
        print("\n" + "="*60)
        print("📊 MIGRATION COMPLETE!")
        print("="*60)
        print(f"✅ Successfully inserted: {inserted_count:,}")
        print(f"⚠️  Skipped (duplicates/no invoice): {skipped_count:,}")
        print(f"❌ Errors: {error_count:,}")
        print(f"📊 Total rows processed: {len(df):,}")
        
        # Verify data in database
        if inserted_count > 0:
            print("\n📋 Verifying data in database...")
            cursor.execute("SELECT COUNT(*) FROM invoices")
            total_count = cursor.fetchone()[0]
            print(f"Total invoices in database: {total_count:,}")
            
            print("\n📄 Sample from database:")
            cursor.execute("""
                SELECT invoice_no, emid, service_area, invoice_date, invoice_total
                FROM invoices
                ORDER BY created_at DESC
                LIMIT 5
            """)
            
            rows = cursor.fetchall()
            print("\nInvoice No | EMID | Service Area | Date | Total")
            print("-" * 80)
            for row in rows:
                date_str = str(row[3]) if row[3] else 'N/A'
                total_str = f"${row[4]:,.2f}" if row[4] else '$0.00'
                area = row[2][:30] + '...' if row[2] and len(row[2]) > 30 else row[2] or 'N/A'
                print(f"{row[0]:<12} | {row[1] or 'N/A':<4} | {area:<33} | {date_str:<10} | {total_str}")
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
        print("\n🔒 Database connection closed")

def insert_batch(cursor, batch_data):
    """Insert a batch of records using execute_values for better performance"""
    query = """
    INSERT INTO invoices (
        invoice_no, emid, nuid, service_reqd_by, service_area,
        post_name, invoice_from, invoice_to, invoice_date,
        edi_date, release_date, add_on_date, chartfield,
        invoice_total, notes, not_transmitted, invoice_no_history,
        original_edi_date, original_add_on_date, original_release_date
    ) VALUES %s
    ON CONFLICT (invoice_no) DO NOTHING
    """
    
    execute_values(cursor, query, batch_data)

if __name__ == "__main__":
    migrate_invoices()
