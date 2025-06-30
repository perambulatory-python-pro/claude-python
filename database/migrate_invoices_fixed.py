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
            raise ValueError("DATABASE_URL not found in .env file")
        
        conn = psycopg2.connect(database_url)
        print("✅ Connected to database")
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def clean_value(value):
    """Clean and prepare value for database insertion"""
    if pd.isna(value) or value == '' or value == 'nan' or value == 'NaN':
        return None
    if isinstance(value, (int, float)):
        if np.isnan(value):
            return None
        return value
    return str(value).strip()

def parse_date(date_value):
    """Parse various date formats"""
    if pd.isna(date_value) or date_value is None:
        return None
    
    # Handle the 'NaT' string that appears in your data
    if isinstance(date_value, str) and date_value.upper() in ['NAT', 'NAN']:
        return None
    
    if isinstance(date_value, pd.Timestamp):
        return date_value.date()
    
    if isinstance(date_value, datetime):
        return date_value.date()
        
    # Try parsing string dates
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
        df = pd.read_excel(file_path)
        print(f"✅ Loaded {len(df)} rows")
        
        # Debug: Show exact column names
        print("\n📋 Column names in file:")
        for i, col in enumerate(df.columns):
            print(f"  [{i}] '{col}'")
        
        # Find the invoice column - being very explicit
        invoice_col = None
        for col in df.columns:
            if 'Invoice No' in col:  # This will match 'Invoice No.' or 'Invoice No'
                invoice_col = col
                break
        
        if not invoice_col:
            print("❌ Could not find invoice column!")
            return
            
        print(f"\n✅ Using invoice column: '{invoice_col}'")
        
        # Check how many valid invoice numbers we have
        valid_invoices = df[df[invoice_col].notna() & (df[invoice_col] != 'NaN')]
        print(f"📊 Found {len(valid_invoices)} rows with valid invoice numbers")
        
        # Show sample data
        print("\n📄 Sample data:")
        sample_cols = [col for col in ['EMID', invoice_col, 'Service Area', 'Invoice Total'] if col in df.columns]
        print(df[sample_cols].head(5).to_string())
        
        # Prepare for insertion
        inserted_count = 0
        skipped_count = 0
        error_count = 0
        
        # Process each row
        print("\n🔄 Processing rows...")
        for idx, row in df.iterrows():
            try:
                invoice_no = clean_value(row[invoice_col])
                
                # Skip if no invoice number
                if not invoice_no:
                    skipped_count += 1
                    continue
                
                # Check if invoice already exists
                cursor.execute(
                    "SELECT 1 FROM invoices WHERE invoice_no = %s",
                    (invoice_no,)
                )
                
                if cursor.fetchone():
                    skipped_count += 1
                    continue
                
                # Prepare data for insertion
                data = {
                    'invoice_no': invoice_no,
                    'emid': clean_value(row.get('EMID')),
                    'nuid': clean_value(row.get('NUID')),
                    'service_reqd_by': clean_value(row.get('SERVICE REQ\'D BY')),
                    'service_area': clean_value(row.get('Service Area')),
                    'post_name': clean_value(row.get('Post Name')),
                    'invoice_from': parse_date(row.get('Invoice From')),
                    'invoice_to': parse_date(row.get('Invoice To')),
                    'invoice_date': parse_date(row.get('Invoice Date')),
                    'edi_date': parse_date(row.get('EDI Date')),
                    'release_date': parse_date(row.get('Release Date')),
                    'add_on_date': parse_date(row.get('Add-On Date')),
                    'chartfield': clean_value(row.get('Chartfield')),
                    'invoice_total': float(row.get('Invoice Total', 0)) if pd.notna(row.get('Invoice Total')) else 0.0,
                    'notes': clean_value(row.get('Notes')),
                    'not_transmitted': bool(row.get('Not Transmitted', False)),
                    'invoice_no_history': clean_value(row.get('Invoice No. History')),
                    'original_edi_date': parse_date(row.get('Original EDI Date')),
                    'original_add_on_date': parse_date(row.get('Original Add-On Date')),
                    'original_release_date': parse_date(row.get('Original Release Date'))
                }
                
                # Insert into database
                cursor.execute("""
                    INSERT INTO invoices (
                        invoice_no, emid, nuid, service_reqd_by, service_area,
                        post_name, invoice_from, invoice_to, invoice_date,
                        edi_date, release_date, add_on_date, chartfield,
                        invoice_total, notes, not_transmitted, invoice_no_history,
                        original_edi_date, original_add_on_date, original_release_date
                    ) VALUES (
                        %(invoice_no)s, %(emid)s, %(nuid)s, %(service_reqd_by)s, %(service_area)s,
                        %(post_name)s, %(invoice_from)s, %(invoice_to)s, %(invoice_date)s,
                        %(edi_date)s, %(release_date)s, %(add_on_date)s, %(chartfield)s,
                        %(invoice_total)s, %(notes)s, %(not_transmitted)s, %(invoice_no_history)s,
                        %(original_edi_date)s, %(original_add_on_date)s, %(original_release_date)s
                    )
                """, data)
                
                inserted_count += 1
                
                # Show progress every 100 records
                if inserted_count % 100 == 0:
                    print(f"  ✅ Inserted {inserted_count} records...")
                    conn.commit()  # Commit in batches
                    
            except Exception as e:
                error_count += 1
                print(f"  ❌ Error on row {idx} (Invoice: {invoice_no}): {str(e)}")
                conn.rollback()  # Rollback this transaction
                continue
        
        # Final commit
        conn.commit()
        
        # Print summary
        print("\n" + "="*60)
        print("📊 MIGRATION COMPLETE!")
        print("="*60)
        print(f"✅ Successfully inserted: {inserted_count:,}")
        print(f"⚠️  Skipped (duplicates/no invoice): {skipped_count:,}")
        print(f"❌ Errors: {error_count:,}")
        print(f"📊 Total processed: {len(df):,}")
        
        # Show sample from database
        if inserted_count > 0:
            print("\n📋 Sample data from database:")
            cursor.execute("""
                SELECT invoice_no, emid, service_area, invoice_date, invoice_total
                FROM invoices
                ORDER BY created_at DESC
                LIMIT 5
            """)
            
            rows = cursor.fetchall()
            print("\nInvoice No | EMID | Service Area | Date | Total")
            print("-" * 70)
            for row in rows:
                date_str = str(row[3]) if row[3] else 'N/A'
                total_str = f"${row[4]:,.2f}" if row[4] else '$0.00'
                print(f"{row[0]} | {row[1]} | {row[2][:30]}... | {date_str} | {total_str}")
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
        print("\n🔒 Database connection closed")

if __name__ == "__main__":
    migrate_invoices()
