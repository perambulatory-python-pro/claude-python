# migrate_invoice_safe.py
import pandas as pd
import psycopg2
import numpy as np
from datetime import datetime

DATABASE_URL = "postgresql://neondb_owner:npg_wm9SuUv0tqgH@ep-cold-cherry-a82w9a7i-pooler.eastus2.azure.neon.tech/neondb?sslmode=require"

def safe_migrate():
    """Safely migrate invoice data one row at a time"""
    try:
        # Read Excel file
        print("📂 Reading invoice_master.xlsx...")
        df = pd.read_excel('invoice_master.xlsx')
        print(f"✅ Found {len(df)} invoices")
        
        # Clean column names
        def clean_name(col):
            return col.lower().replace(' ', '_').replace("'", "").replace('-', '_')
        
        df.columns = [clean_name(col) for col in df.columns]
        
        # Connect to database
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Get list of columns that exist in both DataFrame and database
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'invoice_master' 
            AND column_name NOT IN ('id', 'created_at', 'updated_at')
        """)
        
        db_columns = [row[0] for row in cursor.fetchall()]
        matching_columns = [col for col in df.columns if col in db_columns]
        
        print(f"\n📋 Will insert these columns: {matching_columns}")
        
        # Process dates
        date_cols = ['invoice_from', 'invoice_to', 'invoice_date', 'edi_date', 
                    'release_date', 'add_on_date', 'original_edi_date', 
                    'original_add_on_date', 'original_release_date']
        
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Replace NaN with None
        df = df.replace({np.nan: None})
        
        # Insert each row
        success = 0
        errors = 0
        
        print("\n📤 Inserting invoices...")
        
        for idx, row in df.iterrows():
            try:
                # Build the INSERT statement for this row
                columns = []
                values = []
                
                for col in matching_columns:
                    if col in row:
                        columns.append(col)
                        values.append(row[col])
                
                if 'invoice_number' not in columns:
                    print(f"⚠️  Skipping row {idx}: No invoice number")
                    continue
                
                # Create the query
                placeholders = ','.join(['%s'] * len(columns))
                update_set = ','.join([f"{col}=EXCLUDED.{col}" for col in columns if col != 'invoice_number'])
                
                query = f"""
                    INSERT INTO invoice_master ({','.join(columns)})
                    VALUES ({placeholders})
                    ON CONFLICT (invoice_number) DO UPDATE SET {update_set}
                """
                
                cursor.execute(query, values)
                success += 1
                
                # Progress update
                if success % 500 == 0:
                    conn.commit()  # Commit periodically
                    print(f"   Progress: {success} invoices inserted...")
                    
            except Exception as e:
                errors += 1
                print(f"❌ Error on row {idx}: {str(e)[:100]}")
                if errors > 20:
                    print("Too many errors, stopping...")
                    break
        
        # Final commit
        conn.commit()
        
        # Show results
        cursor.execute("SELECT COUNT(*) FROM invoice_master")
        total = cursor.fetchone()[0]
        
        print(f"\n✅ Migration Complete!")
        print(f"   Successfully inserted: {success}")
        print(f"   Errors: {errors}")
        print(f"   Total in database: {total}")
        
        # Show sample
        print("\n📊 Sample data:")
        cursor.execute("""
            SELECT invoice_number, invoice_date, invoice_total 
            FROM invoice_master 
            ORDER BY invoice_date DESC NULLS LAST
            LIMIT 5
        """)
        
        for row in cursor.fetchall():
            print(f"   {row}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    safe_migrate()