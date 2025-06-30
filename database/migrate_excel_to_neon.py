# migrate_excel_to_neon.py
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

DATABASE_URL = "postgresql://neondb_owner:npg_wm9SuUv0tqgH@ep-cold-cherry-a82w9a7i-pooler.eastus2.azure.neon.tech/neondb?sslmode=require"

def clean_column_name(col):
    """Clean column names for database compatibility"""
    return col.lower().replace(' ', '_').replace("'", "").replace('req_d', 'reqd')

def load_invoice_master(excel_file='invoice_master.xlsx'):
    """Load invoice master data from Excel to Neon"""
    try:
        print(f"📂 Reading {excel_file}...")
        df = pd.read_excel(excel_file)
        
        # Clean column names
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Convert date columns
        date_columns = ['invoice_from', 'invoice_to', 'invoice_date', 'edi_date', 
                       'release_date', 'add_on_date', 'original_edi_date', 
                       'original_add_on_date', 'original_release_date']
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Connect to database
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Prepare data for insertion
        columns = df.columns.tolist()
        records = df.to_dict('records')
        
        print(f"📤 Uploading {len(records)} invoices to Neon...")
        
        # Build insert query
        insert_query = f"""
            INSERT INTO invoice_master ({','.join(columns)})
            VALUES %s
            ON CONFLICT (invoice_number) 
            DO UPDATE SET {','.join([f'{col}=EXCLUDED.{col}' for col in columns if col != 'invoice_number'])}
        """
        
        # Convert records to tuples
        values = [tuple(record.get(col) for col in columns) for record in records]
        
        # Execute batch insert
        execute_values(cursor, insert_query, values)
        
        conn.commit()
        
        # Verify upload
        cursor.execute("SELECT COUNT(*) FROM invoice_master")
        count = cursor.fetchone()[0]
        
        print(f"✅ Successfully loaded {count} invoices into Neon!")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        import traceback
        traceback.print_exc()

def load_reference_data():
    """Load EMID and building reference data"""
    try:
        # Load EMID reference
        if os.path.exists('emid_job_bu_table.xlsx'):
            print("\n📂 Loading EMID reference data...")
            
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            
            # Load EMID sheet
            emid_df = pd.read_excel('emid_job_bu_table.xlsx', sheet_name='emid_job_code')
            
            for _, row in emid_df.iterrows():
                cursor.execute("""
                    INSERT INTO emid_reference (emid, description, job_code)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (emid) DO UPDATE
                    SET description = EXCLUDED.description,
                        job_code = EXCLUDED.job_code
                """, (row['emid'], row.get('description'), row.get('job_code')))
            
            print(f"   ✅ Loaded {len(emid_df)} EMID records")
            
            # Load buildings sheet
            buildings_df = pd.read_excel('emid_job_bu_table.xlsx', sheet_name='buildings')
            
            for _, row in buildings_df.iterrows():
                cursor.execute("""
                    INSERT INTO building_dimension (building_code, emid, mc_service_area, business_unit)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (building_code) DO UPDATE
                    SET emid = EXCLUDED.emid,
                        mc_service_area = EXCLUDED.mc_service_area,
                        business_unit = EXCLUDED.business_unit
                """, (row['building_code'], row['emid'], row['mc_service_area'], row.get('business_unit')))
            
            print(f"   ✅ Loaded {len(buildings_df)} building records")
            
            conn.commit()
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"❌ Error loading reference data: {e}")

if __name__ == "__main__":
    # First load reference data
    load_reference_data()
    
    # Then load invoice data
    load_invoice_master()