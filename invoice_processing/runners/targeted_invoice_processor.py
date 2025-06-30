"""
Targeted Invoice Processor
Process specific failing invoices with maximum error visibility

Python Learning: Precise error handling and debugging techniques
"""

import pandas as pd
import logging
from capital_project_db_manager import CapitalProjectDBManager

# Set up detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_specific_invoices_with_extreme_detail():
    """
    Process the 4 failing invoices with maximum error visibility
    """
    print("🎯 TARGETED PROCESSING OF FAILING INVOICES")
    print("=" * 50)
    
    failed_invoices = ['40011693', '40010754', '40010702', '40011229']
    
    # Load the CSV
    csv_file = 'Blackstone Consulting DPs 06.24.2025 07 00 51 AM.csv'
    df = pd.read_csv(csv_file)
    
    manager = CapitalProjectDBManager()
    
    try:
        for i, invoice_num in enumerate(failed_invoices):
            print(f"\n🔄 Processing Invoice {i+1}/4: {invoice_num}")
            print("-" * 40)
            
            # Get this specific record
            mask = df['Vendor Reference/Invoice Number'].astype(str) == str(invoice_num)
            record_df = df[mask]
            
            if len(record_df) == 0:
                print(f"   ❌ Invoice {invoice_num} not found in CSV!")
                continue
            
            record = record_df.iloc[0]
            print(f"   📋 Found record at CSV row: {record_df.index[0]}")
            
            # Apply EXACT same transformation as main code
            print(f"   🔄 Applying transformations...")
            
            column_mapping = {
                'Vendor Reference/Invoice Number': 'invoice_number',
                'Date Created': 'trimble_date_created',
                'Current Step': 'current_step',
                'Status': 'status',
                'Step Date Created': 'current_step_date',
                'Project Number': 'project_number',
                'Payment Reference': 'payment_reference',
                'OneLink Voucher ID': 'onelink_voucher_id'
            }
            
            # Create transformed record
            transformed = {}
            for csv_col, db_col in column_mapping.items():
                if csv_col in record.index:
                    value = record[csv_col]
                    if pd.isna(value):
                        transformed[db_col] = None
                        print(f"      {db_col}: NULL")
                    else:
                        transformed[db_col] = value
                        print(f"      {db_col}: {value}")
            
            # Handle date conversion exactly like main code
            print(f"   📅 Converting dates...")
            date_columns = ['trimble_date_created', 'current_step_date']
            for date_col in date_columns:
                if date_col in transformed and transformed[date_col] is not None:
                    try:
                        original_value = transformed[date_col]
                        converted_date = pd.to_datetime(transformed[date_col], errors='coerce')
                        transformed[date_col] = converted_date
                        print(f"      {date_col}: '{original_value}' → {converted_date}")
                        
                        if pd.isna(converted_date):
                            print(f"      ⚠️  Date conversion failed for {date_col}")
                    except Exception as e:
                        print(f"      ❌ Date conversion error for {date_col}: {e}")
                        transformed[date_col] = None
            
            # Handle payment reference exactly like main code
            if 'payment_reference' in transformed:
                if transformed['payment_reference'] is not None:
                    try:
                        payment_ref = pd.to_numeric(transformed['payment_reference'], errors='coerce')
                        max_bigint = 9223372036854775807
                        if payment_ref > max_bigint:
                            print(f"      ⚠️  Payment reference {payment_ref} exceeds BIGINT, setting to NULL")
                            transformed['payment_reference'] = None
                        else:
                            transformed['payment_reference'] = payment_ref
                    except:
                        transformed['payment_reference'] = None
                else:
                    print(f"      payment_reference: NULL (already)")
            
            # Now attempt the upsert with extreme error handling
            print(f"   💾 Attempting database upsert...")
            
            try:
                # Use the exact same upsert logic as the main code
                result = manager.upsert_trimble_tracking(transformed)
                print(f"   ✅ SUCCESS: {result}")
                
            except Exception as e:
                print(f"   ❌ UPSERT FAILED: {e}")
                print(f"   Error type: {type(e).__name__}")
                
                # Get detailed error info
                if hasattr(e, 'pgcode'):
                    print(f"   PostgreSQL error code: {e.pgcode}")
                if hasattr(e, 'pgerror'):
                    print(f"   PostgreSQL error message: {e.pgerror}")
                
                # Try a direct SQL insert to isolate the issue
                print(f"   🔧 Trying direct SQL insert...")
                try:
                    with manager.get_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                INSERT INTO capital_project_trimble_tracking (
                                    invoice_number, current_step, current_step_date,
                                    trimble_date_created, status, project_number,
                                    payment_reference, onelink_voucher_id
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                RETURNING id
                            """, (
                                str(transformed['invoice_number']),
                                transformed.get('current_step'),
                                transformed.get('current_step_date'),
                                transformed.get('trimble_date_created'),
                                transformed.get('status'),
                                transformed.get('project_number'),
                                transformed.get('payment_reference'),
                                transformed.get('onelink_voucher_id')
                            ))
                            
                            result = cursor.fetchone()
                            if result:
                                print(f"   ✅ Direct SQL SUCCESS: ID {result[0]}")
                                conn.commit()
                            else:
                                print(f"   ❌ Direct SQL returned no result")
                                conn.rollback()
                                
                except Exception as sql_error:
                    print(f"   ❌ Direct SQL also failed: {sql_error}")
                    if hasattr(sql_error, 'pgcode'):
                        print(f"   SQL error code: {sql_error.pgcode}")
                    if hasattr(sql_error, 'pgerror'):
                        print(f"   SQL error message: {sql_error.pgerror}")
    
    finally:
        manager.close()
    
    # Check final database state
    print(f"\n📊 FINAL DATABASE CHECK")
    print("=" * 30)
    
    manager = CapitalProjectDBManager()
    try:
        total_records = pd.read_sql(
            "SELECT COUNT(*) as count FROM capital_project_trimble_tracking", 
            manager.engine
        )['count'].iloc[0]
        
        target_records = pd.read_sql("""
            SELECT invoice_number, current_step, status 
            FROM capital_project_trimble_tracking 
            WHERE invoice_number = ANY(%s)
            ORDER BY invoice_number
        """, manager.engine, params=[failed_invoices])
        
        print(f"Total records in database: {total_records}")
        print(f"Target invoices found: {len(target_records)}")
        
        if len(target_records) > 0:
            print("✅ Successfully inserted target invoices:")
            for _, row in target_records.iterrows():
                print(f"   {row['invoice_number']}: {row['current_step']} - {row['status']}")
        else:
            print("❌ Target invoices still not in database")
    
    finally:
        manager.close()

if __name__ == "__main__":
    process_specific_invoices_with_extreme_detail()
