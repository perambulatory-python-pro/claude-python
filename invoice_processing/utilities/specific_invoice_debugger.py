"""
Specific Invoice Failure Debugger
Deep dive into why specific invoices are failing to insert

Python Learning: Targeted debugging and data inspection techniques
"""

import pandas as pd
from capital_project_db_manager import CapitalProjectDBManager

def debug_specific_invoices():
    """
    Analyze the 4 specific invoices that are failing to insert
    """
    print("🔍 DEBUGGING SPECIFIC INVOICE FAILURES")
    print("=" * 50)
    
    failed_invoices = ['40011693', '40010754', '40010702', '40011229']
    
    # Load the CSV and examine these specific records
    csv_file = 'Blackstone Consulting DPs 06.24.2025 07 00 51 AM.csv'
    df = pd.read_csv(csv_file)
    
    print(f"📋 Analyzing {len(failed_invoices)} specific failing invoices...")
    
    for invoice_num in failed_invoices:
        print(f"\n🔍 Invoice: {invoice_num}")
        print("-" * 30)
        
        # Find this record in the CSV
        mask = df['Vendor Reference/Invoice Number'].astype(str) == str(invoice_num)
        record = df[mask]
        
        if len(record) == 0:
            print(f"   ❌ NOT FOUND in CSV!")
            continue
        elif len(record) > 1:
            print(f"   ⚠️  MULTIPLE RECORDS FOUND: {len(record)}")
        
        # Show all the data for this record
        record_data = record.iloc[0]
        print(f"   📄 Record details:")
        
        for col in df.columns:
            value = record_data[col]
            if pd.isna(value):
                print(f"      {col}: NULL")
            else:
                print(f"      {col}: {value} (type: {type(value).__name__})")
    
    print(f"\n🔧 MANUAL INSERT TEST")
    print("=" * 30)
    
    # Try to manually insert one of these records step by step
    test_invoice = failed_invoices[0]
    mask = df['Vendor Reference/Invoice Number'].astype(str) == str(test_invoice)
    test_record = df[mask].iloc[0]
    
    print(f"Testing manual insert for invoice: {test_invoice}")
    
    # Apply the same transformations our code does
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
    
    # Transform the record
    transformed_record = {}
    for csv_col, db_col in column_mapping.items():
        if csv_col in test_record.index:
            value = test_record[csv_col]
            if pd.isna(value):
                transformed_record[db_col] = None
            else:
                transformed_record[db_col] = value
    
    print(f"\n📝 Transformed record:")
    for key, value in transformed_record.items():
        print(f"   {key}: {value} (type: {type(value).__name__})")
    
    # Try to insert manually with detailed error catching
    manager = CapitalProjectDBManager()
    
    try:
        print(f"\n🚀 Attempting manual database insert...")
        
        with manager.get_connection() as conn:
            with conn.cursor() as cursor:
                # Try the exact same INSERT that should work
                cursor.execute("""
                    INSERT INTO capital_project_trimble_tracking (
                        invoice_number,
                        current_step,
                        current_step_date,
                        trimble_date_created,
                        status,
                        project_number,
                        payment_reference,
                        onelink_voucher_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, invoice_number
                """, (
                    str(transformed_record.get('invoice_number')),
                    transformed_record.get('current_step'),
                    pd.to_datetime(transformed_record.get('current_step_date'), errors='coerce') if transformed_record.get('current_step_date') else None,
                    pd.to_datetime(transformed_record.get('trimble_date_created'), errors='coerce') if transformed_record.get('trimble_date_created') else None,
                    transformed_record.get('status'),
                    transformed_record.get('project_number'),
                    None,  # payment_reference is NULL anyway
                    transformed_record.get('onelink_voucher_id')
                ))
                
                result = cursor.fetchone()
                if result:
                    print(f"   ✅ SUCCESS! Inserted with ID: {result[0]}")
                    # Rollback since this is just a test
                    conn.rollback()
                else:
                    print(f"   ❌ INSERT returned no result")
                    conn.rollback()
                    
    except Exception as e:
        print(f"   ❌ Manual insert failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        
        # Try to get more details about the error
        if hasattr(e, 'pgcode'):
            print(f"   PostgreSQL error code: {e.pgcode}")
        if hasattr(e, 'pgerror'):
            print(f"   PostgreSQL error: {e.pgerror}")
    
    finally:
        manager.close()

def check_table_constraints():
    """
    Check if there are any table constraints that might be causing issues
    """
    print(f"\n🔍 CHECKING TABLE CONSTRAINTS")
    print("=" * 40)
    
    manager = CapitalProjectDBManager()
    
    try:
        # Check table structure and constraints
        with manager.get_connection() as conn:
            with conn.cursor() as cursor:
                # Get table definition
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_name = 'capital_project_trimble_tracking'
                    ORDER BY ordinal_position
                """)
                
                columns = cursor.fetchall()
                print("📋 Table structure:")
                for col in columns:
                    print(f"   {col[0]}: {col[1]} (nullable: {col[2]}) default: {col[3]}")
                
                # Check constraints
                cursor.execute("""
                    SELECT constraint_name, constraint_type
                    FROM information_schema.table_constraints
                    WHERE table_name = 'capital_project_trimble_tracking'
                """)
                
                constraints = cursor.fetchall()
                print(f"\n🔒 Table constraints:")
                for constraint in constraints:
                    print(f"   {constraint[0]}: {constraint[1]}")
                
                # Check indexes
                cursor.execute("""
                    SELECT indexname, indexdef
                    FROM pg_indexes
                    WHERE tablename = 'capital_project_trimble_tracking'
                """)
                
                indexes = cursor.fetchall()
                print(f"\n📊 Table indexes:")
                for index in indexes:
                    print(f"   {index[0]}: {index[1]}")
    
    except Exception as e:
        print(f"   ❌ Error checking constraints: {e}")
    
    finally:
        manager.close()

if __name__ == "__main__":
    debug_specific_invoices()
    check_table_constraints()
