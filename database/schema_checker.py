"""
Database Schema Checker
Check the actual structure of your invoice_details table
"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def check_database_schema():
    """Check the actual database schema for invoice_details table"""
    
    print("🔍 CHECKING DATABASE SCHEMA")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cursor = conn.cursor()
        
        # Check if invoice_details table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'invoice_details'
            );
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print("❌ invoice_details table does not exist!")
            print("💡 You need to create the table first")
            conn.close()
            return False
        
        print("✅ invoice_details table exists")
        
        # Get table structure
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = 'invoice_details'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        
        print(f"\n📋 Table Structure ({len(columns)} columns):")
        print("-" * 80)
        print(f"{'Column Name':<30} {'Data Type':<20} {'Nullable':<10} {'Default'}")
        print("-" * 80)
        
        for col_name, data_type, nullable, default in columns:
            default_str = str(default)[:20] if default else "None"
            print(f"{col_name:<30} {data_type:<20} {nullable:<10} {default_str}")
        
        # Check constraints
        print(f"\n🔒 Table Constraints:")
        cursor.execute("""
            SELECT con.conname, con.contype, con.consrc
            FROM pg_catalog.pg_constraint con
            INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
            WHERE rel.relname = 'invoice_details';
        """)
        
        constraints = cursor.fetchall()
        if constraints:
            for constraint_name, constraint_type, constraint_src in constraints:
                type_name = {
                    'p': 'PRIMARY KEY',
                    'f': 'FOREIGN KEY', 
                    'u': 'UNIQUE',
                    'c': 'CHECK'
                }.get(constraint_type, constraint_type)
                
                print(f"  {constraint_name}: {type_name}")
                if constraint_src:
                    print(f"    Source: {constraint_src}")
        else:
            print("  No constraints found")
        
        # Check indexes
        print(f"\n📊 Table Indexes:")
        cursor.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = 'invoice_details';
        """)
        
        indexes = cursor.fetchall()
        if indexes:
            for index_name, index_def in indexes:
                print(f"  {index_name}")
                print(f"    {index_def}")
        else:
            print("  No indexes found")
        
        # Test a simple insert to see what happens
        print(f"\n🧪 Testing Simple Insert:")
        try:
            cursor.execute("""
                INSERT INTO invoice_details (invoice_no, source_system, created_at)
                VALUES ('TEST-SCHEMA-CHECK', 'TEST', NOW())
                RETURNING id;
            """)
            
            test_id = cursor.fetchone()[0]
            print(f"  ✅ Simple insert successful (ID: {test_id})")
            
            # Clean up test record
            cursor.execute("DELETE FROM invoice_details WHERE id = %s", (test_id,))
            print(f"  🧹 Test record cleaned up")
            
        except Exception as e:
            print(f"  ❌ Simple insert failed: {e}")
        
        conn.commit()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking schema: {e}")
        return False

def check_sample_data_compatibility():
    """Check if our sample BCI data would be compatible"""
    
    print(f"\n🔍 CHECKING DATA COMPATIBILITY")
    print("=" * 50)
    
    # Sample BCI record structure (what we're trying to insert)
    sample_bci_record = {
        'invoice_no': '40009425',
        'source_system': 'BCI',
        'employee_id': '56159',
        'employee_name': 'John Doe',
        'work_date': '2024-06-01',
        'hours_regular': 8.0,
        'hours_overtime': 0.0,
        'hours_holiday': 0.0,
        'hours_total': 8.0,
        'rate_regular': 25.50,
        'rate_overtime': 0.0,
        'rate_holiday': 0.0,
        'amount_regular': 204.00,
        'amount_overtime': 0.0,
        'amount_holiday': 0.0,
        'amount_total': 204.00,
        'job_number': None,
        'customer_number': '12345',
        'po_number': None,
        'post_description': None,
        'pay_description': None,
        'shift_in': '08:00',
        'shift_out': '17:00',
        'lunch_hours': 1.0,
        'bill_category': 'Regular',
        'notes': None
    }
    
    print("📋 Sample BCI Record Structure:")
    for key, value in sample_bci_record.items():
        value_type = type(value).__name__
        print(f"  {key:<25} = {value} ({value_type})")
    
    return sample_bci_record

if __name__ == "__main__":
    schema_ok = check_database_schema()
    
    if schema_ok:
        sample_data = check_sample_data_compatibility()
        
        print(f"\n💡 NEXT STEPS:")
        print(f"1. Compare the table structure above with our INSERT statement")
        print(f"2. Look for missing columns or data type mismatches")
        print(f"3. Check if any constraints are being violated")
        print(f"4. The specific error should point to the problematic column")
    else:
        print(f"\n🚨 CRITICAL ISSUE:")
        print(f"Cannot proceed until database schema issues are resolved")
