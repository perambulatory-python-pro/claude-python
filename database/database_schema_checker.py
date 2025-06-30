"""
Database Schema Checker
This script will show you the exact column names in your invoice_details table
"""

import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def check_database_schema():
    """Check the exact schema of the invoice_details table"""
    
    print("🔍 DATABASE SCHEMA CHECKER")
    print("=" * 60)
    
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        # Get column information
        cur.execute("""
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_name = 'invoice_details'
            ORDER BY ordinal_position;
        """)
        
        columns = cur.fetchall()
        
        print("\nCOLUMNS IN invoice_details TABLE:")
        print("-" * 60)
        print(f"{'Column Name':<30} {'Type':<20} {'Nullable':<10}")
        print("-" * 60)
        
        for col_name, data_type, max_length, nullable, default in columns:
            type_str = data_type
            if max_length:
                type_str += f"({max_length})"
            print(f"{col_name:<30} {type_str:<20} {nullable:<10}")
        
        print("-" * 60)
        print(f"Total columns: {len(columns)}")
        
        # Close connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_database_schema()
