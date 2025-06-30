import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_invoices_table():
    """Create the invoices table in PostgreSQL"""
    
    print("🏗️  Invoice Table Creation Script")
    print("="*60)
    
    # Get database URL
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL not found in .env file")
        database_url = input("\nEnter your DATABASE_URL: ").strip()
    
    try:
        # Connect to database
        print("\n🔌 Connecting to database...")
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        print("✅ Connected successfully")
        
        # Check if table already exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'invoices'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print("\n⚠️  Table 'invoices' already exists!")
            drop = input("Do you want to DROP and recreate it? (y/n): ").lower()
            if drop == 'y':
                cursor.execute("DROP TABLE invoices CASCADE;")
                print("🗑️  Dropped existing table")
            else:
                print("❌ Cancelled - table already exists")
                return
        
        # Create the table
        print("\n🏗️  Creating invoices table...")
        
        create_table_query = """
        CREATE TABLE invoices (
            -- Primary key
            id SERIAL PRIMARY KEY,
            
            -- Main invoice fields
            invoice_no VARCHAR(50) UNIQUE NOT NULL,
            emid VARCHAR(20),
            nuid VARCHAR(20),
            service_reqd_by VARCHAR(100),
            service_area VARCHAR(200),
            post_name VARCHAR(200),
            
            -- Date fields
            invoice_from DATE,
            invoice_to DATE,
            invoice_date DATE,
            edi_date DATE,
            release_date DATE,
            add_on_date DATE,
            
            -- Financial fields
            chartfield VARCHAR(50),
            invoice_total DECIMAL(12, 2) DEFAULT 0.00,
            
            -- Additional fields
            notes TEXT,
            not_transmitted BOOLEAN DEFAULT FALSE,
            invoice_no_history VARCHAR(50),
            
            -- Original date tracking
            original_edi_date DATE,
            original_add_on_date DATE,
            original_release_date DATE,
            
            -- Metadata
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_table_query)
        print("✅ Table created successfully")
        
        # Create indexes for better performance
        print("\n🔍 Creating indexes...")
        
        indexes = [
            ("idx_invoices_emid", "emid"),
            ("idx_invoices_service_area", "service_area"),
            ("idx_invoices_invoice_date", "invoice_date"),
            ("idx_invoices_edi_date", "edi_date"),
            ("idx_invoices_release_date", "release_date"),
        ]
        
        for index_name, column in indexes:
            cursor.execute(f"CREATE INDEX {index_name} ON invoices({column});")
            print(f"  ✅ Created index on {column}")
        
        # Create a trigger to update the updated_at timestamp
        print("\n⚙️  Creating update trigger...")
        
        cursor.execute("""
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ language 'plpgsql';
        """)
        
        cursor.execute("""
            CREATE TRIGGER update_invoices_updated_at 
            BEFORE UPDATE ON invoices 
            FOR EACH ROW 
            EXECUTE FUNCTION update_updated_at_column();
        """)
        
        print("✅ Trigger created")
        
        # Commit all changes
        conn.commit()
        
        # Show table structure
        print("\n📋 Table Structure:")
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'invoices'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print("\nColumn Name | Type | Max Length | Nullable")
        print("-" * 60)
        for col in columns:
            max_len = col[2] if col[2] else 'N/A'
            print(f"{col[0]:<20} | {col[1]:<15} | {max_len:<10} | {col[3]}")
        
        print("\n✅ Invoice table is ready for data!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        print("\n🔒 Database connection closed")

def show_sample_queries():
    """Show some useful SQL queries for the invoices table"""
    
    print("\n📚 Useful SQL Queries:")
    print("="*60)
    
    queries = {
        "Count all invoices": 
            "SELECT COUNT(*) FROM invoices;",
        
        "Recent invoices": 
            "SELECT invoice_no, emid, service_area, invoice_date, invoice_total\nFROM invoices\nORDER BY invoice_date DESC\nLIMIT 10;",
        
        "Invoices by service area":
            "SELECT service_area, COUNT(*) as count, SUM(invoice_total) as total\nFROM invoices\nGROUP BY service_area\nORDER BY count DESC;",
        
        "Find specific invoice":
            "SELECT * FROM invoices WHERE invoice_no = '16535899';",
        
        "Invoices pending release":
            "SELECT invoice_no, emid, service_area, edi_date\nFROM invoices\nWHERE release_date IS NULL\nORDER BY edi_date;",
    }
    
    for title, query in queries.items():
        print(f"\n-- {title}")
        print(query)

if __name__ == "__main__":
    create_invoices_table()
    show_sample_queries()
