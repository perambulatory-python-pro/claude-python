import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def create_invoice_details_tables():
    """Create tables for invoice details with unified schema"""
    
    print("🏗️  Creating Invoice Details Tables")
    print("="*60)
    
    try:
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cursor = conn.cursor()
        print("✅ Connected to database\n")
        
        # 1. Create unified invoice_details table
        print("📋 Creating invoice_details table...")
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS invoice_details (
            -- Primary key
            id SERIAL PRIMARY KEY,
            
            -- Link to master invoice
            invoice_no VARCHAR(50) NOT NULL,
            
            -- Source system
            source_system VARCHAR(10) NOT NULL CHECK (source_system IN ('BCI', 'AUS')),
            
            -- Common fields (available in both systems)
            work_date DATE,
            employee_id VARCHAR(50),
            employee_name VARCHAR(200),
            
            -- Location information
            location_code VARCHAR(50),
            location_name VARCHAR(200),
            building_code VARCHAR(50),
            emid VARCHAR(20),
            
            -- Position/Job information
            position_code VARCHAR(50),
            position_description VARCHAR(200),
            job_number VARCHAR(50),
            
            -- Hours and rates
            hours_regular DECIMAL(10, 2) DEFAULT 0,
            hours_overtime DECIMAL(10, 2) DEFAULT 0,
            hours_holiday DECIMAL(10, 2) DEFAULT 0,
            hours_total DECIMAL(10, 2) DEFAULT 0,
            
            rate_regular DECIMAL(10, 2) DEFAULT 0,
            rate_overtime DECIMAL(10, 2) DEFAULT 0,
            rate_holiday DECIMAL(10, 2) DEFAULT 0,
            
            amount_regular DECIMAL(12, 2) DEFAULT 0,
            amount_overtime DECIMAL(12, 2) DEFAULT 0,
            amount_holiday DECIMAL(12, 2) DEFAULT 0,
            amount_total DECIMAL(12, 2) DEFAULT 0,
            
            -- Additional BCI fields
            customer_number VARCHAR(50),
            customer_name VARCHAR(200),
            business_unit VARCHAR(50),
            shift_in TIME,
            shift_out TIME,
            
            -- Additional AUS fields
            bill_category VARCHAR(50),
            pay_rate DECIMAL(10, 2),
            in_time TIME,
            out_time TIME,
            lunch_hours DECIMAL(10, 2),
            
            -- Metadata
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Foreign key to invoices table
            CONSTRAINT fk_invoice_no 
                FOREIGN KEY (invoice_no) 
                REFERENCES invoices(invoice_no)
                ON DELETE CASCADE
        );
        """)
        
        print("✅ invoice_details table created")
        
        # 2. Create indexes for performance
        print("\n🔍 Creating indexes...")
        
        indexes = [
            ("idx_invoice_details_invoice_no", "invoice_no"),
            ("idx_invoice_details_work_date", "work_date"),
            ("idx_invoice_details_employee_id", "employee_id"),
            ("idx_invoice_details_emid", "emid"),
            ("idx_invoice_details_source", "source_system"),
            ("idx_invoice_details_composite", "invoice_no, work_date, employee_id")
        ]
        
        for index_name, columns in indexes:
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS {index_name} 
                ON invoice_details({columns});
            """)
            print(f"  ✅ Created index: {index_name}")
        
        # 3. Create summary view
        print("\n📊 Creating summary view...")
        
        cursor.execute("""
        CREATE OR REPLACE VIEW invoice_details_summary AS
        SELECT 
            i.invoice_no,
            i.emid,
            i.service_area,
            i.invoice_date,
            i.invoice_total,
            d.source_system,
            COUNT(DISTINCT d.employee_id) as employee_count,
            COUNT(*) as detail_line_count,
            SUM(d.hours_total) as total_hours,
            SUM(d.amount_total) as detail_total,
            i.invoice_total - COALESCE(SUM(d.amount_total), 0) as variance
        FROM invoices i
        LEFT JOIN invoice_details d ON i.invoice_no = d.invoice_no
        GROUP BY i.invoice_no, i.emid, i.service_area, i.invoice_date, 
                 i.invoice_total, d.source_system;
        """)
        
        print("✅ Summary view created")
        
        # 4. Create mapping tables for standardization
        print("\n🗺️ Creating mapping tables...")
        
        # Building mapping table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS building_mapping (
            id SERIAL PRIMARY KEY,
            source_system VARCHAR(10),
            source_code VARCHAR(100),
            building_code VARCHAR(50),
            emid VARCHAR(20),
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_system, source_code)
        );
        """)
        
        # Position mapping table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS position_mapping (
            id SERIAL PRIMARY KEY,
            source_system VARCHAR(10),
            source_position VARCHAR(200),
            standard_position_code VARCHAR(50),
            standard_position_desc VARCHAR(200),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_system, source_position)
        );
        """)
        
        print("✅ Mapping tables created")
        
        # Commit all changes
        conn.commit()
        
        # Show summary
        print("\n📋 Database objects created:")
        cursor.execute("""
            SELECT table_name, obj_description(pgc.oid, 'pg_class') as description
            FROM information_schema.tables t
            JOIN pg_catalog.pg_class pgc ON pgc.relname = t.table_name
            WHERE table_schema = 'public'
            AND table_name IN ('invoice_details', 'building_mapping', 'position_mapping')
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        for table, desc in tables:
            print(f"  • {table}")
        
        print("\n✅ All invoice details tables created successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def show_table_relationships():
    """Display the relationships between tables"""
    
    print("\n\n📊 Table Relationships:")
    print("="*60)
    print("""
    invoices (master)
         │
         ├─ invoice_no (PK)
         │
         ▼
    invoice_details
         ├─ invoice_no (FK) ──→ invoices.invoice_no
         ├─ source_system (BCI/AUS)
         ├─ employee/hours/amount details
         │
         ├─ building_code ──→ building_mapping
         └─ position_code ──→ position_mapping
    
    Mapping Tables:
    - building_mapping: Standardizes location codes
    - position_mapping: Standardizes job positions
    """)

if __name__ == "__main__":
    create_invoice_details_tables()
    show_table_relationships()
