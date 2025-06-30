# create_invoice_tables_neon.py
import psycopg2

# Neon connection string
DATABASE_URL = "postgresql://neondb_owner:npg_wm9SuUv0tqgH@ep-cold-cherry-a82w9a7i-pooler.eastus2.azure.neon.tech/neondb?sslmode=require"

def create_invoice_tables():
    """Create all tables for the invoice system"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        print("Creating tables...")
        
        # 1. Main invoice master table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS invoice_master (
                id SERIAL PRIMARY KEY,
                invoice_number VARCHAR(50) UNIQUE NOT NULL,
                emid VARCHAR(20),
                nuid VARCHAR(20),
                service_reqd_by VARCHAR(100),
                service_area VARCHAR(100),
                post_name VARCHAR(100),
                building_code VARCHAR(20),
                invoice_from DATE,
                invoice_to DATE,
                invoice_date DATE,
                edi_date DATE,
                release_date DATE,
                add_on_date DATE,
                chartfield VARCHAR(50),
                invoice_total DECIMAL(12,2),
                notes TEXT,
                not_transmitted BOOLEAN DEFAULT FALSE,
                invoice_no_history VARCHAR(50),
                original_edi_date DATE,
                original_add_on_date DATE,
                original_release_date DATE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
        
        # 2. Building dimension table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS building_dimension (
                building_code VARCHAR(20) PRIMARY KEY,
                building_name VARCHAR(100),
                emid VARCHAR(20),
                mc_service_area VARCHAR(100),
                business_unit VARCHAR(50),
                region VARCHAR(50),
                address TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
        
        # 3. EMID reference table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS emid_reference (
                emid VARCHAR(20) PRIMARY KEY,
                description VARCHAR(200),
                job_code VARCHAR(20),
                region VARCHAR(50),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
        
        # 4. Create indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_invoice_number ON invoice_master(invoice_number);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_emid ON invoice_master(emid);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_building_code ON invoice_master(building_code);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_invoice_date ON invoice_master(invoice_date);")
        
        # 5. Create update trigger
        cursor.execute("""
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ language 'plpgsql';
        """)
        
        cursor.execute("""
            DROP TRIGGER IF EXISTS update_invoice_master_updated_at ON invoice_master;
            CREATE TRIGGER update_invoice_master_updated_at 
            BEFORE UPDATE ON invoice_master 
            FOR EACH ROW 
            EXECUTE FUNCTION update_updated_at_column();
        """)
        
        conn.commit()
        print("✅ All tables created successfully!")
        
        # Show created tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        print("\n📋 Tables in your database:")
        for table in tables:
            print(f"   - {table[0]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error creating tables: {e}")

if __name__ == "__main__":
    create_invoice_tables()