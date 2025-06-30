import pandas as pd 
import psycopg2
import os
import openpyxl
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

print("Libraries imported successfully!")
print("Environment variables loaded!")

# Read the excel file
excel_file = 'emid_job_bu_table.xlsx'
sheet_name = 'emid_job_code'

try:
    # Read the specific sheet from the Excel file
    df = pd.read_excel(excel_file, sheet_name=sheet_name)

    print(f"Successfully read {len(df)} records from '{sheet_name}")
    print(f"\nColumns in the Excel file:")
    print(df.columns.tolist())

    # Let's see the first few rows to understand the data
    print(f"\nFirst 5 rows of data:")
    print(df.head())

    # Check if 'emid' and 'region' columns exist
    if 'emid' in df.columns:
        print(f"\n✓ Found 'emid' column")
    else:
        print(f"\n✗ Warning: 'emid' column not found!")

    if 'region' in df.columns:
        print(f"✓ Found 'region' column")
        # Let's see how many records have region data
        non_empty_regions = df['region'].notna().sum()
        print(f"  - {non_empty_regions} records have region data")
        print(f"  - {len(df) - non_empty_regions} records have empty regions")
    else:
        print(f"✗ Warning: 'region' column not found!")

except Exception as e:
    print(f"Error reading Excel file: {e}")

# Get database connection info from environment variables
# NeonDB usually provides a connection string or individual parameters

# First, let's see what environment variables you have
print("Checking for database environment variables...")

# Common patterns for database credentials in .env files:
if os.getenv('DATABASE_URL'):
    print("✓ Found DATABASE_URL")
    db_url = os.getenv('DATABASE_URL')
    # Connect using the full connection string
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        print("✓ Successfully connected to NeonDB!")
    except Exception as e:
        print(f"✗ Connection failed: {e}")
else:
    # Try individual parameters
    print("Looking for individual database parameters...")
    db_params = {
        'host': os.getenv('DB_HOST', os.getenv('PGHOST')),
        'database': os.getenv('DB_NAME', os.getenv('PGDATABASE')),
        'user': os.getenv('DB_USER', os.getenv('PGUSER')),
        'password': os.getenv('DB_PASSWORD', os.getenv('PGPASSWORD')),
        'port': os.getenv('DB_PORT', os.getenv('PGPORT', '5432'))
    }
    
    print(f"Host: {db_params['host']}")
    print(f"Database: {db_params['database']}")
    print(f"User: {db_params['user']}")
    print(f"Port: {db_params['port']}")
    
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        print("✓ Successfully connected to NeonDB!")
    except Exception as e:
        print(f"✗ Connection failed: {e}")

# Test the connection by getting the PostgreSQL version
if 'conn' in locals():
    cursor.execute('SELECT version()')
    version = cursor.fetchone()[0]
    print(f"\nConnected to: {version.split(',')[0]}")

# Check the current state of your emid_reference table
table_name = 'emid_reference'

# First, let's see what columns exist in your table
print(f"\nExamining the '{table_name}' table structure:")
cursor.execute("""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = %s
    ORDER BY ordinal_position;
""", (table_name,))

columns = cursor.fetchall()
print("\nTable columns:")
for col in columns:
    print(f"  - {col[0]}: {col[1]} (nullable: {col[2]})")

# Now let's check how many records exist and their current region status
print(f"\n--- Current state of {table_name} table ---")

# Count total records
cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
total_records = cursor.fetchone()[0]
print(f"Total records in table: {total_records}")

# Count records with empty regions
cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE region IS NULL OR region = ''")
empty_regions = cursor.fetchone()[0]
print(f"Records with empty regions: {empty_regions}")

# Let's see a sample of records with their current region values
print("\nSample of current records:")
cursor.execute(f"""
    SELECT emid, region 
    FROM {table_name} 
    ORDER BY emid 
    LIMIT 10
""")
sample_records = cursor.fetchall()
for record in sample_records:
    region_display = record[1] if record[1] else "[EMPTY]"
    print(f"  emid: {record[0]}, region: {region_display}")

# Check if all EMIDs from Excel exist in the database
print("\nChecking if Excel EMIDs exist in database...")
excel_emids = df['emid'].tolist()
emid_list = "','".join(excel_emids)  # Create list for SQL IN clause
cursor.execute(f"""
    SELECT COUNT(*) 
    FROM {table_name} 
    WHERE emid IN ('{emid_list}')
""")
matching_count = cursor.fetchone()[0]
print(f"EMIDs from Excel found in database: {matching_count} out of {len(excel_emids)}")

# Create a list to track our updates
updates_to_perform = []
no_match_records = []

print("\n--- Preview of Updates ---")
print("The following updates will be performed:\n")

# Go through each row in the Excel file
for index, row in df.iterrows():
    emid = row['emid']
    new_region = row['region']
    
    # Query the current region value in the database
    cursor.execute(f"""
        SELECT emid, region 
        FROM {table_name} 
        WHERE emid = %s
    """, (emid,))
    
    result = cursor.fetchone()
    
    if result:
        current_region = result[1] if result[1] else "[EMPTY]"
        updates_to_perform.append({
            'emid': emid,
            'new_region': new_region,
            'current_region': current_region
        })
        print(f"EMID {emid}: {current_region} → {new_region}")
    else:
        no_match_records.append(emid)
        print(f"WARNING: EMID {emid} not found in database!")

# Summary
print(f"\n--- Update Summary ---")
print(f"Total updates to perform: {len(updates_to_perform)}")
print(f"Records not found in database: {len(no_match_records)}")

if no_match_records:
    print(f"\nEMIDs not found: {no_match_records}")

# Ask for confirmation before proceeding
print("\n" + "="*50)
print("Ready to perform these updates?")
print("Type 'YES' to proceed with updates, or anything else to cancel")
print("="*50)

# Get user confirmation
user_input = input("\nYour response: ").strip().upper()

if user_input == 'YES':
    print("\n--- Executing Updates ---")
    
    # Counter for successful updates
    successful_updates = 0
    failed_updates = 0
    
    # Perform each update
    for update in updates_to_perform:
        emid = update['emid']
        new_region = update['new_region']
        
        try:
            # Execute the UPDATE query
            cursor.execute(f"""
                UPDATE {table_name}
                SET region = %s
                WHERE emid = %s
            """, (new_region, emid))
            
            # Check if the update affected a row
            if cursor.rowcount == 1:
                successful_updates += 1
                print(f"✓ Updated EMID {emid} to region {new_region}")
            else:
                failed_updates += 1
                print(f"✗ Warning: EMID {emid} update affected {cursor.rowcount} rows")
                
        except Exception as e:
            failed_updates += 1
            print(f"✗ Error updating EMID {emid}: {e}")
    
    # Commit all the changes
    print("\nCommitting changes to database...")
    conn.commit()
    print("✓ Changes committed successfully!")
    
    # Final summary
    print(f"\n--- Final Update Summary ---")
    print(f"✓ Successful updates: {successful_updates}")
    print(f"✗ Failed updates: {failed_updates}")