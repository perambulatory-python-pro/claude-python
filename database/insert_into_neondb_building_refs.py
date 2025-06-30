import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

print("Libraries imported successfully!")
print("Environment variables loaded!")

# Read the excel file
excel_file = '2025_Master Lookup_Validation Location with GL Reference_V3.xlsx'
sheet_name = 'Buildings'

try:
    # Read the specific sheet from the Excel file
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    
    print(f"Successfully read {len(df)} records from '{sheet_name}'")
    print(f"\nColumns in the Excel file:")
    print(df.columns.tolist())
    
    # Check if required columns exist using set operations
    required_columns = {'Building Code', 'Building Name', 'Building Address', 'City', 'State', 'Zip Code'}
    existing_columns = set(df.columns)
    
    if required_columns.issubset(existing_columns):
        print("\n✓ All required columns found!")
    else:
        missing = required_columns - existing_columns
        print(f"\n✗ Missing columns: {missing}")
        print("Cannot proceed without all required columns!")
        exit(1)  # Stop the script if columns are missing
        
    # Show first few rows
    print(f"\nFirst 5 rows of data:")
    print(df.head())
    
    # Show preview of address concatenation
    print("\nPreview of address concatenation:")
    for i in range(min(3, len(df))):
        row = df.iloc[i]
        preview_address = f"{row['Building Address']}, {row['City']}, {row['State']} {row['Zip Code']}"
        print(f"  {row['Building Code']}: {preview_address}")
        
except Exception as e:
    print(f"Error reading Excel file: {e}")
    exit(1)

# Connect to database
print("\n--- Connecting to Database ---")
if os.getenv('DATABASE_URL'):
    db_url = os.getenv('DATABASE_URL')
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        print("✓ Successfully connected to NeonDB!")
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        exit(1)
else:
    # Try individual parameters
    db_params = {
        'host': os.getenv('DB_HOST'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT', '5432')
    }
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        print("✓ Successfully connected to NeonDB!")
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        exit(1)

# Check the building_dimension table
table_name = 'building_dimension'
print(f"\n--- Examining '{table_name}' table ---")

# Check columns in the table
cursor.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = %s 
    ORDER BY ordinal_position;
""", (table_name,))

columns = cursor.fetchall()
print("\nTable columns:")
for col in columns:
    print(f"  - {col[0]}: {col[1]}")

# Check which building codes from Excel exist in database
print("\n--- Checking Existing Records ---")
excel_building_codes = df['Building Code'].tolist()

# Get existing building codes from database
cursor.execute(f"""
    SELECT building_code 
    FROM {table_name} 
    WHERE building_code = ANY(%s)
""", (excel_building_codes,))
existing_codes = [row[0] for row in cursor.fetchall()]

print(f"\nBuilding codes in Excel: {len(excel_building_codes)}")
print(f"Building codes that exist in database: {len(existing_codes)}")
print(f"Building codes that DON'T exist in database: {len(excel_building_codes) - len(existing_codes)}")

# Prepare updates (ONLY for existing records)
print("\n--- Preparing Updates ---")
updates_to_perform = []
skipped_records = []

for index, row in df.iterrows():
    building_code = row['Building Code']
    building_name = row['Building Name']
    
    # Concatenate address parts with proper formatting
    address_parts = []
    if pd.notna(row['Building Address']):
        address_parts.append(str(row['Building Address']))
    if pd.notna(row['City']):
        address_parts.append(str(row['City']))
    
    # Combine state and zip
    state_zip = []
    if pd.notna(row['State']):
        state_zip.append(str(row['State']))
    if pd.notna(row['Zip Code']):
        state_zip.append(str(row['Zip Code']))
    
    if state_zip:
        address_parts.append(' '.join(state_zip))
    
    # Join all parts with commas
    full_address = ', '.join(address_parts) if address_parts else ''
    
    # ONLY add to updates if this building code exists in database
    if building_code in existing_codes:
        updates_to_perform.append({
            'building_code': building_code,
            'building_name': building_name,
            'address': full_address
        })
    else:
        skipped_records.append(building_code)

print(f"\nRecords to UPDATE: {len(updates_to_perform)}")
print(f"Records to SKIP (not in database): {len(skipped_records)}")

if skipped_records:
    print(f"\nSkipped building codes: {skipped_records[:10]}...")  # Show first 10

# Show preview of updates
print("\n--- Preview of Updates ---")
print("First 10 updates to be performed:")
for update in updates_to_perform[:10]:
    print(f"  {update['building_code']}: {update['building_name']} | {update['address']}")

# Ask for confirmation
print("\n" + "="*60)
print(f"Ready to UPDATE {len(updates_to_perform)} existing records?")
print("Type 'YES' to proceed, or anything else to cancel")
print("="*60)

user_input = input("\nYour response: ").strip().upper()

if user_input == 'YES':
    print("\n--- Executing Updates ---")
    
    successful_updates = 0
    failed_updates = 0
    
    # Perform updates
    for update in updates_to_perform:
        try:
            cursor.execute(f"""
                UPDATE {table_name}
                SET building_name = %s,
                    address = %s
                WHERE building_code = %s
            """, (update['building_name'], update['address'], update['building_code']))
            
            if cursor.rowcount == 1:
                successful_updates += 1
                print(f"✓ Updated {update['building_code']}")
            else:
                failed_updates += 1
                print(f"✗ Warning: {update['building_code']} affected {cursor.rowcount} rows")
                
        except Exception as e:
            failed_updates += 1
            print(f"✗ Error updating {update['building_code']}: {e}")
    
    # Commit changes
    print("\nCommitting changes...")
    conn.commit()
    print("✓ Changes committed successfully!")
    
    # Summary
    print(f"\n--- Final Summary ---")
    print(f"✓ Successful updates: {successful_updates}")
    print(f"✗ Failed updates: {failed_updates}")
    print(f"⚠ Skipped (not in database): {len(skipped_records)}")
    
    # Verify a sample
    if updates_to_perform:
        print("\n--- Verification ---")
        sample_codes = [u['building_code'] for u in updates_to_perform[:5]]
        cursor.execute(f"""
            SELECT building_code, building_name, address 
            FROM {table_name}
            WHERE building_code = ANY(%s)
            ORDER BY building_code
        """, (sample_codes,))
        
        print("Sample of updated records:")
        for record in cursor.fetchall():
            print(f"  {record[0]}: {record[1]} | {record[2]}")
    
else:
    print("\nUpdate cancelled. No changes were made.")

# Close connection
cursor.close()
conn.close()
print("\n✓ Database connection closed.")