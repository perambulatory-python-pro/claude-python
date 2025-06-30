import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

class FixedInvoiceDetailsTransformer:
    """Fixed transformer that properly handles NaT and invalid dates"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.stats = {
            'bci_processed': 0,
            'aus_processed': 0,
            'bci_skipped': 0,
            'aus_skipped': 0,
            'errors': [],
            'warnings': [],
            'missing_invoices': set()
        }
    
    def connect_db(self):
        """Connect to database"""
        try:
            self.conn = psycopg2.connect(os.getenv('DATABASE_URL'))
            self.cursor = self.conn.cursor()
            print("✅ Connected to database")
            return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def parse_date_safe(self, date_value):
        """Safely parse date, returning None for invalid dates"""
        if pd.isna(date_value):
            return None
        
        # Check if it's already a NaT or None
        if date_value is None:
            return None
            
        # Check for string representations of null/NaT
        if isinstance(date_value, str):
            if date_value.upper() in ['NAT', 'NAN', 'NULL', 'NONE', '']:
                return None
        
        try:
            # Try to parse the date
            parsed = pd.to_datetime(date_value, errors='coerce')
            if pd.isna(parsed):
                return None
            return parsed.date()
        except:
            return None
    
    def clean_float_value(self, value, default=0.0):
        """Safely convert to float, handling various edge cases"""
        if pd.isna(value):
            return default
            
        if isinstance(value, str):
            # Remove common issues
            value = value.strip()
            if value.upper() in ['NAN', 'NAT', 'NULL', 'NONE', '']:
                return default
            if value == '-':
                return default
            # Remove currency symbols and commas
            value = value.replace('$', '').replace(',', '')
        
        try:
            return float(value)
        except:
            return default
    
    def process_aus_file_fixed(self):
        """Process AUS file with better error handling"""
        print("\n📄 Processing AUS Invoice Details (Fixed)...")
        
        try:
            # Read CSV file
            df = pd.read_csv('invoice_details_aus.csv')
            print(f"  Found {len(df)} AUS records")
            
            # Pre-process date columns
            print("  Cleaning date columns...")
            df['Work Date'] = df['Work Date'].apply(self.parse_date_safe)
            
            # Clean numeric columns
            numeric_columns = ['Hours', 'Bill Hours Qty', 'Bill Amount', 'Bill Rate', 'Pay Rate', 'Lunch']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: self.clean_float_value(x, 0.0))
            
            # Process records
            batch_size = 500
            batch_data = []
            
            for idx, row in df.iterrows():
                try:
                    # Skip rows with no work date
                    if pd.isna(row.get('Work Date')):
                        self.stats['aus_skipped'] += 1
                        continue
                    
                    # Transform row
                    transformed = self.transform_aus_row_safe(row)
                    if not transformed:
                        continue
                    
                    # Add to batch
                    batch_data.append(transformed)
                    
                    # Insert when batch is full
                    if len(batch_data) >= batch_size:
                        self.insert_batch_safe(batch_data)
                        batch_data = []
                        if idx % 5000 == 0:
                            print(f"  Processed {idx} records...")
                
                except Exception as e:
                    self.stats['errors'].append(f"Row {idx}: {str(e)}")
                    continue
            
            # Insert remaining records
            if batch_data:
                self.insert_batch_safe(batch_data)
            
            print(f"✅ Completed AUS processing: {self.stats['aus_processed']} records")
            print(f"⚠️  Skipped {self.stats['aus_skipped']} records (no valid date)")
            
        except Exception as e:
            print(f"❌ Error processing AUS file: {e}")
            import traceback
            traceback.print_exc()
    
    def transform_aus_row_safe(self, row):
        """Transform AUS row with comprehensive error handling"""
        try:
            # Clean invoice number
            invoice_no = str(row['Invoice Number']).strip()
            if invoice_no.endswith('.0'):
                invoice_no = invoice_no[:-2]
            
            # Work date already cleaned in preprocessing
            work_date = row.get('Work Date')
            if not work_date:
                return None
            
            # Clean employee info
            emp_id = str(row.get('Employee Number', '')).strip() if pd.notna(row.get('Employee Number')) else None
            if emp_id and emp_id.endswith('.0'):
                emp_id = emp_id[:-2]
            
            # Hours and amounts (already cleaned)
            hours = row.get('Hours', 0)
            bill_hours = row.get('Bill Hours Qty', 0)
            amount = row.get('Bill Amount', 0)
            bill_rate = row.get('Bill Rate', 0)
            pay_rate = row.get('Pay Rate', 0)
            
            # Determine hour type from description
            pay_desc = str(row.get('Pay Hours Description', '')).lower()
            if 'holiday' in pay_desc:
                hours_holiday = hours
                hours_regular = 0
                hours_ot = 0
            elif 'overtime' in pay_desc or 'ot' in pay_desc:
                hours_ot = hours
                hours_regular = 0
                hours_holiday = 0
            else:
                hours_regular = hours
                hours_ot = 0
                hours_holiday = 0
            
            return {
                'invoice_no': invoice_no,
                'source_system': 'AUS',
                'work_date': work_date,
                'employee_id': emp_id,
                'employee_name': row.get('Employee Name'),
                'job_number': str(row.get('Job Number', '')) if pd.notna(row.get('Job Number')) else None,
                'position_description': row.get('Post Description'),
                'hours_regular': hours_regular,
                'hours_overtime': hours_ot,
                'hours_holiday': hours_holiday,
                'hours_total': hours,
                'rate_regular': bill_rate if hours_regular > 0 else 0,
                'rate_overtime': bill_rate if hours_ot > 0 else 0,
                'rate_holiday': bill_rate if hours_holiday > 0 else 0,
                'amount_regular': amount if hours_regular > 0 else 0,
                'amount_overtime': amount if hours_ot > 0 else 0,
                'amount_holiday': amount if hours_holiday > 0 else 0,
                'amount_total': amount,
                'customer_number': str(row.get('Customer Number', '')) if pd.notna(row.get('Customer Number')) else None,
                'bill_category': str(row.get('Bill Cat Number', '')) if pd.notna(row.get('Bill Cat Number')) else None,
                'pay_rate': pay_rate,
                'in_time': self.parse_time(row.get('In Time')),
                'out_time': self.parse_time(row.get('Out Time')),
                'lunch_hours': row.get('Lunch', 0)
            }
            
        except Exception as e:
            self.stats['errors'].append(f"AUS transform error: {str(e)}")
            return None
    
    def parse_time(self, time_str):
        """Parse time string to time object"""
        if pd.isna(time_str) or str(time_str).upper() in ['NAN', 'NAT', 'NULL', 'NONE', '']:
            return None
        
        try:
            time_str = str(time_str).strip()
            if ':' in time_str:
                parts = time_str.split(':')
                hour = int(parts[0])
                minute = int(parts[1][:2])
                return f"{hour:02d}:{minute:02d}:00"
            return None
        except:
            return None
    
    def insert_batch_safe(self, batch_data):
        """Insert batch with proper null handling"""
        if not batch_data:
            return
        
        # Prepare values, ensuring no 'NaT' strings
        values = []
        for record in batch_data:
            # Create tuple with proper None values (not 'NaT' strings)
            values.append((
                record['invoice_no'],
                record['source_system'],
                record.get('work_date'),  # Already processed to be None or valid date
                record.get('employee_id'),
                record.get('employee_name'),
                record.get('location_code'),
                record.get('location_name'),
                record.get('building_code'),
                record.get('emid'),
                record.get('position_code'),
                record.get('position_description'),
                record.get('job_number'),
                record.get('hours_regular', 0),
                record.get('hours_overtime', 0),
                record.get('hours_holiday', 0),
                record.get('hours_total', 0),
                record.get('rate_regular', 0),
                record.get('rate_overtime', 0),
                record.get('rate_holiday', 0),
                record.get('amount_regular', 0),
                record.get('amount_overtime', 0),
                record.get('amount_holiday', 0),
                record.get('amount_total', 0),
                record.get('customer_number'),
                record.get('customer_name'),
                record.get('business_unit'),
                record.get('shift_in'),
                record.get('shift_out'),
                record.get('bill_category'),
                record.get('pay_rate', 0),
                record.get('in_time'),
                record.get('out_time'),
                record.get('lunch_hours', 0)
            ))
        
        # Insert query
        query = """
        INSERT INTO invoice_details (
            invoice_no, source_system, work_date, employee_id, employee_name,
            location_code, location_name, building_code, emid,
            position_code, position_description, job_number,
            hours_regular, hours_overtime, hours_holiday, hours_total,
            rate_regular, rate_overtime, rate_holiday,
            amount_regular, amount_overtime, amount_holiday, amount_total,
            customer_number, customer_name, business_unit,
            shift_in, shift_out, bill_category, pay_rate,
            in_time, out_time, lunch_hours
        ) VALUES %s
        ON CONFLICT DO NOTHING
        """
        
        try:
            execute_values(self.cursor, query, values)
            self.conn.commit()
            self.stats['aus_processed'] += len(batch_data)
        except Exception as e:
            print(f"  ⚠️ Batch insert error: {e}")
            self.conn.rollback()
            # Log the specific row causing issues
            if "NaT" in str(e):
                print("  Found NaT values in batch - this batch was skipped")
                self.stats['aus_skipped'] += len(batch_data)
    
    def run_fix(self):
        """Run the fix for AUS data"""
        print("🔧 Running Fixed AUS Data Import")
        print("="*60)
        
        if not self.connect_db():
            return
        
        # First, let's check how many AUS records we already have
        self.cursor.execute("SELECT COUNT(*) FROM invoice_details WHERE source_system = 'AUS'")
        existing_count = self.cursor.fetchone()[0]
        print(f"\n📊 Existing AUS records in database: {existing_count:,}")
        
        if existing_count > 6000:
            response = input("\nAUS data appears to be already loaded. Re-process anyway? (y/n): ")
            if response.lower() != 'y':
                print("Skipping AUS reprocessing")
                return
        
        # Clear existing AUS data if reprocessing
        if existing_count > 0:
            print("🗑️  Clearing existing AUS data...")
            self.cursor.execute("DELETE FROM invoice_details WHERE source_system = 'AUS'")
            self.conn.commit()
        
        # Process the file
        self.process_aus_file_fixed()
        
        # Print summary
        print("\n" + "="*60)
        print("📊 FIX SUMMARY")
        print("="*60)
        print(f"AUS records processed: {self.stats['aus_processed']:,}")
        print(f"Records skipped (no date): {self.stats['aus_skipped']:,}")
        print(f"Errors encountered: {len(self.stats['errors'])}")
        
        if self.stats['errors']:
            print("\nFirst 5 errors:")
            for err in self.stats['errors'][:5]:
                print(f"  - {err}")
        
        # Close connection
        self.cursor.close()
        self.conn.close()
        print("\n🔒 Database connection closed")

if __name__ == "__main__":
    fixer = FixedInvoiceDetailsTransformer()
    fixer.run_fix()
