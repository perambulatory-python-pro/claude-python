import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

class InvoiceDetailsTransformer:
    """Transform and migrate BCI and AUS invoice details to unified schema"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.stats = {
            'bci_processed': 0,
            'aus_processed': 0,
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
    
    def verify_invoice_exists(self, invoice_no):
        """Check if invoice exists in master table"""
        self.cursor.execute(
            "SELECT 1 FROM invoices WHERE invoice_no = %s",
            (str(invoice_no),)
        )
        return self.cursor.fetchone() is not None
    
    def clean_employee_name(self, first, last, mi=None):
        """Clean and combine employee name parts"""
        parts = []
        if pd.notna(last) and str(last).lower() != 'nan':
            parts.append(str(last).strip())
        if pd.notna(first) and str(first).lower() != 'nan':
            parts.append(str(first).strip())
        if pd.notna(mi) and str(mi).lower() != 'nan':
            parts.append(str(mi).strip())
        
        return ', '.join(parts) if parts else None
    
    def parse_time(self, time_str):
        """Parse time string to time object"""
        if pd.isna(time_str) or str(time_str).lower() == 'nan':
            return None
        
        try:
            # Handle various time formats
            time_str = str(time_str).strip()
            if ':' in time_str:
                # Try HH:MM format
                parts = time_str.split(':')
                hour = int(parts[0])
                minute = int(parts[1][:2])  # Take first 2 chars in case of AM/PM
                return f"{hour:02d}:{minute:02d}:00"
            return None
        except:
            return None
    
    def transform_bci_row(self, row):
        """Transform a BCI row to unified schema"""
        try:
            # Clean invoice number
            invoice_no = str(row['Invoice_No']).strip()
            if invoice_no.endswith('.0'):
                invoice_no = invoice_no[:-2]
            
            # Parse work date
            work_date = pd.to_datetime(row['Date']).date()
            
            # Clean employee info
            emp_id = str(row.get('Emp_No', '')).strip() if pd.notna(row.get('Emp_No')) else None
            if emp_id and emp_id.endswith('.0'):
                emp_id = emp_id[:-2]
            
            emp_name = self.clean_employee_name(
                row.get('Employee_First_Name'),
                row.get('Employee_Last_Name'),
                row.get('Employee_MI')
            )
            
            # Get hours and amounts
            hours_regular = float(row.get('Billed_Regular_Hours', 0) or 0)
            hours_ot = float(row.get('Billed_OT_Hours', 0) or 0)
            hours_holiday = float(row.get('Billed_Holiday_Hours', 0) or 0)
            hours_total = float(row.get('Billed_Total_Hours', 0) or 0)
            
            amount_regular = float(row.get(' Billed_Regular_Wages ', 0) or 0)
            amount_ot = float(row.get(' Billed_OT_Wages ', 0) or 0)
            amount_holiday = float(row.get(' Billed_Holiday_Wages ', 0) or 0)
            amount_total = float(row.get(' Billed_Total_Wages ', 0) or 0)
            
            # Calculate rates if not zero hours
            rate_regular = amount_regular / hours_regular if hours_regular > 0 else 0
            rate_ot = amount_ot / hours_ot if hours_ot > 0 else 0
            rate_holiday = amount_holiday / hours_holiday if hours_holiday > 0 else 0
            
            return {
                'invoice_no': invoice_no,
                'source_system': 'BCI',
                'work_date': work_date,
                'employee_id': emp_id,
                'employee_name': emp_name,
                'location_code': row.get('Location_Number'),
                'location_name': row.get('Location'),
                'position_code': row.get('Position_Number'),
                'position_description': row.get('Position'),
                'hours_regular': hours_regular,
                'hours_overtime': hours_ot,
                'hours_holiday': hours_holiday,
                'hours_total': hours_total,
                'rate_regular': rate_regular,
                'rate_overtime': rate_ot,
                'rate_holiday': rate_holiday,
                'amount_regular': amount_regular,
                'amount_overtime': amount_ot,
                'amount_holiday': amount_holiday,
                'amount_total': amount_total,
                'customer_number': row.get('Customer_Number'),
                'customer_name': row.get('Customer'),
                'business_unit': row.get('Business_Unit'),
                'shift_in': self.parse_time(row.get('Shift_In')),
                'shift_out': self.parse_time(row.get('Shift_Out'))
            }
            
        except Exception as e:
            self.stats['errors'].append(f"BCI transform error: {str(e)}")
            return None
    
    def transform_aus_row(self, row):
        """Transform an AUS row to unified schema"""
        try:
            # Clean invoice number
            invoice_no = str(row['Invoice Number']).strip()
            if invoice_no.endswith('.0'):
                invoice_no = invoice_no[:-2]
            
            # Parse work date
            work_date = pd.to_datetime(row['Work Date']).date()
            
            # Clean employee info
            emp_id = str(row.get('Employee Number', '')).strip() if pd.notna(row.get('Employee Number')) else None
            if emp_id and emp_id.endswith('.0'):
                emp_id = emp_id[:-2]
            
            # Hours and amounts
            hours = float(row.get('Hours', 0) or 0)
            bill_hours = float(row.get('Bill Hours Qty', 0) or 0)
            amount = float(row.get('Bill Amount', 0) or 0)
            bill_rate = float(row.get('Bill Rate', 0) or 0)
            pay_rate = float(row.get('Pay Rate', 0) or 0)
            
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
                'job_number': row.get('Job Number'),
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
                'customer_number': row.get('Customer Number'),
                'bill_category': row.get('Bill Cat Number'),
                'pay_rate': pay_rate,
                'in_time': self.parse_time(row.get('In Time')),
                'out_time': self.parse_time(row.get('Out Time')),
                'lunch_hours': float(row.get('Lunch', 0) or 0)
            }
            
        except Exception as e:
            self.stats['errors'].append(f"AUS transform error: {str(e)}")
            return None
    
    def process_bci_file(self):
        """Process BCI invoice details file"""
        print("\n📄 Processing BCI Invoice Details...")
        
        try:
            # Read CSV file
            df = pd.read_csv('invoice_details_bci.csv')
            print(f"  Found {len(df)} BCI records")
            
            # Process in batches
            batch_size = 500
            batch_data = []
            
            for idx, row in df.iterrows():
                # Transform row
                transformed = self.transform_bci_row(row)
                if not transformed:
                    continue
                
                # Check if invoice exists
                if not self.verify_invoice_exists(transformed['invoice_no']):
                    self.stats['missing_invoices'].add(transformed['invoice_no'])
                    continue
                
                # Add to batch
                batch_data.append(transformed)
                
                # Insert when batch is full
                if len(batch_data) >= batch_size:
                    self.insert_batch(batch_data)
                    batch_data = []
                    print(f"  Processed {idx + 1} records...")
            
            # Insert remaining records
            if batch_data:
                self.insert_batch(batch_data)
            
            print(f"✅ Completed BCI processing: {self.stats['bci_processed']} records")
            
        except Exception as e:
            print(f"❌ Error processing BCI file: {e}")
            import traceback
            traceback.print_exc()
    
    def process_aus_file(self):
        """Process AUS invoice details file"""
        print("\n📄 Processing AUS Invoice Details...")
        
        try:
            # Read CSV file
            df = pd.read_csv('invoice_details_aus.csv')
            print(f"  Found {len(df)} AUS records")
            
            # Process in batches
            batch_size = 500
            batch_data = []
            
            for idx, row in df.iterrows():
                # Transform row
                transformed = self.transform_aus_row(row)
                if not transformed:
                    continue
                
                # Check if invoice exists
                if not self.verify_invoice_exists(transformed['invoice_no']):
                    self.stats['missing_invoices'].add(transformed['invoice_no'])
                    continue
                
                # Add to batch
                batch_data.append(transformed)
                
                # Insert when batch is full
                if len(batch_data) >= batch_size:
                    self.insert_batch(batch_data)
                    batch_data = []
                    print(f"  Processed {idx + 1} records...")
            
            # Insert remaining records
            if batch_data:
                self.insert_batch(batch_data)
            
            print(f"✅ Completed AUS processing: {self.stats['aus_processed']} records")
            
        except Exception as e:
            print(f"❌ Error processing AUS file: {e}")
            import traceback
            traceback.print_exc()
    
    def insert_batch(self, batch_data):
        """Insert batch of records"""
        if not batch_data:
            return
        
        # Prepare values for insertion
        values = []
        for record in batch_data:
            values.append((
                record['invoice_no'],
                record['source_system'],
                record.get('work_date'),
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
                record.get('pay_rate'),
                record.get('in_time'),
                record.get('out_time'),
                record.get('lunch_hours')
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
            
            # Update stats
            if batch_data[0]['source_system'] == 'BCI':
                self.stats['bci_processed'] += len(batch_data)
            else:
                self.stats['aus_processed'] += len(batch_data)
                
        except Exception as e:
            print(f"  ⚠️ Batch insert error: {e}")
            self.conn.rollback()
    
    def run_migration(self):
        """Run the complete migration process"""
        print("🚀 Starting Invoice Details Migration")
        print("="*60)
        
        if not self.connect_db():
            return
        
        # Process both files
        self.process_bci_file()
        self.process_aus_file()
        
        # Print summary
        self.print_summary()
        
        # Close connection
        self.cursor.close()
        self.conn.close()
        print("\n🔒 Database connection closed")
    
    def print_summary(self):
        """Print migration summary"""
        print("\n" + "="*60)
        print("📊 MIGRATION SUMMARY")
        print("="*60)
        print(f"BCI records processed: {self.stats['bci_processed']:,}")
        print(f"AUS records processed: {self.stats['aus_processed']:,}")
        print(f"Total records: {self.stats['bci_processed'] + self.stats['aus_processed']:,}")
        
        if self.stats['missing_invoices']:
            print(f"\n⚠️ Missing invoices: {len(self.stats['missing_invoices'])}")
            print("  Sample missing invoices:")
            for inv in list(self.stats['missing_invoices'])[:10]:
                print(f"    - {inv}")
        
        if self.stats['errors']:
            print(f"\n❌ Errors: {len(self.stats['errors'])}")
            for err in self.stats['errors'][:5]:
                print(f"    - {err}")
        
        # Query and show sample data
        try:
            self.cursor.execute("""
                SELECT 
                    source_system,
                    COUNT(*) as count,
                    COUNT(DISTINCT invoice_no) as invoices,
                    COUNT(DISTINCT employee_id) as employees,
                    SUM(hours_total) as total_hours,
                    SUM(amount_total) as total_amount
                FROM invoice_details
                GROUP BY source_system
            """)
            
            results = self.cursor.fetchall()
            
            print("\n📈 Database Summary:")
            print(f"{'Source':<10} {'Records':<10} {'Invoices':<10} {'Employees':<10} {'Hours':<12} {'Amount'}")
            print("-" * 70)
            
            for row in results:
                print(f"{row[0]:<10} {row[1]:<10,} {row[2]:<10,} {row[3]:<10,} {row[4]:<12,.2f} ${row[5]:,.2f}")
                
        except Exception as e:
            print(f"\n⚠️ Could not query summary: {e}")

if __name__ == "__main__":
    transformer = InvoiceDetailsTransformer()
    transformer.run_migration()
