"""
Smart Duplicate Detection for Invoice Details
Handles the nuance between legitimate multiple records vs actual duplicates

Python Learning Points:
- Composite key generation for unique identification
- Business logic implementation in code
- Efficient database lookups for duplicate checking
"""

import pandas as pd
import psycopg2
import hashlib
from datetime import datetime, date
from typing import Dict, List, Optional, Set, Tuple
import logging

logger = logging.getLogger(__name__)

class SmartDuplicateHandler:
    """
    Intelligent duplicate detection that understands business context
    """
    
    def __init__(self, database_url: str):
        self.database_url = database_url
    
    def generate_record_fingerprint(self, record: Dict) -> str:
        """
        Generate a unique fingerprint for a record based on key business fields
        
        Business Logic: Two records are the same if they have identical:
        - Invoice number
        - Employee ID  
        - Work date
        - Hours (regular, OT, holiday)
        - Amounts (regular, OT, holiday)
        - Position/location context
        
        This allows for legitimate variations while catching true duplicates
        """
        # Core identifying fields
        key_fields = [
            str(record.get('invoice_no', '')),
            str(record.get('employee_id', '')),
            str(record.get('work_date', '')),
            str(record.get('hours_regular', 0)),
            str(record.get('hours_overtime', 0)),
            str(record.get('hours_holiday', 0)),
            str(record.get('amount_regular', 0)),
            str(record.get('amount_overtime', 0)),
            str(record.get('amount_holiday', 0)),
            str(record.get('position_code', '')),
            str(record.get('location_code', '')),
            str(record.get('in_time', '')),  # Include time if available
            str(record.get('out_time', ''))
        ]
        
        # Create a hash of the combined key fields
        combined_key = '|'.join(key_fields)
        fingerprint = hashlib.md5(combined_key.encode()).hexdigest()
        
        return fingerprint
    
    def get_existing_fingerprints(self, invoice_numbers: List[str]) -> Set[str]:
        """
        Get fingerprints of existing records for the given invoice numbers
        This is much more efficient than checking each record individually
        """
        if not invoice_numbers:
            return set()
        
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Build query to get existing records for these invoices
            placeholders = ','.join(['%s'] * len(invoice_numbers))
            
            cursor.execute(f"""
                SELECT 
                    invoice_no, employee_id, work_date,
                    hours_regular, hours_overtime, hours_holiday,
                    amount_regular, amount_overtime, amount_holiday,
                    position_code, location_code, in_time, out_time
                FROM invoice_details 
                WHERE invoice_no IN ({placeholders})
            """, invoice_numbers)
            
            existing_records = cursor.fetchall()
            conn.close()
            
            # Generate fingerprints for existing records
            existing_fingerprints = set()
            
            for row in existing_records:
                record_dict = {
                    'invoice_no': row[0],
                    'employee_id': row[1], 
                    'work_date': row[2],
                    'hours_regular': row[3],
                    'hours_overtime': row[4],
                    'hours_holiday': row[5],
                    'amount_regular': row[6],
                    'amount_overtime': row[7],
                    'amount_holiday': row[8],
                    'position_code': row[9],
                    'location_code': row[10],
                    'in_time': row[11],
                    'out_time': row[12]
                }
                
                fingerprint = self.generate_record_fingerprint(record_dict)
                existing_fingerprints.add(fingerprint)
            
            logger.info(f"Found {len(existing_fingerprints)} existing record fingerprints")
            return existing_fingerprints
            
        except Exception as e:
            logger.error(f"Error getting existing fingerprints: {e}")
            return set()
    
    def filter_duplicates(self, records: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Filter records into new vs duplicate categories
        
        Returns:
            (new_records, duplicate_records)
        """
        if not records:
            return [], []
        
        # Get unique invoice numbers from the incoming records
        invoice_numbers = list(set(record.get('invoice_no') for record in records if record.get('invoice_no')))
        
        # Get existing fingerprints from database
        existing_fingerprints = self.get_existing_fingerprints(invoice_numbers)
        
        # Process each record
        new_records = []
        duplicate_records = []
        incoming_fingerprints = set()  # Track duplicates within the batch itself
        
        for record in records:
            fingerprint = self.generate_record_fingerprint(record)
            
            # Check if it's a duplicate of existing data or within this batch
            if fingerprint in existing_fingerprints or fingerprint in incoming_fingerprints:
                duplicate_records.append(record)
            else:
                new_records.append(record)
                incoming_fingerprints.add(fingerprint)
        
        logger.info(f"Filtered {len(records)} records: {len(new_records)} new, {len(duplicate_records)} duplicates")
        
        return new_records, duplicate_records
    
    def analyze_duplicates(self, duplicate_records: List[Dict]) -> Dict[str, any]:
        """
        Analyze the duplicate records to understand patterns
        """
        if not duplicate_records:
            return {'total': 0, 'by_invoice': {}, 'by_employee': {}}
        
        analysis = {
            'total': len(duplicate_records),
            'by_invoice': {},
            'by_employee': {},
            'sample_duplicates': []
        }
        
        # Count by invoice
        for record in duplicate_records:
            invoice_no = record.get('invoice_no', 'Unknown')
            analysis['by_invoice'][invoice_no] = analysis['by_invoice'].get(invoice_no, 0) + 1
        
        # Count by employee
        for record in duplicate_records:
            employee_id = record.get('employee_id', 'Unknown')
            analysis['by_employee'][employee_id] = analysis['by_employee'].get(employee_id, 0) + 1
        
        # Keep a few samples for inspection
        analysis['sample_duplicates'] = duplicate_records[:5]
        
        return analysis

class SmartDatabaseManager:
    """
    Database manager with smart duplicate handling AND invoice validation
    """
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.duplicate_handler = SmartDuplicateHandler(database_url)
    
    def check_invoice_exists(self, invoice_numbers: List[str]) -> Tuple[Set[str], Set[str]]:
        """
        Check which invoice numbers exist in the main invoices table
        
        Returns:
            (existing_invoices, missing_invoices)
        """
        if not invoice_numbers:
            return set(), set()
        
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Check which invoices exist in the main table
            placeholders = ','.join(['%s'] * len(invoice_numbers))
            cursor.execute(f"""
                SELECT DISTINCT invoice_no 
                FROM invoices 
                WHERE invoice_no IN ({placeholders})
            """, invoice_numbers)
            
            existing_invoices = {row[0] for row in cursor.fetchall()}
            conn.close()
            
            all_invoices = set(invoice_numbers)
            missing_invoices = all_invoices - existing_invoices
            
            if missing_invoices:
                logger.warning(f"🚨 Found {len(missing_invoices)} invoice numbers NOT in main invoices table")
                for missing in list(missing_invoices)[:5]:  # Log first 5
                    logger.warning(f"   Missing invoice: {missing}")
            
            return existing_invoices, missing_invoices
            
        except Exception as e:
            logger.error(f"Error checking invoice existence: {e}")
            return set(), set(invoice_numbers)  # Assume all are missing on error
    
    def check_invoice_exists(self, invoice_numbers: List[str]) -> Tuple[Set[str], Set[str]]:
        """
        Check which invoice numbers exist in the main invoices table
        
        Returns:
            (existing_invoices, missing_invoices)
        """
        if not invoice_numbers:
            return set(), set()
        
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Check which invoices exist in the main table
            placeholders = ','.join(['%s'] * len(invoice_numbers))
            cursor.execute(f"""
                SELECT DISTINCT invoice_no 
                FROM invoices 
                WHERE invoice_no IN ({placeholders})
            """, invoice_numbers)
            
            existing_invoices = {row[0] for row in cursor.fetchall()}
            conn.close()
            
            all_invoices = set(invoice_numbers)
            missing_invoices = all_invoices - existing_invoices
            
            if missing_invoices:
                logger.warning(f"🚨 Found {len(missing_invoices)} invoice numbers NOT in main invoices table")
                for missing in list(missing_invoices)[:5]:  # Log first 5
                    logger.warning(f"   Missing invoice: {missing}")
            
            return existing_invoices, missing_invoices
            
        except Exception as e:
            logger.error(f"Error checking invoice existence: {e}")
            return set(), set(invoice_numbers)  # Assume all are missing on error

    def insert_invoice_details_smart(self, records: List[Dict]) -> Dict[str, any]:
        """
        Insert invoice details with smart duplicate detection AND invoice validation
        """
        result = {
            'success': False,
            'total_input_records': len(records),
            'valid_invoice_records': 0,
            'invalid_invoice_records': 0,
            'missing_invoices': [],
            'new_records': 0,
            'duplicate_records': 0,
            'inserted': 0,
            'errors': [],
            'duplicate_analysis': {},
            'processing_time': 0
        }
        
        if not records:
            result['success'] = True
            return result
        
        start_time = datetime.now()
        
        try:
            # STEP 1: Validate invoice numbers exist in main table
            logger.info("🔍 Step 1: Validating invoice numbers...")
            unique_invoices = list(set(record.get('invoice_no') for record in records if record.get('invoice_no')))
            existing_invoices, missing_invoices = self.check_invoice_exists(unique_invoices)
            
            result['missing_invoices'] = list(missing_invoices)
            
            # Filter out records with missing invoice numbers
            valid_records = []
            invalid_records = []
            
            for record in records:
                invoice_no = record.get('invoice_no')
                if invoice_no in existing_invoices:
                    valid_records.append(record)
                else:
                    invalid_records.append(record)
                    logger.warning(f"🚨 Skipping record for missing invoice: {invoice_no}")
            
            result['valid_invoice_records'] = len(valid_records)
            result['invalid_invoice_records'] = len(invalid_records)
            
            if invalid_records:
                error_msg = f"Found {len(invalid_records)} records with invoice numbers not in main table"
                result['errors'].append(error_msg)
                logger.error(error_msg)
            
            # STEP 2: Process only valid records for duplicates
            if valid_records:
                logger.info(f"🔍 Step 2: Checking {len(valid_records)} valid records for duplicates...")
                new_records, duplicate_records = self.duplicate_handler.filter_duplicates(valid_records)
                
                result['new_records'] = len(new_records)
                result['duplicate_records'] = len(duplicate_records)
                
                # Analyze duplicates
                if duplicate_records:
                    result['duplicate_analysis'] = self.duplicate_handler.analyze_duplicates(duplicate_records)
                    logger.info(f"📊 Found {len(duplicate_records)} duplicate records")
                
                # STEP 3: Insert only new, valid records
                if new_records:
                    logger.info(f"💾 Step 3: Inserting {len(new_records)} new, valid records...")
                    insert_result = self._bulk_insert_records(new_records)
                    result['inserted'] = insert_result['inserted']
                    result['errors'].extend(insert_result['errors'])
            else:
                logger.warning("⚠️ No valid records to process after invoice validation")
            
            result['success'] = True
            result['processing_time'] = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"✅ Smart processing complete:")
            logger.info(f"   📊 {result['inserted']} inserted")
            logger.info(f"   🔄 {result['duplicate_records']} duplicates skipped") 
            logger.info(f"   🚨 {result['invalid_invoice_records']} invalid invoices skipped")
            
        except Exception as e:
            error_msg = f"Error in smart processing: {e}"
            result['errors'].append(error_msg)
            logger.error(error_msg)
        
        return result
    
    def _bulk_insert_records(self, records: List[Dict]) -> Dict[str, any]:
        """
        Bulk insert records (assumes they're already deduplicated)
        """
        insert_result = {
            'inserted': 0,
            'errors': []
        }
        
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Insert in batches
            batch_size = 500
            total_inserted = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                batch_inserted = 0
                
                for record in batch:
                    try:
                        # Clean the record
                        clean_record = self._clean_record_data(record)
                        
                        # Build employee name if needed
                        employee_name = clean_record.get('employee_name')
                        if not employee_name:
                            first = clean_record.get('first_name', '') or ''
                            last = clean_record.get('last_name', '') or ''
                            middle = clean_record.get('middle_initial', '') or ''
                            
                            if first or last:
                                name_parts = [part for part in [first, middle, last] if part]
                                employee_name = ' '.join(name_parts)
                        
                        # Insert with NO conflict handling (records are pre-filtered)
                        cursor.execute("""
                            INSERT INTO invoice_details (
                                invoice_no, source_system, work_date, employee_id, employee_name,
                                location_code, location_name, building_code, emid, position_code,
                                position_description, job_number, hours_regular, hours_overtime,
                                hours_holiday, hours_total, rate_regular, rate_overtime,
                                rate_holiday, amount_regular, amount_overtime, amount_holiday,
                                amount_total, customer_number, customer_name, business_unit,
                                in_time, out_time, bill_category, pay_rate, lunch_hours,
                                po, created_at, updated_at
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s
                            )
                        """, (
                            clean_record.get('invoice_no'),
                            clean_record.get('source_system', 'UNKNOWN'),
                            clean_record.get('work_date'),
                            clean_record.get('employee_id'),
                            employee_name,
                            clean_record.get('location_code'),
                            clean_record.get('location_name'),
                            clean_record.get('building_code'),
                            clean_record.get('emid'),
                            clean_record.get('position_code'),
                            clean_record.get('position_description'),
                            clean_record.get('job_number'),
                            clean_record.get('hours_regular', 0),
                            clean_record.get('hours_overtime', 0),
                            clean_record.get('hours_holiday', 0),
                            clean_record.get('hours_total', 0),
                            clean_record.get('rate_regular', 0),
                            clean_record.get('rate_overtime', 0),
                            clean_record.get('rate_holiday', 0),
                            clean_record.get('amount_regular', 0),
                            clean_record.get('amount_overtime', 0),
                            clean_record.get('amount_holiday', 0),
                            clean_record.get('amount_total', 0),
                            clean_record.get('customer_number'),
                            clean_record.get('customer_name'),
                            clean_record.get('business_unit'),
                            clean_record.get('in_time'),
                            clean_record.get('out_time'),
                            clean_record.get('bill_category'),
                            clean_record.get('pay_rate', 0),
                            clean_record.get('lunch_hours', 0),
                            clean_record.get('po'),
                            datetime.now(),
                            datetime.now()
                        ))
                        
                        batch_inserted += 1
                        
                    except Exception as e:
                        error_msg = f"Error inserting record {clean_record.get('invoice_no', 'unknown')}: {e}"
                        insert_result['errors'].append(error_msg)
                        logger.error(error_msg)
                
                # Commit the batch
                conn.commit()
                total_inserted += batch_inserted
                logger.info(f"Batch {i//batch_size + 1}: {batch_inserted} records inserted")
            
            conn.close()
            insert_result['inserted'] = total_inserted
            
        except Exception as e:
            error_msg = f"Database error in bulk insert: {e}"
            insert_result['errors'].append(error_msg)
            logger.error(error_msg)
        
        return insert_result
    
    def _clean_record_data(self, record: Dict) -> Dict:
        """Clean and validate record data"""
        cleaned = {}
        
        for key, value in record.items():
            if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
                cleaned[key] = None
            elif key == 'work_date':
                if isinstance(value, str) and ' 00:00:00' in value:
                    cleaned[key] = value.split(' ')[0]
                else:
                    cleaned[key] = value
            elif key in ['hours_regular', 'hours_overtime', 'hours_holiday', 'hours_total',
                        'rate_regular', 'rate_overtime', 'rate_holiday', 'pay_rate', 'lunch_hours',
                        'amount_regular', 'amount_overtime', 'amount_holiday', 'amount_total']:
                try:
                    cleaned[key] = float(value) if value is not None else 0.0
                except (ValueError, TypeError):
                    cleaned[key] = 0.0
            else:
                cleaned[key] = str(value).strip() if value is not None else None
        
        return cleaned

# Helper function for easy integration
def process_file_with_smart_duplicates(df: pd.DataFrame, source_system: str, database_url: str) -> Dict[str, any]:
    """
    Process a file with smart duplicate detection
    
    Usage:
    result = process_file_with_smart_duplicates(aus_df, 'AUS', DATABASE_URL)
    """
    # Import the fixed mapper
    from fixed_date_converter import EnhancedDataMapperFixed
    
    # Map the data
    mapper = EnhancedDataMapperFixed()
    
    if source_system.upper() == 'AUS':
        mapped_records = mapper.map_aus_details(df)
    elif source_system.upper() == 'BCI':
        mapped_records = mapper.map_bci_details(df)
    else:
        raise ValueError(f"Unknown source system: {source_system}")
    
    # Process with smart duplicate detection
    smart_manager = SmartDatabaseManager(database_url)
    result = smart_manager.insert_invoice_details_smart(mapped_records)
    
    return result

# Test function
def test_smart_processing():
    """Test the smart duplicate detection"""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("❌ DATABASE_URL not found")
        return
    
    smart_manager = SmartDatabaseManager(database_url)
    
    # Test with sample records
    test_records = [
        {
            'invoice_no': 'TEST123',
            'employee_id': 'EMP001',
            'work_date': '2025-01-01',
            'hours_regular': 8.0,
            'amount_regular': 200.0
        },
        {
            'invoice_no': 'TEST123',
            'employee_id': 'EMP001', 
            'work_date': '2025-01-01',
            'hours_regular': 8.0,
            'amount_regular': 200.0
        }  # This should be detected as duplicate
    ]
    
    result = smart_manager.insert_invoice_details_smart(test_records)
    
    print("Smart processing test:")
    print(f"  Total input: {result['total_input_records']}")
    print(f"  New records: {result['new_records']}")
    print(f"  Duplicates: {result['duplicate_records']}")
    print(f"  Inserted: {result['inserted']}")

if __name__ == "__main__":
    test_smart_processing()
