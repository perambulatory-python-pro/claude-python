"""
Fast bulk insert that validates invoice numbers exist
Add this method to your database_manager_compatible.py
"""

def bulk_insert_invoice_details_fast_validated(self, invoice_details: List[Dict]) -> Dict:
    """
    Fast bulk insert that only validates invoice numbers exist in master table
    Much faster than full duplicate checking but ensures referential integrity
    """
    results = {
        'total_records': len(invoice_details),
        'inserted': 0,
        'failed': 0,
        'missing_invoice_count': 0,
        'missing_invoices': [],
        'missing_invoice_records': [],  # Full records with missing invoices
        'error_records': [],  # Records that failed for other reasons
        'success': False
    }
    
    if not invoice_details:
        results['success'] = True
        return results
    
    try:
        conn = psycopg2.connect(self.database_url)
        
        # Step 1: Get all existing invoice numbers (one query for speed)
        print("   📊 Loading existing invoice numbers for validation...")
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT invoice_no FROM invoices WHERE invoice_no IS NOT NULL")
            existing_invoices = {row[0] for row in cursor.fetchall()}
        print(f"   ✅ Found {len(existing_invoices)} existing invoices in master")
        
        # Step 2: Separate records into valid and invalid
        valid_records = []
        seen_missing_invoices = set()
        
        for record in invoice_details:
            invoice_no = record.get('invoice_no')
            
            if not invoice_no:
                results['error_records'].append({
                    'record': record,
                    'error': 'Missing invoice number'
                })
                results['failed'] += 1
                continue
            
            if invoice_no not in existing_invoices:
                # Track this missing invoice
                if invoice_no not in seen_missing_invoices:
                    seen_missing_invoices.add(invoice_no)
                    results['missing_invoices'].append(invoice_no)
                
                # Store the full record for download
                results['missing_invoice_records'].append({
                    'invoice_no': invoice_no,
                    'employee_id': record.get('employee_id'),
                    'employee_name': record.get('employee_name') or f"{record.get('first_name', '')} {record.get('last_name', '')}".strip(),
                    'work_date': record.get('work_date'),
                    'amount_total': record.get('amount_total', 0),
                    'reason': 'Invoice not found in master table'
                })
                results['missing_invoice_count'] += 1
                results['failed'] += 1
            else:
                valid_records.append(record)
        
        print(f"   📋 Validation complete:")
        print(f"      - Valid records: {len(valid_records)}")
        print(f"      - Missing invoices: {len(results['missing_invoices'])}")
        print(f"      - Other errors: {len(results['error_records'])}")
        
        # Step 3: Bulk insert valid records only
        if valid_records:
            inserted_count = 0
            batch_size = 1000
            total_batches = (len(valid_records) + batch_size - 1) // batch_size
            
            print(f"   🚀 Processing {len(valid_records)} valid records in {total_batches} batches...")
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(valid_records))
                batch = valid_records[start_idx:end_idx]
                
                print(f"   📦 Processing batch {batch_num + 1}/{total_batches} ({len(batch)} records)...")
                
                with conn:
                    with conn.cursor() as cursor:
                        for record in batch:
                            try:
                                # Clean the record data
                                clean_record = self.clean_record_data(record)
                                
                                # Build employee name if needed
                                employee_name = clean_record.get('employee_name')
                                if not employee_name:
                                    first = clean_record.get('first_name', '') or ''
                                    last = clean_record.get('last_name', '') or ''
                                    middle = clean_record.get('middle_initial', '') or ''
                                    
                                    if first or last:
                                        name_parts = [part for part in [first, middle, last] if part]
                                        employee_name = ' '.join(name_parts)
                                
                                # Insert the record
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
                                    clean_record.get('source_system', 'Unknown'),
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
                                
                                inserted_count += 1
                                
                            except psycopg2.IntegrityError as e:
                                # This is likely a duplicate - just skip it silently
                                logger.debug(f"Record already exists: {clean_record.get('invoice_no')}")
                                continue
                            except Exception as e:
                                # Log other errors
                                results['error_records'].append({
                                    'invoice_no': clean_record.get('invoice_no'),
                                    'employee_id': clean_record.get('employee_id'),
                                    'error': str(e)[:200]
                                })
                                results['failed'] += 1
                                logger.warning(f"Insert error: {e}")
                                continue
                
                print(f"      ✅ Batch {batch_num + 1} complete: {inserted_count} inserted so far")
            
            results['inserted'] = inserted_count
        
        conn.close()
        results['success'] = True
        
        # Final summary
        print(f"\n   🎯 Upload Summary:")
        print(f"      Total records: {results['total_records']:,}")
        print(f"      Inserted: {results['inserted']:,}")
        print(f"      Missing invoices: {results['missing_invoice_count']:,}")
        print(f"      Other errors: {len(results['error_records']):,}")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in fast validated bulk insert: {e}")
        results['success'] = False
        results['error_records'].append({'general_error': str(e)})
        return results
