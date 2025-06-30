"""
Enhanced Database Manager Methods - Post Upload Validation
Replace the bulk_insert methods in your database_manager_compatible.py
"""

import psycopg2
import pandas as pd
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DatabaseManagerEnhancements:
    """
    Add these methods to your CompatibleEnhancedDatabaseManager class
    """
    
    def bulk_insert_invoice_details_no_validation(self, invoice_details: List[Dict]) -> Dict:
        """
        Fast bulk insert without duplicate checking
        Returns detailed results including any errors
        """
        results = {
            'total_records': len(invoice_details),
            'inserted': 0,
            'failed': 0,
            'errors': [],
            'success': False
        }
        
        if not invoice_details:
            results['success'] = True
            return results
        
        try:
            conn = psycopg2.connect(self.database_url)
            inserted_count = 0
            failed_count = 0
            
            # Process records in batches for better performance
            batch_size = 1000
            total_batches = (len(invoice_details) + batch_size - 1) // batch_size
            
            print(f"   🚀 Processing {len(invoice_details)} records in {total_batches} batches...")
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(invoice_details))
                batch = invoice_details[start_idx:end_idx]
                
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
                                
                                # Simple insert without duplicate checking
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
                                
                            except psycopg2.Error as e:
                                failed_count += 1
                                error_msg = str(e)[:200]  # Truncate long error messages
                                
                                # Only log unique constraint violations at debug level
                                if "duplicate key value violates unique constraint" in error_msg:
                                    logger.debug(f"Duplicate record skipped: {clean_record.get('invoice_no')}")
                                else:
                                    logger.warning(f"Insert error: {error_msg}")
                                    results['errors'].append({
                                        'invoice_no': clean_record.get('invoice_no'),
                                        'employee_id': clean_record.get('employee_id'),
                                        'error': error_msg
                                    })
                                continue
                
                print(f"      ✅ Batch {batch_num + 1} complete: {inserted_count} inserted so far")
            
            conn.close()
            
            results['inserted'] = inserted_count
            results['failed'] = failed_count
            results['success'] = True
            
            print(f"   🎯 Upload complete: {inserted_count:,} records inserted, {failed_count:,} failed")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in bulk insert: {e}")
            results['success'] = False
            results['errors'].append({'general_error': str(e)})
            return results
    
    def validate_invoice_totals(self) -> Dict:
        """
        Post-upload validation: Compare invoice_details totals against invoices table
        Groups by invoice_number and sums amount_total, comparing to invoice_total
        """
        validation_results = {
            'total_invoices_checked': 0,
            'matching_invoices': 0,
            'mismatched_invoices': 0,
            'missing_in_details': 0,
            'discrepancies': [],
            'missing_invoices': [],
            'validation_timestamp': datetime.now()
        }
        
        try:
            conn = psycopg2.connect(self.database_url)
            
            with conn.cursor() as cursor:
                # Get invoice totals from both tables
                cursor.execute("""
                    WITH invoice_detail_totals AS (
                        SELECT 
                            invoice_no,
                            SUM(amount_total) as detail_total,
                            COUNT(*) as detail_count,
                            STRING_AGG(DISTINCT source_system, ', ') as sources
                        FROM invoice_details
                        GROUP BY invoice_no
                    ),
                    invoice_master AS (
                        SELECT 
                            invoice_no,
                            invoice_total,
                            customer,
                            processing_type
                        FROM invoices
                        WHERE invoice_no IS NOT NULL
                    )
                    SELECT 
                        im.invoice_no,
                        im.invoice_total,
                        COALESCE(idt.detail_total, 0) as detail_total,
                        COALESCE(idt.detail_count, 0) as detail_count,
                        idt.sources,
                        im.customer,
                        im.processing_type,
                        CASE 
                            WHEN idt.invoice_no IS NULL THEN 'MISSING_DETAILS'
                            WHEN ABS(im.invoice_total - idt.detail_total) < 0.01 THEN 'MATCH'
                            ELSE 'MISMATCH'
                        END as status
                    FROM invoice_master im
                    LEFT JOIN invoice_detail_totals idt ON im.invoice_no = idt.invoice_no
                    ORDER BY 
                        CASE 
                            WHEN idt.invoice_no IS NULL THEN 1
                            WHEN ABS(im.invoice_total - idt.detail_total) >= 0.01 THEN 2
                            ELSE 3
                        END,
                        ABS(im.invoice_total - COALESCE(idt.detail_total, 0)) DESC
                """)
                
                results = cursor.fetchall()
                
                for row in results:
                    invoice_no, invoice_total, detail_total, detail_count, sources, customer, proc_type, status = row
                    validation_results['total_invoices_checked'] += 1
                    
                    if status == 'MATCH':
                        validation_results['matching_invoices'] += 1
                    elif status == 'MISSING_DETAILS':
                        validation_results['missing_in_details'] += 1
                        validation_results['missing_invoices'].append({
                            'invoice_no': invoice_no,
                            'invoice_total': float(invoice_total),
                            'customer': customer,
                            'processing_type': proc_type
                        })
                    else:  # MISMATCH
                        validation_results['mismatched_invoices'] += 1
                        discrepancy = float(invoice_total) - float(detail_total)
                        validation_results['discrepancies'].append({
                            'invoice_no': invoice_no,
                            'invoice_total': float(invoice_total),
                            'detail_total': float(detail_total),
                            'discrepancy': discrepancy,
                            'discrepancy_pct': (discrepancy / float(invoice_total) * 100) if invoice_total != 0 else 0,
                            'detail_count': detail_count,
                            'sources': sources,
                            'customer': customer,
                            'processing_type': proc_type
                        })
            
            conn.close()
            
            # Sort discrepancies by absolute amount
            validation_results['discrepancies'].sort(key=lambda x: abs(x['discrepancy']), reverse=True)
            
            # Add summary statistics
            if validation_results['discrepancies']:
                total_discrepancy = sum(d['discrepancy'] for d in validation_results['discrepancies'])
                validation_results['total_discrepancy_amount'] = total_discrepancy
                validation_results['largest_discrepancy'] = validation_results['discrepancies'][0]
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Error in invoice validation: {e}")
            validation_results['error'] = str(e)
            return validation_results
    
    def get_validation_summary_report(self, validation_results: Dict) -> str:
        """
        Generate a formatted summary report of validation results
        """
        report = []
        report.append("\n" + "="*60)
        report.append("INVOICE VALIDATION REPORT")
        report.append(f"Generated: {validation_results['validation_timestamp']}")
        report.append("="*60)
        
        report.append(f"\n📊 SUMMARY:")
        report.append(f"   Total Invoices Checked: {validation_results['total_invoices_checked']:,}")
        report.append(f"   ✅ Matching: {validation_results['matching_invoices']:,}")
        report.append(f"   ❌ Mismatched: {validation_results['mismatched_invoices']:,}")
        report.append(f"   ⚠️  Missing Details: {validation_results['missing_in_details']:,}")
        
        if validation_results.get('total_discrepancy_amount'):
            report.append(f"\n💰 FINANCIAL IMPACT:")
            report.append(f"   Total Discrepancy: ${validation_results['total_discrepancy_amount']:,.2f}")
            
            largest = validation_results['largest_discrepancy']
            report.append(f"   Largest Discrepancy: Invoice {largest['invoice_no']} - ${largest['discrepancy']:,.2f}")
        
        if validation_results['mismatched_invoices'] > 0:
            report.append(f"\n📋 TOP 10 DISCREPANCIES:")
            report.append(f"{'Invoice No':<15} {'Master Total':>12} {'Detail Total':>12} {'Difference':>12} {'%':>6}")
            report.append("-" * 60)
            
            for disc in validation_results['discrepancies'][:10]:
                report.append(
                    f"{disc['invoice_no']:<15} "
                    f"${disc['invoice_total']:>11,.2f} "
                    f"${disc['detail_total']:>11,.2f} "
                    f"${disc['discrepancy']:>11,.2f} "
                    f"{disc['discrepancy_pct']:>5.1f}%"
                )
        
        if validation_results['missing_in_details'] > 0:
            report.append(f"\n⚠️  INVOICES WITHOUT DETAILS (First 10):")
            for inv in validation_results['missing_invoices'][:10]:
                report.append(f"   {inv['invoice_no']} - ${inv['invoice_total']:,.2f} ({inv['processing_type']})")
        
        report.append("\n" + "="*60)
        
        return '\n'.join(report)
    
    def export_validation_results(self, validation_results: Dict, filename: str = None) -> str:
        """
        Export validation results to Excel file
        """
        if filename is None:
            filename = f"invoice_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Summary sheet
                summary_data = {
                    'Metric': [
                        'Total Invoices Checked',
                        'Matching Invoices',
                        'Mismatched Invoices',
                        'Missing Invoice Details',
                        'Total Discrepancy Amount'
                    ],
                    'Count': [
                        validation_results['total_invoices_checked'],
                        validation_results['matching_invoices'],
                        validation_results['mismatched_invoices'],
                        validation_results['missing_in_details'],
                        validation_results.get('total_discrepancy_amount', 0)
                    ]
                }
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
                
                # Discrepancies sheet
                if validation_results['discrepancies']:
                    pd.DataFrame(validation_results['discrepancies']).to_excel(
                        writer, sheet_name='Discrepancies', index=False
                    )
                
                # Missing details sheet
                if validation_results['missing_invoices']:
                    pd.DataFrame(validation_results['missing_invoices']).to_excel(
                        writer, sheet_name='Missing Details', index=False
                    )
            
            print(f"   📊 Validation results exported to: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error exporting validation results: {e}")
            return None
