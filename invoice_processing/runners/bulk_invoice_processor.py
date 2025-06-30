"""
Bulk Invoice Import Helper - Complete Version
Creates invoice-level review files and applies bulk decisions
"""

import pandas as pd
import os
from datetime import datetime
from typing import Dict, List, Tuple
import numpy as np

class BulkInvoiceProcessor:
    def __init__(self):
        """Initialize the bulk invoice processor"""
        print("📋 BULK INVOICE IMPORT HELPER")
        print("=" * 50)
    
    def analyze_missing_invoices(self, missing_file: str) -> pd.DataFrame:
        """Analyze missing records to identify complete vs partial invoices"""
        
        if not os.path.exists(missing_file):
            raise FileNotFoundError(f"Missing analysis file not found: {missing_file}")
        
        print(f"📂 Loading missing analysis: {missing_file}")
        df_missing = pd.read_csv(missing_file)
        
        # Fix date handling
        print("🔧 Processing dates...")
        if 'Work Date' in df_missing.columns:
            # Convert Work Date to datetime, handling errors
            df_missing['Work Date'] = pd.to_datetime(df_missing['Work Date'], errors='coerce')
            
            # Count invalid dates
            invalid_dates = df_missing['Work Date'].isna().sum()
            if invalid_dates > 0:
                print(f"   ⚠️  Found {invalid_dates} records with invalid/missing dates")
        
        # Group by invoice to see completeness status
        invoice_analysis = []
        
        for invoice_no, group in df_missing.groupby('Invoice Number'):
            # Analyze this invoice's missing records
            record_count = len(group)
            total_hours = group['Hours'].sum() if 'Hours' in group.columns else 0
            total_amount = group['Bill Amount'].sum() if 'Bill Amount' in group.columns else 0
            
            # Check if this is a complete missing invoice
            missing_types = group['Missing_Type'].unique() if 'Missing_Type' in group.columns else []
            priorities = group['Priority'].unique() if 'Priority' in group.columns else []
            
            is_complete_missing = 'Complete Invoice Missing' in missing_types
            has_high_priority = 'High' in priorities
            
            # Get unique employees and work dates
            unique_employees = group['Employee Number'].nunique() if 'Employee Number' in group.columns else 0
            
            # Handle date range safely
            if 'Work Date' in group.columns:
                valid_dates = group['Work Date'].dropna()
                if len(valid_dates) > 0:
                    unique_dates = valid_dates.nunique()
                    date_range = f"{valid_dates.min().strftime('%Y-%m-%d')} to {valid_dates.max().strftime('%Y-%m-%d')}"
                else:
                    unique_dates = 0
                    date_range = "No valid dates"
            else:
                unique_dates = 0
                date_range = "No date column"
            
            invoice_analysis.append({
                'Invoice_Number': invoice_no,
                'Record_Count': record_count,
                'Unique_Employees': unique_employees,
                'Unique_Work_Dates': unique_dates,
                'Date_Range': date_range,
                'Total_Hours': round(total_hours, 2),
                'Total_Amount': round(total_amount, 2),
                'Is_Complete_Missing': is_complete_missing,
                'Has_High_Priority': has_high_priority,
                'Missing_Types': ', '.join(map(str, missing_types)),
                'Priorities': ', '.join(map(str, priorities)),
                'Recommended_Action': self._get_recommendation(is_complete_missing, has_high_priority, record_count),
                'Auto_Import': '',  # For manual decision
                'Notes': ''  # For manual notes
            })
        
        analysis_df = pd.DataFrame(invoice_analysis)
        
        # Sort by priority and amount
        analysis_df = analysis_df.sort_values(['Has_High_Priority', 'Total_Amount'], ascending=[False, False])
        
        return analysis_df
    
    def _get_recommendation(self, is_complete: bool, is_high_priority: bool, record_count: int) -> str:
        """Get recommendation for invoice processing"""
        
        if is_complete and is_high_priority:
            return "IMPORT - Complete invoice, high priority"
        elif is_complete:
            return "IMPORT - Complete invoice"
        elif is_high_priority:
            return "REVIEW - High priority partial invoice"
        elif record_count > 20:
            return "REVIEW - Large partial invoice"
        else:
            return "CONSIDER - Small partial invoice"
    
    def create_invoice_review_file(self, missing_file: str) -> str:
        """Create invoice-level review file for bulk decisions"""
        
        try:
            # Analyze invoices
            analysis_df = self.analyze_missing_invoices(missing_file)
            
            # Create output filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            review_file = f'invoice_bulk_review_{timestamp}.csv'
            
            # Add summary stats
            total_invoices = len(analysis_df)
            complete_missing = analysis_df['Is_Complete_Missing'].sum()
            high_priority = analysis_df['Has_High_Priority'].sum()
            total_amount = analysis_df['Total_Amount'].sum()
            total_records = analysis_df['Record_Count'].sum()
            
            print(f"\n📊 INVOICE SUMMARY:")
            print(f"   Total invoices: {total_invoices:,}")
            print(f"   Complete missing: {complete_missing:,}")
            print(f"   High priority: {high_priority:,}")
            print(f"   Total records: {total_records:,}")
            print(f"   Total amount: ${total_amount:,.2f}")
            
            # Save review file
            analysis_df.to_csv(review_file, index=False)
            print(f"\n✅ Invoice review file created: {review_file}")
            
            # Show top priority items
            print(f"\n🔥 TOP PRIORITY INVOICES:")
            top_priority = analysis_df[analysis_df['Has_High_Priority'] == True].head(10)
            if len(top_priority) > 0:
                for _, inv in top_priority.iterrows():
                    print(f"   {inv['Invoice_Number']}: {inv['Record_Count']} records, ${inv['Total_Amount']:,.2f}")
            else:
                print("   No high priority invoices found")
            
            return review_file
            
        except Exception as e:
            print(f"\n❌ Error creating review file: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def apply_bulk_decisions(self, missing_file: str, review_file: str) -> str:
        """Apply bulk Auto_Import decisions from review file back to detail records"""
        
        print(f"\n📋 Applying bulk decisions...")
        print(f"   Missing file: {missing_file}")
        print(f"   Review file: {review_file}")
        
        try:
            # Load both files
            df_missing = pd.read_csv(missing_file)
            df_review = pd.read_csv(review_file)
            
            print(f"\n📊 Files loaded:")
            print(f"   Detail records: {len(df_missing):,}")
            print(f"   Invoices reviewed: {len(df_review):,}")
            
            # Check for Auto_Import column in review file
            if 'Auto_Import' not in df_review.columns:
                print("\n❌ ERROR: No 'Auto_Import' column found in review file!")
                print("   Please add Auto_Import column with TRUE/FALSE values")
                return None
            
            # Create Force_Import column in missing file
            df_missing['Force_Import'] = ''
            
            # Count decisions
            decisions = df_review['Auto_Import'].fillna('').astype(str).str.upper().value_counts()
            print(f"\n📊 Review decisions:")
            for decision, count in decisions.items():
                if decision:
                    print(f"   {decision}: {count} invoices")
            
            # Apply decisions by invoice
            applied_count = 0
            skipped_count = 0
            
            for _, review_row in df_review.iterrows():
                invoice_no = review_row['Invoice_Number']
                auto_import = str(review_row.get('Auto_Import', '')).upper()
                
                if auto_import in ['TRUE', 'FALSE']:
                    # Find all records for this invoice
                    mask = df_missing['Invoice Number'] == invoice_no
                    records_found = mask.sum()
                    
                    if records_found > 0:
                        df_missing.loc[mask, 'Force_Import'] = auto_import
                        applied_count += records_found
                        
                        if auto_import == 'TRUE':
                            amount = df_missing.loc[mask, 'Bill Amount'].sum()
                            print(f"   ✓ Invoice {invoice_no}: {records_found} records marked for import (${amount:,.2f})")
                    else:
                        print(f"   ⚠️  Invoice {invoice_no}: No matching records found")
                else:
                    skipped_count += 1
            
            # Summary
            print(f"\n📊 Application Summary:")
            print(f"   Records with Force_Import = TRUE: {(df_missing['Force_Import'] == 'TRUE').sum():,}")
            print(f"   Records with Force_Import = FALSE: {(df_missing['Force_Import'] == 'FALSE').sum():,}")
            print(f"   Records pending review: {(df_missing['Force_Import'] == '').sum():,}")
            
            # Save the updated file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = missing_file.replace('.csv', f'_bulk_processed_{timestamp}.csv')
            df_missing.to_csv(output_file, index=False)
            
            print(f"\n✅ Bulk decisions applied!")
            print(f"   Output file: {output_file}")
            print(f"\n🔍 Next step: Use force_import_processor.py to import records marked with Force_Import = TRUE")
            
            return output_file
            
        except Exception as e:
            print(f"\n❌ Error applying decisions: {e}")
            import traceback
            traceback.print_exc()
            return None

# Quick diagnostic function
def diagnose_missing_file(filename):
    """Quick diagnostic of the missing analysis file"""
    print(f"\n🔍 Diagnosing file: {filename}")
    
    try:
        df = pd.read_csv(filename)
        print(f"   Total rows: {len(df)}")
        print(f"   Columns: {list(df.columns)}")
        
        # Check Work Date column
        if 'Work Date' in df.columns:
            print(f"\n   Work Date analysis:")
            print(f"   - Data type: {df['Work Date'].dtype}")
            print(f"   - Sample values: {df['Work Date'].head(5).tolist()}")
            print(f"   - Null values: {df['Work Date'].isna().sum()}")
            
            # Try to parse dates
            dates_parsed = pd.to_datetime(df['Work Date'], errors='coerce')
            valid_dates = dates_parsed.notna().sum()
            print(f"   - Valid dates after parsing: {valid_dates}/{len(df)}")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")

# Main execution functions
def run_bulk_invoice_workflow(command: str, *args):
    """Main workflow dispatcher"""
    
    processor = BulkInvoiceProcessor()
    
    if command == 'create':
        if len(args) < 1:
            print("❌ Error: Missing required argument")
            print("Usage: python bulk_invoice_processor.py create <missing_analysis_file>")
            return
        
        missing_file = args[0]
        review_file = processor.create_invoice_review_file(missing_file)
        
        if review_file:
            print(f"\n🔍 NEXT STEPS:")
            print(f"1. Open {review_file} in Excel")
            print(f"2. Review the 'Recommended_Action' column")
            print(f"3. Set 'Auto_Import' = TRUE for invoices you want to import")
            print(f"4. Set 'Auto_Import' = FALSE for invoices to skip")
            print(f"5. Add any notes in the 'Notes' column")
            print(f"6. Save the file")
            print(f"7. Run: python bulk_invoice_processor.py apply {missing_file} {review_file}")
    
    elif command == 'apply':
        if len(args) < 2:
            print("❌ Error: Missing required arguments")
            print("Usage: python bulk_invoice_processor.py apply <missing_analysis_file> <review_file>")
            return
        
        missing_file = args[0]
        review_file = args[1]
        
        output_file = processor.apply_bulk_decisions(missing_file, review_file)
        
        if output_file:
            print(f"\n✅ SUCCESS! Bulk decisions applied")
            print(f"\n🚀 To import approved records to database:")
            print(f"   python force_import_processor.py --missing {output_file}")
    
    elif command == 'diagnose':
        if len(args) < 1:
            print("❌ Error: Missing required argument")
            print("Usage: python bulk_invoice_processor.py diagnose <file>")
            return
        
        diagnose_missing_file(args[0])
    
    else:
        print(f"❌ Unknown command: {command}")
        print("\nAvailable commands:")
        print("  create   - Create invoice-level review file")
        print("  apply    - Apply bulk decisions back to detail records")
        print("  diagnose - Diagnose file issues")

# Command line interface
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python bulk_invoice_processor.py create <missing_analysis_file>")
        print("  python bulk_invoice_processor.py apply <missing_analysis_file> <review_file>")
        print("  python bulk_invoice_processor.py diagnose <file>")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    args = sys.argv[2:] if len(sys.argv) > 2 else []
    
    run_bulk_invoice_workflow(command, *args)
