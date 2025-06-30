"""
AUS Invoice Analysis Suite
Comprehensive analysis of duplicate and missing AUS invoice records
"""

import pandas as pd
import psycopg2
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv
import numpy as np

# Load environment variables
load_dotenv()

class AUSInvoiceAnalyzer:
    def __init__(self):
        """Initialize the analyzer with database connection"""
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL not found in .env file")
        
        self.analysis_stats = {
            'duplicate_analysis': {},
            'missing_analysis': {},
            'recommendations': []
        }
        
        print("🔍 AUS INVOICE ANALYSIS SUITE")
        print("=" * 60)
    
    def load_data_sources(self) -> Tuple[pd.DataFrame, Dict]:
        """Load original file and database records for comparison"""
        
        print("\n📂 Loading data sources...")
        
        # Load original AUS file
        try:
            df_original = pd.read_csv('invoice_details_aus.csv')
            print(f"   ✓ Original file: {len(df_original):,} records")
        except FileNotFoundError:
            raise FileNotFoundError("invoice_details_aus.csv not found in current directory")
        
        # Filter out -ORG invoices
        org_mask = df_original['Invoice Number'].str.contains('-ORG', na=False)
        df_valid = df_original[~org_mask].copy()
        print(f"   ✓ Valid records (no -ORG): {len(df_valid):,}")
        
        # Load database records
        conn = psycopg2.connect(self.database_url)
        cur = conn.cursor()
        
        # Get all AUS records from database
        cur.execute("""
            SELECT invoice_no, employee_id, work_date, hours_regular, 
                   rate_regular, amount_regular, job_number
            FROM invoice_details
            WHERE source_system = 'AUS'
            ORDER BY invoice_no, employee_id, work_date;
        """)
        
        db_records = cur.fetchall()
        db_columns = ['Invoice Number', 'Employee Number', 'Work Date', 
                     'Hours', 'Bill Rate', 'Bill Amount', 'Job Number']
        df_database = pd.DataFrame(db_records, columns=db_columns)
        
        print(f"   ✓ Database records: {len(df_database):,}")
        
        # Create lookup dictionary for database records
        db_lookup = {}
        for _, row in df_database.iterrows():
            key = f"{row['Invoice Number']}_{row['Employee Number']}_{row['Work Date']}"
            db_lookup[key] = row.to_dict()
        
        cur.close()
        conn.close()
        
        return df_valid, db_lookup
    
    def analyze_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Comprehensive duplicate analysis with reversal detection"""
        
        print("\n🔍 Analyzing duplicate records...")
        
        # Define key columns for duplicate detection
        key_columns = ['Invoice Number', 'Employee Number', 'Work Date', 'Pay Hours Description']
        
        # Find all duplicates
        duplicate_mask = df.duplicated(subset=key_columns, keep=False)
        duplicates = df[duplicate_mask].copy()
        
        if len(duplicates) == 0:
            print("   ✅ No duplicates found")
            return pd.DataFrame()
        
        print(f"   📊 Found {len(duplicates)} duplicate records")
        
        # Group duplicates for analysis
        duplicate_groups = []
        
        for key_values, group in duplicates.groupby(key_columns):
            invoice_no, emp_no, work_date, pay_desc = key_values
            
            # Sort by hours to identify patterns
            group_sorted = group.sort_values('Hours')
            
            # Analyze the group
            analysis = self._analyze_duplicate_group(group_sorted, key_values)
            
            # Add each record with analysis
            for idx, record in group_sorted.iterrows():
                duplicate_groups.append({
                    # Original record data
                    **record.to_dict(),
                    
                    # Analysis fields
                    'Group_ID': f"{invoice_no}_{emp_no}_{work_date}",
                    'Group_Size': len(group_sorted),
                    'Pattern_Type': analysis['pattern_type'],
                    'Is_Reversal_Pair': analysis['is_reversal_pair'],
                    'Net_Hours': analysis['net_hours'],
                    'Net_Amount': analysis['net_amount'],
                    'Recommendation': analysis['recommendation'],
                    'Force_Import': '',  # Empty for manual review
                    'Review_Notes': analysis['notes'],
                    'Original_Row_Index': idx
                })
        
        duplicate_df = pd.DataFrame(duplicate_groups)
        
        # Sort by Group_ID for easier review
        duplicate_df = duplicate_df.sort_values(['Group_ID', 'Hours'])
        
        # Store stats
        self.analysis_stats['duplicate_analysis'] = {
            'total_duplicates': len(duplicates),
            'unique_groups': duplicate_df['Group_ID'].nunique(),
            'reversal_pairs': len(duplicate_df[duplicate_df['Is_Reversal_Pair'] == True]) // 2,
            'exact_duplicates': len(duplicate_df[duplicate_df['Pattern_Type'] == 'Exact Duplicate']),
            'other_patterns': len(duplicate_df[duplicate_df['Pattern_Type'] == 'Other'])
        }
        
        return duplicate_df
    
    def _analyze_duplicate_group(self, group: pd.DataFrame, key_values: Tuple) -> Dict:
        """Analyze a group of duplicate records to identify patterns"""
        
        hours_list = group['Hours'].tolist()
        amounts_list = group['Bill Amount'].tolist()
        
        # Check for exact duplicates
        if len(group.drop_duplicates()) == 1:
            return {
                'pattern_type': 'Exact Duplicate',
                'is_reversal_pair': False,
                'net_hours': sum(hours_list),
                'net_amount': sum(amounts_list),
                'recommendation': 'Keep one, remove duplicates',
                'notes': 'Identical records - likely data entry error'
            }
        
        # Check for reversal pattern (negative + positive)
        if len(group) == 2:
            hours_sorted = sorted(hours_list)
            amounts_sorted = sorted(amounts_list)
            
            # Check if we have a negative and positive pair that roughly cancel out
            if (hours_sorted[0] < 0 and hours_sorted[1] > 0 and 
                abs(hours_sorted[0] + hours_sorted[1]) < 0.1):
                return {
                    'pattern_type': 'Wage Adjustment',
                    'is_reversal_pair': True,
                    'net_hours': sum(hours_list),
                    'net_amount': sum(amounts_list),
                    'recommendation': 'Import both - valid adjustment',
                    'notes': f'Reversal: {hours_sorted[0]} hrs + {hours_sorted[1]} hrs = {sum(hours_list)} hrs'
                }
        
        # Check for multiple adjustments
        if any(h < 0 for h in hours_list):
            return {
                'pattern_type': 'Multiple Adjustments',
                'is_reversal_pair': False,
                'net_hours': sum(hours_list),
                'net_amount': sum(amounts_list),
                'recommendation': 'Review - complex adjustment',
                'notes': f'Multiple entries with negatives - net: {sum(hours_list)} hrs, ${sum(amounts_list):.2f}'
            }
        
        # Other pattern
        return {
            'pattern_type': 'Other',
            'is_reversal_pair': False,
            'net_hours': sum(hours_list),
            'net_amount': sum(amounts_list),
            'recommendation': 'Manual review required',
            'notes': f'{len(group)} records - unclear pattern'
        }
    
    def analyze_missing_records(self, df_valid: pd.DataFrame, db_lookup: Dict) -> pd.DataFrame:
        """Analyze missing records and categorize them"""
        
        print("\n🔍 Analyzing missing records...")
        
        missing_records = []
        partial_invoices = {}
        
        for idx, record in df_valid.iterrows():
            # Create lookup key
            key = f"{record['Invoice Number']}_{record['Employee Number']}_{record['Work Date']}"
            
            if key not in db_lookup:
                # Record is missing from database
                missing_analysis = self._analyze_missing_record(record, df_valid, db_lookup)
                
                missing_records.append({
                    **record.to_dict(),
                    'Missing_Type': missing_analysis['missing_type'],
                    'Invoice_Status': missing_analysis['invoice_status'],
                    'Recommendation': missing_analysis['recommendation'],
                    'Priority': missing_analysis['priority'],
                    'Notes': missing_analysis['notes'],
                    'Original_Row_Index': idx
                })
                
                # Track partial invoice status
                invoice_no = record['Invoice Number']
                if invoice_no not in partial_invoices:
                    partial_invoices[invoice_no] = {
                        'total_in_file': 0,
                        'missing_count': 0,
                        'in_db_count': 0
                    }
                partial_invoices[invoice_no]['missing_count'] += 1
        
        # Count total records per invoice in original file
        invoice_totals = df_valid['Invoice Number'].value_counts()
        for invoice_no, total in invoice_totals.items():
            if invoice_no in partial_invoices:
                partial_invoices[invoice_no]['total_in_file'] = total
                partial_invoices[invoice_no]['in_db_count'] = total - partial_invoices[invoice_no]['missing_count']
        
        missing_df = pd.DataFrame(missing_records)
        
        if len(missing_df) > 0:
            # Sort by priority and invoice number
            priority_order = {'High': 1, 'Medium': 2, 'Low': 3}
            missing_df['Priority_Sort'] = missing_df['Priority'].map(priority_order)
            missing_df = missing_df.sort_values(['Priority_Sort', 'Invoice Number', 'Employee Number'])
            missing_df = missing_df.drop('Priority_Sort', axis=1)
        
        # Store analysis stats
        self.analysis_stats['missing_analysis'] = {
            'total_missing': len(missing_records),
            'completely_missing_invoices': sum(1 for inv in partial_invoices.values() if inv['in_db_count'] == 0),
            'partially_missing_invoices': sum(1 for inv in partial_invoices.values() if 0 < inv['in_db_count'] < inv['total_in_file']),
            'missing_by_type': missing_df['Missing_Type'].value_counts().to_dict() if len(missing_df) > 0 else {}
        }
        
        return missing_df
    
    def _analyze_missing_record(self, record: pd.Series, df_all: pd.DataFrame, db_lookup: Dict) -> Dict:
        """Analyze why a specific record might be missing"""
    
        invoice_no = record['Invoice Number']
        
        # Check if entire invoice is missing
        invoice_records = df_all[df_all['Invoice Number'] == invoice_no]
        records_in_db = sum(1 for _, r in invoice_records.iterrows() 
                        if f"{r['Invoice Number']}_{r['Employee Number']}_{r['Work Date']}" in db_lookup)
        
        if records_in_db == 0:
            missing_type = 'Complete Invoice Missing'
            priority = 'High'
            recommendation = 'Load entire invoice'
            notes = f'Invoice {invoice_no} completely missing from database'
        elif records_in_db < len(invoice_records):
            missing_type = 'Partial Invoice Missing'
            priority = 'Medium'
            recommendation = 'Load missing records'
            notes = f'Invoice {invoice_no}: {records_in_db}/{len(invoice_records)} records loaded'
        else:
            missing_type = 'Individual Record Missing'
            priority = 'Low'
            recommendation = 'Investigate individual record'
            notes = 'Single record missing from otherwise complete invoice'
        
        # Check for data quality issues with enhanced logic
        if pd.isna(record.get('Work Date')):
            # Check if this is a valid adjustment/special charge
            post_desc = str(record.get('Post Description', '')).lower()
            
            # Keywords that indicate valid non-hourly charges
            valid_keywords = ['adj', 'adjustment', 'special', 'uniforms', 'leave', 'payout', 'sick']
            
            if any(keyword in post_desc for keyword in valid_keywords):
                # This is a valid non-hourly charge
                missing_type = 'Valid Non-Hourly Charge'
                priority = 'Medium'  # Still important to load
                recommendation = 'Load - valid adjustment/special charge'
                notes = f'No work date required - Post Description: {record.get("Post Description", "")}'
            else:
                # This is likely a data quality issue
                missing_type = 'Data Quality Issue'
                priority = 'Low'
                recommendation = 'Fix data quality, then reload'
                notes = 'Missing work date - not identified as adjustment'
        elif record.get('Hours', 0) == 0:
            # Check if zero hours is valid (could be an adjustment)
            post_desc = str(record.get('Post Description', '')).lower()
            valid_keywords = ['adj', 'adjustment', 'special', 'uniforms', 'leave', 'payout', 'sick']
            
            if any(keyword in post_desc for keyword in valid_keywords):
                missing_type = 'Valid Zero-Hour Charge'
                priority = 'Medium'
                recommendation = 'Load - valid non-hourly charge'
                notes = f'Zero hours acceptable - Post Description: {record.get("Post Description", "")}'
            else:
                missing_type = 'Zero Hours Record'
                priority = 'Low'
                recommendation = 'Review if should be loaded'
                notes = 'Record has zero hours - review if intentional'
        
        return {
            'missing_type': missing_type,
            'invoice_status': f'{records_in_db}/{len(invoice_records)} loaded',
            'recommendation': recommendation,
            'priority': priority,
            'notes': notes
        }
    
    def generate_recommendations(self) -> List[str]:
        """Generate overall recommendations based on analysis"""
        
        recommendations = []
        
        # Duplicate recommendations
        dup_stats = self.analysis_stats['duplicate_analysis']
        if dup_stats:
            if dup_stats['reversal_pairs'] > 0:
                recommendations.append(
                    f"✅ {dup_stats['reversal_pairs']} reversal pairs identified - these are valid wage adjustments"
                )
            
            if dup_stats['exact_duplicates'] > 0:
                recommendations.append(
                    f"⚠️ {dup_stats['exact_duplicates']} exact duplicates found - keep one copy of each"
                )
        
        # Missing records recommendations
        miss_stats = self.analysis_stats['missing_analysis']
        if miss_stats:
            if miss_stats['completely_missing_invoices'] > 0:
                recommendations.append(
                    f"🔴 {miss_stats['completely_missing_invoices']} invoices completely missing - HIGH PRIORITY"
                )
            
            if miss_stats['partially_missing_invoices'] > 0:
                recommendations.append(
                    f"🟡 {miss_stats['partially_missing_invoices']} invoices partially loaded - MEDIUM PRIORITY"
                )
        
        # Processing recommendations
        total_to_process = (dup_stats.get('total_duplicates', 0) + 
                           miss_stats.get('total_missing', 0))
        
        if total_to_process > 0:
            recommendations.append(
                f"📋 Total records to review: {total_to_process:,}"
            )
            recommendations.append(
                "🔄 Process duplicates first (wage adjustments), then missing records"
            )
        
        self.analysis_stats['recommendations'] = recommendations
        return recommendations
    
    def export_analysis_results(self) -> Dict[str, str]:
        """Export all analysis results to CSV files"""
        
        print("\n📤 Exporting analysis results...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        exported_files = {}
        
        # Load data
        df_valid, db_lookup = self.load_data_sources()
        
        # 1. Export duplicate analysis
        duplicate_df = self.analyze_duplicates(df_valid)
        if len(duplicate_df) > 0:
            dup_file = f'duplicate_analysis_{timestamp}.csv'
            duplicate_df.to_csv(dup_file, index=False)
            exported_files['duplicates'] = dup_file
            print(f"   ✓ Duplicate analysis: {dup_file}")
        
        # 2. Export missing records analysis
        missing_df = self.analyze_missing_records(df_valid, db_lookup)
        if len(missing_df) > 0:
            miss_file = f'missing_analysis_{timestamp}.csv'
            missing_df.to_csv(miss_file, index=False)
            exported_files['missing'] = miss_file
            print(f"   ✓ Missing records analysis: {miss_file}")
        
        # 3. Export summary report
        recommendations = self.generate_recommendations()
        
        summary_content = f"""
AUS INVOICE COMPREHENSIVE ANALYSIS
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*60}

DUPLICATE ANALYSIS:
{'-'*30}
Total duplicate records: {self.analysis_stats['duplicate_analysis'].get('total_duplicates', 0):,}
Unique duplicate groups: {self.analysis_stats['duplicate_analysis'].get('unique_groups', 0):,}
Reversal pairs (adjustments): {self.analysis_stats['duplicate_analysis'].get('reversal_pairs', 0):,}
Exact duplicates: {self.analysis_stats['duplicate_analysis'].get('exact_duplicates', 0):,}

MISSING RECORDS ANALYSIS:
{'-'*30}
Total missing records: {self.analysis_stats['missing_analysis'].get('total_missing', 0):,}
Completely missing invoices: {self.analysis_stats['missing_analysis'].get('completely_missing_invoices', 0):,}
Partially missing invoices: {self.analysis_stats['missing_analysis'].get('partially_missing_invoices', 0):,}

RECOMMENDATIONS:
{'-'*30}
"""
        
        for i, rec in enumerate(recommendations, 1):
            summary_content += f"{i}. {rec}\n"
        
        summary_content += f"""

NEXT STEPS:
{'-'*30}
1. Review duplicate_analysis_{timestamp}.csv
   - Set Force_Import = TRUE for records to import
   - Wage adjustments (reversal pairs) should typically be imported
   - Exact duplicates should have only one copy imported

2. Review missing_analysis_{timestamp}.csv
   - Focus on High priority items first
   - Complete missing invoices need immediate attention
   - Partial invoices may indicate load errors

3. Use the force_import_processor.py script to import approved records

FILES CREATED:
{'-'*30}
"""
        
        for file_type, filename in exported_files.items():
            summary_content += f"- {file_type}: {filename}\n"
        
        # Save summary with proper encoding
        summary_file = f'analysis_summary_{timestamp}.txt'
        with open(summary_file, 'w', encoding='utf-8', errors='replace') as f:
            f.write(summary_content)
        
        exported_files['summary'] = summary_file
        print(f"   ✓ Analysis summary: {summary_file}")
        
        return exported_files
    
    def run_complete_analysis(self) -> Dict[str, str]:
        """Run the complete analysis workflow"""
        
        print("🚀 Starting comprehensive AUS invoice analysis...")
        
        try:
            # Export all results
            exported_files = self.export_analysis_results()
            
            print("\n" + "="*60)
            print("✅ ANALYSIS COMPLETE!")
            print("="*60)
            
            # Print summary stats
            print(f"\nKEY FINDINGS:")
            if self.analysis_stats['duplicate_analysis']:
                dup_stats = self.analysis_stats['duplicate_analysis']
                print(f"📊 Duplicates: {dup_stats['total_duplicates']} records in {dup_stats['unique_groups']} groups")
                print(f"   - Reversal pairs: {dup_stats['reversal_pairs']}")
                print(f"   - Exact duplicates: {dup_stats['exact_duplicates']}")
            
            if self.analysis_stats['missing_analysis']:
                miss_stats = self.analysis_stats['missing_analysis']
                print(f"📊 Missing: {miss_stats['total_missing']} records")
                print(f"   - Complete invoices: {miss_stats['completely_missing_invoices']}")
                print(f"   - Partial invoices: {miss_stats['partially_missing_invoices']}")
            
            print(f"\n📁 Files created:")
            for file_type, filename in exported_files.items():
                print(f"   {file_type}: {filename}")
            
            return exported_files
            
        except Exception as e:
            print(f"\n❌ Error during analysis: {e}")
            import traceback
            traceback.print_exc()
            return {}

# Main execution
if __name__ == "__main__":
    analyzer = AUSInvoiceAnalyzer()
    result_files = analyzer.run_complete_analysis()
