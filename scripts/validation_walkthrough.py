"""
Step-by-Step Validation and Implementation Walkthrough
Helps validate master lookup coverage and get started with the unified system
"""

import pandas as pd
import os
from datetime import datetime
from typing import Dict, List, Tuple
import matplotlib.pyplot as plt
import seaborn as sns
from enhanced_lookup_manager import EnhancedLookupManager
from invoice_transformer import InvoiceTransformer


class ValidationWalkthrough:
    def __init__(self):
        """Initialize validation walkthrough"""
        print("="*60)
        print("INVOICE DATA UNIFICATION - VALIDATION WALKTHROUGH")
        print("="*60)
        print()
        
        self.results = {
            'master_lookup_validation': {},
            'sample_transformation': {},
            'data_quality_checks': {}
        }
    
    def step1_validate_master_lookup(self):
        """Step 1: Validate Master Lookup Coverage"""
        print("\n📋 STEP 1: Validating Master Lookup Coverage")
        print("-" * 50)
        
        try:
            # Initialize lookup manager
            print("Loading reference data...")
            lookup_manager = EnhancedLookupManager()
            
            # Get statistics
            print(f"\n✅ Loaded successfully!")
            print(f"   - AUS job mappings: {len(lookup_manager.aus_job_lookup)}")
            print(f"   - Building codes: {len(lookup_manager.consolidated_building_lookup)}")
            print(f"   - EMID codes: {len(lookup_manager.emid_lookup)}")
            
            # Check for issues
            validation_issues = lookup_manager.validate_mappings()
            
            print("\n🔍 Validation Results:")
            for issue_type, items in validation_issues.items():
                if items:
                    print(f"\n⚠️  {issue_type}: {len(items)} issues")
                    # Show first 5 examples
                    for item in items[:5]:
                        print(f"     - {item}")
                    if len(items) > 5:
                        print(f"     ... and {len(items) - 5} more")
                else:
                    print(f"\n✅ {issue_type}: No issues found")
            
            # Test some specific job lookups
            print("\n🧪 Testing Sample Job Number Lookups:")
            test_jobs = ['207168', '281084T', '207169', '999999']  # Including one that shouldn't exist
            
            for job in test_jobs:
                result = lookup_manager.lookup_aus_job_info(job)
                if result:
                    print(f"   ✅ {job} → Building: {result.get('building_code')}, "
                          f"EMID: {result.get('emid')}")
                else:
                    print(f"   ❌ {job} → Not found in master lookup")
            
            # Store results
            self.results['master_lookup_validation'] = {
                'total_mappings': len(lookup_manager.aus_job_lookup),
                'validation_issues': validation_issues,
                'test_results': test_jobs
            }
            
            # Export dimension tables
            print("\n📊 Exporting consolidated dimension tables...")
            lookup_manager.export_dimension_tables("consolidated_dimensions.xlsx")
            print("   ✅ Exported to: consolidated_dimensions.xlsx")
            
            return lookup_manager
            
        except Exception as e:
            print(f"\n❌ Error in Step 1: {str(e)}")
            return None
    
    def step2_test_sample_transformation(self, lookup_manager=None):
        """Step 2: Test transformation with sample data"""
        print("\n\n🔄 STEP 2: Testing Data Transformation")
        print("-" * 50)
        
        try:
            # Initialize transformer
            if not lookup_manager:
                print("Initializing transformer with enhanced lookups...")
                transformer = InvoiceTransformer()
            else:
                print("Using existing lookup manager...")
                transformer = InvoiceTransformer()
            
            # Check if sample files exist
            bci_file = "invoice_details_bci.csv"
            aus_file = "invoice_details_aus.csv"
            
            files_found = []
            if os.path.exists(bci_file):
                files_found.append(("BCI", bci_file))
            if os.path.exists(aus_file):
                files_found.append(("AUS", aus_file))
            
            if not files_found:
                print("❌ No sample invoice files found!")
                print("   Please ensure invoice_details_bci.csv and/or invoice_details_aus.csv are present")
                return None
            
            print(f"\n📁 Found {len(files_found)} sample file(s)")
            
            # Process small samples first
            print("\n🧪 Processing sample records (first 100 from each file)...")
            
            sample_results = {}
            
            for file_type, filepath in files_found:
                print(f"\n   Processing {file_type} sample...")
                
                # Read sample
                if filepath.endswith('.csv'):
                    df = pd.read_csv(filepath, nrows=100)
                else:
                    df = pd.read_excel(filepath, nrows=100)
                
                # Transform
                if file_type == "BCI":
                    # Transform each row
                    transformed_rows = []
                    errors = []
                    
                    for idx, row in df.iterrows():
                        try:
                            unified = transformer.transform_bci_row(row, idx, filepath)
                            if unified:
                                transformed_rows.append(unified.to_dict())
                        except Exception as e:
                            errors.append(f"Row {idx}: {str(e)}")
                    
                    sample_results[file_type] = {
                        'original_rows': len(df),
                        'transformed_rows': len(transformed_rows),
                        'errors': len(errors)
                    }
                    
                else:  # AUS
                    # Transform each row
                    transformed_rows = []
                    errors = []
                    
                    for idx, row in df.iterrows():
                        try:
                            unified = transformer.transform_aus_row(row, idx, filepath)
                            if unified:
                                transformed_rows.append(unified.to_dict())
                        except Exception as e:
                            errors.append(f"Row {idx}: {str(e)}")
                    
                    sample_results[file_type] = {
                        'original_rows': len(df),
                        'transformed_rows': len(transformed_rows),
                        'errors': len(errors)
                    }
                
                print(f"      ✅ Transformed: {sample_results[file_type]['transformed_rows']}/{sample_results[file_type]['original_rows']} rows")
                if sample_results[file_type]['errors'] > 0:
                    print(f"      ⚠️  Errors: {sample_results[file_type]['errors']}")
            
            # Show unmapped records
            print("\n📍 Unmapped Records Summary:")
            if transformer.transformation_stats['unmapped_aus_jobs']:
                print(f"   AUS Jobs without mapping: {len(transformer.transformation_stats['unmapped_aus_jobs'])}")
                for job in list(transformer.transformation_stats['unmapped_aus_jobs'])[:5]:
                    print(f"      - {job}")
            else:
                print("   ✅ All AUS jobs mapped successfully!")
            
            if transformer.transformation_stats['unmapped_bci_locations']:
                print(f"   BCI Locations without mapping: {len(transformer.transformation_stats['unmapped_bci_locations'])}")
                for loc in list(transformer.transformation_stats['unmapped_bci_locations'])[:5]:
                    print(f"      - {loc}")
            else:
                print("   ✅ All BCI locations mapped successfully!")
            
            self.results['sample_transformation'] = sample_results
            
            return transformer
            
        except Exception as e:
            print(f"\n❌ Error in Step 2: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def step3_process_full_sample_month(self, transformer=None):
        """Step 3: Process a full sample month"""
        print("\n\n📅 STEP 3: Processing Full Sample Month")
        print("-" * 50)
        
        try:
            if not transformer:
                transformer = InvoiceTransformer()
            
            # Check for files
            bci_file = "invoice_details_bci.csv"
            aus_file = "invoice_details_aus.csv"
            
            print("🔄 Processing complete files...")
            print("   This may take a moment for 25K+ records...")
            
            # Process with timing
            import time
            
            # Process BCI
            if os.path.exists(bci_file):
                print(f"\n   Processing BCI file...")
                start_time = time.time()
                bci_unified = transformer.transform_bci_file(bci_file)
                bci_time = time.time() - start_time
                print(f"      ✅ Processed {len(bci_unified)} records in {bci_time:.2f} seconds")
                print(f"      📊 Rate: {len(bci_unified)/bci_time:.0f} records/second")
            else:
                bci_unified = pd.DataFrame()
            
            # Process AUS
            if os.path.exists(aus_file):
                print(f"\n   Processing AUS file...")
                start_time = time.time()
                aus_unified = transformer.transform_aus_file(aus_file)
                aus_time = time.time() - start_time
                print(f"      ✅ Processed {len(aus_unified)} records in {aus_time:.2f} seconds")
                print(f"      📊 Rate: {len(aus_unified)/aus_time:.0f} records/second")
            else:
                aus_unified = pd.DataFrame()
            
            # Combine and analyze
            print("\n📊 Combining and analyzing data...")
            combined_df, analysis_stats = transformer.combine_and_analyze(bci_file, aus_file)
            
            # Display results
            print("\n✨ TRANSFORMATION COMPLETE!")
            print(f"   Total records: {analysis_stats['total_records']:,}")
            print(f"   BCI records: {analysis_stats['bci_records']:,}")
            print(f"   AUS records: {analysis_stats['aus_records']:,}")
            print(f"   Date range: {analysis_stats['date_range']['start']} to {analysis_stats['date_range']['end']}")
            print(f"   Total hours: {analysis_stats['total_hours']:,.2f}")
            print(f"   Total billing: ${analysis_stats['total_billing']:,.2f}")
            
            print("\n💼 Pay Type Distribution:")
            for pay_type, count in analysis_stats['pay_type_distribution'].items():
                print(f"   {pay_type}: {count:,} records")
            
            # Export results
            output_file = f"unified_sample_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            print(f"\n💾 Exporting unified data to {output_file}...")
            transformer.export_unified_data(combined_df, output_file)
            print("   ✅ Export complete!")
            
            # Store results
            self.results['full_month_processing'] = analysis_stats
            
            return combined_df
            
        except Exception as e:
            print(f"\n❌ Error in Step 3: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def step4_data_quality_analysis(self, unified_df):
        """Step 4: Perform data quality analysis"""
        print("\n\n🔍 STEP 4: Data Quality Analysis")
        print("-" * 50)
        
        if unified_df is None or unified_df.empty:
            print("❌ No unified data available for analysis")
            return
        
        try:
            quality_checks = {
                'missing_building_codes': 0,
                'missing_emid': 0,
                'invoice_revisions': [],
                'high_hours_records': [],
                'negative_amounts': [],
                'coverage_by_source': {}
            }
            
            # Check for missing dimensional data
            print("\n📍 Checking dimensional data completeness...")
            
            missing_building = unified_df[unified_df['building_code'].isna()]
            quality_checks['missing_building_codes'] = len(missing_building)
            print(f"   Building codes missing: {len(missing_building)} ({len(missing_building)/len(unified_df)*100:.1f}%)")
            
            missing_emid = unified_df[unified_df['emid'].isna()]
            quality_checks['missing_emid'] = len(missing_emid)
            print(f"   EMID missing: {len(missing_emid)} ({len(missing_emid)/len(unified_df)*100:.1f}%)")
            
            # Check for invoice revisions
            print("\n📄 Checking for invoice revisions...")
            revision_pattern = r'[A-Za-z]+$'  # Letters at the end
            
            # Since invoice_number is now a string, we can check for revisions
            potential_revisions = unified_df[
                unified_df['invoice_number'].str.contains(revision_pattern, regex=True, na=False)
            ]
            
            if len(potential_revisions) > 0:
                print(f"   ⚠️  Found {len(potential_revisions)} potential revision invoices")
                sample_revisions = potential_revisions['invoice_number'].unique()[:5]
                for inv in sample_revisions:
                    print(f"      - {inv}")
                quality_checks['invoice_revisions'] = potential_revisions['invoice_number'].unique().tolist()
            else:
                print("   ✅ No invoice revisions detected in this sample")
            
            # Check for data anomalies
            print("\n🚨 Checking for data anomalies...")
            
            # High hours (>24 in a day)
            high_hours = unified_df[unified_df['hours_quantity'] > 24]
            if len(high_hours) > 0:
                print(f"   ⚠️  Records with >24 hours: {len(high_hours)}")
                quality_checks['high_hours_records'] = high_hours[['invoice_line_id', 'hours_quantity']].to_dict('records')
            else:
                print("   ✅ No excessive hours found")
            
            # Negative amounts
            negative_amounts = unified_df[unified_df['bill_amount'] < 0]
            if len(negative_amounts) > 0:
                print(f"   ⚠️  Records with negative amounts: {len(negative_amounts)}")
                quality_checks['negative_amounts'] = negative_amounts[['invoice_line_id', 'bill_amount']].to_dict('records')
            else:
                print("   ✅ No negative amounts found")
            
            # Coverage analysis by source
            print("\n📊 Coverage Analysis by Source System:")
            for source in ['BCI', 'AUS']:
                source_df = unified_df[unified_df['source_system'] == source]
                if len(source_df) > 0:
                    coverage = {
                        'total_records': len(source_df),
                        'with_building_code': len(source_df[source_df['building_code'].notna()]),
                        'with_emid': len(source_df[source_df['emid'].notna()]),
                        'with_service_area': len(source_df[source_df['mc_service_area'].notna()])
                    }
                    quality_checks['coverage_by_source'][source] = coverage
                    
                    print(f"\n   {source}:")
                    print(f"      Total records: {coverage['total_records']:,}")
                    print(f"      With building code: {coverage['with_building_code']:,} ({coverage['with_building_code']/coverage['total_records']*100:.1f}%)")
                    print(f"      With EMID: {coverage['with_emid']:,} ({coverage['with_emid']/coverage['total_records']*100:.1f}%)")
                    print(f"      With service area: {coverage['with_service_area']:,} ({coverage['with_service_area']/coverage['total_records']*100:.1f}%)")
            
            # Store results
            self.results['data_quality_checks'] = quality_checks
            
            # Create quality report
            self.create_quality_report(unified_df, quality_checks)
            
        except Exception as e:
            print(f"\n❌ Error in Step 4: {str(e)}")
            import traceback
            traceback.print_exc()
    
    def create_quality_report(self, df, quality_checks):
        """Create a visual quality report"""
        print("\n📈 Generating quality report visualizations...")
        
        try:
            # Set up the plot style
            plt.style.use('seaborn-v0_8-darkgrid')
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle('Invoice Data Quality Report', fontsize=16)
            
            # 1. Records by Source
            ax1 = axes[0, 0]
            source_counts = df['source_system'].value_counts()
            ax1.pie(source_counts.values, labels=source_counts.index, autopct='%1.1f%%')
            ax1.set_title('Records by Source System')
            
            # 2. Pay Type Distribution
            ax2 = axes[0, 1]
            pay_type_counts = df['pay_type'].value_counts()
            ax2.bar(pay_type_counts.index, pay_type_counts.values)
            ax2.set_title('Pay Type Distribution')
            ax2.set_xlabel('Pay Type')
            ax2.set_ylabel('Count')
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
            
            # 3. Hours Distribution
            ax3 = axes[1, 0]
            df['hours_quantity'].hist(bins=50, ax=ax3, edgecolor='black')
            ax3.set_title('Hours Distribution')
            ax3.set_xlabel('Hours')
            ax3.set_ylabel('Frequency')
            ax3.axvline(x=8, color='red', linestyle='--', label='8 hours')
            ax3.legend()
            
            # 4. Coverage Metrics
            ax4 = axes[1, 1]
            coverage_data = []
            for source, coverage in quality_checks['coverage_by_source'].items():
                coverage_data.append({
                    'Source': source,
                    'Building Code': coverage['with_building_code'] / coverage['total_records'] * 100,
                    'EMID': coverage['with_emid'] / coverage['total_records'] * 100,
                    'Service Area': coverage['with_service_area'] / coverage['total_records'] * 100
                })
            
            if coverage_data:
                coverage_df = pd.DataFrame(coverage_data)
                coverage_df.set_index('Source').plot(kind='bar', ax=ax4)
                ax4.set_title('Dimensional Data Coverage by Source (%)')
                ax4.set_ylabel('Coverage %')
                ax4.set_ylim(0, 105)
                ax4.legend(loc='lower right')
            
            plt.tight_layout()
            
            # Save the report
            report_filename = f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            plt.savefig(report_filename, dpi=300, bbox_inches='tight')
            print(f"   ✅ Quality report saved as: {report_filename}")
            
            plt.close()
            
        except Exception as e:
            print(f"   ⚠️  Could not generate visual report: {str(e)}")
    
    def step5_create_action_plan(self):
        """Step 5: Create action plan based on findings"""
        print("\n\n📋 STEP 5: Action Plan")
        print("-" * 50)
        
        print("\n Based on the validation results, here's your action plan:")
        
        # Check master lookup validation results
        if 'master_lookup_validation' in self.results:
            validation_issues = self.results['master_lookup_validation'].get('validation_issues', {})
            
            if validation_issues.get('missing_building_code'):
                print("\n 🔧 Master Lookup Updates Needed:")
                print(f"    - Add building codes for {len(validation_issues['missing_building_code'])} AUS jobs")
                print("    - Share the list with your billing manager to update the master lookup")
            
            if validation_issues.get('missing_emid'):
                print("\n 🔧 EMID Mappings Needed:")
                print(f"    - Add EMID codes for {len(validation_issues['missing_emid'])} records")
                print("    - Coordinate with operations team for proper EMID assignment")
        
        # Check transformation results
        if 'sample_transformation' in self.results:
            print("\n 📊 Data Processing Readiness:")
            for source, stats in self.results['sample_transformation'].items():
                success_rate = stats['transformed_rows'] / stats['original_rows'] * 100
                print(f"    - {source}: {success_rate:.1f}% transformation success rate")
                if stats['errors'] > 0:
                    print(f"      ⚠️  Review and fix {stats['errors']} transformation errors")
        
        # Check data quality
        if 'data_quality_checks' in self.results:
            quality = self.results['data_quality_checks']
            
            print("\n 🎯 Data Quality Improvements:")
            if quality['missing_building_codes'] > 0:
                print(f"    - Research building codes for {quality['missing_building_codes']} records")
            if quality['missing_emid'] > 0:
                print(f"    - Assign EMIDs to {quality['missing_emid']} records")
            if quality['invoice_revisions']:
                print(f"    - ✅ Good news: System handles invoice revisions (found {len(quality['invoice_revisions'])})")
        
        print("\n 🚀 Next Steps:")
        print("    1. Update master lookup file with missing mappings")
        print("    2. Process all 2025 historical data month by month")
        print("    3. Set up automated weekly processing")
        print("    4. Build analytics dashboards on unified data")
        print("    5. Implement SCR matching when new identifiers are ready")
        
        print("\n 💾 Files Created:")
        print("    - consolidated_dimensions.xlsx (reference data)")
        print("    - unified_sample_[timestamp].xlsx (transformed data)")
        print("    - quality_report_[timestamp].png (visual analysis)")
        
        print("\n✅ Validation walkthrough complete!")


def run_complete_walkthrough():
    """Run the complete validation walkthrough"""
    walkthrough = ValidationWalkthrough()
    
    # Step 1: Validate master lookup
    lookup_manager = walkthrough.step1_validate_master_lookup()
    
    if lookup_manager:
        # Step 2: Test sample transformation
        transformer = walkthrough.step2_test_sample_transformation(lookup_manager)
        
        if transformer:
            # Step 3: Process full sample
            unified_df = walkthrough.step3_process_full_sample_month(transformer)
            
            if unified_df is not None:
                # Step 4: Data quality analysis
                walkthrough.step4_data_quality_analysis(unified_df)
    
    # Step 5: Create action plan
    walkthrough.step5_create_action_plan()
    
    return walkthrough


if __name__ == "__main__":
    # Make sure we have matplotlib and seaborn
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
    except ImportError:
        print("⚠️  Please install matplotlib and seaborn for visualizations:")
        print("   pip install matplotlib seaborn")
    
    # Run the walkthrough
    walkthrough = run_complete_walkthrough()
