"""
Simplified transformation runner - focuses on getting the job done
"""

import pandas as pd
import os
from datetime import datetime


def run_simple_transformation():
    """Run the transformation with minimal complexity"""
    
    print("=" * 70)
    print("SIMPLIFIED EDI-BASED INVOICE TRANSFORMATION")
    print("=" * 70)
    
    try:
        # Step 1: Create dimensions (without Excel export issues)
        print("\nSTEP 1: Creating dimension tables...")
        from dimension_table_creator import DimensionTableCreator
        
        creator = DimensionTableCreator()
        dimensions = creator.export_all_dimensions("clean_dimensions.xlsx")
        print("✅ Dimensions created")
        
        # Step 2: Transform invoice details
        print("\nSTEP 2: Transforming invoice details...")
        from edi_based_transformer import EDIBasedTransformer
        
        transformer = EDIBasedTransformer("clean_dimensions.xlsx")
        
        # Transform BCI
        print("  Transforming BCI...")
        bci_unified = transformer.transform_bci_file("invoice_details_bci.csv")
        print(f"  ✅ Transformed {len(bci_unified)} BCI records")
        
        # Transform AUS  
        print("  Transforming AUS...")
        aus_unified = transformer.transform_aus_file("invoice_details_aus.csv")
        print(f"  ✅ Transformed {len(aus_unified)} AUS records")
        
        # Combine
        print("\nSTEP 3: Combining data...")
        unified_df = pd.concat([bci_unified, aus_unified], ignore_index=True)
        print(f"✅ Combined {len(unified_df)} total records")
        
        # Step 4: Export (simplified)
        print("\nSTEP 4: Exporting results...")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Export as CSV (more reliable)
        csv_file = f"unified_invoice_data_{timestamp}.csv"
        unified_df.to_csv(csv_file, index=False)
        print(f"✅ Exported to {csv_file}")
        
        # Try Excel export
        try:
            excel_file = f"unified_invoice_data_{timestamp}.xlsx"
            unified_df.to_excel(excel_file, index=False)
            print(f"✅ Also exported to {excel_file}")
        except:
            print("⚠️  Excel export failed, but CSV is available")
        
        # Print summary
        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        print(f"Total records: {len(unified_df):,}")
        print(f"BCI records: {len(unified_df[unified_df['source_system'] == 'BCI']):,}")
        print(f"AUS records: {len(unified_df[unified_df['source_system'] == 'AUS']):,}")
        
        emid_coverage = (unified_df['emid'].notna().sum() / len(unified_df)) * 100
        print(f"EMID coverage: {emid_coverage:.1f}%")
        
        building_coverage = (unified_df['building_code'].notna().sum() / len(unified_df)) * 100
        print(f"Building coverage: {building_coverage:.1f}%")
        
        print(f"\nTotal hours: {unified_df['hours_quantity'].sum():,.2f}")
        print(f"Total billing: ${unified_df['bill_amount'].sum():,.2f}")
        
        # Check unmatched invoices
        unmatched_count = len(transformer.transformation_stats['invoices_not_found'])
        if unmatched_count > 0:
            print(f"\n⚠️  {unmatched_count} invoices not found in EDI")
            
            # Save unmatched list
            unmatched_file = f"unmatched_invoices_{timestamp}.txt"
            with open(unmatched_file, 'w') as f:
                for inv in sorted(transformer.transformation_stats['invoices_not_found']):
                    f.write(f"{inv}\n")
            print(f"   See {unmatched_file} for details")
        
        print("\n✅ Transformation complete!")
        
        return unified_df
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    unified_df = run_simple_transformation()
