"""
Quick fix for NaN handling in the transformer
Run this instead of the full fixed_edi_transformer.py
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
from fixed_edi_transformer import FixedEDIBasedTransformer


class QuickFixedTransformer(FixedEDIBasedTransformer):
    """Override just the problematic method"""
    
    def get_invoice_dimensions(self, invoice_number: str):
        """
        Get all dimensional data for an invoice from EDI
        Fixed to handle NaN values properly
        """
        invoice_str = str(invoice_number).strip()
        
        # Direct lookup
        if invoice_str in self.invoice_lookup:
            edi_data = self.invoice_lookup[invoice_str]
            
            # Helper function to handle NaN values
            def get_value(key):
                val = edi_data.get(key)
                # Check if it's NaN (pandas null value)
                if pd.isna(val):
                    return None
                # Also handle string 'nan'
                if isinstance(val, str) and val.lower() == 'nan':
                    return None
                return val
            
            result = {
                'EMID': get_value('EMID'),
                'SERVICE_AREA': get_value('SERVICE_AREA'),
                'BUILDING_CODE': get_value('BUILDING_CODE'),
                'GL_BU': get_value('GL_BU'),
                'GL_LOC': get_value('GL_LOC'),
                'GL_DEPT': get_value('GL_DEPT'),
                'GL_ACCT': get_value('GL_ACCT'),
                'ONELINK_REGION': get_value('REGION'),
                'ONELINK_STATUS': get_value('ONELINK_STATUS'),
                'PAID_DATE': get_value('PAID_DATE')
            }
            
            # If no building code, try LOC lookup
            if not result['BUILDING_CODE'] and result.get('GL_LOC'):
                loc_str = str(result['GL_LOC'])
                if loc_str in self.loc_to_building:
                    result['BUILDING_CODE'] = self.loc_to_building[loc_str]
                    self.transformation_stats['warnings'].append(
                        f"Used LOC {loc_str} to find building {result['BUILDING_CODE']}"
                    )
            
            self.transformation_stats['invoices_matched'] += 1
            return result
        
        # Try without revision letter
        base_invoice = invoice_str.rstrip('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
        if base_invoice != invoice_str and base_invoice in self.invoice_lookup:
            self.transformation_stats['invoices_matched'] += 1
            self.transformation_stats['warnings'].append(
                f"Invoice {invoice_str} matched to {base_invoice} (revision stripped)"
            )
            return self.get_invoice_dimensions(base_invoice)
        
        # Not found
        self.transformation_stats['invoices_not_found'].add(invoice_str)
        return None


def run_quick_fix():
    """Run the transformation with the quick fix"""
    
    print("RUNNING QUICK FIX FOR NaN HANDLING")
    print("=" * 60)
    
    # Check if dimensions exist
    if not os.path.exists("clean_dimensions.xlsx"):
        print("❌ clean_dimensions.xlsx not found. Run the main transformation first.")
        return
    
    print("\n1. Initializing fixed transformer...")
    transformer = QuickFixedTransformer("clean_dimensions.xlsx")
    
    print("\n2. Transforming BCI invoice details...")
    bci_unified = transformer.transform_bci_file("invoice_details_bci.csv")
    print(f"   ✓ Transformed {len(bci_unified)} BCI records")
    
    print("\n3. Transforming AUS invoice details...")
    aus_unified = transformer.transform_aus_file("invoice_details_aus.csv")
    print(f"   ✓ Transformed {len(aus_unified)} AUS records")
    
    print("\n4. Combining data...")
    unified_df = pd.concat([bci_unified, aus_unified], ignore_index=True)
    print(f"   ✓ Combined {len(unified_df)} total records")
    
    # Check field population
    print("\n5. Checking field population...")
    for field in ['business_unit', 'job_code', 'emid', 'building_code']:
        if field in unified_df.columns:
            filled = unified_df[field].notna().sum()
            print(f"   {field}: {filled}/{len(unified_df)} filled ({filled/len(unified_df)*100:.1f}%)")
    
    # Export
    print("\n6. Exporting fixed data...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # CSV export
    csv_file = f"unified_invoice_data_quickfix_{timestamp}.csv"
    unified_df.to_csv(csv_file, index=False)
    print(f"   ✓ Exported to {csv_file}")
    
    # Try Excel export
    try:
        excel_file = f"unified_invoice_data_quickfix_{timestamp}.xlsx"
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Main data
            unified_df.to_excel(writer, sheet_name='Unified_Invoice_Details', index=False)
            
            # Field population summary
            summary_data = {
                'Field': ['business_unit', 'job_code', 'emid', 'building_code'],
                'Records_Filled': [],
                'Percentage': []
            }
            
            for field in summary_data['Field']:
                if field in unified_df.columns:
                    filled = unified_df[field].notna().sum()
                    summary_data['Records_Filled'].append(filled)
                    summary_data['Percentage'].append(f"{filled/len(unified_df)*100:.1f}%")
                else:
                    summary_data['Records_Filled'].append(0)
                    summary_data['Percentage'].append("0.0%")
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Field_Population_Summary', index=False)
        
        print(f"   ✓ Also exported to {excel_file}")
    except:
        print("   ⚠️ Excel export failed, but CSV is available")
    
    print("\n" + "="*60)
    print("✅ QUICK FIX COMPLETE!")
    print("=" * 60)
    
    return unified_df


if __name__ == "__main__":
    unified_df = run_quick_fix()
