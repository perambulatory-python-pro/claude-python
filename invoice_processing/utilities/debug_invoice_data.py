import pandas as pd
import numpy as np

def debug_invoice_data(file_path="invoice_master.xlsx"):
    """
    Debug script to understand the structure and content of the invoice file
    """
    print("🔍 Debugging Invoice Master File")
    print("=" * 60)
    
    # Read the Excel file
    try:
        df = pd.read_excel(file_path)
        print(f"✅ Successfully loaded file: {file_path}")
        print(f"📊 Total rows: {len(df)}")
        print(f"📊 Total columns: {len(df.columns)}")
        print()
        
        # Show column names
        print("📋 Column names:")
        for i, col in enumerate(df.columns):
            print(f"  {i}: {col}")
        print()
        
        # Check for Invoice No. column variations
        print("🔍 Looking for invoice number columns...")
        invoice_columns = [col for col in df.columns if 'invoice' in col.lower() and 'no' in col.lower()]
        print(f"Found potential invoice columns: {invoice_columns}")
        print()
        
        # Check the actual invoice column
        invoice_col = None
        for col in ['Invoice No.', 'Invoice No', 'invoice_no', 'InvoiceNo', 'Invoice Number']:
            if col in df.columns:
                invoice_col = col
                break
        
        if invoice_col:
            print(f"✅ Using invoice column: '{invoice_col}'")
            
            # Analyze invoice numbers
            print(f"\n📊 Invoice Number Analysis:")
            print(f"  - Total rows: {len(df)}")
            print(f"  - Non-null invoice numbers: {df[invoice_col].notna().sum()}")
            print(f"  - Null/empty invoice numbers: {df[invoice_col].isna().sum()}")
            print(f"  - Unique invoice numbers: {df[invoice_col].nunique()}")
            
            # Show sample of data
            print(f"\n📄 Sample of first 10 rows:")
            sample_cols = [invoice_col]
            
            # Add other relevant columns if they exist
            for col in ['EMID', 'SERVICE REQ\'D BY', 'Invoice Date', 'Invoice Total']:
                if col in df.columns:
                    sample_cols.append(col)
            
            print(df[sample_cols].head(10).to_string())
            
            # Check data types
            print(f"\n🔧 Data type of invoice column: {df[invoice_col].dtype}")
            
            # Show some actual values
            print(f"\n📝 Sample invoice numbers (first 20 non-null):")
            non_null_invoices = df[df[invoice_col].notna()][invoice_col].head(20)
            for i, inv in enumerate(non_null_invoices):
                print(f"  {i+1}: '{inv}' (type: {type(inv).__name__})")
                
        else:
            print("❌ Could not find invoice number column!")
            print("\n🔍 Here are ALL columns in the file:")
            for col in df.columns:
                print(f"  - {col}")
                
        # Check if the DataFrame might be reading headers incorrectly
        print("\n🔍 Checking first few rows (might be header issues):")
        print(df.head(3).to_string())
        
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_invoice_data()
