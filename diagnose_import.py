# save as diagnose_import.py
import sys
import traceback

print("Attempting to import EnhancedDataMapper...\n")

try:
    from invoice_processing.core.data_mapper_enhanced import EnhancedDataMapper
    print("✅ Import successful!")
except Exception as e:
    print(f"❌ Import failed: {type(e).__name__}: {e}")
    print("\nFull traceback:")
    traceback.print_exc()
    
    print("\n📁 Python is looking in these paths:")
    for path in sys.path[:5]:
        print(f"  - {path}")