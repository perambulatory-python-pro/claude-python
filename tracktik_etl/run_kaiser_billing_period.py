# run_kaiser_billing_period.py
"""
Execute KAISER billing period processing
"""
import os
import sys
from datetime import datetime
import logging
from dotenv import load_dotenv

# Ensure logs directory exists
os.makedirs('logs', exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/kaiser_billing_{datetime.now():%Y%m%d_%H%M%S}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

load_dotenv()

from etl.kaiser_billing_processor import KaiserBillingProcessor


def main():
    """Process KAISER billing period"""
    
    # Define billing period
    START_DATE = "2025-05-30"
    END_DATE = "2025-06-12"
    
    print(f"""
╔══════════════════════════════════════════════════════════╗
║           KAISER Billing Period Processing               ║
║                                                          ║
║  Period: {START_DATE} to {END_DATE}                        ║
║  Regions: All 8 KAISER sub-regions                       ║
║  Processing: One region at a time with checkpointing     ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    processor = KaiserBillingProcessor()
    
    try:
        # Process the billing period
        stats = processor.process_billing_period(START_DATE, END_DATE)
        
        # Display results
        print("\n" + "="*60)
        print("PROCESSING COMPLETE")
        print("="*60)
        
        print(f"\nBilling Period: {stats['billing_period']}")
        print(f"Total Shifts Processed: {stats['total_shifts']:,}")
        print(f"Total Hours: {stats['total_hours']:,.2f}")
        
        print("\nBy Region:")
        for region_stat in stats['regions_processed']:
            print(f"  {region_stat['region']:20} - "
                  f"{region_stat['shifts_processed']:6,} shifts, "
                  f"{region_stat['total_hours']:10,.2f} hours")
        
        if stats['errors']:
            print(f"\n⚠️  Errors encountered: {len(stats['errors'])}")
            for error in stats['errors']:
                print(f"  - {error}")
        
    except Exception as e:
        print(f"\n❌ Processing failed: {e}")
        logging.exception("Fatal error in processing")
        raise


if __name__ == "__main__":
    main()