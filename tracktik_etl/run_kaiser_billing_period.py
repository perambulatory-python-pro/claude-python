# run_kaiser_billing_period.py
"""
Execute KAISER billing period processing
"""
import os
import sys
from datetime import datetime
import logging
from dotenv import load_dotenv

# Add the parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Ensure logs directory exists
os.makedirs('logs', exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(
            f'logs/kaiser_billing_{datetime.now():%Y%m%d_%H%M%S}.log',
            encoding='utf-8'  # Add this!
        ),
        logging.StreamHandler(sys.stdout)
    ]
)

load_dotenv()

from tracktik_etl.etl.etl_pipeline import ETLPipeline


def main():
    """Process KAISER billing period"""
    
    # Define billing period
    PERIOD_ID = "2025_05"  # This is what your pipeline expects
    START_DATE = "2025-02-21"
    END_DATE = "2025-03-06"
    
    print(f"""
╔══════════════════════════════════════════════════════════╗
║           KAISER Billing Period Processing               ║
║                                                          ║
║  Period ID: {PERIOD_ID}                                      ║
║  Dates: {START_DATE} to {END_DATE}                         ║
║  Regions: All 8 KAISER sub-regions                       ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    pipeline = ETLPipeline()
    
    try:
        # Process the billing period
        stats = pipeline.process_kaiser_billing_period(PERIOD_ID)
        
        # Display results (the pipeline already prints a nice summary)
        print("\n✅ Processing completed successfully!")
        
        # Additional summary if needed
        if stats.get('errors'):
            print(f"\n⚠️  Errors encountered: {len(stats['errors'])}")
            for error in stats['errors']:
                print(f"  - {error}")
        else:
            print("\n🎉 No errors - all regions processed successfully!")
        
    except Exception as e:
        print(f"\n❌ Processing failed: {e}")
        logging.exception("Fatal error in processing")
        raise


if __name__ == "__main__":
    main()