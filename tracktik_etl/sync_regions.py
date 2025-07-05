# sync_regions.py
"""
Sync TrackTik regions to database mapping table
"""
import os
import sys
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from etl.kaiser_billing_processor import KaiserBillingProcessor

def main():
    print("Syncing TrackTik regions to database...")
    
    processor = KaiserBillingProcessor()
    processor._sync_region_mapping()
    
    print("\n✅ Region sync complete!")
    
    # Show KAISER regions
    from etl.database import db
    from etl.config import config
    
    query = f"""
        SELECT region_id, name, parent_region_name, level
        FROM {config.POSTGRES_SCHEMA}.dim_regions
        WHERE is_kaiser_region = true
        ORDER BY level, name
    """
    
    kaiser_regions = db.execute_query(query)
    
    print(f"\nKAISER Regions ({len(kaiser_regions)} total):")
    for region in kaiser_regions:
        indent = "  " * (region['level'] - 1)
        parent = f" (Parent: {region['parent_region_name']})" if region['parent_region_name'] else ""
        print(f"{indent}- {region['name']} (ID: {region['region_id']}){parent}")

if __name__ == "__main__":
    main()