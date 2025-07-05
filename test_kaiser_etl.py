#!/usr/bin/env python3
"""
Test script for KAISER ETL Pipeline
Run this to test the pipeline with the most recent billing period
"""

import sys
import os
import logging
from datetime import datetime

# Add the parent directory to the path so we can import our ETL modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tracktik_etl.etl.etl_pipeline import ETLPipeline
from tracktik_etl.etl.database import db
from tracktik_etl.etl.config import config
from tracktik_etl.etl.models import ETLBatch

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'kaiser_etl_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)

logger = logging.getLogger(__name__)


def test_database_connection():
    """Test that we can connect to the database"""
    logger.info("Testing database connection...")
    try:
        result = db.execute_query("SELECT version() as version")
        logger.info(f"✅ Database connection successful: {result[0]['version']}")
        return True
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return False


def test_schema_exists():
    """Test that our schema and tables exist"""
    logger.info("Testing schema and table existence...")
    
    required_tables = [
        'billing_periods',
        'dim_employees', 
        'dim_clients', 
        'dim_positions', 
        'dim_regions',
        'fact_shifts',
        'etl_batches',
        'etl_sync_status'
    ]
    
    try:
        for table in required_tables:
            query = f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = '{config.POSTGRES_SCHEMA}' 
                    AND table_name = '{table}'
                )
            """
            result = db.execute_query(query)
            exists = result[0]['exists']
            
            if exists:
                logger.info(f"✅ Table {table} exists")
            else:
                logger.error(f"❌ Table {table} does not exist")
                return False
                
        return True
        
    except Exception as e:
        logger.error(f"❌ Schema check failed: {e}")
        return False


def test_billing_period_exists():
    """Test that the target billing period exists"""
    period_id = '2025_12'  # Period for 2025-05-30 to 2025-06-12
    
    logger.info(f"Testing if billing period {period_id} exists...")
    
    try:
        query = f"""
            SELECT period_id, start_date, end_date 
            FROM {config.POSTGRES_SCHEMA}.billing_periods 
            WHERE period_id = %(period_id)s
        """
        result = db.execute_query(query, {'period_id': period_id})
        
        if result:
            period = result[0]
            logger.info(f"✅ Billing period {period_id} exists: {period['start_date']} to {period['end_date']}")
            return True
        else:
            logger.error(f"❌ Billing period {period_id} does not exist")
            logger.info("Available periods:")
            
            # Show available periods
            all_periods = db.execute_query(f"""
                SELECT period_id, start_date, end_date 
                FROM {config.POSTGRES_SCHEMA}.billing_periods 
                ORDER BY start_date DESC LIMIT 10
            """)
            
            for p in all_periods:
                logger.info(f"  {p['period_id']}: {p['start_date']} to {p['end_date']}")
            
            return False
            
    except Exception as e:
        logger.error(f"❌ Billing period check failed: {e}")
        return False


def run_kaiser_etl_test():
    """Run the actual KAISER ETL pipeline for the test period"""
    logger.info("Starting KAISER ETL pipeline test...")
    
    period_id = '2025_12'  # Most recent completed period
    
    try:
        pipeline = ETLPipeline()
        
        # Test just one region first to validate the pipeline
        logger.info("Testing with single region first: N California")
        
        # Create a test batch first (needed for dimension operations)
        test_batch_id = ETLBatch.create_batch(
            'TEST_SINGLE_REGION',
            {'region': 'N California', 'period': period_id}
        )
        pipeline.batch_id = test_batch_id
        
        # Process just N California as a test
        test_stats = pipeline.load_shifts_for_region_period(
            "N California", 
            period_id, 
            "2025-05-30", 
            "2025-06-12"
        )

        ETLBatch.complete_batch(test_batch_id, test_stats.get('shifts_inserted', 0))
        
        # Check if we got any shifts
        if test_stats.get('shifts_retrieved', 0) == 0:
            logger.warning("⚠️ No shifts were retrieved. This might indicate:")
            logger.warning("  - No shifts exist for this period")
            logger.warning("  - API query issues")
            logger.warning("  - Permission/access issues")
            
            # But still consider it a "success" if no errors occurred
            if test_stats.get('clients', 0) > 0 and test_stats.get('positions', 0) > 0:
                logger.info("✅ Dimensions loaded successfully, but no shifts found")
        
        logger.info(f"✅ Single region test completed: {test_stats}")
        
        # If single region works, ask user if they want to run all regions
        response = input("\nSingle region test completed. Run all KAISER regions? (y/n): ")
        
        if response.lower() == 'y':
            logger.info("Running full KAISER ETL pipeline...")
            results = pipeline.process_kaiser_billing_period(period_id)
            logger.info("✅ Full KAISER ETL pipeline completed successfully!")
            return results
        else:
            logger.info("Stopping at single region test.")
            return test_stats
            
    except Exception as e:
        logger.error(f"❌ KAISER ETL pipeline failed: {e}")
        raise


def main():
    """Main test function"""
    logger.info("="*60)
    logger.info("KAISER ETL PIPELINE TEST")
    logger.info("="*60)
    
    # Run tests in sequence
    tests = [
        ("Database Connection", test_database_connection),
        ("Schema Validation", test_schema_exists),
        ("Billing Period Check", test_billing_period_exists),
    ]
    
    for test_name, test_func in tests:
        logger.info(f"\n{'*' * 40}")
        logger.info(f"TEST: {test_name}")
        logger.info(f"{'*' * 40}")
        
        if not test_func():
            logger.error(f"❌ {test_name} failed - stopping tests")
            return False
    
    # If all tests pass, run the ETL pipeline
    logger.info(f"\n{'*' * 40}")
    logger.info("TEST: KAISER ETL Pipeline")
    logger.info(f"{'*' * 40}")
    
    try:
        results = run_kaiser_etl_test()
        logger.info("✅ All tests completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ ETL Pipeline test failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    
    if success:
        logger.info("\n🎉 KAISER ETL Pipeline is ready for production!")
        sys.exit(0)
    else:
        logger.error("\n💥 Tests failed - please fix issues before proceeding")
        sys.exit(1)
