# cleanup_employee_temporal_conflicts.py
"""
Clean up temporal conflicts in dim_employees table before switching to SCD Type 2
"""
import os
import sys
from datetime import datetime
import logging
from dotenv import load_dotenv

# Add the parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

load_dotenv()

from tracktik_etl.etl.config import config
from tracktik_etl.etl.database import db

logger = logging.getLogger(__name__)


def analyze_conflicts():
    """Analyze current temporal conflicts"""
    logger.info("Analyzing temporal conflicts in dim_employees...")
    
    # Find employees with multiple current records
    conflict_query = f"""
        SELECT 
            employee_id, 
            COUNT(*) as record_count,
            STRING_AGG(surrogate_key::text, ', ' ORDER BY valid_from) as surrogate_keys,
            STRING_AGG(valid_from::text, ', ' ORDER BY valid_from) as valid_from_dates,
            STRING_AGG(COALESCE(valid_to::text, 'NULL'), ', ' ORDER BY valid_from) as valid_to_dates
        FROM {config.POSTGRES_SCHEMA}.dim_employees 
        WHERE is_current = TRUE
        GROUP BY employee_id 
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
    """
    
    conflicts = db.execute_query(conflict_query)
    
    logger.info(f"Found {len(conflicts)} employees with multiple current records")
    
    if conflicts:
        logger.info("\nTop 10 conflicts:")
        for i, conflict in enumerate(conflicts[:10], 1):
            logger.info(f"  {i}. Employee {conflict['employee_id']}: {conflict['record_count']} records")
            logger.info(f"     Surrogate keys: {conflict['surrogate_keys']}")
            logger.info(f"     Valid from: {conflict['valid_from_dates']}")
            logger.info(f"     Valid to: {conflict['valid_to_dates']}")
    
    return conflicts


def fix_temporal_conflicts():
    """Fix temporal conflicts by closing older records"""
    logger.info("Fixing temporal conflicts...")
    
    # Strategy: For each employee with multiple current records,
    # keep the most recent one and close the others
    
    fix_query = f"""
        WITH conflict_employees AS (
            SELECT employee_id
            FROM {config.POSTGRES_SCHEMA}.dim_employees 
            WHERE is_current = TRUE
            GROUP BY employee_id 
            HAVING COUNT(*) > 1
        ),
        latest_records AS (
            SELECT DISTINCT ON (employee_id) 
                employee_id,
                surrogate_key as latest_surrogate_key
            FROM {config.POSTGRES_SCHEMA}.dim_employees
            WHERE employee_id IN (SELECT employee_id FROM conflict_employees)
            AND is_current = TRUE
            ORDER BY employee_id, valid_from DESC, surrogate_key DESC
        )
        UPDATE {config.POSTGRES_SCHEMA}.dim_employees 
        SET 
            valid_to = CURRENT_TIMESTAMP - INTERVAL '1 second',
            is_current = FALSE,
            updated_at = CURRENT_TIMESTAMP
        WHERE employee_id IN (SELECT employee_id FROM conflict_employees)
        AND is_current = TRUE
        AND surrogate_key NOT IN (SELECT latest_surrogate_key FROM latest_records)
    """
    
    with db.get_cursor() as cursor:
        cursor.execute(fix_query)
        rows_updated = cursor.rowcount
        
    logger.info(f"Closed {rows_updated} older employee records")
    return rows_updated


def verify_fix():
    """Verify that conflicts are resolved"""
    logger.info("Verifying fix...")
    
    # Check for remaining conflicts
    remaining_conflicts = analyze_conflicts()
    
    if not remaining_conflicts:
        logger.info("✅ All temporal conflicts resolved!")
        return True
    else:
        logger.error(f"❌ Still have {len(remaining_conflicts)} conflicts")
        return False


def main():
    """Main cleanup process"""
    print(f"""
╔══════════════════════════════════════════════════════════╗
║         Employee Temporal Conflict Cleanup               ║
║                                                          ║
║  Purpose: Fix SCD Type 2 conflicts before employee sync  ║
║  Action: Close older duplicate employee records          ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    try:
        # Step 1: Analyze current state
        conflicts = analyze_conflicts()
        
        if not conflicts:
            logger.info("✅ No temporal conflicts found - no cleanup needed!")
            return
        
        # Step 2: Confirm action
        logger.info(f"\nFound {len(conflicts)} employees with temporal conflicts")
        response = input("\nProceed with cleanup? This will close older duplicate records. (y/N): ")
        
        if response.lower() != 'y':
            logger.info("Cleanup cancelled")
            return
        
        # Step 3: Fix conflicts
        rows_updated = fix_temporal_conflicts()
        
        # Step 4: Verify fix
        success = verify_fix()
        
        if success:
            logger.info("\n🎉 Cleanup completed successfully!")
            logger.info("You can now run the employee sync script safely.")
        else:
            logger.error("\n⚠️ Cleanup incomplete - manual intervention may be needed")
        
    except Exception as e:
        logger.error(f"❌ Cleanup failed: {e}")
        raise


if __name__ == "__main__":
    main()
