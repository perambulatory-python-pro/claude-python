# validate_employee_readiness.py
"""
Validate that employee data is ready for billing period processing
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


def check_employee_data_completeness():
    """Check how complete the employee data is"""
    logger.info("Checking employee data completeness...")
    
    # Get overall stats
    stats_query = f"""
        SELECT 
            COUNT(*) as total_employees,
            COUNT(first_name) as has_first_name,
            COUNT(last_name) as has_last_name,
            COUNT(email) as has_email,
            COUNT(phone) as has_phone,
            COUNT(custom_id) as has_custom_id,
            COUNT(CASE WHEN first_name IS NULL AND last_name IS NULL 
                       AND email IS NULL AND phone IS NULL 
                       AND custom_id IS NULL THEN 1 END) as completely_empty
        FROM {config.POSTGRES_SCHEMA}.dim_employees
        WHERE is_current = TRUE
    """
    
    stats = db.execute_query(stats_query)[0]
    
    logger.info(f"\n📊 Employee Data Completeness:")
    logger.info(f"  Total Employees: {stats['total_employees']:,}")
    logger.info(f"  Has First Name: {stats['has_first_name']:,} ({stats['has_first_name']/stats['total_employees']*100:.1f}%)")
    logger.info(f"  Has Last Name: {stats['has_last_name']:,} ({stats['has_last_name']/stats['total_employees']*100:.1f}%)")
    logger.info(f"  Has Email: {stats['has_email']:,} ({stats['has_email']/stats['total_employees']*100:.1f}%)")
    logger.info(f"  Has Phone: {stats['has_phone']:,} ({stats['has_phone']/stats['total_employees']*100:.1f}%)")
    logger.info(f"  Has Custom ID: {stats['has_custom_id']:,} ({stats['has_custom_id']/stats['total_employees']*100:.1f}%)")
    logger.info(f"  Completely Empty: {stats['completely_empty']:,} ({stats['completely_empty']/stats['total_employees']*100:.1f}%)")
    
    return stats


def check_kaiser_employee_coverage():
    """Check employee coverage in KAISER regions"""
    logger.info("\nChecking KAISER employee coverage...")
    
    coverage_query = f"""
        SELECT 
            r.name as region_name,
            COUNT(e.employee_id) as employee_count,
            COUNT(CASE WHEN e.first_name IS NOT NULL THEN 1 END) as with_names,
            COUNT(CASE WHEN e.email IS NOT NULL THEN 1 END) as with_email
        FROM {config.POSTGRES_SCHEMA}.dim_employees e
        JOIN {config.POSTGRES_SCHEMA}.dim_regions r ON e.region_id = r.region_id
        WHERE e.is_current = TRUE 
        AND r.is_current = TRUE
        AND r.parent_region_name ILIKE '%KAISER%'
        GROUP BY r.name
        ORDER BY employee_count DESC
    """
    
    results = db.execute_query(coverage_query)
    
    logger.info(f"\n🏢 KAISER Employee Coverage by Region:")
    for row in results:
        name_pct = (row['with_names'] / row['employee_count'] * 100) if row['employee_count'] > 0 else 0
        email_pct = (row['with_email'] / row['employee_count'] * 100) if row['employee_count'] > 0 else 0
        logger.info(f"  {row['region_name']:<15}: {row['employee_count']:>4,} employees "
                   f"({name_pct:>3.0f}% names, {email_pct:>3.0f}% emails)")
    
    return results


def check_recent_shifts_employee_coverage():
    """Check if recent shifts reference employees that exist in dim_employees"""
    logger.info("\nChecking recent shift employee coverage...")
    
    # Get recent shifts and check employee coverage
    shift_coverage_query = f"""
        WITH recent_shifts AS (
            SELECT DISTINCT employee_id
            FROM {config.POSTGRES_SCHEMA}.fact_shifts
            WHERE shift_date >= CURRENT_DATE - INTERVAL '30 days'
        ),
        shift_employee_coverage AS (
            SELECT 
                COUNT(DISTINCT rs.employee_id) as shift_employees,
                COUNT(DISTINCT e.employee_id) as existing_employees
            FROM recent_shifts rs
            LEFT JOIN {config.POSTGRES_SCHEMA}.dim_employees e 
                ON rs.employee_id = e.employee_id AND e.is_current = TRUE
        )
        SELECT * FROM shift_employee_coverage
    """
    
    try:
        coverage = db.execute_query(shift_coverage_query)[0]
        
        coverage_pct = (coverage['existing_employees'] / coverage['shift_employees'] * 100) if coverage['shift_employees'] > 0 else 0
        
        logger.info(f"  Recent Shifts (30 days): {coverage['shift_employees']:,} unique employees")
        logger.info(f"  In dim_employees: {coverage['existing_employees']:,} ({coverage_pct:.1f}%)")
        
        if coverage_pct < 100:
            missing = coverage['shift_employees'] - coverage['existing_employees']
            logger.warning(f"  ⚠️ Missing {missing:,} employees from dim_employees")
        else:
            logger.info(f"  ✅ All shift employees found in dim_employees")
            
        return coverage
        
    except Exception as e:
        logger.info(f"  No recent shifts found or error: {e}")
        return None


def recommend_next_steps(stats, coverage, shift_coverage):
    """Provide recommendations based on analysis"""
    logger.info(f"\n💡 Recommendations:")
    
    # Check if employee sync is needed
    if stats['completely_empty'] > stats['total_employees'] * 0.5:  # More than 50% empty
        logger.info(f"  🔴 CRITICAL: Run employee sync script immediately")
        logger.info(f"     {stats['completely_empty']:,} employees have no data")
        return "sync_required"
    
    elif stats['has_first_name'] < stats['total_employees'] * 0.8:  # Less than 80% have names
        logger.info(f"  🟡 RECOMMENDED: Run employee sync script to get complete data")
        logger.info(f"     Only {stats['has_first_name']:,} out of {stats['total_employees']:,} have names")
        return "sync_recommended"
    
    else:
        logger.info(f"  ✅ Employee data looks good for billing period processing")
        
        if shift_coverage and shift_coverage['existing_employees'] < shift_coverage['shift_employees']:
            logger.info(f"  🟡 Some recent shift employees missing from dim_employees")
            logger.info(f"     Consider running employee sync to capture missing employees")
            return "partial_sync_recommended"
        
        return "ready"


def main():
    """Main validation process"""
    print(f"""
╔══════════════════════════════════════════════════════════╗
║              Employee Data Readiness Check               ║
║                                                          ║
║  Purpose: Validate employee data before billing periods  ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    try:
        # Check employee data completeness
        stats = check_employee_data_completeness()
        
        # Check KAISER coverage
        coverage = check_kaiser_employee_coverage()
        
        # Check recent shift coverage
        shift_coverage = check_recent_shifts_employee_coverage()
        
        # Provide recommendations
        status = recommend_next_steps(stats, coverage, shift_coverage)
        
        print(f"\n{'='*60}")
        if status == "sync_required":
            print("🔴 Action Required: Run employee sync before billing periods")
            print("   python sync_kaiser_employees.py")
        elif status in ["sync_recommended", "partial_sync_recommended"]:
            print("🟡 Recommended: Run employee sync for complete data")
            print("   python sync_kaiser_employees.py")
        else:
            print("✅ Ready: Employee data sufficient for billing period processing")
        print(f"{'='*60}")
        
    except Exception as e:
        logger.error(f"❌ Validation failed: {e}")
        raise


if __name__ == "__main__":
    main()
