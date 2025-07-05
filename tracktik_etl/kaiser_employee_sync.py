# sync_kaiser_employees.py
"""
Sync complete employee data for all KAISER regions
Fetches full employee details from TrackTik API and updates dim_employees table
"""
import os
import sys
from datetime import datetime
import logging
from typing import Dict, List, Set
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
            f'logs/employee_sync_{datetime.now():%Y%m%d_%H%M%S}.log',
            encoding='utf-8'
        ),
        logging.StreamHandler(sys.stdout)
    ]
)

load_dotenv()

from tracktik_etl.etl.config import config
from tracktik_etl.etl.tracktik_client import TrackTikClient
from tracktik_etl.etl.database import db

logger = logging.getLogger(__name__)


class KaiserEmployeeSync:
    """Sync complete employee data for KAISER regions"""
    
    # KAISER sub-regions from your established configuration
    KAISER_SUBREGIONS = [
        "N California",
        "S California", 
        "Hawaii",
        "Washington",
        "Colorado",
        "Georgia",
        "MidAtlantic",
        "Northwest"
    ]
    
    def __init__(self):
        self.client = TrackTikClient()
        self.kaiser_region_mapping = {}
        self.batch_id = None  # For SCD Type 2 tracking
        self.sync_stats = {
            'total_employees_processed': 0,
            'total_employees_updated': 0,
            'total_employees_inserted': 0,
            'total_employees_unchanged': 0,
            'total_api_calls': 0,
            'errors': [],
            'regions_processed': []
        }
        
    def load_kaiser_region_mapping(self) -> Dict[str, int]:
        """Load KAISER region name to ID mapping from database"""
        logger.info("Loading KAISER region mapping from database...")
        
        query = f"""
            SELECT region_id, name, parent_region_name
            FROM {config.POSTGRES_SCHEMA}.dim_regions 
            WHERE is_current = TRUE 
            AND (parent_region_name ILIKE '%KAISER%' OR name ILIKE '%KAISER%')
        """
        
        results = db.execute_query(query)
        
        # Build mapping: region_name -> region_id
        region_mapping = {}
        for row in results:
            region_mapping[row['name']] = row['region_id']
            
        logger.info(f"Loaded {len(region_mapping)} KAISER regions from database")
        
        if not region_mapping:
            raise ValueError("No KAISER regions found in database. Please run region sync first.")
            
        self.kaiser_region_mapping = region_mapping
        return region_mapping
    
    def get_employees_for_region(self, region_name: str) -> List[Dict]:
        """Get all employees for a specific KAISER region"""
        region_id = self.kaiser_region_mapping.get(region_name)
        if not region_id:
            logger.warning(f"Region '{region_name}' not found in mapping")
            return []
            
        logger.info(f"Fetching employees for region: {region_name} (ID: {region_id})")
        
        try:
            # Use region filter parameter from API documentation
            params = {
                'region': region_id,
                'includeInactive': 'false',  # Only active employees
                'limit': 1000  # Max per page
            }
            
            # Get employees using pagination
            employees = self.client.get_paginated_data('/rest/v1/employees', params)
            self.sync_stats['total_api_calls'] += 1
            
            logger.info(f"Retrieved {len(employees)} employees for {region_name}")
            return employees
            
        except Exception as e:
            error_msg = f"Error fetching employees for region {region_name}: {str(e)}"
            logger.error(error_msg)
            self.sync_stats['errors'].append(error_msg)
            return []
    
    def transform_employee_data(self, employee: Dict, region_name: str) -> Dict:
        """Transform API employee data to database schema"""
        try:
            # Extract data safely with defaults
            transformed = {
                'employee_id': employee.get('id'),
                'custom_id': employee.get('customId'),
                'first_name': employee.get('firstName'),
                'last_name': employee.get('lastName'),
                'email': employee.get('email'),
                'phone': employee.get('primaryPhone'),  # Using primaryPhone from API
                'status': employee.get('status'),
                'region_id': self.kaiser_region_mapping.get(region_name),
                'updated_at': datetime.now()
            }
            
            # Validate required fields
            if not transformed['employee_id']:
                raise ValueError("Missing employee ID")
                
            return transformed
            
        except Exception as e:
            logger.error(f"Error transforming employee {employee.get('id', 'unknown')}: {e}")
            raise
    
    def update_employee_record(self, employee_data: Dict) -> str:
        """Update or insert employee record using SCD Type 2. Returns 'updated' or 'inserted'"""
        employee_id = employee_data['employee_id']
        
        try:
            # Prepare data for SCD Type 2 upsert
            scd_data = {
                'employee_id': employee_data['employee_id'],
                'custom_id': employee_data['custom_id'],
                'first_name': employee_data['first_name'],
                'last_name': employee_data['last_name'],
                'email': employee_data['email'],
                'phone': employee_data['phone'],
                'status': employee_data['status'],
                'region_id': employee_data['region_id'],
                'valid_from': datetime.now(),
                'is_current': True,
                'etl_batch_id': getattr(self, 'batch_id', None)
            }
            
            # Use the existing SCD Type 2 logic from your models
            result = db.upsert_dimension(
                table='dim_employees',
                records=[scd_data],
                id_field='employee_id',
                scd_fields=['custom_id', 'first_name', 'last_name', 'email', 'phone', 'status', 'region_id']
            )
            
            # Determine what happened
            if result['inserted'] > 0:
                return 'inserted'
            elif result['updated'] > 0:
                return 'updated'
            else:
                return 'unchanged'
                
        except Exception as e:
            logger.error(f"Error updating employee {employee_id}: {e}")
            raise
    
    def sync_region_employees(self, region_name: str) -> Dict:
        """Sync all employees for a specific region"""
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing region: {region_name}")
        logger.info(f"{'='*60}")
        
        region_stats = {
            'region_name': region_name,
            'employees_retrieved': 0,
            'employees_updated': 0,
            'employees_inserted': 0,
            'employees_unchanged': 0,
            'errors': 0
        }
        
        try:
            # Get employees from API
            employees = self.get_employees_for_region(region_name)
            region_stats['employees_retrieved'] = len(employees)
            
            if not employees:
                logger.warning(f"No employees found for region {region_name}")
                return region_stats
            
            # Process each employee
            for employee in employees:
                try:
                    # Transform data
                    employee_data = self.transform_employee_data(employee, region_name)
                    
                    # Update/insert record
                    operation = self.update_employee_record(employee_data)
                    
                    if operation == 'updated':
                        region_stats['employees_updated'] += 1
                    elif operation == 'inserted':
                        region_stats['employees_inserted'] += 1
                    else:  # unchanged
                        region_stats['employees_unchanged'] += 1
                        
                    self.sync_stats['total_employees_processed'] += 1
                    
                    # Log progress every 100 employees
                    total_processed = (region_stats['employees_updated'] + 
                                     region_stats['employees_inserted'] + 
                                     region_stats['employees_unchanged'])
                    if total_processed % 100 == 0:
                        logger.info(f"  Processed {total_processed} employees...")
                    
                except Exception as e:
                    error_msg = f"Error processing employee {employee.get('id', 'unknown')} in {region_name}: {e}"
                    logger.error(error_msg)
                    region_stats['errors'] += 1
                    self.sync_stats['errors'].append(error_msg)
            
            # Log region completion
            logger.info(f"✅ {region_name} completed:")
            logger.info(f"  Retrieved: {region_stats['employees_retrieved']}")
            logger.info(f"  Updated: {region_stats['employees_updated']}")
            logger.info(f"  Inserted: {region_stats['employees_inserted']}")
            logger.info(f"  Unchanged: {region_stats['employees_unchanged']}")
            logger.info(f"  Errors: {region_stats['errors']}")
            
            return region_stats
            
        except Exception as e:
            error_msg = f"Failed to process region {region_name}: {str(e)}"
            logger.error(error_msg)
            region_stats['errors'] = 1
            self.sync_stats['errors'].append(error_msg)
            return region_stats
    
    def sync_all_kaiser_employees(self) -> Dict:
        """Sync employees for all KAISER regions"""
        logger.info("Starting KAISER employee sync across all regions...")
        
        start_time = datetime.now()
        
        # Import ETLBatch here to avoid circular imports
        from tracktik_etl.etl.models import ETLBatch
        
        try:
            # Create ETL batch for tracking
            self.batch_id = ETLBatch.create_batch(
                'KAISER_EMPLOYEE_SYNC',
                {
                    'regions': self.KAISER_SUBREGIONS,
                    'sync_type': 'full_employee_sync',
                    'description': 'Complete employee data sync for all KAISER regions'
                }
            )
            logger.info(f"Created ETL batch: {self.batch_id}")
            
            # Load region mapping
            self.load_kaiser_region_mapping()
            
            # Process each region
            for region_name in self.KAISER_SUBREGIONS:
                region_stats = self.sync_region_employees(region_name)
                self.sync_stats['regions_processed'].append(region_stats)
                
                # Update totals
                self.sync_stats['total_employees_updated'] += region_stats['employees_updated']
                self.sync_stats['total_employees_inserted'] += region_stats['employees_inserted']
                self.sync_stats['total_employees_unchanged'] += region_stats.get('employees_unchanged', 0)
            
            # Calculate final stats
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.sync_stats.update({
                'start_time': start_time,
                'end_time': end_time,
                'duration_minutes': duration.total_seconds() / 60,
                'success': len(self.sync_stats['errors']) == 0
            })
            
            # Complete ETL batch
            if self.sync_stats['success']:
                ETLBatch.complete_batch(self.batch_id, self.sync_stats['total_employees_processed'])
            else:
                ETLBatch.complete_batch(
                    self.batch_id, 
                    self.sync_stats['total_employees_processed'], 
                    len(self.sync_stats['errors']),
                    '; '.join(self.sync_stats['errors'][:5])  # First 5 errors
                )
            
            # Print final summary
            self._print_final_summary()
            
            return self.sync_stats
            
        except Exception as e:
            logger.error(f"Employee sync failed: {str(e)}")
            self.sync_stats['errors'].append(str(e))
            self.sync_stats['success'] = False
            
            # Complete batch with error
            if self.batch_id:
                ETLBatch.complete_batch(self.batch_id, 0, 0, str(e))
            
            raise
    
    def _print_final_summary(self):
        """Print comprehensive summary of sync results"""
        stats = self.sync_stats
        
        logger.info(f"\n{'='*80}")
        logger.info("KAISER EMPLOYEE SYNC SUMMARY")
        logger.info(f"{'='*80}")
        logger.info(f"Start Time: {stats['start_time']}")
        logger.info(f"End Time: {stats['end_time']}")
        logger.info(f"Duration: {stats['duration_minutes']:.1f} minutes")
        logger.info(f"")
        logger.info(f"📊 OVERALL TOTALS:")
        logger.info(f"  Employees Processed: {stats['total_employees_processed']:,}")
        logger.info(f"  Employees Updated: {stats['total_employees_updated']:,}")
        logger.info(f"  Employees Inserted: {stats['total_employees_inserted']:,}")
        logger.info(f"  Employees Unchanged: {stats['total_employees_unchanged']:,}")
        logger.info(f"  API Calls Made: {stats['total_api_calls']:,}")
        logger.info(f"  Total Errors: {len(stats['errors'])}")
        logger.info(f"")
        logger.info(f"🏢 BY REGION:")
        
        for region_stats in stats['regions_processed']:
            logger.info(f"  {region_stats['region_name']:<15}: "
                       f"{region_stats['employees_retrieved']:>4} retrieved, "
                       f"{region_stats['employees_updated']:>4} updated, "
                       f"{region_stats['employees_inserted']:>4} inserted, "
                       f"{region_stats['employees_unchanged']:>4} unchanged, "
                       f"{region_stats['errors']:>2} errors")
        
        if stats['errors']:
            logger.info(f"\n❌ ERRORS ({len(stats['errors'])}):")
            for i, error in enumerate(stats['errors'][:10], 1):  # Show first 10 errors
                logger.info(f"  {i}. {error}")
            if len(stats['errors']) > 10:
                logger.info(f"  ... and {len(stats['errors']) - 10} more errors")
        else:
            logger.info(f"\n✅ No errors - sync completed successfully!")
        
        logger.info(f"{'='*80}")
    
    def get_current_employee_counts(self) -> Dict:
        """Get current employee counts by region for comparison"""
        query = f"""
            SELECT 
                r.name as region_name,
                COUNT(e.employee_id) as employee_count
            FROM {config.POSTGRES_SCHEMA}.dim_employees e
            JOIN {config.POSTGRES_SCHEMA}.dim_regions r ON e.region_id = r.region_id
            WHERE e.is_current = TRUE 
            AND r.is_current = TRUE
            AND r.parent_region_name ILIKE '%KAISER%'
            GROUP BY r.name
            ORDER BY employee_count DESC
        """
        
        results = db.execute_query(query)
        return {row['region_name']: row['employee_count'] for row in results}


def main():
    """Main entry point for KAISER employee sync"""
    
    print(f"""
╔══════════════════════════════════════════════════════════╗
║               KAISER Employee Sync                       ║
║                                                          ║
║  Scope: All 8 KAISER sub-regions                        ║
║  Action: Fetch complete employee details from API       ║
║  Target: Update dim_employees table                      ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    syncer = KaiserEmployeeSync()
    
    try:
        # Show current state before sync
        logger.info("Current employee counts by region:")
        current_counts = syncer.get_current_employee_counts()
        for region, count in current_counts.items():
            logger.info(f"  {region}: {count:,} employees")
        
        # Run the sync
        results = syncer.sync_all_kaiser_employees()
        
        if results['success']:
            logger.info("\n🎉 Employee sync completed successfully!")
        else:
            logger.error(f"\n⚠️ Employee sync completed with {len(results['errors'])} errors")
        
        # Show final state
        logger.info("\nFinal employee counts by region:")
        final_counts = syncer.get_current_employee_counts()
        for region, count in final_counts.items():
            change = count - current_counts.get(region, 0)
            change_str = f"(+{change})" if change > 0 else f"({change})" if change < 0 else "(no change)"
            logger.info(f"  {region}: {count:,} employees {change_str}")
        
    except Exception as e:
        logger.error(f"❌ Employee sync failed: {e}")
        raise


if __name__ == "__main__":
    main()
