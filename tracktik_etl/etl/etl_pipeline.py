# etl/etl_pipeline.py
"""
Main ETL Pipeline - Updated for Schema Alignment and KAISER Processing
"""
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import uuid
import json

from .config import config
from .tracktik_client import TrackTikClient
from .database import db
from .transformers import DataTransformer
from .models import (
    DimEmployee, DimClient, DimPosition, DimRegion,
    FactShift, BillingPeriod, ETLBatch, ETLSyncStatus
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """Main ETL Pipeline orchestrator with KAISER focus"""
    
    # KAISER sub-regions (from your previous work)
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
        self.transformer = DataTransformer()
        self.batch_id = None
        self.kaiser_region_mapping = {}
        
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
        
        # If empty, we need to sync regions first
        if not region_mapping:
            logger.warning("No KAISER regions found in database. Running region sync...")
            self.sync_regions_from_api()
            # Retry the query
            results = db.execute_query(query)
            region_mapping = {row['name']: row['region_id'] for row in results}
            
        self.kaiser_region_mapping = region_mapping
        return region_mapping
    
    def sync_regions_from_api(self) -> Dict[str, int]:
        """Sync all regions from TrackTik API to database"""
        logger.info("Syncing regions from TrackTik API...")
        
        try:
            # Create batch for region sync
            batch_id = ETLBatch.create_batch('REGION_SYNC', {
                'description': 'Sync all regions from TrackTik API'
            })
            
            # Get all regions from API
            regions = self.client.get_regions()
            logger.info(f"Retrieved {len(regions)} regions from API")
            
            # Upsert regions
            if regions:
                results = DimRegion.upsert(regions, batch_id)
                logger.info(f"Region sync results: {results}")
                
                # Complete batch
                ETLBatch.complete_batch(batch_id, len(regions))
                
                # Update sync status
                ETLSyncStatus.update_sync_status(
                    'dim_regions', 
                    datetime.now(), 
                    batch_id,
                    {'regions_synced': len(regions)}
                )
            else:
                ETLBatch.complete_batch(batch_id, 0, 0, "No regions returned from API")
                
        except Exception as e:
            logger.error(f"Failed to sync regions: {str(e)}")
            if self.batch_id:
                ETLBatch.complete_batch(batch_id, 0, 0, str(e))
            raise
    
    def get_clients_for_kaiser_region(self, region_name: str) -> List[Dict]:
        """Get all clients for a specific KAISER sub-region"""
        
        # Ensure we have region mapping
        if not self.kaiser_region_mapping:
            self.load_kaiser_region_mapping()
        
        region_id = self.kaiser_region_mapping.get(region_name)
        if not region_id:
            logger.warning(f"KAISER region '{region_name}' not found in mapping")
            return []
            
        logger.info(f"Getting clients for KAISER region: {region_name} (ID: {region_id})")
        
        try:
            # Get all clients and filter by region
            all_clients = self.client.get_clients()
            
            region_clients = []
            for client in all_clients:
                client_region_id = client.get('region', {}).get('id') if isinstance(client.get('region'), dict) else client.get('region')
                if client_region_id == region_id:
                    region_clients.append(client)
            
            logger.info(f"Found {len(region_clients)} clients in {region_name}")
            return region_clients
            
        except Exception as e:
            logger.error(f"Error getting clients for region {region_name}: {str(e)}")
            raise
    
    def load_dimensions_for_region(self, region_name: str, clients: List[Dict]) -> Dict[str, int]:
        """Load dimension data for a specific KAISER region"""
        logger.info(f"Loading dimensions for region: {region_name}")
        
        stats = {
            'clients': 0,
            'positions': 0,
            'employees': 0
        }
        
        try:
            # Load clients
            if clients:
                logger.info(f"  Loading {len(clients)} clients...")
                results = DimClient.upsert(clients, self.batch_id)
                stats['clients'] = results.get('inserted', 0) + results.get('updated', 0)
                logger.info(f"  Clients: {results}")
            
            # Load positions for these clients
            all_positions = []
            client_ids = [c['id'] for c in clients if isinstance(c, dict) and 'id' in c]
            
            logger.info(f"  Loading positions for {len(client_ids)} clients...")
            for client_id in client_ids:
                try:
                    positions = self.client.get_positions(account_id=client_id)
                    if positions:
                        all_positions.extend(positions)
                except Exception as e:
                    logger.warning(f"Error getting positions for client {client_id}: {e}")
            
            if all_positions:
                logger.info(f"  Loading {len(all_positions)} positions...")
                results = DimPosition.upsert(all_positions, self.batch_id)
                stats['positions'] = results.get('inserted', 0) + results.get('updated', 0)
                logger.info(f"  Positions: {results}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error loading dimensions for region {region_name}: {str(e)}")
            raise
    
    def load_shifts_for_region_period(self, region_name: str, period_id: str, 
                                start_date: str, end_date: str) -> Dict[str, int]:
        """Load shifts for a specific region and billing period"""
        logger.info(f"Loading shifts for {region_name}, period {period_id} ({start_date} to {end_date})")
        
        stats = {
            'shifts_retrieved': 0,
            'shifts_inserted': 0,
            'employees_found': 0,
            'data_quality_issues': 0
        }
        
        try:
            # Get clients for this region (still needed for dimensions)
            clients = self.get_clients_for_kaiser_region(region_name)
            if not clients:
                logger.warning(f"No clients found for region {region_name}")
                return stats
            
            # Load dimensions first
            dim_stats = self.load_dimensions_for_region(region_name, clients)
            
            # Get the region ID for this region name
            region_id = self.kaiser_region_mapping.get(region_name)
            if not region_id:
                logger.error(f"No region ID found for {region_name}")
                return stats
            
            logger.info(f"Retrieving shifts for region {region_name} (ID: {region_id})")
            
            # Get shifts filtered by employee region
            # Note: We can't use include parameter as it causes 400 errors
            try:
                params = {
                    'employee.region': region_id
                }
                
                all_shifts = self.client.get_shifts(start_date, end_date, **params)
                logger.info(f"Retrieved {len(all_shifts)} shifts for {region_name}")
                
            except Exception as e:
                logger.error(f"Error getting shifts for region {region_name}: {e}")
                return stats
            
            stats['shifts_retrieved'] = len(all_shifts)
            
            if all_shifts:
                # Extract unique employee IDs and position IDs
                employee_ids = set()
                position_ids = set()
                
                for shift in all_shifts:
                    if shift.get('employee'):
                        employee_ids.add(shift['employee'])
                    if shift.get('position'):
                        position_ids.add(shift['position'])
                
                logger.info(f"Found {len(employee_ids)} unique employees and {len(position_ids)} unique positions")
                
                # We need to fetch employee details since shifts only have IDs
                # For now, we'll create minimal employee records
                # In production, you might want to batch fetch employee details
                employee_records = []
                for emp_id in employee_ids:
                    employee_records.append({
                        'id': emp_id,
                        'region': region_id  # We know they're in this region
                    })
                
                if employee_records:
                    results = DimEmployee.upsert(employee_records, self.batch_id)
                    stats['employees_found'] = len(employee_ids)
                    logger.info(f"  Employees processed: {len(employee_ids)}")
                
                # Transform and insert shifts
                transformed_shifts = []
                for shift in all_shifts:
                    try:
                        # Since account is None, we'll need to get client_id from position
                        # For now, we'll handle this in the transformer
                        transformed = self.transformer.transform_shift(shift, period_id)
                        transformed['etl_batch_id'] = self.batch_id
                        transformed_shifts.append(transformed)
                    except Exception as e:
                        logger.error(f"Error transforming shift {shift.get('id')}: {e}")
                        stats['data_quality_issues'] += 1
                
                # After transforming shifts, look up client_ids from positions
                if transformed_shifts:
                    # Get unique position IDs
                    position_ids = set()
                    for shift in transformed_shifts:
                        if shift.get('position_id'):
                            position_ids.add(shift['position_id'])
                    
                    # Query dim_positions to get position->client mapping
                    if position_ids:
                        position_client_map = {}
                        
                        query = f"""
                            SELECT position_id, client_id 
                            FROM {config.POSTGRES_SCHEMA}.dim_positions 
                            WHERE position_id = ANY(%(position_ids)s) 
                            AND is_current = TRUE
                        """
                        
                        results = db.execute_query(query, {'position_ids': list(position_ids)})
                        for row in results:
                            position_client_map[row['position_id']] = row['client_id']
                        
                        logger.info(f"Loaded client mapping for {len(position_client_map)} positions")
                        
                        # Update transformed shifts with client_ids
                        for shift in transformed_shifts:
                            position_id = shift.get('position_id')
                            if position_id and position_id in position_client_map:
                                shift['client_id'] = position_client_map[position_id]
                            else:
                                # If we can't find the client, we'll need to handle this
                                logger.warning(f"No client found for position {position_id} in shift {shift.get('shift_id')}")
                                # Set a default or skip this shift
                                shift['client_id'] = None
                        
                        # ===== CRITICAL FILTERING CODE - THIS WAS MISSING! =====
                        # Filter out shifts without valid client_id
                        valid_shifts = [s for s in transformed_shifts if s.get('client_id') is not None]
                        if len(valid_shifts) < len(transformed_shifts):
                            logger.warning(f"Filtered out {len(transformed_shifts) - len(valid_shifts)} shifts without client_id")
                            stats['data_quality_issues'] += len(transformed_shifts) - len(valid_shifts)
                        transformed_shifts = valid_shifts
                        # ===== END OF CRITICAL FILTERING CODE =====

                # Insert shifts
                if transformed_shifts:
                    # First, let's check if the table exists and method is available
                    try:
                        # Check if we have the partitioned insert method
                        if hasattr(db, 'insert_fact_batch_partitioned'):
                            inserted_count = db.insert_fact_batch_partitioned(
                                'fact_shifts', 
                                transformed_shifts, 
                                'billing_period_id'
                            )
                        else:
                            # Fallback to regular batch insert
                            logger.warning("Partitioned insert not available, using regular batch insert")
                            inserted_count = len(transformed_shifts)
                            # You'll need to implement a regular batch insert in your database.py
                            # For now, we'll use the FactShift model
                            inserted_count = FactShift.insert_batch(transformed_shifts, period_id, self.batch_id)
                        
                        stats['shifts_inserted'] = inserted_count
                        logger.info(f"Inserted {inserted_count} shifts for {region_name}")
                        
                    except Exception as e:
                        logger.error(f"Error inserting shifts: {e}")
                        stats['data_quality_issues'] += len(transformed_shifts)
            
            return stats
            
        except Exception as e:
            logger.error(f"Error loading shifts for {region_name}: {str(e)}")
            raise
    
    def process_kaiser_billing_period(self, period_id: str) -> Dict[str, Any]:
        """
        Process all KAISER sub-regions for a billing period
        This is the main method you'll call
        """
        logger.info(f"Starting KAISER billing period processing: {period_id}")
        
        # Get period dates
        try:
            period_dates = BillingPeriod.get_period_dates(period_id)
            start_date = period_dates['start_date']
            end_date = period_dates['end_date']
        except ValueError as e:
            logger.error(f"Invalid billing period: {e}")
            raise
        
        # Create ETL batch
        self.batch_id = ETLBatch.create_batch(
            f'KAISER_BILLING_{period_id}',
            {
                'period_id': period_id,
                'start_date': start_date,
                'end_date': end_date,
                'regions': self.KAISER_SUBREGIONS
            }
        )
        
        overall_stats = {
            'period_id': period_id,
            'start_date': start_date,
            'end_date': end_date,
            'regions_processed': [],
            'total_shifts': 0,
            'total_employees': 0,
            'total_clients': 0,
            'total_positions': 0,
            'errors': []
        }
        
        try:
            # Ensure region mapping is loaded
            self.load_kaiser_region_mapping()
            
            # Process each KAISER sub-region
            for region_name in self.KAISER_SUBREGIONS:
                logger.info(f"\n{'='*50}")
                logger.info(f"Processing region: {region_name}")
                logger.info(f"{'='*50}")
                
                try:
                    region_stats = self.load_shifts_for_region_period(
                        region_name, period_id, start_date, end_date
                    )
                    
                    # Add region name to stats
                    region_stats['region_name'] = region_name
                    overall_stats['regions_processed'].append(region_stats)
                    
                    # Aggregate totals
                    overall_stats['total_shifts'] += region_stats['shifts_inserted']
                    overall_stats['total_employees'] += region_stats['employees_found']
                    overall_stats['total_clients'] += region_stats.get('clients', 0)
                    overall_stats['total_positions'] += region_stats.get('positions', 0)
                    
                    logger.info(f"✅ {region_name} complete: {region_stats['shifts_inserted']} shifts")
                    
                except Exception as e:
                    error_msg = f"Failed to process region {region_name}: {str(e)}"
                    logger.error(error_msg)
                    overall_stats['errors'].append(error_msg)
            
            # Complete batch
            if overall_stats['errors']:
                ETLBatch.complete_batch(
                    self.batch_id, 
                    overall_stats['total_shifts'], 
                    len(overall_stats['errors']), 
                    '; '.join(overall_stats['errors'])
                )
            else:
                ETLBatch.complete_batch(self.batch_id, overall_stats['total_shifts'])
            
            # Update sync status
            ETLSyncStatus.update_sync_status(
                'fact_shifts',
                datetime.now(),
                self.batch_id,
                overall_stats
            )
            
            # Print summary
            self._print_processing_summary(overall_stats)
            
            return overall_stats
            
        except Exception as e:
            logger.error(f"KAISER billing period processing failed: {str(e)}")
            ETLBatch.complete_batch(self.batch_id, 0, 0, str(e))
            raise
    
    def _print_processing_summary(self, stats: Dict[str, Any]):
        """Print a nice summary of the processing results"""
        logger.info(f"\n{'='*60}")
        logger.info("KAISER BILLING PERIOD PROCESSING SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Period: {stats['period_id']} ({stats['start_date']} to {stats['end_date']})")
        logger.info(f"Batch ID: {self.batch_id}")
        logger.info(f"")
        logger.info(f"📊 TOTALS:")
        logger.info(f"  Shifts Loaded: {stats['total_shifts']:,}")
        logger.info(f"  Employees: {stats['total_employees']:,}")
        logger.info(f"  Clients: {stats['total_clients']:,}")
        logger.info(f"  Positions: {stats['total_positions']:,}")
        logger.info(f"")
        logger.info(f"🏢 BY REGION:")
        
        for region_stats in stats['regions_processed']:
            logger.info(f"  {region_stats['region_name']:<15}: "
                       f"{region_stats['shifts_inserted']:>6,} shifts, "
                       f"{region_stats['employees_found']:>4} employees")
        
        if stats['errors']:
            logger.info(f"\n❌ ERRORS ({len(stats['errors'])}):")
            for error in stats['errors']:
                logger.info(f"  - {error}")
        else:
            logger.info(f"\n✅ No errors - processing completed successfully!")
        
        logger.info(f"{'='*60}")


def main():
    """Main entry point for KAISER billing period processing"""
    pipeline = ETLPipeline()
    
    # Process the most recent billing period: 2025-05-30 to 2025-06-12
    # This should be period 2025_12 based on your Friday-Thursday schedule
    period_id = '2025_12'
    
    try:
        results = pipeline.process_kaiser_billing_period(period_id)
        logger.info("✅ KAISER billing period processing completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ KAISER billing period processing failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
