# etl/kaiser_billing_processor.py
"""
KAISER billing period processor with sub-region support
"""
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import logging

from etl.tracktik_client import TrackTikClient
from etl.utils.rate_limiter import RateLimiter, CheckpointManager
from etl.database import db
from .models import (
    DimEmployee, DimClient, DimPosition, DimRegion,
    FactShift, BillingPeriod, ETLRunLog
)

logger = logging.getLogger(__name__)


class KaiserBillingProcessor:
    """Process TrackTik data for KAISER billing periods"""
    
    # KAISER sub-regions from the CSV
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
        self.rate_limiter = RateLimiter(calls_per_minute=30)  # Conservative rate
        self.checkpoint_manager = CheckpointManager()
        self.db = db
        
    def process_billing_period(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Process all KAISER sub-regions for a billing period
        
        Args:
            start_date: YYYY-MM-DD format
            end_date: YYYY-MM-DD format
            
        Returns:
            Summary statistics dictionary
        """
        billing_period_id = f"{start_date}_to_{end_date}"
        overall_stats = {
            'billing_period': billing_period_id,
            'start_date': start_date,
            'end_date': end_date,
            'regions_processed': [],
            'total_shifts': 0,
            'total_hours': 0,
            'errors': []
        }
        
        logger.info(f"Starting KAISER billing period processing: {billing_period_id}")
        
        # Process each sub-region
        for region in self.KAISER_SUBREGIONS:
            try:
                logger.info(f"\nProcessing region: {region}")
                region_stats = self._process_single_region(
                    region, start_date, end_date, billing_period_id
                )
                
                overall_stats['regions_processed'].append(region_stats)
                overall_stats['total_shifts'] += region_stats['shifts_processed']
                overall_stats['total_hours'] += region_stats['total_hours']
                
            except Exception as e:
                error_msg = f"Failed to process region {region}: {str(e)}"
                logger.error(error_msg)
                overall_stats['errors'].append(error_msg)
                
        # Log ETL run
        self._log_etl_run(overall_stats)
        
        return overall_stats
    
    def _process_single_region(self, region: str, start_date: str, 
                          end_date: str, billing_period_id: str) -> Dict[str, Any]:
        """Process a single KAISER sub-region"""
        
        # Check for existing checkpoint
        checkpoint = self.checkpoint_manager.load_checkpoint(billing_period_id, region)
        
        stats = {
            'region': region,
            'shifts_processed': 0,
            'total_hours': 0,
            'employees_found': 0,
            'positions_found': 0,
            'clients_found': 0,
            'resumed_from_checkpoint': checkpoint is not None
        }
        
        # Get region's clients
        region_clients = self._get_region_clients(region)
        stats['clients_found'] = len(region_clients)
        
        if not region_clients:
            logger.warning(f"No clients found for region: {region}")
            return stats
        
        # Extract client IDs
        client_ids = [c['id'] for c in region_clients]
        
        # Load dimensions first
        if not checkpoint or checkpoint.get('dimensions_complete') != True:
            self._load_region_dimensions(region, region_clients)
            # Update checkpoint
            self.checkpoint_manager.save_checkpoint(
                billing_period_id, region,
                {'dimensions_complete': True, 'client_ids': client_ids}
            )
        
        # Get all shifts for the region
        all_shifts = self._get_region_shifts(client_ids, start_date, end_date)
        
        # Insert shifts in batches
        if all_shifts:
            # Collect unique employees from shifts
            unique_employees = {}
            for shift in all_shifts:
                if isinstance(shift.get('employee'), dict):
                    emp_id = shift['employee']['id']
                    if emp_id not in unique_employees:
                        unique_employees[emp_id] = shift['employee']
            
            # Load employee dimensions
            if unique_employees:
                employee_stats = DimEmployee.upsert(list(unique_employees.values()))
                stats['employees_found'] = employee_stats['inserted'] + employee_stats['updated']
                logger.info(f"  Employees: {employee_stats}")
            
            # Insert shifts
            inserted = FactShift.insert_batch(all_shifts)
            stats['shifts_processed'] = inserted
            stats['total_hours'] = sum(shift.get('actualHours', 0) for shift in all_shifts)
        
        # Clear checkpoint on completion
        self.checkpoint_manager.clear_checkpoint(billing_period_id, region)
        
        logger.info(f"Region {region} complete: {stats['shifts_processed']} shifts, "
                f"{stats['total_hours']:.2f} hours")
        
        return stats
    
    def _get_region_clients(self, region_name: str) -> List[Dict]:
        """Get all clients for a specific KAISER sub-region"""
        self.rate_limiter.wait_if_needed()
        
        try:
            # Get all clients and filter by region
            all_clients = self.client.get_clients(include='region')
            self.rate_limiter.record_success()
            
            region_clients = []
            for client in all_clients:
                client_region = client.get('region', {})
                if isinstance(client_region, dict):
                    if client_region.get('name') == region_name:
                        # Check if parent is KAISER
                        parent_region = client_region.get('parentRegion', {})
                        if parent_region and 'KAISER' in parent_region.get('name', ''):
                            region_clients.append(client)
            
            return region_clients
            
        except Exception as e:
            self.rate_limiter.record_error()
            raise
    
    def _get_region_shifts(self, client_ids: List[int], start_date: str, 
                          end_date: str, start_from: int = 0) -> List[Dict]:
        """Get all shifts for region's clients"""
        all_shifts = []
        
        # Process in batches of 20 clients to avoid API limits
        for i in range(0, len(client_ids), 20):
            batch_ids = client_ids[i:i+20]
            
            self.rate_limiter.wait_if_needed()
            
            try:
                params = {
                    'account.id:in': ','.join(map(str, batch_ids)),
                    'include': 'employee,position,account'
                }
                
                shifts = self.client.get_shifts(start_date, end_date, **params)
                all_shifts.extend(shifts)
                self.rate_limiter.record_success()
                
                logger.debug(f"Retrieved {len(shifts)} shifts for batch {i//20 + 1}")
                
            except Exception as e:
                self.rate_limiter.record_error()
                logger.error(f"Error getting shifts for batch: {e}")
                # Continue with next batch instead of failing entirely
                
        return all_shifts
    
    def _load_region_dimensions(self, region: str, clients: List[Dict]):
        """Load dimension data for the region"""
        logger.info(f"Loading dimensions for {region}")
        
        # Load region dimension
        regions_to_load = [{
            'name': region,
            'parent_region': 'KAISER',
            'level': 2,
            'active': True
        }]
        region_stats = DimRegion.upsert(regions_to_load)
        logger.info(f"  Regions: {region_stats}")
        
        # Load clients
        client_stats = DimClient.upsert(clients)
        logger.info(f"  Clients: {client_stats}")
        
        # Get and load positions for these clients
        all_positions = []
        for client in clients:
            self.rate_limiter.wait_if_needed()
            try:
                positions = self.client.get_positions(account_id=client['id'])
                all_positions.extend(positions)
                self.rate_limiter.record_success()
            except Exception as e:
                logger.error(f"Error getting positions for client {client['id']}: {e}")
                self.rate_limiter.record_error()
        
        if all_positions:
            position_stats = DimPosition.upsert(all_positions)
            logger.info(f"  Positions: {position_stats}")
        
        # Get and load employees (this might need to be done differently)
        # Employees might come from shifts instead of being pre-loaded

    def _process_shift(self, shift: Dict):
        """Process and store a single shift"""
        # Shifts are collected and batch inserted
        # This method might not be needed - see below
        pass

    def _log_etl_run(self, stats: Dict):
        """Log the ETL run for monitoring"""
        ETLRunLog.log_run(stats)