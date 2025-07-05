# etl/transformers.py
"""
Data transformation logic - Updated for Schema Alignment
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import pytz
import json

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transform TrackTik API data to warehouse schema"""
    
    @staticmethod
    def transform_employee(api_data: Dict) -> Dict:
        """Transform employee from API to dimension format"""
        # Defensive check: ensure api_data is a dictionary
        if not isinstance(api_data, dict):
            logger.error(f"Expected dict for employee data, got {type(api_data)}: {api_data}")
            return {}
        
        # Extract region info safely - handle both int IDs and region objects
        region_data = api_data.get('region')
        region_id = None
        region_name = None
        
        if isinstance(region_data, dict):
            # It's a region object
            region_id = region_data.get('id')
            region_name = region_data.get('name')
        elif isinstance(region_data, (int, str)):
            # It's just a region ID
            region_id = region_data
            # region_name will be None
        
        return {
            'employee_id': api_data.get('id'),
            'custom_id': api_data.get('customId'),
            'first_name': api_data.get('firstName'),
            'last_name': api_data.get('lastName'),
            'email': api_data.get('email'),
            'phone': api_data.get('phone'),
            'status': api_data.get('status'),
            'region_id': region_id,
            'region_name': region_name,
        }
        
    @staticmethod
    def transform_client(api_data: Dict) -> Dict:
        """Transform client/site from API to dimension format"""
        # Defensive check: ensure api_data is a dictionary
        if not isinstance(api_data, dict):
            logger.error(f"Expected dict for client data, got {type(api_data)}: {api_data}")
            return {}
        
        # Extract region info safely - handle both int IDs and region objects
        region_data = api_data.get('region')
        region_id = None
        region_name = None
        parent_region_name = None
        
        if isinstance(region_data, dict):
            # It's a region object
            region_id = region_data.get('id')
            region_name = region_data.get('name')
            parent_region = region_data.get('parentRegion', {})
            if isinstance(parent_region, dict):
                parent_region_name = parent_region.get('name')
        elif isinstance(region_data, (int, str)):
            # It's just a region ID
            region_id = region_data
            # region_name will be None - we'll look it up from dim_regions if needed
        
        # Build address JSONB structure
        address_data = None
        if any([api_data.get('address'), api_data.get('city'), api_data.get('state'), api_data.get('zip')]):
            address_data = {
                'street': api_data.get('address'),
                'city': api_data.get('city'),
                'state': api_data.get('state'),
                'zip': api_data.get('zip')
            }
        
        return {
            'client_id': api_data.get('id'),
            'custom_id': api_data.get('customId'),
            'name': api_data.get('name'),
            'region_id': region_id,
            'region_name': region_name,
            'parent_region_name': parent_region_name,
            'time_zone': api_data.get('timeZone'),
            'address': json.dumps(address_data) if address_data else None,
        }
        
    @staticmethod
    def transform_position(api_data: Dict) -> Dict:
        """Transform position from API to dimension format"""
        # Defensive check: ensure api_data is a dictionary
        if not isinstance(api_data, dict):
            logger.error(f"Expected dict for position data, got {type(api_data)}: {api_data}")
            return {}
        
        # Extract account/client info safely - handle both int IDs and account objects
        account_data = api_data.get('account')
        client_id = None
        client_name = None
        client_custom_id = None
        
        if isinstance(account_data, dict):
            # It's an account object
            client_id = account_data.get('id')
            client_name = account_data.get('name')
            client_custom_id = account_data.get('customId')
        elif isinstance(account_data, (int, str)):
            # It's just an account ID
            client_id = account_data
        
        return {
            'position_id': api_data.get('id'),
            'custom_id': api_data.get('customId'),
            'name': api_data.get('name'),
            'client_id': client_id,
            'status': api_data.get('status'),
            'position_type': api_data.get('type'),
            'client_name': client_name,
            'client_custom_id': client_custom_id,
        }
        
    @staticmethod
    def transform_region(api_data: Dict) -> Dict:
        """Transform region from API to dimension format"""
        # Extract parent region info safely
        parent_info = api_data.get('parentRegion', {}) or {}
        
        return {
            'region_id': api_data['id'],  # Schema uses region_id
            'custom_id': api_data.get('customId'),
            'name': api_data.get('name'),
            'parent_region_id': parent_info.get('id'),
            'parent_region_name': parent_info.get('name'),
            'company': api_data.get('company'),
            'status': api_data.get('status'),
        }
        
    @staticmethod
    def transform_shift(api_data: Dict, billing_period_id: str) -> Dict:
        """Transform shift from API to fact format"""
        
        # Defensive check: ensure api_data is a dictionary
        if not isinstance(api_data, dict):
            logger.error(f"Expected dict for shift data, got {type(api_data)}: {api_data}")
            return {}
        
        # Parse timestamps
        start_dt = None
        end_dt = None
        shift_date = None
        
        try:
            if api_data.get('startDateTime'):
                start_dt = datetime.fromisoformat(api_data['startDateTime'])
                shift_date = start_dt.date()
                
            if api_data.get('endDateTime'):
                end_dt = datetime.fromisoformat(api_data['endDateTime'])
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Error parsing shift timestamps for shift {api_data.get('id')}: {e}")
        
        # Extract IDs - they're already integers in the response
        employee_id = api_data.get('employee')
        position_id = api_data.get('position')
        
        # Since account is None in shifts, we need to get client_id from position
        # We'll need to look this up from dim_positions
        client_id = None
        if position_id:
            # We'll set this to position_id for now and update it after
            # Or you can do a lookup here if you have access to the database
            client_id = position_id  # TEMPORARY - see below for proper solution
        
        # Build the transformed shift record
        transformed_shift = {
            'shift_id': api_data.get('id'),
            'billing_period_id': billing_period_id,
            'employee_id': employee_id,
            'position_id': position_id,
            'client_id': client_id,  # Will be updated with lookup
            'shift_date': shift_date,
            'start_datetime': start_dt,
            'end_datetime': end_dt,
            
            # Hours columns
            'scheduled_hours': DataTransformer._safe_float(api_data.get('plannedDurationHours')),
            'clocked_hours': DataTransformer._safe_float(api_data.get('clockedHours')),
            'approved_hours': DataTransformer._safe_float(api_data.get('approvedHours')),
            'billable_hours': DataTransformer._safe_float(api_data.get('billableHours')),
            'payable_hours': DataTransformer._safe_float(
                api_data.get('payableHours') or api_data.get('plannedPayableHours')
            ),
            
            # Status
            'status': api_data.get('status'),
            
            # Store complete API response as JSONB
            'raw_data': json.dumps(api_data),
        }
        
        return transformed_shift
    
    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        """Safely convert value to float, return None if not possible"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        """Safely convert value to int, return None if not possible"""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def validate_shift_data(shift_data: Dict) -> List[str]:
        """
        Validate transformed shift data and return list of issues
        
        Returns:
            List of validation error messages
        """
        issues = []
        
        # Required fields validation
        required_fields = {
            'shift_id': 'Shift ID',
            'billing_period_id': 'Billing Period ID',
            'employee_id': 'Employee ID',
            'position_id': 'Position ID',
            'client_id': 'Client ID'
        }
        
        for field, display_name in required_fields.items():
            if not shift_data.get(field):
                issues.append(f"Missing {display_name}")
        
        # Date validation
        if not shift_data.get('shift_date'):
            issues.append("Missing shift date")
        
        # Hours validation
        hours_fields = ['scheduled_hours', 'clocked_hours', 'approved_hours', 'billable_hours', 'payable_hours']
        for field in hours_fields:
            value = shift_data.get(field)
            if value is not None and (value < 0 or value > 24):
                issues.append(f"Invalid {field}: {value} (must be 0-24)")
        
        # Status validation
        valid_statuses = ['SCHEDULED', 'IN_PROGRESS', 'COMPLETED', 'APPROVED', 'CANCELLED']
        if shift_data.get('status') and shift_data['status'] not in valid_statuses:
            issues.append(f"Invalid status: {shift_data['status']}")
        
        return issues
    
    @staticmethod
    def calculate_shift_metrics(shift_data: Dict) -> Dict[str, float]:
        """
        Calculate additional metrics for a shift
        
        Returns:
            Dict with calculated metrics
        """
        metrics = {}
        
        # Extract hours
        scheduled = shift_data.get('scheduled_hours', 0) or 0
        clocked = shift_data.get('clocked_hours', 0) or 0
        approved = shift_data.get('approved_hours', 0) or 0
        billable = shift_data.get('billable_hours', 0) or 0
        
        # Calculate variances
        metrics['scheduled_vs_clocked_variance'] = clocked - scheduled
        metrics['scheduled_vs_approved_variance'] = approved - scheduled
        metrics['clocked_vs_approved_variance'] = approved - clocked
        
        # Calculate percentages (avoid division by zero)
        if scheduled > 0:
            metrics['clocked_vs_scheduled_percent'] = (clocked / scheduled) * 100
            metrics['approved_vs_scheduled_percent'] = (approved / scheduled) * 100
        
        if clocked > 0:
            metrics['approved_vs_clocked_percent'] = (approved / clocked) * 100
        
        # Billing efficiency
        if approved > 0 and billable is not None:
            metrics['billable_vs_approved_percent'] = (billable / approved) * 100
        
        # Time calculations
        start_dt = shift_data.get('start_datetime')
        end_dt = shift_data.get('end_datetime')
        
        if start_dt and end_dt:
            duration_hours = (end_dt - start_dt).total_seconds() / 3600
            metrics['actual_duration_hours'] = duration_hours
            
            if clocked > 0:
                metrics['break_time_hours'] = max(0, duration_hours - clocked)
        
        return metrics


# Utility functions for common transformations
def extract_nested_id(obj: Any, default: Any = None) -> Any:
    """Extract ID from nested object or return the value if it's already an ID"""
    if isinstance(obj, dict):
        return obj.get('id', default)
    elif obj:
        return obj
    else:
        return default


def safe_datetime_parse(date_string: str, default: datetime = None) -> Optional[datetime]:
    """Safely parse ISO datetime string"""
    if not date_string:
        return default
        
    try:
        # Handle TrackTik's ISO format with Z suffix
        if date_string.endswith('Z'):
            date_string = date_string.replace('Z', '+00:00')
        return datetime.fromisoformat(date_string)
    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to parse datetime '{date_string}': {e}")
        return default


def build_address_json(api_data: Dict) -> Optional[str]:
    """Build address JSON from API data"""
    address_fields = ['address', 'city', 'state', 'zip', 'country']
    address_data = {}
    
    for field in address_fields:
        value = api_data.get(field)
        if value:
            address_data[field] = value
    
    return json.dumps(address_data) if address_data else None
