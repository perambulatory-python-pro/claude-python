# etl/transformers.py
"""
Data transformation logic
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transform TrackTik API data to warehouse schema"""
    
    @staticmethod
    def transform_employee(api_data: Dict) -> Dict:
        """Transform employee from API to dimension format"""
        return {
            'employee_id': api_data['id'],
            'custom_id': api_data.get('customId'),
            'first_name': api_data.get('firstName'),
            'last_name': api_data.get('lastName'),
            'email': api_data.get('email'),
            'phone': api_data.get('phone'),
            'status': api_data.get('status'),
            'region_id': api_data.get('region', {}).get('id'),
            'region_name': api_data.get('region', {}).get('name'),
        }
        
    @staticmethod
    def transform_client(api_data: Dict) -> Dict:
        """Transform client/site from API to dimension format"""
        return {
            'client_id': api_data['id'],
            'custom_id': api_data.get('customId'),
            'name': api_data.get('name'),
            'region_id': api_data.get('region', {}).get('id'),
            'region_name': api_data.get('region', {}).get('name'),
            'parent_region_name': api_data.get('region', {}).get('parentRegion', {}).get('name'),
            'time_zone': api_data.get('timeZone'),
            'address': {
                'street': api_data.get('address'),
                'city': api_data.get('city'),
                'state': api_data.get('state'),
                'zip': api_data.get('zip')
            } if api_data.get('address') else None,
        }
        
    @staticmethod
    def transform_position(api_data: Dict) -> Dict:
        """Transform position from API to dimension format"""
        account = api_data.get('account', {}) or {}
        
        return {
            'position_id': api_data['id'],
            'custom_id': api_data.get('customId'),
            'name': api_data.get('name'),
            'client_id': account.get('id'),
            'status': api_data.get('status'),
            'position_type': api_data.get('type'),
            'client_name': account.get('name'),
            'client_custom_id': account.get('customId'),
        }
        
    @staticmethod
    def transform_shift(api_data: Dict, billing_period_id: str) -> Dict:
        """Transform shift from API to fact format"""
        # Parse timestamps - API returns UTC, convert to account timezone
        account_tz = api_data.get('account', {}).get('timeZone', 'UTC')
        tz = pytz.timezone(account_tz)
        
        start_dt = datetime.fromisoformat(api_data['startedOn'].replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(api_data['endedOn'].replace('Z', '+00:00'))
        
        # Convert to account timezone
        start_local = start_dt.astimezone(tz)
        end_local = end_dt.astimezone(tz)
        
        # Extract summary data if available
        summary = api_data.get('summary', {}) or {}
        
        return {
            'shift_id': api_data['id'],
            'billing_period_id': billing_period_id,
            'employee_id': api_data.get('employee', {}).get('id'),
            'position_id': api_data.get('position', {}).get('id'),
            'client_id': api_data.get('account', {}).get('id'),
            'shift_date': start_local.date(),
            'start_datetime': start_dt,  # Store as UTC
            'end_datetime': end_dt,      # Store as UTC
            'scheduled_hours': api_data.get('plannedDurationHours'),
            'clocked_hours': api_data.get('clockedHours'),
            'approved_hours': api_data.get('plannedDurationHours'),  # Assuming approved = planned
            'billable_hours': summary.get('billableHours', api_data.get('billableHours')),
            'payable_hours': summary.get('payableHours', api_data.get('payableHours')),
            'bill_rate_regular': summary.get('billBaseRateActual'),
            'bill_rate_effective': summary.get('billEffectiveRateActual'),
            'bill_overtime_hours': summary.get('billOvertimeHours'),
            'bill_overtime_impact': summary.get('billOvertimeImpactActual'),
            'bill_total': summary.get('billTotal'),
            'status': api_data.get('status'),
            'approved_by': api_data.get('approvedBy', {}).get('id') if api_data.get('approvedBy') else None,
            'approved_at': api_data.get('approvedOn'),
            'raw_data': api_data,  # Store complete API response
        }