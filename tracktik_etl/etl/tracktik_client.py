# etl/tracktik_client.py
"""
TrackTik API Client with authentication and retry logic
UPDATED WITH get_regions() method and improved error handling
"""
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import config

logger = logging.getLogger(__name__)


class TrackTikClient:
    """TrackTik API Client with OAuth2 authentication"""
    
    def __init__(self):
        self.base_url = config.TRACKTIK_BASE_URL
        self.access_token = None
        self.token_expires_at = None
        self.session = self._create_session()
        
    def _create_session(self) -> requests.Session:
        """Create session with retry logic"""
        session = requests.Session()
        retry_strategy = Retry(
            total=config.MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
        
    def authenticate(self) -> None:
        """Authenticate and get access token"""
        auth_data = {
            'client_id': config.TRACKTIK_CLIENT_ID,
            'client_secret': config.TRACKTIK_CLIENT_SECRET,
            'username': config.TRACKTIK_USERNAME,
            'password': config.TRACKTIK_PASSWORD,
            'grant_type': 'password'
        }
        
        response = self.session.post(
            f"{self.base_url}/rest/oauth2/access_token",
            data=auth_data
        )
        response.raise_for_status()
        
        tokens = response.json()
        self.access_token = tokens['access_token']
        # Set expiry with 5-minute buffer
        self.token_expires_at = datetime.now() + timedelta(seconds=tokens.get('expires_in', 3600) - 300)
        logger.info("Successfully authenticated with TrackTik API")
        
    def _ensure_authenticated(self) -> None:
        """Ensure we have a valid token"""
        if not self.access_token or datetime.now() >= self.token_expires_at:
            self.authenticate()
            
    def _get_headers(self) -> Dict[str, str]:
        """Get headers with authentication"""
        self._ensure_authenticated()
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }

    def get_paginated_data(self, endpoint: str, params: Dict[str, Any] = None) -> List[Dict]:
        """
        Get all pages of data from an endpoint
        
        Args:
            endpoint: API endpoint (e.g., '/rest/v1/shifts')
            params: Query parameters
            
        Returns:
            List of all records
        """
        if params is None:
            params = {}
            
        all_records = []
        offset = 0
        total_count = None
        
        while True:
            # Add pagination params
            current_params = params.copy()
            current_params.update({
                'limit': config.API_PAGE_SIZE,
                'offset': offset
            })
            
            # Make request
            response = self.session.get(
                f"{self.base_url}{endpoint}",
                headers=self._get_headers(),
                params=current_params
            )
            response.raise_for_status()
            
            data = response.json()
            records = data.get('data', [])
            all_records.extend(records)
            
            # Get total count from meta
            meta = data.get('meta', {})
            if total_count is None and 'count' in meta:
                total_count = meta['count']
                logger.info(f"Fetching {endpoint}: {total_count} total records")
            
            # Stop conditions
            if not records:  # No records returned
                break
                
            if total_count and len(all_records) >= total_count:  # Retrieved all
                break
                
            if len(records) < config.API_PAGE_SIZE:  # Last page
                break
                
            offset += config.API_PAGE_SIZE
            
            # Respect rate limits
            if 'X-RateLimit-Remaining' in response.headers:
                remaining = int(response.headers['X-RateLimit-Remaining'])
                if remaining < 10:
                    logger.warning(f"Rate limit low ({remaining} remaining), sleeping...")
                    time.sleep(5)
                    
        logger.info(f"Retrieved {len(all_records)} records from {endpoint}")
        return all_records

    def get_shifts(self, start_date: str, end_date: str, **kwargs) -> List[Dict]:
        """
        Get shifts for a date range
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            **kwargs: Additional parameters (e.g., status='APPROVED', include='employee,position')
            
        Returns:
            List of shift records
            
        Note: Date range cannot exceed 31 days per API requirements
        """
        # Validate date range
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        if (end - start).days > 31:
            raise ValueError("Date range cannot exceed 31 days per TrackTik API requirements")
        
        # Use :between filter which actually works!
        params = {
            'startDateTime:between': f'{start_date}|{end_date}',
            **kwargs
        }
        
        return self.get_paginated_data('/rest/v1/shifts', params)
        
    def get_employees(self, **kwargs) -> List[Dict]:
        """Get all employees"""
        params = {
            'status': 'ACTIVE',
            **kwargs
        }
        return self.get_paginated_data('/rest/v1/employees', params)
        
    def get_clients(self, **kwargs) -> List[Dict]:
        """Get all clients/sites"""
        return self.get_paginated_data('/rest/v1/clients', kwargs)
        
    def get_positions(self, account_id: int = None, **kwargs) -> List[Dict]:
        """Get all positions, optionally filtered by account/client ID"""
        params = {
            'include': 'account',
            **kwargs
        }
        
        # Add account filter if specified
        if account_id:
            params['account.id'] = account_id
            
        return self.get_paginated_data('/rest/v1/positions', params)
    
    def get_regions(self, **kwargs) -> List[Dict]:
        """Get all regions from TrackTik API"""
        params = {
            'include': 'parentRegion',  # Include parent region data
            **kwargs
        }
        return self.get_paginated_data('/rest/v1/regions', params)
    
    def get_specific_shift(self, shift_id: int) -> Dict:
        """Get a specific shift by ID with full details"""
        endpoint = f'/rest/v1/shifts/{shift_id}'
        params = {
            'include': 'employee,position,account,summary'
        }
        
        response = self.session.get(
            f"{self.base_url}{endpoint}",
            headers=self._get_headers(),
            params=params
        )
        response.raise_for_status()
        
        data = response.json()
        return data.get('data', {})
    
    def test_connection(self) -> bool:
        """Test API connection and authentication"""
        try:
            # Try to get a small amount of data
            response = self.session.get(
                f"{self.base_url}/rest/v1/employees",
                headers=self._get_headers(),
                params={'limit': 1}
            )
            response.raise_for_status()
            
            data = response.json()
            logger.info("✅ TrackTik API connection test successful")
            return True
            
        except Exception as e:
            logger.error(f"❌ TrackTik API connection test failed: {e}")
            return False
    
    def get_account_info(self) -> Dict:
        """Get information about the current account/company"""
        try:
            response = self.session.get(
                f"{self.base_url}/rest/v1/account",
                headers=self._get_headers()
            )
            response.raise_for_status()
            
            data = response.json()
            return data.get('data', {})
            
        except Exception as e:
            logger.error(f"Failed to get account info: {e}")
            return {}
