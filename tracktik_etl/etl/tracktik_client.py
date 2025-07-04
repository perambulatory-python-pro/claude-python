# etl/tracktik_client.py
"""
TrackTik API Client with authentication and retry logic
"""
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

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
        page = 1
        total_pages = None
        
        while True:
            # Add pagination params
            params.update({
                'limit': config.API_PAGE_SIZE,
                'page': page
            })
            
            # Make request
            response = self.session.get(
                f"{self.base_url}{endpoint}",
                headers=self._get_headers(),
                params=params
            )
            response.raise_for_status()
            
            data = response.json()
            all_records.extend(data['data'])
            
            # Check pagination
            if total_pages is None:
                total_pages = data['meta']['pagination']['total_pages']
                logger.info(f"Fetching {endpoint}: {total_pages} pages total")
                
            if page >= total_pages:
                break
                
            page += 1
            
            # Respect rate limits
            if 'X-RateLimit-Remaining' in response.headers:
                remaining = int(response.headers['X-RateLimit-Remaining'])
                if remaining < 10:
                    logger.warning(f"Rate limit low ({remaining} remaining), sleeping...")
                    time.sleep(5)
                    
        logger.info(f"Retrieved {len(all_records)} records from {endpoint}")
        return all_records
        
    def get_shifts(self, start_date: str, end_date: str, **kwargs) -> List[Dict]:
        """Get shifts for a date range"""
        params = {
            'startedOn:gte': start_date,
            'startedOn:lte': end_date,
            'include': 'employee,position,account',
            'status': 'APPROVED',  # Only approved shifts for billing
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
        
    def get_positions(self, **kwargs) -> List[Dict]:
        """Get all positions"""
        params = {
            'include': 'account',
            **kwargs
        }
        return self.get_paginated_data('/rest/v1/positions', params)