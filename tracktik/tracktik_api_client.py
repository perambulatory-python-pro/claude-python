"""
TrackTik API Client
Handles authentication and data retrieval from TrackTik
"""

import requests
import json
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List, Optional, Any
import logging
from functools import wraps
import time
import os
from urllib.parse import urljoin, quote

class TrackTikAPIClient:
    """
    Professional TrackTik API client with OAuth2 authentication
    and comprehensive error handling
    """
    
    def __init__(self, client_id: str, client_secret: str, 
                 base_url: str = "https://api.tracktik.com",
                 environment: str = "production"):
        """
        Initialize TrackTik API client
        
        Args:
            client_id: Your TrackTik client ID
            client_secret: Your TrackTik client secret
            base_url: API base URL (varies by region/environment)
            environment: 'production' or 'sandbox'
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = base_url.rstrip('/')
        self.environment = environment
        
        # OAuth token management
        self.access_token = None
        self.token_expires_at = None
        self.refresh_token = None
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Session for connection pooling
        self.session = requests.Session()
        
        # Rate limiting
        self.rate_limit_remaining = None
        self.rate_limit_reset = None
        
        # Statistics
        self.api_calls_made = 0
        self.api_errors = 0
    
    def authenticate(self):
        """
        Authenticate with TrackTik API using OAuth2 client credentials flow
        """
        auth_url = f"{self.base_url}/oauth/token"
        
        auth_data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': 'read write'  # Adjust scope as needed
        }
        
        try:
            response = self.session.post(auth_url, data=auth_data)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data['access_token']
            
            # Calculate token expiration
            expires_in = token_data.get('expires_in', 3600)  # Default 1 hour
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)
            
            self.logger.info("Successfully authenticated with TrackTik API")
            return True
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Authentication failed: {str(e)}")
            raise
    
    def ensure_authenticated(self):
        """
        Ensure we have a valid authentication token
        """
        if not self.access_token or datetime.now() >= self.token_expires_at:
            self.authenticate()
    
    def _make_request(self, method: str, endpoint: str, 
                     params: Optional[Dict] = None, 
                     data: Optional[Dict] = None,
                     retry_count: int = 3) -> Dict:
        """
        Make an authenticated API request with retry logic
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint (e.g., '/v1/employees')
            params: Query parameters
            data: Request body data
            retry_count: Number of retries for failed requests
            
        Returns:
            Response data as dictionary
        """
        self.ensure_authenticated()
        
        # Build full URL
        url = urljoin(self.base_url, endpoint)
        
        # Add authentication header
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        # Make request with retry logic
        for attempt in range(retry_count):
            try:
                self.api_calls_made += 1
                
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    json=data,
                    timeout=30
                )
                
                # Check rate limiting headers
                self.rate_limit_remaining = response.headers.get('X-RateLimit-Remaining')
                self.rate_limit_reset = response.headers.get('X-RateLimit-Reset')
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    self.logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                self.api_errors += 1
                self.logger.error(f"API request failed (attempt {attempt + 1}/{retry_count}): {str(e)}")
                
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise
    
    # === EMPLOYEE ENDPOINTS ===
    
    def get_employees(self, active_only: bool = True, 
                     page: int = 1, 
                     per_page: int = 100) -> List[Dict]:
        """
        Get list of employees
        
        Args:
            active_only: Only return active employees
            page: Page number for pagination
            per_page: Results per page (max 100)
            
        Returns:
            List of employee records
        """
        params = {
            'page': page,
            'perPage': per_page
        }
        
        if active_only:
            params['filter[status]'] = 'ACTIVE'
        
        response = self._make_request('GET', '/v1/employees', params=params)
        return response.get('data', [])
    
    def get_employee_by_id(self, employee_id: str) -> Dict:
        """Get specific employee details"""
        return self._make_request('GET', f'/v1/employees/{employee_id}')
    
    # === SHIFT ENDPOINTS ===
    
    def get_shifts(self, start_date: datetime, 
                  end_date: datetime,
                  site_id: Optional[str] = None,
                  employee_id: Optional[str] = None,
                  status: Optional[str] = None) -> List[Dict]:
        """
        Get shifts within date range
        
        Args:
            start_date: Start of date range
            end_date: End of date range
            site_id: Filter by site
            employee_id: Filter by employee
            status: Filter by status (PUBLISHED, DRAFT, etc.)
            
        Returns:
            List of shift records
        """
        params = {
            'filter[start][gte]': start_date.strftime('%Y-%m-%d'),
            'filter[end][lte]': end_date.strftime('%Y-%m-%d'),
            'include': 'employee,position,site'  # Include related data
        }
        
        if site_id:
            params['filter[site]'] = site_id
        if employee_id:
            params['filter[employee]'] = employee_id
        if status:
            params['filter[status]'] = status
        
        # Handle pagination
        all_shifts = []
        page = 1
        
        while True:
            params['page'] = page
            response = self._make_request('GET', '/v1/shifts', params=params)
            
            shifts = response.get('data', [])
            all_shifts.extend(shifts)
            
            # Check if there are more pages
            meta = response.get('meta', {})
            if page >= meta.get('last_page', 1):
                break
            
            page += 1
            
            # Brief pause to avoid rate limiting
            time.sleep(0.1)
        
        return all_shifts
    
    # === TIMESHEET ENDPOINTS ===
    
    def get_timesheets(self, start_date: datetime,
                      end_date: datetime,
                      status: Optional[str] = 'APPROVED') -> List[Dict]:
        """
        Get timesheets for payroll processing
        
        Args:
            start_date: Period start
            end_date: Period end
            status: Timesheet status filter
            
        Returns:
            List of timesheet records
        """
        params = {
            'filter[startedOn][gte]': start_date.strftime('%Y-%m-%d'),
            'filter[startedOn][lte]': end_date.strftime('%Y-%m-%d'),
            'include': 'employee,position,site,payCode'
        }
        
        if status:
            params['filter[status]'] = status
        
        return self._paginate_results('/v1/timesheets', params)
    
    # === SITE ENDPOINTS ===
    
    def get_sites(self, active_only: bool = True) -> List[Dict]:
        """Get all sites/locations"""
        params = {}
        if active_only:
            params['filter[status]'] = 'ACTIVE'
        
        return self._paginate_results('/v1/sites', params)
    
    def get_site_by_id(self, site_id: str) -> Dict:
        """Get specific site details"""
        return self._make_request('GET', f'/v1/sites/{site_id}')
    
    # === POSITION ENDPOINTS ===
    
    def get_positions(self, site_id: Optional[str] = None) -> List[Dict]:
        """Get positions (posts)"""
        params = {}
        if site_id:
            params['filter[site]'] = site_id
        
        return self._paginate_results('/v1/positions', params)
    
    # === HELPER METHODS ===
    
    def _paginate_results(self, endpoint: str, params: Dict) -> List[Dict]:
        """
        Handle pagination for endpoints that return multiple pages
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            All results from all pages
        """
        all_results = []
        page = 1
        params['perPage'] = 100  # Max allowed
        
        while True:
            params['page'] = page
            response = self._make_request('GET', endpoint, params=params)
            
            data = response.get('data', [])
            all_results.extend(data)
            
            meta = response.get('meta', {})
            if page >= meta.get('last_page', 1):
                break
            
            page += 1
            time.sleep(0.1)  # Rate limiting pause
        
        return all_results
    
    def export_to_dataframe(self, data: List[Dict], 
                           record_type: str = 'record') -> pd.DataFrame:
        """
        Convert API data to pandas DataFrame for analysis
        
        Args:
            data: List of records from API
            record_type: Type of record for logging
            
        Returns:
            DataFrame with flattened data
        """
        if not data:
            self.logger.warning(f"No {record_type} data to export")
            return pd.DataFrame()
        
        # Flatten nested structures
        flattened_data = []
        
        for record in data:
            flat_record = self._flatten_dict(record)
            flattened_data.append(flat_record)
        
        df = pd.DataFrame(flattened_data)
        self.logger.info(f"Exported {len(df)} {record_type} records to DataFrame")
        
        return df
    
    def _flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        """
        Flatten nested dictionary structures
        
        Args:
            d: Dictionary to flatten
            parent_key: Parent key for nested items
            sep: Separator for concatenated keys
            
        Returns:
            Flattened dictionary
        """
        items = []
        
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Convert lists to comma-separated strings
                if v and isinstance(v[0], dict):
                    # Complex list - just store count
                    items.append((f"{new_key}_count", len(v)))
                else:
                    items.append((new_key, ', '.join(str(x) for x in v)))
            else:
                items.append((new_key, v))
        
        return dict(items)
    
    def get_api_stats(self) -> Dict:
        """Get API usage statistics"""
        return {
            'total_calls': self.api_calls_made,
            'total_errors': self.api_errors,
            'error_rate': self.api_errors / self.api_calls_made if self.api_calls_made > 0 else 0,
            'rate_limit_remaining': self.rate_limit_remaining,
            'authenticated': self.access_token is not None
        }

# Example usage function
def demo_tracktik_integration():
    """
    Demonstration of TrackTik API integration
    """
    # Load credentials from environment or config file
    client_id = os.getenv('TRACKTIK_CLIENT_ID', 'your_client_id')
    client_secret = os.getenv('TRACKTIK_CLIENT_SECRET', 'your_client_secret')
    
    # Initialize client
    client = TrackTikAPIClient(client_id, client_secret)
    
    try:
        # Authenticate
        client.authenticate()
        print("✅ Successfully connected to TrackTik API")
        
        # Get employees
        print("\n📋 Fetching active employees...")
        employees = client.get_employees(active_only=True)
        print(f"Found {len(employees)} active employees")
        
        # Get this week's shifts
        print("\n📅 Fetching this week's shifts...")
        today = datetime.now()
        week_start = today - timedelta(days=today.weekday())
        week_end = week_start + timedelta(days=6)
        
        shifts = client.get_shifts(week_start, week_end)
        print(f"Found {len(shifts)} shifts this week")
        
        # Convert to DataFrame for analysis
        if shifts:
            shifts_df = client.export_to_dataframe(shifts, 'shift')
            print("\n📊 Shift Summary:")
            print(f"Total scheduled hours: {shifts_df.get('duration', pd.Series()).sum() / 3600:.2f}")
            
        # Get sites
        print("\n🏢 Fetching sites...")
        sites = client.get_sites()
        print(f"Found {len(sites)} active sites")
        
        # Show API stats
        stats = client.get_api_stats()
        print("\n📈 API Usage Statistics:")
        print(f"Total API calls: {stats['total_calls']}")
        print(f"Error rate: {stats['error_rate']:.2%}")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    demo_tracktik_integration()