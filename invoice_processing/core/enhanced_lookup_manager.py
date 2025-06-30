"""
Enhanced Lookup Manager with Master Validation Lookup Integration
Incorporates the billing manager's master lookup for AUS job number mapping
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple, List
import logging
from datetime import datetime


class EnhancedLookupManager:
    def __init__(self, 
                 emid_mapping_path: str = "emid_job_bu_table.xlsx",
                 master_lookup_path: str = "2025_Master Lookup_Validation Location with GL Reference_V3.xlsx"):
        """
        Initialize lookup manager with all reference data sources
        
        Args:
            emid_mapping_path: Path to EMID/Building reference file
            master_lookup_path: Path to master validation lookup file
        """
        self.logger = logging.getLogger(__name__)
        
        # Load all reference data
        self.load_emid_mapping(emid_mapping_path)
        self.load_master_lookup(master_lookup_path)
        
        # Create consolidated lookup structures
        self.create_consolidated_lookups()
        
        # Statistics
        self.lookup_stats = {
            'successful_lookups': 0,
            'failed_lookups': 0,
            'lookup_methods_used': {}
        }
    
    def load_emid_mapping(self, path: str):
        """Load EMID and building reference tables"""
        try:
            # Load EMID mapping
            self.emid_df = pd.read_excel(path, sheet_name='emid_job_code')
            
            # Check for duplicate job codes
            if self.emid_df['job_code'].duplicated().any():
                self.logger.warning("Duplicate job_codes found in EMID mapping. Keeping first occurrence.")
                self.emid_df = self.emid_df.drop_duplicates(subset=['job_code'], keep='first')
            
            # Create lookup with error handling
            try:
                self.emid_lookup = self.emid_df.set_index('job_code').to_dict('index')
            except Exception as e:
                self.logger.error(f"Error creating EMID lookup: {e}")
                # Fall back to a simple dictionary
                self.emid_lookup = {}
                for _, row in self.emid_df.iterrows():
                    job_code = row.get('job_code')
                    if job_code and job_code not in self.emid_lookup:
                        self.emid_lookup[job_code] = row.to_dict()
            
            # Load building mapping
            self.building_df = pd.read_excel(path, sheet_name='buildings')
            
            # Create multiple lookup strategies with error handling
            try:
                # Remove duplicates if any
                if self.building_df['building_code'].duplicated().any():
                    self.logger.warning("Duplicate building_codes found. Keeping first occurrence.")
                    building_df_unique = self.building_df.drop_duplicates(subset=['building_code'], keep='first')
                else:
                    building_df_unique = self.building_df
                
                self.building_by_code = building_df_unique.set_index('building_code').to_dict('index')
            except Exception as e:
                self.logger.error(f"Error creating building_by_code lookup: {e}")
                self.building_by_code = {}
            
            try:
                # For kp_loc_ref, convert to int and handle duplicates
                building_df_loc = self.building_df.copy()
                building_df_loc['kp_loc_ref'] = pd.to_numeric(building_df_loc['kp_loc_ref'], errors='coerce')
                building_df_loc = building_df_loc.dropna(subset=['kp_loc_ref'])
                building_df_loc['kp_loc_ref'] = building_df_loc['kp_loc_ref'].astype(int)
                
                if building_df_loc['kp_loc_ref'].duplicated().any():
                    self.logger.warning("Duplicate kp_loc_ref found. Keeping first occurrence.")
                    building_df_loc = building_df_loc.drop_duplicates(subset=['kp_loc_ref'], keep='first')
                
                self.building_by_loc_ref = building_df_loc.set_index('kp_loc_ref').to_dict('index')
            except Exception as e:
                self.logger.error(f"Error creating building_by_loc_ref lookup: {e}")
                self.building_by_loc_ref = {}
            
            self.logger.info(f"Loaded {len(self.emid_df)} EMID mappings and {len(self.building_df)} building mappings")
            
        except Exception as e:
            self.logger.error(f"Error loading EMID mapping: {e}")
            self.emid_lookup = {}
            self.building_by_code = {}
            self.building_by_loc_ref = {}
    
    def load_master_lookup(self, path: str):
        """Load master validation lookup from billing manager"""
        try:
            # Read the master lookup sheet
            self.master_lookup_df = pd.read_excel(path, sheet_name='Master Lookup', header=1)
            
            # Clean column names
            self.master_lookup_df.columns = [col.strip() for col in self.master_lookup_df.columns]
            
            # Key columns we need
            # Column F: "Location/Job No" - This is what appears in AUS files
            # Column X: "Tina- Building Code" - This is our target building code
            
            # Create lookup dictionary for AUS job numbers
            self.aus_job_lookup = {}
            
            for idx, row in self.master_lookup_df.iterrows():
                job_no = str(row.get('Location/Job No', '')).strip()
                building_code = str(row.get('Tina- Building Code', '')).strip()
                
                if job_no and building_code and job_no != 'nan':
                    # Store complete row information for comprehensive mapping
                    self.aus_job_lookup[job_no] = {
                        'building_code': building_code,
                        'emid': row.get('EMID'),
                        'mc_service_area': row.get('MC SERVICE AREA'),
                        'location_name': row.get('Location/Job Name'),
                        'gl_bu': row.get('GL BU'),
                        'loc': row.get('LOC'),
                        'dept': row.get('DEPT'),
                        'kp_building_code': row.get('KP Building Code - based on LOC (column J)'),
                        'address': row.get('Address on file'),
                        'building_match_status': row.get('Building Code Match')
                    }
            
            self.logger.info(f"Loaded {len(self.aus_job_lookup)} AUS job number mappings from master lookup")
            
            # Also load the buildings sheet if it exists
            try:
                self.master_buildings_df = pd.read_excel(path, sheet_name='Buildings')
                self.logger.info(f"Loaded {len(self.master_buildings_df)} records from Buildings sheet")
            except:
                self.master_buildings_df = pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error loading master lookup: {e}")
            self.aus_job_lookup = {}
    
    def create_consolidated_lookups(self):
        """Create consolidated lookup structures for efficient access"""
        
        # Consolidated building lookup by multiple keys
        self.consolidated_building_lookup = {}
        
        # Add buildings from original building table
        for _, row in self.building_df.iterrows():
            building_code = row['building_code']
            self.consolidated_building_lookup[building_code] = {
                'source': 'emid_job_bu_table',
                'emid': row['emid'],
                'mc_service_area': row['mc_service_area'],
                'business_unit': row['business_unit'],
                'kp_loc_ref': row.get('kp_loc_ref')
            }
        
        # Enhance with master lookup data
        for job_no, data in self.aus_job_lookup.items():
            building_code = data['building_code']
            if building_code:
                # If building code exists, enhance it; otherwise add it
                if building_code not in self.consolidated_building_lookup:
                    self.consolidated_building_lookup[building_code] = {}
                
                self.consolidated_building_lookup[building_code].update({
                    'source': 'master_lookup',
                    'job_numbers': self.consolidated_building_lookup[building_code].get('job_numbers', []) + [job_no],
                    'emid': data['emid'] or self.consolidated_building_lookup[building_code].get('emid'),
                    'mc_service_area': data['mc_service_area'] or self.consolidated_building_lookup[building_code].get('mc_service_area'),
                    'gl_bu': data['gl_bu'],
                    'loc': data['loc']
                })
        
        self.logger.info(f"Created consolidated lookup with {len(self.consolidated_building_lookup)} building codes")
    
    def lookup_aus_job_info(self, job_number: str) -> Dict:
        """
        Enhanced lookup for AUS job numbers using master lookup
        
        Args:
            job_number: AUS job number from invoice file
            
        Returns:
            Dictionary with building code and related information
        """
        job_str = str(job_number).strip()
        
        # Direct lookup
        if job_str in self.aus_job_lookup:
            self.lookup_stats['successful_lookups'] += 1
            self.lookup_stats['lookup_methods_used']['direct_match'] = \
                self.lookup_stats['lookup_methods_used'].get('direct_match', 0) + 1
            return self.aus_job_lookup[job_str].copy()
        
        # Try removing common suffixes (T for temporary, etc.)
        if job_str.endswith('T'):
            base_job = job_str[:-1]
            if base_job in self.aus_job_lookup:
                self.lookup_stats['successful_lookups'] += 1
                self.lookup_stats['lookup_methods_used']['suffix_removal'] = \
                    self.lookup_stats['lookup_methods_used'].get('suffix_removal', 0) + 1
                return self.aus_job_lookup[base_job].copy()
        
        # Try partial match for job numbers that might have prefixes
        for key, value in self.aus_job_lookup.items():
            if job_str in key or key in job_str:
                self.lookup_stats['successful_lookups'] += 1
                self.lookup_stats['lookup_methods_used']['partial_match'] = \
                    self.lookup_stats['lookup_methods_used'].get('partial_match', 0) + 1
                return value.copy()
        
        self.lookup_stats['failed_lookups'] += 1
        return {}
    
    def lookup_bci_location_info(self, location_number: str, location_name: str = None) -> Dict:
        """
        Enhanced lookup for BCI location information
        
        Args:
            location_number: BCI location number
            location_name: Optional location name for fallback matching
            
        Returns:
            Dictionary with building code and related information
        """
        # Try direct building code lookup
        if location_number in self.consolidated_building_lookup:
            return self.consolidated_building_lookup[location_number].copy()
        
        # Try kp_loc_ref lookup
        try:
            loc_ref_int = int(location_number)
            if loc_ref_int in self.building_by_loc_ref:
                building_info = self.building_by_loc_ref[loc_ref_int]
                building_code = building_info.get('building_code')
                if building_code in self.consolidated_building_lookup:
                    return self.consolidated_building_lookup[building_code].copy()
        except:
            pass
        
        # Try location name matching if provided
        if location_name:
            for _, row in self.master_lookup_df.iterrows():
                if pd.notna(row.get('Location/Job Name')) and location_name in str(row['Location/Job Name']):
                    building_code = row.get('Tina- Building Code')
                    if building_code and building_code in self.consolidated_building_lookup:
                        return self.consolidated_building_lookup[building_code].copy()
        
        return {}
    
    def get_complete_dimensions(self, building_code: str) -> Dict:
        """
        Get complete dimensional information for a building code
        
        Args:
            building_code: Building code to look up
            
        Returns:
            Dictionary with all dimensional attributes
        """
        if building_code not in self.consolidated_building_lookup:
            return {}
        
        base_info = self.consolidated_building_lookup[building_code].copy()
        
        # Enhance with EMID information if available
        if base_info.get('emid') and base_info['emid'] in self.emid_lookup:
            emid_info = self.emid_lookup[base_info['emid']]
            base_info.update({
                'region': emid_info.get('region'),
                'job_code': emid_info.get('job_code'),
                'description': emid_info.get('description')
            })
        
        return base_info
    
    def export_dimension_tables(self, output_path: str = "consolidated_dimensions.xlsx"):
        """
        Export consolidated dimension tables for use in reporting
        """
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Building dimension table
            building_dim = []
            for building_code, info in self.consolidated_building_lookup.items():
                row = {
                    'building_code': building_code,
                    'emid': info.get('emid'),
                    'mc_service_area': info.get('mc_service_area'),
                    'business_unit': info.get('business_unit'),
                    'gl_bu': info.get('gl_bu'),
                    'loc': info.get('loc'),
                    'source': info.get('source'),
                    'job_numbers': ','.join(info.get('job_numbers', []))
                }
                building_dim.append(row)
            
            building_df = pd.DataFrame(building_dim)
            building_df.to_excel(writer, sheet_name='Building_Dimension', index=False)
            
            # AUS Job mapping table
            aus_mapping = []
            for job_no, info in self.aus_job_lookup.items():
                row = {
                    'job_number': job_no,
                    'building_code': info['building_code'],
                    'location_name': info.get('location_name'),
                    'emid': info.get('emid'),
                    'mc_service_area': info.get('mc_service_area')
                }
                aus_mapping.append(row)
            
            aus_df = pd.DataFrame(aus_mapping)
            aus_df.to_excel(writer, sheet_name='AUS_Job_Mapping', index=False)
            
            # EMID dimension table
            self.emid_df.to_excel(writer, sheet_name='EMID_Dimension', index=False)
            
            # Lookup statistics
            stats_df = pd.DataFrame([
                {'Metric': 'Total Building Codes', 'Value': len(self.consolidated_building_lookup)},
                {'Metric': 'Total AUS Job Mappings', 'Value': len(self.aus_job_lookup)},
                {'Metric': 'Total EMID Codes', 'Value': len(self.emid_df)},
                {'Metric': 'Successful Lookups', 'Value': self.lookup_stats['successful_lookups']},
                {'Metric': 'Failed Lookups', 'Value': self.lookup_stats['failed_lookups']}
            ])
            stats_df.to_excel(writer, sheet_name='Statistics', index=False)
        
        self.logger.info(f"Exported consolidated dimensions to {output_path}")
    
    def validate_mappings(self) -> Dict[str, List]:
        """
        Validate the completeness and consistency of mappings
        
        Returns:
            Dictionary of validation issues
        """
        issues = {
            'missing_emid': [],
            'missing_building_code': [],
            'inconsistent_mappings': [],
            'unmapped_jobs': []
        }
        
        # Check for AUS jobs without building codes
        for job_no, info in self.aus_job_lookup.items():
            if not info.get('building_code'):
                issues['missing_building_code'].append(job_no)
            if not info.get('emid'):
                issues['missing_emid'].append(job_no)
        
        # Check for building codes without complete information
        for building_code, info in self.consolidated_building_lookup.items():
            if not info.get('emid'):
                issues['missing_emid'].append(f"Building: {building_code}")
        
        return issues

# Example usage
if __name__ == "__main__":
    # Initialize enhanced lookup manager
    lookup_manager = EnhancedLookupManager()
    
    # Test AUS job lookup
    test_jobs = ['207168', '281084T', '207169']
    print("Testing AUS job lookups:")
    for job in test_jobs:
        result = lookup_manager.lookup_aus_job_info(job)
        if result:
            print(f"  {job} -> Building: {result.get('building_code')}, "
                  f"EMID: {result.get('emid')}, Area: {result.get('mc_service_area')}")
        else:
            print(f"  {job} -> Not found")
    
    # Export consolidated dimensions
    lookup_manager.export_dimension_tables()
    
    # Validate mappings
    issues = lookup_manager.validate_mappings()
    print("\nValidation Results:")
    for issue_type, items in issues.items():
        if items:
            print(f"  {issue_type}: {len(items)} issues found")
