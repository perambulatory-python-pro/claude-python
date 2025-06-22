#!/usr/bin/env python3
"""
Kaiser SCR Parser - Final Version
Complete position ID construction following your format:
LOCATION_QB_JOB_CODE_INVOICE_CATEGORY_BUILDING_CODE_POSITION_CODE
Example: FRESNO_KP03_000_CN72-01_SO-PO
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import re

# ===== CONFIGURATION SECTION =====
# Customize these mappings based on your business rules

# Location mapping based on Service Area
LOCATION_MAPPING = {
    'Denver/Boulder': 'DENVER',
    'Regional - CO': 'DENVER',
    'Southern Colorado': 'COLORADO_SPRINGS',
    'Northern Colorado': 'FORT_COLLINS',
    # Add more service area to location mappings here
}

# Invoice Category Codes
INVOICE_CATEGORIES = {
    'Recurring Services': '000',
    'Non-Recurring Services': '001',
    'Special Coverage': '002',
    # Add more categories as needed
}

# Default invoice category for regular posts
DEFAULT_INVOICE_CATEGORY = '000'  # Recurring Services

# Rules for determining invoice category based on post attributes
def determine_invoice_category(row):
    """
    Determine invoice category based on post attributes
    Customize this logic based on your business rules
    """
    # Example logic - adjust as needed:
    if pd.notna(row.get('remarks')):
        remarks_lower = str(row['remarks']).lower()
        if 'special' in remarks_lower or 'event' in remarks_lower:
            return '002'  # Special Coverage
        elif 'temp' in remarks_lower or 'one-time' in remarks_lower:
            return '001'  # Non-Recurring
    
    # Check if it's a regular patrol post
    if row.get('post_type') == 'Patrolling':
        return '000'  # Recurring Services
    
    return DEFAULT_INVOICE_CATEGORY

# ===== END CONFIGURATION =====


def parse_kaiser_scr(scr_file: str, codes_file: str = None) -> dict:
    """
    Parse Kaiser SCR file and return clean tables with complete position IDs
    """
    print(f"Parsing SCR file: {scr_file}")
    
    # Read the SCR file
    df = pd.read_excel(scr_file)
    print(f"Loaded {len(df)} rows from SCR file")
    
    # Convert Post ID to integer to remove .0
    df['Post ID'] = df['Post ID'].fillna(0).astype(int).astype(str)
    
    # Add Service Area to posts for location mapping
    df['Service Area'] = df['Service Area'].fillna('')
    
    # 1. Extract Buildings
    buildings = extract_buildings(df)
    
    # 2. Extract Posts with service area
    posts = extract_posts_with_location(df)
    
    # 3. Extract Schedules with parsed shift times
    schedules = extract_schedules(df)
    
    # 4. Add position codes and build complete IDs
    if codes_file:
        posts = add_position_codes_and_build_ids(posts, codes_file)
    
    # 5. Create summary statistics
    summary = create_summary(buildings, posts, schedules)
    
    return {
        'buildings': buildings,
        'posts': posts,
        'schedules': schedules,
        'summary': summary
    }


def extract_buildings(df: pd.DataFrame) -> pd.DataFrame:
    """Extract unique buildings"""
    buildings = df[[
        'Building Code', 'Schedule Name', 'Building Name', 
        'Building Address', 'Service Area', 'Region', 'Zip Code'
    ]].drop_duplicates()
    
    buildings.columns = [
        'building_code', 'schedule_name', 'building_name',
        'building_address', 'service_area', 'region', 'zip_code'
    ]
    
    print(f"Found {len(buildings)} unique buildings")
    return buildings


def extract_posts_with_location(df: pd.DataFrame) -> pd.DataFrame:
    """Extract all posts with their attributes including service area"""
    posts = df[[
        'Building Code', 'Post ID', 'Post Assignment', 
        'Post', 'Remarks', 'KP - Total Hours', 'Service Area'
    ]].copy()
    
    posts.columns = [
        'building_code', 'post_id', 'post_assignment',
        'post_type', 'remarks', 'total_weekly_hours', 'service_area'
    ]
    
    # Create unique post key (without decimals)
    posts['post_key'] = posts['building_code'] + '_' + posts['post_id']
    
    # Clean up post types
    posts['post_type'] = posts['post_type'].fillna('Unknown')
    
    print(f"Found {len(posts)} posts")
    return posts


def extract_schedules(df: pd.DataFrame) -> pd.DataFrame:
    """Extract and parse all schedules"""
    schedules = []
    
    day_columns = [
        ('FRI', 'KP - FRI COV', 'KP - FRI Hours'),
        ('SAT', 'KP - SAT COV', 'KP - SAT Hours'),
        ('SUN', 'KP - SUN COV', 'KP - SUN Hours'),
        ('MON', 'KP - MON COV', 'KP - MON Hours'),
        ('TUE', 'KP - TUE COV', 'KP - TUE Hours'),
        ('WED', 'KP - WED COV', 'KP - WED Hours'),
        ('THU', 'KP - THU COV', 'KP - THU Hours')
    ]
    
    for idx, row in df.iterrows():
        post_id_str = str(int(row['Post ID']) if pd.notna(row['Post ID']) else 0)
        post_key = f"{row['Building Code']}_{post_id_str}"
        
        for day, cov_col, hours_col in day_columns:
            hours_value = row[hours_col]
            if pd.notna(hours_value) and hours_value > 0:
                shift_info = parse_shift_time(row[cov_col], hours_value)
                
                schedule = {
                    'post_key': post_key,
                    'building_code': row['Building Code'],
                    'post_id': post_id_str,
                    'day_of_week': day,
                    'coverage_raw': row[cov_col],
                    'hours': hours_value,
                    'shift_start': shift_info['start'],
                    'shift_end': shift_info['end'],
                    'is_24_hour': shift_info['is_24_hour'],
                    'shift_count': shift_info['shift_count']
                }
                schedules.append(schedule)
    
    schedules_df = pd.DataFrame(schedules)
    print(f"Created {len(schedules_df)} schedule entries")
    return schedules_df


def parse_shift_time(coverage_text, hours=None) -> dict:
    """Parse shift coverage text to extract times"""
    result = {
        'start': None,
        'end': None,
        'is_24_hour': False,
        'shift_count': 0
    }
    
    if pd.isna(coverage_text) or str(coverage_text) == '0':
        return result
    
    coverage_str = str(coverage_text).strip()
    
    # Check for 24-hour coverage patterns
    if ('00:00 - 24:00' in coverage_str or 
        ('00:00 - 00:00' in coverage_str and hours == 24)):
        result['is_24_hour'] = True
        result['start'] = '00:00'
        result['end'] = '24:00'
        result['shift_count'] = 1
        return result
    
    # Extract time patterns
    time_pattern = r'(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})'
    matches = re.findall(time_pattern, coverage_str)
    
    if matches:
        result['shift_count'] = len(matches)
        start_h, start_m, end_h, end_m = matches[0]
        result['start'] = f"{int(start_h):02d}:{start_m}"
        result['end'] = f"{int(end_h):02d}:{end_m}"
        
        if hours == 24 and result['start'] == '00:00':
            result['is_24_hour'] = True
    
    return result


def add_position_codes_and_build_ids(posts_df: pd.DataFrame, codes_file: str) -> pd.DataFrame:
    """
    Add position codes and build complete system position IDs
    Format: LOCATION_QB_JOB_CODE_INVOICE_CATEGORY_BUILDING_CODE_POSITION_CODE
    """
    try:
        codes_df = pd.read_csv(codes_file)
        print(f"Loaded {len(codes_df)} position code mappings")
        
        # Create the full position code (Post Assignment Code + "-" + Post Code)
        codes_df['full_position_code'] = (
            codes_df['Post Assignment Code'].astype(str) + '-' + 
            codes_df['Post Code'].astype(str)
        )
        
        # Merge with posts
        posts_with_codes = pd.merge(
            posts_df,
            codes_df[['Post Assignment', 'Post', 'postPositionCode', 
                     'Post Assignment Code', 'Post Code', 'full_position_code']],
            left_on=['post_assignment', 'post_type'],
            right_on=['Post Assignment', 'Post'],
            how='left'
        )
        
        # Map location from service area
        posts_with_codes['location'] = posts_with_codes['service_area'].map(LOCATION_MAPPING)
        posts_with_codes['location'] = posts_with_codes['location'].fillna('COLORADO')
        
        # QB Job Code is the postPositionCode from your mapping
        posts_with_codes['qb_job_code'] = posts_with_codes['postPositionCode']
        
        # Determine invoice category
        posts_with_codes['invoice_category'] = posts_with_codes.apply(determine_invoice_category, axis=1)
        
        # Position codes for the ID
        posts_with_codes['position_code_full'] = posts_with_codes['full_position_code']
        posts_with_codes['post_assignment_code'] = posts_with_codes['Post Assignment Code']
        posts_with_codes['post_code'] = posts_with_codes['Post Code']
        
        # Build the complete system position ID
        posts_with_codes['system_position_id'] = posts_with_codes.apply(
            lambda row: build_system_position_id(row), axis=1
        )
        
        # Clean up columns
        posts_with_codes = posts_with_codes.drop(columns=[
            'Post Assignment', 'Post', 'postPositionCode', 
            'Post Assignment Code', 'Post Code', 'full_position_code'
        ])
        
        # Calculate match rate
        matched = posts_with_codes['qb_job_code'].notna().sum()
        match_rate = matched / len(posts_with_codes) * 100
        print(f"Position code match rate: {match_rate:.1f}% ({matched}/{len(posts_with_codes)})")
        
        # Show location distribution
        print(f"\nLocation distribution:")
        print(posts_with_codes['location'].value_counts())
        
        return posts_with_codes
        
    except Exception as e:
        print(f"Error loading position codes: {e}")
        posts_df['qb_job_code'] = None
        posts_df['position_code_full'] = None
        posts_df['system_position_id'] = None
        return posts_df


def build_system_position_id(row):
    """Build the complete system position ID"""
    # Handle missing values
    location = row['location'] if pd.notna(row['location']) else 'UNKNOWN'
    qb_code = row['qb_job_code'] if pd.notna(row['qb_job_code']) else 'XX00'
    invoice_cat = row['invoice_category'] if pd.notna(row['invoice_category']) else '000'
    building = row['building_code'] if pd.notna(row['building_code']) else 'XX00-0'
    pos_code = row['position_code_full'] if pd.notna(row['position_code_full']) else 'XX-XX'
    
    return f"{location}_{qb_code}_{invoice_cat}_{building}_{pos_code}"


def create_summary(buildings: pd.DataFrame, posts: pd.DataFrame, 
                   schedules: pd.DataFrame) -> pd.DataFrame:
    """Create summary statistics"""
    summary_data = []
    
    for _, building in buildings.iterrows():
        building_code = building['building_code']
        
        building_posts = posts[posts['building_code'] == building_code]
        building_schedules = schedules[schedules['building_code'] == building_code]
        
        total_posts = len(building_posts)
        total_weekly_hours = building_posts['total_weekly_hours'].sum()
        
        # Count 24x7 posts
        if len(building_schedules) > 0:
            posts_24x7 = building_schedules[building_schedules['is_24_hour']].groupby('post_id').size()
            posts_with_24x7 = (posts_24x7 == 7).sum()
        else:
            posts_with_24x7 = 0
        
        summary_data.append({
            'building_code': building_code,
            'building_name': building['building_name'],
            'service_area': building['service_area'],
            'total_posts': total_posts,
            'total_weekly_hours': total_weekly_hours,
            'avg_hours_per_post': total_weekly_hours / total_posts if total_posts > 0 else 0,
            'posts_with_24x7': posts_with_24x7
        })
    
    return pd.DataFrame(summary_data)


def export_to_excel(results: dict, output_file: str = 'kaiser_scr_parsed.xlsx'):
    """Export all results to Excel with multiple sheets"""
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for sheet_name, df in results.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    print(f"\nExported results to: {output_file}")


def create_billing_match_file(posts_df: pd.DataFrame, output_file: str = 'billing_match.csv'):
    """Create a file specifically for billing system matching"""
    # Select key columns for matching
    columns = [
        'post_key', 'building_code', 'post_id', 
        'post_assignment', 'post_type', 'service_area',
        'location', 'qb_job_code', 'invoice_category',
        'position_code_full', 'system_position_id',
        'total_weekly_hours'
    ]
    
    # Only include columns that exist
    available_cols = [col for col in columns if col in posts_df.columns]
    match_df = posts_df[available_cols].copy()
    
    match_df.to_csv(output_file, index=False)
    print(f"Created billing match file: {output_file}")
    
    return match_df


# Main execution
if __name__ == "__main__":
    # Parse the SCR file
    results = parse_kaiser_scr(
        scr_file='kaiser_scr_master.xlsx',
        codes_file='scr_codes_table.csv'
    )
    
    # Display summary
    print("\n=== Summary ===")
    print(results['summary'].head(10))
    
    # Export to Excel
    export_to_excel(results)
    
    # Create billing match file
    if 'posts' in results:
        billing_match = create_billing_match_file(results['posts'])
        
        # Show sample system position IDs
        print("\n=== Sample System Position IDs ===")
        if 'system_position_id' in billing_match.columns:
            sample = billing_match[['post_key', 'system_position_id', 'post_assignment']].head(10)
            print(sample.to_string(index=False))
    
    # Quick analysis
    print("\n=== Quick Analysis ===")
    schedules = results['schedules']
    posts = results['posts']
    
    print(f"Total coverage hours per week: {schedules['hours'].sum():,.0f}")
    print(f"Total 24-hour coverage entries: {schedules[schedules['is_24_hour']].shape[0]}")
    print(f"Posts with 24-hour coverage: {schedules[schedules['is_24_hour']]['post_id'].nunique()}")
    
    # Show invoice category distribution
    if 'invoice_category' in posts.columns:
        print("\n=== Invoice Category Distribution ===")
        category_names = {v: k for k, v in INVOICE_CATEGORIES.items()}
        category_counts = posts['invoice_category'].value_counts()
        for cat, count in category_counts.items():
            cat_name = category_names.get(cat, f"Category {cat}")
            print(f"{cat_name} ({cat}): {count} posts")
