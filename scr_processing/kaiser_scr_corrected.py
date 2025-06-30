#!/usr/bin/env python3
"""
Kaiser SCR Parser - Corrected Version
Fixes:
1. Uses Region field directly (no location calculation)
2. Removes QB job code, invoice category, and system_position_id
3. Fixes case-sensitive matching for position codes
4. Builds position_code_full as: building_code_assignment_code-post_code
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import re


def parse_kaiser_scr(scr_file: str, codes_file: str = None) -> dict:
    """
    Parse Kaiser SCR file and return clean tables
    
    Args:
        scr_file: Path to Kaiser SCR Excel file
        codes_file: Optional path to position codes mapping CSV
    
    Returns:
        Dictionary containing parsed dataframes
    """
    print(f"Parsing SCR file: {scr_file}")
    
    # Read the SCR file
    df = pd.read_excel(scr_file)
    print(f"Loaded {len(df)} rows from SCR file")
    
    # Convert Post ID to integer to remove .0
    df['Post ID'] = df['Post ID'].fillna(0).astype(int).astype(str)
    
    # 1. Extract Buildings
    buildings = extract_buildings(df)
    
    # 2. Extract Posts (now includes Region)
    posts = extract_posts(df)
    
    # 3. Extract Schedules with parsed shift times
    schedules = extract_schedules(df)
    
    # 4. Add position codes if mapping file provided
    if codes_file:
        posts = add_position_codes(posts, codes_file)
    
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
    
    # Rename columns for consistency
    buildings.columns = [
        'building_code', 'schedule_name', 'building_name',
        'building_address', 'service_area', 'region', 'zip_code'
    ]
    
    print(f"Found {len(buildings)} unique buildings")
    return buildings


def extract_posts(df: pd.DataFrame) -> pd.DataFrame:
    """Extract all posts with their attributes including Region"""
    posts = df[[
        'Building Code', 'Post ID', 'Post Assignment', 
        'Post', 'Remarks', 'KP - Total Hours', 'Region', 'Service Area'
    ]].copy()
    
    # Rename columns
    posts.columns = [
        'building_code', 'post_id', 'post_assignment',
        'post_type', 'remarks', 'total_weekly_hours', 'region', 'service_area'
    ]
    
    # Create unique post key (without decimals)
    posts['post_key'] = posts['building_code'] + '_' + posts['post_id']
    
    # Clean up post types
    posts['post_type'] = posts['post_type'].fillna('Unknown')
    
    # Reorder columns to put region next to service_area
    column_order = [
        'post_key', 'building_code', 'post_id', 'post_assignment',
        'post_type', 'remarks', 'total_weekly_hours', 'region', 'service_area'
    ]
    posts = posts[column_order]
    
    print(f"Found {len(posts)} posts")
    return posts


def extract_schedules(df: pd.DataFrame) -> pd.DataFrame:
    """Extract and parse all schedules"""
    schedules = []
    
    # Define day columns
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
        # Use string formatting to avoid decimals
        post_id_str = str(int(row['Post ID']) if pd.notna(row['Post ID']) else 0)
        post_key = f"{row['Building Code']}_{post_id_str}"
        
        for day, cov_col, hours_col in day_columns:
            # Check if hours exist and are greater than 0
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
    # Pattern 1: "00:00 - 24:00" (what Kaiser uses)
    # Pattern 2: "00:00 - 00:00" with 24 hours
    if ('00:00 - 24:00' in coverage_str or 
        ('00:00 - 00:00' in coverage_str and hours == 24)):
        result['is_24_hour'] = True
        result['start'] = '00:00'
        result['end'] = '24:00'  # Keep as 24:00 to indicate full day
        result['shift_count'] = 1
        return result
    
    # Extract time patterns
    time_pattern = r'(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})'
    matches = re.findall(time_pattern, coverage_str)
    
    if matches:
        result['shift_count'] = len(matches)
        # Take first shift for now
        start_h, start_m, end_h, end_m = matches[0]
        result['start'] = f"{int(start_h):02d}:{start_m}"
        result['end'] = f"{int(end_h):02d}:{end_m}"
        
        # Check if this might be 24-hour based on hours
        if hours == 24 and result['start'] == '00:00':
            result['is_24_hour'] = True
    
    return result


def add_position_codes(posts_df: pd.DataFrame, codes_file: str) -> pd.DataFrame:
    """
    Add position codes from mapping file
    Handles the paired column structure where assignments and types are mapped separately
    Creates position_code_full as: building_code_assignment_code-post_code
    """
    try:
        codes_df = pd.read_csv(codes_file)
        print(f"Loaded codes file with {len(codes_df)} rows")
        
        # Create separate lookups for Post Assignment and Post Type
        # Post Assignment lookup (columns 3-4)
        assignment_lookup = {}
        for _, row in codes_df.iterrows():
            if pd.notna(row['Post Assignment']) and pd.notna(row['Post Assignment Code']):
                assignment_lookup[row['Post Assignment'].upper()] = row['Post Assignment Code']
        
        # Post Type lookup (columns 5-6)
        post_type_lookup = {}
        for _, row in codes_df.iterrows():
            if pd.notna(row['Post']) and pd.notna(row['Post Code']):
                post_type_lookup[row['Post'].upper()] = row['Post Code']
        
        print(f"Created lookups - Assignments: {len(assignment_lookup)}, Types: {len(post_type_lookup)}")
        
        # Create uppercase columns for lookup
        posts_df['post_assignment_upper'] = posts_df['post_assignment'].str.upper()
        posts_df['post_type_upper'] = posts_df['post_type'].str.upper()
        
        # Apply lookups independently
        posts_df['post_assignment_code'] = posts_df['post_assignment_upper'].map(assignment_lookup)
        posts_df['post_code'] = posts_df['post_type_upper'].map(post_type_lookup)
        
        # Create position_code_full: building_code_assignment_code-post_code
        posts_df['position_code_full'] = posts_df.apply(
            lambda row: f"{row['building_code']}_{row['post_assignment_code']}-{row['post_code']}" 
            if pd.notna(row['post_assignment_code']) and pd.notna(row['post_code']) 
            else None,
            axis=1
        )
        
        # Clean up temporary columns
        posts_df = posts_df.drop(columns=['post_assignment_upper', 'post_type_upper'])
        
        # Calculate match statistics
        total_posts = len(posts_df)
        has_assignment_code = posts_df['post_assignment_code'].notna().sum()
        has_post_code = posts_df['post_code'].notna().sum()
        has_full_code = posts_df['position_code_full'].notna().sum()
        
        print(f"\nMapping Statistics:")
        print(f"  Posts with assignment code: {has_assignment_code} ({has_assignment_code/total_posts*100:.1f}%)")
        print(f"  Posts with post type code: {has_post_code} ({has_post_code/total_posts*100:.1f}%)")
        print(f"  Posts with complete code: {has_full_code} ({has_full_code/total_posts*100:.1f}%)")
        
        # Show unmatched values for debugging
        if has_full_code < total_posts:
            print("\nDebugging unmatched values:")
            
            # Missing assignment codes
            missing_assignments = posts_df[posts_df['post_assignment_code'].isna()]['post_assignment'].unique()
            if len(missing_assignments) > 0:
                print(f"\nPost Assignments not found ({len(missing_assignments)}):")
                for assignment in missing_assignments[:5]:
                    print(f"  '{assignment}'")
                if len(missing_assignments) > 5:
                    print(f"  ... and {len(missing_assignments) - 5} more")
            
            # Missing post type codes
            missing_types = posts_df[posts_df['post_code'].isna()]['post_type'].unique()
            if len(missing_types) > 0:
                print(f"\nPost Types not found ({len(missing_types)}):")
                for post_type in missing_types[:5]:
                    print(f"  '{post_type}'")
                if len(missing_types) > 5:
                    print(f"  ... and {len(missing_types) - 5} more")
        
        return posts_df
        
    except Exception as e:
        print(f"Error loading position codes: {e}")
        import traceback
        traceback.print_exc()
        posts_df['post_assignment_code'] = None
        posts_df['post_code'] = None
        posts_df['position_code_full'] = None
        return posts_df


def create_summary(buildings: pd.DataFrame, posts: pd.DataFrame, 
                   schedules: pd.DataFrame) -> pd.DataFrame:
    """Create summary statistics - using schedules as authoritative source"""
    
    # First, deduplicate posts to get accurate counts
    posts_unique = posts.drop_duplicates(subset=['building_code', 'post_id'])
    
    # Calculate total hours from schedules (ground truth)
    hours_by_building = schedules.groupby('building_code')['hours'].sum().reset_index()
    hours_by_building.columns = ['building_code', 'total_weekly_hours']
    
    # Calculate unique post counts
    post_counts = posts_unique.groupby('building_code')['post_id'].count().reset_index()
    post_counts.columns = ['building_code', 'total_posts']
    
    # Count 24x7 posts (posts with 24-hour coverage all 7 days)
    if len(schedules) > 0:
        # Group by building and post, count 24-hour days
        posts_24hr_days = schedules[schedules['is_24_hour']].groupby(['building_code', 'post_id']).size().reset_index(name='days_24hr')
        # Count posts with 7 days of 24-hour coverage
        posts_24x7_count = posts_24hr_days[posts_24hr_days['days_24hr'] == 7].groupby('building_code').size().reset_index(name='posts_with_24x7')
    else:
        posts_24x7_count = pd.DataFrame(columns=['building_code', 'posts_with_24x7'])
    
    # Start with buildings to ensure all are included
    summary = buildings[['building_code', 'building_name', 'service_area']].drop_duplicates()
    
    # Merge the calculated values
    summary = summary.merge(hours_by_building, on='building_code', how='left')
    summary = summary.merge(post_counts, on='building_code', how='left')
    summary = summary.merge(posts_24x7_count, on='building_code', how='left')
    
    # Fill NaN values
    summary['total_weekly_hours'] = summary['total_weekly_hours'].fillna(0)
    summary['total_posts'] = summary['total_posts'].fillna(0).astype(int)
    summary['posts_with_24x7'] = summary['posts_with_24x7'].fillna(0).astype(int)
    
    # Calculate average hours per post
    summary['avg_hours_per_post'] = summary.apply(
        lambda row: row['total_weekly_hours'] / row['total_posts'] if row['total_posts'] > 0 else 0,
        axis=1
    )
    
    # Verify no duplicates
    if summary.duplicated(subset=['building_code']).any():
        print("WARNING: Duplicate buildings found in summary!")
        duplicates = summary[summary.duplicated(subset=['building_code'], keep=False)]
        print(duplicates[['building_code', 'building_name']])
    
    # Print verification
    schedule_total = schedules['hours'].sum()
    summary_total = summary['total_weekly_hours'].sum()
    print(f"\nSummary verification:")
    print(f"  Schedule total hours: {schedule_total:,.1f}")
    print(f"  Summary total hours: {summary_total:,.1f}")
    print(f"  Difference: {abs(schedule_total - summary_total):,.1f}")
    
    return summary


def export_to_excel(results: dict, output_file: str = 'kaiser_scr_parsed.xlsx'):
    """Export all results to Excel with multiple sheets"""
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for sheet_name, df in results.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    print(f"\nExported results to: {output_file}")


def create_billing_match_file(posts_df: pd.DataFrame, output_file: str = 'billing_match.csv'):
    """Create a file specifically for billing system matching"""
    match_df = posts_df[[
        'post_key', 'building_code', 'post_id', 
        'post_assignment', 'post_type', 
        'post_assignment_code', 'post_code', 'position_code_full',
        'total_weekly_hours', 'region', 'service_area'
    ]].copy()
    
    match_df.to_csv(output_file, index=False)
    print(f"Created billing match file: {output_file}")
    
    return match_df


# Main execution
if __name__ == "__main__":
    # Parse the SCR file
    results = parse_kaiser_scr(
        scr_file='kaiser_scr_master.xlsx',
        codes_file='scr_codes_table.csv'  # Optional
    )
    
    # Display summary
    print("\n=== Summary ===")
    print(results['summary'].head(10))
    
    # Export to Excel
    export_to_excel(results)
    
    # Create billing match file
    if 'posts' in results:
        billing_match = create_billing_match_file(results['posts'])
        
        # Show sample matching keys
        print("\n=== Sample Position Codes ===")
        sample_cols = ['post_key', 'position_code_full', 'post_assignment', 'post_type']
        print(billing_match[sample_cols].head(10))
    
    # Quick analysis
    print("\n=== Quick Analysis ===")
    schedules = results['schedules']
    print(f"Total coverage hours per week: {schedules['hours'].sum():,.0f}")
    print(f"Total 24-hour coverage entries: {schedules[schedules['is_24_hour']].shape[0]}")
    print(f"Posts with 24-hour coverage: {schedules[schedules['is_24_hour']]['post_id'].nunique()}")
    print(f"Average hours per post per day: {schedules.groupby('post_key')['hours'].mean().mean():.1f}")
