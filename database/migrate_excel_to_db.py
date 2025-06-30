"""
Data Migration Script - Excel to Database
Migrate your existing Excel files to the database system

Run with: python migrate_excel_to_db.py

Key Python Concepts:
- Command line arguments: Using argparse for user options
- File globbing: Finding files with patterns (*.xlsx)
- Progress tracking: Visual feedback during long operations
- Batch processing: Handling large datasets efficiently
"""

import os
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import logging

# Import our database components
from database_manager import DatabaseManager
from data_mapper import DataMapper

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataMigrator:
    """
    Handles migration of Excel data to database
    """
    
    def __init__(self):
        """Initialize database connection and mapper"""
        load_dotenv()
        self.db = DatabaseManager()
        self.mapper = DataMapper()
        
        # Test database connection
        if not self.db.test_connection():
            raise ConnectionError("Database connection failed")
        
        logger.info("Migration system initialized successfully")
    
    def migrate_master_excel(self, file_path: str) -> dict:
        """
        Migrate master invoice Excel file to database
        
        Args:
            file_path: Path to the master Excel file
            
        Returns:
            Dictionary with migration results
        """
        logger.info(f"Starting migration of master file: {file_path}")
        
        try:
            # Read Excel file
            df = pd.read_excel(file_path)
            logger.info(f"Loaded {len(df)} records from {file_path}")
            
            # Map data to database format
            mapped_data = self.mapper.map_invoice_data(df)
            logger.info(f"Mapped {len(mapped_data)} records for database insertion")
            
            # Insert/update in database
            result = self.db.upsert_invoices(mapped_data)
            
            logger.info(f"Master file migration completed: {result}")
            return {
                'file': file_path,
                'records_processed': len(mapped_data),
                'inserted': result['inserted'],
                'updated': result['updated'],
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Error migrating master file {file_path}: {e}")
            return {
                'file': file_path,
                'error': str(e),
                'success': False
            }
    
    def migrate_invoice_details(self, file_path: str, source_system: str) -> dict:
        """
        Migrate invoice details Excel file to database
        
        Args:
            file_path: Path to the details Excel file
            source_system: 'BCI' or 'AUS'
            
        Returns:
            Dictionary with migration results
        """
        logger.info(f"Starting migration of {source_system} details: {file_path}")
        
        try:
            # Read Excel file
            df = pd.read_excel(file_path)
            logger.info(f"Loaded {len(df)} records from {file_path}")
            
            # Map data based on source system
            if source_system.upper() == 'BCI':
                mapped_data = self.mapper.map_bci_details(df)
            elif source_system.upper() == 'AUS':
                mapped_data = self.mapper.map_aus_details(df)
            else:
                raise ValueError(f"Unknown source system: {source_system}")
            
            logger.info(f"Mapped {len(mapped_data)} {source_system} records for database insertion")
            
            # Insert in database (using batch processing for large datasets)
            result = self.db.bulk_insert_invoice_details(mapped_data)
            
            logger.info(f"{source_system} details migration completed: {result} records inserted")
            return {
                'file': file_path,
                'source_system': source_system,
                'records_processed': len(mapped_data),
                'inserted': result,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Error migrating {source_system} details {file_path}: {e}")
            return {
                'file': file_path,
                'source_system': source_system,
                'error': str(e),
                'success': False
            }
    
    def migrate_kaiser_scr(self, file_path: str) -> dict:
        """
        Migrate Kaiser SCR master file to building dimension table
        """
        logger.info(f"Starting migration of Kaiser SCR data: {file_path}")
        
        try:
            # Read Excel file
            df = pd.read_excel(file_path)
            logger.info(f"Loaded {len(df)} records from {file_path}")
            
            # Map data to building dimension format
            mapped_data = self.mapper.map_kaiser_scr_data(df)
            logger.info(f"Mapped {len(mapped_data)} building records for database insertion")
            
            # Insert/update in database
            result = self.db.upsert_building_dimension(mapped_data)
            
            logger.info(f"Kaiser SCR migration completed: {result} records processed")
            return {
                'file': file_path,
                'records_processed': len(mapped_data),
                'upserted': result,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Error migrating Kaiser SCR {file_path}: {e}")
            return {
                'file': file_path,
                'error': str(e),
                'success': False
            }
    
    def find_files_by_pattern(self, directory: str, pattern: str) -> list:
        """
        Find files matching a pattern in a directory
        
        Python Concept: Using pathlib for cross-platform file operations
        """
        directory_path = Path(directory)
        if not directory_path.exists():
            logger.warning(f"Directory does not exist: {directory}")
            return []
        
        files = list(directory_path.glob(pattern))
        logger.info(f"Found {len(files)} files matching pattern '{pattern}' in {directory}")
        return [str(f) for f in files]
    
    def batch_migrate_directory(self, directory: str) -> dict:
        """
        Automatically detect and migrate all Excel files in a directory
        
        This function demonstrates Python's ability to handle batch operations
        """
        logger.info(f"Starting batch migration of directory: {directory}")
        
        results = {
            'master_files': [],
            'bci_details': [],
            'aus_details': [],
            'kaiser_scr': [],
            'errors': []
        }
        
        # Find different types of files based on naming patterns
        master_files = self.find_files_by_pattern(directory, "*master*.xlsx")
        bci_files = self.find_files_by_pattern(directory, "*bci*.xlsx")
        aus_files = self.find_files_by_pattern(directory, "*aus*.xlsx")
        kaiser_files = self.find_files_by_pattern(directory, "*kaiser*.xlsx")
        
        # Migrate master files
        for file_path in master_files:
            result = self.migrate_master_excel(file_path)
            results['master_files'].append(result)
        
        # Migrate BCI details
        for file_path in bci_files:
            result = self.migrate_invoice_details(file_path, 'BCI')
            results['bci_details'].append(result)
        
        # Migrate AUS details
        for file_path in aus_files:
            result = self.migrate_invoice_details(file_path, 'AUS')
            results['aus_details'].append(result)
        
        # Migrate Kaiser SCR
        for file_path in kaiser_files:
            result = self.migrate_kaiser_scr(file_path)
            results['kaiser_scr'].append(result)
        
        # Summary
        total_files = len(master_files) + len(bci_files) + len(aus_files) + len(kaiser_files)
        successful_migrations = sum([
            len([r for r in results['master_files'] if r['success']]),
            len([r for r in results['bci_details'] if r['success']]),
            len([r for r in results['aus_details'] if r['success']]),
            len([r for r in results['kaiser_scr'] if r['success']])
        ])
        
        logger.info(f"Batch migration completed: {successful_migrations}/{total_files} files migrated successfully")
        
        return results

def main():
    """
    Main function - handles command line arguments and orchestrates migration
    
    Python Concept: argparse for professional command-line interfaces
    """
    parser = argparse.ArgumentParser(description="Migrate Excel data to database")
    parser.add_argument("--file", "-f", help="Single file to migrate")
    parser.add_argument("--directory", "-d", help="Directory to batch migrate")
    parser.add_argument("--type", "-t", choices=['master', 'bci', 'aus', 'kaiser'], 
                       help="Type of file (required for single file migration)")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Preview what would be migrated without actually doing it")
    
    args = parser.parse_args()
    
    print("🚀 DATA MIGRATION TOOL")
    print("=" * 50)
    
    try:
        migrator = DataMigrator()
        
        if args.dry_run:
            print("🔍 DRY RUN MODE - No data will be migrated")
        
        if args.file:
            # Single file migration
            if not args.type:
                print("❌ Error: --type is required for single file migration")
                return
            
            print(f"📁 Migrating single file: {args.file} (type: {args.type})")
            
            if not args.dry_run:
                if args.type == 'master':
                    result = migrator.migrate_master_excel(args.file)
                elif args.type == 'bci':
                    result = migrator.migrate_invoice_details(args.file, 'BCI')
                elif args.type == 'aus':
                    result = migrator.migrate_invoice_details(args.file, 'AUS')
                elif args.type == 'kaiser':
                    result = migrator.migrate_kaiser_scr(args.file)
                
                if result['success']:
                    print(f"✅ Migration successful: {result}")
                else:
                    print(f"❌ Migration failed: {result}")
            else:
                print("✅ Dry run completed - file would be migrated")
        
        elif args.directory:
            # Batch directory migration
            print(f"📁 Batch migrating directory: {args.directory}")
            
            if not args.dry_run:
                results = migrator.batch_migrate_directory(args.directory)
                
                # Print summary
                print("\n📊 MIGRATION SUMMARY:")
                print(f"Master files: {len(results['master_files'])} processed")
                print(f"BCI details: {len(results['bci_details'])} processed")
                print(f"AUS details: {len(results['aus_details'])} processed")
                print(f"Kaiser SCR: {len(results['kaiser_scr'])} processed")
                
                # Show any errors
                all_results = (results['master_files'] + results['bci_details'] + 
                             results['aus_details'] + results['kaiser_scr'])
                failed_results = [r for r in all_results if not r['success']]
                
                if failed_results:
                    print(f"\n❌ {len(failed_results)} files failed to migrate:")
                    for result in failed_results:
                        print(f"  - {result['file']}: {result.get('error', 'Unknown error')}")
                else:
                    print("\n✅ All files migrated successfully!")
            else:
                print("✅ Dry run completed - directory would be migrated")
        
        else:
            print("❌ Error: Either --file or --directory must be specified")
            parser.print_help()
    
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        logger.error(f"Migration failed: {e}")

if __name__ == "__main__":
    main()