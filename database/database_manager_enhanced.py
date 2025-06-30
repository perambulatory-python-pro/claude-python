"""
Enhanced Database Manager with Business Logic
Includes date preservation and "Not Transmitted" logic

Key Python Concepts:
- Business rule implementation: Complex conditional logic
- Date handling: Sophisticated date preservation
- State management: Tracking field changes
- Transaction integrity: Ensuring consistent updates
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
from typing import List, Dict, Optional, Union, Generator, Tuple
import logging
from datetime import datetime, date

# Import our database models
from database_models import Base, Invoice, InvoiceDetail, BuildingDimension, EMIDReference

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnhancedDatabaseManager:
    """
    Enhanced database manager with sophisticated business logic
    Includes date preservation and "Not Transmitted" logic from your original system
    """
    
    def __init__(self, database_url: Optional[str] = None):
        """Initialize database connection"""
        self.database_url = database_url or os.getenv('DATABASE_URL')
        
        if not self.database_url:
            raise ValueError("DATABASE_URL not found. Please check your .env file.")
        
        self.engine = create_engine(
            self.database_url,
            echo=False,
            pool_size=10,
            max_overflow=20
        )
        
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        logger.info("Enhanced database connection initialized successfully")
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Context manager for database sessions"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    def test_connection(self) -> bool:
        """Test if database connection is working"""
        try:
            with self.get_session() as session:
                result = session.execute(text("SELECT 1"))
                logger.info("Database connection test successful")
                return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def preserve_date_logic(self, existing_record: Dict, new_date: date, 
                           current_field: str, original_field: str) -> Tuple[date, Optional[date]]:
        """
        Implement your sophisticated date preservation logic
        
        Business Rule: When updating a date field:
        - If no current date exists, set new date as current
        - If current date exists and new date is different, move current to original
        - Preserve original date if it already exists
        
        Args:
            existing_record: Current database record as dict
            new_date: New date to be set
            current_field: Field name for current date (e.g., 'edi_date')
            original_field: Field name for original date (e.g., 'original_edi_date')
            
        Returns:
            Tuple of (current_date, original_date)
        """
        current_date = existing_record.get(current_field)
        original_date = existing_record.get(original_field)
        
        # If no current date exists, new date becomes current
        if not current_date:
            return new_date, original_date
        
        # If current date exists and new date is different
        if current_date != new_date:
            # Move current to original only if original doesn't exist
            if not original_date:
                return new_date, current_date
            else:
                # Keep existing original, update current
                return new_date, original_date
        
        # If dates are the same, no change needed
        return current_date, original_date
    
    def apply_not_transmitted_logic(self, existing_record: Dict, new_data: Dict, 
                                   processing_type: str) -> bool:
        """
        Apply sophisticated "Not Transmitted" logic
        
        Business Rules:
        1. For EDI processing with new records: not_transmitted = True (held for validation)
        2. For EDI processing with updates: 
           - If new EDI date and not_transmitted in new_data is False/None: set False
           - Otherwise preserve existing logic
        3. For Release/Add-On processing: don't change not_transmitted status
        
        Args:
            existing_record: Current database record as dict
            new_data: New data being processed
            processing_type: 'EDI', 'Release', or 'Add-On'
            
        Returns:
            Boolean value for not_transmitted field
        """
        # For non-EDI processing, don't change not_transmitted status
        if processing_type != 'EDI':
            return existing_record.get('not_transmitted', False)
        
        # For EDI processing
        current_edi_date = existing_record.get('edi_date')
        new_edi_date = new_data.get('edi_date')
        new_not_transmitted = new_data.get('not_transmitted')
        
        # New record with EDI date: held for validation (not transmitted)
        if not current_edi_date and new_edi_date:
            return True
        
        # Existing record being updated with new EDI date
        if current_edi_date and new_edi_date and current_edi_date != new_edi_date:
            # If new data explicitly sets not_transmitted to False/None, use that
            if new_not_transmitted is False or new_not_transmitted is None:
                return False
            # Otherwise, subsequent submissions are transmitted (False)
            return False
        
        # Default: preserve existing status
        return existing_record.get('not_transmitted', False)
    
    def upsert_invoices_with_business_logic(self, invoice_data: List[Dict], 
                                           processing_type: str, 
                                           processing_date: date) -> Dict[str, int]:
        """
        Insert or update invoices with sophisticated business logic
        
        Args:
            invoice_data: List of dictionaries containing invoice data
            processing_type: 'EDI', 'Release', or 'Add-On'
            processing_date: Date to use for the processing type
            
        Returns:
            Dictionary with counts of inserted/updated records
        """
        inserted_count = 0
        updated_count = 0
        
        try:
            with self.get_session() as session:
                for invoice_dict in invoice_data:
                    invoice_no = invoice_dict.get('invoice_no')
                    if not invoice_no:
                        logger.warning("Skipping record without invoice_no")
                        continue
                    
                    # Check if invoice already exists
                    existing_invoice = session.query(Invoice).filter(
                        Invoice.invoice_no == invoice_no
                    ).first()
                    
                    if existing_invoice:
                        # Update existing invoice with business logic
                        existing_dict = {}
                        for column in Invoice.__table__.columns:
                            existing_dict[column.name] = getattr(existing_invoice, column.name)
                        
                        # Apply date preservation logic based on processing type
                        if processing_type == 'EDI':
                            current_edi, original_edi = self.preserve_date_logic(
                                existing_dict, processing_date, 'edi_date', 'original_edi_date'
                            )
                            invoice_dict['edi_date'] = current_edi
                            invoice_dict['original_edi_date'] = original_edi
                            
                        elif processing_type == 'Release':
                            current_release, original_release = self.preserve_date_logic(
                                existing_dict, processing_date, 'release_date', 'original_release_date'
                            )
                            invoice_dict['release_date'] = current_release
                            invoice_dict['original_release_date'] = original_release
                            
                        elif processing_type == 'Add-On':
                            current_addon, original_addon = self.preserve_date_logic(
                                existing_dict, processing_date, 'add_on_date', 'original_add_on_date'
                            )
                            invoice_dict['add_on_date'] = current_addon
                            invoice_dict['original_add_on_date'] = original_addon
                        
                        # Apply not_transmitted logic
                        invoice_dict['not_transmitted'] = self.apply_not_transmitted_logic(
                            existing_dict, invoice_dict, processing_type
                        )
                        
                        # Update existing invoice
                        for key, value in invoice_dict.items():
                            if hasattr(existing_invoice, key):
                                setattr(existing_invoice, key, value)
                        
                        updated_count += 1
                        logger.info(f"Updated existing invoice: {invoice_no} ({processing_type})")
                        
                    else:
                        # Create new invoice
                        # Set the appropriate date field based on processing type
                        if processing_type == 'EDI':
                            invoice_dict['edi_date'] = processing_date
                            # New EDI records are held for validation
                            invoice_dict['not_transmitted'] = True
                        elif processing_type == 'Release':
                            invoice_dict['release_date'] = processing_date
                        elif processing_type == 'Add-On':
                            invoice_dict['add_on_date'] = processing_date
                        
                        # Ensure not_transmitted has a default value
                        if 'not_transmitted' not in invoice_dict:
                            invoice_dict['not_transmitted'] = processing_type == 'EDI'
                        
                        new_invoice = Invoice(**invoice_dict)
                        session.add(new_invoice)
                        inserted_count += 1
                        logger.info(f"Created new invoice: {invoice_no} ({processing_type})")
                
                session.commit()
                logger.info(f"Processed invoices: {inserted_count} inserted, {updated_count} updated")
                
        except Exception as e:
            logger.error(f"Error processing invoices: {e}")
            raise
        
        return {'inserted': inserted_count, 'updated': updated_count}
    
    def process_invoice_history_linking(self, invoice_data: List[Dict]) -> int:
        """
        Handle bidirectional invoice history linking
        
        Args:
            invoice_data: List of invoice dictionaries that may contain history references
            
        Returns:
            Number of history links created
        """
        links_created = 0
        
        try:
            with self.get_session() as session:
                for invoice_dict in invoice_data:
                    current_invoice_no = invoice_dict.get('invoice_no')
                    original_invoice_no = invoice_dict.get('original_invoice_no')  # From "Original invoice #" field
                    
                    if current_invoice_no and original_invoice_no:
                        # Find the original invoice
                        original_invoice = session.query(Invoice).filter(
                            Invoice.invoice_no == original_invoice_no
                        ).first()
                        
                        if original_invoice:
                            # Set bidirectional links
                            # Current invoice points to original
                            current_invoice = session.query(Invoice).filter(
                                Invoice.invoice_no == current_invoice_no
                            ).first()
                            
                            if current_invoice:
                                current_invoice.invoice_no_history = original_invoice_no
                                # Original invoice points to current (revision)
                                original_invoice.invoice_no_history = current_invoice_no
                                links_created += 1
                                logger.info(f"Linked invoices: {original_invoice_no} <-> {current_invoice_no}")
                
                session.commit()
                
        except Exception as e:
            logger.error(f"Error processing invoice history links: {e}")
            raise
        
        return links_created
    
    # Include all other methods from the original DatabaseManager
    def get_invoices(self, filters: Optional[Dict] = None) -> pd.DataFrame:
        """Get invoices with optional filtering"""
        try:
            with self.get_session() as session:
                query = session.query(Invoice)
                
                if filters:
                    for column, value in filters.items():
                        if hasattr(Invoice, column) and value is not None:
                            query = query.filter(getattr(Invoice, column) == value)
                
                invoices = query.all()
                
                invoice_dicts = []
                for invoice in invoices:
                    invoice_dict = {}
                    for column in Invoice.__table__.columns:
                        invoice_dict[column.name] = getattr(invoice, column.name)
                    invoice_dicts.append(invoice_dict)
                
                df = pd.DataFrame(invoice_dicts)
                logger.info(f"Retrieved {len(df)} invoices")
                return df
                
        except Exception as e:
            logger.error(f"Error retrieving invoices: {e}")
            raise
    
    def search_invoices(self, search_term: str) -> pd.DataFrame:
        """Search invoices by invoice number"""
        try:
            with self.get_session() as session:
                invoices = session.query(Invoice).filter(
                    Invoice.invoice_no.ilike(f'%{search_term}%')
                ).all()
                
                invoice_dicts = []
                for invoice in invoices:
                    invoice_dict = {}
                    for column in Invoice.__table__.columns:
                        invoice_dict[column.name] = getattr(invoice, column.name)
                    invoice_dicts.append(invoice_dict)
                
                df = pd.DataFrame(invoice_dicts)
                logger.info(f"Found {len(df)} invoices matching '{search_term}'")
                return df
                
        except Exception as e:
            logger.error(f"Error searching invoices: {e}")
            raise
    
    def bulk_insert_invoice_details(self, details_data: List[Dict], batch_size: int = 1000) -> int:
        """Efficiently insert large amounts of invoice detail data"""
        total_inserted = 0
        
        try:
            with self.get_session() as session:
                for i in range(0, len(details_data), batch_size):
                    batch = details_data[i:i + batch_size]
                    detail_objects = [InvoiceDetail(**detail_dict) for detail_dict in batch]
                    session.bulk_save_objects(detail_objects)
                    session.commit()
                    total_inserted += len(batch)
                    logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} records")
                
                logger.info(f"Total invoice details inserted: {total_inserted}")
                
        except Exception as e:
            logger.error(f"Error bulk inserting invoice details: {e}")
            raise
        
        return total_inserted
    
    def get_table_stats(self) -> Dict[str, int]:
        """Get record counts for all tables"""
        stats = {}
        
        try:
            with self.get_session() as session:
                stats['invoices'] = session.query(Invoice).count()
                stats['invoice_details'] = session.query(InvoiceDetail).count()
                stats['building_dimension'] = session.query(BuildingDimension).count()
                stats['emid_reference'] = session.query(EMIDReference).count()
                
                logger.info(f"Table stats: {stats}")
                
        except Exception as e:
            logger.error(f"Error getting table stats: {e}")
            raise
        
        return stats
    
    def execute_custom_query(self, query: str) -> pd.DataFrame:
        """Execute custom SQL queries and return as DataFrame"""
        try:
            with self.get_session() as session:
                result = session.execute(text(query))
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                logger.info(f"Custom query returned {len(df)} rows")
                return df
                
        except Exception as e:
            logger.error(f"Error executing custom query: {e}")
            raise