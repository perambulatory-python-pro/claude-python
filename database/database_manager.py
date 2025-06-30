"""
Database Manager - The main class for all database operations
This replaces your Excel file operations with database operations.

Key Python Concepts:
- Context managers: 'with' statements for safe resource handling
- Exception handling: try/except blocks for error management
- Generators: yield statements for memory-efficient data processing
- Type hints: Specify what types functions expect and return
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
from typing import List, Dict, Optional, Union, Generator
import logging
from datetime import datetime, date

# Import our database models
from database_models import Base, Invoice, InvoiceDetail, BuildingDimension, EMIDReference

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Main database manager class - handles all database operations
    
    Python Concept: This is a 'Singleton-like' pattern where we create one
    database connection that gets reused throughout the application
    """
    
    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize database connection
        
        Args:
            database_url: PostgreSQL connection string (from .env file)
        """
        # Get database URL from environment variable if not provided
        self.database_url = database_url or os.getenv('DATABASE_URL')
        
        if not self.database_url:
            raise ValueError("DATABASE_URL not found. Please check your .env file.")
        
        # Create database engine
        # Python Concept: The engine is like a 'phone connection' to the database
        self.engine = create_engine(
            self.database_url,
            echo=False,  # Set to True to see all SQL queries (useful for debugging)
            pool_size=10,  # Number of connections to keep open
            max_overflow=20  # Additional connections if needed
        )
        
        # Create session factory
        # Python Concept: Sessions are like 'conversations' with the database
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        logger.info("Database connection initialized successfully")
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Context manager for database sessions
        
        Python Concept: Context managers ensure resources are properly cleaned up
        even if errors occur. The 'with' statement automatically calls __enter__ and __exit__
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()  # Save changes
        except Exception as e:
            session.rollback()  # Undo changes if error occurs
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()  # Always close the session
    
    def test_connection(self) -> bool:
        """
        Test if database connection is working
        Returns True if successful, False otherwise
        """
        try:
            with self.get_session() as session:
                # Simple query to test connection
                result = session.execute(text("SELECT 1"))
                logger.info("Database connection test successful")
                return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def create_tables(self):
        """
        Create all tables if they don't exist
        
        Python Concept: This uses SQLAlchemy's metadata to create tables
        based on our model definitions
        """
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    # INVOICE OPERATIONS (replaces your Excel master file operations)
    
    def upsert_invoices(self, invoice_data: List[Dict]) -> Dict[str, int]:
        """
        Insert or update invoices (replaces your Excel upsert logic)
        
        Args:
            invoice_data: List of dictionaries containing invoice data
            
        Returns:
            Dictionary with counts of inserted/updated records
        """
        inserted_count = 0
        updated_count = 0
        
        try:
            with self.get_session() as session:
                for invoice_dict in invoice_data:
                    # Check if invoice already exists
                    existing_invoice = session.query(Invoice).filter(
                        Invoice.invoice_no == invoice_dict['invoice_no']
                    ).first()
                    
                    if existing_invoice:
                        # Update existing invoice
                        for key, value in invoice_dict.items():
                            if hasattr(existing_invoice, key):
                                setattr(existing_invoice, key, value)
                        updated_count += 1
                    else:
                        # Create new invoice
                        new_invoice = Invoice(**invoice_dict)
                        session.add(new_invoice)
                        inserted_count += 1
                
                session.commit()
                logger.info(f"Upserted invoices: {inserted_count} inserted, {updated_count} updated")
                
        except Exception as e:
            logger.error(f"Error upserting invoices: {e}")
            raise
        
        return {'inserted': inserted_count, 'updated': updated_count}
    
    def get_invoices(self, filters: Optional[Dict] = None) -> pd.DataFrame:
        """
        Get invoices with optional filtering (replaces Excel filtering)
        
        Args:
            filters: Dictionary of column:value pairs to filter by
            
        Returns:
            pandas DataFrame with invoice data
        """
        try:
            with self.get_session() as session:
                query = session.query(Invoice)
                
                # Apply filters if provided
                if filters:
                    for column, value in filters.items():
                        if hasattr(Invoice, column) and value is not None:
                            query = query.filter(getattr(Invoice, column) == value)
                
                # Execute query and convert to DataFrame
                invoices = query.all()
                
                # Convert SQLAlchemy objects to dictionaries
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
        """
        Search invoices by invoice number (replaces Excel search)
        """
        try:
            with self.get_session() as session:
                invoices = session.query(Invoice).filter(
                    Invoice.invoice_no.ilike(f'%{search_term}%')
                ).all()
                
                # Convert to DataFrame (same logic as get_invoices)
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
    
    # INVOICE DETAILS OPERATIONS (handles your 50K+ row BCI/AUS data)
    
    def bulk_insert_invoice_details(self, details_data: List[Dict], batch_size: int = 1000) -> int:
        """
        Efficiently insert large amounts of invoice detail data
        
        Python Concept: Batch processing prevents memory overflow and improves performance
        """
        total_inserted = 0
        
        try:
            with self.get_session() as session:
                # Process in batches to avoid memory issues
                for i in range(0, len(details_data), batch_size):
                    batch = details_data[i:i + batch_size]
                    
                    # Create InvoiceDetail objects
                    detail_objects = [InvoiceDetail(**detail_dict) for detail_dict in batch]
                    
                    # Bulk insert the batch
                    session.bulk_save_objects(detail_objects)
                    session.commit()
                    
                    total_inserted += len(batch)
                    logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} records")
                
                logger.info(f"Total invoice details inserted: {total_inserted}")
                
        except Exception as e:
            logger.error(f"Error bulk inserting invoice details: {e}")
            raise
        
        return total_inserted
    
    def get_invoice_details(self, invoice_no: Optional[str] = None, 
                           source_system: Optional[str] = None) -> pd.DataFrame:
        """
        Get invoice details with optional filtering
        """
        try:
            with self.get_session() as session:
                query = session.query(InvoiceDetail)
                
                if invoice_no:
                    query = query.filter(InvoiceDetail.invoice_no == invoice_no)
                if source_system:
                    query = query.filter(InvoiceDetail.source_system == source_system)
                
                details = query.all()
                
                # Convert to DataFrame
                detail_dicts = []
                for detail in details:
                    detail_dict = {}
                    for column in InvoiceDetail.__table__.columns:
                        detail_dict[column.name] = getattr(detail, column.name)
                    detail_dicts.append(detail_dict)
                
                df = pd.DataFrame(detail_dicts)
                logger.info(f"Retrieved {len(df)} invoice details")
                return df
                
        except Exception as e:
            logger.error(f"Error retrieving invoice details: {e}")
            raise
    
    # REFERENCE DATA OPERATIONS
    
    def upsert_building_dimension(self, building_data: List[Dict]) -> int:
        """
        Insert or update building dimension data from Kaiser SCR master
        """
        upserted_count = 0
        
        try:
            with self.get_session() as session:
                for building_dict in building_data:
                    existing_building = session.query(BuildingDimension).filter(
                        BuildingDimension.building_code == building_dict['building_code']
                    ).first()
                    
                    if existing_building:
                        # Update existing
                        for key, value in building_dict.items():
                            if hasattr(existing_building, key):
                                setattr(existing_building, key, value)
                    else:
                        # Create new
                        new_building = BuildingDimension(**building_dict)
                        session.add(new_building)
                    
                    upserted_count += 1
                
                session.commit()
                logger.info(f"Upserted {upserted_count} building dimension records")
                
        except Exception as e:
            logger.error(f"Error upserting building dimension: {e}")
            raise
        
        return upserted_count
    
    # UTILITY METHODS
    
    def get_table_stats(self) -> Dict[str, int]:
        """
        Get record counts for all tables (useful for monitoring)
        """
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
        """
        Execute custom SQL queries and return as DataFrame
        Useful for complex analytics and reporting
        """
        try:
            with self.get_session() as session:
                result = session.execute(text(query))
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                logger.info(f"Custom query returned {len(df)} rows")
                return df
                
        except Exception as e:
            logger.error(f"Error executing custom query: {e}")
            raise