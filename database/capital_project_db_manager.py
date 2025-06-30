"""
Capital Project Database Manager
Handles PostgreSQL integration for capital project tracking

Key Python Concepts Covered:
1. Database Connection Management - Using connection pooling
2. Environment Variables - Secure credential handling  
3. Upsert Operations - INSERT ... ON CONFLICT patterns
4. Data Validation - Business rule enforcement
5. Error Handling - Comprehensive exception management
6. Type Hints - Modern Python typing practices
"""

import os
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from sqlalchemy import create_engine
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date
import logging
from contextlib import contextmanager

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not installed. Install with: pip install python-dotenv")
    print("Or set DATABASE_URL environment variable manually")

class CapitalProjectDBManager:
    """
    Manages all capital project database operations with NeonDB PostgreSQL
    
    Python Learning: This class encapsulates all database logic in one place,
    making it reusable and maintainable.
    """
    
    def __init__(self, database_url: str = None):
        """
        Initialize database manager with connection pooling
        
        Args:
            database_url: PostgreSQL connection string (if None, reads from env)
        """
        # Python Concept: Environment variables for secure config
        self.database_url = database_url or os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable required")
        
        # Python Concept: Connection pooling for efficiency
        self.connection_pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=self.database_url
        )
        
        # SQLAlchemy engine for pandas operations
        self.engine = create_engine(self.database_url)
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections
        
        Python Concept: Context managers ensure proper resource cleanup
        """
        conn = None
        try:
            conn = self.connection_pool.getconn()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                self.connection_pool.putconn(conn)
    
    def identify_capital_projects(self, df: pd.DataFrame, 
                                invoice_col: str = 'Invoice No.',
                                chartfield_col: str = 'Chartfield') -> pd.DataFrame:
        """
        Identify capital projects from invoice data using 'CAP' in chartfield
        
        Args:
            df: DataFrame containing invoice data
            invoice_col: Column name for invoice numbers
            chartfield_col: Column name for chartfield
            
        Returns:
            DataFrame with capital project invoices only
            
        Python Learning: DataFrame filtering and string operations
        """
        # Filter for capital projects (chartfield contains 'CAP')
        cap_mask = df[chartfield_col].astype(str).str.contains('CAP', case=False, na=False)
        capital_projects = df[cap_mask].copy()
        
        # Rename invoice column to standard name and extract capital project number
        capital_projects = capital_projects.rename(columns={invoice_col: 'invoice_number'})
        capital_projects['capital_project_number'] = capital_projects[chartfield_col]
        
        self.logger.info(f"Identified {len(capital_projects)} capital project invoices")
        return capital_projects
    
    def upsert_capital_project_invoices(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Upsert capital project invoice mappings
        
        Args:
            df: DataFrame with invoice_number and capital_project_number columns
            
        Returns:
            Dictionary with insert/update counts
            
        Python Learning: Batch database operations and transaction management
        """
        results = {'inserted': 0, 'updated': 0, 'errors': 0}
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for _, row in df.iterrows():
                    try:
                        # Python Concept: Parameterized queries prevent SQL injection
                        cursor.execute("""
                            INSERT INTO capital_project_invoices 
                            (invoice_number, capital_project_number)
                            VALUES (%s, %s)
                            ON CONFLICT (invoice_number, capital_project_number) 
                            DO NOTHING
                            RETURNING id
                        """, (
                            str(row['invoice_number']),
                            str(row['capital_project_number'])
                        ))
                        
                        if cursor.fetchone():
                            results['inserted'] += 1
                        else:
                            results['updated'] += 1
                            
                    except Exception as e:
                        results['errors'] += 1
                        self.logger.error(f"Error upserting {row['invoice_number']}: {e}")
                
                # Python Concept: Explicit transaction commit
                conn.commit()
        
        self.logger.info(f"Capital project upsert complete: {results}")
        return results
    
    def process_trimble_csv(self, csv_path: str) -> Dict[str, int]:
        """
        Process Trimble CSV file and upsert tracking data
        
        Args:
            csv_path: Path to Trimble CSV file
            
        Returns:
            Dictionary with processing results
            
        Python Learning: File processing and data transformation
        """
        # Read and clean the CSV
        df = pd.read_csv(csv_path)
        
        # Rename columns to match our database schema
        column_mapping = {
            'Vendor Reference/Invoice Number': 'invoice_number',  # Updated to align with table schema
            'Date Created': 'trimble_date_created',
            'Current Step': 'current_step',
            'Status': 'status',
            'Step Date Created': 'current_step_date',
            'Project Number': 'project_number',
            'Payment Reference': 'payment_reference',
            'OneLink Voucher ID': 'onelink_voucher_id'
        }
        
        df = df.rename(columns=column_mapping)
        
        # Convert date columns
        date_columns = ['trimble_date_created', 'current_step_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Process each record
        results = {'inserted': 0, 'updated': 0, 'step_changes': 0, 'errors': 0}
        
        for _, row in df.iterrows():
            try:
                result = self.upsert_trimble_tracking(row.to_dict())
                if result['step_changed']:
                    results['step_changes'] += 1
                if result['inserted']:
                    results['inserted'] += 1
                else:
                    results['updated'] += 1
            except Exception as e:
                results['errors'] += 1
                self.logger.error(f"Error processing {row.get('invoice_number')}: {e}")
        
        self.logger.info(f"Trimble CSV processing complete: {results}")
        return results
    
    def upsert_trimble_tracking(self, record: Dict[str, Any]) -> Dict[str, bool]:
        """
        Upsert single Trimble tracking record with 3-step history
        
        Args:
            record: Dictionary containing Trimble data
            
        Returns:
            Dictionary indicating what changed
            
        Python Learning: Robust upsert logic with comprehensive error handling
        """
        invoice_num = str(record['invoice_number'])
        new_step = record.get('current_step')
        new_step_date = record.get('current_step_date')
        
        result = {'inserted': False, 'step_changed': False}
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                try:
                    # Check if record exists
                    cursor.execute("""
                        SELECT invoice_number, current_step, current_step_date,
                               previous_step, previous_step_date
                        FROM capital_project_trimble_tracking 
                        WHERE invoice_number = %s
                    """, (invoice_num,))
                    
                    existing = cursor.fetchone()
                    
                    if existing and existing['current_step'] != new_step:
                        # Step changed - shift history and update
                        result['step_changed'] = True
                        
                        cursor.execute("""
                            UPDATE capital_project_trimble_tracking SET
                                current_step = %s,
                                current_step_date = %s,
                                previous_step = %s,
                                previous_step_date = %s,
                                previous_step_2 = %s,
                                previous_step_2_date = %s,
                                status = %s,
                                project_number = %s,
                                payment_reference = %s,
                                onelink_voucher_id = %s,
                                last_updated = CURRENT_TIMESTAMP
                            WHERE invoice_number = %s
                        """, (
                            new_step,
                            new_step_date,
                            existing['current_step'],
                            existing['current_step_date'],
                            existing['previous_step'],
                            existing['previous_step_date'],
                            record.get('status'),
                            record.get('project_number'),
                            record.get('payment_reference'),
                            record.get('onelink_voucher_id'),
                            invoice_num
                        ))
                        
                    else:
                        # New record or same step - upsert with safer parameter handling
                        payment_ref = record.get('payment_reference')
                        
                        # Robust NULL handling for payment_reference
                        if payment_ref is not None:
                            # Handle pandas NaN, numpy NaN, and string representations
                            if pd.isna(payment_ref) or payment_ref == 'NaN' or payment_ref == 'nan':
                                payment_ref = None
                            else:
                                try:
                                    # Try to convert to int, checking range
                                    payment_ref = int(float(payment_ref))
                                    min_bigint = -9223372036854775808
                                    max_bigint = 9223372036854775807
                                    if payment_ref < min_bigint or payment_ref > max_bigint:
                                        self.logger.warning(f"Payment reference {payment_ref} out of BIGINT range for invoice {invoice_num}, setting to NULL")
                                        payment_ref = None
                                except (ValueError, TypeError, OverflowError):
                                    self.logger.warning(f"Invalid payment reference '{payment_ref}' for invoice {invoice_num}, setting to NULL")
                                    payment_ref = None
                        
                        cursor.execute("""
                            INSERT INTO capital_project_trimble_tracking (
                                invoice_number,
                                current_step,
                                current_step_date,
                                trimble_date_created,
                                status,
                                project_number,
                                payment_reference,
                                onelink_voucher_id
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (invoice_number) 
                            DO UPDATE SET
                                current_step = EXCLUDED.current_step,
                                current_step_date = EXCLUDED.current_step_date,
                                status = EXCLUDED.status,
                                project_number = EXCLUDED.project_number,
                                payment_reference = EXCLUDED.payment_reference,
                                onelink_voucher_id = EXCLUDED.onelink_voucher_id,
                                last_updated = CURRENT_TIMESTAMP
                            RETURNING (xmax = 0) as is_insert
                        """, (
                            invoice_num,
                            new_step,
                            new_step_date,
                            record.get('trimble_date_created'),
                            record.get('status'),
                            record.get('project_number'),
                            payment_ref,
                            record.get('onelink_voucher_id')
                        ))
                        
                        db_result = cursor.fetchone()
                        result['inserted'] = db_result['is_insert'] if db_result else False
                    
                    conn.commit()
                    
                except Exception as e:
                    conn.rollback()
                    self.logger.error(f"Database error for invoice {invoice_num}: {e}")
                    # Re-raise the exception so it can be caught by the calling function
                    raise e
        
        return result
    
    def check_for_new_releases(self) -> List[Dict[str, Any]]:
        """
        Check for newly released capital projects that need notifications
        
        Returns:
            List of capital projects with new release dates
            
        Python Learning: Using SQLAlchemy engine for consistent database access
        """
        query = """
            SELECT 
                cpi.invoice_number,
                cpi.capital_project_number,
                i.release_date,
                i.chartfield,
                i.invoice_total,
                i.service_area,
                i.post_name
            FROM capital_project_invoices cpi
            JOIN invoices i ON cpi.invoice_number = i.invoice_no
            LEFT JOIN notification_log nl ON cpi.invoice_number = nl.invoice_number 
                AND nl.notification_type = 'release_notification'
            WHERE i.release_date IS NOT NULL 
                AND nl.invoice_number IS NULL
            ORDER BY i.release_date DESC
        """
        
        df = pd.read_sql(query, self.engine)
        return df.to_dict('records')
    
    def log_notification(self, invoice_number: str, recipient_email: str, 
                        notification_type: str = 'release_notification') -> bool:
        """
        Log that a notification was sent
        
        Args:
            invoice_number: Invoice that triggered notification
            recipient_email: Who was notified
            notification_type: Type of notification
            
        Returns:
            True if logged successfully
            
        Python Learning: Audit trail implementation
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO notification_log (
                            notification_type,
                            invoice_number,
                            recipient_email,
                            sent_date,
                            status,
                            message_subject,
                            trigger_event
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        notification_type,
                        invoice_number,
                        recipient_email,
                        datetime.now(),
                        'sent',
                        f'Capital Project Released: {invoice_number}',
                        'release_date_populated'
                    ))
                    
                    conn.commit()
                    return True
        except Exception as e:
            self.logger.error(f"Failed to log notification: {e}")
            return False
    
    def get_capital_project_dashboard_data(self) -> pd.DataFrame:
        """
        Get comprehensive capital project status for dashboard
        
        Returns:
            DataFrame with complete capital project status
            
        Python Learning: Complex SQL queries returning structured data
        """
        with self.get_connection() as conn:
            query = """
                SELECT 
                    cpi.capital_project_number,
                    cpi.invoice_number,
                    i.chartfield,
                    i.release_date,
                    i.edi_date,
                    i.add_on_date,
                    i.invoice_total,
                    i.service_area,
                    i.post_name,
                    tt.current_step,
                    tt.current_step_date,
                    tt.previous_step,
                    tt.previous_step_date,
                    tt.status as trimble_status,
                    tt.project_number as trimble_project_number,
                    CASE 
                        WHEN ie.invoice_number IS NOT NULL THEN 'Emailed'
                        WHEN i.release_date IS NOT NULL THEN 'Ready to Email'
                        ELSE 'Pending Release'
                    END as email_status,
                    ie.emailed_date,
                    nl.sent_date as notification_sent_date
                FROM capital_project_invoices cpi
                JOIN invoices i ON cpi.invoice_number = i.invoice_no
                LEFT JOIN capital_project_trimble_tracking tt 
                    ON cpi.invoice_number = tt.vendor_reference_invoice_number  
                LEFT JOIN capital_project_invoices_emailed ie 
                    ON cpi.invoice_number = ie.invoice_number
                LEFT JOIN notification_log nl 
                    ON cpi.invoice_number = nl.invoice_number 
                    AND nl.notification_type = 'release_notification'
                ORDER BY i.release_date DESC NULLS LAST, cpi.capital_project_number
            """
            
            return pd.read_sql(query, conn)
    
    def close(self):
        """Close the connection pool and engine"""
        if self.connection_pool:
            self.connection_pool.closeall()
        if hasattr(self, 'engine'):
            self.engine.dispose()


# Example usage and testing functions
def test_capital_project_manager():
    """
    Test function demonstrating how to use the CapitalProjectDBManager
    
    Python Learning: Testing patterns and example usage
    """
    # Initialize manager (make sure DATABASE_URL environment variable is set)
    manager = CapitalProjectDBManager()
    
    try:
        # Test 1: Process sample invoice data to identify capital projects
        sample_invoices = pd.DataFrame({
            'Invoice No.': ['INV001', 'INV002', 'INV003'],
            'Chartfield': ['CAP12345', 'REG67890', 'CAP54321'],
            'Invoice Total': [1000.00, 500.00, 1500.00]
        })
        
        capital_projects = manager.identify_capital_projects(sample_invoices)
        print(f"Found {len(capital_projects)} capital projects")
        
        # Test 2: Upsert capital project mappings
        result = manager.upsert_capital_project_invoices(capital_projects)
        print(f"Upsert results: {result}")
        
        # Test 3: Check for new releases
        new_releases = manager.check_for_new_releases()
        print(f"Found {len(new_releases)} new releases needing notification")
        
        # Test 4: Get dashboard data
        dashboard_data = manager.get_capital_project_dashboard_data()
        print(f"Dashboard shows {len(dashboard_data)} total capital project invoices")
        
    finally:
        manager.close()


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run tests
    test_capital_project_manager()
