# etl/database.py
"""
Database connection and utilities
"""
import logging
from contextlib import contextmanager
from typing import Dict, List, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from psycopg2.pool import SimpleConnectionPool

from .config import config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manage PostgreSQL connections and operations"""
    
    def __init__(self):
        self.pool = SimpleConnectionPool(
            1, 20,  # min and max connections
            host=config.POSTGRES_HOST,
            port=config.POSTGRES_PORT,
            database=config.POSTGRES_DB,
            user=config.POSTGRES_USER,
            password=config.POSTGRES_PASSWORD
        )
        
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool"""
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)
            
    @contextmanager
    def get_cursor(self, cursor_factory=RealDictCursor):
        """Get a cursor with automatic commit/rollback"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()
                
    def execute_query(self, query: str, params: Dict = None) -> List[Dict]:
        """Execute a SELECT query"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
            
    def execute_batch_insert(self, query: str, data: List[Dict], page_size: int = 100):
        """Execute batch insert with psycopg2 execute_batch"""
        with self.get_cursor() as cursor:
            execute_batch(cursor, query, data, page_size=page_size)
            return cursor.rowcount
            
    def upsert_dimension(self, table: str, records: List[Dict], 
                        id_field: str, scd_fields: List[str]) -> Dict[str, int]:
        """
        Upsert dimension table with SCD Type 2
        
        Returns:
            Dict with counts of inserted and updated records
        """
        if not records:
            return {'inserted': 0, 'updated': 0}
            
        inserted = 0
        updated = 0
        
        with self.get_cursor() as cursor:
            for record in records:
                # Check if record exists and has changed
                check_query = f"""
                    SELECT * FROM {config.POSTGRES_SCHEMA}.{table}
                    WHERE {id_field} = %({id_field})s AND is_current = TRUE
                """
                cursor.execute(check_query, record)
                existing = cursor.fetchone()
                
                if not existing:
                    # Insert new record
                    record['is_current'] = True
                    columns = list(record.keys())
                    values_template = ', '.join([f'%({col})s' for col in columns])
                    
                    insert_query = f"""
                        INSERT INTO {config.POSTGRES_SCHEMA}.{table} ({', '.join(columns)})
                        VALUES ({values_template})
                    """
                    cursor.execute(insert_query, record)
                    inserted += 1
                    
                else:
                    # Check if any SCD fields have changed
                    has_changed = any(
                        str(existing.get(field)) != str(record.get(field))
                        for field in scd_fields
                    )
                    
                    if has_changed:
                        # Close out existing record
                        update_query = f"""
                            UPDATE {config.POSTGRES_SCHEMA}.{table}
                            SET valid_to = CURRENT_TIMESTAMP, is_current = FALSE
                            WHERE {id_field} = %s AND is_current = TRUE
                        """
                        cursor.execute(update_query, (record[id_field],))
                        
                        # Insert new version
                        record['is_current'] = True
                        columns = list(record.keys())
                        values_template = ', '.join([f'%({col})s' for col in columns])
                        
                        insert_query = f"""
                            INSERT INTO {config.POSTGRES_SCHEMA}.{table} ({', '.join(columns)})
                            VALUES ({values_template})
                        """
                        cursor.execute(insert_query, record)
                        updated += 1
                        
        return {'inserted': inserted, 'updated': updated}

# Create singleton instance
db = DatabaseManager()