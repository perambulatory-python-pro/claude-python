# etl/config.py
"""
Configuration for TrackTik ETL Pipeline
"""
import os
from dataclasses import dataclass
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Config:
    """ETL Configuration"""
    # TrackTik API
    TRACKTIK_BASE_URL = os.getenv('TRACKTIK_BASE_URL')
    TRACKTIK_CLIENT_ID = os.getenv('TRACKTIK_CLIENT_ID')
    TRACKTIK_CLIENT_SECRET = os.getenv('TRACKTIK_CLIENT_SECRET')
    TRACKTIK_USERNAME = os.getenv('TRACKTIK_USERNAME')
    TRACKTIK_PASSWORD = os.getenv('TRACKTIK_PASSWORD')
    
    # PostgreSQL - Using your variable names
    POSTGRES_HOST = os.getenv('PGHOST', 'localhost')
    POSTGRES_PORT = os.getenv('PGPORT', '5432')
    POSTGRES_DB = os.getenv('PGDATABASE')
    POSTGRES_USER = os.getenv('PGUSER')
    POSTGRES_PASSWORD = os.getenv('PGPASSWORD')
    POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'tracktik')
    
    # ETL Settings
    API_PAGE_SIZE = 100  # Max records per API call
    BATCH_SIZE = 1000    # Records to process at once
    MAX_RETRIES = 3
    RETRY_DELAY = 5      # seconds
    
    @property
    def postgres_url(self):
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

config = Config()