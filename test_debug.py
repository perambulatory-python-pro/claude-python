#!/usr/bin/env python3
"""Debug test for client loading issue"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tracktik_etl.etl.tracktik_client import TrackTikClient
from tracktik_etl.etl.models import DimClient, ETLBatch
from tracktik_etl.etl.database import db
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def test_client_fetch():
    """Test fetching and examining client data"""
    client = TrackTikClient()
    
    # Get first few clients
    all_clients = client.get_clients()
    
    print(f"\nTotal clients: {len(all_clients)}")
    print("\nFirst client structure:")
    if all_clients:
        first_client = all_clients[0]
        print(f"Type: {type(first_client)}")
        print(f"Keys: {first_client.keys()}")
        print(f"Region field type: {type(first_client.get('region'))}")
        print(f"Region value: {first_client.get('region')}")
        print(f"\nFull client data:")
        import json
        print(json.dumps(first_client, indent=2, default=str))
    
    return all_clients

def test_dim_client_upsert():
    """Test the DimClient.upsert method"""
    # Create test batch
    batch_id = ETLBatch.create_batch('DEBUG_TEST', {'test': 'client_upsert'})
    
    # Get one client
    clients = test_client_fetch()
    if clients:
        test_client = [clients[0]]  # Just test with one
        
        print(f"\nTesting DimClient.upsert with 1 client...")
        result = DimClient.upsert(test_client, batch_id)
        
        print(f"Result type: {type(result)}")
        print(f"Result value: {result}")
        
        ETLBatch.complete_batch(batch_id, 1)

if __name__ == "__main__":
    print("=== Testing Client Data Structure ===")
    test_dim_client_upsert()