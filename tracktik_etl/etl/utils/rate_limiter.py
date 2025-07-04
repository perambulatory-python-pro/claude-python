# etl/utils/rate_limiter.py
"""
Rate limiting and checkpoint management for API calls
"""
import time
import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """Preemptive rate limiter with adaptive backoff"""
    
    def __init__(self, calls_per_minute: int = 60):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self.last_call_time = None
        self.consecutive_errors = 0
        
    def wait_if_needed(self):
        """Wait if necessary to maintain rate limit"""
        if self.last_call_time:
            elapsed = time.time() - self.last_call_time
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                # Add extra time if we've had errors
                if self.consecutive_errors > 0:
                    sleep_time *= (1.5 ** self.consecutive_errors)
                time.sleep(sleep_time)
        
        self.last_call_time = time.time()
    
    def record_success(self):
        """Reset error counter on successful call"""
        self.consecutive_errors = 0
    
    def record_error(self):
        """Increment error counter for adaptive backoff"""
        self.consecutive_errors = min(self.consecutive_errors + 1, 5)  # Cap at 5


class CheckpointManager:
    """Manage extraction checkpoints for resumability"""
    
    def __init__(self, checkpoint_dir: str = "etl/checkpoints"):
        self.checkpoint_dir = checkpoint_dir
        os.makedirs(checkpoint_dir, exist_ok=True)
        
    def get_checkpoint_path(self, billing_period: str, region: str) -> str:
        """Generate checkpoint file path"""
        safe_region = region.replace(" ", "_").replace("/", "_")
        return os.path.join(
            self.checkpoint_dir,
            f"checkpoint_{billing_period}_{safe_region}.json"
        )
    
    def save_checkpoint(self, billing_period: str, region: str, data: Dict[str, Any]):
        """Save checkpoint data"""
        checkpoint_path = self.get_checkpoint_path(billing_period, region)
        data['last_updated'] = datetime.now().isoformat()
        
        with open(checkpoint_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.debug(f"Checkpoint saved for {region}: {checkpoint_path}")
    
    def load_checkpoint(self, billing_period: str, region: str) -> Optional[Dict[str, Any]]:
        """Load checkpoint if exists"""
        checkpoint_path = self.get_checkpoint_path(billing_period, region)
        
        if os.path.exists(checkpoint_path):
            with open(checkpoint_path, 'r') as f:
                data = json.load(f)
                logger.info(f"Checkpoint loaded for {region}: Last updated {data.get('last_updated')}")
                return data
        
        return None
    
    def clear_checkpoint(self, billing_period: str, region: str):
        """Remove checkpoint after successful completion"""
        checkpoint_path = self.get_checkpoint_path(billing_period, region)
        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)
            logger.info(f"Checkpoint cleared for {region}")