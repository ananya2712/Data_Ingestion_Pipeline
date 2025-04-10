import os
import logging
from typing import Optional
import redis
from redis.exceptions import ConnectionError, RedisError

class RedisConnector:
    """
    A connector class to handle Redis operations for caching and deduplication.
    """
    def __init__(self, url: Optional[str] = None):
        """
        Initialize Redis connection.
        
        Args:
            url: Redis connection URL (defaults to environment variable)
        """
        self.logger = logging.getLogger(__name__)
        
        # Get connection URL from environment variable if not provided
        self.redis_url = url or os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
        
        self.client: Optional[redis.Redis] = None
        
    def connect(self) -> bool:
        """
        Establish connection to Redis.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            self.client = redis.from_url(self.redis_url)
            # Test connection
            self.client.ping()
            self.logger.info(f"Connected to Redis at {self.redis_url}")
            return True
        except ConnectionError as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to Redis: {e}")
            return False
    
    def close(self) -> None:
        """Close the Redis connection."""
        if self.client:
            self.client.close()
            self.logger.info("Redis connection closed")
    
    def add_to_set(self, set_name: str, value: str) -> bool:
        """
        Add a value to a Redis set (useful for deduplication).
        
        Args:
            set_name: Name of the set
            value: Value to add
            
        Returns:
            bool: True if value was added, False if already exists or error
        """
        if not self.client and not self.connect():
            return False
            
        try:
            return bool(self.client.sadd(set_name, value))
        except RedisError as e:
            self.logger.error(f"Error adding to set: {e}")
            return False
    
    def is_in_set(self, set_name: str, value: str) -> bool:
        """
        Check if a value exists in a Redis set.
        
        Args:
            set_name: Name of the set
            value: Value to check
            
        Returns:
            bool: True if value is in set, False otherwise
        """
        if not self.client and not self.connect():
            return False
            
        try:
            return bool(self.client.sismember(set_name, value))
        except RedisError as e:
            self.logger.error(f"Error checking set membership: {e}")
            return False
    
    def add_job_id(self, job_id: str, source: str = "default") -> bool:
        """
        Add job ID fingerprint to deduplication set.
        
        Args:
            job_id: Job ID to store
            source: Source name for namespace
            
        Returns:
            bool: True if job ID was new, False if already seen
        """
        dedupe_set = f"scraped_jobs:{source}"
        return self.add_to_set(dedupe_set, job_id)
    
    def is_job_seen(self, job_id: str, source: str = "default") -> bool:
        """
        Check if job ID has been processed before.
        
        Args:
            job_id: Job ID to check
            source: Source name for namespace
            
        Returns:
            bool: True if job ID was seen before, False otherwise
        """
        dedupe_set = f"scraped_jobs:{source}"
        return self.is_in_set(dedupe_set, job_id)