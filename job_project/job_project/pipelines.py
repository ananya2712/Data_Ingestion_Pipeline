import logging
from datetime import datetime
from typing import Dict, Any

from infra.mongodb_connector import MongoDBConnector
from infra.redis_connector import RedisConnector
from job_project.items import JobItem


class JobDataCleaningPipeline:
    """
    Pipeline for cleaning and normalizing job data.
    """
    def process_item(self, item: JobItem, spider) -> JobItem:
        """Clean and normalize job data."""
        # Clean job title
        if 'title' in item:
            item['title'] = self._clean_text(item['title'])
        
        # Clean company name
        if 'company' in item:
            item['company'] = self._clean_text(item['company'])
        
        # Clean location
        if 'location' in item:
            item['location'] = self._clean_text(item['location'])
        
        # Clean description (keep reasonable length)
        if 'description' in item and item['description']:
            item['description'] = self._clean_text(item['description'])
            # Truncate if too long
            if len(item['description']) > 10000:
                item['description'] = item['description'][:10000] + '...'
        
        # Normalize job_type
        if 'job_type' in item:
            item['job_type'] = self._normalize_job_type(item['job_type'])
        
        # Convert skills to list if it's a string
        if 'skills' in item and isinstance(item['skills'], str):
            item['skills'] = [skill.strip() for skill in item['skills'].split(',') if skill.strip()]
        
        return item
    
    def _clean_text(self, text: str) -> str:
        """Clean text by removing extra whitespace and normalizing."""
        if not text:
            return ""
            
        # Convert to string if not already
        text = str(text)
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        return text.strip()
    
    def _normalize_job_type(self, job_type: str) -> str:
        """Normalize job type to standard values."""
        job_type = self._clean_text(job_type).lower()
        
        # Map common variations to standard types
        if any(term in job_type for term in ['full time', 'full-time', 'fulltime']):
            return 'Full-time'
        elif any(term in job_type for term in ['part time', 'part-time', 'parttime']):
            return 'Part-time'
        elif any(term in job_type for term in ['contract', 'contractor']):
            return 'Contract'
        elif any(term in job_type for term in ['temp', 'temporary']):
            return 'Temporary'
        elif any(term in job_type for term in ['intern', 'internship']):
            return 'Internship'
        elif any(term in job_type for term in ['freelance', 'freelancer']):
            return 'Freelance'
        
        # If we can't normalize, return the original
        return job_type.capitalize()


class JobRedisDedupePipeline:
    """
    Pipeline for deduplicating job items using Redis.
    """
    def __init__(self, redis_url=None):
        self.redis_connector = RedisConnector(url=redis_url)
        self.logger = logging.getLogger(__name__)
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            redis_url=crawler.settings.get('REDIS_URL')
        )
    
    def open_spider(self, spider):
        """Connect to Redis when spider starts."""
        if not self.redis_connector.connect():
            self.logger.error("Failed to connect to Redis")
    
    def close_spider(self, spider):
        """Close Redis connection when spider finishes."""
        self.redis_connector.close()
    
    def process_item(self, item: JobItem, spider) -> JobItem:
        """Check if job has been seen before and mark as processed."""
        # Skip if job_id or source is missing
        if 'job_id' not in item or 'source' not in item:
            return item
        
        # Check if job has been seen before
        job_id = str(item['job_id'])
        source = str(item['source'])
        
        if self.redis_connector.is_in_set(f"scraped_jobs:{source}", job_id):
            self.logger.debug(f"Job already processed: {job_id} from {source}")
        else:
            # Mark job as seen
            self.redis_connector.add_to_set(f"scraped_jobs:{source}", job_id)
        
        return item


class JobsMongoDBPipeline:
    """
    Pipeline for storing job items in MongoDB.
    """
    collection_name = 'jobs'
    
    def __init__(self, mongo_uri=None, mongo_db=None):
        self.mongo_connector = MongoDBConnector(uri=mongo_uri, db_name=mongo_db)
        self.logger = logging.getLogger(__name__)
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )
    
    def open_spider(self, spider):
        """Connect to MongoDB when spider starts."""
        if not self.mongo_connector.connect():
            self.logger.error("Failed to connect to MongoDB")
    
    def close_spider(self, spider):
        """Close MongoDB connection when spider finishes."""
        self.mongo_connector.close()
    
    def process_item(self, item: JobItem, spider) -> JobItem:
        """Process and store job item in MongoDB."""
        # Add timestamp if not present
        if 'scraped_at' not in item:
            item['scraped_at'] = datetime.utcnow()
        
        # Create document from item
        doc = dict(item)
        
        # Use job_id and source as unique keys to avoid duplicates
        unique_keys = ['job_id', 'source']
        
        # Insert into MongoDB
        self.mongo_connector.insert_one(
            self.collection_name,
            doc,
            unique_keys=unique_keys if all(key in doc for key in unique_keys) else None
        )
        
        return item