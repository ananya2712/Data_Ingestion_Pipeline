import os
import logging
from typing import Dict, List, Optional, Any
from pymongo import MongoClient, errors
from pymongo.collection import Collection
from pymongo.database import Database

class MongoDBConnector:
    """
    A connector class to handle MongoDB operations.
    """
    def __init__(self, uri: Optional[str] = None, db_name: Optional[str] = None):
        """
        Initialize MongoDB connection.
        
        Args:
            uri: MongoDB connection URI (defaults to environment variable)
            db_name: Database name (defaults to environment variable)
        """
        self.logger = logging.getLogger(__name__)
        
        # Get connection parameters from environment variables if not provided
        self.mongo_uri = uri or os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
        self.db_name = db_name or os.environ.get('MONGO_DATABASE', 'jobs_data')
        
        self.client: Optional[MongoClient] = None
        self.db: Optional[Database] = None
        
    def connect(self) -> bool:
        """
        Establish connection to MongoDB.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            self.client = MongoClient(self.mongo_uri)
            # Force connection to verify it works
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            self.logger.info(f"Connected to MongoDB at {self.mongo_uri}, database: {self.db_name}")
            return True
        except errors.ConnectionFailure as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            return False
    
    def close(self) -> None:
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()
            self.logger.info("MongoDB connection closed")
    
    def get_collection(self, collection_name: str) -> Optional[Collection]:
        """
        Get a MongoDB collection.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            Collection object or None if not connected
        """
        if self.db is None:
            if not self.connect():
                return None
        return self.db[collection_name]
        
    def insert_one(self, collection_name: str, document: Dict, unique_keys: Optional[List[str]] = None) -> Optional[str]:
        """
        Insert a single document into MongoDB with duplicate handling.

        Args:
            collection_name: Name of the target collection
            document: Dictionary to insert
            unique_keys: List of keys to create a unique index on
            
        Returns:
            str: Inserted document ID or None if failed
        """
        if self.db is None and not self.connect():
            return None
            
        try:
            # Create unique index if unique_keys specified
            collection = self.db[collection_name]
            if unique_keys:
                index_dict = {key: 1 for key in unique_keys}
                try:
                    collection.create_index(index_dict, unique=True)
                except Exception as e:
                    self.logger.warning(f"Could not create unique index: {e}")
            
            # Insert the document
            result = collection.insert_one(document)
            return str(result.inserted_id)
        except Exception as e:
            self.logger.error(f"Error inserting document: {e}")
            return None
            
    
    def insert_many(self, collection_name: str, documents: List[Dict], ordered: bool = False) -> int:
        """
        Insert multiple documents into MongoDB.
        
        Args:
            collection_name: Name of the target collection
            documents: List of dictionaries to insert
            ordered: If False, continues inserting after errors
            
        Returns:
            int: Number of documents inserted
        """
        if not documents:
            return 0
            
        collection = self.get_collection(collection_name)
        if not collection:
            return 0
            
        try:
            result = collection.insert_many(documents, ordered=ordered)
            inserted_count = len(result.inserted_ids)
            self.logger.info(f"Inserted {inserted_count} documents")
            return inserted_count
        except errors.BulkWriteError as e:
            # Extract successfully inserted documents count
            self.logger.warning(f"Bulk write error: {e.details}")
            return e.details.get('nInserted', 0)
        except Exception as e:
            self.logger.error(f"Error inserting documents: {e}")
            return 0
    
    def find(self, collection_name: str, query: Dict = None, projection: Dict = None, 
         limit: int = 0) -> List[Dict]:
        """
        Find documents matching the query.
        
        Args:
            collection_name: Name of the collection
            query: MongoDB query dict
            projection: Fields to include/exclude
            limit: Maximum number of results
            
        Returns:
            List of matching documents
        """
        collection = self.get_collection(collection_name)
        if collection is None:  # Change from: if not collection:
            return []
            
        try:
            cursor = collection.find(query or {}, projection or {})
            
            if limit > 0:
                cursor = cursor.limit(limit)
                
            return list(cursor)
        except Exception as e:
            self.logger.error(f"Error finding documents: {e}")
            return []
    
    def count_documents(self, collection_name: str, query: Dict = None) -> int:
        """
        Count documents matching the query.
        
        Args:
            collection_name: Name of the collection
            query: Query to match documents
            
        Returns:
            Number of matching documents
        """
        collection = self.get_collection(collection_name)
        if not collection:
            return 0
            
        try:
            return collection.count_documents(query or {})
        except Exception as e:
            self.logger.error(f"Error counting documents: {e}")
            return 0