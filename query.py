#!/usr/bin/env python3
"""
Query script for retrieving job data from MongoDB and exporting to CSV.

This script:
1. Establishes a connection to MongoDB using the mongodb_connector
2. Retrieves job data with optional filtering
3. Exports the data to a CSV file
"""

import os
import csv
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional

# Import our infrastructure connectors
from infra.mongodb_connector import MongoDBConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger('query')


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Query job data from MongoDB and export to CSV')
    
    parser.add_argument('--output', type=str, default='final_jobs.csv',
                        help='Output CSV file path (default: final_jobs.csv)')
    
    parser.add_argument('--limit', type=int, default=0,
                        help='Maximum number of results to return (0 for all)')
    
    parser.add_argument('--company', type=str, default=None,
                        help='Filter by company name (case insensitive)')
    
    parser.add_argument('--job-type', type=str, default=None,
                        help='Filter by job type (e.g., Full-time, Contract)')
    
    parser.add_argument('--location', type=str, default=None,
                        help='Filter by location (case insensitive)')
    
    parser.add_argument('--mongo-uri', type=str, 
                        default=os.environ.get('MONGO_URI', 'mongodb://mongodb:27017/'),
                        help='MongoDB connection URI')
    
    parser.add_argument('--mongo-db', type=str,
                        default=os.environ.get('MONGO_DATABASE', 'jobs_data'),
                        help='MongoDB database name')
    
    return parser.parse_args()


def build_query(args) -> Dict:
    """
    Build MongoDB query from command line arguments.
    
    Args:
        args: Command line arguments
        
    Returns:
        Dict: MongoDB query
    """
    query = {}
    
    # Filter by company
    if args.company:
        query['company'] = {'$regex': args.company, '$options': 'i'}
    
    # Filter by job type
    if args.job_type:
        query['job_type'] = {'$regex': args.job_type, '$options': 'i'}
    
    # Filter by location
    if args.location:
        query['location'] = {'$regex': args.location, '$options': 'i'}
    
    return query


def export_to_csv(data: List[Dict], output_path: str) -> bool:
    """
    Export job data to CSV file.
    
    Args:
        data: List of job documents
        output_path: Path to output CSV file
        
    Returns:
        bool: True if export was successful, False otherwise
    """
    if not data:
        logger.warning("No data to export")
        return False
    
    try:
        # Determine CSV fieldnames (columns)
        # Start with common fields in a specific order
        fieldnames = [
            'job_id', 'title', 'company', 'location', 'job_type',
            'salary', 'url', 'source', 'scraped_at'
        ]
        
        # Add any additional fields found in the data
        for doc in data:
            for key in doc.keys():
                if key not in fieldnames:
                    fieldnames.append(key)
        
        # Write to CSV
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for doc in data:
                # Process any special fields
                row = doc.copy()
                
                # Handle lists (e.g., skills) by joining with commas
                for key, value in row.items():
                    if isinstance(value, list):
                        row[key] = ', '.join(str(item) for item in value)
                    elif isinstance(value, datetime):
                        row[key] = value.isoformat()
                
                writer.writerow(row)
        
        logger.info(f"Exported {len(data)} records to {output_path}")
        return True
    
    except Exception as e:
        logger.error(f"Error exporting to CSV: {e}")
        return False


def main():
    """Main function to retrieve job data and export to CSV."""
    args = parse_args()
    
    # Initialize MongoDB connector
    mongo_connector = MongoDBConnector(uri=args.mongo_uri, db_name=args.mongo_db)
    if not mongo_connector.connect():
        logger.error("Failed to connect to MongoDB")
        return
    
    try:
        # Build query from arguments
        query = build_query(args)
        
        # Retrieve data using our reusable MongoDB connector
        collection_name = 'jobs'
        logger.info(f"Executing query: {query}")
        
        # Use the find method from our MongoDB connector
        # This demonstrates using reusable queries
        jobs_data = mongo_connector.find(
            collection_name=collection_name,
            query=query,
            limit=args.limit
        )
        
        # Log result count
        logger.info(f"Query returned {len(jobs_data)} results")
        
        # Export to CSV
        if jobs_data:
            export_to_csv(jobs_data, args.output)
            logger.info(f"Data successfully exported to {args.output}")
        else:
            logger.warning("No data found matching the query criteria")
        
    finally:
        # Close database connection
        mongo_connector.close()


if __name__ == "__main__":
    main()