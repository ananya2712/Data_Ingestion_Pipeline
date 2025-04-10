import json
import os
import logging
from datetime import datetime

import scrapy
from scrapy.http import Request
from job_project.items import JobItem


class JobSpider(scrapy.Spider):
    name = 'job_spider'
    
    def __init__(self, *args, **kwargs):
        super(JobSpider, self).__init__(*args, **kwargs)
        # Initialize logger in a way that won't cause attribute error
        self._logger = logging.getLogger(self.__class__.__name__)
        self.data_dir = kwargs.get('data_dir', '/app/job_project/data')
        self.files_processed = 0
        self.items_processed = 0
        
    def start_requests(self):
        """Start requests by reading local JSON files."""
        # Check if data directory exists
        if not os.path.exists(self.data_dir):
            self._logger.error(f"Data directory doesn't exist: {self.data_dir}")
            return
        
        # List files in data directory
        self._logger.info(f"Looking for JSON files in: {self.data_dir}")
        try:
            for filename in os.listdir(self.data_dir):
                if filename.endswith('.json'):
                    file_path = os.path.join(self.data_dir, filename)
                    self._logger.info(f"Processing file: {file_path}")
                    
                    # Use file:// URLs for local files
                    yield Request(
                        url=f"file://{file_path}",
                        callback=self.parse_json,
                        meta={'filename': filename}
                    )
        except Exception as e:
            self._logger.error(f"Error in start_requests: {e}")
    
    def parse_json(self, response):
        """Parse JSON data from response."""
        filename = response.meta['filename']
        source = os.path.basename(filename).split('.')[0]
        
        try:
            # Parse JSON data
            data = json.loads(response.text)
            jobs = data.get('jobs', [])
            
            if not jobs:
                self._logger.warning(f"No jobs found in {filename}")
                return
                
            self._logger.info(f"Found {len(jobs)} jobs in {filename}")
            
            # Process each job
            for job in jobs:
                job_item = self.process_job(job, source)
                yield job_item
                
            self.files_processed += 1
            
        except json.JSONDecodeError:
            self._logger.error(f"Invalid JSON in file: {filename}")
        except Exception as e:
            self._logger.error(f"Error processing {filename}: {e}")
    
    def process_job(self, job_data, source):
        """Process a single job entry from the JSON data."""
        self.items_processed += 1
        
        job_item = JobItem()
        job_item['job_id'] = str(job_data.get('id', f"{source}_{self.items_processed}"))
        job_item['title'] = job_data.get('title', '')
        job_item['company'] = job_data.get('company', {}).get('name', '')
        job_item['url'] = job_data.get('url', '')
        job_item['location'] = job_data.get('location', {}).get('name', '')
        job_item['description'] = job_data.get('description', '')
        job_item['source'] = source
        job_item['scraped_at'] = datetime.utcnow()
        
        # Extract additional fields if available
        if 'salary' in job_data:
            salary_data = job_data['salary']
            if isinstance(salary_data, dict):
                min_salary = salary_data.get('min')
                max_salary = salary_data.get('max')
                currency = salary_data.get('currency', 'USD')
                
                if min_salary and max_salary:
                    job_item['salary'] = f"{currency} {min_salary}-{max_salary}"
                elif min_salary:
                    job_item['salary'] = f"{currency} {min_salary}+"
                elif max_salary:
                    job_item['salary'] = f"Up to {currency} {max_salary}"
        
        # Extract job type
        if 'type' in job_data:
            job_item['job_type'] = job_data['type']
        
        # Extract skills
        if 'skills' in job_data and isinstance(job_data['skills'], list):
            skills = []
            for skill in job_data['skills']:
                if isinstance(skill, dict) and 'name' in skill:
                    skills.append(skill['name'])
                elif isinstance(skill, str):
                    skills.append(skill)
            job_item['skills'] = skills
        
        return job_item