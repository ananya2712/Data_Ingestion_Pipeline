import scrapy

class JobItem(scrapy.Item):
    """Item for storing job listings data."""
    # Required fields
    job_id = scrapy.Field()    # Unique identifier for the job
    title = scrapy.Field()     # Job title
    company = scrapy.Field()   # Company name
    url = scrapy.Field()       # URL of the job listing
    
    # Optional fields
    description = scrapy.Field()     # Job description
    location = scrapy.Field()        # Job location
    salary = scrapy.Field()          # Salary information
    job_type = scrapy.Field()        # Full-time, part-time, contract, etc.
    posted_date = scrapy.Field()     # When the job was posted
    skills = scrapy.Field()          # Required skills (list)
    
    # Metadata fields
    source = scrapy.Field()          # Source website/platform
    scraped_at = scrapy.Field()      # Timestamp when this was scraped
    raw_data = scrapy.Field()        # Raw data in case we need it later