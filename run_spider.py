import os
import sys
import logging
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger('run_spider')

def main():
    """Run the job spider to process JSON files."""
    # Ensure we're in the right directory
    project_root = os.path.dirname(os.path.abspath(__file__))
    os.chdir(project_root)
    
    # Create data directory if it doesn't exist
    data_dir = os.path.join(project_root, 'job_project', 'data')
    os.makedirs(data_dir, exist_ok=True)
    
    # Check if data files exist
    s01_path = os.path.join(data_dir, 's01.json')
    s02_path = os.path.join(data_dir, 's02.json')
    
    if not os.path.exists(s01_path) or not os.path.exists(s02_path):
        logger.warning(f"Data files not found in {data_dir}")
        logger.info("Please place s01.json and s02.json in the data directory")
        return
    
    logger.info("Starting spider process...")
    
    # Get the project settings
    settings = get_project_settings()
    
    # Override settings if needed
    settings.set('DATA_DIR', data_dir)
    
    # Create the crawler process
    process = CrawlerProcess(settings)
    
    # Add the spider to the process
    process.crawl('job_spider', data_dir=data_dir)
    
    # Start the process
    process.start()
    
    logger.info("Spider process completed")


if __name__ == "__main__":
    main()