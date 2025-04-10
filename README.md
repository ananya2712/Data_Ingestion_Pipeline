# Job Data Processing 

A containerized application that processes job data from JSON files, stores it in MongoDB, uses Redis for de-duplication, and provides query functionality to export the data as CSV.

## Project Structure

```
/app/
├── infra/                      # Infrastructure modules
│   ├── __init__.py             # Package initialization
│   ├── mongodb_connector.py    # MongoDB connection handler
│   └── redis_connector.py      # Redis connection handler (for caching/deduplication)
│
├── job_project/                # Scrapy project
│   ├── data/                   # Data directory for JSON files
│   │   ├── s01.json            # Source 1 JSON data
│   │   └── s02.json            # Source 2 JSON data
│   │
│   ├── job_project/            # Scrapy application
│   │   ├── __init__.py         # Package initialization
│   │   ├── items.py            # Item definitions for scraped data
│   │   ├── middlewares.py      # Middleware components 
│   │   ├── pipelines.py        # Processing pipelines
│   │   ├── settings.py         # Scrapy settings
│   │   └── spiders/            # Spider definitions
│   │       ├── __init__.py
│   │       ├── basic_spider.py # Basic test spider
│   │       └── spiders.py      # Main job data spider
│   │
│   └── scrapy.cfg              # Scrapy configuration
│
├── docker-compose.yaml         # Docker Compose configuration
├── Dockerfile                  # Docker image definition
├── final_jobs.csv              # Output CSV file (generated)
├── query.py                    # Data query and export script
├── README.md                   # This documentation
├── requirements.txt            # Python dependencies
└── run_spider.py               # Helper script to run the spider
```

## Prerequisites

You will need to have these installed on your system.
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Setup and Running

### 1. Clone the Repository

Clone or download this repository to your local machine.

### 2. Prepare Data Files

Place your JSON job data files in the `job_project/data/` directory. The application expects files named `s01.json` and `s02.json`.

The JSON files have the following structure:
```json
{
  "jobs": [
    {
      "id": "12345",
      "title": "Software Engineer",
      "company": {
        "name": "Example Corp"
      },
      "location": {
        "name": "San Francisco, CA"
      },
      "description": "Job description text...",
      "url": "https://example.com/jobs/12345",
      "type": "Full-time",
      "salary": {
        "min": 100000,
        "max": 150000,
        "currency": "USD"
      },
      "skills": [
        {"name": "Python"},
        {"name": "Docker"}
      ]
    },
    // more job entries...
  ]
}
```

### 3. Start the Docker Containers

From the project root directory, run:

```bash
docker build 
docker-compose up -d
```

This will start three containers:
- `scrapy_crawler`: The Python application environment
- `mongodb`: MongoDB database for storing job data
- `redis`: Redis for caching and deduplication (optional)

Check if the containers are running using:

```bash
docker ps
```

### 4. Process the Data

To process the JSON files and store the data in MongoDB:

```bash
docker exec -it scrapy_crawler bash
cd /app/job_project
PYTHONPATH=/app scrapy crawl job_spider
```

You should see logs indicating that the spider is processing the files and storing data in MongoDB.

### 5. Query and Export Data

To export all job data to a CSV file:

```bash
docker exec -it scrapy_crawler bash
cd /app
PYTHONPATH=/app python query.py
```

By default, this will create a file named `final_jobs.csv` with all the job data.

You can also use filters to export specific subsets of data:

```bash
# Export only jobs from a specific company
PYTHONPATH=/app python query.py --company "Google" --output google_jobs.csv

# Export only full-time jobs
PYTHONPATH=/app python query.py --job-type "Full-time" --output fulltime_jobs.csv

# Export jobs in a specific location
PYTHONPATH=/app python query.py --location "Remote" --output remote_jobs.csv

# Limit the number of results
PYTHONPATH=/app python query.py --limit 100 --output sample_jobs.csv
```

### 6. Accessing the Data

The exported CSV file will be created inside the container. To copy it to your local machine:

```bash
docker cp scrapy_crawler:/app/final_jobs.csv ./final_jobs.csv
```

### 7. Stopping the Containers

When you're done, stop the containers:

```bash
docker-compose down
```

## Advanced Usage

### MongoDB Connection

The application connects to MongoDB using the following default settings:
- URI: `mongodb://mongodb:27017/`
- Database: `jobs_data`
- Collection: `jobs`

You can override these by setting environment variables in the `docker-compose.yaml` file:
```yaml
environment:
  - MONGO_URI=mongodb://custom-host:27017/
  - MONGO_DATABASE=custom_db
```

### Redis Connection

If you're using Redis for caching/deduplication, the default connection is:
- URI: `redis://redis:6379/0`

This can be overridden similarly with the `REDIS_URL` environment variable.

## License

This project is licensed for educational purposes only.

---

Created for Data Ingestion Project (DIP)
