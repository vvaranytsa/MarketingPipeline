# Marketing Domain Data Processor

## Overview
This project is an Airflow DAG designed to automate the process of extracting, processing, and storing data related to marketing domains. It reads URLs from a file, extracts image URLs from these sites, processes these images using the Google Vision API to extract domain names and additional information, and then stores this data in a PostgreSQL database.

## Features
- **Data Extraction**: Extract image URLs from a list of web pages.
- **Image Processing**: Analyze images to extract domain-related text using the Google Vision API.
- **Data Storage**: Store and update extracted information in a PostgreSQL database.

## Prerequisites
- Docker and Docker Compose
- Airflow (running in Docker)
- PostgreSQL (running in Docker)
- Python libraries: `requests`, `beautifulsoup4`, `google-cloud-vision`
- Google Cloud service account with Vision API enabled
- OpenAI API enabled

## Installation

### Docker Setup
Use the provided `docker-compose.override.yml` to set up the environment. This file configures the necessary services, including Airflow webserver, scheduler, and PostgreSQL, with the required environment variables and Python dependencies.

Here is the `docker-compose.override.yml` content for reference:

```yaml
version: '3.8'

services:
  airflow-webserver:
    environment:
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
      - _PIP_ADDITIONAL_REQUIREMENTS=openai==0.28 google-cloud-vision requests beautifulsoup4

  airflow-scheduler:
    environment:
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
      - _PIP_ADDITIONAL_REQUIREMENTS=google-cloud-vision requests beautifulsoup4

  requirements:
    image: python:3.11
    volumes:
      - .:/project
      - pip38:/usr/local/lib/python3.11/site-packages
    working_dir: /project
    environment:
      - _PIP_ADDITIONAL_REQUIREMENTS=google-cloud-vision requests beautifulsoup4

  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: 2004
      POSTGRES_DB: marketing
    ports:
      - "5432:5432"

volumes:
  pip38:
    external: true
```
## Airflow and Database Setup
1. Clone the repository.
2. Start the Docker environment using `docker-compose up -d`.
3. Access the Airflow web interface and configure the necessary connections and variables.
4. Ensure that the PostgreSQL database is set up and connected properly.


## DAG Overview
The DAG includes three main tasks:
- **Create PostgreSQL Table**: Initialize a database table for data storage.
- **Extract and Process Data**: Extract image URLs from websites and process them using Google Vision API.
- **Insert Data into Database**: Insert the processed data into the PostgreSQL database.
## Task Details

### 1. `create_postgres_table`
Creates a PostgreSQL table (`marketing_domains_db`) to store processed data.

### 2. `extract_and_process_data_task`
Extracts image URLs from a list of websites and processes these images to extract domain names and additional info.

### 3. `insert_data_task`
Inserts or updates the extracted data in the `marketing_domains_db` table.

## Usage
Place the URLs to be processed in the `urls.txt` file. Trigger the DAG from the Airflow UI or CLI.
