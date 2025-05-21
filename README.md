# Reddit Data Engineering Pipeline

This project provides a comprehensive data pipeline solution to extract, transform, and load (ETL) Reddit data into AWS S3, with orchestration via Apache Airflow. The pipeline is modular and production-ready, leveraging Docker for reproducibility and security best practices for sensitive data.

## Features
- **Extract** top posts from any subreddit using the Reddit API (via PRAW)
- **Transform** and clean data for analytics
- **Load** data to AWS S3
- **Orchestrate** with Apache Airflow (CeleryExecutor, Postgres, Redis)
- **Dockerized** for easy local development and deployment
- **Sensitive data** (credentials, configs, data) excluded from version control

## Architecture
```
Reddit API → Airflow DAG → Extract & Clean → Save CSV → Upload to S3
```
- **Airflow**: Orchestrates the ETL process
- **Postgres**: Metadata DB for Airflow
- **Redis**: Celery broker
- **AWS S3**: Data lake for extracted Reddit data

## Quickstart
### Prerequisites
- Docker & Docker Compose
- AWS account (S3 access)
- Reddit API credentials

### Setup
1. **Clone the repo:**
   ```bash
   git clone https://github.com/akashp-dev/Reddit-data-pipeline.git
   cd Reddit-data-pipeline
   ```
2. **Configure credentials:**
   - Copy and fill in your secrets:
     - `reddit_credentials.env` (Reddit API keys)
     - `config/config.conf` (AWS keys, DB, etc.)
   - These files are excluded from git for security.
3. **Build and start the stack:**
   ```bash
   docker-compose build
   docker-compose up -d
   ```
4. **Access Airflow UI:**
   - Open [http://localhost:8080](http://localhost:8080)
   - Default login: `admin` / `admin`
5. **Trigger the DAG:**
   - Find `reddit_etl_pipeline` in the Airflow UI and trigger it.

## File Structure
```
├── dags/                # Airflow DAGs
├── etls/                # ETL utility modules
├── pipelines/           # Pipeline logic (Reddit, S3)
├── utils/               # Constants, helpers
├── config/              # Config templates (excluded from git)
├── data/                # Output data (excluded from git)
├── requirements.txt     # Python dependencies
├── docker-compose.yml   # Docker stack
├── .gitignore           # Excludes sensitive files
```

## Security & Best Practices
- **Never commit secrets**: All sensitive files are in `.gitignore`.
- **Rotate credentials** regularly.
- **Use IAM roles** for AWS in production.

## Customization
- Change subreddit, time filter, or post limit in `dags/reddit_dag.py`.
- Extend the pipeline to load data into Redshift, Athena, or other analytics tools.

## License
MIT

---

