from airflow import DAG
import datetime as dt
import os
import sys
import shutil
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.reddit_pipeline import reddit_pipeline
from dotenv import load_dotenv

# Load environment variables from reddit_credentials.env
load_dotenv("/opt/airflow/reddit_credentials.env")

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

default_args = {
    'owner': 'Akash Ponnam',
    'start_date': dt.datetime(2025, 4, 29),
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=1),
    'email': ['your-email@example.com'],  # Add your email for notifications
    'email_on_failure': True,
    'email_on_retry': True
}

file_postfix = dt.datetime.now().strftime("%Y-%m-%d")

dag = DAG(
    dag_id='reddit_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline'],
    description='ETL pipeline for Reddit data',
    max_active_runs=1
)

# This will copy the credentials file to make sure it's accessible
setup_credentials = BashOperator(
    task_id='setup_credentials',
    bash_command="""
    # Find the credentials file
    CREDS_FILE="reddit_credentials.env"
    PROJECT_DIR="/opt/airflow"
    
    # Make sure the file exists in the destination
    if [ -f "$PROJECT_DIR/$CREDS_FILE" ]; then
        echo "Credentials file already exists at $PROJECT_DIR/$CREDS_FILE"
        echo "Content of credentials file (first line):"
        head -n 1 "$PROJECT_DIR/$CREDS_FILE"
    else
        # Look for the file in the project directory
        if [ -f "$CREDS_FILE" ]; then
            echo "Copying credentials file to $PROJECT_DIR/$CREDS_FILE"
            cp "$CREDS_FILE" "$PROJECT_DIR/$CREDS_FILE"
        else
            echo "WARNING: Could not find credentials file at $CREDS_FILE"
            echo "Creating a sample credentials file with placeholders"
            echo "# Reddit API credentials" > "$PROJECT_DIR/$CREDS_FILE"
            echo "REDDIT_CLIENT_ID=your_client_id_here" >> "$PROJECT_DIR/$CREDS_FILE"
            echo "REDDIT_CLIENT_SECRET=your_client_secret_here" >> "$PROJECT_DIR/$CREDS_FILE"
            echo "REDDIT_USER_AGENT=Reddit Data Engineering Pipeline by u/yourusername" >> "$PROJECT_DIR/$CREDS_FILE"
        fi
    fi
    
    # Make sure the credentials file has the right permissions
    chmod 644 "$PROJECT_DIR/$CREDS_FILE"
    
    echo "Credential setup complete"
    """,
    dag=dag
)

# Create data directory if it doesn't exist
create_data_dir = BashOperator(
    task_id='create_data_dir',
    bash_command='mkdir -p /opt/airflow/data',
    dag=dag
)

# Extraction from Reddit
extract = PythonOperator(
    task_id='extract_reddit_data',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'dataengineering',
        'time_filter': 'day',
        'limit': 100
    },
    dag=dag
)

# Upload to S3
upload_s3 = PythonOperator(
    task_id='s3_upload',
    python_callable=upload_s3_pipeline,
    dag=dag
)

# Clean up old files
cleanup = BashOperator(
    task_id='cleanup_old_files',
    bash_command="""
    # Keep only the last 7 days of files
    find /opt/airflow/data -name "reddit_*.csv" -mtime +7 -delete
    """,
    dag=dag
)

# Define dependencies
setup_credentials >> create_data_dir >> extract >> upload_s3 >> cleanup

