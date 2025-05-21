import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import praw
import pandas as pd
import json
from dotenv import load_dotenv

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
try:
    load_dotenv("/opt/airflow/reddit_credentials.env")
    print("Loaded environment variables from /opt/airflow/reddit_credentials.env")
except Exception as e:
    print(f"Error loading environment variables: {e}")
    
# Print environment variables for debugging (masked)
client_id = os.getenv('REDDIT_CLIENT_ID')
client_secret = os.getenv('REDDIT_CLIENT_SECRET')
user_agent = os.getenv('REDDIT_USER_AGENT', 'script by u/yourusername')

print(f"Reddit Client ID: {client_id[:4] + '*****' if client_id else 'Not set'}")
print(f"Reddit Client Secret: {'*****' if client_secret else 'Not set'}")
print(f"Reddit User Agent: {user_agent if user_agent else 'Not set'}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'reddit_direct_extraction',  # Changed to avoid conflicts
    default_args=default_args,
    description='Extract data from Reddit using PRAW',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def extract_reddit_data(**kwargs):
    """
    Extract data from Reddit using PRAW.
    """
    # Reddit API credentials
    client_id = os.getenv('REDDIT_CLIENT_ID')
    client_secret = os.getenv('REDDIT_CLIENT_SECRET')
    user_agent = os.getenv('REDDIT_USER_AGENT', 'script by u/yourusername')
    
    if not all([client_id, client_secret]):
        error_msg = "Missing Reddit API credentials. Check reddit_credentials.env file."
        print(error_msg)
        raise ValueError(error_msg)
    
    print(f"Initializing Reddit client with: client_id={client_id[:4]}***, user_agent={user_agent}")
    
    # Initialize PRAW Reddit instance
    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
    )
    
    # Verify connection
    try:
        # This will fail if credentials are wrong
        print("Testing Reddit API connection...")
        reddit.user.me()
        print("Successfully connected to Reddit API")
    except Exception as e:
        error_msg = f"Failed to connect to Reddit API: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)
    
    # Subreddits to extract data from
    subreddits = ['dataengineering', 'datascience', 'bigdata']
    
    all_posts = []
    
    for subreddit_name in subreddits:
        subreddit = reddit.subreddit(subreddit_name)
        
        # Get top posts from the past day
        for post in subreddit.top('day', limit=25):
            post_data = {
                'id': post.id,
                'title': post.title,
                'score': post.score,
                'author': str(post.author),
                'num_comments': post.num_comments,
                'created_utc': datetime.fromtimestamp(post.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                'url': post.url,
                'selftext': post.selftext,
                'subreddit': subreddit_name,
                'upvote_ratio': post.upvote_ratio
            }
            all_posts.append(post_data)
    
    # Convert to DataFrame
    df = pd.DataFrame(all_posts)
    
    # Save to CSV
    output_dir = '/opt/airflow/data'
    os.makedirs(output_dir, exist_ok=True)
    output_file = f"{output_dir}/reddit_data_{datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(output_file, index=False)
    
    print(f"Extracted {len(all_posts)} posts from Reddit subreddits: {', '.join(subreddits)}")
    print(f"Data saved to {output_file}")
    
    return output_file

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_reddit_data',
    python_callable=extract_reddit_data,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task 