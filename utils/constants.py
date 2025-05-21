import os
import configparser

from configupdater import Section

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))

SECRET = parser.get('api_keys','reddit_secret_key')
CLIENT_ID = parser.get('api_keys','reddit_client_id')

DATABASE_HOST = parser.get('database', 'database_host')
DATABASE_NAME = parser.get( 'database', 'database_name')
DATABASE_PORT = parser.get('database', 'database_port')
DATABASE_USERNAME = parser.get('database','database_username')
DATABASE_PASSWORD = parser.get( 'database','database_password')

#AWS
AWS_ACCESS_KEY_ID = parser.get('aws', 'aws_access_key_id')
AWS_SECRET_ACCESS_KEY = parser.get('aws', 'aws_secret_access_key')
AWS_SESSION_TOKEN = parser.get('aws', 'aws_session_token')
AWS_REGION = parser.get('aws', 'aws_region')
AWS_BUCKET_NAME = parser.get('aws', 'aws_bucket_name')


INPUT_PATH = parser.get('file_paths', 'input_path')
OUTPUT_PATH = parser.get('file_paths','output_path')

POST_FIELDS = ['id', 'title', 'selftext', 'subreddit', 'url', 'num_comments', 'score', 'upvote_ratio', 'created_utc', 'author', 'upvote_ratio','over_18', 'spoiler','stickied','edited','link_flair_text','author_flair_text','author_flair_css_class','author_flair_background_color','author_flair_text_color','author_flair_template_id','author_flair_type','author_flair_css_class','author_flair_background_color','author_flair_text_color','author_flair_template_id','author_flair_type']

