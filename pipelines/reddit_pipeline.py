import os
import praw
import pandas as pd
import datetime as dt
import time
import json
import re
from dotenv import load_dotenv
from utils.constants import CLIENT_ID, SECRET, POST_FIELDS

def reddit_pipeline(file_name, subreddit, time_filter, limit):
    """
    Extract data from Reddit using PRAW
    
    Args:
        file_name (str): Name of the output file
        subreddit (str): Subreddit to extract data from
        time_filter (str): Time filter ('day', 'week', 'month', 'year', 'all')
        limit (int): Number of posts to extract
        
    Returns:
        str: Status message
    """
    print("Starting Reddit data extraction pipeline...")
    
    try:
        # Initialize PRAW
        reddit = praw.Reddit(
            client_id=CLIENT_ID,
            client_secret=SECRET,
            user_agent="DataEngineeringPipeline/1.0 (Educational Project)"
        )
        
        print(f"Attempting to connect to subreddit: r/{subreddit}")
        
        # Get subreddit instance
        subreddit_instance = reddit.subreddit(subreddit)
        
        # Get top posts
        posts = []
        for post in subreddit_instance.top(time_filter=time_filter, limit=limit):
            try:
                # Clean the text fields
                title = clean_text(post.title)
                selftext = clean_text(post.selftext[:500]) if post.selftext else ''
                
                post_data = {
                    'id': post.id,
                    'title': title,
                    'score': post.score,
                    'num_comments': post.num_comments,
                    'author': str(post.author),
                    'created_utc': dt.datetime.fromtimestamp(post.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                    'url': post.url,
                    'selftext': selftext,
                    'upvote_ratio': post.upvote_ratio,
                    'subreddit': subreddit,
                    'over_18': post.over_18,
                    'spoiler': post.spoiler,
                    'stickied': post.stickied,
                    'edited': bool(post.edited),
                    'link_flair_text': post.link_flair_text,
                    'author_flair_text': post.author_flair_text,
                    'author_flair_css_class': post.author_flair_css_class,
                    'author_flair_background_color': post.author_flair_background_color,
                    'author_flair_text_color': post.author_flair_text_color,
                    'author_flair_template_id': post.author_flair_template_id,
                    'author_flair_type': post.author_flair_type
                }
                
                # Only include fields that exist in POST_FIELDS
                filtered_post = {k: v for k, v in post_data.items() if k in POST_FIELDS}
                posts.append(filtered_post)
                
            except Exception as post_error:
                print(f"Error processing post {post.id}: {str(post_error)}")
                continue
            
            # Add a small delay to respect rate limits
            time.sleep(0.1)
        
        print(f"Successfully processed {len(posts)} posts from Reddit")
        
        # Convert to DataFrame
        df = pd.DataFrame(posts)
        
        # Create data directory if it doesn't exist
        data_dir = '/opt/airflow/data'
        os.makedirs(data_dir, exist_ok=True)
        
        # Save data to CSV with proper encoding and quoting
        output_file = f"{data_dir}/{file_name}.csv"
        df.to_csv(output_file, index=False, encoding='utf-8', quoting=1)  # quoting=1 means QUOTE_ALL
        
        print(f"Data saved to {output_file}")
        
        return output_file
    
    except Exception as e:
        error_message = f"Error in Reddit pipeline: {str(e)}"
        print(error_message)
        raise Exception(error_message)

def clean_text(text):
    """
    Clean text for CSV export by removing newlines and other problematic characters.
    
    Args:
        text (str): Text to clean
        
    Returns:
        str: Cleaned text
    """
    if not text:
        return ""
    
    # Replace newlines with spaces
    text = text.replace('\n', ' ').replace('\r', ' ')
    
    # Replace multiple spaces with a single space
    text = re.sub(r'\s+', ' ', text)
    
    # Remove any other special characters that might cause issues
    text = re.sub(r'[^\w\s.,;:!?()[\]{}\'\"+-=*/&%$#@^<>]', '', text)
    
    return text.strip()






