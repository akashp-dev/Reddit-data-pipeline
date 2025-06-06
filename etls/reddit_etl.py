import praw
from praw import Reddit 
import pandas as pd
import numpy as np
from utils.constants import POST_FIELDS

def connect_reddit(client_id, secret, user_agent) -> Reddit:
    try:
        reddit = praw.Reddit(client_id = client_id,
                             client_secret = secret,
                             user_agent = user_agent)
        print("Connected to Reddit!")
        return reddit
    except Exception as e:
        print(f"Error connecting to Reddit: {e}")
        raise e
    sys.exit(1)


def extract_posts(reddit_instance: Reddit, subreddit: str, time_filter: str, limit: int = None):
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    post_lists = []
   
    for post in posts:
        post_dict = vars(post)
        print(post_dict)
        post = {key: post_dict[key] for key in POST_FIELDS}
        post_lists.append(post)

    return post_lists


def transform_data(post_df: pd.DataFrame) -> pd.DataFrame:
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where(post_df['over_18'] == True, 1, 0)
    post_df['author'] = post_df['author'].astype(str)
    edited_mode = post_df['edited'].mode()[0]
    post_df['edited'] = np.where(post_df['edited'] == edited_mode, 1, 0)
    post_df['score'] = post_df['score'].astype(int)
    post_df['upvote_ratio'] = post_df['upvote_ratio'].astype(float)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['title'] = post_df['title'].astype(str)
    post_df['selftext'] = post_df['selftext'].astype(str)
    return post_df

def load_data_to_csv(data: pd.DataFrame, file_path: str):
    data.to_csv(file_path, index=False)


