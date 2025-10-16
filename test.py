import praw
from dotenv import load_dotenv
import os

load_dotenv()

reddit = praw.Reddit(
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    password=os.getenv("PASSWORD"),
    user_agent=os.getenv("USER_AGENT"),
    username=os.getenv("USER_NAME"),
)

# print(reddit.user.me())

# get 10 hot posts from the MachineLearning subreddit
hot_posts = reddit.subreddit('MachineLearning').hot(limit=10)
for post in hot_posts:
    print(post.title)

