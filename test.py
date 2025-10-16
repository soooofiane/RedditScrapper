import praw
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

docs = []

reddit = praw.Reddit(
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    password=os.getenv("PASSWORD"),
    user_agent=os.getenv("USER_AGENT"),
    username=os.getenv("USER_NAME"),
)

# print(reddit.user.me())

posts = []
subreddit_name = reddit.subreddit('Basketball')
for post in subreddit_name.hot(limit=10):
    posts.append([post.title, post.score, post.id, post.subreddit, post.url, post.num_comments, post.selftext, post.created])
    docs.append(post.selftext.replace('\n', ' '))
posts = pd.DataFrame(posts,columns=['title', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'created'])
# print(posts)
print(docs)


