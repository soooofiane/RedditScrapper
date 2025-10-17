import praw
from dotenv import load_dotenv
import os
import pandas as pd
import urllib.parse
import urllib.request
import xmltodict

load_dotenv()

docs = []

############ PARTIE 1 ################

# 1.1: FETCH WITH REDDIT 
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
    docs.append({
        'text': post.selftext.replace('\n', ' '),
        'source': 'reddit'
    })
posts = pd.DataFrame(posts,columns=['title', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'created'])
# print(posts)

# 1.2: FETCH WITH ARXIV

# Define base URL and parameters
base_url = 'http://export.arxiv.org/api/query'
params = {
    'search_query': 'all:Basketball',
    'start': 0,
    'max_results': 10
}

# Build full URL
query_string = urllib.parse.urlencode(params)
full_url = f'{base_url}?{query_string}'

# Send request and read response
with urllib.request.urlopen(full_url) as response:
    xml_data = response.read()

# Parse XML into Python dict
parsed = xmltodict.parse(xml_data)

# Access and print entries
entries = parsed['feed'].get('entry', [])

# Print basic info from each entry
for entry in entries:
    docs.append({
        'text': entry['summary'],
        'source': 'arxiv'
    })
    """
    print(f"Title: {entry['title'].strip()}")
    print(f"ID: {entry['id']}")
    print(f"Published: {entry['published']}")
    print(f"Summary: {entry['summary']}")  # First 100 chars
    print('-' * 60)
    """


print(docs)

############ PARTIE 2 ################

# Create an empty list to hold rows
rows = []

# Basic loop to build each row
i = 1
for doc in docs:
    row = {
        'id': i,
        'text': doc['text'],
        'source': doc['source']
    }
    rows.append(row)
    i += 1

# Create DataFrame from the list of rows
df = pd.DataFrame(rows)
df.to_csv('texts_dataset.csv', sep='\t', index=False)




