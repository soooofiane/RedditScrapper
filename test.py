import praw
from dotenv import load_dotenv
import os
import pandas as pd
import urllib.parse
import urllib.request
import xmltodict
import ssl
import certifi
import requests

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

try:
    posts = []
    subreddit_name = reddit.subreddit('Basketball')
    for post in subreddit_name.hot(limit=10):
        try:
            author_name = post.author.name if (post.author is not None) else ''
        except Exception:
            author_name = ''
        posts.append([post.title, post.score, post.id, post.subreddit, post.url, post.num_comments, post.selftext, post.created])
        docs.append({
            'text': post.selftext.replace('\n', ' '),
            'source': 'reddit',
            'authors': [author_name] if author_name else []
        })
    posts = pd.DataFrame(posts, columns=['title','score','id','subreddit','url','num_comments','body','created'])
except Exception as e:
    import traceback
    traceback.print_exc()
    print("Erreur lors de la récupération Reddit :", e)
    posts = pd.DataFrame()

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

# Send request and read response (use certifi SSL context)
ctx = ssl.create_default_context(cafile=certifi.where())
with urllib.request.urlopen(full_url, context=ctx) as response:
    xml_data = response.read()

# Parse XML into Python dict
parsed = xmltodict.parse(xml_data)

# Access entries, s'assurer d'avoir une liste
entries = parsed['feed'].get('entry', [])
if isinstance(entries, dict):
    entries = [entries]

# Print basic info from each entry
for entry in entries:
    # extraire résumé
    summary = entry.get('summary', '') if isinstance(entry, dict) else ''
    # extraire auteurs (peut être dict ou liste)
    authors_field = entry.get('author', [])
    if isinstance(authors_field, dict):
        authors_field = [authors_field]
    names = []
    if isinstance(authors_field, list):
        for a in authors_field:
            if isinstance(a, dict):
                name = a.get('name', '')
                if name:
                    names.append(name.strip())
    docs.append({
        'text': summary.replace('\n', ' '),
        'source': 'arxiv',
        'authors': names
    })

# compact display
# print(docs)

############ PARTIE 2 ################

# Create an empty list to hold rows
rows = []

# Basic loop to build each row (inclut authors)
i = 1
for doc in docs:
    row = {
        'id': i,
        'text': doc.get('text', ''),
        'source': doc.get('source', ''),
        'authors': doc.get('authors', [])  # stocker en liste pour usage interne
    }
    rows.append(row)
    i += 1

# Create DataFrame from the list of rows
df = pd.DataFrame(rows)

# Construire id2doc et id2aut
id2doc = {}
id2aut = {}

for _, row in df.iterrows():
    doc_id = int(row['id'])
    authors = row.get('authors', [])
    # si, par hasard, la colonne authors est une chaîne (ex: lors d'un reload), la convertir en liste
    if isinstance(authors, str):
        authors = authors.split('|') if authors else []
    id2doc[doc_id] = {
        'text': row.get('text', ''),
        'source': row.get('source', ''),
        'authors': authors
    }
    for a in authors:
        if a not in id2aut:
            # instance minimale d'auteur : nom + liste des ids de documents
            id2aut[a] = {'name': a, 'doc_ids': [doc_id]}
        else:
            id2aut[a]['doc_ids'].append(doc_id)

print(f"Nombre de docs (id2doc): {len(id2doc)}")
print(f"Nombre d'auteurs uniques (id2aut): {len(id2aut)}")

# Avant de sauvegarder, convertir authors en chaîne séparée par '|' pour le CSV
df_to_save = df.copy()
df_to_save['authors'] = df_to_save['authors'].apply(lambda lst: '|'.join(lst) if isinstance(lst, list) else (lst if pd.notna(lst) else ''))

df_to_save.to_csv('texts_dataset.csv', sep='\t', index=False)




