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

from classes.Document import DocumentFactory
from classes.Corpus import Corpus

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
        posts.append([post.title, post.score, post.id, str(post.subreddit), post.url, post.num_comments, post.selftext, post.created])
        docs.append({
            'title': post.title,
            'text': post.selftext.replace('\n', ' '),
            'source': 'reddit',
            'authors': [author_name] if author_name else [],
            'url': post.url,
            'created': post.created,
            'num_comments': post.num_comments
        })
    posts = pd.DataFrame(posts, columns=['title','score','id','subreddit','url','num_comments','body','created'])
except Exception as e:
    import traceback
    traceback.print_exc()
    print("Erreur lors de la récupération Reddit :", e)
    posts = pd.DataFrame()

# 1.2: FETCH WITH ARXIV

base_url = 'http://export.arxiv.org/api/query'
params = {
    'search_query': 'all:Basketball',
    'start': 0,
    'max_results': 10
}
query_string = urllib.parse.urlencode(params)
full_url = f'{base_url}?{query_string}'

# use certifi for SSL
ctx = ssl.create_default_context(cafile=certifi.where())
with urllib.request.urlopen(full_url, context=ctx) as response:
    xml_data = response.read()

parsed = xmltodict.parse(xml_data)
entries = parsed['feed'].get('entry', [])
if isinstance(entries, dict):
    entries = [entries]

for entry in entries:
    summary = entry.get('summary', '') if isinstance(entry, dict) else ''
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
        'title': entry.get('title', ''),
        'text': summary.replace('\n', ' '),
        'source': 'arxiv',
        'authors': names,
        'url': entry.get('id', ''),
        'created': entry.get('published', '')
    })

############ PARTIE 2 ################

rows = []
i = 1
for doc in docs:
    row = {
        'id': i,
        'title': doc.get('title', ''),
        'text': doc.get('text', ''),
        'source': doc.get('source', ''),
        'authors': doc.get('authors', []),
        'url': doc.get('url', ''),
        'created': doc.get('created', ''),
        'num_comments': doc.get('num_comments', 0)
    }
    rows.append(row)
    i += 1

df = pd.DataFrame(rows)

# Construire id2doc/id2aut (conserve structure existante)
id2doc = {}
id2aut = {}
for _, row in df.iterrows():
    doc_id = int(row['id'])
    authors = row.get('authors', [])
    if isinstance(authors, str):
        authors = authors.split('|') if authors else []
    id2doc[doc_id] = {
        'text': row.get('text', ''),
        'source': row.get('source', ''),
        'authors': authors
    }
    for a in authors:
        if a not in id2aut:
            id2aut[a] = {'name': a, 'doc_ids': [doc_id]}
        else:
            id2aut[a]['doc_ids'].append(doc_id)

print(f"Nombre de docs (id2doc): {len(id2doc)}")
print(f"Nombre d'auteurs uniques (id2aut): {len(id2aut)}")

# Sauvegarde CSV (authors => "a|b")
df_to_save = df.copy()
df_to_save['authors'] = df_to_save['authors'].apply(lambda lst: '|'.join(lst) if isinstance(lst, list) else (lst if pd.notna(lst) else ''))
df_to_save.to_csv('texts_dataset.csv', sep='\t', index=False)

# --- Nouveau : construire un Corpus avec des instances via DocumentFactory ---
corpus = Corpus('BasketballCorpus')

for _, row in df.iterrows():
    data = {
        'title': row.get('title', ''),
        'text': row.get('text', ''),
        'source': row.get('source', ''),
        'authors': row.get('authors', []),
        'url': row.get('url', ''),
        'created': row.get('created', ''),
        'num_comments': row.get('num_comments', 0)
    }
    doc_obj = DocumentFactory.create_from_dict(data)
    corpus.add_doc(doc_obj, doc_id=int(row['id']))

print(corpus)
print("Nombre de docs dans corpus :", corpus.ndoc)
print("Nombre d'auteurs dans corpus :", corpus.naut)

print("\nTop documents triés par date :")
corpus.show_by_date(5)

print("\nTop documents triés par titre :")
corpus.show_by_title(5)

# sauvegarder le corpus (ré-écrit texts_dataset.csv avec le champ 'source' mis à jour depuis doc.type)
corpus.save('texts_dataset.csv')




