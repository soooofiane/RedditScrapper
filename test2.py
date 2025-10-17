import os
import pandas as pd
import urllib.parse
import urllib.request
import xmltodict
from dotenv import load_dotenv
import praw

load_dotenv()

CSV_PATH = 'texts_dataset.csv'

# Structures globales
docs = []
posts = pd.DataFrame()

def load_from_csv(path=CSV_PATH):
    """Charge df et docs depuis le CSV si présent."""
    df = pd.read_csv(path, sep='\t', dtype={'id': int, 'text': str, 'source': str})
    docs_local = []
    for _, row in df.iterrows():
        docs_local.append({
            'text': row['text'] if pd.notna(row['text']) else '',
            'source': row['source'] if pd.notna(row['source']) else ''
        })
    return df, docs_local

def fetch_reddit(limit=10, subreddit_name_str='Basketball'):
    """Récupère les posts Reddit et ajoute à docs et posts DataFrame."""
    reddit = praw.Reddit(
        client_id=os.getenv("CLIENT_ID"),
        client_secret=os.getenv("CLIENT_SECRET"),
        password=os.getenv("PASSWORD"),
        user_agent=os.getenv("USER_AGENT"),
        username=os.getenv("USER_NAME"),
    )

    posts_list = []
    docs_local = []
    subreddit = reddit.subreddit(subreddit_name_str)
    for post in subreddit.hot(limit=limit):
        posts_list.append([
            post.title,
            post.score,
            post.id,
            str(post.subreddit),  # sous-reddit en texte
            post.url,
            post.num_comments,
            post.selftext.replace('\n', ' '),
            post.created
        ])
        docs_local.append({
            'text': post.selftext.replace('\n', ' '),
            'source': 'reddit'
        })

    posts_df = pd.DataFrame(posts_list, columns=[
        'title', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'created'
    ])
    return posts_df, docs_local

def fetch_arxiv(query='all:Basketball', start=0, max_results=10):
    """Interroge l'API arXiv et renvoie une liste de docs (text+source)."""
    base_url = 'http://export.arxiv.org/api/query'
    params = {
        'search_query': query,
        'start': start,
        'max_results': max_results
    }
    query_string = urllib.parse.urlencode(params)
    full_url = f'{base_url}?{query_string}'

    with urllib.request.urlopen(full_url) as response:
        xml_data = response.read()

    parsed = xmltodict.parse(xml_data)
    entries = parsed['feed'].get('entry', [])

    docs_local = []
    # entry peut être un dict si un seul résultat
    if isinstance(entries, dict):
        entries = [entries]

    for entry in entries:
        summary = entry.get('summary', '') if isinstance(entry, dict) else ''
        # nettoyer les nouvelles lignes
        summary = summary.replace('\n', ' ')
        docs_local.append({
            'text': summary,
            'source': 'arxiv'
        })

    return docs_local

def build_dataframe_from_docs(docs_list):
    """Construit un DataFrame avec colonnes id, text, source."""
    rows = []
    for i, doc in enumerate(docs_list, start=1):
        rows.append({
            'id': i,
            'text': doc.get('text', ''),
            'source': doc.get('source', '')
        })
    return pd.DataFrame(rows)

# --- logique principale ---
if os.path.exists(CSV_PATH):
    print(f"Fichier '{CSV_PATH}' trouvé — chargement depuis le disque (bypass des APIs).")
    df, docs = load_from_csv(CSV_PATH)
    print(f"Chargé {len(df)} entrées depuis '{CSV_PATH}'.")
else:
    print(f"Fichier '{CSV_PATH}' non trouvé — interrogation des APIs (Reddit + arXiv).")
    # 1) Reddit
    try:
        posts, docs_reddit = fetch_reddit(limit=10, subreddit_name_str='Basketball')
        print(f"Récupéré {len(docs_reddit)} posts depuis Reddit.")
    except Exception as e:
        print("Erreur lors de la récupération Reddit :", e)
        posts = pd.DataFrame()  # vide en fallback
        docs_reddit = []

    # 2) arXiv
    try:
        docs_arxiv = fetch_arxiv(query='all:Basketball', start=0, max_results=10)
        print(f"Récupéré {len(docs_arxiv)} entrées depuis arXiv.")
    except Exception as e:
        print("Erreur lors de la récupération arXiv :", e)
        docs_arxiv = []

    # Concat docs
    docs = docs_reddit + docs_arxiv

    # Construire DataFrame et sauvegarder
    df = build_dataframe_from_docs(docs)
    df.to_csv(CSV_PATH, sep='\t', index=False)
    print(f"Sauvegardé {len(df)} entrées dans '{CSV_PATH}'.")

# Affichage rapide pour vérification
print(df.head())
print("Nombre d'items dans docs :", len(docs))
# posts (Reddit) est défini seulement si on a fait l'appel; sinon DataFrame vide
try:
    print("Aperçu posts Reddit (si présent) :")
    print(posts.head())
except NameError:
    pass

# PARTIE 3

# 3.1
print("Taille du corpus :", len(df))

# 3.2
df['text'] = df['text'].fillna('')
df['nb_mots'] = df['text'].apply(lambda x: len(x.split()))
df['nb_phrases'] = df['text'].apply(lambda x: len(x.split('.')))
print(df[['id', 'nb_mots', 'nb_phrases']])

# 3.3
df = df[df['text'].str.len() >= 20].reset_index(drop=True)
docs = [{'text': row['text'], 'source': row['source']} for _, row in df.iterrows()]
print("Taille du corpus après suppression des petits documents :", len(df))

# 3.4
corpus_str = ' '.join(df['text'].tolist())
print("Corpus unique (extrait) :", corpus_str[:100])

