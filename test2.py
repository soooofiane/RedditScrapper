import os
import pandas as pd
import urllib.parse
import urllib.request
import xmltodict
from dotenv import load_dotenv
import praw
from datetime import datetime
from classes.Document import Document
from classes.Author import Author
from classes.Corpus import Corpus

load_dotenv()

CSV_PATH = 'texts_dataset.csv'

# Structures globales
docs = []
posts = pd.DataFrame()

def load_from_csv(path=CSV_PATH):
    """Charge df et docs depuis le CSV si présent."""
    df = pd.read_csv(path, sep='\t', dtype=str)
    # s'assurer que l'id est entier si présent
    if 'id' in df.columns:
        df['id'] = df['id'].astype(int)
    # normaliser la colonne authors (stockée comme "a|b|c" ou vide)
    if 'authors' in df.columns:
        df['authors'] = df['authors'].fillna('').apply(lambda s: s.split('|') if s != '' else [])
    else:
        df['authors'] = [[] for _ in range(len(df))]
    docs_local = []
    for _, row in df.iterrows():
        docs_local.append({
            'text': row['text'] if pd.notna(row['text']) else '',
            'source': row['source'] if pd.notna(row['source']) else '',
            'authors': row['authors'] if row['authors'] is not None else []
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
        author_name = ''
        try:
            if getattr(post, 'author') and post.author is not None:
                author_name = getattr(post.author, 'name', '') or ''
        except Exception:
            author_name = ''
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
            'source': 'reddit',
            'authors': [author_name] if author_name else []
        })

    posts_df = pd.DataFrame(posts_list, columns=[
        'title', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'created'
    ])
    return posts_df, docs_local

def fetch_arxiv(query='all:Basketball', start=0, max_results=10):
    """Interroge l'API arXiv et renvoie une liste de docs (text+source+authors)."""
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
        # extraire auteurs (peut être dict ou liste)
        authors_field = entry.get('author', [])
        names = []
        if isinstance(authors_field, list):
            for a in authors_field:
                if isinstance(a, dict):
                    name = a.get('name', '')
                    if name:
                        names.append(name)
        elif isinstance(authors_field, dict):
            name = authors_field.get('name', '')
            if name:
                names.append(name)

        docs_local.append({
            'text': summary,
            'source': 'arxiv',
            'authors': names
        })

    return docs_local

def build_dataframe_from_docs(docs_list):
    """Construit un DataFrame avec colonnes id, text, source, authors (authors stockés en "a|b")."""
    rows = []
    for i, doc in enumerate(docs_list, start=1):
        authors_list = doc.get('authors', []) or []
        # stocker authors sous forme de chaîne séparée par '|' pour le CSV
        authors_str = '|'.join(authors_list)
        rows.append({
            'id': i,
            'text': doc.get('text', ''),
            'source': doc.get('source', ''),
            'authors': authors_str
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

# Après avoir construit/chargé df, créer et remplir le Corpus
corpus = Corpus('BasketballCorpus')

# si df contient des lignes, instancier Document et ajouter au corpus en conservant l'id
for _, row in df.iterrows():
    doc_id = int(row['id'])
    texte = row.get('text', '') or ''
    titre = row.get('title', '') if 'title' in df.columns else ''
    url = row.get('url', '') if 'url' in df.columns else ''
    created = row.get('created', '') if 'created' in df.columns else ''
    # auteurs : peut être liste (load_from_csv) ou chaîne "a|b"
    authors = row.get('authors', [])
    if isinstance(authors, list):
        auteur_str = '|'.join(authors)
    else:
        auteur_str = authors or ''
    doc_obj = Document(titre=titre, auteur=auteur_str, date=str(created), url=url, texte=texte)
    corpus.add_doc(doc_obj, doc_id=doc_id)

print(corpus)  # utilise __repr__

# Affichage rapide pour vérification
print(df.head())
print("Nombre d'items dans docs (raw list) :", len(docs))
print("Nombre de docs dans corpus :", corpus.ndoc)
print("Nombre d'auteurs dans corpus :", corpus.naut)

# Exemple d'utilisation des méthodes d'affichage
print("\nTop documents triés par date :")
corpus.show_by_date(5)

print("\nTop documents triés par titre :")
corpus.show_by_title(5)

# Sauvegarde du corpus
corpus.save(CSV_PATH)

# --- Statistiques par auteur (interactif) ---
def author_stats(author_name, corpus):
    """Retourne (nb_docs, avg_words) pour un auteur donné."""
    author_obj = corpus.authors.get(author_name)
    if author_obj is None:
        return None
    doc_ids = list(author_obj.production.keys())
    nb_docs = author_obj.ndoc
    if nb_docs == 0:
        return (0, 0.0)
    total_words = 0
    for did in doc_ids:
        doc_obj = author_obj.production.get(did) or corpus.id2doc.get(did)
        text = ''
        if doc_obj is not None:
            text = getattr(doc_obj, 'texte', '') or getattr(doc_obj, 'text', '') or ''
        total_words += len(text.split())
    avg_words = total_words / nb_docs if nb_docs > 0 else 0.0
    return (nb_docs, avg_words)

# boucle interactive simple
try:
    while True:
        name = input("\nEntrez le nom de l'auteur (ou 'q' pour quitter) : ").strip()
        if name.lower() in ('q', 'quit', 'exit', ''):
            print("Fin des requêtes.")
            break
        res = author_stats(name, corpus)
        if res is None:
            print(f"Auteur '{name}' introuvable dans le corpus.")
        else:
            nb_docs, avg_words = res
            print(f"Auteur : {name}")
            print(f"  - Nombre de documents : {nb_docs}")
            print(f"  - Taille moyenne des documents (en mots) : {avg_words:.2f}")
except KeyboardInterrupt:
    print("\nInterrompu par l'utilisateur.")

