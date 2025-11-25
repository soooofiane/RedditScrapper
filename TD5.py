import os
import pandas as pd
import urllib.parse
import urllib.request
import xmltodict
from datetime import datetime
from dotenv import load_dotenv
import praw

from classes.Document import Document, RedditDocument, ArxivDocument
from classes.Corpus import Corpus
from classes.DocumentFactory import DocumentFactory

load_dotenv()

JSON_PATH = 'corpus.json'

# Structures globales
# Utilisation du Singleton pour obtenir l'instance unique du corpus ---
corpus = Corpus.getInstance("RedditScrapper")
posts = pd.DataFrame()

def parse_date(value):
    # --- Tente de convertir une chaîne stockée en datetime ---
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    value_str = str(value).strip()
    if not value_str:
        return None
    try:
        if value_str.endswith('Z'):
            value_str = value_str.replace('Z', '+00:00')
        return datetime.fromisoformat(value_str)
    except ValueError:
        try:
            return datetime.strptime(value_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return value_str


def build_dataframe_from_corpus(corpus_obj):
    # --- Construit un DataFrame à partir du corpus pour compatibilité ---
    rows = []
    for doc_id in sorted(corpus_obj.id2doc.keys()):
        doc = corpus_obj.id2doc[doc_id]
        rows.append({
            'id': doc_id,
            'auteur': doc.auteur or '',
            'source': doc.source or '',
            'date': format_date_for_csv(doc.date),
            'text': doc.texte
        })
    return pd.DataFrame(rows, columns=['id', 'auteur', 'source', 'date', 'text'])

def fetch_reddit(limit=10, subreddit_name_str='Basketball'):
    # --- Récupère les posts Reddit et renvoie des objets Document ---
    reddit = praw.Reddit(
        client_id=os.getenv("CLIENT_ID"),
        client_secret=os.getenv("CLIENT_SECRET"),
        password=os.getenv("PASSWORD"),
        user_agent=os.getenv("USER_AGENT"),
        username=os.getenv("USER_NAME"),
    )

    posts_list = []
    documents = []
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
        texte = post.selftext.replace('\n', ' ')
        # --- Utilisation de la Factory pour créer un RedditDocument ---
        doc = DocumentFactory.create_document(
            source='reddit',
            titre=post.title,
            auteur=str(post.author) if post.author else 'inconnu',
            date=datetime.fromtimestamp(post.created),
            url=post.url,
            texte=texte,
            nb_commentaires=post.num_comments
        )
        documents.append(doc)

    posts_df = pd.DataFrame(posts_list, columns=[
        'title', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'created'
    ])
    return posts_df, documents

def fetch_arxiv(query='all:Basketball', start=0, max_results=10):
    # --- Interroge l'API arXiv et renvoie une liste de Document ---
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

    documents = []
    # entry peut être un dict si un seul résultat
    if isinstance(entries, dict):
        entries = [entries]

    for entry in entries:
        summary = entry.get('summary', '') if isinstance(entry, dict) else ''
        # nettoyer les nouvelles lignes
        summary = summary.replace('\n', ' ')
        title = entry.get('title', 'arXiv entry').strip()
        published = entry.get('published')
        try:
            date_pub = datetime.strptime(published, "%Y-%m-%dT%H:%M:%SZ") if published else None
        except ValueError:
            date_pub = None
        author_field = entry.get('author', {})
        co_auteurs = []
        if isinstance(author_field, list):
            co_auteurs = [a.get('name', '') for a in author_field if isinstance(a, dict) and a.get('name')]
        elif isinstance(author_field, dict):
            nom_auteur = author_field.get('name', '')
            if nom_auteur:
                co_auteurs = [nom_auteur]
        
        # Premier auteur pour le champ auteur (compatibilité)
        auteur = co_auteurs[0] if co_auteurs else 'arxiv'
        
        url = entry.get('id', '')
        # --- Utilisation de la Factory pour créer un ArxivDocument ---
        doc = DocumentFactory.create_document(
            source='arxiv',
            titre=title,
            auteur=auteur,
            date=date_pub,
            url=url,
            texte=summary,
            co_auteurs=co_auteurs
        )
        documents.append(doc)

    return documents

def format_date_for_csv(value):
    # --- Convertit datetime vers une chaîne ISO pour stockage ---
    if isinstance(value, datetime):
        return value.isoformat()
    return value if value else ''


def format_date_for_display(value):
    # --- Retourne une représentation lisible de la date ---
    if isinstance(value, datetime):
        return value.isoformat(sep=' ', timespec='seconds')
    return str(value) if value else 'n/a'


def show_author_stats(name):
    # --- Affiche des statistiques simples pour un auteur donné ---
    author = corpus.authors.get(name)
    if not author:
        print(f"Auteur '{name}' inconnu.")
        return
    nb_docs = author.ndoc
    if nb_docs == 0:
        print(f"{author.name} n'a enregistré aucun document.")
        return
    avg_size = sum(len(doc.texte) for doc in author.production.values()) / nb_docs
    print(f"\nAuteur : {author.name}")
    print(f"Documents publiés : {nb_docs}")
    print(f"Taille moyenne des documents : {avg_size:.2f} caractères")

    def sort_key(item):
        doc = item[1]
        value = doc.date
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            converted = parse_date(value)
            if isinstance(converted, datetime):
                return converted
        return datetime.min

    print("Documents associés :")
    for doc_id, doc in sorted(author.production.items(), key=sort_key, reverse=True)[:5]:
        print(
            f"  - [{doc_id}] {doc.titre} | source: {doc.source} | "
            f"date: {format_date_for_display(doc.date)}"
        )


# --- logique principale ---
if os.path.exists(JSON_PATH):
    print(f"Fichier '{JSON_PATH}' trouvé — chargement depuis le disque (bypass des APIs).")
    corpus.load(JSON_PATH)
    df = build_dataframe_from_corpus(corpus)
else:
    print(f"Fichier '{JSON_PATH}' non trouvé — interrogation des APIs (Reddit + arXiv).")
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

    for document in docs_reddit + docs_arxiv:
        corpus.register_document(document)

    # Sauvegarder le corpus
    corpus.save(JSON_PATH)
    df = build_dataframe_from_corpus(corpus)

# Affichage rapide pour vérification
print(df.head())
print("Nombre d'items dans id2doc :", len(corpus.id2doc))
# posts (Reddit) est défini seulement si on a fait l'appel; sinon DataFrame vide
try:
    print("Aperçu posts Reddit (si présent) :")
    print(posts.head())
except NameError:
    pass


print("Taille du corpus :", len(df))


df['text'] = df['text'].fillna('')
df['nb_mots'] = df['text'].apply(lambda x: len(x.split()))
df['nb_phrases'] = df['text'].apply(lambda x: len(x.split('.')))
print(df[['id', 'nb_mots', 'nb_phrases']])


df = df[df['text'].str.len() >= 20].reset_index(drop=True)
print("Taille du corpus après suppression des petits documents :", len(df))


corpus_str = ' '.join(df['text'].tolist())
print("Corpus unique (extrait) :", corpus_str[:100])

print("Top 3 documents (date décroissante) :")
corpus.show_by_date(limit=3)
print("Top 3 documents (ordre alphabétique) :")
corpus.show_by_title(limit=3)

# --- Affichage des documents par source (Partie 3.2) ---
corpus.afficher_documents_par_source()

if corpus.authors:
    print("Auteurs disponibles :", ', '.join(sorted(corpus.authors.keys())))
else:
    print("Aucun auteur enregistré pour l'instant.")

author_query = input("Entrez un nom d'auteur pour afficher ses statistiques (laisser vide pour ignorer) : ").strip()
if author_query:
    show_author_stats(author_query)

