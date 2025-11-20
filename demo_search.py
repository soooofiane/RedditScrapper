import os
import pandas as pd
from classes.Corpus import Corpus
from classes.Document import DocumentFactory
from classes.SearchEngine import SearchEngine

CSV = 'texts_dataset.csv'

# charge le CSV (ou stoppe)
if not os.path.exists(CSV):
    print(f"Fichier absent: {CSV}. Exécutez test.py pour générer le dataset.")
    raise SystemExit(1)

# charge corpus depuis le CSV existant (utilise DocumentFactory dans load)
corpus = Corpus.load(CSV, nom='demo_from_csv')
print(corpus)
print("docs:", corpus.ndoc)

# crée le moteur et cherche
engine = SearchEngine(corpus, use_tfidf=True)
q = "Basketball"
print(f"Recherche demo pour: {q}")
res = engine.search(q, topk=10)
print(res.to_string(index=False) if not res.empty else "Aucun résultat pertinent.")