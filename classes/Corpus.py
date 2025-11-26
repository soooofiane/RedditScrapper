import os
import re
import string
import math
import numpy as np
import pandas as pd
from datetime import datetime
from scipy.sparse import csr_matrix

from classes.Author import Author
from classes.Document import Document


class Corpus:
    # --- Variable de classe pour stocker l'instance unique (Singleton) ---
    _instance = None
    _initialized = False
    
    def __new__(cls, nom=None):
        # --- Crée une seule instance (Singleton) ---
        if cls._instance is None:
            cls._instance = super(Corpus, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, nom=None):
        # --- Initialise une seule fois (Singleton) ---
        if not Corpus._initialized:
            self.nom = nom if nom else "Corpus"
            self.authors = {}
            self.id2doc = {}
            self.ndoc = 0
            self.naut = 0
            self.next_doc_id = 1
            self.corpus_text = None
            Corpus._initialized = True
    
    @classmethod
    def getInstance(cls, nom=None):
        # --- Méthode statique pour obtenir l'instance unique ---
        if cls._instance is None:
            cls._instance = cls(nom)
        return cls._instance

    def register_document(self, doc, doc_id=None):
        # --- Ajoute un document et met à jour les auteurs ---
        if doc_id is None:
            doc_id = self.next_doc_id
            self.next_doc_id += 1
        else:
            self.next_doc_id = max(self.next_doc_id, doc_id + 1)
        self.id2doc[doc_id] = doc
        self.ndoc = len(self.id2doc)
        author = self.get_or_create_author(doc.auteur)
        author.add(doc_id, doc)
        self.naut = len(self.authors)
        # Invalider le cache de la chaîne concaténée ---
        self.corpus_text = None
        return doc_id

    def get_or_create_author(self, name):
        # --- Retourne un auteur existant ou l'initialise ---
        if not name:
            name = 'inconnu'
        if name not in self.authors:
            self.authors[name] = Author(name=name)
        return self.authors[name]

    def show_by_date(self, limit=5):
        # --- Affiche les documents triés par date décroissante ---
        def normalize_date(doc):
            value = doc.date
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                try:
                    value_str = value.replace('Z', '+00:00') if value.endswith('Z') else value
                    return datetime.fromisoformat(value_str)
                except ValueError:
                    try:
                        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        return datetime.min
            return datetime.min

        sorted_docs = sorted(
            self.id2doc.items(),
            key=lambda item: normalize_date(item[1]),
            reverse=True,
        )
        for doc_id, doc in sorted_docs[:limit]:
            print(f"[{doc_id}] {doc.date} — {doc.titre}")

    def show_by_title(self, limit=5):
        # --- Affiche les documents triés par titre ---
        sorted_docs = sorted(
            self.id2doc.items(), key=lambda item: item[1].titre or ''
        )
        for doc_id, doc in sorted_docs[:limit]:
            print(f"[{doc_id}] {doc.titre}")

    def __repr__(self):
        return (
            f"Corpus '{self.nom}' — {self.ndoc} document(s), "
            f"{self.naut} auteur(s)"
        )
    
    def afficher_documents_par_source(self):
        # --- Affiche la liste des articles avec leur source (Reddit ou Arxiv) ---
        print(f"\nListe des documents dans le corpus '{self.nom}':")
        print("-" * 80)
        for doc_id in sorted(self.id2doc.keys()):
            doc = self.id2doc[doc_id]
            doc_type = doc.getType()
            print(f"[{doc_id}] {doc.titre} — Source: {doc_type}")
        print("-" * 80)
        print(f"Total: {self.ndoc} document(s)")
    
    def build_corpus_text(self):
        # --- Construit la chaîne concaténée une seule fois (lazy loading) ---
        if self.corpus_text is None:
            self.corpus_text = ' '.join(
                doc.texte for doc in self.id2doc.values() if doc.texte
            )
        return self.corpus_text
    
    def search(self, mot_cle):
        # --- Recherche les passages contenant le mot-clé dans le corpus ---
        # Construit la chaîne concaténée si nécessaire (une seule fois)
        corpus_text = self.build_corpus_text()
        
        if not corpus_text:
            return []
        
        # Utilise re pour chercher le mot-clé (insensible à la casse)
        pattern = re.compile(re.escape(mot_cle), re.IGNORECASE)
        matches = []
        
        # Trouve toutes les occurrences avec contexte
        for match in pattern.finditer(corpus_text):
            start = max(0, match.start() - 50)  
            end = min(len(corpus_text), match.end() + 50)
            passage = corpus_text[start:end]
            matches.append(passage)
        
        return matches
    
    def concorde(self, expression, taille_contexte=30):
        # --- Construit un concordancier pour une expression donnée ---
        # Construit la chaîne concaténée si nécessaire (une seule fois)
        corpus_text = self.build_corpus_text()
        
        if not corpus_text:
            return pd.DataFrame(columns=['contexte gauche', 'motif trouvé', 'contexte droit'])
        
        # Utilise re pour chercher l'expression (insensible à la casse)
        pattern = re.compile(re.escape(expression), re.IGNORECASE)
        resultats = []
        
        # Trouve toutes les occurrences avec contexte gauche et droit
        for match in pattern.finditer(corpus_text):
            # Contexte gauche
            start_gauche = max(0, match.start() - taille_contexte)
            contexte_gauche = corpus_text[start_gauche:match.start()]
            
            # Motif trouvé
            motif_trouve = match.group()
            
            # Contexte droit
            end_droit = min(len(corpus_text), match.end() + taille_contexte)
            contexte_droit = corpus_text[match.end():end_droit]
            
            resultats.append({
                'contexte gauche': contexte_gauche,
                'motif trouvé': motif_trouve,
                'contexte droit': contexte_droit
            })
        
        # Créer et retourner un DataFrame pandas
        df = pd.DataFrame(resultats)
        return df
    
    def nettoyer_texte(self, texte):
        # --- Nettoie une chaîne de caractères ---
        texte = texte.lower()
        
        # Remplacement des passages à la ligne \n par des espaces
        texte = texte.replace('\n', ' ')
        texte = texte.replace('\r', ' ')
        texte = texte.replace('\t', ' ')
        
        # Suppression de la ponctuation et des chiffres avec regex
        # Garde uniquement les lettres et les espaces
        texte = re.sub(r'[^a-zàâäéèêëïîôùûüÿç\s]', ' ', texte)
        
        # Normaliser les espaces multiples en un seul espace
        texte = re.sub(r'\s+', ' ', texte)
        
        # Supprimer les espaces en début et fin
        texte = texte.strip()
        
        return texte
    
    def stats(self):
        # --- Construit le vocabulaire et compte les occurrences en une seule passe ---
        frequences = {}  # Dictionnaire pour compter les occurrences (term frequency)
        doc_frequences = {}  # Dictionnaire pour compter les documents contenant chaque mot (document frequency)
        
        for doc in self.id2doc.values():
            if doc.texte:
                # Nettoie le texte
                texte_nettoye = self.nettoyer_texte(doc.texte)
                
                # Créer une regex qui split sur espace, tabulation et ponctuation
                delimiters = r'[\s' + re.escape(string.punctuation) + r']+'
                mots = re.split(delimiters, texte_nettoye)
                
                # Set pour suivre les mots uniques dans ce document
                mots_dans_doc = set()
                
                # Compter les occurrences directement
                for mot in mots:
                    if mot:
                        frequences[mot] = frequences.get(mot, 0) + 1
                        mots_dans_doc.add(mot)
                
                # Pour chaque mot unique dans ce document, incrémenter la document frequency
                for mot in mots_dans_doc:
                    doc_frequences[mot] = doc_frequences.get(mot, 0) + 1
        
        # Créer un DataFrame pandas avec les fréquences
        freq = pd.DataFrame(list(frequences.items()), columns=['mot', 'frequence'])
        
        # Ajouter la colonne document frequency
        freq['document_frequency'] = freq['mot'].map(doc_frequences)
        
        freq = freq.sort_values('frequence', ascending=False).reset_index(drop=True)
        
        # Afficher le nombre de mots différents dans le corpus
        nb_mots_differents = len(freq)
        print(f"\nNombre de mots différents dans le corpus : {nb_mots_differents}")
        
        return freq
    
    def format_date_for_csv(self, value):
        # --- Convertit datetime vers une chaîne ISO pour stockage ---
        if isinstance(value, datetime):
            return value.isoformat()
        return value if value else ''

    def parse_date(self, value):
        # --- Tente de convertir une chaîne stockée en datetime ---
        if value is None or (isinstance(value, float) and str(value).lower() == 'nan'):
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

    def save(self, path='corpus.json'):
        # --- Enregistre le corpus sur le disque dur en format JSON ---
        import json
        data = {
            'nom': self.nom,
            'documents': {}
        }
        for doc_id, doc in self.id2doc.items():
            doc_data = {
                'titre': doc.titre,
                'auteur': doc.auteur,
                'source': doc.source,
                'date': self.format_date_for_csv(doc.date),
                'url': doc.url,
                'texte': doc.texte
            }
            # Ajouter les attributs spécifiques des sous-classes
            if hasattr(doc, 'nb_commentaires'):
                doc_data['nb_commentaires'] = doc.nb_commentaires
            if hasattr(doc, 'co_auteurs'):
                doc_data['co_auteurs'] = doc.co_auteurs
            data['documents'][str(doc_id)] = doc_data
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Corpus sauvegardé dans '{path}' ({self.ndoc} documents).")

    def load(self, path='corpus.json'):
        # --- Charge le corpus depuis le disque dur en format JSON ---
        if not os.path.exists(path):
            print(f"Fichier '{path}' non trouvé.")
            return False

        import json
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        self.nom = data.get('nom', self.nom)
        for doc_id_str, doc_data in data['documents'].items():
            doc_id = int(doc_id_str)
            date_value = self.parse_date(doc_data.get('date'))
            doc = Document(
                titre=doc_data.get('titre', ''),
                auteur=doc_data.get('auteur', 'inconnu'),
                source=doc_data.get('source', 'inconnu'),
                date=date_value,
                url=doc_data.get('url', ''),
                texte=doc_data.get('texte', '')
            )
            self.register_document(doc, doc_id=doc_id)
        print(f"Corpus chargé depuis '{path}' ({self.ndoc} documents, {self.naut} auteurs).")
        return True
