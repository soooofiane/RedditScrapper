import os
from datetime import datetime

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
            self._next_doc_id = 1
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
            doc_id = self._next_doc_id
            self._next_doc_id += 1
        else:
            self._next_doc_id = max(self._next_doc_id, doc_id + 1)
        self.id2doc[doc_id] = doc
        self.ndoc = len(self.id2doc)
        author = self.get_or_create_author(doc.auteur)
        author.add(doc_id, doc)
        self.naut = len(self.authors)
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
