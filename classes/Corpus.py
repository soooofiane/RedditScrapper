class Corpus:
    """
    Représente un corpus :
    - nom : nom du corpus
    - authors : dict name -> Author instance
    - id2doc : dict id -> Document instance
    - ndoc : nombre de documents
    - naut : nombre d'auteurs
    """

    def __init__(self, nom):
        self.nom = nom
        self.authors = {}
        self.id2doc = {}
        self.ndoc = 0
        self.naut = 0

    def add_doc(self, doc_obj, doc_id=None):
        """Ajoute une instance Document au corpus. Si doc_id None, génère un id."""
        if doc_id is None:
            next_id = max(self.id2doc.keys(), default=0) + 1
            doc_id = next_id
        else:
            try:
                doc_id = int(doc_id)
            except Exception:
                doc_id = max(self.id2doc.keys(), default=0) + 1

        self.id2doc[doc_id] = doc_obj
        self.ndoc = len(self.id2doc)

        # authors stored in Document as a single string (ex: "a|b") — normaliser en liste
        auteur_field = getattr(doc_obj, 'auteur', '') or ''
        if isinstance(auteur_field, str):
            authors_list = [s for s in auteur_field.split('|') if s.strip()] if auteur_field else []
        elif isinstance(auteur_field, (list, tuple)):
            authors_list = list(auteur_field)
        else:
            authors_list = []

        # mettre à jour authors (instances Author)
        from classes.Author import Author  # import local pour éviter dépendance circulaire en import global
        for name in authors_list:
            if name not in self.authors:
                self.authors[name] = Author(name)
            # stocker l'objet Document dans la production de l'auteur
            self.authors[name].add(doc_id, doc_obj)

        self.naut = len(self.authors)
        return doc_id

    def show_by_date(self, n=10):
        """Affiche n documents triés par date (si possible)."""
        def parse_date(d):
            if d is None:
                return ''
            try:
                from datetime import datetime
                return datetime.fromisoformat(str(d))
            except Exception:
                try:
                    return datetime.fromtimestamp(float(d))
                except Exception:
                    return str(d)
        items = list(self.id2doc.items())
        items_sorted = sorted(items, key=lambda kv: parse_date(getattr(kv[1], 'date', getattr(kv[1], 'date', ''))), reverse=True)
        for did, doc in items_sorted[:n]:
            print(f"[{did}] {getattr(doc, 'titre', '')} — {getattr(doc, 'date', '')} — auteurs: {getattr(doc, 'auteur', '')}")

    def show_by_title(self, n=10):
        """Affiche n documents triés par titre."""
        items = list(self.id2doc.items())
        items_sorted = sorted(items, key=lambda kv: (getattr(kv[1], 'titre', '') or '').lower())
        for did, doc in items_sorted[:n]:
            print(f"[{did}] {getattr(doc, 'titre', '')} — {getattr(doc, 'date', '')} — auteurs: {getattr(doc, 'auteur', '')}")

    def __repr__(self):
        return f"Corpus(name={self.nom}, ndoc={self.ndoc}, naut={self.naut})"

    def save(self, path):
        """Sauvegarde le corpus en CSV via pandas DataFrame (colonnes id,titre,text,source,authors,url,created)."""
        import pandas as pd
        rows = []
        for did, doc in self.id2doc.items():
            rows.append({
                'id': did,
                'title': getattr(doc, 'titre', '') or '',
                'text': getattr(doc, 'texte', '') or '',
                'source': getattr(doc, 'source', '') or '',
                'authors': getattr(doc, 'auteur', '') or '',
                'url': getattr(doc, 'url', '') or '',
                'created': getattr(doc, 'date', '') or ''
            })
        df = pd.DataFrame(rows)
        df = df.sort_values('id')
        df.to_csv(path, sep='\t', index=False)
        return path

    @classmethod
    def load(cls, path, nom=None):
        """Charge un corpus depuis un CSV créé par save()."""
        import pandas as pd
        from classes.Document import Document
        if nom is None:
            nom = path
        df = pd.read_csv(path, sep='\t', dtype=str).fillna('')
        corpus = cls(nom)
        for _, row in df.iterrows():
            did = int(row['id'])
            titre = row.get('title', '') or ''
            texte = row.get('text', '') or ''
            auteur = row.get('authors', '') or ''
            url = row.get('url', '') or ''
            created = row.get('created', '') or ''
            # créer Document
            doc_obj = Document(titre=titre, auteur=auteur, date=created, url=url, texte=texte)
            corpus.add_doc(doc_obj, doc_id=did)
        return corpus



