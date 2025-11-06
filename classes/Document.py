from typing import List, Optional
from datetime import datetime

class Document:
    def __init__(self, titre: str, auteur: str, date: str, url: str, texte: str):
        self.titre = titre
        self.auteur = auteur
        self.date = date
        self.url = url
        self.texte = texte
        self.type = 'generic'

    def afficher_infos(self):
        print(f"Titre : {self.titre}")
        print(f"Auteur : {self.auteur}")
        print(f"Date : {self.date}")
        print(f"Url : {self.url}")
        print(f"Text : {self.texte}")

    def __str__(self):
        return f"Titre : {self.titre}"

    def getType(self) -> str:
        """Renvoie le type/source du document — à surcharger par les classes filles."""
        return self.type


class RedditDocument(Document):
    """Document spécifique Reddit — ajoute par exemple le nombre de commentaires."""

    def __init__(self, titre: str, auteur: str, date: str, url: str, texte: str, num_comments: int = 0):
        super().__init__(titre=titre, auteur=auteur, date=date, url=url, texte=texte)
        self.num_comments = int(num_comments) if num_comments is not None else 0
        self.type = 'reddit'

    # accesseur / mutateur
    def get_num_comments(self) -> int:
        return self.num_comments

    def set_num_comments(self, n: int):
        try:
            self.num_comments = int(n)
        except Exception:
            self.num_comments = 0

    def __str__(self):
        return f"[Reddit] {self.titre} — comments: {self.num_comments}"


class ArxivDocument(Document):
    """Document spécifique arXiv — gère la liste de co-auteurs."""

    def __init__(self, titre: str, auteur: str, date: str, url: str, texte: str, coauthors: Optional[List[str]] = None):
        # ici `auteur` peut être la chaîne principale ; nous stockons aussi la liste des co-auteurs
        super().__init__(titre=titre, auteur=auteur, date=date, url=url, texte=texte)
        self.coauthors = list(coauthors) if coauthors else []
        self.type = 'arxiv'

    def add_coauthor(self, name: str):
        if name and name not in self.coauthors:
            self.coauthors.append(name)

    def get_coauthors(self) -> List[str]:
        return list(self.coauthors)

    def __str__(self):
        ca = ', '.join(self.coauthors[:5]) + (', ...' if len(self.coauthors) > 5 else '')
        return f"[ArXiv] {self.titre} — coauthors: {ca}"


class DocumentFactory:
    """Factory simple pour créer le bon type de Document à partir d'un dict/source."""

    @staticmethod
    def create_from_dict(d: dict):
        """
        d attendu : keys usuelles 'title'/'titre','text','source','authors','url','created','num_comments'
        Retourne une instance de RedditDocument, ArxivDocument ou Document.
        """
        source = (d.get('source') or '').lower()
        titre = d.get('title') or d.get('titre') or ''
        texte = d.get('text') or d.get('texte') or ''
        url = d.get('url') or ''
        created = d.get('created') or d.get('date') or ''
        authors = d.get('authors') or d.get('authors_list') or d.get('coauthors') or ''
        # authors peut être liste ou chaîne
        if isinstance(authors, list):
            auteur_str = '|'.join(authors)
        else:
            auteur_str = str(authors)

        if source == 'reddit':
            num_comments = d.get('num_comments') or d.get('comments') or 0
            return RedditDocument(titre=titre, auteur=auteur_str, date=str(created), url=url, texte=texte, num_comments=num_comments)
        if source in ('arxiv', 'arXiv'.lower()):
            coauthors = []
            if isinstance(d.get('authors'), list):
                coauthors = d.get('authors')
            elif isinstance(d.get('authors'), str) and d.get('authors'):
                coauthors = [s for s in d.get('authors').split('|') if s.strip()]
            return ArxivDocument(titre=titre, auteur=auteur_str, date=str(created), url=url, texte=texte, coauthors=coauthors)

        # fallback generic
        return Document(titre=titre, auteur=auteur_str, date=str(created), url=url, texte=texte)



