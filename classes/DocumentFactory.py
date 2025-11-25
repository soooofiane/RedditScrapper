from datetime import datetime
from classes.Document import Document, RedditDocument, ArxivDocument


class DocumentFactory:
    # --- Factory Pattern pour créer des documents selon leur source ---
    @staticmethod
    def create_document(source, titre, auteur, date, url, texte, **kwargs):
        if source.lower() == 'reddit':
            nb_commentaires = kwargs.get('nb_commentaires', 0)
            return RedditDocument(
                titre=titre,
                auteur=auteur,
                source=source,
                date=date,
                url=url,
                texte=texte,
                nb_commentaires=nb_commentaires
            )
        elif source.lower() == 'arxiv':
            co_auteurs = kwargs.get('co_auteurs', None)
            return ArxivDocument(
                titre=titre,
                auteur=auteur,
                source=source,
                date=date,
                url=url,
                texte=texte,
                co_auteurs=co_auteurs
            )
        else:
            # Par défaut, crée un Document de base
            return Document(
                titre=titre,
                auteur=auteur,
                source=source,
                date=date,
                url=url,
                texte=texte
            )

