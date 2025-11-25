class Document:
    def __init__(self, titre, auteur, source, date, url, texte):
        self.titre = titre
        self.auteur = auteur
        self.source = source
        self.date = date
        self.url = url
        self.texte = texte
    
    def afficher_infos(self):
        print(f"Titre : {self.titre}")
        print(f"Auteur : {self.auteur}")
        print(f"Source : {self.source}")
        print(f"Date : {self.date}")
        print(f"Url : {self.url}")
        print(f"Text : {self.texte}")
        
    def __str__(self):
        return f"Titre : {self.titre}"
    
    def getType(self):
        return self.source


class RedditDocument(Document):
    def __init__(self, titre, auteur, source, date, url, texte, nb_commentaires=0):
        super().__init__(titre, auteur, source, date, url, texte)
        self.nb_commentaires = nb_commentaires
    
    def get_nb_commentaires(self):
        return self.nb_commentaires
    
    def set_nb_commentaires(self, nb_commentaires):
        self.nb_commentaires = nb_commentaires
    
    def __str__(self):
        return f"{self.titre} — {self.nb_commentaires} commentaire(s)"
    
    def getType(self):
        return "Reddit"


class ArxivDocument(Document):
    def __init__(self, titre, auteur, source, date, url, texte, co_auteurs=None):
        super().__init__(titre, auteur, source, date, url, texte)
        self.co_auteurs = co_auteurs if co_auteurs is not None else []
    
    def __str__(self):
        if len(self.co_auteurs) > 0:
            auteurs_str = ', '.join(self.co_auteurs)
            return f"{self.titre} — {len(self.co_auteurs)} auteur(s): {auteurs_str}"
        else:
            return f"{self.titre} — {self.auteur}"
    
    def getType(self):
        return "Arxiv"




