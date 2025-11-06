class Author:
    """
    Représente un auteur.
    - name : nom (clé unique)
    - ndoc : nombre de documents (int)
    - production : dict mapping doc_id -> Document (ou tout objet représentant le document)
    """
    def __init__(self, name):
        self.name = name
        self.ndoc = 0
        self.production = {}

    def add(self, doc_id, doc_obj=None):
        """
        Ajoute un document à la production.
        - doc_id : identifiant unique du document (clé)
        - doc_obj : objet Document (optionnel, permet de stocker l'objet)
        """
        if doc_id not in self.production:
            self.production[doc_id] = doc_obj
            self.ndoc = len(self.production)
        else:
            # si déjà présent, on remplace l'objet associé si fourni
            if doc_obj is not None:
                self.production[doc_id] = doc_obj

    def __str__(self):
        return f"Author(name={self.name}, ndoc={self.ndoc}, docs={list(self.production.keys())})"

author = Author("TATO")

author.add("texte 3 blabla") 
print(author)
