class Author:
    def __init__(self, name, ndoc=0, production=None):
        self.name = name
        self.ndoc = ndoc
        self.production = production if production is not None else {}

    def add(self, doc_id, doc):
        """Associe un document à l'auteur et met à jour son compteur."""
        self.production[doc_id] = doc
        self.ndoc = len(self.production)

    def __str__(self):
        return f"{self.name} — {self.ndoc} document(s)"
