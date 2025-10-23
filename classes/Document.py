class Document:
    def __init__(self, titre, auteur, date, url, texte):
        self.titre = titre
        self.auteur = auteur
        self.date = date
        self.url = url
        self.texte = texte
    
    def afficher_infos(self):
        print(f"Titre : {self.titre}")
        print(f"Auteur : {self.auteur}")
        print(f"Date : {self.date}")
        print(f"Url : {self.url}")
        print(f"Text : {self.texte}")
        
    def __str__(self):
        return f"Titre : {self.titre}"



