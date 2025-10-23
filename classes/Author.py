class Author:
    def __init__(self, name, ndoc, production):
        self.name = name
        self.ndoc = ndoc
        self.production = production
    
    def add(self, doc):
        self.production[self.ndoc + 1] = doc
        self.ndoc += 1
    
    def __str__(self):
        return f"docs : {self.production}"
        
author = Author("TATO", 2, prod)

author.add("texte 3 blabla") 
print(author)         
