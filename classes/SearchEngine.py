import re
import string
import math
import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix, diags


class SearchEngine:
    # --- Moteur de recherche basé sur TFxIDF et similarité cosinus ---
    
    def __init__(self, corpus):
        # --- Initialise le moteur de recherche avec un corpus ---
        self.corpus = corpus
        
        # Construire le vocabulaire de base et la matrice TF
        self.vocab_base, self.mots = self.construire_vocab_base()
        self.mat_TF = self.construire_matrice_TF(self.vocab_base)
        
        # Construire le vocabulaire complet avec les statistiques
        self.vocab = self.construire_vocab(self.mat_TF, self.mots)
        
        # Construire la matrice TFxIDF
        self.mat_TFxIDF = self.construire_matrice_TFxIDF(self.mat_TF, self.vocab)
        
        # Créer le mapping mot -> index pour la requête
        self.mot_to_index = {mot: idx for idx, mot in enumerate(self.mots)}
    
    def construire_vocab_base(self):
        # --- Construit le vocabulaire de base (sans les stats) ---
        frequences = {}
        for doc in self.corpus.id2doc.values():
            if doc.texte:
                texte_nettoye = self.corpus.nettoyer_texte(doc.texte)
                delimiters = r'[\s' + re.escape(string.punctuation) + r']+'
                mots_list = re.split(delimiters, texte_nettoye)
                for mot in mots_list:
                    if mot:
                        frequences[mot] = frequences.get(mot, 0) + 1
        
        mots = sorted(frequences.keys())
        vocab_base = {}
        for idx, mot in enumerate(mots):
            vocab_base[mot] = {'id': idx + 1}
        
        return vocab_base, mots
    
    def construire_vocab(self, mat_TF=None, mots=None):
        # --- Construit le dictionnaire vocab avec les mots du corpus à partir de la matrice TF ---
        # Si la matrice n'est pas fournie, on doit d'abord construire le vocabulaire de base et la matrice
        if mat_TF is None or mots is None:
            vocab_base, mots = self.construire_vocab_base()
            mat_TF = self.construire_matrice_TF(vocab_base)
        
        # Calculer les statistiques à partir de la matrice TF
        # Nombre total d'occurrences = somme de chaque colonne
        nb_occurrences = mat_TF.sum(axis=0).A1  # .A1 convertit en array 1D
        
        # Nombre de documents contenant le mot = nombre de lignes non nulles pour chaque colonne
        nb_documents = (mat_TF > 0).sum(axis=0).A1  # Compte les valeurs > 0 par colonne
        
        # Construire le dictionnaire vocab
        vocab = {}
        for idx, mot in enumerate(mots):
            vocab[mot] = {
                'id': idx + 1,
                'nb_occurrences': int(nb_occurrences[idx]),
                'nb_documents': int(nb_documents[idx])
            }
        
        return vocab
    
    def construire_matrice_TF(self, vocab=None):
        # --- Construit la matrice Documents x Termes (Term Frequency) ---
        if vocab is None:
            vocab, _ = self.construire_vocab_base()
        
        documents = list(self.corpus.id2doc.values())
        mots = sorted(vocab.keys()) 
        
        # Créer un dictionnaire pour mapper les mots à leur index dans la matrice
        mot_to_index = {mot: idx for idx, mot in enumerate(mots)}
        
        # Initialiser les listes pour construire la matrice sparse
        data = []  # Valeurs (TF)
        row_indices = []  # Indices de ligne (documents)
        col_indices = []  # Indices de colonne (mots)
        
        # Parcourir tous les documents
        for doc_idx, doc in enumerate(documents):
            if doc.texte:
                texte_nettoye = self.corpus.nettoyer_texte(doc.texte)
                
                # Créer une regex qui split sur espace, tabulation et ponctuation
                delimiters = r'[\s' + re.escape(string.punctuation) + r']+'
                mots_doc = re.split(delimiters, texte_nettoye)
                
                # Compter les occurrences de chaque mot du vocabulaire dans ce document
                compteur_mots = {}
                for mot in mots_doc:
                    if mot and mot in vocab:  # Vérifier que le mot est dans le vocabulaire
                        compteur_mots[mot] = compteur_mots.get(mot, 0) + 1
                
                # Ajouter les données à la matrice
                for mot, tf in compteur_mots.items():
                    col_idx = mot_to_index[mot]
                    data.append(tf)
                    row_indices.append(doc_idx)
                    col_indices.append(col_idx)
        
        # Construire la matrice sparse CSR
        mat_TF = csr_matrix((data, (row_indices, col_indices)), 
                           shape=(len(documents), len(mots)))
        
        return mat_TF
    
    def construire_matrice_TFxIDF(self, mat_TF=None, vocab=None):
        # --- Construit la matrice TFxIDF (Term Frequency × Inverse Document Frequency) ---
        
        if mat_TF is None:
            if vocab is None:
                vocab_base, mots = self.construire_vocab_base()
                mat_TF = self.construire_matrice_TF(vocab_base)
                vocab = self.construire_vocab(mat_TF, mots)
            else:
                vocab_base, _ = self.construire_vocab_base()
                mat_TF = self.construire_matrice_TF(vocab_base)
        
        if vocab is None:
            vocab_base, mots = self.construire_vocab_base()
            vocab = self.construire_vocab(mat_TF, mots)
        
        N = mat_TF.shape[0]  # Nombre de lignes = nombre de documents
        
        # Calculer l'IDF pour chaque terme
        # IDF(t) = log(N / df(t)) où df(t) est le nombre de documents contenant le terme t
        mots = sorted(vocab.keys())
        idf_values = []
        
        for mot in mots:
            df_t = vocab[mot]['nb_documents']
            if df_t > 0:
                idf = math.log(N / df_t)
            else:
                idf = 0 
            idf_values.append(idf)
        
        # Convertir en array numpy pour la multiplication
        idf_array = np.array(idf_values)
        
        # Multiplier chaque colonne de la matrice TF par l'IDF correspondant
        idf_diag = diags(idf_array, format='csr')
        
        # Multiplier mat_TF par la diagonale IDF (chaque colonne est multipliée par son IDF)
        mat_TFxIDF = mat_TF.dot(idf_diag)
        
        return mat_TFxIDF
    
    def search(self, mots_cles, nb_documents=10):
        # --- Recherche de documents basée sur les mots-clés ---
        # mots_cles : liste de mots-clés de la requête
        # nb_documents : nombre de documents à retourner
        
        # Transformer la requête en vecteur
        vecteur_requete = self._construire_vecteur_requete(mots_cles)
        
        # Calculer la similarité cosinus avec tous les documents
        scores = self._calculer_similarite_cosinus(vecteur_requete)
        
        # Trier les scores par ordre décroissant
        indices_tries = np.argsort(scores)[::-1]
        
        # Récupérer les nb_documents meilleurs résultats
        resultats = []
        documents = list(self.corpus.id2doc.values())
        
        for i in range(min(nb_documents, len(indices_tries))):
            doc_idx = indices_tries[i]
            score = scores[doc_idx]
            if score > 0:  # Ne garder que les documents avec un score > 0
                doc = documents[doc_idx]
                resultats.append({
                    'titre': doc.titre,
                    'auteur': doc.auteur,
                    'source': doc.getType(),
                    'date': doc.date,
                    'url': doc.url,
                    'score': float(score)
                })
        
        # Créer un DataFrame pandas avec les résultats
        df_resultats = pd.DataFrame(resultats)
        
        return df_resultats
    
    def _construire_vecteur_requete(self, mots_cles):
        # --- Construit le vecteur requête à partir des mots-clés ---
        import re
        import string
        import math
        
        # Nettoie et transforme les mots-clés en vecteur
        requete_nettoyee = []
        for mot_cle in mots_cles:
            mot_nettoye = self.corpus.nettoyer_texte(mot_cle)
            # Split pour obtenir les mots individuels
            delimiters = r'[\s' + re.escape(string.punctuation) + r']+'
            mots_split = re.split(delimiters, mot_nettoye)
            requete_nettoyee.extend([m for m in mots_split if m])
        
        # Compter les occurrences dans la requête
        requete_freq = {}
        for mot in requete_nettoyee:
            if mot in self.vocab:
                requete_freq[mot] = requete_freq.get(mot, 0) + 1
        
        # Construire le vecteur requête (même dimension que le vocabulaire)
        vecteur_requete = np.zeros(len(self.mots))
        for mot, freq in requete_freq.items():
            idx = self.mot_to_index[mot]
            vecteur_requete[idx] = freq
        
        # Calculer l'IDF pour la requête et multiplier
        N = self.mat_TFxIDF.shape[0]
        for mot, freq in requete_freq.items():
            idx = self.mot_to_index[mot]
            df_t = self.vocab[mot]['nb_documents']
            if df_t > 0:
                idf = math.log(N / df_t)
                vecteur_requete[idx] = freq * idf
        
        return vecteur_requete
    
    def _calculer_similarite_cosinus(self, vecteur_requete):
        # --- Calcule la similarité cosinus entre le vecteur requête et tous les documents ---
        # Normaliser le vecteur requête
        norme_requete = np.linalg.norm(vecteur_requete)
        if norme_requete > 0:
            vecteur_requete_normalise = vecteur_requete / norme_requete
        else:
            vecteur_requete_normalise = vecteur_requete
        
        # Normaliser chaque ligne de la matrice (chaque document)
        normes_docs = np.sqrt(np.array(self.mat_TFxIDF.multiply(self.mat_TFxIDF).sum(axis=1)).flatten())
        # Éviter division par zéro
        normes_docs = np.where(normes_docs > 0, normes_docs, 1)
        
        # Produit scalaire avec chaque document normalisé
        vecteur_requete_sparse = csr_matrix(vecteur_requete_normalise)
        scores = self.mat_TFxIDF.dot(vecteur_requete_sparse.T).toarray().flatten()
        # Diviser par les normes des documents
        scores = scores / normes_docs
        
        return scores

