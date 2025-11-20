from typing import List, Dict, Tuple
import math
import numpy as np
import pandas as pd
import re

# essayer d'utiliser scipy.sparse si disponible (meilleur pour grandes matrices)
try:
    from scipy.sparse import csr_matrix, issparse
    from scipy.sparse.linalg import norm as sparse_norm
    _SCIPY_AVAILABLE = True
except Exception:
    csr_matrix = None
    issparse = lambda x: False
    sparse_norm = None
    _SCIPY_AVAILABLE = False

class SearchEngine:
    """
    Moteur de recherche simple Documents x Termes.
    - Instancier avec un objet Corpus : la construction des matrices TF / TF-IDF est lancée.
    - search(query, topk) : retourne un pandas.DataFrame des meilleurs documents (colonnes: doc_id, score, titre, type, snippet)
    """

    def __init__(self, corpus, use_tfidf: bool = True):
        self.corpus = corpus
        self.use_tfidf = use_tfidf
        # construira vocab, mat_tf, mat_tfidf, idf, norms
        self.vocab: Dict[str, int] = {}
        self.inv_vocab: List[str] = []
        self.mat_tf = None
        self.mat_tfidf = None
        self.idf = None
        self.doc_norms = None
        self._build_index()

    def _tokenize(self, text: str) -> List[str]:
        if text is None:
            return []
        # utiliser la fonction de nettoyage du corpus si disponible
        cleaner = getattr(self.corpus, "nettoyer_texte", None)
        if callable(cleaner):
            cleaned = cleaner(text)
            # tokens par regexp (lettres et apostrophes)
            tokens = re.findall(r"[a-zàâçéèêëîïôûùüÿñæœ']+", cleaned)
            return tokens
        # fallback
        cleaned = str(text).lower().replace('\n', ' ')
        tokens = re.findall(r"[a-z']+", cleaned)
        return tokens

    def _build_index(self):
        # 1) parcourir docs, collecter tokens per doc, construire vocab set
        docs = []
        token_counters = []
        vocab_set = set()
        id_order = []
        for doc_id in sorted(self.corpus.id2doc.keys()):
            doc = self.corpus.id2doc[doc_id]
            text = getattr(doc, 'texte', None) or getattr(doc, 'text', None) or ''
            tokens = self._tokenize(text)
            ctr = {}
            for t in tokens:
                ctr[t] = ctr.get(t, 0) + 1
                vocab_set.add(t)
            token_counters.append(ctr)
            docs.append(doc_id)
            id_order.append(doc_id)

        # vocab sorted for stable ids
        self.inv_vocab = sorted(vocab_set)
        self.vocab = {w: i for i, w in enumerate(self.inv_vocab)}
        n_docs = len(token_counters)
        n_terms = len(self.inv_vocab)

        if n_docs == 0 or n_terms == 0:
            # matrices vides
            self.mat_tf = None
            self.mat_tfidf = None
            self.idf = np.array([])
            self.doc_norms = np.array([])
            self._doc_index = id_order
            self._n_docs = n_docs
            self._n_terms = n_terms
            return

        # 2) construire TF matrix (sparse si possible)
        rows = []
        cols = []
        data = []
        for i, ctr in enumerate(token_counters):
            for term, count in ctr.items():
                j = self.vocab.get(term)
                if j is None:
                    continue
                rows.append(i)
                cols.append(j)
                data.append(count)

        if _SCIPY_AVAILABLE:
            self.mat_tf = csr_matrix((data, (rows, cols)), shape=(n_docs, n_terms), dtype=float)
            # df: documents contenant term
            df_vec = np.array((self.mat_tf > 0).sum(axis=0)).ravel()
        else:
            mat = np.zeros((n_docs, n_terms), dtype=float)
            for r, c, v in zip(rows, cols, data):
                mat[r, c] = v
            self.mat_tf = mat
            df_vec = np.count_nonzero(mat, axis=0)

        # 3) idf
        df_vec = np.asarray(df_vec, dtype=float)
        df_vec[df_vec == 0] = 1.0
        idf = np.log((n_docs) / df_vec) + 1.0
        self.idf = idf

        # 4) construire TF-IDF matrix
        if _SCIPY_AVAILABLE:
            from scipy.sparse import diags
            D = diags(self.idf)
            self.mat_tfidf = self.mat_tf.dot(D)
        else:
            self.mat_tfidf = self.mat_tf * self.idf[np.newaxis, :]

        # 5) pré-calculer normes document (pour cosinus)
        if _SCIPY_AVAILABLE:
            self.doc_norms = np.array([sparse_norm(self.mat_tfidf.getrow(i)) for i in range(n_docs)])
        else:
            self.doc_norms = np.linalg.norm(self.mat_tfidf, axis=1)

        self._doc_index = id_order
        self._n_docs = n_docs
        self._n_terms = n_terms

    def _query_vector(self, query: str):
        tokens = self._tokenize(query)
        if len(tokens) == 0 or len(self.vocab) == 0:
            if _SCIPY_AVAILABLE:
                return csr_matrix((1, len(self.vocab)), dtype=float)
            return np.zeros((1, len(self.vocab)), dtype=float)

        q_counts = {}
        for t in tokens:
            if t in self.vocab:
                q_counts[t] = q_counts.get(t, 0) + 1

        if _SCIPY_AVAILABLE:
            rows = []
            cols = []
            data = []
            for term, cnt in q_counts.items():
                j = self.vocab[term]
                rows.append(0)
                cols.append(j)
                data.append(cnt * self.idf[j] if self.use_tfidf else cnt)
            qvec = csr_matrix((data, (rows, cols)), shape=(1, len(self.vocab)), dtype=float)
            return qvec
        else:
            qvec = np.zeros((1, len(self.vocab)), dtype=float)
            for term, cnt in q_counts.items():
                j = self.vocab[term]
                qvec[0, j] = cnt * (self.idf[j] if self.use_tfidf else 1.0)
            return qvec

    def search(self, query: str, topk: int = 10) -> pd.DataFrame:
        """
        Recherche : retourne DataFrame trié par score décroissant.
        Colonnes : ['doc_id','score','title','type','snippet']
        """
        if not query or (self.mat_tfidf is None and self.mat_tf is None):
            return pd.DataFrame(columns=['doc_id','score','title','type','snippet'])

        qvec = self._query_vector(query)

        mat = self.mat_tfidf if self.use_tfidf else self.mat_tf

        if _SCIPY_AVAILABLE and issparse(mat):
            scores = np.asarray((mat.dot(qvec.T)).todense()).ravel()
            q_norm = sparse_norm(qvec)
        else:
            scores = (mat @ qvec.T).ravel()
            q_norm = np.linalg.norm(qvec)

        doc_norms = self.doc_norms.copy()
        denom = doc_norms * (q_norm if q_norm != 0 else 1.0)
        with np.errstate(divide='ignore', invalid='ignore'):
            cos = np.where(denom > 0, scores / denom, 0.0)

        ranked_idx = np.argsort(-cos)
        top_idx = ranked_idx[:topk]

        # build snippets once
        snippets_map = {}
        all_snippets = self.corpus.search(query, regex=False, case_insensitive=True, context=60)
        for m in all_snippets:
            doc_id = m[0]
            if doc_id not in snippets_map:
                snippets_map[doc_id] = f"...{m[1]}{m[2]}{m[3]}..."

        rows = []
        for idx in top_idx:
            score = float(cos[idx])
            if score <= 0:
                continue
            doc_id = self._doc_index[idx]
            doc = self.corpus.id2doc.get(doc_id)
            title = getattr(doc, 'titre', '') or getattr(doc, 'title', '')
            dtype = getattr(doc, 'type', '')
            snippet = snippets_map.get(doc_id, '')
            rows.append({'doc_id': doc_id, 'score': score, 'title': title, 'type': dtype, 'snippet': snippet})

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values('score', ascending=False).reset_index(drop=True)
        return df