from datetime import datetime
from typing import Dict
import re
import pandas as pd
from collections import Counter, defaultdict

# Singleton metaclass
class Singleton(type):
    _instances: Dict[type, object] = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

class Corpus(metaclass=Singleton):
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
        # caches pour search / concorde
        self._concat_text = None        # chaîne concaténée (tous les textes)
        self._offsets = None            # liste de tuples (doc_id, start, end) pour mapper positions → doc

    def _build_concat_cache(self):
        """Construit la chaîne concaténée et les offsets si nécessaire."""
        if self._concat_text is not None and self._offsets is not None:
            return
        parts = []
        offsets = []
        pos = 0
        # trier par id pour garder ordre stable
        for did in sorted(self.id2doc.keys()):
            doc = self.id2doc[did]
            text = getattr(doc, 'texte', None) or getattr(doc, 'text', None) or ''
            if text is None:
                text = ''
            start = pos
            parts.append(text)
            end = start + len(text)
            offsets.append((did, start, end))
            # ajouter un séparateur d'un caractère pour ne pas fusionner fin/début de documents
            parts.append("\n")
            pos = end + 1
        self._concat_text = ''.join(parts)
        self._offsets = offsets

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

        # invalider cache concat si on modifie le corpus
        self._concat_text = None
        self._offsets = None

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

    def _find_doc_for_pos(self, pos):
        """Retourne doc_id et (start,end) correspondant à la position pos dans la chaine concaténée."""
        if self._offsets is None:
            return None, (None, None)
        # recherche linéaire (suffisant pour corpus modestes) ; remplacer par bisect si nécessaire
        for did, s, e in self._offsets:
            if s <= pos < e:
                return did, (s, e)
        return None, (None, None)

    def search(self, motif: str, regex: bool = False, case_insensitive: bool = True, context: int = 60):
        """
        Recherche les passages contenant motif dans le corpus concaténé.
        Retour : liste de tuples (doc_id, contexte_gauche, motif, contexte_droit, match_start, match_end)
        - motif : chaîne ou expression régulière selon regex
        - regex : si True, motif est interprété comme regex
        - case_insensitive : bool
        - context : nombre de caractères de contexte à gauche et à droite
        """
        if not motif:
            return []

        self._build_concat_cache()
        big = self._concat_text or ''
        flags = re.IGNORECASE if case_insensitive else 0
        pattern = motif if regex else re.escape(motif)
        try:
            it = re.finditer(pattern, big, flags)
        except re.error:
            # motif invalide
            return []

        results = []
        for m in it:
            start, end = m.start(), m.end()
            doc_id, (doc_start, doc_end) = self._find_doc_for_pos(start)
            if doc_id is None:
                # si position tombe dans le séparateur, tenter la doc suivante/précédente
                doc_id, (doc_start, doc_end) = self._find_doc_for_pos(max(0, start-1))
            # limiter contexte à l'intérieur du document
            left_bound = max(doc_start if doc_start is not None else 0, start - context)
            right_bound = min(doc_end if doc_end is not None else len(big), end + context)
            contexte_gauche = big[left_bound:start]
            motif_trouve = big[start:end]
            contexte_droit = big[end:right_bound]
            results.append((doc_id, contexte_gauche, motif_trouve, contexte_droit, start, end))
        return results

    def concorde(self, motif: str, regex: bool = False, case_insensitive: bool = True, context: int = 30):
        """
        Construit un concordancier pour motif. Retourne un pandas.DataFrame avec colonnes :
        ['doc_id', 'contexte_gauche', 'motif', 'contexte_droit', 'match_start', 'match_end']
        """
        matches = self.search(motif, regex=regex, case_insensitive=case_insensitive, context=context)
        rows = []
        for doc_id, gauche, mot, droite, s, e in matches:
            rows.append({
                'doc_id': doc_id,
                'contexte_gauche': gauche,
                'motif': mot,
                'contexte_droit': droite,
                'match_start': s,
                'match_end': e
            })
        df = pd.DataFrame(rows, columns=['doc_id', 'contexte_gauche', 'motif', 'contexte_droit', 'match_start', 'match_end'])
        return df

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

    def nettoyer_texte(self, s: str) -> str:
        """
        Nettoie une chaîne :
        - passage en minuscules
        - suppression des retours à la ligne
        - suppression des chiffres et de la ponctuation (conserve les apostrophes)
        - normalisation des espaces
        """
        if s is None:
            return ''
        s = str(s).lower()
        s = s.replace('\n', ' ')
        # conserver lettres (y compris accents courants) et apostrophes, remplacer le reste par espace
        s = re.sub(r"[^a-zàâçéèêëîïôûùüÿñæœ'\s]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def stats(self, n: int = 20, min_len: int = 1, show: bool = True) -> pd.DataFrame:
        """
        Calcule et retourne un DataFrame des fréquences de termes dans le corpus.
        Colonnes : ['term', 'tf', 'df'] où tf = term frequency (occurrences totales),
        df = document frequency (nombre de documents contenant le terme).
        - n : nombre d'éléments les plus fréquents à afficher si show=True
        - min_len : taille minimale du token pour être pris en compte
        - show : si True affiche un résumé et les top-n
        """
        if not self.id2doc:
            df_empty = pd.DataFrame(columns=['term', 'tf', 'df'])
            if show:
                print("Corpus vide — aucune statistique.")
            return df_empty

        tf = Counter()
        df_counts = defaultdict(int)

        for did, doc in self.id2doc.items():
            text = getattr(doc, 'texte', None) or getattr(doc, 'text', None) or ''
            cleaned = self.nettoyer_texte(text)
            # tokens : séquence de lettres/apostrophe
            tokens = re.findall(r"[a-zàâçéèêëîïôûùüÿñæœ']+", cleaned)
            # filtrer par longueur minimale
            tokens = [t for t in tokens if len(t) >= min_len]
            tf.update(tokens)
            # doc frequency : compter chaque terme une seule fois par document
            unique_terms = set(tokens)
            for term in unique_terms:
                df_counts[term] += 1

        # construire DataFrame
        terms = list(tf.keys())
        data = {
            'term': terms,
            'tf': [tf[t] for t in terms],
            'df': [df_counts.get(t, 0) for t in terms]
        }
        freq_df = pd.DataFrame(data)
        freq_df = freq_df.sort_values(by=['tf', 'df'], ascending=[False, False]).reset_index(drop=True)

        if show:
            print(f"Nombre de mots différents dans le corpus : {len(freq_df)}")
            top = freq_df.head(n)
            print(f"Top {n} mots les plus fréquents :")
            print(top.to_string(index=False))

        return freq_df

if __name__ == "__main__":
    # petit test local : crée un corpus, ajoute un document et affiche quelques vues
    from classes.Document import Document
    c = Corpus("test_corpus")
    d = Document(titre="Document de test", auteur="Alice|Bob", date=datetime.now().isoformat(), url="http://example.com", texte="Texte de test pour vérifier le corpus.")
    c.add_doc(d)
    print(c)
    print("Affichage trié par date :")
    c.show_by_date(5)
    print("Affichage trié par titre :")
    c.show_by_title(5)
    # test search / concorde
    print("\nExemple search('test') :")
    print(c.search('test', context=20))
    print("\nExemple concorde('test') :")
    print(c.concorde('test', context=20))
    # sauvegarde de vérification
    try:
        c.save("test_corpus.tsv")
        print("Corpus sauvegardé dans test_corpus.tsv")
    except Exception as e:
        print("Échec sauvegarde:", e)



