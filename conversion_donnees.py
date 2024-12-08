"""
Fichier : process_imdb_data.py
Description :
Ce script traite les fichiers IMDb `title.basics.tsv` et `title.ratings.tsv` pour extraire et filtrer 
des données pertinentes sur les films. Les résultats sont combinés et sauvegardés en tant que fichiers 
Pickle pour une utilisation future.

Étapes principales :
1. Charger les fichiers IMDb en morceaux (chunks) pour gérer les fichiers volumineux.
2. Appliquer des filtres pour extraire uniquement les films nécessaires.
3. Combiner les morceaux filtrés en un DataFrame final.
4. Sauvegarder les résultats dans des fichiers Pickle (`movies.pkl` et `movies_ratings.pkl`).

"""

import pandas as pd

# Chemins vers les fichiers IMDb
title_basics_path = "data/title.basics.tsv"  # Fichier contenant les métadonnées de base des titres
title_ratings_path = "data/title.ratings.tsv"  # Fichier contenant les notes et votes des titres

# Taille des chunks (nombre de lignes à traiter par itération)
chunksize = 1000  

# Charger les fichiers IMDb par morceaux (chunks) pour éviter les surcharges de mémoire
title_chunks = pd.read_csv(title_basics_path, sep='\t', na_values='\\N', low_memory=False, chunksize=chunksize, encoding='utf-8')
ratings_chunks = pd.read_csv(title_ratings_path, sep='\t', na_values='\\N', low_memory=False, chunksize=chunksize, encoding='utf-8')

# Étape 1 : Traitement des données de `title.basics.tsv`
# -----------------------------------------------------
filtered_movies = []  # Liste pour stocker les morceaux filtrés
for chunk in title_chunks:
    # Filtrer les films dans chaque chunk en vérifiant leur présence dans 'tconst'
    movies_chunk = chunk[chunk['tconst'].isin(chunk['tconst'])]
    filtered_movies.append(movies_chunk)  # Ajouter les morceaux filtrés à la liste

# Combiner tous les morceaux filtrés en un DataFrame unique
movies_principals = pd.concat(filtered_movies)

# Sauvegarder les données filtrées dans un fichier Pickle
movies_principals.to_pickle("data/movies.pkl")
print("Les données des films ont été sauvegardées dans 'movies.pkl'.")

# Étape 2 : Traitement des données de `title.ratings.tsv`
# -------------------------------------------------------
filtered_movies_rating = []  # Liste pour stocker les morceaux filtrés
for chunk in ratings_chunks:
    # Filtrer les films dans chaque chunk en vérifiant leur présence dans 'tconst'
    movies_rating_chunk = chunk[chunk['tconst'].isin(chunk['tconst'])]
    filtered_movies_rating.append(movies_rating_chunk)  # Ajouter les morceaux filtrés à la liste

# Combiner tous les morceaux filtrés en un DataFrame unique
movies_rating_principals = pd.concat(filtered_movies_rating)

# Sauvegarder les données filtrées dans un fichier Pickle
movies_rating_principals.to_pickle("data/movies_ratings.pkl")
print("Les données des notes des films ont été sauvegardées dans 'movies_ratings.pkl'.")
