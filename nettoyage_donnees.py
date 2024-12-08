"""
Fichier : nettoyage_donnees.py
Description :
Ce script combine deux ensembles de données IMDb : les métadonnées des films (`movies.pkl`) 
et leurs notes/évaluations (`movies_ratings.pkl`). Il filtre, nettoie et fusionne les informations 
pour produire un fichier final contenant des films enrichis avec leurs métadonnées et leurs notes. 

Étapes principales :
1. Chargement des données prétraitées depuis des fichiers Pickle.
2. Filtrage des entrées pour garder uniquement les films.
3. Sélection et renommage des colonnes nécessaires.
4. Fusion des métadonnées des films avec leurs notes.
5. Sauvegarde des données finales dans un nouveau fichier Pickle.
"""

import pandas as pd

# Charger les données depuis les fichiers Pickle
# ----------------------------------------------
# Chemin vers les fichiers contenant les métadonnées et notes des films
movies_title = pd.read_pickle("data/movies.pkl")  # Contient les métadonnées des titres
movies_ratings = pd.read_pickle("data/movies_ratings.pkl")  # Contient les notes et votes des titres

# Filtrer uniquement les films
# ----------------------------
# Garder uniquement les lignes où le type de titre est 'movie'
movies = movies_title[movies_title['titleType'] == 'movie']

# Garder les colonnes nécessaires
# -------------------------------
# Sélectionner uniquement les colonnes importantes pour l'analyse ou l'application
movies = movies[['tconst', 'primaryTitle', 'startYear', 'runtimeMinutes', 'genres']]

# Joindre les notes aux films
# ---------------------------
# Fusionner les métadonnées des films avec leurs notes sur la base de l'ID unique 'tconst'
movies_with_ratings = pd.merge(movies, movies_ratings, on='tconst', how='inner')

# Renommer les colonnes pour correspondre à votre base
# ----------------------------------------------------
# Renommer les colonnes pour qu'elles soient plus explicites et adaptées à l'application
movies_with_ratings = movies_with_ratings.rename(columns={
    'tconst': 'Id',                 # Identifiant unique IMDb
    'primaryTitle': 'Title',        # Titre principal
    'startYear': 'Year',            # Année de sortie
    'runtimeMinutes': 'Runtime',    # Durée en minutes
    'genres': 'Genres',             # Genres associés au film
    'averageRating': 'Rating',      # Note moyenne
    'numVotes': 'Votes'             # Nombre de votes
})

# Afficher un aperçu des données
# ------------------------------
# Afficher les premières lignes du DataFrame pour vérifier que tout est correct
print(movies_with_ratings.head())

# Sauvegarder les données finales
# -------------------------------
# Sauvegarder le DataFrame final enrichi dans un fichier Pickle
movies_with_ratings.to_pickle("data/movies_with_ratings.pkl")
print("Les données ont été sauvegardées dans movies_with_ratings.pkl")
