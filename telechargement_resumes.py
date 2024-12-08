"""
Fichier : telechargement_resumes.py
Description :
Ce script enrichit un ensemble de données de films avec des résumés (overviews) récupérés via l'API TMDb. 
Il utilise les identifiants IMDb des films pour interroger l'API et ajoute les résumés au DataFrame existant.
Les données enrichies sont ensuite sauvegardées pour une utilisation future.

Étapes principales :
1. Charger les données existantes depuis un fichier Pickle contenant les informations des films.
2. Interroger l'API TMDb pour récupérer les résumés (overviews) des films à partir de leurs IDs IMDb.
3. Ajouter les résumés au DataFrame existant.
4. Sauvegarder le DataFrame enrichi dans un fichier Pickle.
5. Vérifier les résultats en affichant les premières lignes du DataFrame enrichi.
"""

import pandas as pd
import requests
from tqdm import tqdm
import time

# Charger le DataFrame fusionné
# -----------------------------
# Chemin vers le fichier contenant les données de films
file_path = "data/movies_with_ratings.pkl"

# Charger les données à partir du fichier Pickle
movies_echantillon = pd.read_pickle(file_path)

# API TMDb - Configuration
# -------------------------
# URL de base pour accéder à l'API TMDb
api_url_base = "https://api.themoviedb.org/3/find/"

# Token d'accès (Bearer Token) pour authentification à l'API TMDb
api_token = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJhZDVhMTM2Nzc5M2MwZTY0NWFkZjI2MmUxNTEwMmU3OSIsIm5iZiI6MTczMzYxNTE2Ny43NDcsInN1YiI6IjY3NTRkZTNmZDQyOGNhMWM1ZDYyYzgwMyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.utZhK2R60OF9-YkX7lXYmnmaqwGt4otvYZe2h07J6GI"

# En-têtes HTTP requis pour les requêtes API
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {api_token}"
}

# Fonction : fetch_overview
# -------------------------
# Description :
# Cette fonction interroge l'API TMDb pour récupérer le résumé (overview) d'un film à partir de son ID IMDb.
# Paramètre :
# - imdb_id : L'identifiant IMDb du film (ex. : "tt0133093" pour The Matrix).
# Retour :
# - Le résumé (overview) du film sous forme de chaîne de caractères, ou un message indiquant qu'aucun résumé n'est disponible.
def fetch_overview(imdb_id):
    try:
        # Construire l'URL pour interroger l'API avec un ID IMDb
        url = f"{api_url_base}{imdb_id}?external_source=imdb_id"
        response = requests.get(url, headers=headers)

        # Vérifier si la requête a réussi
        if response.status_code == 200:
            data = response.json()
            if 'movie_results' in data and data['movie_results']:
                # Extraire le titre et le résumé du film
                movie = data['movie_results'][0]
                title = movie.get('title', "Titre inconnu")
                overview = movie.get('overview', "Pas de résumé disponible")
                print(f"Titre trouvé : {title}")
                print(f"Résumé : {overview}")
                return overview
            else:
                return "Pas de résumé disponible"
        else:
            print(f"Erreur HTTP {response.status_code} pour l'ID IMDb {imdb_id}")
            return "Pas de résumé disponible"
    except Exception as e:
        print(f"Erreur pour {imdb_id} : {e}")
        return "Pas de résumé disponible"

# Ajouter les résumés au DataFrame
# --------------------------------
# Activer une barre de progression avec tqdm
tqdm.pandas()

# Appliquer la fonction `fetch_overview` à chaque ID IMDb du DataFrame
movies_echantillon['overview'] = movies_echantillon['Id'].progress_apply(fetch_overview)

# Pause entre les requêtes pour éviter les limitations de l'API
time.sleep(0.2)

# Sauvegarder le DataFrame enrichi
# --------------------------------
# Chemin de sortie pour le fichier Pickle contenant les données enrichies
output_path = "data/movies_with_overviews.pkl"

# Sauvegarder le DataFrame enrichi
movies_echantillon.to_pickle(output_path)
print(f"Les descriptions ont été ajoutées et le DataFrame a été sauvegardé dans {output_path}")

# Charger et afficher les données pour vérification
# -------------------------------------------------
movies_echantillon = pd.read_pickle(output_path)
print("Les données enrichies ont été chargées avec succès.")
print(movies_echantillon.head(10))
