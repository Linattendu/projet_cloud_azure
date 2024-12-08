"""
Fichier : app.py
Description :
Ce script implémente une application Flask pour recommander des films en fonction des préférences des utilisateurs collectées via un quiz.
Il utilise une base de données Azure SQL pour stocker les données sur les films et détecte les thèmes à partir des résumés de films.

Fonctionnalités principales :
1. Charger un DataFrame enrichi contenant des informations sur les films.
2. Insérer les données dans une base de données Azure SQL (étape exécutée une seule fois si nécessaire).
3. Détecter les thèmes des films à partir de leurs résumés.
4. Rechercher des films selon les réponses d'un quiz utilisateur.
5. Renvoyer les résultats sous forme de page HTML.

Structure principale :
- Routes Flask :
  - `/` : Affiche la page du quiz.
  - `/submit_quiz` : Traite les réponses du quiz et affiche les résultats.
- Connexion à une base de données Azure SQL pour stocker et récupérer les données.
"""
import pandas as pd
import os
import pymysql
from flask import Flask, request, render_template,jsonify
from blob_storage import upload_to_blob, download_from_blob, list_blobs

# Configuration de Flask
app = Flask(__name__)


file_path = "movies_with_overviews.pkl"
if not os.path.exists(file_path):
    print("Téléchargement du fichier depuis Azure Blob Storage")
    download_from_blob("movies_with_overviews.pkl", file_path)

# Charger les données des films
# -----------------------------
# Nom du dossier contenant les fichiers
""" data_folder = "data"
file_name = "movies_with_overviews.pkl"

# Construire le chemin relatif vers le fichier contenant les données
file_path = os.path.join(os.path.dirname(__file__), data_folder, file_name)
 """
# Charger les données enrichies
movies_with_overviews = pd.read_pickle(file_path)

# Configuration de la base de données Azure SQL
# ----------------------------------------------
conn_str = {
    "host": "movie-recommendation-db.mysql.database.azure.com",
    "user": "adminuser",
    "password": "Lina131201",
    "database": "movie_recommendation",
    "ssl": {"ssl-mode": "REQUIRED"}
}

# Définition du schéma de la table Movies (si nécessaire)
"""
CREATE TABLE Movies (
    Id VARCHAR(50) PRIMARY KEY,
    Title NVARCHAR(255),
    Genres NVARCHAR(255),
    Rating FLOAT,
    Votes INT,
    overview TEXT,
    theme TEXT
);
"""

# Fonction : insert_movies_into_db
# --------------------------------
# Insère les données des films dans la base de données Azure SQL.
# - `movies_data` : Un DataFrame contenant les données des films.
def insert_movies_into_db(movies_data):
    try:
        # Détecter les thèmes pour chaque film
        movies_data['theme'] = movies_data['overview'].apply(detect_theme)
        movies_data = movies_data.dropna()

        # Connexion à la base de données
        conn = pymysql.connect(**conn_str)
        cursor = conn.cursor()

        # Requête pour insérer les données
        query = """
        INSERT INTO Movies (Id, Title, Genres, Rating, Votes, overview, theme)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        for _, row in movies_data.iterrows():
            cursor.execute(query, (row["Id"], row["Title"], row["Genres"],
                                   row["Rating"], row["Votes"], row["overview"], row["theme"]))
        conn.commit()
        print(f"{len(movies_data)} films ont été insérés avec succès.")
    except pymysql.MySQLError as e:
        print("Erreur lors de l'insertion des données :", e)
    finally:
        if conn:
            conn.close()

# Fonction : detect_theme
# ------------------------
# Détecte les thèmes des films à partir des mots-clés présents dans leurs résumés.
# - `overview` : Le résumé du film.
# Retourne une liste de thèmes détectés ou "Aucun thème détecté".
def detect_theme(overview):
    themes = {
        "time travel": ["time travel", "future", "past", "timeline", "paradox", "dimension", "alternate reality"],
        "superheroes": ["superhero", "powers", "save", "world", "vigilante", "justice", "mask", "heroic", "villain"],
        "romance": ["love", "romance", "relationship", "affection", "heartbreak", "couple", "passion", "kiss", "breakup"],
        "dystopia": ["war", "apocalypse", "dystopia", "survival", "rebellion", "famine", "oppression", "tyranny", "post-apocalyptic"],
        "adventure": ["adventure", "journey", "quest", "exploration", "treasure", "travel", "expedition", "discovery"],
        "comedy": ["funny", "comedy", "humor", "laugh", "satire", "parody", "joke", "prank", "slapstick"],
        "thriller": ["thriller", "suspense", "mystery", "crime", "detective", "chase", "intrigue", "hostage", "tension"],
        "horror": ["horror", "fear", "haunted", "ghost", "monster", "evil", "zombie", "curse", "dark", "vampire", "werewolf"],
        "sci-fi": ["science fiction", "aliens", "space", "technology", "robots", "cyberpunk", "futuristic", "AI", "spaceship", "genetics"],
        "fantasy": ["magic", "wizard", "dragon", "fantasy", "mythical", "kingdom", "sword", "prophecy", "elves", "dwarves"],
        "historical": ["history", "war", "period", "biography", "true story", "ancient", "medieval", "historical figure", "battle"],
        "drama": ["drama", "emotional", "family", "struggle", "conflict", "tragedy", "intense", "grief", "loss", "redemption"],
        "action": ["action", "fight", "battle", "chase", "explosion", "gunfight", "martial arts", "warrior", "combat", "weapon"],
        "musical": ["musical", "singing", "dancing", "music", "performance", "concert", "band", "song", "melody"],
        "sports": ["sports", "competition", "team", "athlete", "training", "match", "victory", "stadium", "coach"],
        "animation": ["animated", "cartoon", "family-friendly", "kids", "pixar", "3D", "CGI", "anime", "stop-motion"],
        "crime": ["crime", "murder", "gangster", "heist", "law", "detective", "underworld", "robbery", "justice"],
        "psychological": ["psychological", "mind", "twist", "intense", "obsession", "hallucination", "paranoia", "dark"],
        "western": ["western", "cowboy", "gunslinger", "outlaw", "desert", "sheriff", "horse", "duel", "saloon"],
        "political": ["political", "power", "government", "corruption", "leader", "election", "revolution", "policy"],
        "mystery": ["mystery", "whodunit", "clues", "detective", "puzzle", "investigation", "secrets", "twist"],
        "war": ["war", "battle", "soldier", "army", "invasion", "weapon", "heroism", "strategy", "victory", "sacrifice"],
        "spy": ["spy", "espionage", "undercover", "mission", "intel", "agent", "double-cross", "stealth", "covert"],
        "family": ["family", "kids", "parents", "siblings", "bond", "relationship", "fun", "togetherness", "values"],
        "documentary": ["documentary", "real", "true story", "biography", "facts", "nature", "history", "investigation"],
        "coming-of-age": ["coming of age", "teen", "growth", "youth", "identity", "journey", "self-discovery", "adolescence"],
        "romantic comedy": ["romantic comedy", "love", "funny", "couple", "awkward", "relationship", "dating"],
        "dark comedy": ["dark comedy", "satire", "irony", "dark humor", "absurd", "tragicomic"],
        "fantasy adventure": ["fantasy adventure", "magic", "quest", "dragon", "epic", "mythical"],
        "survival": ["survival", "isolation", "rescue", "desperate", "wilderness", "hope"],
        "heist": ["heist", "robbery", "crime", "plan", "thieves", "team", "money"],
        "paranormal": ["paranormal", "ghost", "spirit", "haunted", "possession", "supernatural"]
    }
    detected = []
    for theme, keywords in themes.items():
        if any(keyword in overview.lower() for keyword in keywords):
            detected.append(theme)
    return ", ".join(detected) if detected else "Aucun thème détecté"

# Fonction : search_movies
# -------------------------
# Recherche des films dans la base de données Azure SQL en fonction des critères.
# - `criteria` : Un dictionnaire contenant les réponses au quiz.
# Retourne une liste de films correspondant aux critères.
def search_movies(criteria):
    try:
        conn = pymysql.connect(**conn_str)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # Requête SQL pour chercher des films
        query = """
        SELECT * FROM Movies
        WHERE theme LIKE %s
          AND overview LIKE %s
          AND Rating >= %s
        """
        params = [
            f"%{criteria.get('theme', '')}%",
            f"%{criteria.get('setting', '')}%",
            criteria.get('min_rating', 0)
        ]
        cursor.execute(query, params)
        return cursor.fetchall()
    except pymysql.MySQLError as e:
        print("Erreur lors de la requête :", e)
        return []
    finally:
        if conn:
            conn.close()

# Route : Quiz (GET)
# ------------------
# Affiche la page HTML contenant le quiz.
@app.route('/', methods=['GET'])
def quiz():
    return render_template('index.html')

# Route : Soumettre le quiz (POST)
# --------------------------------
# Récupère les réponses du quiz, effectue une recherche et affiche les résultats.
@app.route('/submit_quiz', methods=['POST'])
def submit_quiz():
    emotion = request.form['emotion']
    theme = request.form['theme']
    setting = request.form['setting']
    criteria = {
        "emotion": emotion,
        "theme": theme,
        "setting": setting,
        "min_rating": 7.0
    }
    results = search_movies(criteria)
    return render_template('results.html', results=results)


# Exécution de l'application Flask
if __name__ == '__main__':


    app.run(debug=True)
