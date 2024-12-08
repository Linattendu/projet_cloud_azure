from flask import Flask, request, render_template
from blob_storage import upload_to_blob, list_blobs
import os

# Initialisation de l'application Flask
app = Flask(__name__)

@app.route('/')
def home():
    """
    Route racine.
    Affiche directement la page de téléversement de fichiers.
    """
    return render_template('deploiement.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """
    Gère le téléversement d'un fichier vers Azure Blob Storage.
    """
    if 'file' not in request.files:
        return "Aucun fichier sélectionné", 400

    file = request.files['file']
    if file.filename == '':
        return "Nom de fichier vide", 400

    # Sauvegarde temporaire du fichier local
    file_path = os.path.join("uploads", file.filename)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    file.save(file_path)

    # Téléversement vers Azure Blob Storage
    upload_to_blob(file_path, file.filename)

    # Supprimer le fichier temporaire local après téléversement
    os.remove(file_path)

    return f"Fichier {file.filename} téléversé avec succès.", 200

@app.route('/list_blobs', methods=['GET'])
def list_all_blobs():
    """
    Affiche la liste des fichiers présents dans Azure Blob Storage.
    """
    blobs = list_blobs()
    return '''
    <h1>Liste des fichiers dans Azure Blob Storage</h1>
    <ul>
        {}
    </ul>
    <a href="/">Retour à l'accueil</a>
    '''.format("".join([f"<li>{blob}</li>" for blob in blobs]))

if __name__ == "__main__":
    app.run(debug=True)
