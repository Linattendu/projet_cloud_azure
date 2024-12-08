from azure.storage.blob import BlobServiceClient
import os

# Initialisation de la chaîne de connexion et du conteneur
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=mystorageaccoutmovies;AccountKey=83pEv4BIMv3geDfzVdgfGB0OoJt5m/+YnUJAKzB8iyR0/rI779j0S+t0YDcGAcR8PIGtazoAoqFR+AStw4pHBw==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "movie-data"

# Initialisation du client Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

def upload_to_blob(file_path, blob_name):
    """
    Télécharge un fichier local vers Azure Blob Storage.
    """
    try:
        blob_client = container_client.get_blob_client(blob_name)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"Fichier {file_path} téléchargé sous le nom {blob_name}")
    except Exception as e:
        print(f"Erreur lors du téléchargement : {e}")

def download_from_blob(blob_name, download_path):
    """
    Télécharge un fichier depuis Azure Blob Storage vers le système local.
    """
    try:
        blob_client = container_client.get_blob_client(blob_name)
        with open(download_path, "wb") as file:
            data = blob_client.download_blob()
            file.write(data.readall())
        print(f"Fichier {blob_name} téléchargé vers {download_path}")
    except Exception as e:
        print(f"Erreur lors du téléchargement : {e}")

def list_blobs():
    """
    Liste tous les blobs dans le conteneur Azure Blob Storage.
    """
    try:
        blobs = container_client.list_blobs()
        print("Blobs disponibles dans le conteneur :")
        for blob in blobs:
            print(f"- {blob.name}")
        return [blob.name for blob in blobs]
    except Exception as e:
        print(f"Erreur lors de la récupération des blobs : {e}")
        return []