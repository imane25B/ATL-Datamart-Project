import urllib.request
import os
import sys
import shutil
from minio import Minio

# Fonction principale qui coordonne l'extraction et le chargement des données
def main():
    grab_data_2023_to_2024()  # Télécharger les données pour les années 2023 à 2024

    # Définir les variables nécessaires pour l'upload
    bucket_name = "newyork-data-bucket"  # Nom du bucket MinIO
    folder_path = os.path.expanduser("C:/Users/DELL/Downloads/ATL-Datamart-main/ATL-Datamart-main/data/raw")  # Dossier contenant les fichiers 

    write_data_minio(bucket_name, folder_path)
# Fonction pour télécharger les données pour la période 2023 à 2024
def grab_data_2023_to_2024() -> None:
    """Supprime les fichiers existants et télécharge les fichiers de janvier 2018 à août 2023."""
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"  # URL de base des fichiers de données
    years = range(2023, 2024)  # Années à traiter
    months = range(1, 3)       # Mois à traiter
    data_dir = "C:/Users/DELL/Downloads/ATL-Datamart-main/ATL-Datamart-main/data/raw"  # Répertoire local pour stocker les fichiers

    # Supprimer les fichiers existants dans le répertoire cible
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)  # Suppression du répertoire existant et de son contenu
    os.makedirs(data_dir, exist_ok=True)  # Création du répertoire cible

    # Télécharger les fichiers pour chaque année et mois spécifié
    for year in years:
        for month in months:
            if year == 2024 and month > 2:  # Limiter les téléchargements jusqu'en février 2024
                break
            filename = f"yellow_tripdata_{year}-{month:02d}.parquet"  # Nom du fichier à télécharger
            file_url = base_url + filename  # URL complète du fichier
            output_path = os.path.join(data_dir, filename)  # Chemin local pour enregistrer le fichier
            try:
                # Téléchargement du fichier depuis l'URL et sauvegarde locale
                urllib.request.urlretrieve(file_url, output_path)
                print(f"Downloaded {filename}")  # Confirmation du téléchargement réussi
            except Exception as e:
                # Gestion des échecs de téléchargement
                print(f"Failed to download {filename}: {e}")

# Fonction pour uploader les fichiers sur un serveur MinIO
def write_data_minio(bucket_name: str, folder_path: str):
    """Charge les fichiers .parquet téléchargés sur un bucket MinIO."""
    client = Minio(
        "localhost:9000",         # Adresse du serveur MinIO
        secure=False,              # Connexion non sécurisée (HTTP)
        access_key="minio",       # Clé d'accès MinIO
        secret_key="minio123"     # Clé secrète MinIO
    )
    bucket: str = "newyork-data-bucket"  # Nom du bucket MinIO cible
    
    # Vérifier l'existence du bucket, le créer s'il n'existe pas
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)  # Création du bucket
    else:
        print("Bucket " + bucket + " existe déjà")
    
    # Parcours des fichiers dans le dossier et upload sur MinIO
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)  # Chemin complet du fichier
        
        # Vérification que le fichier est bien un fichier .parquet
        if os.path.isfile(file_path) and file_name.endswith(".parquet"):
            try:
                # Upload du fichier sur MinIO
                client.fput_object(
                    bucket_name=bucket_name,   # Nom du bucket cible
                    object_name=file_name,    # Nom de l'objet dans MinIO
                    file_path=file_path       # Chemin local du fichier
                )
                print(f"Fichier '{file_name}' téléchargé dans le bucket '{bucket_name}'.")
            except Exception as e:
                # Gestion des échecs d'upload
                print(f"Erreur lors du téléchargement de {file_name}: {e}")

if __name__ == '__main__':
    sys.exit(main())  # Exécution de la fonction principale et gestion de la sortie
