import os
import sys
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv
load_dotenv()
# For google storage
credential_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
mon_du_bucket_des_fichiers_a_traiter = os.getenv("NOM_DU_BUCKET_DES_FICHIERS_À_TRAITER")
mon_du_bucket_des_fichiers_traites = os.getenv("NOM_DU_BUCKET_DES_FICHIERS_TRAITER")
mon_du_bucket_des_fichiers_a_inserer_bigquery = os.getenv("NOM_DU_BUCKET_DES_FICHIERS_TEMPORAIRE_A_INSERER_DANS_BIGQUERY")

def addNamePolluant(row):
    if row.code_polluant == 24:
        return "PM10"
    elif row['code_polluant'] == 1:
        return "SO2"


print("Ce programme permet d'effectuer des traitements les donnees des polluants PM10 et S02")
storage_client = storage.Client()
bucket = storage_client.bucket(mon_du_bucket_des_fichiers_a_traiter)
files = list(bucket.list_blobs())
if len(files) == 0:
    print("Aucun fichier à traiter")
    exit(40)
print("Voici les fichiers disponible pour le traitement")
print("\n")
# Affichage de chaque nom de fichier
i = 0
for file in files:
    print(i+1, file.name, "  ", file.time_created)
    i = i + 1
print("\n Etes vous sure de vouloir effectuer le traitement  (o/n) ?")
choix = input()
if choix.lower() == 'o':
    i = 0
    for file in files:
        print("Traitement du fichier: ", file.name)
        # Get file with pandas
        print("Chargement du fichier")
        data = pd.read_csv('gs://' + mon_du_bucket_des_fichiers_a_traiter + '/' + file.name)
        # Suppression des doublons
        print("Suppression des doublons")
        data = data.drop_duplicates()
        # Suppression des champs non nécessaire:
        print("Suppression des colonnes non necessaires")
        colonnes = ['code_configuration_de_mesure', 'code_point_de_prelevement', 'Unnamed: 0', 'id',
                    'code_zone_affichage', 'valeur_originale', 'date_heure_tu']
        for c in colonnes:
            if c in data.columns:
                data.drop(c, axis=1, inplace=True)
        print("Les colonnes restantes après la suppression")
        print(data.columns.tolist())
        # Renommer la colone date_heure_local et transformer en date time
        print("Formatage de la colonne date heure local")
        if 'date_heure_local' in data.columns:
            data['date_heure_local'] = pd.to_datetime(data['date_heure_local'], utc=True)
            data = data.rename(columns={'date_heure_local': 'date_debut'})
        # Changement des types
        data = data.astype({'validite': bool, 'valeur': float})
        # Ajout de la colonne nom polluant en fonction du code polluant
        print("Ajout et remplissage de la colonne nom polluat")
        if 'nom_polluant' not in data.columns:
            data['nom_polluant'] = data.apply(addNamePolluant, axis=1)
        # Suppression de toutes les lignes dont validite === false
        data = data.drop(data[data['validite'] == False].index)
        # Enregistrement du fichier dans le dossier des fichiers traiter et dans un dossier temporaire.
        print("Enregistrement du fichier dans le bucket correspondant")
        data.to_csv('gs://' + mon_du_bucket_des_fichiers_traites + '/' + file.name)
        data.to_csv('gs://' + mon_du_bucket_des_fichiers_a_inserer_bigquery + '/' + file.name)
        blob = bucket.blob(file.name)
        print("Suppresion du fichier dans le bucket")
        blob.delete()
    print("Traitement terminé")
else:
    print("Vous avez choisi de ne pas faire le traitement")
print("Fin du programme")
