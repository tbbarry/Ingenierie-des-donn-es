from datetime import datetime

import pandas as pd
import os
from google.cloud import storage, bigquery
from dotenv import load_dotenv
load_dotenv()
# For google storage
credential_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
mon_du_bucket_des_fichiers_a_inserer_bigquery = os.getenv("NOM_DU_BUCKET_DES_FICHIERS_TEMPORAIRE_A_INSERER_DANS_BIGQUERY")
id_ensemble_des_donnee = os.getenv("ID_ESPACE_DES_DONNEES")

client = bigquery.Client()


# Creation des schemas
def creation_schema():
    schema_dimension_temps = [
        bigquery.SchemaField("date_debut", "TIMESTAMP"),
        bigquery.SchemaField("jour", "INTEGER"),
        bigquery.SchemaField("mois", "INTEGER"),
        bigquery.SchemaField("annee", "INTEGER"),
        bigquery.SchemaField("heure", "INTEGER"),
        bigquery.SchemaField("periode", "STRING")
    ]
    table_ref = id_ensemble_des_donnee + ".D_temps"
    table = bigquery.Table(table_ref, schema=schema_dimension_temps)
    table = client.create_table(table, exists_ok=True)

    schema_dimension_station = [
        bigquery.SchemaField("code_station", "STRING"),
        bigquery.SchemaField("code_commune", "INTEGER"),
        bigquery.SchemaField("nom_station", "STRING"),
        bigquery.SchemaField("periode", "STRING")
    ]
    table_ref = id_ensemble_des_donnee + ".D_station"
    table = bigquery.Table(table_ref, schema=schema_dimension_station)
    table = client.create_table(table, exists_ok=True)
    schema_fait_mesures = [
        bigquery.SchemaField("date_debut", "TIMESTAMP"),
        bigquery.SchemaField("code_station", "STRING"),
        bigquery.SchemaField("nom_polluant", "STRING"),
        bigquery.SchemaField("periode", "STRING"),
        bigquery.SchemaField("valeur", "FLOAT"),
    ]
    table_ref = id_ensemble_des_donnee + ".F_mesure"
    table = bigquery.Table(table_ref, schema=schema_fait_mesures)
    table = client.create_table(table, exists_ok=True)

def extract_d_temps(data):
    # Creation du schema (table) s'il n'existe pas
    data['date_debut'] = pd.to_datetime(data['date_debut'], utc=True)
    data['jour'] = data['date_debut'].dt.day
    data['mois'] = data['date_debut'].dt.month
    data['annee'] = data['date_debut'].dt.year
    data['heure'] = data['date_debut'].dt.hour
    dates = data[['date_debut', 'jour', 'mois', 'annee', 'heure', 'periode']]
    dates = dates.drop_duplicates()
    dates.to_gbq(id_ensemble_des_donnee + ".D_temps", if_exists='append')

def extract_d_station(data):
    stations = data[['code_station', 'nom_station', 'code_commune', 'periode']]
    stations = stations.drop_duplicates()
    stations.to_gbq(id_ensemble_des_donnee + ".D_station", if_exists='append')


def extract_f_mesure(data):
    mesures = data[['code_station', 'date_debut', 'nom_polluant', 'valeur', 'periode']]
    mesures.to_gbq(id_ensemble_des_donnee + ".F_mesure", if_exists='append')
    
    
print("Ce programme permet de creer le schema des données dans big query s'il n'existe pas et d'inserer les données dans les tables des dimensions et faits")
storage_client = storage.Client()
bucket = storage_client.bucket(mon_du_bucket_des_fichiers_a_inserer_bigquery)
files = list(bucket.list_blobs())
print("Verification du schema")
creation_schema()
print("\n Les fichiers disponible à inserer")
# Affichage de chaque nom de fichier
i = 0
for file in files:
    print(i+1, file.name, "  ", file.time_created)
    i = i + 1
print("\n Etes vous sure d'inserer les données dans bigquery (o/n) ?")
choix = input()
if choix.lower() == 'o':
    i = 1
    for file in files:
        print("Traitement du fichier: ", i, " en cours ....")
        data = pd.read_csv('gs://' + mon_du_bucket_des_fichiers_a_inserer_bigquery + '/' + file.name)
        # Extraction et insertion dans de la table D_temps
        print("Insertion dans la table dimension: D_temps")
        extract_d_temps(data)
        # Extraction et insertion dans de la table D_station
        print("Insertion dans la table dimension: D_station")
        extract_d_station(data)
        # Extraction et insertion dans de la table F_Mesure
        print("Insertion dans la table des faits: F_mesure")
        extract_f_mesure(data)
        i = i + 1
        blob = bucket.blob(file.name)
        blob.delete()
print('Fin de traitement')
