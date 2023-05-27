import requests
import json
import pandas as pd
import datetime
import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
# For google storage
credential_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
id_ensemble_des_donnee = os.getenv("ID_ESPACE_DES_DONNEES")
client = bigquery.Client()
mon_du_bucket_des_fichiers_a_traiter = os.getenv("NOM_DU_BUCKET_DES_FICHIERS_À_TRAITER")


def load_commune():
    url = 'https://paysdelaloire.opendatasoft.com/api/explore/v2.1/catalog/datasets/234400034_communes-des-pays-de-la-loire/exports/csv?lang=fr&facet=facet(name%3D%22insee_comm%22%2C%20disjunctive%3Dtrue)&facet=facet(name%3D%22nom_comm%22%2C%20disjunctive%3Dtrue)&refine=insee_dep%3A%2244%22&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B'
    response = requests.get(url)
    with open("./Donnees/commune.csv", "wb") as file:
        file.write(response.content)
    data = pd.read_csv("Donnees/commune.csv", sep=";")
    data = data.rename(
    columns={'INSEE': 'code_commune', 'Libellé commune': 'nom_commune', 'Code département': 'code_departement',
                 'Périmètres': 'perimetre', 'Superficie hectares': 'superficie'})
    data = data.assign(nom_departement="Loire-Atlantique")
    colonnes = ['epci', 'Communes nouvelles', 'Geo Shape']
    for c in colonnes:
        if c in data.columns:
            data.drop(c, axis=1, inplace=True)
    print("Creation du schema et l'import des données dans Bigquery")
    schema_table_commune = [
        bigquery.SchemaField("nom_commune", "STRING"),
        bigquery.SchemaField("code_commune", "STRING"),
        bigquery.SchemaField("superficie", "FLOAT"),
        bigquery.SchemaField("periode", "STRING"),
        bigquery.SchemaField("nom_departement", "STRING"),
        bigquery.SchemaField("code_departement", "STRING"),
        bigquery.SchemaField("Localisation", "STRING")
    ]
    table_ref = id_ensemble_des_donnee + ".D_temps"
    table = bigquery.Table(table_ref, schema=schema_table_commune)
    table = client.create_table(table, exists_ok=True)
    data.to_gbq(id_ensemble_des_donnee + ".D_commune", if_exists='replace')


def load_population():
    url = 'https://data.paysdelaloire.fr/api/explore/v2.1/catalog/datasets/12002701600563_population_pays_de_la_loire_2019_communes_epci/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B'
    response = requests.get(url)
    with open("./Donnees/population.csv", "wb") as file:
        file.write(response.content)
    data = pd.read_csv("Donnees/population.csv", sep=";")
    # Suppression des colonnes
    colonnes = ['Code arrondissement', 'epci', 'Code région', 'Code canton', 'geo_point_2d', 'Nom de la région', 'Population comptée à part', 'Population municipale', 'Geo Shape']
    for c in colonnes:
        if c in data.columns:
            data.drop(c, axis=1, inplace=True)
    data = data.rename(columns={'Code commune': 'code_commune', 'Nom de la commune': 'nom_commune', 'Code département': 'code_departement',
                 'Population totale': 'population', 'departement': 'nom_departement'})
    data = data.drop(data[data['code_departement'] != 44].index)
    print(data.columns.tolist())
    print("Creation du schema et l'import des données dans Bigquery")
    schema_table_population = [
        bigquery.SchemaField("nom_commune", "STRING"),
        bigquery.SchemaField("code_commune", "STRING"),
        bigquery.SchemaField("population", "FLOAT"),
        bigquery.SchemaField("nom_departement", "STRING"),
        bigquery.SchemaField("code_departement", "STRING"),
    ]
    table_ref = id_ensemble_des_donnee + ".D_temps"
    table = bigquery.Table(table_ref, schema=schema_table_population)
    table = client.create_table(table, exists_ok=True)
    data.to_gbq(id_ensemble_des_donnee + ".D_population", if_exists='replace')


print("Ce programme permet de telecharger les communes et les populations des pays de la loire et les importer dans BigQuery")
print("\nTelechargement des communes. Patienter....")
load_commune()
print("\n Telechargement des population")
load_population()
print("\n Fin du programme")



