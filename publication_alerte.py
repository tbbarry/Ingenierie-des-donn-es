import json
import os
import datetime as dt
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
from google.cloud import pubsub_v1
import schedule
import time
import pandas as pd
from dotenv import load_dotenv
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
load_dotenv()
# For google storage
credential_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
seuil = os.getenv("SEUIL")
frequence = os.getenv("FREQUENCE_HORAIRE")
debut_execution = os.getenv("DEBUT_EXECUTION")
# Définir le nom du sujet et le message à envoyer
topic_name = os.getenv("TOPIC_NAME")
# Instancier un client Pub/Sub
publisher = pubsub_v1.PublisherClient()


def my_task():
    dateJ = dt.date.today()
    all_data = []
    urlPM10 = 'https://data.airpl.org/api/v1/mesure/horaire/?&code_configuration_de_mesure__code_point_de_prelevement__code_polluant=01&date_heure_tu__range='+ str(dateJ) + ',' + str(dateJ) + '%2023:00:00&code_configuration_de_mesure__code_point_de_prelevement__code_station__code_commune__code_departement__in=44,&export=json'
    response = requests.get(urlPM10)
    if response.status_code == 200 and response.headers.get('content-type') == 'application/json':
        data = response.json()
        if 'results' in data:
            results = data['results']
            all_data.extend(results)
        with open("./Donnees/mesureSO2.json", "w") as outfile:
            json.dump(all_data, outfile)

    data = pd.read_json("./Donnees/mesureSO2.json")
    data['date_heure_local'] = pd.to_datetime(data['date_heure_local'], utc=False)
    data['heure'] = data['date_heure_local'].dt.hour
    # Filtrer pour prendre que les 3h dernières heures
    maintenant = datetime.now()
    heure_precedente = maintenant - timedelta(hours=3)
    # Supprimer les données de l'heure courante
    data = data.drop(data[data['heure'] == maintenant.hour].index)
    # Supprimer les données reccueillies il ya plus de 3h
    data = data.drop(data[data['heure'] < heure_precedente.hour].index)
    data = data[["nom_commune", "nom_station", "date_heure_local", "valeur_originale", "heure"]]
    data = data.groupby(["nom_station", "nom_commune", "date_heure_local"])
    mean_value = data.mean("valeur_originale")
    print("Données des polluants à l'instant t:")
    print(mean_value)
    # Parcourir les groupes et afficher la moyenne de chaque groupe
    for group, mean_row in mean_value.iterrows():
        condition_respectee = (mean_row["valeur_originale"] >= float(seuil))
        if(condition_respectee):
            # Publication du message
            message = "Alerte seuil critique ! Station: " + group[0] + "Commune: " + group[1], "Date:" + str(group[2]) + "Valeur: " + str(mean_row['valeur_originale'])
            publier(message)


def publier(message):
    sujet = publisher.publish(topic_name, str(message).encode('utf-8'))
    print("Message publié")


def main():
    schedule.every().day.at(str(debut_execution)).do(my_task)
    schedule.every(frequence).hour.do(my_task)
    print("Patienter les données sont chargées chaque 3h")
    while True:
        schedule.run_pending()
        time.sleep(1)


main()
