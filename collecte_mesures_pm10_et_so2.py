import requests
import json
import pandas as pd
import datetime
import os
from dotenv import load_dotenv
load_dotenv()
# For google storage
credential_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
mon_du_bucket_des_fichiers_a_traiter = os.getenv("NOM_DU_BUCKET_DES_FICHIERS_À_TRAITER")
def loadData(url, filename):
    i: int = 0
    all_data = []
    while url is not None:
        response = requests.get(url)
        if response.status_code == 200 and response.headers.get('content-type') == 'application/json':
            data = response.json()
            if 'results' in data:
                results = data['results']
                all_data.extend(results)
                if not results:
                    break
                url = data['next']
            else:
                break
            i = i + 1
            if i == 1:
                print("Itération: ", end='')
            print(i, " ", end='')
    with open("./Donnees/" + filename+".json", "w") as outfile:
        json.dump(all_data, outfile)


annee = datetime.date.today().year
trimestre_courant = (datetime.date.today().month - 1)/4 + 1
print("Ce programme permet de telecharger les données: PM10 et SO2 sur AirPL, fusionner les deux fichiers et les importer dans google storage\n")
print("Choisir l'année: \n 1- Année courante: ", annee, "\n 2- Année précedente:", annee-1, "\n")
choixAnnee = input()
print(choixAnnee)
if choixAnnee == "1" or choixAnnee == "2":
    if choixAnnee == "2":
        anneeDonnee = annee-1
    else:
        anneeDonnee = annee
else:
   exit(0)
print("Choisir le trimestre de l'année:", anneeDonnee)
print("1: Janvier-Mars")
print("2: Avril-Juin")
print("3: Juillet-Septembre")
print("4: Octobre - Decembre")
print("0: Quitter")
while True:
    try:
        choixTrimestre = int(input("\n Votre choix 1 à 4\n"))
    except ValueError:
        print("Saisir un nombre \n")
    if choixTrimestre == 0:
        exit(0)
    elif choixTrimestre <= 4:
        if anneeDonnee == annee and choixTrimestre >= trimestre_courant:
            print("Toutes les données de ce trimestre ne sont pas  disponible")
        else:
            break
    else:
        print("Saisir une valeur comprise entre 1 et 4 ou 0 pour quitter")
if choixTrimestre == 1:
    date_heure_tu__range = str(anneeDonnee)+"-1-1,"+str(anneeDonnee)+"-3-31 23:00:00"
    periode = "janvier_mars_" + str(anneeDonnee)
elif choixTrimestre == 2:
    date_heure_tu__range = str(anneeDonnee) + "-4-1," + str(anneeDonnee) + "-6-30 23:00:00"
    periode = "avril_juin_" + str(anneeDonnee)
elif choixTrimestre == 3:
    date_heure_tu__range = str(anneeDonnee) + "-7-1," + str(anneeDonnee) + "-9-31 23:00:00"
    periode = "juillet_septembre_" + str(anneeDonnee)
else:
    date_heure_tu__range = str(anneeDonnee) + "-10-1," + str(anneeDonnee) + "-12-31 23:00:00"
    periode = "octobre_decembre_" + str(anneeDonnee)
print(date_heure_tu__range)
print("#################################################################")
print("Etes vous sure de vouloir telecharger les données des polluants du trimestre", choixTrimestre, "-", anneeDonnee, "O/N")
choix = input()
if choix.lower() == 'o':
    print("Techargement des données du polluant PM10 en cours ......")
    urlPM10 = 'https://data.airpl.org/api/v1/mesure/horaire/?&code_configuration_de_mesure__code_point_de_prelevement__code_polluant=24&date_heure_tu__range='+ date_heure_tu__range +'&code_configuration_de_mesure__code_point_de_prelevement__code_station__code_commune__code_departement__in=44,&export=json'
    loadData(urlPM10, "PM10" + date_heure_tu__range)
    print("\n Techargement des données du polluant S02 en cours ......")
    urlSO2 = 'https://data.airpl.org/api/v1/mesure/horaire/?&code_configuration_de_mesure__code_point_de_prelevement__code_polluant=01&date_heure_tu__range=' + date_heure_tu__range + '&code_configuration_de_mesure__code_point_de_prelevement__code_station__code_commune__code_departement__in=44,&export=json'
    loadData(urlSO2, "S02" + date_heure_tu__range)
else:
    exit(0)
print("Fin de telechargement")
print("Fusion des fichiers: PM10" + date_heure_tu__range, ".json et SO2" + date_heure_tu__range + ".json et import dans google storage")
print("Traitement encours .....")
# Chargement du fichier SO2 with pendas
dataSO2 = pd.read_json("./Donnees/S02" + date_heure_tu__range + ".json")
# Chargement du fichier PM10 with pendas
dataPM10 = pd.read_json("./Donnees/PM10" + date_heure_tu__range + ".json")
# Fusion
fusion = pd.concat([dataPM10, dataSO2], ignore_index=True)
# Ajout d'un champ periode pour l'horodatage
fusion = fusion.assign(periode=periode)
# Import dans google storage
fusion.to_csv("gs://" + mon_du_bucket_des_fichiers_a_traiter +"/pm10_so2_data" + periode + ".csv")
print("Fin de fusion et import ")
