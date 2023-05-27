import os
import requests
import pandas as pd
from dotenv import load_dotenv
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
# Load variables from .env
data = pd.read_csv("Donnees/mesure.csv", sep=";")
data = data[["nom_com", "nom_station", "date_debut", "valeur"]]
print(data.columns.tolist())
data = data.groupby(["nom_station", 'date_debut'])
meanvalue = data.mean("valeur")
print(meanvalue)


data = {'colonne1': ['A', 'B', 'A', 'B', 'A', 'C'],
        'Valeur': [10, 20, 30, 40, 50, 54]}


df = pd.DataFrame(data)
df = df.groupby("colonne1")
df = df.mean("valeur")
print(df)
moyennes_sup_10 = df > 40
print(moyennes_sup_10)
alert = moyennes_sup_10 = False
print(alert)
