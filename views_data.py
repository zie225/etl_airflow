
import pandas as pd
from time import time
from sqlalchemy import create_engine
import psycopg2
import random
import uuid
import pandas as pd
from datetime import datetime, timedelta
import geopandas as gpd
import pandas
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import sqlite3


# Connexion à la base de données
conn = sqlite3.connect('pollution.sqlite')

# Création d'un curseur
cur = conn.cursor()

# Exécution d'une requête
cur.execute("SELECT * FROM pollution_data")

# Récupération des résultats
#results = pd.DataFrame(cur.fetchall(),columns=['code_insee', 'val_ind', 'conc_pm10', 'conc_pm25', 'conc_no2',
#'conc_so2', 'conc_o3', 'poll_resp', 'date_run', 'date_ech
results = pd.DataFrame(cur.fetchall())

print(results.head())
# Fermeture de la connexion
#conn.close()

# Affichage des résultats
#for row in results:
#print(row)
