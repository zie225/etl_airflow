from extract import data_creator, data_creator_1

from transform import Data_Quality, Data_Quality_1, Transform_df, Transform_df_1
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3
from time import time
from sqlalchemy import create_engine
import psycopg2
import random
import uuid
from extract import data_creator, data_creator_1
import pandas as pd 

pg_user = "airflow"
pg_password = "airflow"
pg_host = "localhost"
pg_port = "5432"
pg_db = "poll_data"
#pg_table=


engine = create_engine(
    f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")

con = engine.connect()


pg_user = "airflow"
pg_password = "airflow"
pg_host = "localhost"
pg_port = "5432"
pg_db = "poll_data"
#pg_table=


engine = create_engine(
    f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")

con = engine.connect()


if __name__ == "__main__":

    #Importing the pollution data and georeference data from the Extract.py
    load_df = data_creator()
    load_df_1 = data_creator_1()

    if(Data_Quality(load_df) == False):
        raise ("Failed at Data Validation")
    Transformed_df = Transform_df(load_df)

    if(Data_Quality_1(load_df_1) == False):
        raise ("Failed at Data Validation")
    Transformed_df_1 = Transform_df_1(load_df_1)

    #The Two Data Frame that need to be Loaded in to the DataBase

    #Loading into Database

    
    #print(Transformed_df.to_sql(con=con, name="pollution_data", if_exists="replace"))

    
    #print(Transformed_df_1.to_sql(con=con, name="geo_ref", if_exists="replace"))

    #Load Data pollution into postgres
    try:
        
        print(Transformed_df.to_sql(
            con=con, name="pollution_data", if_exists="replace"))

    except:
        print(" Pollution Data  already exists in the database")



        #Load geoData  into postgres

    try:
        
        print(Transformed_df_1.to_sql(
            con=con, name="georef_data", if_exists="replace"))
    except:
        print(" Pollution   GeoData  already exists in the database")

    con.close()
    print("Close database successfully")    
