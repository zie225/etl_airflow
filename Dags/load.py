from extract import data_creator, data_creator_1,save_files,load_file

from transform import Data_Quality,Data_Quality_1, Transform_df, Transform_df_1
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///pollution.sqlite"
DATABASE_LOCATION_1 = "sqlite:///georef.sqlite"

if __name__ == "__main__":

    #Importing the songs_df from the Extract.py
    load_df = data_creator()
    print(save_files(load_df))
    load_df_1 = data_creator_1()
    print(save_files(load_df_1))

    if(Data_Quality(load_df) == False):
        raise ("Failed at Data Validation")
    Transformed_df = Transform_df(load_df)

    #if(Data_Quality_1(load_df_1) == False):
        #raise ("Failed at Data Validation")
    Transformed_df_1 = Transform_df_1(load_df_1)

    #The Two Data Frame that need to be Loaded in to the DataBase

#Loading into Database
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('pollution.sqlite')
    cursor = conn.cursor()

    engine_1 = sqlalchemy.create_engine(DATABASE_LOCATION_1)
    conn_1 = sqlite3.connect('georef.sqlite')
    cursor_1 = conn_1.cursor()

    #SQL Query to Create pollution

    sql_query = """
    CREATE TABLE IF NOT EXISTS pollution_data(
	code_insee BIGINT,
	val_ind BIGINT,
	conc_pm10 FLOAT(53),
	conc_pm25 FLOAT(53),
	conc_no2 FLOAT(53),
	conc_so2 FLOAT(53),
	conc_o3 FLOAT(53),
	poll_resp TEXT,
	date_run DATE,
	date_ech DATE
    ) """



    #SQL Query to Create georef

    sql_query_1 = """
    CREATE TABLE IF NOT EXISTS georef_data(
    year     BIGINT,
	reg_code BIGINT,
	reg_name TEXT,
	dep_code BIGINT,
	dep_name TEXT,
	arrdep_code BIGINT,
	arrdep_name TEXT,
	ze2020_code BIGINT,
	ze2020_name TEXT,
	bv2012_code BIGINT,
	bv2012_name TEXT,
	epci_code BIGINT,
	epci_name TEXT,
	com_code BIGINT,
	com_current BIGINT,
	com_name TEXT,
	com_name_up TEXT,
	com_name_lo TEXT,
	com_area_co TEXT,
	com_type TEXT,
	com_cateaav TEXT,
	com_uu2020_ TEXT,
	com_aav2020 BIGINT,
	com_cv_code BIGINT,
	com_in_ctu TEXT,
	com_siren_c BIGINT,
	com_is_moun TEXT,
	geometry TEXT
    )"""

    cursor.execute(sql_query)
    cursor_1.execute(sql_query_1)
    print("Opened database successfully")


    """
    # Store the synthetic data in the 'pollution' table
    for index, row in load_df.iterrows():
        
        insert_query = '''
    INSERT INTO customers ('code_insee', 'val_ind', 'conc_pm10', 'conc_pm25', 'conc_no2',
       'conc_so2', 'conc_o3', 'poll_resp', 'date_run', 'date_ech') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    
    '''
        cursor.execute(insert_query, (
        row['code_insee'],
        row['val_ind'],
        row['conc_pm10'],
        row['conc_pm25'],
        row['conc_no2'],
        row['conc_so2'],
        row['conc_o3'],
            row['poll_resp'],
        row['date_run'],
        row['subscription_end'],
        row['balance'],
        row['date_ech']
    ))

    # Commit the changes and close the cursor
    conn.commit()
                
                """

    #We need to only Append New Data to avoid duplicates
    #columns = ['code_insee', 'val_ind', 'conc_pm10', 'conc_pm25', 'conc_no2',
               #'conc_so2', 'conc_o3', 'poll_resp', 'date_run', 'date_ech']
    
    #columns_1=['year', 'reg_code', 'reg_name', 'dep_code', 'dep_name', 'arrdep_code',
       #'arrdep_name', 'ze2020_code', 'ze2020_name', 'bv2012_code',
       #'bv2012_name', 'epci_code', 'epci_name', 'com_code', 'com_current',
       #'com_name', 'com_name_up', 'com_name_lo', 'com_area_co', 'com_type',
       #'com_cateaav', 'com_uu2020_', 'com_aav2020', 'com_cv_code',
       #'com_in_ctu', 'com_siren_c', 'com_is_moun', 'geometry']

    try:
        Transformed_df.to_sql("pollution_data", engine,
                       index=False, if_exists='append',index_label=columns)
    except:
        print(" Pollution Data  already exists in the database")


    try:
        
        Transformed_df_1.to_sql("georef_data", engine_1,
                              index=False, if_exists='append',index_label=columns_1 )
    except:
        
        print("Georef Data already exists in the database")    


    #cursor.execute('DROP TABLE pollution_data')
    #cursor.execute('DROP TABLE georef_data')

    conn.close()
    print("Close database successfully")
