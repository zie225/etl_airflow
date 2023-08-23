import os.path
import argparse
import numpy as np
import pandas as pd
import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine
from extract import data_creator, data_creator_1
from transform import Data_Quality,Data_Quality_1, Transform_df, Transform_df_1

from airflow.utils.dates import days_ago
from data_etl import pollution_etl
import os

#AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
#RAW_DATA_PATH_CSV = AIRFLOW_HOME + "Dags/data/data.csv"
#GEO_DATA_PATH = AIRFLOW_HOME + "Dags/data/georef.csv"

#import geopandas as gpd

#df = pd.read_csv(RAW_DATA_PATH_CSV, sep=';')
#df.date_run = pd.to_datetime(df.date_run)
#df.date_ech = pd.to_datetime(df.date_ech)

#gdf = gpd.read_file("georef-france-commune-millesime.shp")
#gdf = gdf.to_csv("georef.csv")
#print(gdf)
#gdf = pd.read_csv(GEO_DATA_PATH)


#def data_creator():
    #new_df = df.iloc[np.random.randint(0,df.shape[0])]
    #new_df = df
    #return new_df

#def data_creator_1():
    #new_df_1 = gdf.iloc[np.random.randint(0,gdf.shape[0])]
    #new_df_1=gdf
    #return new_df_1

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2023, 8, 20),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

dag = DAG(
    'pollution_dag',
    default_args=default_args,
    description='pollution ETL process 1-min',
    schedule_interval=dt.timedelta(minutes=50),
)


def ETL():
    print("started")
    #load_df=load_file(load_df)
    load_df = data_creator()
    #load_df_1=load_file(load_df_1)
    load_df_1 = data_creator_1()
    #Data quality
    data_q = Data_Quality(load_df)
    print(data_q)
    data_q_1 = Data_Quality_1(load_df_1)
    print(data_q_1)

    #Data Transform
    Transformed_df = Transform_df(load_df)
    Transformed_df_1 = Transform_df_1(load_df_1)
    #print(df)
    conn = BaseHook.get_connection('postgre_sql')
    engine = create_engine(
        f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    Transformed_df.to_sql('pollution_data', engine, if_exists='replace')
    Transformed_df_1.to_sql('georef_data', engine, if_exists='replace')



with dag:
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgre_sql',
        sql="""
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

    )

    create_table_1 = PostgresOperator(
        task_id='create_table_1',
        postgres_conn_id='postgre_sql',
        sql="""
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

    )

    run_etl = PythonOperator(
        task_id='etl_dags',
        python_callable=ETL,
        dag=dag,
    )

    create_table >>create_table_1 >>run_etl
