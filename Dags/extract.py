import pandas as pd
import numpy as np
import argparse
import os.path
#import geopandas as gpd

df = pd.read_csv("/opt/airflow/data/data.csv",sep=';')
#df.date_run = pd.to_datetime(df.date_run)
#df.date_ech = pd.to_datetime(df.date_ech)

#gdf = gpd.read_file("georef-france-commune-millesime.shp")
#gdf = gdf.to_csv("georef.csv")
#print(gdf)
gdf = pd.read_csv("/opt/airflow/data/georef.csv")


def data_creator():
    #new_df = df.iloc[np.random.randint(0,df.shape[0])]
    new_df=df
    return new_df


def data_creator_1():
    #new_df_1 = gdf.iloc[np.random.randint(0,gdf.shape[0])]
    new_df_1=gdf
    return new_df_1


def save_files(df_list):
    '''
    accepts dataframe list as input
    saves each dataframe in the tmp folder as csv
    the file name corresponds to the dataframe "name" attribute
    '''
    [df.to_csv('/opt/airflow/data/' + df.name + '.csv',
               sep=';', index=False) for df in df_list]


def load_files(names_list):
    '''
    accepts a list of names (str) as input
    load each csv file from the tmp folder with the input names
    returns a list of loaded dataframes
    '''
    df_list = []
    [df_list.append(pd.read_csv("/opt/airflow/data/" + name + ".csv"))
     for name in names_list if os.path.isfile('/opt/airflow/data/' + name + '.csv')]

    return df_list


print(data_creator())
