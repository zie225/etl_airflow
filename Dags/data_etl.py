from extract import data_creator, data_creator_1
from transform import Data_Quality,Data_Quality_1, Transform_df, Transform_df_1
import pandas as pd
import numpy as np
import argparse
#import geopandas as gpd

"""df = pd.read_csv("data.csv",sep=';')
df.head()
#df.date_run = pd.to_datetime(df.date_run)
#df.date_ech = pd.to_datetime(df.date_ech)

gdf = gpd.read_file("georef-france-commune-millesime.shp")
gdf = gdf.to_csv("C:/Users/ZIE MAMADOU/Desktop/airflow/Dags/georef.csv")
print(gdf)
gdf = pd.read_csv("georef.csv")


def data_creator():
    #new_df = df.iloc[np.random.randint(0,df.shape[0])]
    new_df=df
    return new_df


def data_creator_1():
    #new_df_1 = gdf.iloc[np.random.randint(0,gdf.shape[0])]
    new_df_1=gdf
    return new_df_1

    """

# Set of Data Quality Checks Needed to Perform Before Loading

"""
def Data_Quality(load_df):
    #Checking Whether the DataFrame is empty
    if load_df.empty:
        print('No pollution data Extracted')
        return False

#Checking for Nulls in our data frame
    if load_df.isnull().values.any():
        raise Exception("Null values found")
# Set of Data Quality Checks Needed to Perform Before Loading


def Data_Quality_1(load_df_1):
    #Checking Whether the DataFrame is empty
    if load_df_1.empty:
        print('No georeference data Extracted')
        return False


    #Checking for Nulls in our data frame
    #if load_df_1.isnull().values.any():
        #raise Exception("Null values found")

# Writing some Transformation Queries to get the count of artist
"""

"""
def Transform_df(load_df):

    #Applying transformation
    load_df.date_run = pd.to_datetime(load_df.date_run)
    load_df.date_ech = pd.to_datetime(load_df.date_ech)

    return load_df.date_run, load_df.date_ech


def Transform_df_1(load_df_1):

    #Applying transformation
    load_df_1.drop('Unnamed: 0', axis=1, inplace=True)
    load_df_1.dropna(axis=1, inplace=True)

    return load_df_1
"""

def pollution_etl():
    #Importing the pollution data and georeference data from the extract.py
    load_df = data_creator()
    load_df_1 = data_creator_1()
    Data_Quality(load_df)
    Data_Quality_1(load_df_1)
    #calling the transformation
    Transformed_df = Transform_df(load_df)
    Transformed_df_1 = Transform_df_1(load_df_1)
    print( Transformed_df)
    print(Transformed_df_1)
    return  Transformed_df,Transformed_df_1


pollution_etl()
