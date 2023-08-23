import pandas as pd
import numpy as np
import argparse
#import geopandas as gpd

df = pd.read_csv("data.csv",sep=';')
#df.date_run = pd.to_datetime(df.date_run)
#df.date_ech = pd.to_datetime(df.date_ech)

#gdf = gpd.read_file("georef-france-commune-millesime.shp")
#gdf = gdf.to_csv("georef.csv")
#print(gdf)
gdf = pd.read_csv("georef.csv")
print(gdf)


def data_creator():
    #new_df = df.iloc[np.random.randint(0,df.shape[0])]
    new_df=df
    return new_df


def data_creator_1():
    #new_df_1 = gdf.iloc[np.random.randint(0,gdf.shape[0])]
    new_df_1=gdf
    return new_df_1
