from extract import data_creator, data_creator_1
import pandas as pd 

# Set of Data Quality Checks Needed to Perform Before Loading
# Set of Data Quality Checks Needed to Perform Before Loading


def Data_Quality(load_df):
    #Checking Whether the DataFrame is empty
    if load_df.empty:
        print('No pollution data Extracted')
        return False

#Checking for Nulls in our data frame
    if load_df.isnull().values.any() == True:
        print("Null values found on pollution data")
        print("data pollution will be pretraited....")
    else:
        print("Not Null values found on pollution data")
        #raise Exception("Null values found")
# Set of Data Quality Checks Needed to Perform Before Loading


def Data_Quality_1(load_df_1):

    #Checking Whether the DataFrame is empty
    if load_df_1.empty:
        print('No georeference data Extracted')
        return False

    #Checking for Nulls in our data frame
    if load_df_1.isnull().values.any() == True:
        print("Null values found on georeference data")
        print("georeference data will be pretraited.....")
    else:

        print("Not Null values found on georeference data")

        #raise Exception("Null values found")
    
    
    #Checking for Nulls in our data frame 
    #if load_df_1.isnull().values.any():
        #raise Exception("Null values found")

# Writing some Transformation Queries to get the count of artist
def Transform_df(load_df):

    #Applying transformation 
    load_df.date_run = pd.to_datetime(load_df.date_run)
    load_df.date_ech = pd.to_datetime(load_df.date_ech)

    

    return load_df.date_run,load_df.date_ech


def Transform_df_1(load_df_1):
    
    #Applying transformation 
    #load_df_1.drop('Unnamed: 0', axis=1, inplace=True)
    load_df_1.dropna(axis=1, inplace=True)

    return load_df_1

if __name__ == "__main__":

    #Importing the songs_df from the Extract.py
    load_df=data_creator()
    load_df_1 = data_creator_1()
    data_q=Data_Quality(load_df)
    print(data_q)
    data_q_1=Data_Quality_1(load_df_1)
    print(data_q_1)
    #calling the transformation
    Transformed_df=Transform_df(load_df)    
    print(Transformed_df)
    Transformed_df_1=Transform_df_1(load_df_1) 
    print(Transformed_df_1)