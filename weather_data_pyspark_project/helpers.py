from weather_data_pyspark_project import weather_data_columns
from pyspark.sql import SparkSession
import os
import shutil
from functools import lru_cache

@lru_cache(maxsize=None)
def get_spark_session():
    """Helper function to create a Spark session using PySpark.
    """
    return (SparkSession.builder
                .master("local")
                .appName("weather_data_pyspark_project")
                .getOrCreate())

def read_csv_into_df(session, csvpath):
    """Helper function to read csv files into a data frame.
    
    session -- PySpark session object
    
    csvpath (str) -- CSV file(s) path
    """
    df = session.read \
        .option("header", "true") \
        .option("inferSchema", value = True) \
        .csv(csvpath)
    return df

def store_df_as_parquet(df, parquetpath):
    '''Helper function to store a data frame as parquet.

    df -- Data frame
    
    parquetpath (str) -- Parquet path
    '''
    df.write.parquet(parquetpath)

def read_parquet_into_df(session, parquetpath, columns=None):
    """Helper function to read the parquet data and return a data frame.

    session -- PySpark session object
    
    parquetpath (str) -- Parquet path
    
    columns (list, optional) -- List of columns to retrieve. If None, all columns are returned.
    """
    if not columns:
        columns = weather_data_columns
    df = session.read.parquet(parquetpath).select(columns)
    return df

def output_validation(outpath, overwrite):
    """Helper function to check path if exists. It raises ValueError if the file already exists and arg 'overwrite' is False. Otherwises removes the existing file.
    
    outpath (str) -- Path of ourput file.
    
    overwrite (bool) -- Whether to raise an error or remove existing data.
    """
    exists = os.path.exists(outpath)

    if exists and overwrite is False:
        raise ValueError('{} already exists. If OVERWRITE would be True, it can remove and replace.\n'.format(outpath))
    if exists and overwrite is True:
        shutil.rmtree(outpath)


