Function: 
get_spark_session

Explain: 
Helper function to create a Spark session using PySpark.
#######################################
Function: 
read_csv_into_df

Explain: 
Helper function to read csv files into a data frame.

Args:
session -- PySpark session object
csvpath (str) -- CSV file(s) path
#######################################
Function: 
store_df_as_parquet

Explain: 
Helper function to store a data frame as parquet.

Args:
df -- Data frame
parquetpath (str) -- Parquet path
#######################################
Function: 
read_parquet_into_df

Explain: 
Helper function to read the parquet data and return a data frame.

Args:
session -- PySpark session object
parquetpath (str) -- Parquet path
columns (list, optional) -- List of columns to retrieve. If None, all columns are returned.
#######################################
Function: 
output_validation

Explain: 
Helper function to check path if exists. It raises ValueError if overwrite is False. Otherwises removes the existing file.

Args:
outpath (str) -- Path of ourput file.
overwrite (bool) -- Whether to raise an error or remove existing data.
#######################################
Function: 
find_max_in_df

Explain: 
Function to return the maximum value of a column in the input data frame.

Args:
df -- Data frame
column (str) -- Column name to check for the maximum value.
#######################################
Function: 
filter_df_based_on_column_value

Explain: 
Function to filter rows from the input dataframe based on a value defined for a column. For example, give me records which have a given temprature.

Args:
df -- Data frame
column (str) -- Column name to check for the given value.
value (num) -- Given value for filtering
#######################################
Function: 
select_distinct_column_from_df

Explain: 
Function to return a column with distinct values from the input dataframe.

Args:
df -- Data frame
column (str) -- Column name to fetch from the input data frame.
#######################################
Function: 
convert_timestamp_column_to_date_column

Explain: 
Function to receive column with timestamp type and return column with date type.

Args:
df -- Data frame
column (str) -- Column name with timestamp type in the input data frame.
#######################################


