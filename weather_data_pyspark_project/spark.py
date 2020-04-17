from pyspark.sql.functions import max


def find_max_in_df(df, column):
    """Function to return the maximum value of a column in the input data frame.
    
    df -- Data frame
    
    column (str) -- Column name to check for the maximum value.
    """
    max_value = df.select(max(column)).first()[0]
    return max_value

def filter_df_based_on_column_value(df, column, value):
    """Function to filter rows from the input dataframe based on a value defined for a column. For example, give me records which have a given temprature.
    
    df -- Data frame
    
    column (str) -- Column name to check for the given value.
    
    value (num) -- Given value for filtering
    """
    filtered_df = df.filter(df[column] == value)
    return filtered_df

def select_distinct_column_from_df(df, column):
    """Function to return a column with distinct values from the input dataframe.

    df -- Data frame
    
    column (str) -- Column name to fetch from the input data frame.
    """
    df = df.select(column).distinct()
    return df

def convert_timestamp_column_to_date_column(df, column):
    """Function to receive column with timestamp type and return column with date type.

    df -- Data frame
    
    column (str) -- Column name with timestamp type in the input data frame.
    """
    df = df.select(df[column].cast('date')).distinct()
    return df

