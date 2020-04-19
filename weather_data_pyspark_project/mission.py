from weather_data_pyspark_project import conf

from weather_data_pyspark_project.helpers import get_spark_session
from weather_data_pyspark_project.helpers import read_csv_into_df
from weather_data_pyspark_project.helpers import store_df_as_parquet
from weather_data_pyspark_project.helpers import read_parquet_into_df
from weather_data_pyspark_project.helpers import output_validation

from weather_data_pyspark_project.spark import find_max_in_df
from weather_data_pyspark_project.spark import filter_df_based_on_column_value
from weather_data_pyspark_project.spark import select_distinct_column_from_df
from weather_data_pyspark_project.spark import convert_timestamp_column_to_date_column

def main():

    print('Mission Started')
    result = {}
    
    # The results formatted based on parquet will be stored 3 different subfolders
    RESULTS_LOCATION_for_Hottest_ObservationDate = conf['RESULTS_LOCATION'] + "/" + "Hottest_" + "ObservationDate"
    RESULTS_LOCATION_for_Hottest_ScreenTemperature = conf['RESULTS_LOCATION'] + "/" + "Hottest_" + "ScreenTemperature"
    RESULTS_LOCATION_for_Hottest_Region = conf['RESULTS_LOCATION'] + "/" + "Hottest_" + "Region"
    
    # Read 3 columns "ObservationDate", "ScreenTemperature" and "Region" from the input date
    session = get_spark_session()
    df = read_csv_into_df(session, conf['CSV_LOCATION'])
    output_validation(conf['PARQUET_LOCATION'], conf['OVERWRITE'])
    store_df_as_parquet(df, conf['PARQUET_LOCATION'])
    df = read_parquet_into_df(session, conf['PARQUET_LOCATION'], ["ObservationDate", "ScreenTemperature", "Region"])
    
    # Fetch the maximum value of column named "ScreenTemperature" as the hottest temperature
    max_value = find_max_in_df(df, "ScreenTemperature")
    
    # Fetch record(s) including the hottest temprature
    df = filter_df_based_on_column_value(df, "ScreenTemperature", max_value)
    
    # Find and store the hottest ObservationDate(s)
    hottest_timestamp_df = select_distinct_column_from_df(df, "ObservationDate")
    hottest_date_df = convert_timestamp_column_to_date_column(hottest_timestamp_df, "ObservationDate")
    print("\n==== Hottest Day(s) =====")
    hottest_date_df.show()
    output_validation(RESULTS_LOCATION_for_Hottest_ObservationDate, conf['OVERWRITE'])
    store_df_as_parquet(hottest_date_df, RESULTS_LOCATION_for_Hottest_ObservationDate)
    result['hottest_date_df'] = hottest_date_df
    
    # Find and store the hottest ScreenTemperature
    hottest_temperature_df = select_distinct_column_from_df(df, "ScreenTemperature")
    print("\n==== Hottest Temperature =====")
    hottest_temperature_df.show()
    output_validation(RESULTS_LOCATION_for_Hottest_ScreenTemperature, conf['OVERWRITE'])
    store_df_as_parquet(hottest_temperature_df, RESULTS_LOCATION_for_Hottest_ScreenTemperature)
    result['hottest_temperature_df'] = hottest_temperature_df
    
    # Find and store the hottest Region(s)
    hottest_region_df = select_distinct_column_from_df(df, "Region")
    print("\n==== Hottest Region(s) =====")
    hottest_region_df.show()
    output_validation(RESULTS_LOCATION_for_Hottest_Region, conf['OVERWRITE'])
    store_df_as_parquet(hottest_region_df, RESULTS_LOCATION_for_Hottest_Region)
    result['hottest_region_df'] = hottest_region_df
    
    print('Mission Ended Successfully - Result is: {}'.format(str(result)))
    return result

if __name__ == "__main__":
    main()

