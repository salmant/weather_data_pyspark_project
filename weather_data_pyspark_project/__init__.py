import os
import yaml
from pprint import pprint

__author__ = 'Salman Taherizadeh'

"""
    config_file.yml: The YAML configuration file which defines the following parameters to interact with the
        execution environment. Therefore, there is an expectation a YAML file with
        the following parameters:

        * CSV_LOCATION - Location of the raw data which is in the CSV format.
        * PARQUET_LOCATION - Location where the parquet data formatted should be stored.
        * RESULTS_LOCATION - Location where the results (hottest day, hottest temprature and hottest region) should be stored.
        * OVERWRITE - Boolean identifying whether the generated data stored in PARQUET_LOCATION and RESULTS_LOCATION should be overwritten (True) or not (False) if there already exists some data in these folders.

    print_config_file - Boolean identifying whether print out the configuration once saved (True) or not (False). Defaults to ``True``.
"""

conf = {
    "CSV_LOCATION": "",
    "PARQUET_LOCATION": "",
    "RESULTS_LOCATION": "",
    "OVERWRITE": True,
}

weather_data_columns = [
    'ForecastSiteCode',
    'ObservationTime',
    'ObservationDate',
    'WindDirection',
    'WindSpeed',
    'WindGust',
    'Visibility',
    'ScreenTemperature',
    'Pressure',
    'SignificantWeatherCode',
    'SiteName',
    'Latitude',
    'Longitude',
    'Region',
    'Country',
]

config_file = "config_file.yml"
print_config_file = True

if not os.path.exists(config_file):
    raise ValueError('configuration file named "config_file.yml" is not found')

with open(config_file, 'r') as f:
    config = yaml.safe_load(f.read())
    conf['CSV_LOCATION'] = config['CSV_LOCATION']
    conf['PARQUET_LOCATION'] = config['PARQUET_LOCATION']
    conf['RESULTS_LOCATION'] = config['RESULTS_LOCATION']
    conf['OVERWRITE'] = bool(config['OVERWRITE'])

if print_config_file:
    pprint(conf)

