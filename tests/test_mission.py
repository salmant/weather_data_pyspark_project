import pytest

import logging

import pyspark.sql.functions as F

from weather_data_pyspark_project import conf

from weather_data_pyspark_project.helpers import get_spark_session

from weather_data_pyspark_project.mission import main


class TestMission(object):

    def suppress_py4j_logging(cls):
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.WARN)

    def test_main(self):
        
        # Change configurations to a temporary folder
        conf['CSV_LOCATION'] = "tmp/input/"
        conf['PARQUET_LOCATION'] = "tmp/output/parquet"
        conf['RESULTS_LOCATION'] = "tmp/output/results"
        
        # Write some real-world data input 
        source_cvs = """ForecastSiteCode,ObservationTime,ObservationDate,WindDirection,WindSpeed,WindGust,Visibility,ScreenTemperature,Pressure,SignificantWeatherCode,SiteName,Latitude,Longitude,Region,Country
3002,0,2016-02-01T00:00:00,12,8,,30000,2.10,997,8,BALTASOUND (3002),60.7490,-0.8540,Orkney & Shetland,SCOTLAND
3005,0,2016-02-01T00:00:00,10,2,,35000,0.10,997,7,LERWICK (S. SCREEN) (3005),60.1390,-1.1830,Orkney & Shetland,SCOTLAND
3008,0,2016-02-01T00:00:00,8,6,,50000,2.80,997,-99,FAIR ISLE (3008),59.5300,-1.6300,Orkney & Shetland,
3017,0,2016-02-01T00:00:00,6,8,,40000,1.60,996,8,KIRKWALL (3017),58.9540,-2.9000,Orkney & Shetland,SCOTLAND
3023,0,2016-02-01T00:00:00,10,30,37,2600,9.80,991,11,SOUTH UIST RANGE (3023),57.3580,-7.3970,Highland & Eilean Siar,SCOTLAND
3026,0,2016-02-01T00:00:00,5,15,,3900,4.30,991,12,STORNOWAY (3026),58.2140,-6.3250,Highland & Eilean Siar,SCOTLAND
3031,0,2016-02-01T00:00:00,4,9,,5000,1.10,995,11,LOCH GLACARNOCH SAWS (3031),57.7250,-4.8960,Highland & Eilean Siar,SCOTLAND
3034,0,2016-02-01T00:00:00,5,7,,5000,3.10,992,11,AULTBEA (3034),57.8590,-5.6360,Highland & Eilean Siar,SCOTLAND
3037,0,2016-02-01T00:00:00,8,15,,3400,8.60,993,15,SKYE/LUSA (SAMOS) (3037),57.2570,-5.8090,Highland & Eilean Siar,SCOTLAND
3039,0,2016-02-01T00:00:00,9,32,43,,3.90,,-99,BEALACH NA BA (3039),57.4200,-5.6900,Highland & Eilean Siar,
3041,0,2016-02-01T00:00:00,10,44,82,,3.90,,-99,AONACH MOR (3041),56.8200,-4.9700,Highland & Eilean Siar,
3044,0,2016-02-01T00:00:00,12,6,,40000,1.30,994,8,ALTNAHARRA SAWS (3044),58.2880,-4.4420,Highland & Eilean Siar,SCOTLAND
3047,0,2016-02-01T00:00:00,9,15,,10000,4.80,997,15,TULLOCH BRIDGE (3047),56.8670,-4.7080,Highland & Eilean Siar,SCOTLAND
3062,0,2016-02-01T00:00:00,6,6,,5000,2.40,995,9,TAIN RANGE (3062),57.8200,-3.9700,Highland & Eilean Siar,
3063,0,2016-02-01T00:00:00,9,9,22,22000,2.70,996,9,AVIEMORE (3063),57.2060,-3.8270,Highland & Eilean Siar,SCOTLAND
3075,0,2016-02-01T00:00:00,7,10,,27000,4.20,996,8,WICK AIRPORT (3075),58.4540,-3.0890,Highland & Eilean Siar,SCOTLAND
3066,0,2016-02-01T00:00:00,4,5,,12000,2.80,995,12,KINLOSS (3066),57.6494,-3.5606,Grampian,SCOTLAND
3068,0,2016-02-01T00:00:00,6,8,,23000,2.50,996,8,LOSSIEMOUTH (3068),57.7120,-3.3220,Grampian,SCOTLAND
3080,0,2016-02-01T00:00:00,12,2,,16000,0.80,998,7,ABOYNE (3080),57.0770,-2.8360,Grampian,SCOTLAND
3088,0,2016-02-01T00:00:00,8,17,,400,4.30,998,6,INVERBERVIE (3088),56.8500,-2.2700,Grampian,
3091,0,2016-02-01T00:00:00,8,8,,4700,3.00,998,11,ABERDEEN DYCE (3091),57.2060,-2.2020,Grampian,SCOTLAND
"""
        # Write the real-world data to a csv file
        csv_file = open(conf['CSV_LOCATION'] + "sample.csv", "w", encoding='utf8')
        csv_file.write(source_cvs)
        csv_file.close()
        
        result = main()
        
        # Test Hottest Date
        actual_df = result['hottest_date_df']
        expected_data = [
            ("2016-02-01",)
        ]
        expected_df = get_spark_session().createDataFrame(
            expected_data,
            ["ObservationDate"]
        )
        expected_df = expected_df.withColumn("ObservationDate", F.to_date(expected_df.ObservationDate, 'yyyy-MM-dd').cast('timestamp').cast('date'))
        assert(expected_df.collect() == actual_df.collect())

        # Test Hottest Temperature
        actual_df = result['hottest_temperature_df']
        expected_data = [
            (9.8,)
        ]
        expected_df = get_spark_session().createDataFrame(
            expected_data,
            ["ScreenTemperature"]
        )
        expected_df = expected_df.withColumn("ScreenTemperature", expected_df.ScreenTemperature.cast('double'))
        assert(expected_df.collect() == actual_df.collect())

        # Test Hottest Region
        actual_df = result['hottest_region_df']
        expected_data = [
            ("Highland & Eilean Siar",)
        ]
        expected_df = get_spark_session().createDataFrame(
            expected_data,
            ["Region"]
        )
        expected_df = expected_df.withColumn("Region", expected_df.Region.cast('string'))
        assert(expected_df.collect() == actual_df.collect())


    def test_main_if_max_is_in_multiple_regions_and_multiple_dates(self):
        
        # Change configurations to a temporary folder
        conf['CSV_LOCATION'] = "tmp/input/"
        conf['PARQUET_LOCATION'] = "tmp/output/parquet"
        conf['RESULTS_LOCATION'] = "tmp/output/results"
        
        # Write some real-world data input 
        source_cvs = """ForecastSiteCode,ObservationTime,ObservationDate,WindDirection,WindSpeed,WindGust,Visibility,ScreenTemperature,Pressure,SignificantWeatherCode,SiteName,Latitude,Longitude,Region,Country
3002,0,2016-03-01T00:00:00,12,8,,30000,9.80,997,8,BALTASOUND (3002),60.7490,-0.8540,Orkney & Shetland,SCOTLAND
3005,0,2016-02-01T00:00:00,10,2,,35000,0.10,997,7,LERWICK (S. SCREEN) (3005),60.1390,-1.1830,Orkney & Shetland,SCOTLAND
3008,0,2016-02-01T00:00:00,8,6,,50000,2.80,997,-99,FAIR ISLE (3008),59.5300,-1.6300,Orkney & Shetland,
3017,0,2016-02-01T00:00:00,6,8,,40000,1.60,996,8,KIRKWALL (3017),58.9540,-2.9000,Orkney & Shetland,SCOTLAND
3023,0,2016-02-01T00:00:00,10,30,37,2600,9.80,991,11,SOUTH UIST RANGE (3023),57.3580,-7.3970,Highland & Eilean Siar,SCOTLAND
3026,0,2016-02-01T00:00:00,5,15,,3900,4.30,991,12,STORNOWAY (3026),58.2140,-6.3250,Highland & Eilean Siar,SCOTLAND
3031,0,2016-02-01T00:00:00,4,9,,5000,1.10,995,11,LOCH GLACARNOCH SAWS (3031),57.7250,-4.8960,Highland & Eilean Siar,SCOTLAND
3034,0,2016-02-01T00:00:00,5,7,,5000,3.10,992,11,AULTBEA (3034),57.8590,-5.6360,Highland & Eilean Siar,SCOTLAND
3037,0,2016-02-01T00:00:00,8,15,,3400,8.60,993,15,SKYE/LUSA (SAMOS) (3037),57.2570,-5.8090,Highland & Eilean Siar,SCOTLAND
3039,0,2016-02-01T00:00:00,9,32,43,,3.90,,-99,BEALACH NA BA (3039),57.4200,-5.6900,Highland & Eilean Siar,
3041,0,2016-02-01T00:00:00,10,44,82,,3.90,,-99,AONACH MOR (3041),56.8200,-4.9700,Highland & Eilean Siar,
3044,0,2016-02-01T00:00:00,12,6,,40000,1.30,994,8,ALTNAHARRA SAWS (3044),58.2880,-4.4420,Highland & Eilean Siar,SCOTLAND
3047,0,2016-02-01T00:00:00,9,15,,10000,4.80,997,15,TULLOCH BRIDGE (3047),56.8670,-4.7080,Highland & Eilean Siar,SCOTLAND
3062,0,2016-02-01T00:00:00,6,6,,5000,2.40,995,9,TAIN RANGE (3062),57.8200,-3.9700,Highland & Eilean Siar,
3063,0,2016-02-01T00:00:00,9,9,22,22000,2.70,996,9,AVIEMORE (3063),57.2060,-3.8270,Highland & Eilean Siar,SCOTLAND
3075,0,2016-02-01T00:00:00,7,10,,27000,4.20,996,8,WICK AIRPORT (3075),58.4540,-3.0890,Highland & Eilean Siar,SCOTLAND
3066,0,2016-02-01T00:00:00,4,5,,12000,2.80,995,12,KINLOSS (3066),57.6494,-3.5606,Grampian,SCOTLAND
3068,0,2016-02-01T00:00:00,6,8,,23000,2.50,996,8,LOSSIEMOUTH (3068),57.7120,-3.3220,Grampian,SCOTLAND
3080,0,2016-02-01T00:00:00,12,2,,16000,0.80,998,7,ABOYNE (3080),57.0770,-2.8360,Grampian,SCOTLAND
3088,0,2016-02-01T00:00:00,8,17,,400,4.30,998,6,INVERBERVIE (3088),56.8500,-2.2700,Grampian,
3091,0,2016-02-01T00:00:00,8,8,,4700,3.00,998,11,ABERDEEN DYCE (3091),57.2060,-2.2020,Grampian,SCOTLAND
"""
        # Write the real-world data to a csv file
        csv_file = open(conf['CSV_LOCATION'] + "sample.csv", "w", encoding='utf8')
        csv_file.write(source_cvs)
        csv_file.close()
        
        result = main()
        
        # Test Hottest Date
        actual_df = result['hottest_date_df']
        expected_data = [
            ("2016-03-01",),
            ("2016-02-01",)
        ]
        expected_df = get_spark_session().createDataFrame(
            expected_data,
            ["ObservationDate"]
        )
        expected_df = expected_df.withColumn("ObservationDate", F.to_date(expected_df.ObservationDate, 'yyyy-MM-dd').cast('timestamp').cast('date'))
        assert(expected_df.collect() == actual_df.collect())

        # Test Hottest Temperature
        actual_df = result['hottest_temperature_df']
        expected_data = [
            (9.8,)
        ]
        expected_df = get_spark_session().createDataFrame(
            expected_data,
            ["ScreenTemperature"]
        )
        expected_df = expected_df.withColumn("ScreenTemperature", expected_df.ScreenTemperature.cast('double'))
        assert(expected_df.collect() == actual_df.collect())

        # Test Hottest Region
        actual_df = result['hottest_region_df']
        expected_data = [
            ("Orkney & Shetland",),
            ("Highland & Eilean Siar",),
        ]
        expected_df = get_spark_session().createDataFrame(
            expected_data,
            ["Region"]
        )
        expected_df = expected_df.withColumn("Region", expected_df.Region.cast('string'))
        assert(expected_df.collect() == actual_df.collect())






