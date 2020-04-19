import pytest

import logging

import pyspark.sql.functions as F

from weather_data_pyspark_project.helpers import get_spark_session

from weather_data_pyspark_project.spark import find_max_in_df
from weather_data_pyspark_project.spark import filter_df_based_on_column_value
from weather_data_pyspark_project.spark import select_distinct_column_from_df
from weather_data_pyspark_project.spark import convert_timestamp_column_to_date_column

class TestSpark(object):

    def suppress_py4j_logging(cls):
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.WARN)

    def test_find_max_in_df(self):
        source_data = [
            ("2016-02-01T00:00:00", -3.9, "Highland & Eilean Siar"),
            ("2016-02-01T00:00:00", 1.3, "Highland & Eilean Siar"),
            ("2016-02-01T00:00:00", 2.8, "Grampian"),
            ("2016-02-01T00:00:00", 2.5, "Grampian"),
            ("2016-02-01T00:00:00", 0.8, "Grampian"),
            ("2016-03-01T00:00:00", 3.2, "Grampian"),
            ("2016-03-01T00:00:00", 10.4, "Strathclyde"),
            ("2016-03-01T00:00:00", 2.7, "Central Tayside & Fife"),
            ("2016-03-01T00:00:00", 2.3, "Central Tayside & Fife"),
        ]
        source_df = get_spark_session().createDataFrame(
            source_data,
            ["ObservationDate", "ScreenTemperature", "Region"]
        )

        actual_data = find_max_in_df(source_df, "ScreenTemperature")

        expected_data = 10.4

        assert(expected_data == actual_data)


    def test_filter_df_based_on_column_value(self):
        source_data = [
            ("2016-02-01T00:00:00", -3.9, "Highland & Eilean Siar"),
            ("2016-02-01T00:00:00", 1.3, "Highland & Eilean Siar"),
            ("2016-02-01T00:00:00", 2.8, "Grampian"),
            ("2016-02-01T00:00:00", 2.5, "Grampian"),
            ("2016-02-01T00:00:00", 0.8, "Grampian"),
            ("2016-03-01T00:00:00", 3.2, "Grampian"),
            ("2016-03-01T00:00:00", 10.4, "Strathclyde"),
            ("2016-03-01T00:00:00", 2.7, "Central Tayside & Fife"),
            ("2016-03-01T00:00:00", 2.3, "Central Tayside & Fife"),
        ]
        source_df = get_spark_session().createDataFrame(
            source_data,
            ["ObservationDate", "ScreenTemperature", "Region"]
        )

        actual_df = filter_df_based_on_column_value(source_df, "ScreenTemperature", 10.4)

        expected_data = [
            ("2016-03-01T00:00:00", 10.4, "Strathclyde")
        ]
        expected_df = get_spark_session().createDataFrame(
            expected_data,
            ["ObservationDate", "ScreenTemperature", "Region"]
        )

        assert(expected_df.collect() == actual_df.collect())


    def test_select_distinct_ObservationDate_from_df(self):
        source_data = [
            ("2016-03-01T00:00:00", 10.4, "Strathclyde"),
        ]
        source_df = get_spark_session().createDataFrame(
            source_data,
            ["ObservationDate", "ScreenTemperature", "Region"]
        )

        actual_df = select_distinct_column_from_df(source_df, "ObservationDate")

        expected_data = [
            ('2016-03-01T00:00:00',)
        ]
        expected_df = get_spark_session().createDataFrame(
            expected_data,
            ["ObservationDate"]
        )

        assert(expected_df.collect() == actual_df.collect())


    def test_select_distinct_Region_from_df(self):
        source_data = [
            ("2016-03-01T00:00:00", 10.4, "Strathclyde"),
        ]
        source_df = get_spark_session().createDataFrame(
            source_data,
            ["ObservationDate", "ScreenTemperature", "Region"]
        )

        actual_df = select_distinct_column_from_df(source_df, "Region")

        expected_data = [
            ("Strathclyde",)
        ]
        expected_df = get_spark_session().createDataFrame(
            expected_data,
            ["Region"]
        )

        assert(expected_df.collect() == actual_df.collect())

    def test_select_distinct_Region_from_df_if_max_is_in_multiple_records(self):
        source_data = [
            ("2016-03-01T00:00:00", 10.4, "Strathclyde"),
            ("2016-02-01T00:00:00", 10.4, "Central Tayside & Fife"),
        ]
        source_df = get_spark_session().createDataFrame(
            source_data,
            ["ObservationDate", "ScreenTemperature", "Region"]
        )

        actual_df = select_distinct_column_from_df(source_df, "Region")

        expected_data = [
            ("Strathclyde",),
            ("Central Tayside & Fife",)
        ]
        expected_df = get_spark_session().createDataFrame(
            expected_data,
            ["Region"]
        )

        assert(expected_df.collect() == actual_df.collect())

    def test_convert_timestamp_column_to_date_column(self):
        source_data = [
            ("20160301", 10.4, "Strathclyde"),
        ]
        source_df = get_spark_session().createDataFrame(
            source_data,
            ["ObservationDate", "ScreenTemperature", "Region"]
        )
        source_df = source_df.withColumn("ObservationDate", F.to_date(source_df.ObservationDate, 'yyyyMMdd').cast('timestamp'))

        actual_df = convert_timestamp_column_to_date_column(source_df, "ObservationDate")

        expected_data = [
            ("20160301",)
        ]
        expected_df = get_spark_session().createDataFrame(
            expected_data,
            ["ObservationDate"]
        )
        expected_df = expected_df.withColumn("ObservationDate", F.to_date(expected_df.ObservationDate, 'yyyyMMdd').cast('timestamp').cast('date'))

        assert(expected_df.collect() == actual_df.collect())


    def test_find_max_even_if_max_is_in_multiple_records(self):
        source_data = [
            ("2016-02-01T00:00:00", 10.4, "Highland & Eilean Siar"),
            ("2016-02-01T00:00:00", 10.4, "Highland & Eilean Siar"),
            ("2016-02-01T00:00:00", 2.8, "Grampian"),
            ("2016-02-01T00:00:00", 2.5, "Grampian"),
            ("2016-02-01T00:00:00", 0.8, "Grampian"),
            ("2016-03-01T00:00:00", 3.2, "Grampian"),
            ("2016-03-01T00:00:00", 10.4, "Strathclyde"),
            ("2016-03-01T00:00:00", 2.7, "Central Tayside & Fife"),
            ("2016-03-01T00:00:00", 2.3, "Central Tayside & Fife"),
        ]
        source_df = get_spark_session().createDataFrame(
            source_data,
            ["ObservationDate", "ScreenTemperature", "Region"]
        )

        actual_data = find_max_in_df(source_df, "ScreenTemperature")

        expected_data = 10.4

        assert(expected_data == actual_data)









