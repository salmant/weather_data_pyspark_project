# PySpark project named `weather_data_pyspark_project` using pytest with egg file

The whole project is structured as follows:
<br>

```
.
+-- config_file.yml
+-- input
¦   +-- weather.20160201.csv
¦   +-- weather.20160301.csv
+-- output
¦   +-- parquet
¦   ¦   +-- _SUCCESS
¦   ¦   +-- part-00000-3eb61fea-fba7-464b-9d1d-953c8dbde6a3-c000.snappy.parquet
¦   +-- results
¦       +-- Hottest_ObservationDate
¦       ¦   +-- _SUCCESS
¦       ¦   +-- part-00000-8f142c13-f387-49ea-8899-c841cde1d9ed-c000.snappy.parquet
¦       +-- Hottest_Region
¦       ¦   +-- _SUCCESS
¦       ¦   +-- part-00000-b57e4aba-ff26-47fb-af9d-46e52c6e5885-c000.snappy.parquet
¦       +-- Hottest_ScreenTemperature
¦           +-- _SUCCESS
¦           +-- part-00000-8e344238-499d-4c67-ade4-7c242aab00c9-c000.snappy.parquet
+-- requirements.txt
+-- setup.py
+-- tests
¦   +-- test_mission.py
¦   +-- test_spark.py
+-- tmp
¦   +-- input
¦   ¦   +-- sample.csv
¦   +-- output
¦       +-- parquet
¦       ¦   +-- _SUCCESS
¦       ¦   +-- part-00000-b7b4159e-4be4-485b-8576-ebd44d203063-c000.snappy.parquet
¦       +-- results
¦           +-- Hottest_ObservationDate
¦           ¦   +-- _SUCCESS
¦           ¦   +-- part-00000-950bda1d-7a43-47e9-b36f-500dab6d221f-c000.snappy.parquet
¦           +-- Hottest_Region
¦           ¦   +-- _SUCCESS
¦           ¦   +-- part-00000-e768ef9f-9f3a-44f9-b829-3967a2051c8e-c000.snappy.parquet
¦           +-- Hottest_ScreenTemperature
¦               +-- _SUCCESS
¦               +-- part-00000-8813fb4b-7fa7-4834-9bd7-070c48ee1a6b-c000.snappy.parquet
+-- weather_data_pyspark_project
    +-- __init__.py
    +-- helpers.py
    +-- mission.py
    +-- spark.py
```

<br>
<br>It is assumed that the CSV input files including weather data are already fetched from a data source API and stored on the `input` folder. We need to developed a process to fetch input files from SFTP, HTTP or any other servers.
<br>
<br>Each input CSV file has a specific set of columns as follows:
<br>

```
 |-- ForecastSiteCode
 |-- ObservationTime
 |-- ObservationDate
 |-- WindDirection
 |-- WindSpeed
 |-- WindGust
 |-- Visibility
 |-- ScreenTemperature
 |-- Pressure
 |-- SignificantWeatherCode
 |-- SiteName
 |-- Latitude
 |-- Longitude
 |-- Region
 |-- Country
```

<br>
<br>We would like to have a PySpark project able to convert the CSV input data into a format named `parquet`. The `parquet` data formatted will be strored in the `output` folder in the `parquet` subfolder.
<br>

<br>The PySpark project should be able to answer different questions: (Question 1) what has/have been the hottest day(s), (Question 2) what has been the hottest temperature and (Question 3) what has/have been the hottest region(s).
For sure, we just have got one hottest temperature. However, there would be one or more than one day/region with such hottest temperature. Therefore, the PySpark project should be able to deal with all such cases.
<br>

<br>The results (answers to Questions 1, 2 and 3) formatted based on `parquet` will be stored in the `output` folder in the `results` subfolder as follows:
  *  Answer 1: `Hottest_ObservationDate` - what has/have been the hottest day(s)?
  *  Answer 2: `Hottest_Region` - what has been the hottest temperature?
  *  Answer 3: `Hottest_ScreenTemperature` - what has/have been the hottest region(s)?
<br>

<br>The folder named `weather_data_pyspark_project` includes various Python files as follows:
  *  `__init__.py`: It contains the information about configuration variables throughout the whole project, and an explanation of the package.
  *  `helpers.py`: It contains some functions which are general and they can be also used in other projects as well. All these functions have got their own comments to explain what they do, and what the inputs/outputs are. 
  *  `spark.py`: It contains some functions which are mainly specific to this current project. Every single one of these functions should be unit-tested. All these functions have got their own comments to explain what they do, and what the inputs/outputs are. 
  *  `mission.py`: It contains the main function to execute the whole process from the start to the end. The main function calls other functions to perform the codes and prepare what we need to do in order to answer all aforementioned questions. This function also needs to be tested before the production stage as well.
<br>

<br>There is a file named `config_file.yml`: This is a YAML configuration file which defines the following parameters to interact with the execution environment. 
Therefore, there is an expectation a YAML file with the following parameters:
  *  `CSV_LOCATION`: Location of the raw data which is in the CSV format.
  *  `PARQUET_LOCATION`: Location where the parquet data formatted should be stored.
  *  `RESULTS_LOCATION`: Location where the results (hottest day, hottest temprature and hottest region) should be stored.
  *  `OVERWRITE`: Boolean identifying whether the generated data stored in `PARQUET_LOCATION` and `RESULTS_LOCATION` should be overwritten (True) or not (False) if there already exists some data in these folders. This is very important point since we need to consider how the data strategy is defined in this project.
<br>

<br>A file named `requirements.txt` is added to determine the current PySpark project requirements. This is important for the maintainance since it helps other developers to maintain and use the code.
<br>

<br>A file named `setup.py` is added to describe the current PySpark project. It is used to package the whole code that can be attached to the Spark cluster for the production stage. It gives us an egg file to be executed on the Spark cluster.
We run the file named `setup.py` with this command: 
<br>`python setup.py bdist_egg`
<br>

<br>When you have the egg file, the following command is used to execute the application code:
<br>`spark-submit --py-files dist/weather_data_pyspark_project-0.0.1-py3.6.egg weather_data_pyspark_project/mission.py`
<br>

<br>There is a folder named `docs` which includes the documentations.
<br>

<br>The folder named `tests` includes all tests codes. There are two files as follows:
  *  `test_spark.py`: It includes all unit tests to ensure that the results of all functions defined in `spark.py` are as expected. 
  *  `test_mission.py`:  It includes the test to ensure the entire Spark development from beginning to the end in order to test the application flow behaves as expected. It also generates some data stored in a folder named `tmp`.

