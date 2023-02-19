import configparser
import os
from datetime import datetime, timedelta
import calendar
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.window import Window

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

#os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['KEY']
#os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['SECRET']
#
OUTPUT_PATH = "s3://my-kanika-009/output"
INPUT_PATH = "s3://my-kanika-009" #"data"
#OUTPUT_PATH = "output"
#INPUT_PATH = "data"

I94_INPUT_PATH = f"{INPUT_PATH}/sas_data"
AIRPORT_CODES = f"{INPUT_PATH}/airport-codes_csv.csv"
I94_SAS_LABELS = f"{INPUT_PATH}/I94_SAS_Labels_Descriptions.SAS"
US_CITY_DEMOGRAPHICS = f"{INPUT_PATH}/us-cities-demographics.csv"

def create_spark_session():
    """
    Creates Spark session
    """
    spark = SparkSession \
        .builder \
        .appName("Extract and cleanse i94 Immigration Data") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    return spark


def create_local_spark_session():
    """
    Creates local Spark session
    :return: Spark session configured for local mode
    """
    number_cores = 2
    memory_gb = 2
    spark = SparkSession \
        .builder \
        .appName("Extract and cleanse i94 Immigration Data") \
        .master('local[{}]'.format(number_cores)) \
        .config('spark.driver.memory', '{}g'.format(memory_gb)) \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def convert_epoch_to_days(date: str):
    """
    Converts SAS date stored as days since 1/1/1960 to datetime
    :param days: Days since 1/1/1960
    :return: datetime
    """
    if not date: return None

    date = str(date).split(".")[0]
    date_diff = datetime.strptime(date,"%Y%d%m") - datetime(year=1960, month=1, day=1)
    return date_diff.days


def convert_epoch_to_date(day_cnt):
    """
    Converts SAS date stored as days since 1/1/1960 to day of month
    :param days: Days since 1/1/1960
    :return: Day of month value as integer
    """
    if not day_cnt: return None
    return (datetime(year=1960, month=1, day=1) + timedelta(days=day_cnt))


def convert_i94mode(mode):
    """
    Converts i94 travel mode code to a description
    :param mode: int i94 mode as integer
    :return: i94 mode description
    """
    modes = {
        1 : "Air",
        2 : "Sea",
        3 : "Land",
    }
    
    return modes.get(mode, "No Info")


def convert_visa(visa):
    """
    Converts visa numeric code to description
    :param visa: str
    :return: Visa description: str
    """
    visas = {
        1 : "Business",
        2 : "Pleasere",
        3 : "Student",
    }
    
    return visas.get(visa, "No Info")

def clean_i94_data(spark, input_path, output_path):
    """
    Loads SAS i94 immigration data into data frame.
    Data is cleaned and projected, and then written to Parquet.
    Partitioned by year, month, and day.
    :param spark: Spark session
    :param input_path: Input path to SAS data
    :param output_path: Output path for Parquet files
    :return: None
    """

    convert_i94mode_udf = F.udf(convert_i94mode, StringType())
    convert_visa_udf = F.udf(convert_visa, StringType())
    convert_epoch_date_udf = F.udf(convert_epoch_to_date, DateType())

    input_path = f"{input_path}"
    print(f"Cleaning {input_path}...")

    # Read SAS data for month
    df = spark.read.parquet(input_path)

    # Set appropriate names and data types for columns
    df = df.withColumn('arrival_date', df['arrdate']) \
        .withColumn('departure_date', df['depdate']) \
        .withColumn('year', df['i94yr'].cast(IntegerType())) \
        .withColumn('month', df['i94mon'].cast(IntegerType())) \
        .withColumn('age', df['i94bir'].cast(IntegerType())) \
        .withColumn('address_code', df['i94addr'].cast(StringType())) \
        .withColumn('country_code', df['i94cit'].cast(IntegerType()).cast(StringType())) \
        .withColumn('port_code', df['i94port'].cast(StringType())) \
        .withColumn('birth_year', df['biryear'].cast(IntegerType())) \
        .withColumn('mode', convert_i94mode_udf(df['i94mode'])) \
        .withColumn('visa_category', convert_visa_udf(df['i94visa'])) \
        .withColumn('visa_type', convert_visa_udf(df['visatype'])) \
        .withColumn('airline', convert_visa_udf(df['airline']))

    # Project final data set
    immigration_df = df.select(
        ['year', 'month', 'age', 'country_code', 'port_code', 'mode', 'visa_category', 'visa_type',
            'gender', 'birth_year', 'arrival_date', 'departure_date', 'address_code'])

    # Write data in Apache Parquet format partitioned by year, month, and day
    print(f"Writing {input_path} to output...")

    immigration_df.write.mode("overwrite").partitionBy("year", "month", "arrival_date") \
        .parquet(f"{output_path}/immigration_data")
    print(immigration_df.printSchema())
    print(f"Completed {input_path}.")
    
    date_df = immigration_df.select("arrival_date") \
        .union(immigration_df.select("departure_date")) \
        .withColumn("ts",convert_epoch_date_udf("arrival_date")) \
        .withColumn("datetime",immigration_df["arrival_date"])
    
    date_df = date_df.select(
        "datetime",
        F.hour(F.col("ts")).alias("hour"), 
        F.dayofmonth(F.col("ts")).alias("dayofmonth"), 
        F.weekofyear(F.col("ts")).alias("weekofyear"), 
        F.month(F.col("ts")).alias("month"), 
        F.year(F.col("ts")).alias("year"),
        F.dayofweek(F.col("ts")).alias("dayofweek")
    )
    print(f"Writing {input_path} to output...")
    date_df.write.mode("overwrite").partitionBy("year", "month") \
        .parquet(f"{output_path}/datetime")
    
    print(date_df.printSchema())
    print(f"{output_path}/datetime.")
   


def extract_airport_data(spark, input_path, output_path):
    """
    Load GlobalLandTemperaturesByCity
    Extract latest temperature for U.S. cities
    Write to Parquet format
    :param spark: Spark Session
    :param input_path: 
    :param output_path:
    :return:
    """
    airport_df = spark.read.option("header", True).option("inferSchema",True).csv(input_path)
    airport_df = airport_df.filter(airport_df.local_code.isNotNull())
    print("writing airport data")
    airport_df.write.mode("overwrite").parquet(f"{output_path}/airport_data")
    print(airport_df.printSchema())
    print("Airport data complete")

def extract_city_demographics(spark, input_path, output_path):
    """
    Load GlobalLandTemperaturesByCountry
    Extract latest temperature for each country
    Write to Parquet format
    :param spark: Spark Session
    :param input_path:
    :param output_path:
    :return:
    """
    city_df = spark.read.option("header", True).option("inferSchema",True).option("delimiter", ";").csv(input_path)
    city_df = city_df.withColumnRenamed("Median Age", "median_age") \
        .withColumnRenamed("Male Population", "male_population") \
        .withColumnRenamed("Female Population", "female_population") \
        .withColumnRenamed("Total Population", "total_population") \
        .withColumnRenamed("Number of Veterans", "no_of_veterans") \
        .withColumnRenamed("Foreign-born", "foreign_born") \
        .withColumnRenamed("Average Household Size", "avg_house_hold_size") \
        .withColumnRenamed("State Code", "state_code") \
        .withColumnRenamed("Race", "race") \
        .withColumnRenamed("count", "count")
        
    print(
        city_df.printSchema()
    )
    city_df = city_df.filter(city_df.state_code.isNotNull())
    print("Writing city demographics data")
    city_df.write.mode("overwrite").parquet(f"{output_path}/us_city_demographics")
    print(city_df.printSchema())
    print("Demographics data complete")


spark = create_spark_session()
clean_i94_data(spark, I94_INPUT_PATH, OUTPUT_PATH)
extract_airport_data(spark, AIRPORT_CODES, OUTPUT_PATH)
extract_city_demographics(spark, US_CITY_DEMOGRAPHICS, OUTPUT_PATH)
