# Queries for datamodel

IMMIGRATION = \
"""
CREATE TABLE IF NOT EXISTS immigration(
    id INT IDENTITY(1,1) PRIMARY KEY,
    year INT,
    month INT,
    age INT,
    country_code VARCHAR(5),
    port_code VARCHAR(6),
    mode VARCHAR(10),
    visa_category VARCHAR(10),
    visa_type VARCHAR(10),
    gender varchar(10),
    birth_year INT,
    arrival_date INT,
    departure_date INT,
    address_code VARCHAR(10)
)
"""

CALENDAR = \
"""
CREATE TABLE IF NOT EXISTS CALENDAR(
    datetime INT PRIMARY KEY,
    hour INT,
    dayofmonth INT,
    weekofyear INT,
    month INT,
    year INT,
    dayofweek INT
)
"""

AIRPORT = \
"""
CREATE TABLE IF NOT EXISTS AIRPORT(
    ID INT IDENTITY(1,1) PRIMARY KEY,
    ident VARCHAR(10),
    type VARCHAR(50),
    name VARCHAR(255),
    elevation_ft INT,
    continent VARCHAR(255),
    iso_country VARCHAR(255),
    iso_region VARCHAR(255),
    municipality VARCHAR(255),
    gps_code VARCHAR(255),
    iata_code VARCHAR(255),
    local_code VARCHAR(255),
    coordinates VARCHAR(255)
)
"""

CITY = \
"""
CREATE TABLE IF NOT EXISTS CITY (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    City VARCHAR(255),
    State VARCHAR(255),
    median_age NUMERIC,
    male_population NUMERIC,
    female_population NUMERIC,
    total_population NUMERIC,
    no_of_veterans NUMERIC,
    foreign_born NUMERIC,
    avg_house_hold_size NUMERIC,
    state_code VARCHAR(255),
    race VARCHAR(255),
    count INT
)
"""

INSERT_IMMIGRATION = \
"""
copy immigration from 's3://{}/output/immigration_data'
credentials 'aws_iam_role={}'
format as json 'auto'
region 'us-east-1';
"""

INSERT_CALENDAR = \
"""
copy CALENDAR from 's3://{}/output/datetime'
credentials 'aws_iam_role={}'
format as json 'auto'
region 'us-east-1';
"""

INSERT_AIRPORT = \
"""
copy AIRPORT from 's3://{}/output/airport_data'
credentials 'aws_iam_role={}'
format as json 'auto'
region 'us-east-1';
"""

INSERT_CITY = \
"""
copy CITY from 's3://{}/output/us_city_demographics'
credentials 'aws_iam_role={}'
format as json 'auto'
region 'us-east-1';
"""

CREATE_TABLES = [
    IMMIGRATION, CALENDAR, AIRPORT, CITY
]

CREATE_PG_TABLES = [
    IMMIGRATION.replace("IDENTITY(1,1)", "SERIAL"),
    CALENDAR.replace("IDENTITY(1,1)", "SERIAL"),
    AIRPORT.replace("IDENTITY(1,1)", "SERIAL"),
    CITY.replace("IDENTITY(1,1)", "SERIAL"),
]

DROP_TABLES = [
    f"DROP IF EXISTS {table}" for table in ["immigration", "CALENDAR", "AIRPORT", "CITY"]
]

INSERT_QUERIES = [INSERT_IMMIGRATION, INSERT_CITY, INSERT_AIRPORT, INSERT_CALENDAR]

def __init__():
    pass
