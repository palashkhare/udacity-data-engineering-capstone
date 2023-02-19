# Data Dictionary

## Dimension: CITY

|Field|Type|Description|
|----|-----|-----------|
|ID | INT IDENTITY(1,1) PRIMARY KEY |
|City | VARCHAR(255) |
|State | VARCHAR(255) |
|median_age | NUMERIC |
|male_population | NUMERIC |
|female_population | NUMERIC |
|total_population | NUMERIC |
|no_of_veterans | NUMERIC |
|foreign_born | NUMERIC |
|avg_house_hold_size | NUMERIC |
|state_code | VARCHAR(255) |
|race | VARCHAR(255) |
|count | INT |


## Dimension: AIRPORT

|Field|Type|Description|
|----|-----|-----------|
|ID | INT IDENTITY(1,1) PRIMARY KEY |
|ident | VARCHAR(10) |
|type | VARCHAR(50) |
|name | VARCHAR(255) |
|elevation_ft | INT |
|continent | VARCHAR(255) |
|iso_country | VARCHAR(255) |
|iso_region | VARCHAR(255) |
|municipality | VARCHAR(255) |
|gps_code | VARCHAR(255) |
|iata_code | VARCHAR(255) |
|local_code | VARCHAR(255) |
|coordinates | VARCHAR(255) |

## Dimension: CALENDAR

|Field|Type|Description|
|----|-----|-----------|
|datetime | INT IDENTITY(1,1) PRIMARY KEY |
|hour | INT |
|dayofmonth | INT |
|weekofyear | INT |
|month | INT |
|year | INT |
|dayofweek | IN |

## FACT: immigration

|Field|Type|Description|
|----|-----|-----------|
|ID | INT | IDENTITY(1,1) PRIMARY KEY |
|City | VARCHAR(255) |
|State | VARCHAR(255) |
|median_age | NUMERIC |
|male_population | NUMERIC |
|female_population | NUMERIC |
|total_population | NUMERIC |
|no_of_veterans | NUMERIC |
|foreign_born | NUMERIC |
|avg_house_hold_size | NUMERIC |
|state_code | VARCHAR(255) |
|race | VARCHAR(255) |
|count | INT |
