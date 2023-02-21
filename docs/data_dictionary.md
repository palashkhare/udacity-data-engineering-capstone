# Data Dictionary

## Dimension: CITY

|Field|Type|Description|
|----|-----|-----------|
|ID | INT IDENTITY(1,1) PRIMARY KEY | incremetal sequence key |
|City | VARCHAR(255) | City name in US |
|State | VARCHAR(255) | State of US |
|median_age | NUMERIC | Median age of people in the city |
|male_population | NUMERIC | Number of males in the city |
|female_population | NUMERIC | Number of females in the city |
|total_population | NUMERIC | total population |
|no_of_veterans | NUMERIC | Number of veterans from the city |
|foreign_born | NUMERIC | People born in foreign |
|avg_house_hold_size | NUMERIC | Average size of a family |
|state_code | VARCHAR(255) | State Code. Connected with Immigration table |
|race | VARCHAR(255) | Race of people |
|count | INT | Population |


## Dimension: AIRPORT

|Field|Type|Description|
|----|-----|-----------|
|ID | INT IDENTITY(1,1) PRIMARY KEY | Sequence Key |
|ident | VARCHAR(10) | identity code of the airport |
|type | VARCHAR(50) | type of airport, halipad, airport etc |
|name | VARCHAR(255) | name of airport |
|elevation_ft | INT | elevation of airport |
|continent | VARCHAR(255) | Continent where the airport is situated |
|iso_country | VARCHAR(255) | Country code |
|iso_region | VARCHAR(255) | region code |
|municipality | VARCHAR(255) | Municipality location |
|gps_code | VARCHAR(255) | gps code of airport |
|iata_code | VARCHAR(255) | air traffic control code |
|local_code | VARCHAR(255) | local code of airport. Connected to Immigration table |
|coordinates | VARCHAR(255) | Coordinates of airport |

## Dimension: CALENDAR

|Field|Type|Description|
|----|-----|-----------|
|datetime | INT PRIMARY KEY | Datetime in epoch |
|hour | INT | Hour |
|dayofmonth | INT | Day of month |
|weekofyear | INT | Week of year |
|month | INT | month |
|year | INT | year |
|dayofweek | IN | day of week |

## FACT: immigration

|Field|Type|Description|
|----|-----|-----------|
|id INT | IDENTITY(1,1) PRIMARY KEY | unique seqence of immigration |
|year | INT | Year of immmigration |
|month | INT | month of immigration |
|age | INT | age of immigrant |
|country_code | VARCHAR(5) | Country code where immigration happened |
|port_code | VARCHAR(6) | Port code of immi. FK to airport code. |
|mode | VARCHAR(10) | Mode |
|visa_category | VARCHAR(10) | Visa category of immigrant |
|visa_type | VARCHAR(10) | Visa type |
|gender | varchar(10) | gender |
|birth_year | INT | Birth year of immigrant |
|arrival_date | INT | arrival date. FK to datetime in calendar.|
|departure_date | INT | departure date FK to datetime in calendar. |
|address_code | VARCHAR(10) | Address code. FK to City |
