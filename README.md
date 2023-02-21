# udacity-data-engineering-capstone

## 1. Purpose of project
Project is based on combining data sources from various data sources in different format and store them in a distributed database for further consumption.
Data sources are udacity provided. Various formats which data is provided are csv, parquet, SAS etc.
Extraction and transformation is done using pyspark on memory and resultant data is converted into parquet format for further load.
Here PySpark is serving for staging operations and cleaned data is further ingested into AWS Redshift.

This data will be useful in understanding below KPIs

- Data will help understanding the number of immigrants landing on each airport. 
- Number of airports in each city by different types
- Age group of travelers passing immigration
- Days when high density of immigrats can be expected

## 1.2 Specification
- Data analysis made on more than 2 million rows
- DB can be used my multiple users and can be scaled further depending on higher needs.
- Data pipeline runs every night at 12 in order to least interfere during working hours

## 1.3 Choice of tools
### Python
Python is used as mode of programming
### PySpark
Pyspark is used to query huge dataset quickly. This also provide help in performing visual analysis.
### Redshift
Redshift is distributed database. This is used to store data so that it can be queried on demand
### Data Models
Data models are defined in detail in data dictionary.
Basically data is divided into Three dimensions i.e. Airport, City and calendara and one fact Immigration data.
Immigration data is filterted for only useful fields.

## 2. Data Sources
### 2.1 Airport data (**CSV**)
Airport data is a csv file which is holding information about location, type of port and their codes.
This information is useful to add useful attributes to immigration data.

### 2.2 City data (**CSV**)
City data file is csv file and is holding information city demographics. This is adding information to immigartion data.
This information can be useful to understand which city is more likely for immigration.

### 2.3 Immigration data (**parquet**)
Immigration is master data and provide information about immigrations passed in US in different cities at different airports.

## 3. Dimensional Model
### Dimensions
- City
- Airport
- Calendar

### Facts
- immigration

## 4. Project File Structure
### 4.1 data
Holds the data set provided by udacity

### 4.2 pipeline_scripts
This folder hold various scripts to perform different operations which are contributing to data movement
- **data_extraction :** This script is used to extract data from various sources, clean them and save it into parquet format on S3/locally
- **create_tables :** This script is used to hold database queries. Both local/redshift mode is available
- **upload_to_redshift :** This run scripts one by one queries to create database and further upload data from AWS S3 to Redshift.

### 4.3 redshift
- Create Cluster script start redshift cluster online and assign relevant policies to access S3 bucket.
- Delete cluster script drops the cluster

### 4.4 docs
- **data_dictionary** This file contains the details about data model
- **project_writeup** Detailed description about project