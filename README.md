# udacity-data-engineering-capstone

## 1. Purpose of project
Project is based on combining data sources from various data sources in different format and store them in a distributed database for further consumption.
Data sources are udacity provided. Various formats which data is provided are csv, parquet, SAS etc.
Extraction and transformation is done using pyspark on memory and resultant data is converted into parquet format for further load.
Here PySpark is serving for staging operations and cleaned data is further ingested into AWS Redshift.

## 2. Data Sources
### 2.1 Airport data
Airport data is a csv file which is holding information about location, type of port and their codes.
This information is useful to add useful attributes to immigration data.

### 2.2 City data
City data file is csv file and is holding information city demographics. This is adding information to immigartion data.
This information can be useful to understand which city is more likely for immigration.

### 2.3 Immigration data
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