# Project 1: Implementation of a (Big) Data Management Backbone #
Adrià Casanova Lloveras and Víctor Garcia Pizarro
Big Data Management - Data Science Master’s Degree - 07/04/2024

## Project description ##
Implementation of Big Data Management Backbone using a Pyhon API to connect to HDFS, 
installed in UPC virtual machine. The application ingests data sources from a local 
machine to the "landing zone" of HDFS, which is divided into temporal and persistent landing.

The project is divided into a main file that synchronizes the process and two different files 
that contain "data collectors", that ingest data sources into temporal landing, 
and "data loaders", that transform files to .parquet and place them into persistent landing. 
Lastly, there is an extra file, "utils.py" that contains additional functions. 


## How to run the code ##
To run the application simply run "main.py". Before doing so, ensure that:

1- Datasources are valid* and placed in the same folder of the code or you are
providing paths to the functions.

2- You are connected to UPN link VPN and have all Python modules installed

3- The virtual machine is active. To check it simply google "http://10.4.41.35:9870". 
If it is not active contact the authors of the project.


## * Note about datasources ###
It is possible to use any data source from .csv, .json or .gpkg formats, however, we 
have tried the application with two different data sources:
- Data.zip, provided by the teachers at learnsql2
- "2019_turisme_allotjament.gpkg" (downloaded from
https://opendata-ajuntament.barcelona.cat/data/es/dataset/intensitat-activitat-turistica)

If you would like to use other formats it is required to implement a "data collector" 
and "data loader" function and modify their respective orchestration, which can be found in "utils.py".


