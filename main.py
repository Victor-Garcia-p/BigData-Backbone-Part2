"""
Project presentation 
  Master's Degree in Data Science
  Spring 2024
  Big Data Management

  Project 2: Descriptive and predictive analysis (Formatted and Exploitation Zones)

  Team-X
  Víctor García Pizarro

  Program info: 
  ...
"""

from datetime import datetime
from data_formatters import *

user_name = "bdm"
host = "http://10.4.41.35:9870/"
persistent_path = "user/bdm/persistent_landing"
limit = [datetime(2000, 6, 1), datetime(2200, 6, 1)]

# Define dict with the correct metadata
groups_info = {
    "hotels": {"Description": "Information about hotels in neighbourhoods"},
    "renda_familiar": {
        "keywords": ["renda_familiar"],
        "Description": "Data from Open Barcelona with incomes of families. Can be joined with 'idealista' file using a lookup file",
    },
    "idealista": {
        "keywords": ["idealista"],
        "Description": "Appartments from idealista. Can be joined with 'renta familiar' file using a lookup file",
    },
    "lookup_renta_idealista": {
        "keywords": ["extended"],
        "Description": "Lookup datable to join 'Idealista' and 'renda familiar'",
    },
}

## Formatted zone ##
# persistent_data_selector(user_name, host, groups_info)
formatted_data_loader(user_name, host, groups_info)

## Explotation zone (predictive model) ##

# read dataframes from explotation zone
