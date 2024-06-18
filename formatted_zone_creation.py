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
from data_SelectorsAndFormatters import *
from utils import *

log = log_config("MyLog.log")
logging_info = logging()

persistent_path = "user/bdm/persistent_landing"

"""
Define dict following the correct syntax
dict = {file_name1 : {metadata_field:value}, ...}

where 
file_name = name of the file that we want to create in the formatted
metadata_field = Optional information about the file (description) or "keywords" used
to define which files will be merged
"""

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

groups_info = {"hotels": {"Description": "Information about hotels in neighbourhoods"}}
limit = [datetime(2000, 6, 1), datetime(2200, 6, 1)]

## Formatted zone ##
# Download data from persistent zone, then merge files appliying soft preprocessing and
# upload it to the formatted zone

persistent_data_selector(
    log, logging_info["user_name"], logging_info["host"], groups_info, limit
)
formatted_data_loader(logging_info["user_name"], logging_info["host"], groups_info)
