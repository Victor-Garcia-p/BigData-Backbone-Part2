"""
Info: Request data from persistent landing, process it and
creates a formatted zone.
Input: credentials of the user and host to access virtual machine
"""

from datetime import datetime
from data_SelectorsAndFormatters import *
from utils import *

# Identify the user and create a log file
log = log_config("MyLog.log")
logging_info = logging_creation("credentials.logging")

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
    "hotels": {
        "keywords": ["hotels"],
        "Description": "Information about hotels in neighbourhoods",
    },
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
limit = [datetime(2000, 6, 1), datetime(2200, 6, 1)]

## Formatted zone ##
# Download data from persistent zone, then merge
# files appliying soft preprocessing and save it

persistent_data_selector(
    log, logging_info["user_name"], logging_info["host"], groups_info, limit
)
formatted_data_loader(log, logging_info["user_name"], logging_info["host"], groups_info)
