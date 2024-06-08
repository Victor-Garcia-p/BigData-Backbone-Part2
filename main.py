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
import re
from hdfs import InsecureClient
import pandas as pd
from datetime import datetime

user_name = "bdm"
host = "http://10.4.41.35:9870"

keywords = ['idealista']
limit = datetime(2000, 6, 1)
groups = {}

hdfs_client = InsecureClient(host, user=user_name)

all_files = hdfs_client.list("user/bdm/persistent_landing")


# Read all data, only process it if timestamp is above limit
for file in all_files:
    full_name = file.split('-', maxsplit = 2)
    date_format = '%Y%m%d %H%M'

    date = datetime.strptime(' '.join(full_name[0:2]), date_format)
    
    if date >= limit:
        process_file = set(full_name[2:])

        while len(process_file) != 0:
            file = process_file.pop()
            print(file)
            for i in keywords:
                if re.search(i, file):
                    if i not in groups.keys():
                        groups[i] = [file]
                    else:
                        groups[i].append(file)
                else:
                    if 'others' not in groups.keys():
                        groups['others'] = [file]
                    else:
                        groups['others'].append(file)
                    
print(groups['others'])

"""
treshold     = 0.70
minGroupSize = 1

from jellyfish import jaro_similarity
from itertools import combinations

paired = { c:{c} for c in data }
for a,b in combinations(data,2):
    if jaro_similarity(a,b) < treshold: continue
    paired[a].add(b)
    paired[b].add(a)

groups    = list()
ungrouped = set(data)
while ungrouped:
    bestGroup = {}
    for city in ungrouped:
        g = paired[city] & ungrouped
        for c in g.copy():
            g &= paired[c] 
        if len(g) > len(bestGroup):
            bestGroup = g
    if len(bestGroup) < minGroupSize : break  # to terminate grouping early change minGroupSize to 3
    ungrouped -= bestGroup
    groups.append(bestGroup)

# Manually
key_names = ['idealista', 'Distribucio_territorial_renda_familiar']
"""

