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
from datetime import datetime
from time import strftime, gmtime
import os

import findspark
import warnings

import pyspark as py
from pyspark.sql import SparkSession
from pyspark import SparkConf


user_name = "bdm"
host = "http://10.4.41.35:9870/"
persistent_path = "user/bdm/persistent_landing"
limit = [datetime(2000, 6, 1), datetime(2200, 6, 1)]

"""
Keywords to make groups, will be used to create names.
Description of the group (opt)
pk, if any
fk (how is related with other sources). File-field.
"""

groups_info = {
    "renda_familiar": {
        "keywords": ["renda_familiar"],
        "Description": "this is datasource1",
        "fk": {"Any": "Open123"},
    },
    "idealista": {
        "keywords": ["extended"],
        "Description": "this is datasource2",
        "fk": {"Any": "Open123"},
    },
}


# Select data for formatting zone
def data_selector(
    user_name,
    host,
    groups_info,
    inp_path="user/bdm/persistent_landing",
    time_limit=[datetime(1, 1, 1), datetime(9999, 12, 12)],
    out_path="temp",
):

    # Create an new dictionary to store names of files for each group
    file_groups = {}
    for keyword in groups_info:
        file_groups[keyword] = []

    # Scan all files on the directory and process only those that are bounded
    # in the time limit
    hdfs_client = InsecureClient(host, user=user_name)
    all_files = hdfs_client.list(inp_path)

    for file in all_files:
        full_name = file.split("-", maxsplit=2)
        date_format = "%Y%m%d %H%M"

        date = datetime.strptime(" ".join(full_name[0:2]), date_format)

        if date >= time_limit[0] and date <= time_limit[1]:
            process_file = set(full_name[2:])

            while len(process_file) != 0:
                file_processed = process_file.pop()

                for group_name in file_groups.keys():

                    for keyword in groups_info[group_name]["keywords"]:
                        if re.search(keyword, file_processed):
                            file_groups[group_name].append(file)

    k = [i for i in file_groups.keys()]
    n = [len(i) for i in file_groups.values()]

    print("Found", n, "files that matches", k, "keys")

    # Download files (if required), otherwise return the files
    # that correspon to each group
    if out_path != None:
        for group in file_groups.keys():
            if len(file_groups[group]) == 0:
                warnings.warn(f"Unable to retrieve any file for '{group}' group")
                pass
            else:
                for file in file_groups[group]:
                    if os.path.exists(out_path) == False:
                        os.mkdir(out_path)

                    out_group_path = out_path + "/" + group
                    if os.path.exists(out_group_path) == False:
                        os.mkdir(out_group_path)

                    # create the output directory if it does not exist
                    if os.path.exists(out_group_path) == False:
                        os.mkdir(out_path)

                    hdfs_client.download(
                        (persistent_path + "/" + file),
                        (out_group_path + "/" + file),
                        overwrite=True,
                    )
                print(
                    f"All files from '{group}' key downloaded in '{out_group_path}' path"
                )
    else:
        return file_groups


# data_selector(user_name, host, groups_info)


# in remove duplicates, you can pass a list will all pk of the file.
# how two attributes are related
def data_formatter(
    user_name,
    host,
    out_name,
    preprocessing={"Remove duplicates": None},
    spark_path="D:\spark\spark-3.5.1-bin-hadoop3",
    inp_path="temp",
    out_path="temp",
):

    # Ensure that the sparl sesion is correctly loaded
    findspark.init(spark_path)

    conf = (
        SparkConf()
        .set("spark.master", "local")
        .set("spark.app.name", "Formatted zone loader")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    mergedDF = spark.read.format("parquet").option("mergeSchema", "true").load(inp_path)

    file_names = os.listdir(inp_path)
    print(f"Starting to merge {len(file_names)} files")

    # Split columns
    unsplitted_name = mergedDF.columns[0]
    col_names = unsplitted_name.replace('"', "").split(",")

    split_DF = py.sql.functions.split(mergedDF[mergedDF.columns[0]], ",")

    for i in range(len(col_names)):
        mergedDF = mergedDF.withColumn(col_names[i], split_DF.getItem(i))

    # Remove original name
    mergedDF = mergedDF.drop(unsplitted_name)
    print(f"Dataframe has {mergedDF.count()} rows and {len(mergedDF.columns)} columns")

    # Preprocessing (if defined)
    for key, value in preprocessing.items():
        if key == "Remove duplicates":
            print("Starting to remove duplicates")
            old_n_rows = mergedDF.count()

            if value != None:
                mergedDF = mergedDF.dropDuplicates(value)
            else:
                mergedDF = mergedDF.dropDuplicates(mergedDF.columns)

            new_n_rows = mergedDF.count()

    print(f"Deleated {old_n_rows - new_n_rows} rows")

    # Add metadata to all fields
    timestr = strftime("%Y%m%d-%H%M-", gmtime())

    metadata = {"mod_time": timestr, "origin_file_names": file_names}
    for col in mergedDF.columns:
        mergedDF = mergedDF.withMetadata(col, metadata)

    print(mergedDF.schema["Any"].metadata["mod_time"])

    # Save the file
    merged_name = timestr + out_name + ".parquet"

    # mergedDF.write.parquet(out_path + "/" + merged_name, mode="overwrite")
    print("File processed")


# data_formatter(user_name, host, "test")
