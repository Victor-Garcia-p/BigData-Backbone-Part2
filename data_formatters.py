import re
from hdfs import InsecureClient
import os
from time import strftime, gmtime
from datetime import datetime
import shutil

import findspark
import warnings

import pyspark as py
from pyspark.sql import SparkSession
from pyspark import SparkConf


# Select data for formatting zone
def persistent_data_selector(
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
                        (inp_path + "/" + file),
                        (out_group_path + "/" + file),
                        overwrite=True,
                    )
                print(
                    f"All files from '{group}' key downloaded in '{out_group_path}' path"
                )
    else:
        return file_groups


# in remove duplicates, you can pass a list will all pk of the file.
# how two attributes are related
def formatted_data_loader(
    user_name,
    host,
    groups_info,
    inp_path="temp",
    out_path="user/bdm/Formatted_zone",
    spark_path="D:\spark\spark-3.5.1-bin-hadoop3",
    remove_temp_files=True,
):
    # Ensure that the sparl sesion is correctly loaded,
    # then create a sesion
    findspark.init(spark_path)

    conf = (
        SparkConf()
        .set("spark.master", "local")
        .set("spark.app.name", "Formatted zone loader")
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print("Spark sesion correctly initialized")

    hdfs_client = InsecureClient(host, user=user_name)
    for file in groups_info.keys():
        full_inp_path = inp_path + "/" + file

        file_names = os.listdir(full_inp_path)
        print(f"Starting to merge {len(file_names)} files for {file} key")

        # Try to find a non-empty file in the folder
        mergedDF_len = 0
        while mergedDF_len == 0:
            name = file_names.pop(0)
            format = name.split(".")[-1]

            mergedDF = (
                spark.read.option("multiline", "true")
                .format(str(format))
                .load(full_inp_path + "/" + name)
            )
            mergedDF_len = len(mergedDF.columns)

        # Join all files in the folder
        while len(file_names) > 0:
            name = file_names.pop(0)
            format = name.split(".")[-1]
            mergedDF2 = (
                spark.read.option("multiline", "true")
                .format(str(format))
                .load(full_inp_path + "/" + name)
            )

            # Only join non-empty files
            if len(mergedDF2.columns) > 0:
                mergedDF = mergedDF.unionByName(mergedDF2, allowMissingColumns=True)
            else:
                warnings.warn(f"File: {name} is empty and was not processed")

        n_rows_old = mergedDF.count()
        print(
            f"Merged dataframe has {n_rows_old} rows and {len(mergedDF.columns)} columns"
        )

        # Split columns (if necessary)
        if len(mergedDF.columns) == 1:
            unsplitted_name = mergedDF.columns[0]
            col_names = unsplitted_name.replace('"', "").split(",")

            split_DF = py.sql.functions.split(mergedDF[mergedDF.columns[0]], ",")

            for i in range(len(col_names)):
                mergedDF = mergedDF.withColumn(col_names[i], split_DF.getItem(i))

            # Remove original name
            mergedDF = mergedDF.drop(unsplitted_name)

        # Preprocessing (remove duplicates)
        mergedDF = mergedDF.dropDuplicates(mergedDF.columns)

        n_rows_new = mergedDF.count()
        print(f"Deleated {n_rows_old - n_rows_new} duplicated rows from {file} file")

        # Add metadata to all fields
        timestr = strftime("%Y%m%d-%H%M-", gmtime())

        metadata = {"mod_time": timestr, "origin_file_names": file_names}
        metadata.update(groups_info[file])

        for col in mergedDF.columns:
            mergedDF = mergedDF.withMetadata(col, metadata)

        # Save the file
        merged_name = timestr + file
        inp_full_path = inp_path + "/" + file
        out_full_path = out_path + "/" + file + "/" + merged_name

        mergedDF.write.parquet(inp_full_path, mode="overwrite")
        hdfs_client.upload(out_full_path, inp_full_path, overwrite=True)

        # Remove temporal all files (if required)
        if remove_temp_files == True:
            shutil.rmtree(full_inp_path)

        print(f"File {file} uploaded correctly at '{out_full_path}' path")
