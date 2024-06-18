import os
import re
from time import strftime, gmtime
from hdfs import InsecureClient

from datetime import datetime
import shutil

import findspark

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import split


def persistent_data_selector(
    log,
    user_name,
    host,
    groups_info,
    time_limit=[datetime(1, 1, 1), datetime(9999, 12, 12)],
    inp_path="user/bdm/persistent_landing",
    out_path="temp",
):
    """
    Use: Select files from HDFS to be merged in new files.
    To do it, we match keywords defined by the user with filenames in HDFS,
    for each new file. This function also downloads data if an out path is defined.
    """

    # Create a dictionary to store names of files that will
    # be merged for each key
    file_groups = {}
    for keyword in groups_info:
        file_groups[keyword] = []

    # Scan all files on the directory and process only those that
    # are bounded in the time limit
    hdfs_client = InsecureClient(host, user=user_name)
    all_files = hdfs_client.list(inp_path)

    for file in all_files:
        full_name = file.split("-", maxsplit=2)
        date_format = "%Y%m%d %H%M"

        date = datetime.strptime(" ".join(full_name[0:2]), date_format)

        if date >= time_limit[0] and date <= time_limit[1]:
            process_file = set(full_name[2:])

            # Match filenames with keywords defined for each file
            # (only exact coincidences are considered)
            while len(process_file) != 0:
                file_processed = process_file.pop()

                for group_name in file_groups.keys():

                    for keyword in groups_info[group_name]["keywords"]:
                        if re.search(keyword, file_processed):
                            file_groups[group_name].append(file)

    k = [i for i in file_groups.keys()]
    n = [len(i) for i in file_groups.values()]

    log.info("Found", n, "files that matches", k, "keys")

    # Download files to a temporal folder or simply return names
    # of the files from HDFS to merge
    if out_path != None:
        for group in file_groups.keys():
            if len(file_groups[group]) == 0:
                log.warn(f"Unable to retrieve any file for '{group}' group")
                pass
            else:
                for file in file_groups[group]:
                    if os.path.exists(out_path) == False:
                        os.mkdir(out_path)

                    out_group_path = out_path + "/" + group
                    if os.path.exists(out_group_path) == False:
                        os.mkdir(out_group_path)

                    # Create the temp directory if it does not exist
                    if os.path.exists(out_group_path) == False:
                        os.mkdir(out_path)

                    hdfs_client.download(
                        (inp_path + "/" + file),
                        (out_group_path + "/" + file),
                        overwrite=True,
                    )
                log.info(
                    f"All files from '{group}' key downloaded in '{out_group_path}' path"
                )
    else:
        return file_groups


def formatted_data_loader(
    log,
    user_name,
    host,
    groups_info,
    inp_path="temp",
    out_path="user/bdm/Formatted_zone",
    spark_path="D:\spark\spark-3.5.1-bin-hadoop3",
    remove_temp_files=True,
):
    """
    Use: Merge files downloaded from persistent zone and upload them to
    the formatted zone. All files should have csv or parquet format and they
    should be located in subfolders of the inp path.

    Ex:
    -Inp_path
        -Folder (File1): With all files that will be merged to create File1
        -Folder (File2): With all files that will be merged to create File2
    """

    # Ensure that the sparl sesion is correctly defined then create a sesion
    findspark.init(spark_path)

    conf = (
        SparkConf()
        .set("spark.master", "local")
        .set("spark.app.name", "Formatted zone loader")
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    log.info("Spark sesion correctly initialized")

    for file in groups_info.keys():

        # Get names of all files to merge
        full_inp_path = inp_path + "/" + file
        file_names = os.listdir(full_inp_path)

        # Try to find a non-empty file in the folder
        mergedDF_len = 0
        while mergedDF_len == 0:
            name = file_names.pop(0)
            format = name.split(".")[-1]

            if format == "csv":
                mergedDF = (
                    spark.read.option("header", True)
                    .option("delimiter", ",")
                    .option("encoding", "UTF-16")  # change encoding if required
                    .csv(full_inp_path + "/" + name)
                )

            if format == "parquet":
                mergedDF = (
                    spark.read.option("multiline", "true")
                    .format(str(format))
                    .load(full_inp_path + "/" + name)
                )

            mergedDF_len = len(mergedDF.columns)

        # Join all files in the folder
        if len(file_names) != 0:

            log.info(f"Starting to merge {len(file_names)} files for {file} key")
            while len(file_names) > 0:

                name = file_names.pop(0)
                format = name.split(".")[-1]
                mergedDF2 = (
                    spark.read.option("multiline", "true")
                    .format(str(format))
                    .load(full_inp_path + "/" + name)
                )

                # Only merge non-empty files
                if len(mergedDF2.columns) > 0:
                    mergedDF = mergedDF.unionByName(mergedDF2, allowMissingColumns=True)
                else:
                    log.warn(f"File: {name} is empty and was not processed")

        n_rows_old = mergedDF.count()
        log.info(
            f"Merged dataframe has {mergedDF.count()} rows and {len(mergedDF.columns)} columns"
        )

        # Split columns (if necessary)
        if len(mergedDF.columns) == 1:
            unsplitted_name = mergedDF.columns[0]
            col_names = unsplitted_name.replace('"', "").split(",")

            split_DF = split(mergedDF[mergedDF.columns[0]], ",")

            for i in range(len(col_names)):
                mergedDF = mergedDF.withColumn(col_names[i], split_DF.getItem(i))

            # Remove original col names
            mergedDF = mergedDF.drop(unsplitted_name)

        # Remove duplicated rows
        mergedDF = mergedDF.dropDuplicates(mergedDF.columns)

        n_rows_new = mergedDF.count()
        log.info(f"Deleated {n_rows_old - n_rows_new} duplicated rows from {file} file")

        # Add metadata to all fields:
        # timstamp, files that were merged and information defined by the user
        timestr = strftime("%Y%m%d-%H%M-", gmtime())

        metadata = {"mod_time": timestr, "origin_file_names": file_names}
        metadata.update(groups_info[file])

        for col in mergedDF.columns:
            mergedDF = mergedDF.withMetadata(col, metadata)

        # Save the file in local and upload it to the formatted zone
        merged_name = timestr + file
        inp_full_path = inp_path + "/" + file
        out_full_path = out_path + "/" + file + "/" + merged_name

        mergedDF.write.parquet(inp_full_path, mode="overwrite")
        hdfs_client = InsecureClient(host, user=user_name)
        hdfs_client.upload(out_full_path, inp_full_path, overwrite=True)
        log.info(f"File {file} uploaded correctly at '{out_full_path}' path")

        # Remove all local files (if required)
        if remove_temp_files == True:
            shutil.rmtree(full_inp_path)


def formatted_data_selector(
    log,
    user_name,
    host,
    group_name,
    inp_path="user/bdm/Formatted_zone",
    out_path="model_temp",
):
    """
    Use: For each file requested download the latest version from HDFS to local
    """

    # Scan all files on the directory and process only those that are bounded
    # in the time limit
    hdfs_client = InsecureClient(host, user=user_name)
    all_files = hdfs_client.list(inp_path + "/" + group_name)
    max_timestamp = datetime(1, 1, 1)
    last_version = all_files.pop(0)

    while len(all_files) != 0:
        file = all_files.pop(0)
        full_name = file.split("-", maxsplit=2)
        date_format = "%Y%m%d %H%M"

        date = datetime.strptime(" ".join(full_name[0:2]), date_format)

        if date > max_timestamp:
            max_timestamp = date
            last_version = file

    # Download last version of the dataframe
    if os.path.exists(out_path) == False:
        os.mkdir(out_path)

    dataframe_path = out_path + "/" + last_version
    if os.path.exists(dataframe_path) == False:
        os.mkdir(dataframe_path)

    all_files = hdfs_client.list(inp_path + "/" + group_name + "/" + last_version)
    if os.path.exists(out_path) == False:
        os.mkdir(out_path)

    for df_file in all_files:
        in_full_path = inp_path + "/" + group_name + "/" + last_version + "/" + df_file
        out_full_path = out_path + "/" + last_version + "/" + df_file
        hdfs_client.download(
            (in_full_path),
            (out_full_path),
            overwrite=True,
        )
    log.info(f"File {last_version} downloaded correctly")
