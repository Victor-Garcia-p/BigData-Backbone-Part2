from time import strftime, gmtime
from datetime import datetime
import os
from hdfs import InsecureClient

import re
import os
from time import strftime, gmtime
from datetime import datetime
import shutil

import findspark
import warnings

import pyspark as py
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Select data from formatted zone to upload
def formatted_data_selector(
    user_name,
    host,
    group_name,
    inp_path="user/bdm/Formatted_zone",
    out_path="model_temp",
):

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


def model_creation(
    user_name,
    host,
    inp_path="model_temp",
    out_path="user/bdm/Model_explotation_zone",
    spark_path="D:\spark\spark-3.5.1-bin-hadoop3",
    remove_temp_files=False,
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

    # Load all dataframes in temporal directory
    # store them in a dictionary
    dfs = {}
    file_names = os.listdir(inp_path)

    # Create corrected schema for "hotels"
    new_names = [
        "register_id",
        "name",
        "institution_id",
        "institution_name",
        "created",
        "modified",
        "addresses_roadtype_id",
        "addresses_roadtype_name",
        "addresses_road_id",
        "addresses_road_name",
        "addresses_start_street_number",
        "addresses_end_street_number",
        "addresses_neighborhood_id",
        "addresses_neighborhood_name",
        "addresses_district_id",
        "addresses_district_name",
        "addresses_zip_code",
        "addresses_town",
        "addresses_main_address",
        "addresses_type",
        "values_id",
        "values_attribute_id",
        "values_category",
        "values_attribute_name",
        "values_value",
        "values_outstanding",
        "values_description",
        "secondary_filters_id",
        "secondary_filters_name",
        "secondary_filters_fullpath",
        "secondary_filters_tree",
        "secondary_filters_asia_id",
        "geo_epgs_25831_x",
        "geo_epgs_25831_y",
        "geo_epgs_4326_lat",
        "geo_epgs_4326_lon",
        "estimated_dates",
        "start_date",
        "end_date",
    ]

    for file in file_names:
        full_in_path = inp_path + "/" + file

        # Load dataframes
        short_name = file.split("-", maxsplit=2)[2]
        short_name = "".join(short_name)

        dfs[short_name] = (
            spark.read.option("multiline", "true").format("parquet").load(full_in_path)
        )

        if short_name == "hotels":
            old_names = dfs[short_name].schema.names

            for i in range(len(new_names)):
                dfs[short_name] = dfs[short_name].withColumnRenamed(
                    old_names[i], new_names[i]
                )

    # Select only usefull columns for the analysis
    selected_fields = {
        "renda_familiar": [
            "Nom_Districte",
            "Codi_Districte",
            "Índex RFD Barcelona = 100",
        ],
        "idealista": [
            "district",
            "numPhotos",
            "price",
            "floor",
            "size",
            "bathrooms",
            "hasLift",
        ],
        "lookup_renta_idealista": ["neighborhood_id"],
        "hotels": [
            "addresses_district_id",
            "name",
            "secondary_filters_name",
            "geo_epgs_4326_lat",
            "geo_epgs_4326_lon",
        ],
    }

    # Select columns and drop NA's
    for key, df in dfs.items():

        df = df.select(selected_fields[key])
        print(f"From '{key}' dataframe, selected the following schema")
        df.printSchema()
        n_col_raw = df.count()
        df = df.na.drop()

        print(
            f"Removed {((n_col_raw - df.count())/n_col_raw) * 100} % rows that contained NA's"
        )

        dfs[key] = df

    # Merge files

    """
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
        hdfs_client = InsecureClient(host, user=user_name)
        merged_name = timestr + file
        inp_full_path = inp_path + "/" + file
        out_full_path = out_path + "/" + file + "/" + merged_name

        mergedDF.write.parquet(inp_full_path, mode="overwrite")
        hdfs_client.upload(out_full_path, inp_full_path, overwrite=True)

        # Remove temporal all files (if required)
        if remove_temp_files == True:
            shutil.rmtree(full_inp_path)

        print(f"File {file} uploaded correctly at '{out_full_path}' path")
        """


master_files = {
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

master_files = {"hotels": {"Description": "Information about hotels in neighbourhoods"}}

user_name = "bdm"
host = "http://10.4.41.35:9870/"

# Load last versions of all dataframes from explotation zone
for key_file in master_files.keys():
    formatted_data_selector(user_name, host, key_file)

# Create the model
model_creation(user_name, host)
