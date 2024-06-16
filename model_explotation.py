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
from pyspark.sql.functions import split, col, avg, count

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

    for file in file_names:
        full_in_path = inp_path + "/" + file

        # Load dataframes
        short_name = file.split("-", maxsplit=2)[2]
        short_name = "".join(short_name)

        dfs[short_name] = (
            spark.read.option("multiline", "true").format("parquet").load(full_in_path)
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
        "lookup_renta_idealista": ["district", "district_id"],
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

    # Calculate KPI's: Average number of hotels and stars per district
    dfs["hotels"] = (
        dfs["hotels"]
        .withColumn(
            "stars",
            split(col("secondary_filters_name"), " ").cast("array<int>"),
        )
        .withColumn("stars", col("stars")[1])
    )
    dfs["hotels"] = dfs["hotels"].drop("secondary_filters_name")

    # Declare numeric variables
    dfs["hotels"] = (
        dfs["hotels"]
        .withColumn(
            "addresses_district_id",
            dfs["hotels"]["addresses_district_id"].cast(IntegerType()),
        )
        .withColumn(
            "geo_epgs_4326_lat",
            dfs["hotels"]["geo_epgs_4326_lat"].cast(IntegerType()),
        )
        .withColumn(
            "geo_epgs_4326_lon",
            dfs["hotels"]["geo_epgs_4326_lon"].cast(IntegerType()),
        )
    )

    # Transform "districts names" from "renda familiar" so that they
    # match the lookup table
    dfs["renda_familiar"] = (
        dfs["renda_familiar"]
        .withColumn(
            "Nom_Districte",
            split(col("Nom_Districte"), '"'),
        )
        .withColumn("Nom_Districte", col("Nom_Districte")[1])
        .withColumn(
            "Índex RFD Barcelona = 100",
            split(col("Índex RFD Barcelona = 100"), '"'),
        )
        .withColumn("Índex RFD Barcelona = 100", col("Índex RFD Barcelona = 100")[1])
    )

    # Group dataframes by district to reduce cardinality of joins
    dfs["hotels"] = (
        dfs["hotels"]
        .groupBy("addresses_district_id")
        .agg(
            avg("stars").alias("Avg_stars"),
            count("name").alias("N_hotels"),
            avg("geo_epgs_4326_lat").alias("Avg_lat"),
            avg("geo_epgs_4326_lon").alias("Avg_long"),
        )
    )

    dfs["renda_familiar"] = (
        dfs["renda_familiar"]
        .groupBy(["Nom_Districte", "Codi_Districte"])
        .agg(avg("Índex RFD Barcelona = 100").alias("Avg_Index_RFD"))
    )
    print(dfs["renda_familiar"].show())

    """
    ## Join files
    # 'renta familiar' and lookup tables

    dfs["model"] = dfs["renda_familiar"].join(
        dfs["lookup_renta_idealista"],
        dfs["renda_familiar"].Nom_Districte == dfs["lookup_renta_idealista"].district,
        "inner",
    )

    dfs["model"].printSchema()
    print(dfs["model"].count())

    # join with hotels

    # join with idealista
    print(dfs["model"].count(), "model BEFORE")
    print(dfs["idealista"].count(), "idealista")

    dfs["model"] = dfs["model"].join(
        dfs["idealista"],
        dfs["model"].district == dfs["idealista"].district,
        "inner",
    )
    dfs["model"] = dfs["model"].drop("district")

    print(dfs["model"].count())

    # Upload solution to hdfs

    # Use tableau

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

# master_files = {"hotels": {"Description": "Information about hotels in neighbourhoods"}}

user_name = "bdm"
host = "http://10.4.41.35:9870/"

# Load last versions of all dataframes from explotation zone
# for key_file in master_files.keys():
#    formatted_data_selector(user_name, host, key_file)

# Create the model
model_creation(user_name, host)
