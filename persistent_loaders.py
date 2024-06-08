import os
import io
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import json as js

from hdfs import InsecureClient
from time import strftime, gmtime
from fiona import BytesCollection
from geopandas import GeoDataFrame


def csv_loader(file_path, persistent_path, host, user_name):
    """
    Use: It reads a csv file in HDFS, adds custom metadata ("metadata_dict"), transforms it into .parquet
    and updates the file into persistent landing path

    Input:
        Path of a file in HDFS temporal landing
        Identification (host and user name) using "InsecureClient" identification from HDFS module
    Ouput:
        Path of the file in HDFS
    """
    hdfs_client = InsecureClient(host, user=user_name)

    # Create a new directory if it does not exist
    hdfs_client.makedirs(persistent_path)

    # Read from HDFS parsing the file correctly
    print("Starting to process a csv file", end=" ")
    with hdfs_client.read(file_path, encoding="utf-8") as reader:
        source = io.BytesIO((reader.read()).encode())

    parse = pa.csv.ParseOptions(delimiter=";", newlines_in_values=True)
    table = pv.read_csv(source, parse_options=parse)

    ## Add custom metadata to the table. Store all values as str.
    file_name = file_path.split("/")[-1]
    timestr = strftime("%Y%m%d-%H%M", gmtime())

    metadata_dict = {
        "ingested_timestamp": file_name[0:13],
        "original name": file_name[14 : len(file_name)],
        "format": file_name.split(".")[-1],
        "n_col": str(table.num_columns),
        "n_rows": str(table.num_rows),
        "file_size": str(table.nbytes),
        "persistent_loader_timestamp": timestr,
    }

    # Store the metadata in the table and create a parquet file
    # Create one field per column
    fields = [pa.field(name, "string") for name in table.column_names]

    schema = pa.schema(fields, metadata=metadata_dict)
    table = table.cast(schema)

    # Create a parquet with all information and load it
    name = file_name.split(".")[0]

    pq.write_table(table, name + ".parquet")
    hdfs_client.upload(persistent_path, name + ".parquet", overwrite=True)

    # Remove the file locally
    os.remove(name + ".parquet")
    print("File is processed", end=" ")


def json_loader(file_path, persistent_path, host, user_name):
    """
    Use: It reads a json file in HDFS, adds custom metadata ("metadata_dict"), transforms it into .parquet
    and updates the file into persistent landing path

    Input:
        Path of a file in HDFS temporal landing
        Identification (host and user name) using "InsecureClient" identification from HDFS module
    Ouput:
        Path of the file in HDFS
    """

    hdfs_client = InsecureClient(host, user=user_name)

    # Create a new directory if it does not exist
    hdfs_client.makedirs(persistent_path)

    # Read the file and convert it to a dataframe to process it better
    print("Starting to process a json file", end=" ")

    with hdfs_client.read(file_path, encoding="utf-8") as reader:
        t = js.loads(reader.read())

    df = pd.DataFrame.from_records(t)
    table = pa.Table.from_pandas(df)

    ## Create custom metadata and save it as str
    file_name = file_path.split("/")[-1]
    timestr = strftime("%Y%m%d-%H%M", gmtime())

    metadata_dict = {
        "ingested_timestamp": file_name[0:13],
        "original name": file_name[14 : len(file_name)],
        "format": file_name.split(".")[-1],
        "n_col": str(table.num_columns),
        "n_rows": str(table.num_rows),
        "file_size": str(table.nbytes),
        "persistent_loader_timestamp": timestr,
    }

    # Store the metadata in the table and create a parquet file
    # Append custom metadata to existing one, from pandas, following
    # https://towardsdatascience.com/saving-metadata-with-dataframes-71f51f558d8e
    custom_meta_key = "custom_metadata"
    custom_meta_json = js.dumps(metadata_dict)
    existing_meta = table.schema.metadata
    combined_meta = {
        custom_meta_key.encode(): custom_meta_json.encode(),
        **existing_meta,
    }

    table = table.replace_schema_metadata(combined_meta)

    # Create a parquet with all information and load it
    name = file_name.split(".")[0]

    pq.write_table(table, name + ".parquet")
    hdfs_client.upload(persistent_path, name + ".parquet", overwrite=True)

    # Remove the file locally
    os.remove(name + ".parquet")
    print("File is processed", end=" ")


def gpkg_loader(file_path, persistent_path, host, user_name):
    """
    Use: It reads a gpkj file in HDFS, adds custom metadata ("metadata_dict"), transforms it into .parquet
    and updates the file into persistent landing path

    Input:
        Path of a file in HDFS temporal landing
        Identification (host and user name) using "InsecureClient" identification from HDFS module
    Ouput:
        Path of the file in HDFS
    """
    hdfs_client = InsecureClient(host, user=user_name)

    # Create a new directory if it does not exist
    hdfs_client.makedirs(persistent_path)
    print("Starting to process a gpkg file", end=" ")

    # Read file data and transform it to a geodataframe to process it better
    with hdfs_client.read(file_path) as reader:
        binary_data = reader.read()
        with BytesCollection(binary_data) as f:
            crs = f.crs
            gdf = GeoDataFrame.from_features(f, crs=crs)

    # Save the file as a parquet
    file_name = file_path.split("/")[-1]
    name = file_name.split(".")[0]

    GeoDataFrame.to_parquet(gdf, name + ".parquet")

    # Read the file again to add custom metadata
    initial_table = pq.read_table(name + ".parquet")

    timestr = strftime("%Y%m%d-%H%M", gmtime())

    metadata_dict = {
        "ingested_timestamp": file_name[0:13],
        "original name": file_name[14 : len(file_name)],
        "format": file_name.split(".")[-1],
        "n_col": str(gdf.shape[1]),
        "n_rows": str(gdf.shape[0]),
        "persistent_loader_timestamp": timestr,
    }

    # Add metadata and save the file again, code extracted from
    # https://stackoverflow.com/questions/52122674/how-to-write-parquet-metadata-with-pyarrow
    existing_metadata = initial_table.schema.metadata
    merged_metadata = {**metadata_dict, **existing_metadata}
    all_data = initial_table.replace_schema_metadata(merged_metadata)

    pq.write_table(all_data, name + ".parquet")

    # Upload the final file and delete it locally
    initial_table = pq.read_table(name + ".parquet")
    hdfs_client.upload(persistent_path, name + ".parquet", overwrite=True)

    os.remove(name + ".parquet")
    print("File is processed", end=" ")
