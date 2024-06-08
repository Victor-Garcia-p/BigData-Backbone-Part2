import os
from time import strftime, gmtime
from hdfs import InsecureClient


def csv_collector(local_path, hdfs_path, host, user_name, overwrite=False):
    """
    Use: It loads csv files into HDFS temporal landing, in a "csv" subfolder,
    and adds a timestamp to the name in format (YYYYMMDD-HHMM)
    Input:
        Path of a file in local
        Identification (host and user name) using "InsecureClient" identification from HDFS module
    Ouput:
        Path of the file in HDFS
    """

    hdfs_client = InsecureClient(host, user=user_name)

    # Create a new name for the file and rename it locally
    timestr = strftime("%Y%m%d-%H%M-", gmtime())
    path, filename = os.path.split(local_path)
    filename = os.path.splitext(filename)[0]
    newfilename = timestr + filename + ".csv"

    newpath = os.path.join(path, newfilename)
    os.rename(local_path, newpath)

    # Create a new directory in HDFS if it does not exist
    csv_directory = hdfs_path + "/csv"
    hdfs_client.makedirs(csv_directory)

    # Update it into HDFS and rename the local file into the original name,
    # even if the update fails
    try:
        print("Starting to upload a csv file", end=" ")
        hdfs_client.upload(csv_directory, newpath, overwrite=overwrite)
        print("Upload was successfull", end=" ")
        os.rename(newpath, local_path)
    except Exception:
        os.rename(newpath, local_path)
        raise
    return str(csv_directory + "/" + newfilename)


def json_collector(local_path, hdfs_path, host, user_name, overwrite=False):
    """
    Use: It loads json files into HDFS temporal landing, in a "json" subfolder,
    and adds a timestamp to the name in format (YYYYMMDD-HHMM)
    Input:
        Path of a file in local
        Identification (host and user name) using "InsecureClient" identification from HDFS module
    Ouput:
        Path of the file in HDFS
    """
    hdfs_client = InsecureClient(host, user=user_name)

    # Create a new name for the file and rename it locally
    timestr = strftime("%Y%m%d-%H%M-", gmtime())
    path, filename = os.path.split(local_path)
    filename = os.path.splitext(filename)[0]
    newfilename = timestr + filename + ".json"
    newpath = os.path.join(path, newfilename)
    os.rename(local_path, newpath)

    # Create a new directory if it does not exist
    json_directory = hdfs_path + "/json"
    hdfs_client.makedirs(json_directory)

    # Update it into HDFS and rename the local file into the original name,
    # even if the update fails
    try:
        print("Starting to upload a json file", end=" ")
        hdfs_client.upload(json_directory, newpath, overwrite=overwrite)
        print("Upload was successfull", end=" ")
        os.rename(newpath, local_path)
    except Exception:
        os.rename(newpath, local_path)
        raise
    return str(json_directory + "/" + newfilename)


def gpkg_collector(local_path, hdfs_path, host, user_name, overwrite=False):
    """
    Use: It loads gpkg files into HDFS temporal landing, in a "json" subfolder,
    and adds a timestamp to the name in format (YYYYMMDD-HHMM)
    Input:
        Path of a file in local
        Identification (host and user name) using "InsecureClient" identification from HDFS module
    Ouput:
        Path of the file in HDFS
    """
    hdfs_client = InsecureClient(host, user=user_name)

    # Create a new name for the file and rename it localy
    timestr = strftime("%Y%m%d-%H%M-", gmtime())
    path, filename = os.path.split(local_path)
    filename = os.path.splitext(filename)[0]
    newfilename = timestr + filename + ".gpkg"
    newpath = os.path.join(path, newfilename)
    os.rename(local_path, newpath)

    # Create a new directory if it does not exist
    gpkg_directory = hdfs_path + "/gpkg"
    hdfs_client.makedirs(gpkg_directory)

    # Update it into HDFS and rename the local file into the original name,
    # even if the update fails
    try:
        print("Starting to upload a gpkg file", end=" ")
        hdfs_client.upload(gpkg_directory, newpath, overwrite=overwrite)
        print("Upload was successfull", end=" ")
        os.rename(newpath, local_path)
    except Exception:
        os.rename(local_path, newpath)
        raise
    return str(gpkg_directory + "/" + newfilename)
