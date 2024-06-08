import os
from zipfile import ZipFile
from data_collectors import *
from persistent_loaders import *
from progress.bar import Bar


def zip_collector(inp_path="data.zip", out_path="temp"):
    """
    Use: Read all files and folders of a zip file and unzip them in a new folder
    Input: zip data path
    Output: folder path to unzip
    """

    with ZipFile(inp_path, "r") as zip_ref:
        # Unzip in a new folder
        zip_ref.extractall(path=out_path)

        # Inform the user about the number of files and folders
        # names  after unzip
        zip_files = os.listdir(out_path)

        # Count number of files
        count = 0
        folders = False
        for _, _, files in os.walk(out_path):
            count += len(files)
            if files == []:
                folders = True

        print(f"Extracted {count} file(s)", end=" ")

        # Count number of folders and their names
        if folders == True:
            print(f"from {len(zip_files)} folders:", end=" ")

        for i in range(len(zip_files)):
            print(zip_files[i], end="")
            if i != (len(zip_files) - 1):
                print(",", end=" ")


def data_collectors_ochestration(
    host,
    user_name,
    data_path="temp",
    uploaded_files=set(),
    hdfs_path="user/bdm/temporal_landing",
    overwrite=True,
):
    """
    Use: Load data from local to HDFS server. It allows to load multiple files if located in the same folder, otherwise
    it is required to call the function multiple times, updating the "uploaded_files" set.
    It only works with csv, json and gpkg files

    Input:
      Identification (host and user name) using "InsecureClient" identification from HDFS module
      Data path - Path of the file or folder to load in temporal landing
      uploaded_files - set with paths of files that are loaded into temporal landing
      hdfs_path - Path to create the temporal landing in HDFS

    Output:
      A updated version of "uploaded_files" set
    """
    paths_to_process = []
    files_to_process = []

    # Check if the path is a file or a folder
    if data_path.find(".") != -1:
        full_path = os.path.abspath(data_path)
        paths_to_process.append(full_path)

        name = os.path.split(data_path)[-1]
        files_to_process.append(name)

    else:
        for root, _, files in os.walk(data_path, topdown=False):
            for name in files:
                path = os.path.join(root, name)
                full_path = os.path.abspath(path)

                paths_to_process.append(full_path)
                files_to_process.append(name)

    # Load files into HDFS untill progressively
    last_file = len(files_to_process)

    with Bar("Processing...", suffix="%(percent).1f%%", max=last_file) as bar:
        for _ in range(last_file):
            path = paths_to_process.pop()
            file = files_to_process.pop()
            format = file.split(".")[-1]

            # Call the correct data collector depending on the format
            # and update the bar
            if format == "csv":
                uploaded_file = csv_collector(
                    path, hdfs_path, host, user_name, overwrite
                )
                uploaded_files.add(uploaded_file)
                bar.next()

            elif format == "json":
                uploaded_file = json_collector(
                    path, hdfs_path, host, user_name, overwrite
                )
                uploaded_files.add(uploaded_file)
                bar.next()

            elif format == "gpkg":
                uploaded_file = gpkg_collector(
                    path, hdfs_path, host, user_name, overwrite
                )
                uploaded_files.add(uploaded_file)
                bar.next()

            else:
                print("There is not a data collector for", format, "format", end=" ")
                bar.next()

        return uploaded_files


def persistent_loader_orchestration(
    file_names, host, user_name, persistent_path="user/bdm/persistent_landing"
):
    """
    Info: Read a set of files from temporal landing, transform the to .parquet and load them into persistent landing.
    It only works with csv, json and gpkg files
    Input:
      Identification (host and user name) using "InsecureClient" identification from HDFS module
      File_names - A set with paths of temporal landing to process
      Persistent landing path in HDFS
    """
    last_file = len(file_names)

    with Bar("Processing...", suffix="%(percent).1f%%", max=last_file) as bar:
        for _ in range(last_file):

            # For a given file, select the correct loader
            full_path = str(file_names.pop())
            name = str(os.path.split(full_path))
            format = name.split(".")[-1][:-2]

            if format == "csv":
                csv_loader(full_path, persistent_path, host, user_name)
                bar.next()
            elif format == "json":
                json_loader(full_path, persistent_path, host, user_name)
                bar.next()
            elif format == "gpkg":
                gpkg_loader(full_path, persistent_path, host, user_name)
                bar.next()
            else:
                print("There is not a data loader for", format, "format", end=" ")
                bar.next()
