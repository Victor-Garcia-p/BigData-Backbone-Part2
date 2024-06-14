from time import strftime, gmtime
from datetime import datetime
import os


def formatted_data_selector(
    user_name,
    host,
    group_name,
    inp_path="user/bdm/Formatted_zone",
    out_path="temporal",
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


# formatted_data_selector(user_name, host, "hotels")
