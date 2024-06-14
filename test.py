from hdfs import InsecureClient

inp_path = "user/bdm/Formatted_zone/idealista"
user_name = "bdm"
host = "http://10.4.41.35:9870/"

hdfs_client = InsecureClient(host, user=user_name)
# all_files = hdfs_client.delete(inp_path, recursive=True)
all_files = hdfs_client.list(inp_path)
print(all_files)
