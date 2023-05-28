import time
import pyarrow.parquet as pq
from elasticsearch import Elasticsearch
import pandas as pd
from elasticsearch.helpers import bulk
import os

def query_files_dropped_msgs():
    # Define the search query
    query = {
        "query": {
            "match_all": {}
        },
        "sort": [
            {"s_no": {"order": "asc"}}
        ]
    }
    # Calculate the percentage of battery status per station ID
    # search_results = es.search(index=index_name, body=query)
    search_results = es.search(index=index_name, body=query, scroll='5m', size=1000)
    hits = search_results["hits"]["hits"]
    station_stats = {}
    for hit in hits:
        source = hit["_source"]
        station_id = source["station_id"]
        sno = source["s_no"]
        dropped = 0
        if station_id not in station_stats:
            if sno > 1: dropped = sno -1
        else:
            dropped = station_stats[station_id]["dropped"]
            if station_stats[station_id]["curr_sno"]+1 < sno:
                dropped += (sno - station_stats[station_id]["curr_sno"] - 1)

        station_stats[station_id] = {"curr_sno": sno, "dropped": dropped}


    #Print the percentage of low battery status per station ID
    for station_id, stats in station_stats.items():
        total = stats["curr_sno"]
        dropped = stats["dropped"]
        percentage = (dropped / total) * 100 if total > 0 else 0
        print(f"Station ID: {station_id}, dropped Percentage: {percentage:.2f}%")


def query_files_battery_status():
    # Define the search query
    query = {
        "query": {
            "match_all": {}
        }
    }

    # Calculate the percentage of battery status per station ID
    #search_results = es.search(index=index_name, body=query)
    search_results = es.search(index=index_name, body=query, scroll='5m', size=1000)
    hits = search_results["hits"]["hits"]
    ids = []
    station_stats = {}
    for hit in hits:
        source = hit["_source"]
        station_id = source["station_id"]
        ids.append(station_id)
        battery_status = source["battery_status"]
        if station_id not in station_stats:
            station_stats[station_id] = {"total": 0, "low_battery": 0}
        station_stats[station_id]["total"] += 1
        if battery_status == "low":
            station_stats[station_id]["low_battery"] += 1

    # Print the percentage of low battery status per station ID
    print(station_stats.keys())
    for station_id, stats in station_stats.items():
        total = stats["total"]
        low_battery = stats["low_battery"]
        percentage = (low_battery / total) * 100 if total > 0 else 0
        print(f"Station ID: {station_id}, Low Battery Percentage: {percentage:.2f}%")
    print("query idssss")
    print(set(ids))

def load_index_files(files):
    for file in files:
        print("loading file path:" + file)
        # Read Parquet file using Pandas
        df = pd.read_parquet(file)
        print("df")

        # Convert Pandas DataFrame to a list of dictionaries
        data = df.to_dict(orient='records')
        print(data)
        print("data")

        # Bulk index data into Elasticsearch
        actions = [
            {
                '_index': index_name,
                '_source': d
            }
            for d in data
        ]
        print("actions")
        bulk(es, actions)
        es.indices.refresh(index=index_name)


def elastic_search_worker():
    # while True :
    print("start of work batch")
    indexed_files = []
    # Set the path of the folder to read
    folder_path = '/home/hadoopuser/Downloads/parquet'

    # Get a list of all files in the folder
    current_files = []
    for folder in os.listdir(folder_path):
        # Check if the item is a folder
        if os.path.isdir(os.path.join(folder_path, folder)):
            print('station id:', folder)

            # Loop through all files in the folder
            for sub_folder in os.listdir(os.path.join(folder_path, folder)):
                for file in os.listdir(os.path.join(folder_path, folder, sub_folder)):
                    file = os.path.join(folder_path, folder, sub_folder, file)
                    print('File:', file)
                    if os.path.getsize(file) != 0:
                        current_files.append(file)

    not_indexed_files = set(current_files) - set(indexed_files)
    # index them
    load_index_files(not_indexed_files)
    indexed_files = current_files

    # one minute sleep
    # time.sleep(60)


# Define Elasticsearch connection details
es = Elasticsearch(
    ['https://localhost:9200'], verify_certs=False,
    basic_auth=('elastic', 'u*Q_ZKivFbgtQFHJ8KLu')
)

# Define Elasticsearch index name and document type
index_name = 'weather_stations'
doc_type = 'parquet'

#elastic_search_worker()
#query_files_battery_status()
query_files_dropped_msgs()
