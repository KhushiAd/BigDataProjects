# Allows us to connect to the data source and pulls the information
from sodapy import Socrata
import requests
from requests.auth import HTTPBasicAuth
import argparse
import sys
import os
import json

# Creates a parser. Parser is the thing where you add your arguments. 
parser = argparse.ArgumentParser(description='311 Requests Data')
# In the parse, we have two arguments to add.
# The first one is a required argument for the program to run. If page_size is not passed in, don’t let the program to run
parser.add_argument('--page_size', type=int, help='how many rows to get per page', required=True)
# The second one is an optional argument for the program to run. It means that with or without it your program should be able to work.
parser.add_argument('--num_pages', type=int, help='how many pages to get in total')
# Take the command line arguments passed in (sys.argv) and pass them through the parser.
# Then you will ned up with variables that contains page size and num pages.  
args = parser.parse_args(sys.argv[1:])
print(args)



INDEX_NAME=os.environ["INDEX_NAME"]
DATASET_ID=os.environ["DATASET_ID"]
APP_TOKEN=os.environ["APP_TOKEN"]
ES_HOST=os.environ["ES_HOST"]
ES_USERNAME=os.environ["ES_USERNAME"]
ES_PASSWORD=os.environ["ES_PASSWORD"]


if __name__ == '__main__': 
    

    try:
        #Using requests.put(), we are creating an index (db) first.
        resp = requests.put(f"{ES_HOST}/{INDEX_NAME}", auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            json={
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
             
                "mappings": {
                    "properties": {
                        "starfire_incident_id": {"type": "keyword"},
                        "incident_datetime": {"type": "date"},
                        "incident_borough": {"type": "keyword"},
                        "alarm_source_description_tx": {"type": "keyword"},
                        "highest_alarm_level": {"type": "keyword"},
                        "incident_classification": {"type": "keyword"},
                        # "zipcode": {"type": "keyword"},
                        "incident_response_seconds_qy": {"type": "float"}, #This should normally be considered for keyword 
                        #but I need a numeric field for the next steps. 
                    }
                },
            }
        )
        resp.raise_for_status()
        print(resp.json())
        
    #If the index is already created, it will raise an excepion and the program will not be crashed. 
    except Exception as e:
        print("Index already exists! Skipping")    
    
    # Remove the comments
    client = Socrata("data.cityofnewyork.us", APP_TOKEN, timeout=10000)

    max_count = int(client.get(DATASET_ID, select='Count(*)')[0]['Count'])
    
    #is num page is 8m and data point is 8m that will creatse issue 
    
    # if not args.num_pages and not args.page_size:
    #     pages = 2000
    #     num_pages = max_count/pages
    # elif not args.num_pages and args.page_size:
    #     pages = args.page_size
    #     num_pages = max_count/pages
    # elif args.num_pages and not args.page_size:
    #     num_pages = args.num_pages
    #     pages = max_count/num_pages
    # else:
    #   pages = args.page_size
    #   num_pages = args.num_pages
    page_size = args.page_size

    try:
        num_pages = args.num_pages
    except Exception as e:
        print("No number given", e)
        num_pages = max_count/page_size

    count = 0
    offset = 0

    
    for pages in range(num_pages):
        
        rows = client.get(DATASET_ID, limit=page_size,where='starfire_incident_id IS NOT NULL AND incident_datetime IS NOT NULL',offset=offset)
        offset+=args.page_size

        es_rows=[]

        for row in rows:
            try:
                # Convert
                es_row = {}
                es_row["starfire_incident_id"] = row["starfire_incident_id"]
                es_row["incident_datetime"] = row["incident_datetime"]
                es_row["incident_borough"] = row["incident_borough"]
                es_row["alarm_source_description_tx"] = row["alarm_source_description_tx"]
                es_row["highest_alarm_level"] = row["highest_alarm_level"]
                es_row["incident_classification"] = row["incident_classification"]
                #The data that we will collect comes in strings. That's why we might need to do cleaning here. 
                # es_row["zipcode"] = row["zipcode"]  
                es_row["incident_response_seconds_qy"] = float(row["incident_response_seconds_qy"]) 
            except Exception as e:
                print (f"Error!: {e}, skipping row: {row}")
                continue
                
            es_rows.append(es_row)
    
        bulk_upload_data = ""
        for line in es_rows:
            print(f'Handling row {line["starfire_incident_id"]}')
            action = '{"index": {"_index": "' + INDEX_NAME + '", "_type": "_doc", "_id": "' + line["starfire_incident_id"] + '"}}'
            data = json.dumps(line)
            bulk_upload_data += f"{action}\n"
            bulk_upload_data += f"{data}\n"

        try:
            # Upload to Elasticsearch by creating a document
            resp = requests.post(f"{ES_HOST}/_bulk",
                # We upload es_row to Elasticsearch
                        data=bulk_upload_data,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers = {"Content-Type": "application/x-ndjson"})
            resp.raise_for_status()
            count+=1
            print ('Done',count)

            # If it fails, skip that row and move on.
        except Exception as e:
            print(f"Failed to insert in ES: {e}")
