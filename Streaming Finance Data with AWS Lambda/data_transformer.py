import json
import boto3
from datetime import datetime
import yfinance as yf
import pandas as pd
from time import sleep
import os

REGION = os.environ['REGION']
STREAM_NAME = os.environ['STREAM_NAME']

kinesis = boto3.client('kinesis', REGION)

def put_record(ydat,ticker):
    
    data = {}
    
    for datetime, row in ydat.iterrows():
        data['high']=row["High"]
        data['low']=row["Low"]
        data['volatility']=row["volitility"]
        data["ts"] = str(datetime)
        data['name'] = ticker
    
        output = json.dumps(data)+"\n"
    
        #print(output)
        kinesis_output = kinesis.put_record(
                    StreamName=STREAM_NAME,
                    Data=output,
                    PartitionKey="partitionkey")
        print(kinesis_output)
    
    
def lambda_handler(event, context):
    start_date = "2023-04-10"
    end_date = "2023-04-21"
    interval = "5m"
    tickers = ["AMZN","BABA","WMT","EBAY","TGT","SHOP","BBY","HD","COST","KR"]
    
    for ticker in tickers:

        info = yf.Ticker(ticker)
        ydat = info.history(start=start_date,end=end_date,interval=interval)
        ydat['volitility'] = ydat["High"]-ydat["Low"]
        
        put_record(ydat,ticker)
        
        sleep(10) 
    
    return {
        'statusCode': 200,
        'body': json.dumps('Done!')
    }
