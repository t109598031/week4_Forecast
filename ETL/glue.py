import requests
import pandas as pd
import boto3
import json
import re
from datetime import datetime
import io
import os

elasticsearchUrl = ""
elasticsearchIndex = ""
AWS_S3_BUCKET = ""
S3_FILE_NAME = ""
AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
es_AN = ""
es_password = ""


s3_client = boto3.client(
    "s3",
    aws_access_key_id = AWS_ACCESS_KEY,
    aws_secret_access_key = AWS_SECRET_KEY,
    region_name = "us-west-2"
    )

def elasticsearchQueryGetMatchAll(es_url,index_name,num):
    query = json.dumps({'query': {'match_all': {}}})
    headers={'Accept': 'application/json','Content-type': 'application/json'}
    #這裡因為是使用t3.samll一次能query的資料量比較小，為了防止ES崩潰所以一次只query50筆資料
    response = requests.get("{es_url}/{index_name}/_search/?size={doc_num}&from={num}".format(es_url = es_url,
                                                                                            index_name = index_name,
                                                                                            doc_num = 50,
                                                                                            num = num),
                        data=query, 
                        auth=(es_AN,es_password),
                        headers = headers)
    result = json.loads(response.text)

    return result['hits']['hits']
    
    
response = requests.get("{es_url}/_cat/count/{index_name}".format(es_url = elasticsearchUrl,index_name = elasticsearchIndex),auth=(es_AN,es_password))
docCount = int(re.split('\s',response.text)[2])
data = []
start = 0
for doc in range(int(docCount/50)+1):
    data += elasticsearchQueryGetMatchAll(elasticsearchUrl,elasticsearchIndex,start)
    start+=50
    
    
    
csv = []
for doc in data:
    personCount = 0
    for labels in doc['_source']['response']['Labels']:  
        if labels['Name'] =='Person':
            personCount = len(labels['Instances'])
    dateTime = datetime.strptime(doc['_source']['eventTimestamp'],"%Y-%m-%dT%H:%M:%SZ")
    dateTimeFormat = dateTime.strftime("%Y-%m-%d %H:%M:%SZ")
    csv.append([dateTimeFormat,personCount,doc['_source']['sourceId']])
   
csv_pd = pd.DataFrame(csv)
csv_pd.columns = ['eventTimestamp',"personCount","site"]
csv_pd['eventTimestampSort'] = pd.to_datetime(csv_pd.eventTimestamp)
csv_pd.sort_values(by = ['eventTimestampSort'],inplace=True,ascending=True)
csv_pd.drop('eventTimestampSort',axis = 'columns',inplace=True)

with io.StringIO() as csv_buffer:
    csv_pd.to_csv(csv_buffer, index=False,header=None)

    response = s3_client.put_object(
        Bucket=AWS_S3_BUCKET, Key=S3_FILE_NAME, Body=csv_buffer.getvalue(),ACL='public-read',ContentType = 'text/csv'
    )






