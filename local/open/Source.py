import cv2
import time
import boto3
import json
import base64  
import io
import requests
import pafy
from datetime import datetime


elasticsearch_url = ""
elasticsearch_AN = ""
elasticsearch_password = ""
elasticsearch_index = ""
sleep_time = 300
aws_access_key = ""
aws_secret_access_key = ""
aws_region_name = ""


client = boto3.client('rekognition', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_access_key,region_name= aws_region_name)

def main():
    
    while True:
        try:
            channel = pafy.new('https://www.youtube.com/watch?v=2e4PJSiXfPE')
            streamPort = channel.getbest(preftype="mp4").url 
            videoSource = cv2.VideoCapture(streamPort)

            if videoSource.isOpened():

                ret, frame = videoSource.read()

                if ret == False:

                    continue

                print("Streaming........")

                image_binary = base64.b64encode(cv2.imencode('.jpg', frame.copy())[1])

                image_binary = base64.b64decode(image_binary)

                current_time = datetime.fromtimestamp(int(time.time()))
                print(current_time)
                eventTimestamp = datetime.fromtimestamp(int(time.time()-28800))
                print(type(image_binary))
                response = client.detect_labels(Image={'Bytes': image_binary},MaxLabels=100,MinConfidence=0)
                videoSource.release()    
                nowString = eventTimestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
                print(nowString)
                data = {
                    "sourceId":"大甲鎮瀾宮",
                    "response": response,
                    "eventTimestamp": nowString

                }

                #print(response)
                

                data = json.dumps(data)
                headers={'Accept': 'application/json', 'Content-type': 'application/json'}
                elastic_url ='{elasticsearch_url}/{elasticsearch_index}/_doc/'.format(elasticsearch_url= elasticsearch_url,
                elasticsearch_index = elasticsearch_index)
                response = requests.post(elastic_url, data = data, auth=(elasticsearch_AN,elasticsearch_password), headers = headers)

                time.sleep(sleep_time)

        except:
            time.sleep(1)   
            print("fail to connect")
            continue

            
    videoSource.release()                

if __name__ == '__main__':

    main()

