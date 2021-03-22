import cv2
import time
import copy
import threading
import boto3
import json
import base64  
import awsconfig
import io
import requests
import pafy
from PIL import Image
from datetime import datetime


elasitcsearch_url = ""
elasticsearch_AN = ""
elasticsearch_password = ""
elasticsearch_index = ""
sleep_time = 300
aws_access_key = ""
aws_secret_access_key = ""
aws_region_name = ""


client = boto3.client('rekognition', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_access_key,region_name= aws_region_name)
frame = None
def stream():

    while True:
        try:
            if videoSource.isOpened():
                ret, frame = videoSource.read()
                Height , Width = frame.shape[:2]
                scale = None
                if Height/640 > Width/960:
                    scale = Height/640
                else:
                    scale = Width/960
                frame = cv2.resize(frame, (int(Width/scale), int(Height/scale)), interpolation=cv2.INTER_CUBIC)
                #cv2.imshow("CSI",frame)
                #cv2.waitKey(1)
                if ret == False:
                    videoSource = cv2.VideoCapture(streamPort)
        except:
            print('Source video is unavailable! reconnecting ....')

    videoSource.release()





def main():
    
    while True:
        try:
            channel = pafy.new('https://www.youtube.com/watch?v=WHeYEZIUbZQ')
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
                eventTimestamp = datetime.fromtimestamp(int(time.time()-28800))
                print(type(image_binary))
                image_id = "image_" + eventTimestamp.strftime("%Y.%m.%d.%H%M") +".jpg"



                response = client.detect_labels(Image={'Bytes': image_binary},MaxLabels=100,MinConfidence=0)
                #data['eventTimestamp'] = datetime.fromtimestamp(float(data['eventTimestamp']))
                videoSource.release()    
                nowString = eventTimestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
                print(nowString)
                data = {
                    "sourceId":"東區樂成宮",
                    "response": response,
                    "eventTimestamp": nowString

                }
                #print(response)
                

                data = json.dumps(data)
                headers={'Accept': 'application/json', 'Content-type': 'application/json'}
                elastic_url ='{elasticsearch_url}/{elasticsearch_index}/_doc/'.format(elasticsearch_url= elasticsearch_url,
                elasticsearch_index = elasticsearch_index)
                response = requests.post(elastic_url, data = data, auth=(elasticsearch_AN,elasticsearch_password), headers = headers)
                # print('elasticsearch Check---------------------------')
                time.sleep(sleep_time)

        except:
            time.sleep(1)   
            print("fail to connect")
            continue

            
    videoSource.release()                

if __name__ == '__main__':

    main()
