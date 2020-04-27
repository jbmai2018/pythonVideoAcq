import sys
import time

from kafka import KafkaProducer,KeyedProducer
from threading import Thread
from time import sleep
from multiprocessing import Pool,Pipe
import multiprocessing as mp
import datetime
from threading import Thread
import json
import base64
# from bson import json_util
import json
import _pickle as cPickle
import random
# from utils_ob.app_utils import WebcamVideoStream
# package protot
# from imutils.video.webcamvideostream import WebcamVideoStream
from cv2 import imencode
import cv2
from multiprocessing import Process,Queue
from multiprocessing import Manager
import hashlib
import threading
import time
import os
import sys
import multiprocessing
import logging
import psutil
import gc
import threading
import queue
from collections import namedtuple
import threading

topics = []

import yaml
global db
import pymongo
import csv

#########################################################################################

topic_test_acq = "singlevidpart"
topic_kafka = 'VideoStream_zero_two'
camera_array = []

with open('cams.csv', mode='r') as infile:
    reader = csv.reader(infile)
    for rows in reader:
            cam_password = str(rows[3])
            if(cam_password == 'password@123'):
                cam_password = 'password%40123'
            if(rows[1] == 'Covert'):
                rtsp_link = "rtsp://" + rows[2] + ":" + rows[3] + '@' + rows[4] + "/live/0/MAIN"
            elif(rows[1] == 'Samsung'):
                rtsp_link = "rtsp://" + rows[2] + ":" + rows[3] + '@' + rows[4] + "/onvif/profile2/media.smp"
            elif(rows[1] == 'Hikvision'):
                rtsp_link = "rtsp://" + rows[2] + ":" + rows[3] + '@' + rows[4] + "/Streaming/Channels/101"
            camera_array.append((rows[0],rtsp_link, rows[0]))
############################################################################################

print(camera_array)

max_qsize = len(camera_array)*10
q_cameras = Queue(maxsize=max_qsize)
manager = Manager()

class VidAcqWorker(multiprocessing.Process):

    def __init__(self,data_queue):
        multiprocessing.Process.__init__(self)
        self.data_queue = data_queue



    def publish_camera(self,producer,camera_id,stream,mongoId,i):

        camera_id = camera_id
        stream = stream
        maxtries = 3
        video_capture_cv = cv2.VideoCapture(stream)
        key_topic = camera_id.encode('utf-8')
        data = {}
        xss = time.time()
        for iss in range(5):
            _ = video_capture_cv.grab()

        del _
        gc.collect()
        # ret,frame = video_capture_cv.read()
        _ = video_capture_cv.grab()
        _,frame = video_capture_cv.retrieve()
        timestamp = datetime.datetime.now()



        collected = gc.collect()
        yss = time.time()-xss
        counter_try = 1
        while True :

            if counter_try >= maxtries:
                break

            try:
                h, w, c = frame.shape

                print(camera_id, "under processing")
                frame = cv2.resize(frame,(1920,1080))
                buffer_ = imencode('.jpg', frame)[1].tobytes()
                data['time'] = str(timestamp)
                data['image'] = buffer_
                data['camera'] = camera_id
                data['camId'] = mongoId

                producer.send(topic_kafka, value=data,partition=i)

                print(camera_id)
                buffer = []
                frame = []
                del frame
                del buffer_
                del data
                break

            except Exception as e:
                print (e)
                print ("==> Loss from {}".format(camera_id))
                counter_try = counter_try + 1
                #continue
        video_capture_cv.release()
        del video_capture_cv
        gc.collect()
        return None

    def run(self):

       producer =  KafkaProducer(bootstrap_servers='103.94.66.107:9092', value_serializer=lambda v: cPickle.dumps(v))

       while True:
            curr_cam = self.data_queue.get()
            cam_id = curr_cam[0]
            stream = curr_cam[1]
            mongoId = curr_cam[2]
            ind = curr_cam[3]
            self.publish_camera(producer,cam_id,stream,mongoId,ind)

def producer_cameras():

        Cam = namedtuple('CameraObject','cameId,stream,mongoId,ind')
        while True:

            for i , cam in enumerate(camera_array):
                c = Cam(cam[0],cam[1],cam[2],i)
                data_list=[]
                data_list.append(c[0])
                data_list.append(c[1])
                data_list.append(c[2])
                data_list.append(c[3])
                try :

                    q_cameras.put_nowait(data_list)
                    print (q_cameras.qsize())
                except:
                    (q_cameras.qsize())
                    time.sleep(120)
                    break
            time.sleep(1)

if __name__ == '__main__':

    workers = 50
    p_list=[]

    thread_prod = Thread(target = producer_cameras)
    thread_prod.start()


    for i in range(workers):
            p = VidAcqWorker(q_cameras)
            p_list.append(p)

    for p in p_list:
        p.start()

    try:
        for p in p_list:
            p.join()

    except KeyboardInterrupt:
        for p in p_list:
            p.terminate()
