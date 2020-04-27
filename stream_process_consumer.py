from kafka import KafkaConsumer,KafkaProducer
from multiprocessing import Process
import _pickle as  cPickle
from multiprocessing import Process, Value,Pool,Queue, Manager
import base64
import time
import re
from kafka import TopicPartition
import datetime
import yaml
import os
import psutil
import datetime
manager = Manager()
dict_timestamp = manager.dict()
import random

#importing camera config file for camera configurations
try:
    with open("config/cameras.yml", 'r') as ymlfile:
        cfg_camera = yaml.load(ymlfile)
except IOError:
    print("Camera Config file does not appear to exist.")

########################################################################################
global db
import pymongo

#connecting to the specified database
def connectDB(dbObject):
    global db
    try:
        print("MongoDB Connection Started")
        client = pymongo.MongoClient(dbObject['mongodb']['address'], dbObject['mongodb']['port'])
        db = client[dbObject['mongodb']['dbName']]
        print("MongoDB Connection successfull")
    except:
        print("DB Connection error occurred")

#importing server config file for camera configurations
try:
    with open("config/server.yml", 'r') as ymlfile:
        cfg_server = yaml.load(ymlfile)
    connectDB(cfg_server['database'])
except IOError:
    print("Server Config file does not appear to exist.")

group_id = cfg_server['kafka']['groupIdStreamConsumer']
topic_ingestion = cfg_server['kafka']['vidAcqTopicOut']
topic_out = cfg_server['kafka']['streamConsumerTopicOut']
num_workers = cfg_server['streamProcess']['numworkers']
#########################################################################################
#For making the camera array dynamically
cameras = db.cameras
camera_array = []
for camera in cameras.find():
    if(camera['status'] == 1 or camera['status'] == 2):
        cam_password = str(camera['login']['password'])
        if(cam_password == 'password@123'):
            cam_password = 'password%40123'
        if(camera['hardware']['make'] == 'Covert'):
            rtsp_link = "rtsp://" + camera['login']['username'] + ":" + cam_password + '@' + camera['hardware']['ip'] + "/live/0/MAIN"
        elif(camera['hardware']['make'] == 'Samsung'):
            rtsp_link = "rtsp://" + camera['login']['username'] + ":" + cam_password + '@' + camera['hardware']['ip'] + "/onvif/profile2/media.smp"
        elif(camera['hardware']['make'] == 'Hikvision'):
            rtsp_link = "rtsp://" + camera['login']['username'] + ":" + cam_password + '@' + camera['hardware']['ip'] + "/Streaming/Channels/101"
        camera_array.append((camera['camName'],rtsp_link, camera['_id']))



def consumer_topic(topic_ingestion,topic_out,group_id,i):
    lang_current = "python"
    def value_desiralizer(lang):

        def des_c(msg):
            return base64.b64decode(msg)
        def des_python(msg):
            return cPickle.loads(msg)

        if lang=="cpp":
          return des_c
        else :
            return des_python

    def parse_time(msg,lang):

            def parse_time_c(msg):
                time = msg["time"]
                x = datetime.datetime.strptime(time,'%Y-%m-%d %H:%M:%S:%f')
                return x

            def parse_time_python(msg):
                return msg["KafkaTimestamp"]
            if lang == "cpp":
                return parse_time_c(msg)
            else :
                return parse_time_python(msg)


    def parse_message(msg,lang):

        def message_parser_c(msg):
            x={}
            # print (re.findall(r'[0-9]+(?:\.[0-9]+){3}', msg.headers[0][1].decode()))
            x["camera"]= msg.headers[0][1].decode()
            x["image"] = msg.value
            x["KafkaTimestamp"] = "NA"
            x["time"] = msg.headers[1][1].decode()
            x["key_t"] = msg.headers[0][1]
            x["height"] = "NA"
            x["width"] = "NA"
            return x

        def message_parser_python(msg):
            msg.value["KafkaTimestamp"] = msg.timestamp
            # msg.value["headers"] = str(msg.headers)
            msg.value["key_t"] = msg.key
            return msg.value

        if lang == "cpp":
            return message_parser_c(msg)
        else:
            return message_parser_python(msg)

    def get_time_diff(msg1,msg2,lang):

        def time_diff_python(msg1,msg2):
            t1 = parse_time(msg1,"python")
            t2 = msg2
            return abs(t1-t2)

        def time_diff_c(msg1,msg2):
            t1 = parse_time(msg1,"cpp")
            t2 = msg2
            return (t1-t2).total_seconds()*1000
        if lang == "cpp":
            return time_diff_c(msg1,msg2)
        else:
            return time_diff_python(msg1,msg2)

    def set_time(lang,msg):
        if lang=="cpp":
            dict_timestamp[i]= msg["time"]
        else:
            dict_timestamp[i] = msg["KafkaTimestamp"]
            print ("pp")



    pid = os.getpid()
    ps_proc = psutil.Process(pid=pid)
    ps_proc.cpu_affinity(list(range(10,30)))
    # consumer = KafkaConsumer(group_id=group_id,bootstrap_servers='localhost:9092',value_deserializer=lambda m: cPickle.loads(m))
    consumer = KafkaConsumer(group_id=group_id,bootstrap_servers='localhost:9092',value_deserializer=value_desiralizer(lang_current))
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: cPickle.dumps(v))

    partition_v = TopicPartition(topic_ingestion, i)
    consumer.assign([TopicPartition(topic_ingestion, i)])
    print ("assigned partitions to consumer {}  - {}".format(pid,i) )
    #dict_timestamp[i] = 1547547728999
    for msg in consumer:
        # print ("------ msg from partition {}".format(msg.partition))
        msg_parsed = parse_message(msg,lang_current)
        print(msg_parsed["camera"], msg_parsed["time"],"process : {}".format(pid))
        try :
            print(dict_timestamp[i],"here1")
        except:
          set_time(lang_current,msg_parsed)
          # print (dict_timestamp[i],"88888888888")

        # print(dict_timestamp[i],"here")
        # print (msg_parsed["time"])
        # print (get_time_diff(msg_parsed,dict_timestamp[i],"python"))
        if (get_time_diff(msg_parsed,dict_timestamp[i],"python")) >2500:

            set_time(lang_current,msg_parsed)
            print(msg_parsed["key_t"], msg_parsed["camera"], msg_parsed["time"])
            value = msg_parsed
            if num_workers is not None and num_workers > 0 :
                part = random.randint(0,num_workers-1)
            else :
                part = 0
            print ("---------partition : ",part)
            producer.send(topic_out,value=value,partition=part)

procs=[]
for x in range(len(camera_array)):
         proc = Process(target=consumer_topic,args = (topic_ingestion,topic_out,group_id,x))
         proc.daemon = True
         procs.append(proc)

for proc in procs:
    print ("##################################  Starting consumer ########################")
    proc.start()

for proc in procs:
        print ("Joining...")
        proc.join()
