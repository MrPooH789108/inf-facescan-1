"""This module check if worker is unregister but have active status , it will register worker"""

from module import alicloudDatabase
from module import alicloudMQTT
from module import connection
from pymongo import MongoClient
import configparser
import threading
import requests
import logging
import base64
import time
import sys
import os

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)

infsecinterval = config_obj["interval_sec"]
infcollection = config_obj["collection"]
infoperation = config_obj["operation"]
infdatabase = config_obj["db"]
inftopic = config_obj["topic"]
inflog = config_obj["log"]

dbUser = str(os.environ['DB_USER'])
dbPass = str(os.environ['DB_PASS'])

dbHost = "mongodb://"+infdatabase['nodes']
dbReplicaSet = infdatabase['replicaSet']
dbClient = MongoClient(host=dbHost, replicaset=dbReplicaSet, username=dbUser,
                       password=dbPass, authSource='admin', authMechanism='SCRAM-SHA-256')

dbName = infdatabase['name']
workertb = infcollection['workers']
transectiontb = infcollection['transections']

parent_topic = inftopic['parent']

# run every 10 mins = 600 seconds
unregister_review_interval = int(infsecinterval['unregister_review'])
create_worker_operation_name = infoperation['create_worker']

LOG_PATH = inflog['path']
# Creating and Configuring Logger
logger = logging.getLogger('checkUnregister')
fileHandler = logging.FileHandler(LOG_PATH+"/inf-worker-sync.log")
streamHandler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '{"timestamp":"%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "function": "%(funcName)s", "message": "%(message)s"}')
streamHandler.setFormatter(formatter)
fileHandler.setFormatter(formatter)
logger.addHandler(streamHandler)
logger.addHandler(fileHandler)
logger.setLevel(logging.DEBUG)

# reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)


def get_as_base64(url):
    return base64.b64encode(requests.get(url).content)


def checkRegistration():
    data = {}
    try:
        threading.Timer(unregister_review_interval,
                        checkRegistration).start()

        mydb = dbClient[dbName]
        mycol = mydb[workertb]

        myquery = {"devices": {"$elemMatch": {"$or": [{"status": "INACTIVE", "regester": "registered"}, {
            "$or": [{"status": "ACTIVE", "regester": "unregistered"}, {"status": "BLACKLISTED", "regester": "unregistered"}]}]}}}

        workerlist = mycol.find(myquery)

        for w in workerlist:
            print(w)
            messageId = w["registration"]["last_messageId"]
            workerCode = w['info']['workerCode']
            workerName = w['info']['name']
            workerGender = w['info']['gender']
            devices = w["devices"]
            pictureURL = w['pictureURL']

            all_transection = []

            for device in devices:
                d_status = device["status"]
                d_regis = device["regester"]

                if d_status == "ACTIVE" and d_regis == "unregistered":
                    print("unregister device : "+str(device))
                    tempCardType = 0  # permanent

                    if workerGender == "MALE":
                        gender = 0  # male
                    else:
                        gender = 1  # female

                    worker_json = {
                        "messageId": messageId,
                        "operator": "EditPerson",
                        "info":
                        {
                            "customId": workerCode,
                            "name": workerName,
                            "gender": gender,
                            "address": device["facility"],
                            "idCard": workerCode,  # ID number show on web service
                            "tempCardType": tempCardType,
                            "personType": 0,  # 0=White list, 1=blacklist
                            "cardType": 0,
                            "picURI": pictureURL
                        }
                    }

                    print("---- worker_json ----")
                    print(worker_json)
                    print("---- pub_topic ----")
                    pub_topic = parent_topic+"/face/"+device["deviceCode"]
                    print(pub_topic)
                    alicloudMQTT.mqttPublish(worker_json, pub_topic)

                    transection = {}
                    transection["topic"] = pub_topic
                    transection["body"] = worker_json
                    transection["ackcode"] = "wating ack"
                    transection["ackdetail"] = ""

                    all_transection.append(transection)
                    time.sleep(5)

                elif d_status == "INACTIVE" and d_regis == "registered":
                    worker_json = {
                        "operator": "DelPerson",
                        "messageId": messageId,
                        "info":
                            {
                                "customId": workerCode
                            }
                    }

                    print("---- worker_json ----")
                    print(worker_json)
                    print("---- pub_topic ----")
                    pub_topic = parent_topic+"/face/"+device["deviceCode"]
                    print(pub_topic)
                    alicloudMQTT.mqttPublish(worker_json, pub_topic)

                    transection = {}
                    transection["topic"] = pub_topic
                    transection["body"] = worker_json
                    transection["ackcode"] = "wating ack"
                    transection["ackdetail"] = ""

                    all_transection.append(transection)
                    time.sleep(5)

                elif d_status == "BLACKLISTED" and d_regis == "unregistered":
                    print("unregister device : "+str(device))
                    tempCardType = 0  # permanent

                    if workerGender == "MALE":
                        gender = 0  # male
                    else:
                        gender = 1  # female
                        
                    worker_json = {
                        "messageId": messageId,
                        "operator": "EditPerson",
                        "info":
                        {
                            "customId": workerCode,
                            "name": workerName,
                            "gender": gender,
                            "address": device["facility"],
                            "idCard": workerCode,  # ID number show on web service
                            "tempCardType": tempCardType,
                            "personType": 1,  # 0=Whitelist, 1=blacklist
                            "cardType": 0,
                            "picURI": pictureURL
                        }
                    }

                    print("---- worker_json ----")
                    print(worker_json)
                    print("---- pub_topic ----")
                    pub_topic = parent_topic+"/face/"+device["deviceCode"]
                    print(pub_topic)
                    alicloudMQTT.mqttPublish(worker_json, pub_topic)

                    transection = {}
                    transection["topic"] = pub_topic
                    transection["body"] = worker_json
                    transection["ackcode"] = "wating ack"
                    transection["ackdetail"] = ""

                    all_transection.append(transection)
                    time.sleep(5)

            data = {
                "_id": messageId,
                "messageId": messageId,
                "operation": create_worker_operation_name,
                "info": w['info'],
                "transection": all_transection,
                "recieveAllack": False,
                "recheck": 0,
                "timeout": False
            }

            isSuccess = alicloudDatabase.insertToDB(transectiontb, data)

            if isSuccess == True:
                print("Insert transection success")
                log = {
                    "data": data,
                    "tasks": {
                        "database": {
                            "collection": workertb,
                            "operation": "insert",
                            "success": isSuccess
                        }
                    }
                }

                logs = str(log)
                logger.info(logs.replace("'", '"'))
            else:
                print("Transection already exist")

        print()

    except Exception as e:
        print(str(e))
        log = {
            "data": data,
            "error": str(e)
        }
        logs = str(log)
        logger.error(logs.replace("'", '"'))


checkRegistration()
